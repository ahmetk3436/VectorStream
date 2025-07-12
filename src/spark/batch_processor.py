#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp
from loguru import logger
import json

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.spark.embedding_job import SparkEmbeddingJob
from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig
from src.exceptions.embedding_exceptions import EmbeddingProcessingError

class SparkBatchProcessor:
    """
    Spark tabanlı batch işleme sistemi
    
    Bu sınıf büyük veri setlerini batch'ler halinde işler,
    embedding'leri oluşturur ve sonuçları depolar.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Batch processor'ı başlat
        
        Args:
            config: Sistem konfigürasyonu
        """
        self.config = config
        self.spark_config = config.get('spark', {})
        self.qdrant_config = config.get('qdrant', {})
        self.batch_config = config.get('batch', {})
        
        # Batch ayarları
        self.batch_size = self.batch_config.get('size', 10000)
        self.max_retries = self.batch_config.get('max_retries', 3)
        self.retry_delay = self.batch_config.get('retry_delay', 60)  # seconds
        
        # Paths
        self.input_path = self.batch_config.get('input_path', '/data/input')
        self.output_path = self.batch_config.get('output_path', '/data/output')
        self.checkpoint_path = self.batch_config.get('checkpoint_path', '/data/checkpoints')
        self.failed_path = self.batch_config.get('failed_path', '/data/failed')
        
        # Embedding job
        self.embedding_job = SparkEmbeddingJob(self.spark_config)
        
        # Circuit breaker
        self.circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=300.0,  # 5 minutes
            timeout=1800.0  # 30 minutes for large batches
        )
    
    def initialize(self):
        """
        Batch processor'ı başlat
        """
        try:
            logger.info("Batch processor başlatılıyor...")
            
            # Spark session'ını başlat
            self.embedding_job.initialize_spark()
            
            # Gerekli dizinleri oluştur
            self._create_directories()
            
            logger.info("✅ Batch processor başlatıldı")
            
        except Exception as e:
            logger.error(f"Batch processor başlatma hatası: {e}")
            raise EmbeddingProcessingError(f"Batch processor initialization failed: {e}")
    
    def _create_directories(self):
        """
        Gerekli dizinleri oluştur
        """
        directories = [
            self.input_path,
            self.output_path,
            self.checkpoint_path,
            self.failed_path
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Dizin oluşturuldu/kontrol edildi: {directory}")
    
    @circuit_breaker("batch_processing")
    def process_batch_files(self, file_pattern: str = "*.json") -> Dict[str, Any]:
        """
        Batch dosyalarını işle
        
        Args:
            file_pattern: İşlenecek dosya pattern'i
            
        Returns:
            Dict[str, Any]: İşleme sonuçları
        """
        try:
            logger.info(f"Batch dosyaları işleniyor: {file_pattern}")
            
            # Input dosyalarını bul
            input_files = list(Path(self.input_path).glob(file_pattern))
            
            if not input_files:
                logger.warning(f"İşlenecek dosya bulunamadı: {self.input_path}/{file_pattern}")
                return {
                    'status': 'no_files',
                    'processed_files': 0,
                    'total_records': 0,
                    'failed_files': 0
                }
            
            logger.info(f"İşlenecek dosya sayısı: {len(input_files)}")
            
            results = {
                'status': 'success',
                'processed_files': 0,
                'total_records': 0,
                'failed_files': 0,
                'file_results': []
            }
            
            # Her dosyayı işle
            for file_path in input_files:
                try:
                    file_result = self._process_single_file(file_path)
                    results['file_results'].append(file_result)
                    
                    if file_result['status'] == 'success':
                        results['processed_files'] += 1
                        results['total_records'] += file_result['record_count']
                    else:
                        results['failed_files'] += 1
                        
                except Exception as e:
                    logger.error(f"Dosya işleme hatası {file_path}: {e}")
                    results['failed_files'] += 1
                    results['file_results'].append({
                        'file': str(file_path),
                        'status': 'error',
                        'error': str(e),
                        'record_count': 0
                    })
            
            # Genel durum
            if results['failed_files'] > 0:
                results['status'] = 'partial_success' if results['processed_files'] > 0 else 'failed'
            
            logger.info(f"✅ Batch işleme tamamlandı: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Batch işleme hatası: {e}")
            raise EmbeddingProcessingError(f"Batch processing failed: {e}")
    
    def _process_single_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Tek dosyayı işle
        
        Args:
            file_path: İşlenecek dosya yolu
            
        Returns:
            Dict[str, Any]: Dosya işleme sonucu
        """
        try:
            logger.info(f"Dosya işleniyor: {file_path}")
            
            # Checkpoint kontrolü
            checkpoint_file = Path(self.checkpoint_path) / f"{file_path.stem}.checkpoint"
            if checkpoint_file.exists():
                logger.info(f"Dosya zaten işlenmiş (checkpoint mevcut): {file_path}")
                with open(checkpoint_file, 'r') as f:
                    return json.load(f)
            
            # Dosyayı oku
            spark = self.embedding_job.spark
            df = spark.read.json(str(file_path))
            
            record_count = df.count()
            logger.info(f"Dosya kayıt sayısı: {record_count}")
            
            if record_count == 0:
                logger.warning(f"Boş dosya: {file_path}")
                return {
                    'file': str(file_path),
                    'status': 'empty',
                    'record_count': 0,
                    'processing_time': 0
                }
            
            start_time = datetime.now()
            
            # Veri kalitesi kontrolü
            df_validated = self._validate_data(df)
            
            # Embedding'leri oluştur
            df_processed = self.embedding_job.process_dataframe(df_validated)
            
            # Çıktı dosya yolu
            output_file = Path(self.output_path) / f"{file_path.stem}_processed.json"
            
            # Sonuçları kaydet
            df_processed.write.mode("overwrite").json(str(output_file))
            
            # Qdrant'a yaz
            self.embedding_job.write_to_qdrant(df_processed, self.qdrant_config)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Checkpoint oluştur
            result = {
                'file': str(file_path),
                'status': 'success',
                'record_count': record_count,
                'processing_time': processing_time,
                'output_file': str(output_file),
                'timestamp': datetime.now().isoformat()
            }
            
            with open(checkpoint_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            # Başarılı dosyayı arşivle
            self._archive_processed_file(file_path)
            
            logger.info(f"✅ Dosya başarıyla işlendi: {file_path} ({processing_time:.2f}s)")
            return result
            
        except Exception as e:
            logger.error(f"Dosya işleme hatası {file_path}: {e}")
            
            # Başarısız dosyayı failed dizinine taşı
            self._move_failed_file(file_path, str(e))
            
            return {
                'file': str(file_path),
                'status': 'error',
                'error': str(e),
                'record_count': 0,
                'timestamp': datetime.now().isoformat()
            }
    
    def _validate_data(self, df: DataFrame) -> DataFrame:
        """
        Veri kalitesi kontrolü yap
        
        Args:
            df: Kontrol edilecek DataFrame
            
        Returns:
            DataFrame: Temizlenmiş DataFrame
        """
        try:
            logger.debug("Veri kalitesi kontrolü yapılıyor...")
            
            # Gerekli sütunları kontrol et
            required_columns = ['content']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Gerekli sütunlar eksik: {missing_columns}")
            
            # Boş içerikleri filtrele
            df_clean = df.filter(
                col('content').isNotNull() & 
                (col('content') != '') &
                (col('content') != ' ')
            )
            
            # ID sütunu yoksa oluştur
            if 'id' not in df.columns:
                df_clean = df_clean.withColumn('id', 
                    concat(lit('doc_'), monotonically_increasing_id().cast('string'))
                )
            
            # Timestamp sütunu yoksa oluştur
            if 'timestamp' not in df.columns:
                df_clean = df_clean.withColumn('timestamp', current_timestamp())
            
            # Çok uzun içerikleri kırp (32KB limit)
            max_content_length = 32768
            df_clean = df_clean.withColumn(
                'content',
                when(length(col('content')) > max_content_length,
                     substring(col('content'), 1, max_content_length)
                ).otherwise(col('content'))
            )
            
            original_count = df.count()
            clean_count = df_clean.count()
            
            logger.info(f"Veri kalitesi kontrolü: {original_count} -> {clean_count} kayıt")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Veri kalitesi kontrolü hatası: {e}")
            raise EmbeddingProcessingError(f"Data validation failed: {e}")
    
    def _archive_processed_file(self, file_path: Path):
        """
        İşlenmiş dosyayı arşivle
        
        Args:
            file_path: Arşivlenecek dosya yolu
        """
        try:
            archive_dir = Path(self.input_path) / 'processed'
            archive_dir.mkdir(exist_ok=True)
            
            archive_path = archive_dir / f"{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_path.suffix}"
            file_path.rename(archive_path)
            
            logger.debug(f"Dosya arşivlendi: {file_path} -> {archive_path}")
            
        except Exception as e:
            logger.warning(f"Dosya arşivleme hatası {file_path}: {e}")
    
    def _move_failed_file(self, file_path: Path, error_message: str):
        """
        Başarısız dosyayı failed dizinine taşı
        
        Args:
            file_path: Taşınacak dosya yolu
            error_message: Hata mesajı
        """
        try:
            failed_dir = Path(self.failed_path)
            failed_dir.mkdir(exist_ok=True)
            
            failed_path = failed_dir / f"{file_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{file_path.suffix}"
            file_path.rename(failed_path)
            
            # Hata bilgisini kaydet
            error_file = failed_path.with_suffix('.error')
            with open(error_file, 'w') as f:
                json.dump({
                    'original_file': str(file_path),
                    'error': error_message,
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2)
            
            logger.warning(f"Başarısız dosya taşındı: {file_path} -> {failed_path}")
            
        except Exception as e:
            logger.error(f"Başarısız dosya taşıma hatası {file_path}: {e}")
    
    def process_scheduled_batches(self) -> Dict[str, Any]:
        """
        Zamanlanmış batch'leri işle
        
        Returns:
            Dict[str, Any]: İşleme sonuçları
        """
        try:
            logger.info("Zamanlanmış batch işleme başlatılıyor...")
            
            # Farklı dosya türlerini işle
            file_patterns = [
                "*.json",
                "*.jsonl",
                "pending_*.json"
            ]
            
            all_results = {
                'total_processed_files': 0,
                'total_records': 0,
                'total_failed_files': 0,
                'pattern_results': []
            }
            
            for pattern in file_patterns:
                logger.info(f"Pattern işleniyor: {pattern}")
                
                pattern_result = self.process_batch_files(pattern)
                pattern_result['pattern'] = pattern
                
                all_results['pattern_results'].append(pattern_result)
                all_results['total_processed_files'] += pattern_result['processed_files']
                all_results['total_records'] += pattern_result['total_records']
                all_results['total_failed_files'] += pattern_result['failed_files']
            
            # Genel durum
            if all_results['total_failed_files'] > 0:
                all_results['status'] = 'partial_success' if all_results['total_processed_files'] > 0 else 'failed'
            else:
                all_results['status'] = 'success' if all_results['total_processed_files'] > 0 else 'no_files'
            
            logger.info(f"✅ Zamanlanmış batch işleme tamamlandı: {all_results['status']}")
            return all_results
            
        except Exception as e:
            logger.error(f"Zamanlanmış batch işleme hatası: {e}")
            raise EmbeddingProcessingError(f"Scheduled batch processing failed: {e}")
    
    def cleanup_old_files(self, days: int = 7):
        """
        Eski dosyaları temizle
        
        Args:
            days: Kaç gün önceki dosyalar silinecek
        """
        try:
            logger.info(f"Eski dosyalar temizleniyor ({days} gün öncesi)...")
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Temizlenecek dizinler
            cleanup_dirs = [
                Path(self.checkpoint_path),
                Path(self.input_path) / 'processed',
                Path(self.failed_path)
            ]
            
            total_cleaned = 0
            
            for cleanup_dir in cleanup_dirs:
                if not cleanup_dir.exists():
                    continue
                
                for file_path in cleanup_dir.iterdir():
                    if file_path.is_file():
                        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                        
                        if file_time < cutoff_date:
                            file_path.unlink()
                            total_cleaned += 1
                            logger.debug(f"Eski dosya silindi: {file_path}")
            
            logger.info(f"✅ Temizlik tamamlandı: {total_cleaned} dosya silindi")
            
        except Exception as e:
            logger.error(f"Dosya temizleme hatası: {e}")
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        İşleme istatistiklerini getir
        
        Returns:
            Dict[str, Any]: İstatistikler
        """
        try:
            stats = {
                'input_files': len(list(Path(self.input_path).glob('*.json'))),
                'processed_files': 0,
                'failed_files': 0,
                'checkpoints': 0
            }
            
            # Processed files
            processed_dir = Path(self.input_path) / 'processed'
            if processed_dir.exists():
                stats['processed_files'] = len(list(processed_dir.glob('*')))
            
            # Failed files
            if Path(self.failed_path).exists():
                stats['failed_files'] = len(list(Path(self.failed_path).glob('*.json')))
            
            # Checkpoints
            if Path(self.checkpoint_path).exists():
                stats['checkpoints'] = len(list(Path(self.checkpoint_path).glob('*.checkpoint')))
            
            return stats
            
        except Exception as e:
            logger.error(f"İstatistik alma hatası: {e}")
            return {}
    
    def stop(self):
        """
        Batch processor'ı durdur
        """
        try:
            logger.info("Batch processor durduruluyor...")
            
            if self.embedding_job:
                self.embedding_job.stop()
            
            logger.info("✅ Batch processor durduruldu")
            
        except Exception as e:
            logger.error(f"Batch processor durdurma hatası: {e}")

# Helper imports for DataFrame operations
from pyspark.sql.functions import concat, monotonically_increasing_id, length, substring