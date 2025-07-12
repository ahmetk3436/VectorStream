#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import threading
import time
import psutil
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, concat, monotonically_increasing_id, 
    length, substring, size, count as spark_count
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from loguru import logger
import json

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.spark.embedding_job import SparkEmbeddingJob
from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig
from src.exceptions.embedding_exceptions import EmbeddingProcessingError
from src.utils.error_handler import retry_with_policy, RetryPolicy, BackoffStrategy

@dataclass
class BatchConfig:
    """Batch processing konfigürasyonu"""
    # Batch boyutları
    min_batch_size: int = 1000
    max_batch_size: int = 50000
    adaptive_batch_size: bool = True
    
    # Memory management
    max_memory_usage_percent: float = 80.0
    memory_check_interval: int = 100  # Her 100 kayıtta bir kontrol
    
    # Parallel processing
    max_parallel_files: int = 4
    enable_parallel_processing: bool = True
    
    # Performance optimization
    enable_caching: bool = True
    cache_storage_level: str = "MEMORY_AND_DISK_SER"
    enable_compression: bool = True
    compression_codec: str = "snappy"
    
    # Monitoring
    enable_metrics: bool = True
    metrics_interval: int = 60  # seconds
    
    # Checkpointing
    checkpoint_interval: int = 1000  # Her 1000 kayıtta bir
    enable_incremental_checkpoints: bool = True

@dataclass
class ProcessingMetrics:
    """İşleme metrikleri"""
    start_time: datetime
    end_time: Optional[datetime] = None
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    avg_processing_time_per_file: float = 0.0
    avg_processing_time_per_record: float = 0.0
    memory_usage_peak: float = 0.0
    cpu_usage_avg: float = 0.0
    throughput_records_per_second: float = 0.0
    
    def calculate_final_metrics(self):
        """Final metrikleri hesapla"""
        if self.end_time and self.processed_files > 0:
            total_time = (self.end_time - self.start_time).total_seconds()
            self.avg_processing_time_per_file = total_time / self.processed_files
            
            if self.processed_records > 0:
                self.avg_processing_time_per_record = total_time / self.processed_records
                self.throughput_records_per_second = self.processed_records / total_time

class OptimizedSparkBatchProcessor:
    """
    Optimize edilmiş Spark tabanlı batch işleme sistemi
    
    Bu sınıf yüksek performanslı batch processing için optimize edilmiştir:
    - Adaptive batch sizing
    - Memory-efficient processing
    - Parallel file processing
    - Real-time performance monitoring
    - Advanced caching strategies
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Optimized batch processor'ı başlat
        
        Args:
            config: Sistem konfigürasyonu
        """
        self.config = config
        self.spark_config = config.get('spark', {})
        self.qdrant_config = config.get('qdrant', {})
        
        # Batch konfigürasyonu
        batch_config_dict = config.get('batch', {})
        self.batch_config = BatchConfig(
            min_batch_size=batch_config_dict.get('min_size', 1000),
            max_batch_size=batch_config_dict.get('max_size', 50000),
            adaptive_batch_size=batch_config_dict.get('adaptive_sizing', True),
            max_memory_usage_percent=batch_config_dict.get('max_memory_percent', 80.0),
            max_parallel_files=batch_config_dict.get('max_parallel_files', 4),
            enable_parallel_processing=batch_config_dict.get('enable_parallel', True),
            enable_caching=batch_config_dict.get('enable_caching', True),
            enable_metrics=batch_config_dict.get('enable_metrics', True)
        )
        
        # Paths
        self.input_path = batch_config_dict.get('input_path', '/data/input')
        self.output_path = batch_config_dict.get('output_path', '/data/output')
        self.checkpoint_path = batch_config_dict.get('checkpoint_path', '/data/checkpoints')
        self.failed_path = batch_config_dict.get('failed_path', '/data/failed')
        self.metrics_path = batch_config_dict.get('metrics_path', '/data/metrics')
        
        # Components
        self.embedding_job = SparkEmbeddingJob(self.spark_config)
        
        # Circuit breaker
        self.circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=300.0,
            timeout=3600.0  # 1 hour for large batches
        )
        
        # Metrics and monitoring
        self.current_metrics: Optional[ProcessingMetrics] = None
        self.metrics_lock = threading.Lock()
        self.stop_monitoring = threading.Event()
        
        # Current batch size (adaptive)
        self.current_batch_size = self.batch_config.min_batch_size
        
        # Retry policy for batch operations
        self.batch_retry_policy = RetryPolicy(
            max_attempts=3,
            base_delay=30.0,
            max_delay=300.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            timeout=1800.0,  # 30 minutes
            retryable_exceptions=[Exception],
            non_retryable_exceptions=[ValueError, TypeError]
        )
    
    def initialize(self):
        """
        Optimized batch processor'ı başlat
        """
        try:
            logger.info("Optimized batch processor başlatılıyor...")
            
            # Spark session'ını optimize et
            self._optimize_spark_session()
            
            # Gerekli dizinleri oluştur
            self._create_directories()
            
            # Performance monitoring başlat
            if self.batch_config.enable_metrics:
                self._start_monitoring()
            
            logger.info("✅ Optimized batch processor başlatıldı")
            logger.info(f"Batch config: min_size={self.batch_config.min_batch_size}, "
                       f"max_size={self.batch_config.max_batch_size}, "
                       f"parallel_files={self.batch_config.max_parallel_files}")
            
        except Exception as e:
            logger.error(f"Optimized batch processor başlatma hatası: {e}")
            raise EmbeddingProcessingError(f"Optimized batch processor initialization failed: {e}")
    
    def _optimize_spark_session(self):
        """
        Spark session'ını performance için optimize et
        """
        try:
            # Embedding job'ı başlat
            self.embedding_job.initialize_spark()
            
            spark = self.embedding_job.spark
            
            # Adaptive query execution
            spark.conf.set("spark.sql.adaptive.enabled", "true")
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            
            # Memory optimization
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(self.current_batch_size))
            
            # Compression
            if self.batch_config.enable_compression:
                spark.conf.set("spark.sql.parquet.compression.codec", self.batch_config.compression_codec)
                spark.conf.set("spark.io.compression.codec", self.batch_config.compression_codec)
            
            # Caching
            if self.batch_config.enable_caching:
                spark.conf.set("spark.sql.cache.serializer", "org.apache.spark.serializer.KryoSerializer")
            
            logger.info("✅ Spark session optimize edildi")
            
        except Exception as e:
            logger.error(f"Spark optimization hatası: {e}")
            raise
    
    def _create_directories(self):
        """
        Gerekli dizinleri oluştur
        """
        directories = [
            self.input_path,
            self.output_path,
            self.checkpoint_path,
            self.failed_path,
            self.metrics_path
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Dizin oluşturuldu/kontrol edildi: {directory}")
    
    def _start_monitoring(self):
        """
        Performance monitoring başlat
        """
        def monitor():
            while not self.stop_monitoring.is_set():
                try:
                    if self.current_metrics:
                        with self.metrics_lock:
                            # Memory usage
                            memory_percent = psutil.virtual_memory().percent
                            self.current_metrics.memory_usage_peak = max(
                                self.current_metrics.memory_usage_peak, memory_percent
                            )
                            
                            # CPU usage
                            cpu_percent = psutil.cpu_percent(interval=1)
                            self.current_metrics.cpu_usage_avg = (
                                (self.current_metrics.cpu_usage_avg + cpu_percent) / 2
                            )
                            
                            # Adaptive batch size adjustment
                            if self.batch_config.adaptive_batch_size:
                                self._adjust_batch_size(memory_percent, cpu_percent)
                    
                    time.sleep(self.batch_config.metrics_interval)
                    
                except Exception as e:
                    logger.warning(f"Monitoring hatası: {e}")
        
        monitoring_thread = threading.Thread(target=monitor, daemon=True)
        monitoring_thread.start()
        logger.debug("Performance monitoring başlatıldı")
    
    def _adjust_batch_size(self, memory_percent: float, cpu_percent: float):
        """
        Memory ve CPU kullanımına göre batch size'ı ayarla
        
        Args:
            memory_percent: Memory kullanım yüzdesi
            cpu_percent: CPU kullanım yüzdesi
        """
        try:
            old_batch_size = self.current_batch_size
            
            # Memory pressure - batch size'ı küçült
            if memory_percent > self.batch_config.max_memory_usage_percent:
                self.current_batch_size = max(
                    self.batch_config.min_batch_size,
                    int(self.current_batch_size * 0.8)
                )
            
            # Low resource usage - batch size'ı büyüt
            elif memory_percent < 60 and cpu_percent < 70:
                self.current_batch_size = min(
                    self.batch_config.max_batch_size,
                    int(self.current_batch_size * 1.2)
                )
            
            if old_batch_size != self.current_batch_size:
                logger.info(f"Adaptive batch size: {old_batch_size} -> {self.current_batch_size} "
                           f"(mem: {memory_percent:.1f}%, cpu: {cpu_percent:.1f}%)")
                
                # Spark configuration'ı güncelle
                if self.embedding_job.spark:
                    self.embedding_job.spark.conf.set(
                        "spark.sql.execution.arrow.maxRecordsPerBatch", 
                        str(self.current_batch_size)
                    )
        
        except Exception as e:
            logger.warning(f"Batch size adjustment hatası: {e}")
    
    @circuit_breaker("optimized_batch_processing")
    @retry_with_policy
    def process_batch_files_optimized(self, file_pattern: str = "*.json") -> Dict[str, Any]:
        """
        Optimize edilmiş batch dosya işleme
        
        Args:
            file_pattern: İşlenecek dosya pattern'i
            
        Returns:
            Dict[str, Any]: İşleme sonuçları
        """
        try:
            logger.info(f"Optimized batch işleme başlatılıyor: {file_pattern}")
            
            # Metrics başlat
            self.current_metrics = ProcessingMetrics(start_time=datetime.now())
            
            # Input dosyalarını bul ve sırala (boyuta göre)
            input_files = self._get_sorted_input_files(file_pattern)
            
            if not input_files:
                logger.warning(f"İşlenecek dosya bulunamadı: {self.input_path}/{file_pattern}")
                return self._create_empty_result()
            
            self.current_metrics.total_files = len(input_files)
            logger.info(f"İşlenecek dosya sayısı: {len(input_files)}")
            
            # Parallel veya sequential processing
            if self.batch_config.enable_parallel_processing and len(input_files) > 1:
                results = self._process_files_parallel(input_files)
            else:
                results = self._process_files_sequential(input_files)
            
            # Final metrics
            self.current_metrics.end_time = datetime.now()
            self.current_metrics.calculate_final_metrics()
            
            # Metrics kaydet
            self._save_metrics()
            
            logger.info(f"✅ Optimized batch işleme tamamlandı: {results['status']}")
            logger.info(f"Throughput: {self.current_metrics.throughput_records_per_second:.2f} records/sec")
            
            return results
            
        except Exception as e:
            logger.error(f"Optimized batch işleme hatası: {e}")
            if self.current_metrics:
                self.current_metrics.end_time = datetime.now()
                self._save_metrics()
            raise EmbeddingProcessingError(f"Optimized batch processing failed: {e}")
    
    def _get_sorted_input_files(self, file_pattern: str) -> List[Tuple[Path, int]]:
        """
        Input dosyalarını boyuta göre sıralı olarak getir
        
        Args:
            file_pattern: Dosya pattern'i
            
        Returns:
            List[Tuple[Path, int]]: (dosya_yolu, boyut) tuple'ları
        """
        try:
            input_files = []
            
            for file_path in Path(self.input_path).glob(file_pattern):
                if file_path.is_file():
                    file_size = file_path.stat().st_size
                    input_files.append((file_path, file_size))
            
            # Küçük dosyalardan büyüğe sırala (küçük dosyalar önce işlensin)
            input_files.sort(key=lambda x: x[1])
            
            return input_files
            
        except Exception as e:
            logger.error(f"Dosya listeleme hatası: {e}")
            return []
    
    def _create_empty_result(self) -> Dict[str, Any]:
        """
        Boş sonuç oluştur
        """
        return {
            'status': 'no_files',
            'processed_files': 0,
            'total_records': 0,
            'failed_files': 0,
            'processing_time': 0.0,
            'throughput': 0.0,
            'metrics': None
        }
    
    def _process_files_parallel(self, input_files: List[Tuple[Path, int]]) -> Dict[str, Any]:
        """
        Dosyaları parallel olarak işle
        
        Args:
            input_files: İşlenecek dosyalar
            
        Returns:
            Dict[str, Any]: İşleme sonuçları
        """
        try:
            logger.info(f"Parallel processing başlatılıyor: {self.batch_config.max_parallel_files} thread")
            
            results = {
                'status': 'success',
                'processed_files': 0,
                'total_records': 0,
                'failed_files': 0,
                'file_results': []
            }
            
            with ThreadPoolExecutor(max_workers=self.batch_config.max_parallel_files) as executor:
                # Submit all files
                future_to_file = {
                    executor.submit(self._process_single_file_optimized, file_path, file_size): (file_path, file_size)
                    for file_path, file_size in input_files
                }
                
                # Process completed futures
                for future in as_completed(future_to_file):
                    file_path, file_size = future_to_file[future]
                    
                    try:
                        file_result = future.result()
                        results['file_results'].append(file_result)
                        
                        if file_result['status'] == 'success':
                            results['processed_files'] += 1
                            results['total_records'] += file_result['record_count']
                            
                            with self.metrics_lock:
                                self.current_metrics.processed_files += 1
                                self.current_metrics.processed_records += file_result['record_count']
                        else:
                            results['failed_files'] += 1
                            
                            with self.metrics_lock:
                                self.current_metrics.failed_files += 1
                                
                    except Exception as e:
                        logger.error(f"Parallel file processing hatası {file_path}: {e}")
                        results['failed_files'] += 1
                        
                        with self.metrics_lock:
                            self.current_metrics.failed_files += 1
                        
                        results['file_results'].append({
                            'file': str(file_path),
                            'status': 'error',
                            'error': str(e),
                            'record_count': 0
                        })
            
            # Genel durum
            if results['failed_files'] > 0:
                results['status'] = 'partial_success' if results['processed_files'] > 0 else 'failed'
            
            return results
            
        except Exception as e:
            logger.error(f"Parallel processing hatası: {e}")
            raise
    
    def _process_files_sequential(self, input_files: List[Tuple[Path, int]]) -> Dict[str, Any]:
        """
        Dosyaları sequential olarak işle
        
        Args:
            input_files: İşlenecek dosyalar
            
        Returns:
            Dict[str, Any]: İşleme sonuçları
        """
        try:
            logger.info("Sequential processing başlatılıyor")
            
            results = {
                'status': 'success',
                'processed_files': 0,
                'total_records': 0,
                'failed_files': 0,
                'file_results': []
            }
            
            for file_path, file_size in input_files:
                try:
                    file_result = self._process_single_file_optimized(file_path, file_size)
                    results['file_results'].append(file_result)
                    
                    if file_result['status'] == 'success':
                        results['processed_files'] += 1
                        results['total_records'] += file_result['record_count']
                        
                        with self.metrics_lock:
                            self.current_metrics.processed_files += 1
                            self.current_metrics.processed_records += file_result['record_count']
                    else:
                        results['failed_files'] += 1
                        
                        with self.metrics_lock:
                            self.current_metrics.failed_files += 1
                            
                except Exception as e:
                    logger.error(f"Sequential file processing hatası {file_path}: {e}")
                    results['failed_files'] += 1
                    
                    with self.metrics_lock:
                        self.current_metrics.failed_files += 1
                    
                    results['file_results'].append({
                        'file': str(file_path),
                        'status': 'error',
                        'error': str(e),
                        'record_count': 0
                    })
            
            # Genel durum
            if results['failed_files'] > 0:
                results['status'] = 'partial_success' if results['processed_files'] > 0 else 'failed'
            
            return results
            
        except Exception as e:
            logger.error(f"Sequential processing hatası: {e}")
            raise
    
    def _process_single_file_optimized(self, file_path: Path, file_size: int) -> Dict[str, Any]:
        """
        Tek dosyayı optimize edilmiş şekilde işle
        
        Args:
            file_path: İşlenecek dosya yolu
            file_size: Dosya boyutu
            
        Returns:
            Dict[str, Any]: Dosya işleme sonucu
        """
        try:
            logger.info(f"Optimized dosya işleme: {file_path} ({file_size} bytes)")
            
            # Checkpoint kontrolü
            checkpoint_file = Path(self.checkpoint_path) / f"{file_path.stem}.checkpoint"
            if checkpoint_file.exists():
                logger.info(f"Dosya zaten işlenmiş (checkpoint mevcut): {file_path}")
                with open(checkpoint_file, 'r') as f:
                    return json.load(f)
            
            start_time = datetime.now()
            
            # Dosyayı memory-efficient şekilde oku
            df = self._read_file_optimized(file_path, file_size)
            
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
            
            # Adaptive batch processing
            df_processed = self._process_dataframe_in_batches(df, record_count)
            
            # Çıktı dosya yolu
            output_file = Path(self.output_path) / f"{file_path.stem}_processed.json"
            
            # Memory-efficient write
            self._write_results_optimized(df_processed, output_file)
            
            # Qdrant'a batch write
            self._write_to_qdrant_optimized(df_processed)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Checkpoint oluştur
            result = {
                'file': str(file_path),
                'status': 'success',
                'record_count': record_count,
                'processing_time': processing_time,
                'output_file': str(output_file),
                'timestamp': datetime.now().isoformat(),
                'batch_size_used': self.current_batch_size
            }
            
            with open(checkpoint_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            # Başarılı dosyayı arşivle
            self._archive_processed_file(file_path)
            
            logger.info(f"✅ Optimized dosya işleme tamamlandı: {file_path} ({processing_time:.2f}s)")
            return result
            
        except Exception as e:
            logger.error(f"Optimized dosya işleme hatası {file_path}: {e}")
            
            # Başarısız dosyayı failed dizinine taşı
            self._move_failed_file(file_path, str(e))
            
            return {
                'file': str(file_path),
                'status': 'error',
                'error': str(e),
                'record_count': 0,
                'timestamp': datetime.now().isoformat()
            }
    
    def _read_file_optimized(self, file_path: Path, file_size: int) -> DataFrame:
        """
        Dosyayı memory-efficient şekilde oku
        
        Args:
            file_path: Dosya yolu
            file_size: Dosya boyutu
            
        Returns:
            DataFrame: Okunan DataFrame
        """
        try:
            spark = self.embedding_job.spark
            
            # Büyük dosyalar için partition sayısını ayarla
            if file_size > 100 * 1024 * 1024:  # 100MB
                num_partitions = min(8, max(2, file_size // (50 * 1024 * 1024)))
                df = spark.read.option("multiline", "true").json(str(file_path))
                df = df.repartition(num_partitions)
            else:
                df = spark.read.json(str(file_path))
            
            # Caching stratejisi
            if self.batch_config.enable_caching and file_size < 500 * 1024 * 1024:  # 500MB
                df = df.cache()
            
            return df
            
        except Exception as e:
            logger.error(f"Dosya okuma hatası {file_path}: {e}")
            raise
    
    def _process_dataframe_in_batches(self, df: DataFrame, total_records: int) -> DataFrame:
        """
        DataFrame'i batch'ler halinde işle
        
        Args:
            df: İşlenecek DataFrame
            total_records: Toplam kayıt sayısı
            
        Returns:
            DataFrame: İşlenmiş DataFrame
        """
        try:
            logger.debug(f"Batch processing: {total_records} kayıt, batch_size: {self.current_batch_size}")
            
            # Veri kalitesi kontrolü
            df_validated = self._validate_data_optimized(df)
            
            # Embedding'leri batch'ler halinde oluştur
            if total_records > self.current_batch_size:
                # Büyük dataset için batch processing
                df_processed = self._process_large_dataset(df_validated, total_records)
            else:
                # Küçük dataset için direct processing
                df_processed = self.embedding_job.process_dataframe(df_validated)
            
            return df_processed
            
        except Exception as e:
            logger.error(f"Batch DataFrame işleme hatası: {e}")
            raise
    
    def _process_large_dataset(self, df: DataFrame, total_records: int) -> DataFrame:
        """
        Büyük dataset'i batch'ler halinde işle
        
        Args:
            df: İşlenecek DataFrame
            total_records: Toplam kayıt sayısı
            
        Returns:
            DataFrame: İşlenmiş DataFrame
        """
        try:
            logger.info(f"Large dataset processing: {total_records} kayıt")
            
            # DataFrame'i batch'lere böl
            num_batches = (total_records + self.current_batch_size - 1) // self.current_batch_size
            logger.info(f"Batch sayısı: {num_batches}")
            
            processed_dfs = []
            
            for i in range(num_batches):
                start_idx = i * self.current_batch_size
                end_idx = min((i + 1) * self.current_batch_size, total_records)
                
                logger.debug(f"Batch {i+1}/{num_batches} işleniyor: {start_idx}-{end_idx}")
                
                # Batch DataFrame oluştur
                batch_df = df.limit(end_idx).offset(start_idx) if hasattr(df, 'offset') else df
                
                # Memory kontrolü
                if i % self.batch_config.memory_check_interval == 0:
                    memory_percent = psutil.virtual_memory().percent
                    if memory_percent > self.batch_config.max_memory_usage_percent:
                        logger.warning(f"High memory usage: {memory_percent:.1f}%, pausing...")
                        time.sleep(2)
                
                # Batch'i işle
                batch_processed = self.embedding_job.process_dataframe(batch_df)
                processed_dfs.append(batch_processed)
                
                # Incremental checkpoint
                if self.batch_config.enable_incremental_checkpoints and i % 10 == 0:
                    logger.debug(f"Incremental checkpoint: batch {i+1}")
            
            # Tüm batch'leri birleştir
            if len(processed_dfs) == 1:
                return processed_dfs[0]
            else:
                return processed_dfs[0].union(*processed_dfs[1:])
            
        except Exception as e:
            logger.error(f"Large dataset processing hatası: {e}")
            raise
    
    def _validate_data_optimized(self, df: DataFrame) -> DataFrame:
        """
        Optimize edilmiş veri kalitesi kontrolü
        
        Args:
            df: Kontrol edilecek DataFrame
            
        Returns:
            DataFrame: Temizlenmiş DataFrame
        """
        try:
            logger.debug("Optimized veri kalitesi kontrolü...")
            
            # Gerekli sütunları kontrol et
            required_columns = ['content']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Gerekli sütunlar eksik: {missing_columns}")
            
            # Efficient filtering
            df_clean = df.filter(
                col('content').isNotNull() & 
                (col('content') != '') &
                (col('content') != ' ') &
                (length(col('content')) > 10)  # Minimum content length
            )
            
            # ID sütunu yoksa oluştur
            if 'id' not in df.columns:
                df_clean = df_clean.withColumn('id', 
                    concat(lit('doc_'), monotonically_increasing_id().cast('string'))
                )
            
            # Timestamp sütunu yoksa oluştur
            if 'timestamp' not in df.columns:
                df_clean = df_clean.withColumn('timestamp', current_timestamp())
            
            # Content length optimization
            max_content_length = 32768
            df_clean = df_clean.withColumn(
                'content',
                when(length(col('content')) > max_content_length,
                     substring(col('content'), 1, max_content_length)
                ).otherwise(col('content'))
            )
            
            # Duplicate removal (optional)
            if df_clean.count() > 1000:  # Only for larger datasets
                df_clean = df_clean.dropDuplicates(['content'])
            
            original_count = df.count()
            clean_count = df_clean.count()
            
            logger.info(f"Optimized veri kalitesi: {original_count} -> {clean_count} kayıt")
            
            return df_clean
            
        except Exception as e:
            logger.error(f"Optimized veri kalitesi kontrolü hatası: {e}")
            raise EmbeddingProcessingError(f"Optimized data validation failed: {e}")
    
    def _write_results_optimized(self, df: DataFrame, output_file: Path):
        """
        Sonuçları optimize edilmiş şekilde yaz
        
        Args:
            df: Yazılacak DataFrame
            output_file: Çıktı dosya yolu
        """
        try:
            # Compression ve partitioning
            writer = df.write.mode("overwrite")
            
            if self.batch_config.enable_compression:
                writer = writer.option("compression", self.batch_config.compression_codec)
            
            # Büyük dataset'ler için partitioning
            record_count = df.count()
            if record_count > 10000:
                writer = writer.option("maxRecordsPerFile", str(self.current_batch_size))
            
            writer.json(str(output_file))
            
            logger.debug(f"Results yazıldı: {output_file}")
            
        except Exception as e:
            logger.error(f"Results yazma hatası {output_file}: {e}")
            raise
    
    def _write_to_qdrant_optimized(self, df: DataFrame):
        """
        Qdrant'a optimize edilmiş şekilde yaz
        
        Args:
            df: Yazılacak DataFrame
        """
        try:
            # Batch write to Qdrant
            self.embedding_job.write_to_qdrant(df, self.qdrant_config)
            logger.debug("Qdrant write tamamlandı")
            
        except Exception as e:
            logger.error(f"Qdrant write hatası: {e}")
            raise
    
    def _save_metrics(self):
        """
        Metrics'leri dosyaya kaydet
        """
        try:
            if not self.current_metrics or not self.batch_config.enable_metrics:
                return
            
            metrics_file = Path(self.metrics_path) / f"batch_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            metrics_data = {
                'start_time': self.current_metrics.start_time.isoformat(),
                'end_time': self.current_metrics.end_time.isoformat() if self.current_metrics.end_time else None,
                'total_files': self.current_metrics.total_files,
                'processed_files': self.current_metrics.processed_files,
                'failed_files': self.current_metrics.failed_files,
                'total_records': self.current_metrics.total_records,
                'processed_records': self.current_metrics.processed_records,
                'failed_records': self.current_metrics.failed_records,
                'avg_processing_time_per_file': self.current_metrics.avg_processing_time_per_file,
                'avg_processing_time_per_record': self.current_metrics.avg_processing_time_per_record,
                'memory_usage_peak': self.current_metrics.memory_usage_peak,
                'cpu_usage_avg': self.current_metrics.cpu_usage_avg,
                'throughput_records_per_second': self.current_metrics.throughput_records_per_second,
                'batch_config': {
                    'current_batch_size': self.current_batch_size,
                    'max_parallel_files': self.batch_config.max_parallel_files,
                    'enable_caching': self.batch_config.enable_caching,
                    'enable_compression': self.batch_config.enable_compression
                }
            }
            
            with open(metrics_file, 'w') as f:
                json.dump(metrics_data, f, indent=2)
            
            logger.debug(f"Metrics kaydedildi: {metrics_file}")
            
        except Exception as e:
            logger.warning(f"Metrics kaydetme hatası: {e}")
    
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
                    'timestamp': datetime.now().isoformat(),
                    'batch_size_used': self.current_batch_size
                }, f, indent=2)
            
            logger.warning(f"Başarısız dosya taşındı: {file_path} -> {failed_path}")
            
        except Exception as e:
            logger.error(f"Başarısız dosya taşıma hatası {file_path}: {e}")
    
    def get_performance_report(self) -> Dict[str, Any]:
        """
        Detaylı performance raporu getir
        
        Returns:
            Dict[str, Any]: Performance raporu
        """
        try:
            # Son metrics dosyasını oku
            metrics_files = list(Path(self.metrics_path).glob('batch_metrics_*.json'))
            
            if not metrics_files:
                return {'status': 'no_metrics', 'message': 'Henüz metrics verisi yok'}
            
            # En son metrics dosyasını al
            latest_metrics_file = max(metrics_files, key=lambda x: x.stat().st_mtime)
            
            with open(latest_metrics_file, 'r') as f:
                latest_metrics = json.load(f)
            
            # Sistem durumu
            system_info = {
                'memory_usage': psutil.virtual_memory().percent,
                'cpu_usage': psutil.cpu_percent(interval=1),
                'disk_usage': psutil.disk_usage('/').percent,
                'current_batch_size': self.current_batch_size
            }
            
            # İstatistikler
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
            
            return {
                'status': 'success',
                'latest_metrics': latest_metrics,
                'system_info': system_info,
                'stats': stats,
                'batch_config': {
                    'min_batch_size': self.batch_config.min_batch_size,
                    'max_batch_size': self.batch_config.max_batch_size,
                    'current_batch_size': self.current_batch_size,
                    'adaptive_batch_size': self.batch_config.adaptive_batch_size,
                    'max_parallel_files': self.batch_config.max_parallel_files,
                    'enable_caching': self.batch_config.enable_caching,
                    'enable_compression': self.batch_config.enable_compression
                }
            }
            
        except Exception as e:
            logger.error(f"Performance raporu hatası: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def stop(self):
        """
        Optimized batch processor'ı durdur
        """
        try:
            logger.info("Optimized batch processor durduruluyor...")
            
            # Monitoring'i durdur
            self.stop_monitoring.set()
            
            # Final metrics kaydet
            if self.current_metrics:
                self.current_metrics.end_time = datetime.now()
                self._save_metrics()
            
            # Embedding job'ı durdur
            if self.embedding_job:
                self.embedding_job.stop()
            
            logger.info("✅ Optimized batch processor durduruldu")
            
        except Exception as e:
            logger.error(f"Optimized batch processor durdurma hatası: {e}")