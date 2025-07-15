#!/usr/bin/env python3

import os
import sys
import argparse
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.spark.embedding_job import SparkEmbeddingJob
from src.spark.batch_processor import SparkBatchProcessor
from src.spark.kafka_spark_connector import KafkaSparkConnector
from src.config.app_config import load_config
from src.exceptions.embedding_exceptions import EmbeddingProcessingError

class SparkCLI:
    
    def __init__(self):
        self.config = None
        self.embedding_job = None
        self.batch_processor = None
        self.kafka_connector = None
    
    def load_configuration(self, config_path: Optional[str] = None):
        try:
            if config_path:
                with open(config_path, 'r') as f:
                    self.config = json.load(f)
            else:
                self.config = load_config()
            
            logger.info("✅ Konfigürasyon yüklendi")
            
        except Exception as e:
            logger.error(f"Konfigürasyon yükleme hatası: {e}")
            raise
    
    def run_batch_job(self, 
                     input_path: str, 
                     output_path: str,
                     config_path: Optional[str] = None):
        try:
            logger.info(f"Batch job başlatılıyor: {input_path} -> {output_path}")
            
            # Konfigürasyonu yükle
            self.load_configuration(config_path)
            
            # Embedding job'ını oluştur
            self.embedding_job = SparkEmbeddingJob(self.config.get('spark', {}))
            
            # Job'ı çalıştır
            self.embedding_job.run_batch_job(
                input_path=input_path,
                output_path=output_path,
                qdrant_config=self.config.get('qdrant', {})
            )
            
            logger.info("✅ Batch job tamamlandı")
            
        except Exception as e:
            logger.error(f"Batch job hatası: {e}")
            raise
        finally:
            if self.embedding_job:
                self.embedding_job.stop()
    
    def run_batch_processor(self, 
                           config_path: Optional[str] = None,
                           file_pattern: str = "*.json",
                           cleanup_days: int = 7,
                           use_optimized: bool = False):
        try:
            processor_type = "Optimized" if use_optimized else "Standard"
            logger.info(f"{processor_type} batch processor başlatılıyor: {file_pattern}")
            
            # Konfigürasyonu yükle
            self.load_configuration(config_path)
            
            # Optimized processor kullanımını config'e ekle
            if use_optimized:
                self.config['use_optimized'] = True
            
            # Batch processor'ı oluştur
            self.batch_processor = SparkBatchProcessor(self.config)
            self.batch_processor.initialize()
            
            # Zamanlanmış batch'leri işle
            results = self.batch_processor.process_scheduled_batches()
            
            logger.info(f"Batch işleme sonuçları: {results}")
            
            # Eski dosyaları temizle
            if cleanup_days > 0:
                self.batch_processor.cleanup_old_files(cleanup_days)
            
            # İstatistikleri göster
            stats = self.batch_processor.get_processing_stats()
            logger.info(f"İşleme istatistikleri: {stats}")
            
            logger.info(f"✅ {processor_type} batch processor tamamlandı")
            
        except Exception as e:
            logger.error(f"Batch processor hatası: {e}")
            raise
        finally:
            if self.batch_processor:
                self.batch_processor.stop()
    
    def run_streaming(self, 
                     config_path: Optional[str] = None,
                     duration: Optional[int] = None):
        try:
            logger.info("Streaming pipeline başlatılıyor...")
            
            # Konfigürasyonu yükle
            self.load_configuration(config_path)
            
            # Kafka connector'ı oluştur
            self.kafka_connector = KafkaSparkConnector(self.config)
            self.kafka_connector.initialize()
            
            # Pipeline'ları başlat
            main_query = self.kafka_connector.start_streaming_pipeline()
            monitoring_query = self.kafka_connector.start_monitoring_pipeline()
            qdrant_query = self.kafka_connector.start_qdrant_sink_pipeline()
            
            logger.info("✅ Streaming pipeline'ları başlatıldı")
            
            # Belirtilen süre kadar çalıştır
            if duration:
                logger.info(f"Streaming {duration} saniye çalışacak...")
                time.sleep(duration)
            else:
                logger.info("Streaming süresiz çalışıyor... (Ctrl+C ile durdurun)")
                try:
                    main_query.awaitTermination()
                except KeyboardInterrupt:
                    logger.info("Kullanıcı tarafından durduruldu")
            
            logger.info("✅ Streaming tamamlandı")
            
        except Exception as e:
            logger.error(f"Streaming hatası: {e}")
            raise
        finally:
            if self.kafka_connector:
                self.kafka_connector.stop()
    
    def show_streaming_status(self, config_path: Optional[str] = None):
        try:
            logger.info("Streaming durumu kontrol ediliyor...")
            
            # Konfigürasyonu yükle
            self.load_configuration(config_path)
            
            # Kafka connector'ı oluştur
            self.kafka_connector = KafkaSparkConnector(self.config)
            self.kafka_connector.initialize()
            
            # Durum bilgisini al
            status = self.kafka_connector.get_streaming_status()
            
            print("\n=== STREAMING DURUMU ===")
            print(json.dumps(status, indent=2, ensure_ascii=False))
            
        except Exception as e:
            logger.error(f"Streaming durum kontrolü hatası: {e}")
            raise
        finally:
            if self.kafka_connector:
                self.kafka_connector.stop()
    
    def test_embedding(self, 
                      text: str,
                      config_path: Optional[str] = None):
        try:
            logger.info(f"Embedding test ediliyor: '{text[:50]}...'")
            
            # Konfigürasyonu yükle
            self.load_configuration(config_path)
            
            # Embedding job'ını oluştur
            self.embedding_job = SparkEmbeddingJob(self.config.get('spark', {}))
            self.embedding_job.initialize_spark()
            
            # Test DataFrame'i oluştur
            test_data = [{'id': 'test_1', 'content': text}]
            df = self.embedding_job.spark.createDataFrame(test_data)
            
            # Embedding'i oluştur
            result_df = self.embedding_job.process_dataframe(df)
            
            # Sonucu göster
            result = result_df.collect()[0]
            
            print("\n=== EMBEDDING TEST SONUCU ===")
            print(f"Metin: {result['content']}")
            print(f"Embedding boyutu: {len(result['embedding'])}")
            print(f"İlk 10 değer: {result['embedding'][:10]}")
            
            logger.info("✅ Embedding test tamamlandı")
            
        except Exception as e:
            logger.error(f"Embedding test hatası: {e}")
            raise
        finally:
            if self.embedding_job:
                self.embedding_job.stop()
    
    def create_sample_data(self, 
                          output_path: str,
                          count: int = 100):
        try:
            logger.info(f"Örnek veri oluşturuluyor: {count} kayıt -> {output_path}")
            
            import uuid
            from datetime import datetime
            
            sample_texts = [
                "Yapay zeka teknolojileri hızla gelişiyor.",
                "Machine learning algoritmaları veri analizi için kullanılır.",
                "Deep learning neural network tabanlı bir yaklaşımdır.",
                "Natural language processing metin verilerini işler.",
                "Computer vision görüntü tanıma sistemleridir.",
                "Big data büyük veri setlerinin analizidir.",
                "Cloud computing bulut tabanlı hesaplama hizmetleridir.",
                "DevOps geliştirme ve operasyon süreçlerini birleştirir.",
                "Microservices mimarisi uygulamaları küçük servislere böler.",
                "API'ler farklı sistemler arasında iletişim sağlar."
            ]
            
            sample_data = []
            for i in range(count):
                sample_data.append({
                    'id': str(uuid.uuid4()),
                    'content': sample_texts[i % len(sample_texts)] + f" (Kayıt #{i+1})",
                    'timestamp': datetime.now().isoformat(),
                    'metadata': json.dumps({
                        'source': 'sample_generator',
                        'index': i,
                        'category': 'technology'
                    }),
                    'source': 'cli_generator',
                    'priority': (i % 5) + 1
                })
            
            # Dosyaya yaz
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for item in sample_data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
            logger.info(f"✅ Örnek veri oluşturuldu: {output_path}")
            
        except Exception as e:
            logger.error(f"Örnek veri oluşturma hatası: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(
        description="NewMind AI Spark İşlemleri CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Örnekler:
  # Batch job çalıştır
  python spark_cli.py batch-job --input /data/input.json --output /data/output
  
  # Batch processor çalıştır
  python spark_cli.py batch-processor --pattern "*.json" --cleanup-days 7
  
  # Optimized batch processor çalıştır
  python spark_cli.py batch-processor --pattern "*.json" --optimized
  
  # Streaming başlat
  python spark_cli.py streaming --duration 300
  
  # Streaming durumunu kontrol et
  python spark_cli.py status
  
  # Embedding test et
  python spark_cli.py test-embedding --text "Bu bir test metnidir"
  
  # Örnek veri oluştur
  python spark_cli.py create-sample --output /data/sample.jsonl --count 1000
        """
    )
    
    # Global arguments
    parser.add_argument('--config', '-c', 
                       help='Konfigürasyon dosya yolu')
    parser.add_argument('--verbose', '-v', 
                       action='store_true',
                       help='Detaylı log çıktısı')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Komutlar')
    
    # Batch job command
    batch_parser = subparsers.add_parser('batch-job', help='Batch job çalıştır')
    batch_parser.add_argument('--input', '-i', required=True,
                             help='Girdi dosya yolu')
    batch_parser.add_argument('--output', '-o', required=True,
                             help='Çıktı dosya yolu')
    
    # Batch processor command
    processor_parser = subparsers.add_parser('batch-processor', help='Batch processor çalıştır')
    processor_parser.add_argument('--pattern', '-p', default='*.json',
                                 help='Dosya pattern (varsayılan: *.json)')
    processor_parser.add_argument('--cleanup-days', '-d', type=int, default=7,
                                 help='Temizlenecek eski dosyaların gün sayısı (varsayılan: 7)')
    processor_parser.add_argument('--optimized', action='store_true',
                                 help='Optimize edilmiş batch processor kullan')
    
    # Streaming command
    streaming_parser = subparsers.add_parser('streaming', help='Streaming pipeline başlat')
    streaming_parser.add_argument('--duration', '-d', type=int,
                                 help='Çalışma süresi (saniye, belirtilmezse süresiz)')
    
    # Status command
    subparsers.add_parser('status', help='Streaming durumunu göster')
    
    # Test embedding command
    test_parser = subparsers.add_parser('test-embedding', help='Embedding test et')
    test_parser.add_argument('--text', '-t', required=True,
                            help='Test metni')
    
    # Create sample command
    sample_parser = subparsers.add_parser('create-sample', help='Örnek veri oluştur')
    sample_parser.add_argument('--output', '-o', required=True,
                              help='Çıktı dosya yolu')
    sample_parser.add_argument('--count', '-n', type=int, default=100,
                              help='Oluşturulacak kayıt sayısı (varsayılan: 100)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Configure logging
    if args.verbose:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.remove()
        logger.add(sys.stderr, level="INFO")
    
    # Create CLI instance
    cli = SparkCLI()
    
    try:
        # Execute command
        if args.command == 'batch-job':
            cli.run_batch_job(
                input_path=args.input,
                output_path=args.output,
                config_path=args.config
            )
        
        elif args.command == 'batch-processor':
            cli.run_batch_processor(
                config_path=args.config,
                file_pattern=args.pattern,
                cleanup_days=args.cleanup_days,
                use_optimized=args.optimized
            )
        
        elif args.command == 'streaming':
            cli.run_streaming(
                config_path=args.config,
                duration=args.duration
            )
        
        elif args.command == 'status':
            cli.show_streaming_status(config_path=args.config)
        
        elif args.command == 'test-embedding':
            cli.test_embedding(
                text=args.text,
                config_path=args.config
            )
        
        elif args.command == 'create-sample':
            cli.create_sample_data(
                output_path=args.output,
                count=args.count
            )
        
        else:
            parser.print_help()
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("İşlem kullanıcı tarafından durduruldu")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Hata: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()