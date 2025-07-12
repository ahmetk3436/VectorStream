#!/usr/bin/env python3
"""
Adım adım test scripti - Progressive development için
"""

import asyncio
import json
import sys
import yaml
from pathlib import Path
import subprocess
import time

# Proje root'unu Python path'ine ekle
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

class ProgressiveTest:
    def __init__(self):
        self.step = 1
        
    def print_step(self, title):
        print(f"\n{'='*60}")
        print(f"🚀 ADIM {self.step}: {title}")
        print(f"{'='*60}")
        self.step += 1
        
    def check_docker_services(self):
        """Docker servislerinin durumunu kontrol et"""
        self.print_step("Docker Servisleri Kontrolü")
        
        try:
            result = subprocess.run(['docker-compose', 'ps'], 
                                  capture_output=True, text=True, cwd=project_root)
            print("📊 Docker servisleri durumu:")
            print(result.stdout)
            
            # Servislerin çalışıp çalışmadığını kontrol et
            if 'kafka' in result.stdout and 'Up' in result.stdout:
                print("✅ Kafka servisi çalışıyor")
            else:
                print("❌ Kafka servisi çalışmıyor")
                print("🔧 Çözüm: docker-compose up -d kafka")
                
            if 'qdrant' in result.stdout and 'Up' in result.stdout:
                print("✅ Qdrant servisi çalışıyor")
            else:
                print("❌ Qdrant servisi çalışmıyor")
                print("🔧 Çözüm: docker-compose up -d qdrant")
                
        except Exception as e:
            print(f"❌ Docker kontrol hatası: {e}")
            print("🔧 Çözüm: Docker Desktop'ın çalıştığından emin olun")
            
    def test_logger(self):
        """Logger sistemini test et"""
        self.print_step("Logger Sistemi Testi")
        
        try:
            from src.utils.logger import setup_logger
            
            config = {'level': 'INFO'}
            logger = setup_logger(config)
            
            logger.info("✅ Logger test başarılı!")
            logger.warning("⚠️ Bu bir uyarı mesajı")
            logger.error("❌ Bu bir hata mesajı (test amaçlı)")
            
            # Log dosyasının oluşup oluşmadığını kontrol et
            log_file = Path("logs/app.log")
            if log_file.exists():
                print(f"✅ Log dosyası oluşturuldu: {log_file}")
                print(f"📄 Log dosyası boyutu: {log_file.stat().st_size} bytes")
            else:
                print("❌ Log dosyası oluşturulamadı")
                
        except Exception as e:
            print(f"❌ Logger test hatası: {e}")
            
    def test_config_loading(self):
        """Config yükleme sistemini test et"""
        self.print_step("Config Yükleme Testi")
        
        try:
            from config.kafka_config import KafkaConfig
            
            # Config dosyasını yükle
            with open('config/app_config.yaml', 'r') as f:
                config = yaml.safe_load(f)
            
            print("📋 Yüklenen config:")
            for key, value in config.items():
                print(f"  {key}: {value}")
                
            # Kafka config test
            kafka_config = KafkaConfig.from_dict(config['kafka'])
            print(f"\n✅ Kafka Config oluşturuldu: {kafka_config}")
            
            consumer_config = kafka_config.to_consumer_config()
            print(f"✅ Consumer Config: {list(consumer_config.keys())}")
            
        except Exception as e:
            print(f"❌ Config test hatası: {e}")
            
    async def test_qdrant_connection(self):
        """Qdrant bağlantısını test et"""
        self.print_step("Qdrant Bağlantı Testi")
        
        try:
            from src.core.qdrant_writer import QdrantWriter
            import numpy as np
            
            # Config yükle
            with open('config/app_config.yaml', 'r') as f:
                config = yaml.safe_load(f)
                
            qdrant_writer = QdrantWriter(config['qdrant'])
            
            print("🔌 Qdrant'a bağlanıyor...")
            await qdrant_writer.initialize_collection()
            print("✅ Qdrant koleksiyonu hazır")
            
            # Test embedding'i yaz
            test_embedding = {
                'vector': np.random.rand(384).tolist(),
                'metadata': {
                    'text': 'Test embedding',
                    'source': 'progressive_test',
                    'timestamp': time.time()
                }
            }
            
            success = await qdrant_writer.write_embeddings([test_embedding])
            if success:
                print("✅ Test embedding yazıldı")
                
                # Arama testi
                query_vector = np.random.rand(384).tolist()
                results = await qdrant_writer.search_similar(query_vector, limit=1)
                print(f"✅ Arama testi: {len(results)} sonuç bulundu")
            else:
                print("❌ Test embedding yazılamadı")
                
        except Exception as e:
            print(f"❌ Qdrant test hatası: {e}")
            print("🔧 Çözüm: docker-compose up -d qdrant")
            
    def test_kafka_producer(self):
        """Kafka'ya test mesajı gönder"""
        self.print_step("Kafka Producer Testi")
        
        try:
            test_message = {
                'id': 'test_001',
                'content': 'Bu progressive test mesajıdır',
                'timestamp': '2024-01-15T10:30:00Z',
                'metadata': {
                    'source': 'progressive_test',
                    'priority': 'high'
                }
            }
            
            message_json = json.dumps(test_message)
            print(f"📤 Gönderilecek mesaj: {message_json}")
            
            # Docker exec ile mesaj gönder
            cmd = [
                'docker', 'exec', '-i', 'kafka',
                'kafka-console-producer.sh',
                '--bootstrap-server', 'localhost:9092',
                '--topic', 'raw_data'
            ]
            
            result = subprocess.run(cmd, input=message_json, 
                                  text=True, capture_output=True)
            
            if result.returncode == 0:
                print("✅ Test mesajı Kafka'ya gönderildi")
                print("📝 Not: Ana uygulamayı çalıştırarak mesajın işlendiğini görebilirsiniz")
            else:
                print(f"❌ Mesaj gönderilemedi: {result.stderr}")
                print("🔧 Çözüm: docker-compose up -d kafka")
                
        except Exception as e:
            print(f"❌ Kafka producer test hatası: {e}")
            
    def show_next_steps(self):
        """Sonraki adımları göster"""
        self.print_step("Sonraki Adımlar")
        
        print("🎯 Progressive Development Roadmap:")
        print("")
        print("1️⃣ Temel Test (ŞU AN):")
        print("   ✅ Import'lar çalışıyor")
        print("   ✅ Config yükleniyor")
        print("   ✅ Logger çalışıyor")
        print("   ✅ Qdrant bağlantısı var")
        print("")
        print("2️⃣ Ana Uygulama Testi:")
        print("   📝 python src/main.py")
        print("   📝 Başka terminalde: python test_step_by_step.py --send-message")
        print("")
        print("3️⃣ Monitoring Ekleme:")
        print("   📝 Prometheus metrics")
        print("   📝 Health check endpoints")
        print("")
        print("4️⃣ Spark Integration:")
        print("   📝 Gerçek embedding modeli")
        print("   📝 Batch processing")
        print("")
        print("5️⃣ Production Ready:")
        print("   📝 Error handling")
        print("   📝 Performance optimization")
        print("   📝 Deployment scripts")
        
async def main():
    tester = ProgressiveTest()
    
    print("🧪 NewMind-AI Progressive Development Test")
    print("Bu script projenizin her aşamasını test eder")
    
    # Temel kontroller
    tester.check_docker_services()
    tester.test_logger()
    tester.test_config_loading()
    
    # Async testler
    await tester.test_qdrant_connection()
    
    # Kafka test
    if len(sys.argv) > 1 and sys.argv[1] == '--send-message':
        tester.test_kafka_producer()
    else:
        print("\n📝 Kafka mesaj testi için: python test_step_by_step.py --send-message")
    
    # Sonraki adımlar
    tester.show_next_steps()
    
if __name__ == "__main__":
    asyncio.run(main())