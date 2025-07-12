#!/usr/bin/env python3
"""
Import testleri - tüm modüllerin doğru import edilip edilmediğini kontrol eder
"""

import sys
from pathlib import Path

# Proje root'unu Python path'ine ekle
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

def test_imports():
    """Tüm import'ları test et"""
    print("🧪 Import testleri başlatılıyor...")
    
    try:
        # Config imports
        print("📁 Config modülleri test ediliyor...")
        from config.kafka_config import KafkaConfig
        print("✅ config.kafka_config import edildi")
        
        # Utils imports
        print("🔧 Utils modülleri test ediliyor...")
        from src.utils.logger import setup_logger
        print("✅ src.utils.logger import edildi")
        
        # Core imports
        print("⚙️ Core modülleri test ediliyor...")
        from src.core.kafka_consumer import KafkaConsumer
        print("✅ src.core.kafka_consumer import edildi")
        
        from src.core.qdrant_writer import QdrantWriter
        print("✅ src.core.qdrant_writer import edildi")
        
        # Main import
        print("🚀 Main modül test ediliyor...")
        from src.main import NewMindAI
        print("✅ src.main import edildi")
        
        print("\n🎉 Tüm import testleri başarılı!")
        return True
        
    except ImportError as e:
        print(f"❌ Import hatası: {e}")
        return False
    except Exception as e:
        print(f"❌ Beklenmeyen hata: {e}")
        return False

def test_config_loading():
    """Config yükleme testini yap"""
    print("\n📋 Config yükleme testi...")
    
    try:
        import yaml
        from config.kafka_config import KafkaConfig
        
        # Config dosyasını yükle
        with open('config/app_config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        
        # Kafka config test
        kafka_config = KafkaConfig.from_dict(config['kafka'])
        print(f"✅ Kafka Config: {kafka_config}")
        
        # Consumer config test
        consumer_config = kafka_config.to_consumer_config()
        print(f"✅ Consumer Config: {consumer_config}")
        
        return True
        
    except Exception as e:
        print(f"❌ Config test hatası: {e}")
        return False

if __name__ == "__main__":
    print("🔍 NewMind-AI Import ve Config Testleri")
    print("=" * 50)
    
    # Import testleri
    import_success = test_imports()
    
    # Config testleri
    config_success = test_config_loading()
    
    print("\n" + "=" * 50)
    if import_success and config_success:
        print("🎯 Tüm testler başarılı! Artık main.py çalıştırabilirsiniz.")
        print("\n📝 Sonraki adımlar:")
        print("1. docker-compose up -d  # Servisleri başlat")
        print("2. pip install -r requirements.txt  # Bağımlılıkları yükle")
        print("3. python src/main.py  # Uygulamayı çalıştır")
    else:
        print("❌ Bazı testler başarısız! Lütfen hataları düzeltin.")
        sys.exit(1)