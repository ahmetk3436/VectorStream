#!/usr/bin/env python3

import json
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

from kafka import KafkaProducer
from config.kafka_config import KafkaConfig
import yaml

def load_config():
    """Load configuration from YAML file"""
    config_path = project_root / "config" / "app_config.yaml"
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def send_test_message():
    """Send a test message to Kafka"""
    try:
        # Load configuration
        config = load_config()
        kafka_config = KafkaConfig.from_dict(config['kafka'])
        
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=kafka_config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Test message
        test_message = {
            "id": "test_001",
            "content": "Bu progressive test mesajıdır",
            "timestamp": "2024-01-15T10:30:00Z",
            "metadata": {
                "source": "progressive_test",
                "priority": "high"
            }
        }
        
        # Send message
        future = producer.send(kafka_config.topic, test_message)
        result = future.get(timeout=10)
        
        print(f"✅ Mesaj başarıyla gönderildi!")
        print(f"   Topic: {kafka_config.topic}")
        print(f"   Partition: {result.partition}")
        print(f"   Offset: {result.offset}")
        print(f"   Mesaj: {json.dumps(test_message, indent=2, ensure_ascii=False)}")
        
        producer.close()
        
    except Exception as e:
        print(f"❌ Mesaj gönderme hatası: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("🚀 Kafka'ya test mesajı gönderiliyor...")
    success = send_test_message()
    if success:
        print("\n✅ Test başarılı! Şimdi main.py'yi çalıştırarak mesajın işlendiğini görebilirsiniz.")
        print("   Komut: python src/main.py")
    else:
        print("\n❌ Test başarısız!")