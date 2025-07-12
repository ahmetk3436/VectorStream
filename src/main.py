import asyncio
import yaml
import numpy as np
import sys
from pathlib import Path
from loguru import logger

# Proje root'unu Python path'ine ekle
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from src.core.qdrant_writer import QdrantWriter
from src.config.kafka_config import KafkaConfig
from src.utils.logger import setup_logger

class NewMindAI:
    def __init__(self, config_path: str = "config/app_config.yaml"):
        self.config = self.load_config(config_path)
        self.setup_components()
        
    def load_config(self, config_path: str):
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
            
    def setup_components(self):
        # Logger'ı ayarla
        setup_logger(self.config['logging'])
        
        # Kafka consumer
        kafka_config = KafkaConfig.from_dict(self.config['kafka'])
        self.kafka_consumer = KafkaConsumer(kafka_config)
        self.kafka_consumer.set_message_handler(self.process_message)
        
        # Qdrant writer
        self.qdrant_writer = QdrantWriter(self.config['qdrant'])
        
    async def process_message(self, data):
        """Kafka mesajını işle"""
        try:
            logger.info(f"Mesaj işleniyor: {data}")
            
            # Basit embedding oluştur (gerçek projede Spark kullanılacak)
            text = data.get('content', '')
            embedding_vector = self.create_simple_embedding(text)
            
            # Qdrant'a yaz
            embeddings = [{
                'vector': embedding_vector,
                'metadata': {
                    'text': text,
                    'message_id': data.get('id'),
                    'timestamp': data.get('timestamp'),
                    'source': data.get('metadata', {}).get('source', 'unknown')
                }
            }]
            
            success = await self.qdrant_writer.write_embeddings(embeddings)
            if success:
                logger.info(f"✅ Mesaj başarıyla işlendi: {data.get('id')}")
            else:
                logger.error(f"❌ Mesaj işlenemedi: {data.get('id')}")
                
        except Exception as e:
            logger.error(f"Mesaj işleme hatası: {e}")
            
    def create_simple_embedding(self, text: str) -> list:
        """Basit embedding oluştur (demo amaçlı)"""
        # Gerçek projede Spark + ML modeli kullanılacak
        np.random.seed(hash(text) % 2**32)
        return np.random.rand(384).tolist()
        
    async def start(self):
        """Uygulamayı başlat"""
        logger.info("🚀 NewMind-AI başlatılıyor...")
        
        # Qdrant koleksiyonunu başlat
        await self.qdrant_writer.initialize_collection()
        
        # Kafka consumer'ı başlat
        await self.kafka_consumer.start_consuming()
        
    async def stop(self):
        """Uygulamayı durdur"""
        logger.info("🛑 NewMind-AI durduruluyor...")
        await self.kafka_consumer.close()

async def main():
    app = NewMindAI()
    
    try:
        await app.start()
    except KeyboardInterrupt:
        logger.info("Kullanıcı tarafından durduruldu")
    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())