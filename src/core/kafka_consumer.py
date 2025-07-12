import asyncio
import json
import sys
from pathlib import Path
from kafka import KafkaConsumer as PyKafkaConsumer
from loguru import logger
from typing import Dict, Any, Callable, Optional

# Proje root'unu Python path'ine ekle
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.kafka_config import KafkaConfig

class KafkaConsumer:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer: Optional[PyKafkaConsumer] = None
        self.message_handler: Optional[Callable] = None
        self.running = False
        
    def set_message_handler(self, handler: Callable[[Dict[str, Any]], None]):
        """Mesaj işleyici fonksiyonu ayarla"""
        self.message_handler = handler
        
    async def start_consuming(self):
        """Kafka mesajlarını tüketmeye başla"""
        try:
            # Test ortamında timeout ekle
            consumer_config = self.config.to_consumer_config()
            consumer_config['consumer_timeout_ms'] = 1000  # 1 saniye timeout
            
            self.consumer = PyKafkaConsumer(
                self.config.topic,
                **consumer_config
            )
            
            self.running = True
            logger.info(f"Kafka consumer başlatıldı. Topic: {self.config.topic}")
            
            try:
                for message in self.consumer:
                    if not self.running:
                        break
                        
                    try:
                        # Mesajı JSON olarak parse et
                        data = json.loads(message.value)
                        logger.info(f"Mesaj alındı: {data}")
                        
                        # Mesaj işleyiciye gönder
                        if self.message_handler:
                            await self.message_handler(data)
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parse hatası: {e}")
                    except Exception as e:
                        logger.error(f"Mesaj işleme hatası: {e}")
            except StopIteration:
                # Consumer timeout - normal durum
                logger.info("Consumer timeout - mesaj bekleme süresi doldu")
                    
        except Exception as e:
            logger.error(f"Kafka consumer hatası: {e}")
            
    async def close(self):
        """Consumer'ı kapat"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer kapatıldı")