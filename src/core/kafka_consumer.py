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
from utils.circuit_breaker import CircuitBreakerConfig, circuit_breaker_manager, CircuitBreakerError

class KafkaConsumer:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer: Optional[PyKafkaConsumer] = None
        self.message_handler: Optional[Callable] = None
        self.running = False
        
        # Circuit breaker setup
        self.circuit_breaker = circuit_breaker_manager.create_circuit_breaker(
            name="kafka_consumer",
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=30.0,
                timeout=10.0
            )
        )
        
    def set_message_handler(self, handler: Callable[[Dict[str, Any]], None]):
        """Mesaj işleyici fonksiyonu ayarla"""
        self.message_handler = handler
        
    async def start_consuming(self):
        """Kafka mesajlarını tüketmeye başla"""
        try:
            await self._initialize_consumer()
            
            self.running = True
            logger.info(f"Kafka consumer başlatıldı. Topic: {self.config.topic}")
            
            try:
                for message in self.consumer:
                    if not self.running:
                        break
                        
                    try:
                        # Circuit breaker ile mesaj işleme
                        await self.circuit_breaker.call(self._process_message, message)
                            
                    except CircuitBreakerError as e:
                        logger.error(f"Circuit breaker hatası: {e}")
                        # Circuit breaker açıksa kısa süre bekle
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"Mesaj işleme hatası: {e}")
            except StopIteration:
                # Consumer timeout - normal durum
                logger.info("Consumer timeout - mesaj bekleme süresi doldu")
                    
        except Exception as e:
            logger.error(f"Kafka consumer hatası: {e}")
    
    async def _initialize_consumer(self):
        """Consumer'ı başlat"""
        # Test ortamında timeout ekle
        consumer_config = self.config.to_consumer_config()
        consumer_config['consumer_timeout_ms'] = 1000  # 1 saniye timeout
        
        self.consumer = PyKafkaConsumer(
            self.config.topic,
            **consumer_config
        )
    
    async def _process_message(self, message):
        """Tek mesajı işle"""
        # Mesajı JSON olarak parse et
        data = json.loads(message.value)
        logger.info(f"Mesaj alındı: {data}")
        
        # Mesaj işleyiciye gönder
        if self.message_handler:
            await self.message_handler(data)
            
    async def close(self):
        """Consumer'ı kapat"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer kapatıldı")