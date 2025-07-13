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

from src.config.kafka_config import KafkaConfig
from src.utils.circuit_breaker import CircuitBreakerConfig, circuit_breaker_manager, CircuitBreakerError
from src.utils.dead_letter_queue import get_dlq, FailureReason
from src.utils.error_handler import (
    retry_with_policy, RetryPolicy, BackoffStrategy,
    NETWORK_RETRY_POLICY, retry_network_errors
)

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
        
        # DLQ instance'ını al
        self.dlq = get_dlq()
        
    def set_message_handler(self, handler: Callable[[Dict[str, Any]], None]):
        """Mesaj işleyici fonksiyonu ayarla"""
        self.message_handler = handler
        
    async def start_consuming(self):
        """Kafka mesajlarını tüketmeye başla"""
        try:
            await self._initialize_consumer()
            
            self.running = True
            logger.info(f"Kafka consumer başlatıldı. Topic: {self.config.topic}")
            
            while self.running:
                try:
                    # Polling ile mesajları al
                    message_pack = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_pack:
                        # Mesaj yoksa kısa süre bekle
                        await asyncio.sleep(0.1)
                        continue
                    
                    # Tüm topic partition'larındaki mesajları işle
                    for topic_partition, messages in message_pack.items():
                        for message in messages:
                            if not self.running:
                                break
                                
                            try:
                                # Circuit breaker ile mesaj işleme
                                await self.circuit_breaker.call(self._process_message, message)
                                    
                            except CircuitBreakerError as e:
                                logger.error(f"Circuit breaker hatası: {e}")
                                # Circuit breaker açık olduğunda DLQ'ya gönder
                                await self.dlq.send_to_dlq(
                                    original_topic=message.topic,
                                    original_partition=message.partition,
                                    original_offset=message.offset,
                                    original_key=message.key.decode('utf-8') if message.key else None,
                                    original_value=message.value.decode('utf-8'),
                                    failure_reason=FailureReason.CIRCUIT_BREAKER_OPEN,
                                    error_message=str(e),
                                    retry_count=0
                                )
                                # Circuit breaker açıksa kısa süre bekle
                                await asyncio.sleep(1)
                            except Exception as e:
                                logger.error(f"Mesaj işleme hatası: {e}")
                                
                        if not self.running:
                            break
                            
                except Exception as e:
                    logger.error(f"Polling hatası: {e}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Kafka consumer hatası: {e}")
            raise
    
    async def _initialize_consumer(self):
        """Consumer'ı başlat"""
        # Consumer config'ini al
        consumer_config = self.config.to_consumer_config()
        # Timeout'u kaldır, sürekli dinle
        consumer_config.pop('consumer_timeout_ms', None)
        
        self.consumer = PyKafkaConsumer(
            self.config.topic,
            **consumer_config
        )
    
    async def _process_message(self, message):
        """Tek mesajı işle"""
        retry_count = 0
        
        # Retry policy for message processing
        processing_policy = RetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            retryable_exceptions=[ConnectionError, TimeoutError, OSError],
            non_retryable_exceptions=[json.JSONDecodeError, ValueError, TypeError]
        )
        
        @retry_with_policy(processing_policy)
        async def _process_with_retry():
            nonlocal retry_count
            retry_count += 1
            
            # Mesajı JSON olarak parse et
            try:
                data = json.loads(message.value)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse hatası: {e}")
                # DLQ'ya gönder - JSON parse hatası retry edilmez
                await self.dlq.send_to_dlq(
                    original_topic=message.topic,
                    original_partition=message.partition,
                    original_offset=message.offset,
                    original_key=message.key.decode('utf-8') if message.key else None,
                    original_value=message.value.decode('utf-8'),
                    failure_reason=FailureReason.VALIDATION_ERROR,
                    error_message=f"JSON parse hatası: {str(e)}",
                    retry_count=retry_count
                )
                raise  # Non-retryable exception
            
            logger.info(f"Mesaj alındı (attempt {retry_count}): {data}")
            
            # Mesaj işleyiciye gönder
            if self.message_handler:
                # Handler async değilse sync olarak çalıştır
                if asyncio.iscoroutinefunction(self.message_handler):
                    await self.message_handler(data)
                else:
                    # Sync handler'ı thread pool'da çalıştır
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.message_handler, data)
        
        try:
            await _process_with_retry()
            logger.debug(f"Mesaj başarıyla işlendi (toplam {retry_count} deneme)")
            
        except json.JSONDecodeError:
            # JSON parse hatası - zaten DLQ'ya gönderildi
            pass
        except Exception as e:
            logger.error(f"Mesaj işleme hatası (toplam {retry_count} deneme): {e}")
            # DLQ'ya gönder
            await self.dlq.send_to_dlq(
                original_topic=message.topic,
                original_partition=message.partition,
                original_offset=message.offset,
                original_key=message.key.decode('utf-8') if message.key else None,
                original_value=message.value.decode('utf-8'),
                failure_reason=FailureReason.PROCESSING_ERROR,
                error_message=str(e),
                retry_count=retry_count
            )
            
    async def close(self):
        """Consumer'ı kapat"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer kapatıldı")