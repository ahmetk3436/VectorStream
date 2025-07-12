"""Dead Letter Queue implementation for failed message processing."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class FailureReason(Enum):
    """Hata nedenleri enum'u"""
    PROCESSING_ERROR = "processing_error"
    TIMEOUT_ERROR = "timeout_error"
    VALIDATION_ERROR = "validation_error"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    UNKNOWN_ERROR = "unknown_error"


@dataclass
class DeadLetterMessage:
    """Dead letter queue mesaj yapısı"""
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str]
    original_value: str
    failure_reason: FailureReason
    error_message: str
    failed_at: str
    retry_count: int
    original_headers: Optional[Dict[str, str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Dict'e dönüştür"""
        data = asdict(self)
        data['failure_reason'] = self.failure_reason.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DeadLetterMessage':
        """Dict'ten oluştur"""
        data['failure_reason'] = FailureReason(data['failure_reason'])
        return cls(**data)


@dataclass
class DLQConfig:
    """Dead Letter Queue konfigürasyonu"""
    dlq_topic: str = "dead-letter-queue"
    max_retry_count: int = 3
    kafka_bootstrap_servers: str = "localhost:9092"
    producer_config: Optional[Dict[str, Any]] = None
    consumer_config: Optional[Dict[str, Any]] = None
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Producer konfigürasyonu döndür"""
        config = {
            'bootstrap_servers': self.kafka_bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'retries': 3,
            'retry_backoff_ms': 1000
        }
        if self.producer_config:
            config.update(self.producer_config)
        return config
    
    def get_consumer_config(self) -> Dict[str, Any]:
        """Consumer konfigürasyonu döndür"""
        config = {
            'bootstrap_servers': self.kafka_bootstrap_servers,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True
        }
        if self.consumer_config:
            config.update(self.consumer_config)
        return config


class DeadLetterQueue:
    """Dead Letter Queue yöneticisi"""
    
    def __init__(self, config: DLQConfig):
        self.config = config
        self._producer: Optional[KafkaProducer] = None
        self._consumer: Optional[KafkaConsumer] = None
        self._is_running = False
    
    def _get_producer(self) -> KafkaProducer:
        """Producer instance'ını al"""
        if self._producer is None:
            self._producer = KafkaProducer(**self.config.get_producer_config())
        return self._producer
    
    def _get_consumer(self) -> KafkaConsumer:
        """Consumer instance'ını al"""
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                self.config.dlq_topic,
                **self.config.get_consumer_config()
            )
        return self._consumer
    
    async def send_to_dlq(
        self,
        original_topic: str,
        original_partition: int,
        original_offset: int,
        original_key: Optional[str],
        original_value: str,
        failure_reason: FailureReason,
        error_message: str,
        retry_count: int = 0,
        original_headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """Mesajı DLQ'ya gönder"""
        try:
            dlq_message = DeadLetterMessage(
                original_topic=original_topic,
                original_partition=original_partition,
                original_offset=original_offset,
                original_key=original_key,
                original_value=original_value,
                failure_reason=failure_reason,
                error_message=error_message,
                failed_at=datetime.now(timezone.utc).isoformat(),
                retry_count=retry_count,
                original_headers=original_headers
            )
            
            producer = self._get_producer()
            
            # Async olarak gönder
            future = producer.send(
                self.config.dlq_topic,
                key=original_key,
                value=dlq_message.to_dict()
            )
            
            # Future'ı bekle
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Mesaj DLQ'ya gönderildi: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"DLQ'ya mesaj gönderme hatası: {e}")
            return False
        except Exception as e:
            logger.error(f"DLQ'ya mesaj gönderme beklenmeyen hatası: {e}")
            return False
    
    def get_dlq_messages(self, timeout_ms: int = 1000) -> List[DeadLetterMessage]:
        """DLQ'dan mesajları al"""
        messages = []
        try:
            consumer = self._get_consumer()
            
            # Mesajları poll et
            message_batch = consumer.poll(timeout_ms=timeout_ms)
            
            for topic_partition, records in message_batch.items():
                for record in records:
                    try:
                        dlq_message = DeadLetterMessage.from_dict(record.value)
                        messages.append(dlq_message)
                    except Exception as e:
                        logger.error(f"DLQ mesajı parse hatası: {e}")
                        continue
            
            return messages
            
        except Exception as e:
            logger.error(f"DLQ mesajları alma hatası: {e}")
            return []
    
    async def replay_message(
        self,
        dlq_message: DeadLetterMessage,
        target_topic: Optional[str] = None
    ) -> bool:
        """DLQ mesajını tekrar oynat"""
        try:
            topic = target_topic or dlq_message.original_topic
            
            producer = self._get_producer()
            
            # Replay headers'ı ekle
            headers = dlq_message.original_headers or {}
            headers.update({
                'dlq_replay': 'true',
                'dlq_replay_at': datetime.now(timezone.utc).isoformat(),
                'original_failure_reason': dlq_message.failure_reason.value
            })
            
            future = producer.send(
                topic,
                key=dlq_message.original_key,
                value=dlq_message.original_value.encode('utf-8'),
                headers=[(k, v.encode('utf-8')) for k, v in headers.items()]
            )
            
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"DLQ mesajı tekrar oynatıldı: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"DLQ mesajı replay hatası: {e}")
            return False
    
    def get_dlq_stats(self) -> Dict[str, Any]:
        """DLQ istatistiklerini al"""
        try:
            consumer = self._get_consumer()
            
            # Topic partitions'ları al
            partitions = consumer.partitions_for_topic(self.config.dlq_topic)
            if not partitions:
                return {'total_messages': 0, 'partitions': {}}
            
            stats = {
                'total_messages': 0,
                'partitions': {},
                'failure_reasons': {}
            }
            
            # Her partition için istatistik topla
            for partition in partitions:
                tp = consumer.assignment()
                if tp:
                    high_water_mark = consumer.highwater(list(tp)[0])
                    low_water_mark = consumer.position(list(tp)[0])
                    
                    partition_stats = {
                        'high_water_mark': high_water_mark,
                        'low_water_mark': low_water_mark,
                        'lag': high_water_mark - low_water_mark
                    }
                    
                    stats['partitions'][partition] = partition_stats
                    stats['total_messages'] += partition_stats['lag']
            
            return stats
            
        except Exception as e:
            logger.error(f"DLQ istatistikleri alma hatası: {e}")
            return {'error': str(e)}
    
    def close(self):
        """Kaynakları temizle"""
        try:
            if self._producer:
                self._producer.close()
                self._producer = None
            
            if self._consumer:
                self._consumer.close()
                self._consumer = None
                
            logger.info("DLQ kaynakları temizlendi")
            
        except Exception as e:
            logger.error(f"DLQ kaynakları temizleme hatası: {e}")


# Global DLQ instance
_dlq_instance: Optional[DeadLetterQueue] = None


def get_dlq(config: Optional[DLQConfig] = None) -> DeadLetterQueue:
    """Global DLQ instance'ını al"""
    global _dlq_instance
    
    if _dlq_instance is None:
        if config is None:
            config = DLQConfig()
        _dlq_instance = DeadLetterQueue(config)
    
    return _dlq_instance


def reset_dlq():
    """Global DLQ instance'ını sıfırla (test amaçlı)"""
    global _dlq_instance
    if _dlq_instance:
        _dlq_instance.close()
    _dlq_instance = None