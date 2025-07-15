from dataclasses import dataclass
from typing import Dict, Any
import logging
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka.admin.new_topic import NewTopic
from kafka.errors import TopicAlreadyExistsError, InvalidPartitionsError
import asyncio

logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = "earliest"
    batch_size: int = 100
    max_poll_records: int = 500
    consumer_timeout_ms: int = 1000
    enable_auto_commit: bool = True
    partitions: int = 16
    replication_factor: int = 1
    auto_create_topic: bool = True
    auto_configure_partitions: bool = True
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        valid_params = {k: v for k, v in config_dict.items() 
                       if k in cls.__annotations__}
        return cls(**valid_params)
    
    async def ensure_topic_configuration(self):
        """Topic'i oluştur ve partition sayısını ayarla"""
        if not self.auto_create_topic and not self.auto_configure_partitions:
            return
            
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='vectorstream-admin'
            )
            
            metadata = admin_client.list_topics()
            topic_exists = self.topic in metadata
            
            if not topic_exists and self.auto_create_topic:
                topic_list = [NewTopic(
                    name=self.topic,
                    num_partitions=self.partitions,
                    replication_factor=self.replication_factor
                )]
                
                try:
                    admin_client.create_topics(new_topics=topic_list, validate_only=False)
                    logger.info(f"✅ Topic '{self.topic}' oluşturuldu: {self.partitions} partition")
                except TopicAlreadyExistsError:
                    logger.info(f"ℹ️ Topic '{self.topic}' zaten mevcut")
                    
            elif topic_exists and self.auto_configure_partitions:
                try:
                    topic_metadata = admin_client.describe_topics([self.topic])
                    if topic_metadata and self.topic in topic_metadata:
                        topic_info = topic_metadata[self.topic]
                        current_partitions = len(topic_info.partitions) if hasattr(topic_info, 'partitions') else self.partitions
                    else:
                        current_partitions = self.partitions
                except Exception as e:
                    logger.warning(f"⚠️ Topic metadata alınamadı, varsayılan partition sayısı kullanılıyor: {e}")
                    current_partitions = self.partitions
                
                if current_partitions < self.partitions:
                    partition_update = {self.topic: NewPartitions(total_count=self.partitions)}
                    
                    try:
                        admin_client.create_partitions(partition_update)
                        logger.info(f"✅ Topic '{self.topic}' partition sayısı güncellendi: {current_partitions} -> {self.partitions}")
                    except InvalidPartitionsError as e:
                        logger.warning(f"⚠️ Partition güncellemesi başarısız: {e}")
                else:
                    logger.info(f"ℹ️ Topic '{self.topic}' zaten {current_partitions} partition'a sahip")
                    
            admin_client.close()
            
        except Exception as e:
            logger.error(f"❌ Kafka topic konfigürasyon hatası: {e}")
    
    def to_consumer_config(self) -> Dict[str, Any]:
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'value_deserializer': lambda x: x.decode('utf-8') if x else None,
            'max_poll_records': self.max_poll_records,
            'enable_auto_commit': self.enable_auto_commit
        }