from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = "latest"
    batch_size: int = 5000
    max_poll_records: int = 10000
    consumer_timeout_ms: int = 1000
    enable_auto_commit: bool = True
    partitions: int = 16
    replication_factor: int = 1
    auto_create_topic: bool = True
    auto_configure_partitions: bool = True
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        filtered_config = {
            k: v for k, v in config_dict.items() 
            if k in ['bootstrap_servers', 'topic', 'group_id', 'auto_offset_reset', 
                    'batch_size', 'max_poll_records', 'consumer_timeout_ms', 'enable_auto_commit',
                    'partitions', 'replication_factor', 'auto_create_topic', 'auto_configure_partitions']
        }
        return cls(**filtered_config)
    
    def to_consumer_config(self) -> Dict[str, Any]:
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'value_deserializer': lambda x: x.decode('utf-8') if x else None,
            'max_poll_records': self.max_poll_records,
            'enable_auto_commit': self.enable_auto_commit
        }