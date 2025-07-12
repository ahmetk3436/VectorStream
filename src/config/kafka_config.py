from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = "latest"
    batch_size: int = 100
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        return cls(**config_dict)
    
    def to_consumer_config(self) -> Dict[str, Any]:
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'value_deserializer': lambda x: x.decode('utf-8') if x else None
        }