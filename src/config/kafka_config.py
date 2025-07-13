from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = "latest"
    batch_size: int = 100
    max_poll_records: int = 500
    consumer_timeout_ms: int = 1000
    enable_auto_commit: bool = True
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        # Only use parameters that the class accepts
        valid_params = {k: v for k, v in config_dict.items() 
                       if k in cls.__annotations__}
        return cls(**valid_params)
    
    def to_consumer_config(self) -> Dict[str, Any]:
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'value_deserializer': lambda x: x.decode('utf-8') if x else None,
            'max_poll_records': self.max_poll_records,
            'enable_auto_commit': self.enable_auto_commit
        }