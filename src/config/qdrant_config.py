from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class QdrantConfig:
    host: str
    port: int
    collection_name: str
    vector_size: int = 384
    distance_metric: str = "Cosine"
    api_key: Optional[str] = None
    timeout: int = 30
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]):
        return cls(**config_dict)
    
    def to_client_config(self) -> Dict[str, Any]:
        config = {
            'host': self.host,
            'port': self.port,
            'timeout': self.timeout
        }
        if self.api_key:
            config['api_key'] = self.api_key
        return config
    
    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"