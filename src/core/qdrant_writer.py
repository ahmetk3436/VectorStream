import asyncio
import sys
from pathlib import Path
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from loguru import logger
import uuid

# Proje root'unu Python path'ine ekle
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.circuit_breaker import CircuitBreakerConfig, circuit_breaker_manager, CircuitBreakerError

class QdrantWriter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = QdrantClient(
            host=config['host'],
            port=config['port']
        )
        self.collection_name = config['collection_name']
        
        # Circuit breaker setup
        self.circuit_breaker = circuit_breaker_manager.create_circuit_breaker(
            name="qdrant_writer",
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=30.0,
                timeout=15.0
            )
        )
        
    async def initialize_collection(self):
        """Koleksiyonu oluştur"""
        try:
            await self.circuit_breaker.call(self._initialize_collection_impl)
        except CircuitBreakerError as e:
            logger.error(f"Circuit breaker hatası (koleksiyon oluşturma): {e}")
            raise
        except Exception as e:
            logger.error(f"Koleksiyon oluşturma hatası: {e}")
            raise
    
    def _initialize_collection_impl(self):
        """Koleksiyon oluşturma implementasyonu"""
        # Koleksiyon var mı kontrol et
        collections = self.client.get_collections()
        collection_names = [col.name for col in collections.collections]
        
        if self.collection_name not in collection_names:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.config['vector_size'],
                    distance=Distance.COSINE
                )
            )
            logger.info(f"Koleksiyon oluşturuldu: {self.collection_name}")
        else:
            logger.info(f"Koleksiyon zaten mevcut: {self.collection_name}")
            
    async def write_embeddings(self, embeddings: List[Dict[str, Any]]):
        """Embedding'leri Qdrant'a yaz"""
        try:
            await self.circuit_breaker.call(self._write_embeddings_impl, embeddings)
            logger.info(f"✅ {len(embeddings)} embedding yazıldı")
            return True
            
        except CircuitBreakerError as e:
            logger.error(f"Circuit breaker hatası (embedding yazma): {e}")
            return False
        except Exception as e:
            logger.error(f"Embedding yazma hatası: {e}")
            return False
    
    def _write_embeddings_impl(self, embeddings: List[Dict[str, Any]]):
        """Embedding yazma implementasyonu"""
        points = []
        for emb in embeddings:
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=emb['vector'],
                payload=emb.get('metadata', {})
            )
            points.append(point)
            
        self.client.upsert(
            collection_name=self.collection_name,
            points=points
        )
            
    async def search_similar(self, query_vector: List[float], limit: int = 5):
        """Benzer vektörleri ara"""
        try:
            results = await self.circuit_breaker.call(
                self._search_similar_impl, query_vector, limit
            )
            return results
        except CircuitBreakerError as e:
            logger.error(f"Circuit breaker hatası (arama): {e}")
            return []
        except Exception as e:
            logger.error(f"Arama hatası: {e}")
            return []
    
    def _search_similar_impl(self, query_vector: List[float], limit: int = 5):
        """Arama implementasyonu"""
        return self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=limit
        )
