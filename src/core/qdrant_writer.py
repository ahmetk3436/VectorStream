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
from src.utils.error_handler import (
    retry_with_policy, RetryPolicy, BackoffStrategy,
    NETWORK_RETRY_POLICY, retry_network_errors
)

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
        # Qdrant operations için retry policy
        qdrant_policy = RetryPolicy(
            max_attempts=4,
            base_delay=1.0,
            max_delay=30.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            timeout=30.0,
            retryable_exceptions=[ConnectionError, TimeoutError, OSError],
            non_retryable_exceptions=[ValueError, TypeError]
        )
        
        @retry_with_policy(qdrant_policy)
        async def _initialize_with_retry():
            await self.circuit_breaker.call(self._initialize_collection_impl)
        
        try:
            await _initialize_with_retry()
            logger.info(f"Koleksiyon başarıyla başlatıldı: {self.collection_name}")
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
        # Write operations için aggressive retry policy
        write_policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=60.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            timeout=45.0,
            retryable_exceptions=[ConnectionError, TimeoutError, OSError],
            non_retryable_exceptions=[ValueError, TypeError]
        )
        
        @retry_with_policy(write_policy)
        async def _write_with_retry():
            await self.circuit_breaker.call(self._write_embeddings_impl, embeddings)
        
        try:
            await _write_with_retry()
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
        # Search operations için conservative retry policy
        search_policy = RetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            max_delay=15.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            timeout=20.0,
            retryable_exceptions=[ConnectionError, TimeoutError, OSError],
            non_retryable_exceptions=[ValueError, TypeError]
        )
        
        @retry_with_policy(search_policy)
        async def _search_with_retry():
            return await self.circuit_breaker.call(
                self._search_similar_impl, query_vector, limit
            )
        
        try:
            results = await _search_with_retry()
            logger.debug(f"Arama tamamlandı: {len(results)} sonuç bulundu")
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
