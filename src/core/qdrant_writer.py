import asyncio
import sys
from pathlib import Path
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from loguru import logger
import uuid
import time

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
        """High-performance Qdrant writer with gRPC optimization"""
        self.config = config
        self.host = config.get('host', 'localhost')
        # Use gRPC port for 15-20% lower latency
        self.port = config.get('port', 6334)  # gRPC port instead of 6333 HTTP
        self.use_grpc = config.get('use_grpc', True)
        
        # QdrantClient configuration for gRPC optimization
        # port=6333 for REST API, grpc_port=6334 for gRPC API
        self.client = QdrantClient(
            host=self.host,
            port=6333,  # REST API port
            grpc_port=6334,  # gRPC API port
            prefer_grpc=self.use_grpc
        )
        self.collection_name = config['collection_name']
        
        # Performance tracking
        self.total_vectors = 0
        self.total_time = 0.0
        self.start_time = None
        
        # Circuit breaker setup
        self.circuit_breaker = circuit_breaker_manager.create_circuit_breaker(
            name="qdrant_writer",
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=30.0,
                timeout=15.0
            )
        )
        
        logger.info(f"🚀 High-performance Qdrant Writer yapılandırıldı:")
        logger.info(f"   🌐 Host: {self.host}:{self.port} ({'gRPC' if self.use_grpc else 'HTTP'})")
        logger.info(f"   📚 Collection: {self.collection_name}")
        logger.info(f"   ⚡ gRPC enabled: {self.use_grpc} (15-20% lower latency)")
        logger.info(f"   🎯 Target: 1200+ RPS (Qdrant optimized)")
        
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
            
    async def write_embeddings(self, embeddings: List[Dict[str, Any]], batch_size: int = 1000):
        """Embedding'leri Qdrant'a yaz - Performans için büyük batch size destekli"""
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
            await self.circuit_breaker.call(self._write_embeddings_impl, embeddings, batch_size)
        
        try:
            await _write_with_retry()
            logger.info(f"✅ {len(embeddings)} embedding yazıldı (batch_size={batch_size})")
            return True
            
        except CircuitBreakerError as e:
            logger.error(f"Circuit breaker hatası (embedding yazma): {e}")
            return False
        except Exception as e:
            logger.error(f"Embedding yazma hatası: {e}")
            return False
    
    def _write_embeddings_impl(self, embeddings: List[Dict[str, Any]], batch_size: int = 5000):
        """High-performance write operation with gRPC and performance tracking"""
        start_time = time.time()
        
        # Performance tracking
        if self.start_time is None:
            self.start_time = start_time
        
        # Process in optimized 5000-vector chunks for 8k-10k vec/s throughput
        for i in range(0, len(embeddings), batch_size):
            batch = embeddings[i:i + batch_size]
            points = []
            
            for emb in batch:
                point = PointStruct(
                    id=emb.get('id', str(uuid.uuid4())),  # ID'yi embedding'den al
                    vector=emb['vector'],
                    payload=emb.get('payload', emb.get('metadata', {}))
                )
                points.append(point)
                
            # High-performance batch upsert with gRPC and wait=False (8k-10k vec/s)
            operation_info = self.client.upsert(
                collection_name=self.collection_name,
                points=points,
                wait=False  # Critical for 8k-10k vec/s burst performance
            )
            
            # Performance tracking
            elapsed_time = time.time() - start_time
            vector_count = len(points)
            self.total_vectors += vector_count
            self.total_time += elapsed_time
            
            # Calculate performance metrics
            current_rate = vector_count / elapsed_time if elapsed_time > 0 else 0
            total_elapsed = time.time() - self.start_time
            avg_rate = self.total_vectors / total_elapsed if total_elapsed > 0 else 0
            
            if len(embeddings) > batch_size:
                logger.debug(f"⚡ Batch {i//batch_size + 1} yazıldı: {len(points)} points (gRPC, wait=False)")
                logger.debug(f"📊 Qdrant Performance: {current_rate:.1f} vec/s (current), {avg_rate:.1f} vec/s (avg)")
            
            # Log performance milestones
            if self.total_vectors % 1000 == 0:
                logger.info(f"📊 Qdrant Writer Performance:")
                logger.info(f"   📈 Current: {current_rate:.1f} vec/s")
                logger.info(f"   📊 Average: {avg_rate:.1f} vec/s")
                logger.info(f"   🎯 Target: 1200+ vec/s (sustained)")
                logger.info(f"   ⚡ Operation ID: {operation_info.operation_id}")
                
                if avg_rate > 1200:
                    logger.info(f"🎯 HEDEF AŞILDI! Qdrant yazma hızı: {avg_rate:.1f} vec/s > 1200 vec/s")
                elif avg_rate > 800:
                    logger.info(f"🚀 İyi performans: {avg_rate:.1f} vec/s")
                else:
                    logger.warning(f"⚠️ Hedefin altında: {avg_rate:.1f} vec/s")
            
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
    
    async def close(self):
        """QdrantWriter'ı kapat"""
        try:
            if hasattr(self.client, 'close'):
                self.client.close()
            logger.info("QdrantWriter başarıyla kapatıldı")
        except Exception as e:
            logger.warning(f"QdrantWriter kapatma sırasında hata: {e}")
