import asyncio
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from loguru import logger
import uuid

class QdrantWriter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = QdrantClient(
            host=config['host'],
            port=config['port']
        )
        self.collection_name = config['collection_name']
        
    async def initialize_collection(self):
        """Koleksiyonu oluştur"""
        try:
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
                
        except Exception as e:
            logger.error(f"Koleksiyon oluşturma hatası: {e}")
            raise
            
    async def write_embeddings(self, embeddings: List[Dict[str, Any]]):
        """Embedding'leri Qdrant'a yaz"""
        try:
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
            
            logger.info(f"✅ {len(points)} embedding yazıldı")
            return True
            
        except Exception as e:
            logger.error(f"Embedding yazma hatası: {e}")
            return False
            
    async def search_similar(self, query_vector: List[float], limit: int = 5):
        """Benzer vektörleri ara"""
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=limit
            )
            return results
        except Exception as e:
            logger.error(f"Arama hatası: {e}")
            return []
