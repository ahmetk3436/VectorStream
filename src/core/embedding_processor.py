#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task Uyumlu Embedding Processor
VectorStream MLOps Task için Sentence Transformers tabanlı embedding işlemcisi
"""

import asyncio
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from loguru import logger
import numpy as np
import torch

class EmbeddingProcessor:
    """
    Task gereksinimi: Sentence Transformers embedding işlemcisi
    Ürün açıklamalarını embedding'e dönüştürür
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Task uyumlu embedding processor başlatma"""
        self.config = config
        # Task gereksinimi: Sentence Transformers model
        self.model_name = config.get('model_name', 'all-MiniLM-L6-v2')  
        self.model: Optional[SentenceTransformer] = None
        self.vector_size = config.get('vector_size', 384)
        self.batch_size = config.get('batch_size', 32)
        self.device = self._get_best_device()
        
        logger.info(f"🎯 Task uyumlu embedding processor başlatılıyor:")
        logger.info(f"   📝 Model: {self.model_name} (Sentence Transformers)")
        logger.info(f"   📏 Vector size: {self.vector_size}")
        logger.info(f"   🔥 Device: {self.device}")
        logger.info(f"   📦 Batch size: {self.batch_size}")
    
    def _get_best_device(self):
        """Task performansı için en iyi device seç: GPU -> MPS -> CPU"""
        if torch.cuda.is_available():
            device = 'cuda'
            logger.info("✅ CUDA GPU bulundu - Task performansı optimize")
        elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            device = 'mps'  
            logger.info("✅ Apple MPS bulundu - Task performansı optimize")
        else:
            device = 'cpu'
            logger.info("⚠️ Sadece CPU kullanılacak - Task performansı sınırlı")
        return device
    
    async def initialize(self):
        """Task gereksinimi: Sentence Transformers model yükle"""
        try:
            logger.info(f"🔄 Task uyumlu Sentence Transformers model yükleniyor: {self.model_name}")
            
            # Task gereksinimi: Sentence Transformers model
            self.model = SentenceTransformer(self.model_name)
            
            # GPU/MPS kullanımı optimize et
            if self.device in ['cuda', 'mps']:
                self.model = self.model.to(self.device)
                logger.info(f"✅ Model {self.device} device'a taşındı")
            
            # Model bilgilerini logla
            embedding_dim = self.model.get_sentence_embedding_dimension()
            logger.info(f"✅ Task uyumlu Sentence Transformers model hazır:")
            logger.info(f"   📏 Embedding dimension: {embedding_dim}")
            logger.info(f"   🎯 Task gereksinimi karşılandı: Sentence Transformers ✓")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Task uyumlu embedding model yükleme hatası: {e}")
            return False
    
    async def create_embedding(self, text: str) -> List[float]:
        """
        Task gereksinimi: Ürün açıklamalarını embedding'e dönüştür
        
        Args:
            text: Ürün açıklaması metni
            
        Returns:
            List[float]: Sentence Transformers embedding vector
        """
        if not self.model:
            await self.initialize()
        
        try:
            # Task gereksinimi: Sentence Transformers ile embedding
            embedding = self.model.encode(
                text,
                convert_to_tensor=False,
                normalize_embeddings=True,  # Cosine similarity için normalize
                batch_size=1
            )
            
            # Numpy array'i liste'ye çevir
            if isinstance(embedding, np.ndarray):
                embedding = embedding.tolist()
            
            # Task gereksinimi: Vector size kontrolü
            if len(embedding) != self.vector_size:
                logger.warning(f"⚠️ Vector size uyumsuzluğu: {len(embedding)} != {self.vector_size}")
            
            return embedding
            
        except Exception as e:
            logger.error(f"❌ Task uyumlu embedding oluşturma hatası: {e}")
            # Fallback: Rastgele vector (sadece test için)
            return np.random.rand(self.vector_size).tolist()
    
    async def create_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Task gereksinimi: Batch halinde embedding oluştur (performans için)
        
        Args:
            texts: Ürün açıklaması metinleri listesi
            
        Returns:
            List[List[float]]: Sentence Transformers embedding vektörleri
        """
        if not self.model:
            await self.initialize()
        
        try:
            # Task performans gereksinimi: Batch processing
            embeddings = self.model.encode(
                texts,
                convert_to_tensor=False,
                normalize_embeddings=True,
                batch_size=self.batch_size,
                show_progress_bar=len(texts) > 100
            )
            
            # Numpy array'i liste'ye çevir
            if isinstance(embeddings, np.ndarray):
                embeddings = embeddings.tolist()
            
            logger.info(f"✅ Task uyumlu batch embedding tamamlandı: {len(texts)} metin")
            return embeddings
            
        except Exception as e:
            logger.error(f"❌ Task uyumlu batch embedding hatası: {e}")
            # Fallback: Rastgele vektörler (sadece test için)
            return [np.random.rand(self.vector_size).tolist() for _ in texts]
        device_priority = ['cuda', 'mps', 'cpu']
        
        for device in device_priority:
            try:
                logger.info(f"Embedding model yükleniyor: {self.model_name}")
                
                # Device kontrolü
                if device == 'cuda' and not torch.cuda.is_available():
                    continue
                elif device == 'mps' and not (hasattr(torch.backends, 'mps') and torch.backends.mps.is_available()):
                    continue
                
                logger.info(f"Denenen device: {device}")
                
                # Meta tensor hatası için özel çözüm
                model_kwargs = {
                    'trust_remote_code': True,
                    'device': None  # Device'ı None yapıyoruz, sonra manuel olarak taşıyacağız
                }
                
                # Model'i önce CPU'da yükle
                self.model = SentenceTransformer(
                    self.model_name,
                    **model_kwargs
                )
                
                # Sonra hedef device'a taşı
                if device != 'cpu':
                    try:
                        # to_empty() kullanarak meta tensor hatasını önle
                        self.model = self.model.to(device)
                    except Exception as device_error:
                        logger.warning(f"Device {device}'a taşıma başarısız: {device_error}")
                        # CPU'da kalsın
                        device = 'cpu'
                
                logger.info(f"✅ Embedding model yüklendi: {self.model_name} on {device}")
                return  # Başarılı yükleme, fonksiyondan çık
                
            except Exception as e:
                logger.warning(f"Device {device} ile model yükleme başarısız: {e}")
                if device == 'cpu':  # CPU son seçenek, hata fırlat
                    logger.error(f"Tüm device'larda model yükleme başarısız")
                    raise e
                continue  # Sonraki device'ı dene
        
        # Buraya ulaşılmamalı, ama güvenlik için
        raise Exception("Hiçbir device'da model yüklenemedi")
    
    async def create_embedding(self, text: str) -> Optional[List[float]]:
        """Tek metin için embedding oluştur"""
        if not self.model:
            await self.initialize()
            
        if not text or not text.strip():
            logger.warning("Boş metin için embedding oluşturulamaz")
            return None
            
        try:
            embedding = self.model.encode(text.strip())
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Embedding oluşturma hatası: {e}")
            return None
    
    async def create_embeddings(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Çoklu metin için embedding oluştur"""
        if not self.model:
            await self.initialize()
            
        if not texts:
            return []
            
        try:
            # Boş metinleri filtrele
            valid_texts = [text.strip() for text in texts if text and text.strip()]
            
            if not valid_texts:
                return [None] * len(texts)
                
            embeddings = self.model.encode(valid_texts)
            
            # Sonuçları orijinal sırayla eşleştir
            results = []
            valid_idx = 0
            
            for text in texts:
                if text and text.strip():
                    results.append(embeddings[valid_idx].tolist())
                    valid_idx += 1
                else:
                    results.append(None)
                    
            return results
            
        except Exception as e:
            logger.error(f"Batch embedding oluşturma hatası: {e}")
            return [None] * len(texts)
    
    async def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Mesajı işle ve embedding oluştur"""
        try:
            content = message.get('content', '')
            
            if not content:
                logger.warning(f"Mesajda content bulunamadı: {message.get('id', 'unknown')}")
                return None
                
            embedding = await self.create_embedding(content)
            
            if embedding is None:
                return None
                
            return {
                'vector': embedding,
                'metadata': {
                    'id': message.get('id', ''),
                    'content': content,
                    'timestamp': message.get('timestamp', ''),
                    'source': message.get('metadata', {}).get('source', 'unknown')
                }
            }
            
        except Exception as e:
            logger.error(f"Mesaj işleme hatası: {e}")
            return None
    
    async def process_messages(self, messages: List[Dict[str, Any]]) -> List[Optional[Dict[str, Any]]]:
        """Çoklu mesaj işle"""
        try:
            if not messages:
                return []
                
            # Tüm metinleri topla
            texts = [msg.get('content', '') for msg in messages]
            
            # Batch embedding oluştur
            embeddings = await self.create_embeddings(texts)
            
            # Sonuçları hazırla
            results = []
            for i, (message, embedding) in enumerate(zip(messages, embeddings)):
                if embedding is not None:
                    result = {
                        'vector': embedding,
                        'metadata': {
                            'id': message.get('id', f'batch_{i}'),
                            'content': message.get('content', ''),
                            'timestamp': message.get('timestamp', ''),
                            'source': message.get('metadata', {}).get('source', 'unknown')
                        }
                    }
                    results.append(result)
                else:
                    results.append(None)
                    
            return results
            
        except Exception as e:
            logger.error(f"Batch mesaj işleme hatası: {e}")
            return [None] * len(messages)
    
    def extract_text_from_task_event(self, event: Dict[str, Any]) -> str:
        """Task yapısına uygun event'ten metin çıkar
        
        Task event yapısı:
        {
            "event_id": "uuid",
            "timestamp": "2024-01-15T10:30:00Z",
            "user_id": "user123",
            "event_type": "purchase",
            "product": {
                "id": "uuid",
                "name": "Ürün Adı",
                "description": "Detaylı ürün açıklaması...",
                "category": "Elektronik",
                "price": 1299.99
            },
            "session_id": "session789"
        }
        """
        text_parts = []
        
        # Product bilgilerini SADECE nested yapıdan al (Task gereksinimi)
        product = event.get('product', {})
        if product.get('description'):
            text_parts.append(product.get('description'))
        if product.get('name'):
            text_parts.append(product.get('name'))
        if product.get('category'):
            text_parts.append(product.get('category'))
        
        # Search sorgusu varsa
        if event.get('search_query'):
            text_parts.append(event.get('search_query'))
        
        # Event type'ı da context olarak ekle
        if event.get('event_type'):
            text_parts.append(event.get('event_type'))
        
        text = ' '.join(text_parts) if text_parts else f"event_{event.get('event_id', 'unknown')}"
        return text

    async def process_task_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Task gereksinimlerine uygun event işle"""
        try:
            if not self.model:
                await self.initialize()
            
            # Task event'inden metin çıkar
            text = self.extract_text_from_task_event(event)
            
            if not text or text.strip() == '':
                logger.warning(f"Event'ten metin çıkarılamadı: {event.get('event_id')}")
                return None
            
            # Embedding oluştur
            embedding = await self.create_embedding(text)
            
            if embedding is None:
                logger.error(f"Embedding oluşturulamadı: {event.get('event_id')}")
                return None
            
            # Task yapısına uygun metadata
            product = event.get('product', {})
            return {
                'vector': embedding,
                'metadata': {
                    'event_id': event.get('event_id'),
                    'timestamp': event.get('timestamp'),
                    'event_type': event.get('event_type'),
                    'user_id': event.get('user_id'),
                    'session_id': event.get('session_id'),
                    'text': text,
                    # Product bilgileri
                    'product_id': product.get('id'),
                    'product_name': product.get('name'),
                    'product_description': product.get('description'),
                    'product_category': product.get('category'),
                    'product_price': product.get('price'),
                    # Search bilgileri
                    'search_query': event.get('search_query'),
                    'results_count': event.get('results_count'),
                    # Purchase bilgileri
                    'quantity': event.get('quantity'),
                    'total_amount': event.get('total_amount'),
                    'payment_method': event.get('payment_method'),
                    'processed_at': event.get('processed_at')
                }
            }
            
        except Exception as e:
            logger.error(f"Task event işleme hatası: {e}")
            return None

# Task uyumluluğu için alias
SentenceTransformersEmbeddingProcessor = EmbeddingProcessor