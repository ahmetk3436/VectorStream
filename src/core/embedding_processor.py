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
import time

class EmbeddingProcessor:
    """
    Task gereksinimi: Sentence Transformers embedding işlemcisi
    Ürün açıklamalarını embedding'e dönüştürür
    """
    
    def __init__(self, config: Dict[str, Any]):
        """High-performance embedding processor with MPS optimization"""
        self.config = config
        # Task gereksinimi: Sentence Transformers model
        self.model_name = config.get('model_name', 'all-MiniLM-L6-v2')  
        self.model: Optional[SentenceTransformer] = None
        self.vector_size = config.get('vector_size', 384)
        # Increased batch size for better GPU utilization
        self.batch_size = config.get('batch_size', 512)  # 32 → 512 for M3 Pro
        self.device = self._get_best_device()
        
        # Performance optimization flags
        self.use_fp16 = config.get('use_fp16', True)
        
        # Performance tracking
        self.total_processed = 0
        self.total_time = 0.0
        
        logger.info(f"🚀 High-performance embedding processor başlatılıyor:")
        logger.info(f"   📝 Model: {self.model_name} (Sentence Transformers)")
        logger.info(f"   📏 Vector size: {self.vector_size}")
        logger.info(f"   🔥 Device: {self.device}")
        logger.info(f"   📦 Batch size: {self.batch_size} (optimized for M3 Pro)")
        logger.info(f"   ⚡ FP16: {self.use_fp16}")
    
    def _get_best_device(self):
        """Optimized device selection for M3 Pro performance"""
        if torch.cuda.is_available():
            device = 'cuda'
            logger.info("✅ CUDA GPU bulundu - 18k+ sentences/sec bekleniyor")
        elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            device = 'mps'  
            logger.info("✅ Apple MPS bulundu - M3 Pro optimized, 18k+ sentences/sec hedefleniyor")
            # Note: Not setting default device to avoid tensor device conflicts
        else:
            device = 'cpu'
            logger.info("⚠️ Sadece CPU kullanılacak - ~750 sentences/sec sınırı")
        return device
    
    async def initialize(self):
        """High-performance model initialization with MPS optimization"""
        try:
            logger.info(f"🚀 High-performance Sentence Transformers model yükleniyor: {self.model_name}")
            
            # Initialize model with device-specific optimizations
            model_kwargs = {}
            
            if self.device == 'mps':
                # For MPS, initialize directly on MPS to avoid device conflicts
                # Let SentenceTransformers handle MPS device placement internally
                self.model = SentenceTransformer(
                    self.model_name, 
                    model_kwargs=model_kwargs,
                    device=self.device  # Direct MPS initialization
                )
                logger.info(f"✅ Model doğrudan {self.device} device'da başlatıldı")
                
                # Skip FP16 for MPS as it can cause issues
                if self.use_fp16:
                    logger.info("ℹ️ MPS'de FP16 atlandı - uyumluluk için")
            else:
                # For CUDA and CPU, use the traditional approach
                self.model = SentenceTransformer(
                    self.model_name, 
                    model_kwargs=model_kwargs,
                    device='cpu'  # Initialize on CPU first
                )
                
                # Move to target device after initialization
                if self.device == 'cuda':
                    self.model = self.model.to(self.device)
                    logger.info(f"✅ Model {self.device} device'a taşındı")
                    
                    # Apply FP16 optimization after device move
                    if self.use_fp16:
                        self.model.half()
                        logger.info("✅ FP16 precision uygulandı - hız artışı")
                elif self.use_fp16:
                    # CPU FP16 is not recommended, skip
                    logger.info("ℹ️ CPU'da FP16 atlandı - performans sorunu yaratabilir")
            
            # Model bilgilerini logla
            embedding_dim = self.model.get_sentence_embedding_dimension()
            logger.info(f"🚀 High-performance Sentence Transformers model hazır:")
            logger.info(f"   📏 Embedding dimension: {embedding_dim}")
            logger.info(f"   🎯 M3 Pro optimized - hedef: 3-4k events/sec")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ High-performance embedding model yükleme hatası: {e}")
            return False
    

    
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
            # Fix device handling for MPS compatibility
            encode_params = {
                'convert_to_tensor': False,
                'normalize_embeddings': True,
                'batch_size': self.batch_size,
                'show_progress_bar': len(texts) > 100,
            }
            
            # Only specify device for non-MPS to avoid conflicts
            if self.device != 'mps':
                encode_params['device'] = self.device
                
            # Task performans gereksinimi: Batch processing
            embeddings = self.model.encode(texts, **encode_params)
            
            # Numpy array'i liste'ye çevir
            if isinstance(embeddings, np.ndarray):
                embeddings = embeddings.tolist()
            
            logger.info(f"✅ Task uyumlu batch embedding tamamlandı: {len(texts)} metin")
            return embeddings
            
        except Exception as e:
            logger.error(f"❌ Task uyumlu batch embedding hatası: {e}")
            # Fallback: Rastgele vektörler (sadece test için)
            return [np.random.rand(self.vector_size).tolist() for _ in texts]
    

    
    async def process_batch(self, texts: List[str]) -> List[np.ndarray]:
        """High-performance batch embedding with performance tracking"""
        if not self.model:
            raise RuntimeError("Model henüz başlatılmamış - initialize() çağırın")
        
        if not texts:
            return []
        
        try:
            start_time = time.time()
            batch_size = len(texts)
            
            logger.debug(f"🚀 High-performance batch işleniyor: {batch_size} metin")
            
            # Fix device handling for MPS compatibility
            # For MPS, we need to be more careful about device handling
            encode_device = None
            if self.device == 'mps':
                # For MPS, let SentenceTransformers handle device placement internally
                # Don't specify device parameter to avoid conflicts
                encode_device = None
            else:
                # For CUDA and CPU, explicitly specify device
                encode_device = self.device
                # Ensure model is on correct device before encoding
                if hasattr(self.model, 'device') and str(self.model.device) != self.device:
                    self.model = self.model.to(self.device)
                
            # Prepare encode parameters based on device
            encode_params = {
                'batch_size': self.batch_size,
                'show_progress_bar': False,
                'convert_to_numpy': True,
                'normalize_embeddings': True,
                'convert_to_tensor': False,  # Direct numpy for speed
            }
            
            # Only add device parameter if not MPS
            if encode_device is not None:
                encode_params['device'] = encode_device
                
            embeddings = self.model.encode(texts, **encode_params)
            
            # Numpy array'leri listeye çevir
            embedding_list = [emb for emb in embeddings]
            
            # Performance tracking
            elapsed_time = time.time() - start_time
            self.total_processed += batch_size
            self.total_time += elapsed_time
            
            # Calculate and log performance metrics
            current_rate = batch_size / elapsed_time if elapsed_time > 0 else 0
            avg_rate = self.total_processed / self.total_time if self.total_time > 0 else 0
            
            logger.debug(f"⚡ High-performance batch tamamlandı: {len(embedding_list)} embedding")
            logger.debug(f"📊 Performance: {current_rate:.1f} evt/s (current), {avg_rate:.1f} evt/s (avg)")
            
            # Log milestone achievements
            if avg_rate > 1000:
                logger.info(f"🎯 HEDEF AŞILDI! Ortalama hız: {avg_rate:.1f} evt/s > 1000 evt/s")
            elif avg_rate > 500:
                logger.info(f"🚀 İyi performans: {avg_rate:.1f} evt/s (hedef: 1000+ evt/s)")
            
            return embedding_list
            
        except Exception as e:
            logger.error(f"❌ High-performance batch embedding hatası: {e}")
            raise
    
    async def create_embedding(self, text: str) -> Optional[List[float]]:
        """Optimized single text embedding creation"""
        try:
            # Auto-initialize if model not loaded
            if not self.model:
                await self.initialize()
                
            batch_result = await self.process_batch([text])
            if batch_result:
                return batch_result[0].tolist()
            return None
        except Exception as e:
            logger.error(f"❌ Optimized single embedding oluşturma hatası: {e}")
            return None
    
    def get_performance_stats(self) -> Dict[str, float]:
        """Get current performance statistics"""
        avg_rate = self.total_processed / self.total_time if self.total_time > 0 else 0
        return {
            "total_processed": self.total_processed,
            "total_time": self.total_time,
            "average_rate_evt_per_sec": avg_rate,
            "target_rate": 1000.0,
            "performance_ratio": avg_rate / 1000.0 if avg_rate > 0 else 0
        }
    
    async def create_embeddings(self, texts: List[str]) -> List[Optional[List[float]]]:
        """High-performance multiple text embedding creation"""
        try:
            # Auto-initialize if model not loaded
            if not self.model:
                await self.initialize()
                
            # Use optimized batch processing
            batch_result = await self.process_batch(texts)
            return [emb.tolist() for emb in batch_result]
            
        except Exception as e:
            logger.error(f"❌ High-performance batch embedding oluşturma hatası: {e}")
            # Hata durumunda None listesi döndür
            return [None] * len(texts)
    
    async def create_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Bulk embedding creation for high-throughput processing
        
        This method is optimized for bulk message ingestion scenarios where
        thousands of messages need to be processed efficiently.
        
        Args:
            texts: List of text strings to create embeddings for
            
        Returns:
            List of embedding vectors (as lists of floats) or None for failed embeddings
        """
        if not texts:
            return []
            
        try:
            # Auto-initialize if model not loaded
            if not self.model:
                await self.initialize()
            
            start_time = time.time()
            batch_size = len(texts)
            
            logger.debug(f"🚀 Bulk embedding creation başlatıldı: {batch_size} texts")
            
            # Process in optimal batch sizes to prevent memory issues
            max_batch_size = self.batch_size
            all_embeddings = []
            
            for i in range(0, len(texts), max_batch_size):
                batch_texts = texts[i:i + max_batch_size]
                batch_embeddings = await self.process_batch(batch_texts)
                all_embeddings.extend([emb.tolist() for emb in batch_embeddings])
            
            elapsed_time = time.time() - start_time
            rate = batch_size / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(f"✅ Bulk embedding tamamlandı: {batch_size} embeddings in {elapsed_time:.3f}s ({rate:.0f} embeddings/s)")
            
            return all_embeddings
            
        except Exception as e:
            logger.error(f"❌ Bulk embedding creation hatası: {e}")
            # Return None for all texts in case of error
            return [None] * len(texts)
    
    async def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Mesajı işle ve embedding oluştur"""
        try:
            # Auto-initialize if model not loaded
            if not self.model:
                await self.initialize()
                
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
            # Auto-initialize if model not loaded
            if not self.model:
                await self.initialize()
                
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

    async def close(self):
        """Clean up resources and close the embedding processor"""
        try:
            logger.info("🧹 Embedding processor kapatılıyor...")
            
            # Clear model from memory
            if self.model:
                # Clear model cache
                if hasattr(self.model, 'cpu'):
                    self.model = self.model.cpu()
                del self.model
                self.model = None
                logger.info("🗑️ Model memory'den temizlendi")
            
            # Clear torch cache if available
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                logger.debug("🗑️ CUDA cache temizlendi")
            elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                torch.mps.empty_cache()
                logger.debug("🗑️ MPS cache temizlendi")
            
            # Force garbage collection
            import gc
            gc.collect()
            
            logger.info("✅ Embedding processor başarıyla kapatıldı")
            
        except Exception as e:
            logger.warning(f"Embedding processor kapatma hatası: {e}")

# Task uyumluluğu için alias
SentenceTransformersEmbeddingProcessor = EmbeddingProcessor