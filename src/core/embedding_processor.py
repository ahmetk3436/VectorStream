#!/usr/bin/env python3

import asyncio
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from loguru import logger
import numpy as np
import torch

class EmbeddingProcessor:
    """Metin embedding işlemcisi"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize embedding processor"""
        self.config = config
        self.model_name = config.get('model_name', 'all-MiniLM-L6-v2')
        self.model: Optional[SentenceTransformer] = None
        self.vector_size = config.get('vector_size', 384)
        
    def _get_best_device(self):
        """En iyi device'ı belirle: GPU -> MPS -> CPU sıralamasında"""
        if torch.cuda.is_available():
            return 'cuda'
        elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            return 'mps'
        else:
            return 'cpu'
    
    async def initialize(self):
        """Model yükle"""
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