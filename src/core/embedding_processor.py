import asyncio
import time
from typing import Any, Dict, List, Optional

import numpy as np
import torch
from loguru import logger
from sentence_transformers import SentenceTransformer


class EmbeddingProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_name = config.get("model_name", "all-MiniLM-L6-v2")
        self.model: Optional[SentenceTransformer] = None
        self.vector_size = config.get("vector_size", 384)
        self.batch_size = config.get("batch_size", 512)
        self.device = self._get_best_device()
        self.use_fp16 = config.get("use_fp16", True)

        self.total_processed = 0
        self.total_time = 0.0

        logger.info("🚀 High-performance embedding processor başlatılıyor:")
        logger.info(f"   📝 Model: {self.model_name} (Sentence Transformers)")
        logger.info(f"   📏 Vector size: {self.vector_size}")
        logger.info(f"   🔥 Device: {self.device}")
        logger.info(f"   📦 Batch size: {self.batch_size} (optimized for M3 Pro)")
        logger.info(f"   ⚡ FP16: {self.use_fp16}")

    # --------------------------------------------------------------------- #
    # INITIALISATION
    # --------------------------------------------------------------------- #
    def _get_best_device(self) -> str:
        if torch.cuda.is_available():
            logger.info("✅ CUDA GPU bulundu - 18k+ sentences/sec bekleniyor")
            return "cuda"

        if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            logger.info("✅ Apple MPS bulundu - M3 Pro optimized, 18k+ sentences/sec hedefleniyor")
            return "mps"

        logger.info("⚠️  Sadece CPU kullanılacak - ~750 sentences/sec sınırı")
        return "cpu"

    async def initialize(self) -> bool:
        try:
            logger.info(f"🚀 Sentence-Transformers modeli yükleniyor: {self.model_name}")

            model_kwargs: Dict[str, Any] = {}

            # MPS – doğrudan aygıta yükle
            if self.device == "mps":
                self.model = SentenceTransformer(self.model_name, model_kwargs=model_kwargs, device="mps")
                logger.info("✅ Model doğrudan mps aygıtında başlatıldı")

                if self.use_fp16:
                    logger.info("ℹ️  MPS'de FP16 atlandı (uyumluluk için)")

            # CUDA veya CPU
            else:
                self.model = SentenceTransformer(self.model_name, model_kwargs=model_kwargs, device="cpu")

                if self.device == "cuda":
                    self.model = self.model.to("cuda")
                    logger.info("✅ Model cuda aygıtına taşındı")

                    if self.use_fp16:
                        self.model.half()
                        logger.info("✅ FP16 precision uygulandı")
                elif self.use_fp16:
                    logger.info("ℹ️  CPU'da FP16 atlandı (performans sorunu)")

            emb_dim = self.model.get_sentence_embedding_dimension()
            logger.info("🚀 Model hazır:")
            logger.info(f"   📏 Embedding dimension: {emb_dim}")
            logger.info("   🎯 M3 Pro optimizasyonu – hedef 3-4 k evt/s")
            return True

        except Exception as exc:  # pragma: no cover
            logger.error(f"❌ Model yükleme hatası: {exc}")
            return False

    # --------------------------------------------------------------------- #
    # EMBEDDING OLUŞTURMA
    # --------------------------------------------------------------------- #
    async def create_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Kısa yoldan embed dizisi dön.  ndarray -> Python list."""
        if not self.model:
            await self.initialize()

        try:
            encode_params: Dict[str, Any] = {
                "convert_to_tensor": False,
                "normalize_embeddings": True,
                "batch_size": self.batch_size,
                "show_progress_bar": len(texts) > 100,
            }
            if self.device != "mps":
                encode_params["device"] = self.device

            embeddings = self.model.encode(texts, **encode_params)
            if isinstance(embeddings, np.ndarray):
                embeddings = embeddings.tolist()

            logger.info(f"✅ Batch embedding tamamlandı: {len(texts)} metin")
            return embeddings

        except Exception as exc:
            logger.error(f"❌ Batch embedding hatası: {exc}")
            # Sentineller – rastgele vektör
            return [np.random.rand(self.vector_size).tolist() for _ in texts]

    async def process_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Düşük seviye toplu işleme – ndarray döner."""
        if not self.model:
            raise RuntimeError("Model henüz başlatılmamış – initialize() çağırın")

        if not texts:
            return []

        start_time = time.time()
        try:
            encode_params: Dict[str, Any] = {
                "batch_size": self.batch_size,
                "show_progress_bar": False,
                "convert_to_numpy": True,
                "normalize_embeddings": True,
                "convert_to_tensor": False,
            }
            if self.device != "mps":
                encode_params["device"] = self.device
                # model yanlış aygıtta ise taşı
                if hasattr(self.model, "device") and str(self.model.device) != self.device:
                    self.model = self.model.to(self.device)

            embeddings = self.model.encode(texts, **encode_params)
            elapsed = time.time() - start_time

            # Performans metrikleri
            self.total_processed += len(texts)
            self.total_time += elapsed
            cur_rate = len(texts) / elapsed if elapsed else 0
            avg_rate = self.total_processed / self.total_time if self.total_time else 0

            logger.debug(f"⚡ Batch tamam: {len(texts)} embedding")
            logger.debug(f"📊 {cur_rate:.1f} evt/s (anlık) – {avg_rate:.1f} evt/s (ortalama)")
            if avg_rate > 1000:
                logger.info(f"🎯 Hedef aşıldı → {avg_rate:.1f} evt/s")
            elif avg_rate > 500:
                logger.info(f"🚀 İyi gidiş: {avg_rate:.1f} evt/s")

            return list(embeddings)

        except Exception as exc:  # pragma: no cover
            logger.error(f"❌ Batch embedding hatası: {exc}")
            raise

    async def create_embedding(self, text: str) -> Optional[List[float]]:
        try:
            if not self.model:
                await self.initialize()

            result = await self.process_batch([text])
            return result[0].tolist() if result else None

        except Exception as exc:  # pragma: no cover
            logger.error(f"❌ Tekil embedding hatası: {exc}")
            return None

    async def create_embeddings(self, texts: List[str]) -> List[Optional[List[float]]]:
        try:
            if not self.model:
                await self.initialize()

            batch_result = await self.process_batch(texts)
            return [vec.tolist() for vec in batch_result]

        except Exception as exc:
            logger.error(f"❌ Batch embedding oluşturma hatası: {exc}")
            return [None] * len(texts)

    async def create_embeddings_batch(
        self, texts: List[str]
    ) -> List[Optional[List[float]]]:
        """Büyük listeler için tekrar tekrar `process_batch` çağırır."""
        if not texts:
            return []

        if not self.model:
            await self.initialize()

        start_time = time.time()
        try:
            max_batch = self.batch_size
            all_embeddings: List[List[float]] = []

            for i in range(0, len(texts), max_batch):
                batch_texts = texts[i : i + max_batch]
                batch_vecs = await self.process_batch(batch_texts)
                all_embeddings.extend([vec.tolist() for vec in batch_vecs])

            elapsed = time.time() - start_time
            rate = len(texts) / elapsed if elapsed else 0
            logger.info(f"✅ Bulk embedding: {len(texts)} item, {elapsed:.2f}s – {rate:.0f} emb/s")
            return all_embeddings

        except Exception as exc:
            logger.error(f"❌ Bulk embedding hatası: {exc}")
            return [None] * len(texts)

    # --------------------------------------------------------------------- #
    # MESAJ / EVENT İŞLEME
    # --------------------------------------------------------------------- #
    async def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.model:
            await self.initialize()

        content = message.get("content", "")
        if not content:
            logger.warning(f"Mesajda content yok: {message.get('id', 'unknown')}")
            return None

        embedding = await self.create_embedding(content)
        if embedding is None:
            return None

        return {
            "vector": embedding,
            "metadata": {
                "id": message.get("id", ""),
                "content": content,
                "timestamp": message.get("timestamp", ""),
                "source": message.get("metadata", {}).get("source", "unknown"),
            },
        }

    async def process_messages(
        self, messages: List[Dict[str, Any]]
    ) -> List[Optional[Dict[str, Any]]]:
        if not self.model:
            await self.initialize()
        if not messages:
            return []

        texts = [m.get("content", "") for m in messages]
        embeddings = await self.create_embeddings(texts)

        results: List[Optional[Dict[str, Any]]] = []
        for i, (msg, emb) in enumerate(zip(messages, embeddings)):
            if emb is not None:
                results.append(
                    {
                        "vector": emb,
                        "metadata": {
                            "id": msg.get("id", f"batch_{i}"),
                            "content": msg.get("content", ""),
                            "timestamp": msg.get("timestamp", ""),
                            "source": msg.get("metadata", {}).get("source", "unknown"),
                        },
                    }
                )
            else:
                results.append(None)
        return results

    # --------------------------------------------------------------------- #
    # EVENT YARDIMCI METOTLARI
    # --------------------------------------------------------------------- #
    def extract_text_from_task_event(self, event: Dict[str, Any]) -> str:
        parts: List[str] = []

        product = event.get("product", {})
        parts.extend(
            [
                product.get("description", ""),
                product.get("name", ""),
                product.get("category", ""),
            ]
        )

        if event.get("search_query"):
            parts.append(event["search_query"])
        if event.get("event_type"):
            parts.append(event["event_type"])

        parts = [p for p in parts if p]  # boşları at
        return " ".join(parts) if parts else f"event_{event.get('event_id', 'unknown')}"

    async def process_task_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.model:
            await self.initialize()

        text = self.extract_text_from_task_event(event)
        if not text:
            logger.warning(f"Event metni çıkarılamadı: {event.get('event_id')}")
            return None

        embedding = await self.create_embedding(text)
        if embedding is None:
            logger.error(f"Embedding oluşmadı: {event.get('event_id')}")
            return None

        product = event.get("product", {})
        return {
            "vector": embedding,
            "metadata": {
                "event_id": event.get("event_id"),
                "timestamp": event.get("timestamp"),
                "event_type": event.get("event_type"),
                "user_id": event.get("user_id"),
                "session_id": event.get("session_id"),
                "text": text,
                # ürün
                "product_id": product.get("id"),
                "product_name": product.get("name"),
                "product_description": product.get("description"),
                "product_category": product.get("category"),
                "product_price": product.get("price"),
                # arama / ödeme
                "search_query": event.get("search_query"),
                "results_count": event.get("results_count"),
                "quantity": event.get("quantity"),
                "total_amount": event.get("total_amount"),
                "payment_method": event.get("payment_method"),
                "processed_at": event.get("processed_at"),
            },
        }

    # --------------------------------------------------------------------- #
    # KAPATMA
    # --------------------------------------------------------------------- #
    async def close(self) -> None:
        """Kaynakları serbest bırak."""
        logger.info("🧹 Embedding processor kapatılıyor…")
        try:
            if self.model:
                if hasattr(self.model, "cpu"):
                    self.model = self.model.cpu()
                del self.model
                self.model = None
                logger.info("🗑️  Model bellekten temizlendi")

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            elif hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                torch.mps.empty_cache()

            import gc

            gc.collect()
            logger.info("✅ Kapatma tamam")

        except Exception as exc:  # pragma: no cover
            logger.warning(f"Kapatma hatası: {exc}")
