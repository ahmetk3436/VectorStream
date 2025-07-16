#!/usr/bin/env python3

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    current_timestamp,
    explode,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.exceptions.embedding_exceptions import EmbeddingProcessingError
from src.spark.embedding_job import SparkEmbeddingJob
from src.spark.distributed_embedding_processor import execute_distributed_processing
from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig


class KafkaSparkConnector:


    def __init__(
        self,
        config: Dict[str, Any],
        metrics: Any = None,
        embedding_processor: Any = None,
        qdrant_writer: Any = None,
    ) -> None:
        self.config = config
        self.kafka_config = config.get("kafka", {})
        self.spark_config = config.get("spark", {})
        self.qdrant_config = config.get("qdrant", {})
        self.streaming_config = config.get("streaming", {})
        

        self.use_distributed_processing = config.get("performance", {}).get("use_distributed_processing", True)

        self.metrics = metrics
        self.embedding_processor = embedding_processor
        self.qdrant_writer = qdrant_writer


        self.spark: Optional[SparkSession] = None
        self.active_queries: Dict[str, StreamingQuery] = {}


        self.batch_interval = self.streaming_config.get("batch_duration", "10 seconds")
        self.checkpoint_location = self.streaming_config.get(
            "checkpoint_location", "/tmp/spark-checkpoints"
        )
        self.watermark_delay = self.streaming_config.get("watermark_delay", "1 minute")


        self.bootstrap_servers = self.kafka_config.get(
            "bootstrap_servers", "localhost:9092"
        )
        self.input_topic = self.kafka_config.get("topic", "ecommerce-events")
        self.output_topic = self.kafka_config.get(
            "output_topic", "processed_embeddings"
        )
        self.consumer_group = self.kafka_config.get(
            "group_id", "spark_embedding_processor"
        )


        self.embedding_model = None
        self.qdrant_client = None


        self.circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=5, recovery_timeout=120.0, timeout=600.0
        )

        logger.info("🔥 Kafka-Spark Connector initialized for VectorStream Pipeline")
        logger.info("   ✅ Apache Spark Structured Streaming")
        logger.info(f"   ✅ Batch interval: {self.batch_interval}")
        logger.info(f"   ✅ Kafka topic: {self.input_topic}")


        self.embedding_job = SparkEmbeddingJob(self.spark_config)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def initialize(self) -> None:
        """Create Spark session & apply streaming-specific settings."""
        try:
            logger.info("Kafka-Spark connector başlatılıyor…")

            self.spark = self.embedding_job.initialize_spark()
            self._configure_streaming()

            Path(self.checkpoint_location).mkdir(parents=True, exist_ok=True)
            logger.info("✅ Kafka-Spark connector başlatıldı")

        except Exception as exc:  # pragma: no cover
            logger.exception("Kafka-Spark connector initialization failed")
            raise EmbeddingProcessingError(str(exc)) from exc

    @circuit_breaker("kafka_stream_processing")
    def start_streaming_pipeline(
        self, output_mode: str = "append", trigger_interval: str = "10 seconds"  # Task gereksinimi: 10 saniye
    ) -> StreamingQuery:
        """
        Read Kafka, parse events, create embeddings and dispatch each micro-batch
        to `_process_embeddings_batch()`.
        """
        try:
            logger.info(f"Streaming pipeline başlatılıyor: {self.input_topic} → {self.output_topic}")

            kafka_stream = self._create_kafka_stream()
            parsed_stream = self._parse_kafka_messages(kafka_stream)
            processed_stream = self._process_embeddings(parsed_stream)

            def _foreach_batch(df: DataFrame, epoch_id: int) -> None:
                try:
                    record_count = df.count()
                    if record_count == 0:
                        return

                    logger.info(f"Processing batch {epoch_id} ({record_count} records)")
                    if self.metrics:
                        self.metrics.record_kafka_message_processed(
                            self.input_topic, "success"
                        )

                    # Performans modu seçimi
                    if self.use_distributed_processing and record_count > 100:
                        # Büyük batch'ler için distributed processing
                        logger.info(f"Using distributed processing for {record_count} records")
                        execute_distributed_processing(df, self.config, self.qdrant_config)
                    else:
                        # Küçük batch'ler için driver-based processing
                        self._process_embeddings_batch(df, epoch_id)

                except Exception as exc:  # pragma: no cover
                    logger.exception("Batch processing error")
                    if self.metrics:
                        self.metrics.record_processing_error(
                            "spark_streaming", type(exc).__name__
                        )

            query = (
                processed_stream.writeStream.foreachBatch(_foreach_batch)
                .outputMode(output_mode)
                .option("checkpointLocation", f"{self.checkpoint_location}/main_pipeline")
                .trigger(processingTime=trigger_interval)
                .start()
            )

            self.active_queries["main_pipeline"] = query
            logger.info(f"✅ Streaming pipeline başlatıldı: {query.id}")
            return query

        except Exception as exc:  # pragma: no cover
            logger.exception("Streaming pipeline failed")
            if self.metrics:
                self.metrics.record_processing_error("spark_streaming", type(exc).__name__)
            raise EmbeddingProcessingError(str(exc)) from exc

    def stop_all_queries(self) -> None:
        """Stop every active StreamingQuery."""
        for name in list(self.active_queries):
            try:
                self.active_queries[name].stop()
                logger.info(f"✅ Query durduruldu: {name}")
            except Exception:  # pragma: no cover
                logger.exception(f"Query {name} durdurulamadı")
            finally:
                self.active_queries.pop(name, None)

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------
    def _configure_streaming(self) -> None:
        """Set Spark & Kafka confs that influence streaming stability."""
        spark = self.spark  # alias
        spark.conf.set("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
        spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
        spark.conf.set("spark.sql.streaming.kafka.useUninterruptibleThread", "true")
        spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        # Performans iyileştirmesi: max_offsets_per_trigger artırıldı
        max_offsets = self.streaming_config.get("max_offsets_per_trigger", 100000)  # 1000 -> 20000
        spark.conf.set("spark.streaming.maxOffsetsPerTrigger", str(max_offsets))

    def _create_kafka_stream(self) -> DataFrame:
        """Create structured-streaming DataFrame from Kafka topic."""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.input_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("kafka.consumer.group.id", self.consumer_group)
            .option("maxOffsetsPerTrigger", self.streaming_config.get("max_offsets_per_trigger", 100000))  # 1000 -> 20000
            .option("kafka.fetch.max.bytes", "10485760")  # 10 MB
            .option("kafka.max.poll.records", "10000")
            .load()
        )

    # -------------------------------------------------------------------------
    # Parsing & embedding creation
    # -------------------------------------------------------------------------
    @staticmethod
    def _kafka_schema() -> StructType:
        """Return StructType matching expected Kafka JSON payload."""
        product_schema = StructType(
            [
                StructField("id", StringType()),
                StructField("name", StringType()),
                StructField("description", StringType()),
                StructField("category", StringType()),
                StructField("price", FloatType()),
            ]
        )

        return StructType(
            [
                StructField("event_id", StringType()),
                StructField("timestamp", StringType()),
                StructField("user_id", StringType()),
                StructField("event_type", StringType()),
                StructField("product", product_schema),
                StructField("session_id", StringType()),
                StructField("search_query", StringType()),
                StructField("results_count", IntegerType()),
                StructField("quantity", IntegerType()),
                StructField("total_amount", FloatType()),
                StructField("payment_method", StringType()),
            ]
        )

    def _parse_kafka_messages(self, kafka_stream: DataFrame) -> DataFrame:
        schema = self._kafka_schema()

        parsed = (
            kafka_stream.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("message_value"),
                "topic",
                "partition",
                "offset",
                col("timestamp").alias("kafka_timestamp"),
            )
            .withColumn("parsed_data", from_json(col("message_value"), schema))
            .select("message_key", "topic", "partition", "offset", "kafka_timestamp", "parsed_data.*")
            .withColumn("processing_timestamp", current_timestamp())
        )

        return parsed.withWatermark("processing_timestamp", self.watermark_delay)

    def _process_embeddings(self, parsed_stream: DataFrame) -> DataFrame:
        """
        Derive a `content` string per event, then attach a *dummy* 384-d vector
        via a Pandas UDF (faster than per-row Python UDF).
        """
        from pyspark.sql.functions import pandas_udf
        import pandas as pd
        import hashlib

        @pandas_udf(ArrayType(FloatType()))
        def create_embedding_safe(texts: pd.Series) -> pd.Series:
            def _hash_vec(t: str) -> List[float]:
                if not t or not isinstance(t, str):
                    return [0.0] * 384
                h = hashlib.md5(t.encode()).hexdigest()
                vec = [(int(h[i : i + 2], 16) / 255.0) for i in range(0, 32, 2)]
                return (vec * (384 // len(vec) + 1))[:384]

            return texts.fillna("").apply(_hash_vec)

        stream = (
            parsed_stream.withColumn(
                "content",
                concat_ws(
                    " ",
                    col("event_type"),
                    col("product.name"),
                    col("product.description"),
                    col("product.category"),
                    col("search_query"),
                ),
            )
            .filter(col("content").isNotNull() & (col("content") != ""))
            .withColumn("embedding", create_embedding_safe(col("content")))
            .withColumn("embedding_timestamp", current_timestamp())
        )

        return stream

    # -------------------------------------------------------------------------
    # Micro-batch sink
    # -------------------------------------------------------------------------
    def _process_embeddings_batch(self, df: DataFrame, epoch_id: int) -> None:
        """
        Generate *real* embeddings with Sentence-Transformers using batch processing
        for optimal performance. Runs on driver side for each micro-batch.
        """
        # Lazily load heavy deps once
        if self.embedding_model is None:
            from sentence_transformers import SentenceTransformer

            self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("📝 Sentence Transformers model loaded")

        if self.qdrant_client is None:
            from src.core.qdrant_writer import QdrantWriter

            self.qdrant_client = QdrantWriter(self.qdrant_config)

        # PERFORMANS İYİLEŞTİRMESİ: Batch processing
        # Önce tüm metinleri ve metadata'yı topla
        texts = []
        metadata_list = []
        
        for row in df.toLocalIterator():
            prod = getattr(row, "product", None)

            prod_desc = getattr(row, "product_description", None) or (
                prod.description if prod and getattr(prod, "description", None) else ""
            )
            prod_name = prod.name if prod and getattr(prod, "name", None) else ""
            prod_cat = prod.category if prod and getattr(prod, "category", None) else ""
            prod_price = prod.price if prod and getattr(prod, "price", None) else None
            prod_id = prod.id if prod and getattr(prod, "id", None) else ""

            text_parts = [prod_desc, f"Product: {prod_name}", f"Category: {prod_cat}"]
            text = " ".join(filter(None, text_parts)) or "empty event"
            
            texts.append(text)
            metadata_list.append({
                "event_id": getattr(row, "event_id", str(uuid.uuid4())),
                "event_type": getattr(row, "event_type", None),
                "user_id": getattr(row, "user_id", None),
                "session_id": getattr(row, "session_id", None),
                "timestamp": getattr(row, "timestamp", None),
                "product_id": prod_id,
                "product_name": prod_name,
                "product_description": prod_desc,
                "product_category": prod_cat,
                "product_price": prod_price,
                "embedding_text": text,
            })

        # KRITIK: Tüm embeddings'i tek seferde hesapla (batch processing)
        if texts:
            vectors = self.embedding_model.encode(
                texts, 
                batch_size=5000,  # Optimize edilmiş batch size
                show_progress_bar=False,
                convert_to_tensor=False
            )
            
            # Embedding data'yı hazırla
            embeddings_data = [
                {
                    "id": meta["event_id"],
                    "vector": vector.tolist() if hasattr(vector, 'tolist') else vector,
                    "payload": {
                        **meta,
                        "processed_at": str(pd.Timestamp.now()),
                        "embedding_model": "all-MiniLM-L6-v2",
                        "epoch_id": epoch_id,
                        "pipeline_version": "1.0.0",
                    },
                }
                for meta, vector in zip(metadata_list, vectors)
            ]

            # PERFORMANS İYİLEŞTİRMESİ: Tek event loop kullan
            if not hasattr(self, '_event_loop'):
                import asyncio
                try:
                    self._event_loop = asyncio.get_event_loop()
                except RuntimeError:
                    self._event_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self._event_loop)

            async def _push():
                await self.qdrant_client.write_embeddings(embeddings_data, batch_size=2000)  # Büyük batch size

            self._event_loop.run_until_complete(_push())
            logger.info(
                f"✅ Wrote {len(embeddings_data)} embeddings to Qdrant (epoch {epoch_id}) - Batch processed"
            )
    
    def stop_streaming(self) -> None:
        """Stop all active streaming queries"""
        logger.info("🛑 Stopping Kafka-Spark streaming queries...")
        
        for query_name, query in self.active_queries.items():
            try:
                if query.isActive:
                    query.stop()
                    logger.info(f"✅ Stopped query: {query_name}")
            except Exception as e:
                logger.error(f"❌ Error stopping query {query_name}: {e}")
        
        self.active_queries.clear()
        
        if self.spark:
            try:
                self.spark.stop()
                logger.info("✅ Spark session stopped")
            except Exception as e:
                logger.error(f"❌ Error stopping Spark session: {e}")
        
        logger.info("✅ Kafka-Spark connector stopped")