#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Callable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp,
    window, count, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, ArrayType, FloatType
)
from pyspark.sql.streaming import StreamingQuery
from loguru import logger
import json
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.spark.embedding_job import SparkEmbeddingJob
from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig
from src.exceptions.embedding_exceptions import EmbeddingProcessingError

class KafkaSparkConnector:
    """
    Kafka ve Spark arasında köprü görevi gören connector
    
    Bu sınıf Kafka stream'lerini Spark ile işler,
    real-time embedding oluşturur ve sonuçları depolar.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Kafka-Spark connector'ını başlat
        
        Args:
            config: Sistem konfigürasyonu
        """
        self.config = config
        self.kafka_config = config.get('kafka', {})
        self.spark_config = config.get('spark', {})
        self.qdrant_config = config.get('qdrant', {})
        self.streaming_config = config.get('streaming', {})
        
        # Streaming ayarları
        self.batch_duration = self.streaming_config.get('batch_duration', '10 seconds')
        self.checkpoint_location = self.streaming_config.get('checkpoint_location', '/tmp/spark-checkpoints')
        self.watermark_delay = self.streaming_config.get('watermark_delay', '1 minute')
        self.max_files_per_trigger = self.streaming_config.get('max_files_per_trigger', 1)
        
        # Kafka ayarları
        self.bootstrap_servers = self.kafka_config.get('bootstrap_servers', 'localhost:9092')
        self.input_topic = self.kafka_config.get('topic', 'embeddings')
        self.output_topic = self.kafka_config.get('output_topic', 'processed_embeddings')
        self.consumer_group = self.kafka_config.get('group_id', 'spark_embedding_processor')
        
        # Embedding job
        self.embedding_job = SparkEmbeddingJob(self.spark_config)
        self.spark: Optional[SparkSession] = None
        
        # Active queries
        self.active_queries: Dict[str, StreamingQuery] = {}
        
        # Circuit breaker
        self.circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=120.0,  # 2 minutes
            timeout=600.0  # 10 minutes for streaming
        )
    
    def initialize(self):
        """
        Connector'ı başlat
        """
        try:
            logger.info("Kafka-Spark connector başlatılıyor...")
            
            # Spark session'ını başlat
            self.spark = self.embedding_job.initialize_spark()
            
            # Streaming için gerekli konfigürasyonları ekle
            self._configure_streaming()
            
            # Checkpoint dizinini oluştur
            Path(self.checkpoint_location).mkdir(parents=True, exist_ok=True)
            
            logger.info("✅ Kafka-Spark connector başlatıldı")
            
        except Exception as e:
            logger.error(f"Kafka-Spark connector başlatma hatası: {e}")
            raise EmbeddingProcessingError(f"Kafka-Spark connector initialization failed: {e}")
    
    def _configure_streaming(self):
        """
        Streaming için Spark konfigürasyonunu ayarla
        """
        try:
            # Streaming optimizasyonları
            self.spark.conf.set("spark.sql.streaming.checkpointLocation", self.checkpoint_location)
            self.spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
            self.spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
            
            # Kafka optimizasyonları
            self.spark.conf.set("spark.sql.streaming.kafka.consumer.pollTimeoutMs", "5000")
            self.spark.conf.set("spark.sql.streaming.kafka.consumer.fetchOffset.numRetries", "3")
            
            logger.debug("Streaming konfigürasyonu tamamlandı")
            
        except Exception as e:
            logger.error(f"Streaming konfigürasyon hatası: {e}")
            raise
    
    def _get_kafka_schema(self) -> StructType:
        """
        Kafka mesajları için schema tanımla
        
        Returns:
            StructType: Kafka mesaj schema'sı
        """
        return StructType([
            StructField("id", StringType(), True),
            StructField("content", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("metadata", StringType(), True),
            StructField("source", StringType(), True),
            StructField("priority", IntegerType(), True)
        ])
    
    @circuit_breaker("kafka_stream_processing")
    def start_streaming_pipeline(self, 
                                output_mode: str = "append",
                                trigger_interval: str = "10 seconds") -> StreamingQuery:
        """
        Streaming pipeline'ını başlat
        
        Args:
            output_mode: Çıktı modu (append, complete, update)
            trigger_interval: Trigger aralığı
            
        Returns:
            StreamingQuery: Başlatılan streaming query
        """
        try:
            logger.info(f"Streaming pipeline başlatılıyor: {self.input_topic} -> {self.output_topic}")
            
            # Kafka stream'ini oku
            kafka_stream = self._create_kafka_stream()
            
            # Mesajları parse et
            parsed_stream = self._parse_kafka_messages(kafka_stream)
            
            # Embedding'leri oluştur
            processed_stream = self._process_embeddings(parsed_stream)
            
            # Sonuçları Kafka'ya yaz
            query = self._write_to_kafka(processed_stream, output_mode, trigger_interval)
            
            # Query'yi kaydet
            self.active_queries['main_pipeline'] = query
            
            logger.info(f"✅ Streaming pipeline başlatıldı: {query.id}")
            return query
            
        except Exception as e:
            logger.error(f"Streaming pipeline başlatma hatası: {e}")
            raise EmbeddingProcessingError(f"Streaming pipeline failed: {e}")
    
    def _create_kafka_stream(self) -> DataFrame:
        """
        Kafka stream'ini oluştur
        
        Returns:
            DataFrame: Kafka stream DataFrame
        """
        try:
            logger.debug(f"Kafka stream oluşturuluyor: {self.bootstrap_servers}/{self.input_topic}")
            
            kafka_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.consumer.group.id", self.consumer_group) \
                .option("maxOffsetsPerTrigger", self.streaming_config.get('max_offsets_per_trigger', 1000)) \
                .load()
            
            logger.debug("✅ Kafka stream oluşturuldu")
            return kafka_stream
            
        except Exception as e:
            logger.error(f"Kafka stream oluşturma hatası: {e}")
            raise
    
    def _parse_kafka_messages(self, kafka_stream: DataFrame) -> DataFrame:
        """
        Kafka mesajlarını parse et
        
        Args:
            kafka_stream: Ham Kafka stream
            
        Returns:
            DataFrame: Parse edilmiş mesajlar
        """
        try:
            logger.debug("Kafka mesajları parse ediliyor...")
            
            schema = self._get_kafka_schema()
            
            parsed_stream = kafka_stream.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("message_value"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            ).withColumn(
                "parsed_data",
                from_json(col("message_value"), schema)
            ).select(
                col("message_key"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("parsed_data.*")
            ).withColumn(
                "processing_timestamp",
                current_timestamp()
            )
            
            # Watermark ekle (late data handling için)
            watermarked_stream = parsed_stream.withWatermark(
                "processing_timestamp", 
                self.watermark_delay
            )
            
            logger.debug("✅ Kafka mesajları parse edildi")
            return watermarked_stream
            
        except Exception as e:
            logger.error(f"Kafka mesaj parse hatası: {e}")
            raise
    
    def _process_embeddings(self, parsed_stream: DataFrame) -> DataFrame:
        """
        Embedding'leri işle
        
        Args:
            parsed_stream: Parse edilmiş stream
            
        Returns:
            DataFrame: Embedding'ler eklenmiş stream
        """
        try:
            logger.debug("Embedding'ler işleniyor...")
            
            # Boş içerikleri filtrele
            filtered_stream = parsed_stream.filter(
                col("content").isNotNull() & 
                (col("content") != "") &
                (col("content") != " ")
            )
            
            # Embedding UDF'ini oluştur
            from pyspark.sql.functions import udf
            
            def create_embedding_safe(text: str) -> List[float]:
                """
                Güvenli embedding oluşturma
                """
                try:
                    return self.embedding_job._create_single_embedding(text)
                except Exception as e:
                    logger.error(f"Embedding oluşturma hatası: {e}")
                    return [0.0] * self.embedding_job.vector_size
            
            embedding_udf = udf(create_embedding_safe, ArrayType(FloatType()))
            
            # Embedding'leri ekle
            processed_stream = filtered_stream.withColumn(
                "embedding",
                embedding_udf(col("content"))
            ).withColumn(
                "embedding_timestamp",
                current_timestamp()
            )
            
            logger.debug("✅ Embedding'ler işlendi")
            return processed_stream
            
        except Exception as e:
            logger.error(f"Embedding işleme hatası: {e}")
            raise
    
    def _write_to_kafka(self, 
                       processed_stream: DataFrame, 
                       output_mode: str,
                       trigger_interval: str) -> StreamingQuery:
        """
        İşlenmiş veriyi Kafka'ya yaz
        
        Args:
            processed_stream: İşlenmiş stream
            output_mode: Çıktı modu
            trigger_interval: Trigger aralığı
            
        Returns:
            StreamingQuery: Streaming query
        """
        try:
            logger.debug(f"Kafka'ya yazma başlatılıyor: {self.output_topic}")
            
            # Çıktı formatını hazırla
            output_stream = processed_stream.select(
                col("id").alias("key"),
                to_json(struct(
                    col("id"),
                    col("content"),
                    col("embedding"),
                    col("timestamp"),
                    col("metadata"),
                    col("source"),
                    col("processing_timestamp"),
                    col("embedding_timestamp")
                )).alias("value")
            )
            
            # Kafka'ya yaz
            query = output_stream.writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("topic", self.output_topic) \
                .option("checkpointLocation", f"{self.checkpoint_location}/kafka_output") \
                .outputMode(output_mode) \
                .trigger(processingTime=trigger_interval) \
                .start()
            
            logger.debug(f"✅ Kafka yazma başlatıldı: {query.id}")
            return query
            
        except Exception as e:
            logger.error(f"Kafka yazma hatası: {e}")
            raise
    
    def start_monitoring_pipeline(self) -> StreamingQuery:
        """
        Monitoring pipeline'ını başlat
        
        Returns:
            StreamingQuery: Monitoring query
        """
        try:
            logger.info("Monitoring pipeline başlatılıyor...")
            
            # Kafka stream'ini oku
            kafka_stream = self._create_kafka_stream()
            
            # Mesajları parse et
            parsed_stream = self._parse_kafka_messages(kafka_stream)
            
            # Windowed aggregations
            windowed_stats = parsed_stream \
                .withWatermark("processing_timestamp", "1 minute") \
                .groupBy(
                    window(col("processing_timestamp"), "1 minute"),
                    col("source")
                ).agg(
                    count("*").alias("message_count"),
                    avg(col("priority")).alias("avg_priority"),
                    spark_max(col("processing_timestamp")).alias("latest_timestamp"),
                    spark_min(col("processing_timestamp")).alias("earliest_timestamp")
                )
            
            # Console'a yaz (monitoring için)
            query = windowed_stats.writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .option("checkpointLocation", f"{self.checkpoint_location}/monitoring") \
                .trigger(processingTime="30 seconds") \
                .start()
            
            # Query'yi kaydet
            self.active_queries['monitoring'] = query
            
            logger.info(f"✅ Monitoring pipeline başlatıldı: {query.id}")
            return query
            
        except Exception as e:
            logger.error(f"Monitoring pipeline hatası: {e}")
            raise EmbeddingProcessingError(f"Monitoring pipeline failed: {e}")
    
    def start_qdrant_sink_pipeline(self) -> StreamingQuery:
        """
        Qdrant sink pipeline'ını başlat
        
        Returns:
            StreamingQuery: Qdrant sink query
        """
        try:
            logger.info("Qdrant sink pipeline başlatılıyor...")
            
            # Processed topic'ten oku
            processed_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.output_topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            # Mesajları parse et
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("content", StringType(), True),
                StructField("embedding", ArrayType(FloatType()), True),
                StructField("timestamp", StringType(), True),
                StructField("metadata", StringType(), True),
                StructField("source", StringType(), True),
                StructField("processing_timestamp", TimestampType(), True),
                StructField("embedding_timestamp", TimestampType(), True)
            ])
            
            parsed_stream = processed_stream.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")
            
            # Qdrant'a yazma fonksiyonu
            def write_to_qdrant_batch(df, epoch_id):
                """
                Batch'i Qdrant'a yaz
                """
                try:
                    logger.info(f"Qdrant'a yazılıyor: epoch {epoch_id}, {df.count()} kayıt")
                    
                    # DataFrame'i collect et
                    rows = df.collect()
                    
                    if not rows:
                        return
                    
                    # Qdrant writer'ı import et
                    from src.core.qdrant_writer import QdrantWriter
                    
                    qdrant_writer = QdrantWriter(self.qdrant_config)
                    
                    # Embedding verilerini hazırla
                    embeddings_data = []
                    for row in rows:
                        embeddings_data.append({
                            'vector': row['embedding'],
                            'metadata': {
                                'id': row['id'],
                                'content': row['content'],
                                'timestamp': row['timestamp'],
                                'source': row['source'],
                                'processing_timestamp': str(row['processing_timestamp']),
                                'embedding_timestamp': str(row['embedding_timestamp'])
                            }
                        })
                    
                    # Async olmayan versiyonu kullan
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(qdrant_writer.write_embeddings(embeddings_data))
                        logger.info(f"✅ Qdrant'a yazıldı: {len(embeddings_data)} kayıt")
                    finally:
                        loop.close()
                    
                except Exception as e:
                    logger.error(f"Qdrant yazma hatası (epoch {epoch_id}): {e}")
            
            # Qdrant sink query
            query = parsed_stream.writeStream \
                .foreachBatch(write_to_qdrant_batch) \
                .option("checkpointLocation", f"{self.checkpoint_location}/qdrant_sink") \
                .trigger(processingTime="30 seconds") \
                .start()
            
            # Query'yi kaydet
            self.active_queries['qdrant_sink'] = query
            
            logger.info(f"✅ Qdrant sink pipeline başlatıldı: {query.id}")
            return query
            
        except Exception as e:
            logger.error(f"Qdrant sink pipeline hatası: {e}")
            raise EmbeddingProcessingError(f"Qdrant sink pipeline failed: {e}")
    
    def get_streaming_status(self) -> Dict[str, Any]:
        """
        Streaming durumunu getir
        
        Returns:
            Dict[str, Any]: Streaming durumu
        """
        try:
            status = {
                'active_queries': len(self.active_queries),
                'queries': {}
            }
            
            for name, query in self.active_queries.items():
                try:
                    progress = query.lastProgress
                    status['queries'][name] = {
                        'id': query.id,
                        'is_active': query.isActive,
                        'run_id': query.runId,
                        'last_progress': {
                            'batch_id': progress.get('batchId', 0),
                            'input_rows_per_second': progress.get('inputRowsPerSecond', 0),
                            'processed_rows_per_second': progress.get('processedRowsPerSecond', 0),
                            'batch_duration': progress.get('batchDuration', 0),
                            'timestamp': progress.get('timestamp', '')
                        } if progress else None
                    }
                except Exception as e:
                    status['queries'][name] = {
                        'error': str(e)
                    }
            
            return status
            
        except Exception as e:
            logger.error(f"Streaming durum alma hatası: {e}")
            return {'error': str(e)}
    
    def stop_query(self, query_name: str):
        """
        Belirli bir query'yi durdur
        
        Args:
            query_name: Durdurulacak query adı
        """
        try:
            if query_name in self.active_queries:
                query = self.active_queries[query_name]
                query.stop()
                del self.active_queries[query_name]
                logger.info(f"✅ Query durduruldu: {query_name}")
            else:
                logger.warning(f"Query bulunamadı: {query_name}")
                
        except Exception as e:
            logger.error(f"Query durdurma hatası {query_name}: {e}")
    
    def stop_all_queries(self):
        """
        Tüm aktif query'leri durdur
        """
        try:
            logger.info("Tüm streaming query'ler durduruluyor...")
            
            for query_name in list(self.active_queries.keys()):
                self.stop_query(query_name)
            
            logger.info("✅ Tüm streaming query'ler durduruldu")
            
        except Exception as e:
            logger.error(f"Query'leri durdurma hatası: {e}")
    
    def stop(self):
        """
        Connector'ı durdur
        """
        try:
            logger.info("Kafka-Spark connector durduruluyor...")
            
            # Tüm query'leri durdur
            self.stop_all_queries()
            
            # Embedding job'ını durdur
            if self.embedding_job:
                self.embedding_job.stop()
            
            logger.info("✅ Kafka-Spark connector durduruldu")
            
        except Exception as e:
            logger.error(f"Connector durdurma hatası: {e}")