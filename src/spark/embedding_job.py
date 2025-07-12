#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, explode, struct
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField, StringType
from sentence_transformers import SentenceTransformer
import numpy as np
from loguru import logger

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig
from src.exceptions.embedding_exceptions import EmbeddingProcessingError

class SparkEmbeddingJob:
    """
    Spark tabanlı dağıtık embedding işleme job'ı
    
    Bu sınıf büyük veri setlerini Spark kullanarak dağıtık şekilde işler
    ve embedding'leri oluşturur.
    """
    
    def __init__(self, spark_config: Dict[str, Any]):
        """
        Spark embedding job'ını başlat
        
        Args:
            spark_config: Spark konfigürasyonu
        """
        self.spark_config = spark_config
        self.spark: Optional[SparkSession] = None
        self.model_name = spark_config.get('embedding_model', 'all-MiniLM-L6-v2')
        self.batch_size = spark_config.get('batch_size', 1000)
        self.vector_size = spark_config.get('vector_size', 384)
        
        # Circuit breaker for embedding operations
        self.circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=60.0,
            timeout=300.0  # 5 minutes for large batch processing
        )
    
    def initialize_spark(self) -> SparkSession:
        """
        Spark session'ını başlat
        
        Returns:
            SparkSession: Başlatılmış Spark session
        """
        try:
            logger.info("Spark session başlatılıyor...")
            
            spark_builder = SparkSession.builder \
                .appName("NewMind-AI-Embedding-Processor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            
            # Master URL'i ayarla
            master_url = self.spark_config.get('master', 'local[*]')
            spark_builder = spark_builder.master(master_url)
            
            # Executor konfigürasyonu
            executor_memory = self.spark_config.get('executor_memory', '2g')
            executor_cores = self.spark_config.get('executor_cores', '2')
            
            spark_builder = spark_builder \
                .config("spark.executor.memory", executor_memory) \
                .config("spark.executor.cores", executor_cores)
            
            # Driver konfigürasyonu
            driver_memory = self.spark_config.get('driver_memory', '1g')
            spark_builder = spark_builder.config("spark.driver.memory", driver_memory)
            
            self.spark = spark_builder.getOrCreate()
            
            # Log level'ı ayarla
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"✅ Spark session başlatıldı: {self.spark.version}")
            logger.info(f"   Master: {master_url}")
            logger.info(f"   Executor Memory: {executor_memory}")
            logger.info(f"   Executor Cores: {executor_cores}")
            
            return self.spark
            
        except Exception as e:
            logger.error(f"Spark session başlatma hatası: {e}")
            raise EmbeddingProcessingError(f"Spark initialization failed: {e}")
    
    def create_embedding_udf(self):
        """
        Embedding oluşturma UDF'ini oluştur
        
        Returns:
            UDF: Spark UDF fonksiyonu
        """
        def create_embeddings_batch(texts: List[str]) -> List[List[float]]:
            """
            Batch halinde embedding oluştur
            
            Args:
                texts: İşlenecek metinler
                
            Returns:
                List[List[float]]: Embedding'ler
            """
            try:
                # Model'i lazy loading ile yükle
                if not hasattr(create_embeddings_batch, 'model'):
                    logger.info(f"Embedding model yükleniyor: {self.model_name}")
                    create_embeddings_batch.model = SentenceTransformer(self.model_name)
                
                # Boş metinleri filtrele
                valid_texts = [text for text in texts if text and text.strip()]
                
                if not valid_texts:
                    return [[0.0] * self.vector_size] * len(texts)
                
                # Embedding'leri oluştur
                embeddings = create_embeddings_batch.model.encode(
                    valid_texts,
                    batch_size=min(len(valid_texts), 32),  # Chunk size for memory efficiency
                    show_progress_bar=False
                )
                
                # Sonuçları orijinal sırayla eşleştir
                results = []
                valid_idx = 0
                
                for text in texts:
                    if text and text.strip():
                        results.append(embeddings[valid_idx].tolist())
                        valid_idx += 1
                    else:
                        results.append([0.0] * self.vector_size)
                
                return results
                
            except Exception as e:
                logger.error(f"Batch embedding oluşturma hatası: {e}")
                # Hata durumunda sıfır vektörler döndür
                return [[0.0] * self.vector_size] * len(texts)
        
        # UDF'i tanımla
        return udf(
            create_embeddings_batch,
            ArrayType(ArrayType(FloatType()))
        )
    
    @circuit_breaker("spark_embedding_processing")
    def process_dataframe(self, df: DataFrame, text_column: str = "content") -> DataFrame:
        """
        DataFrame'i işle ve embedding'leri oluştur
        
        Args:
            df: İşlenecek DataFrame
            text_column: Metin içeren sütun adı
            
        Returns:
            DataFrame: Embedding'ler eklenmiş DataFrame
        """
        try:
            logger.info(f"DataFrame işleniyor: {df.count()} satır")
            
            # Embedding UDF'ini oluştur
            embedding_udf = self.create_embedding_udf()
            
            # Batch'lere böl ve işle
            df_with_batch_id = df.withColumn(
                "batch_id", 
                (col("monotonically_increasing_id") / self.batch_size).cast("int")
            )
            
            # Her batch için embedding'leri oluştur
            result_df = df_with_batch_id.groupBy("batch_id").agg(
                collect_list(text_column).alias("texts"),
                collect_list(struct([col(c) for c in df.columns])).alias("original_data")
            ).withColumn(
                "embeddings", 
                embedding_udf(col("texts"))
            )
            
            # Sonuçları düzleştir
            final_df = result_df.select(
                explode(
                    arrays_zip(
                        col("original_data"),
                        col("embeddings")
                    )
                ).alias("result")
            ).select(
                col("result.original_data.*"),
                col("result.embeddings").alias("embedding")
            )
            
            logger.info(f"✅ DataFrame işleme tamamlandı: {final_df.count()} satır")
            return final_df
            
        except Exception as e:
            logger.error(f"DataFrame işleme hatası: {e}")
            raise EmbeddingProcessingError(f"DataFrame processing failed: {e}")
    
    def process_kafka_stream(self, kafka_config: Dict[str, Any]) -> DataFrame:
        """
        Kafka stream'ini işle
        
        Args:
            kafka_config: Kafka konfigürasyonu
            
        Returns:
            DataFrame: İşlenmiş stream DataFrame
        """
        try:
            logger.info("Kafka stream işleme başlatılıyor...")
            
            # Kafka stream'ini oku
            kafka_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
                .option("subscribe", kafka_config['topic']) \
                .option("startingOffsets", "latest") \
                .load()
            
            # JSON'u parse et
            from pyspark.sql.functions import from_json
            
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("content", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("metadata", StringType(), True)
            ])
            
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")
            
            # Embedding'leri oluştur (streaming için basitleştirilmiş)
            embedding_udf = udf(
                lambda text: self._create_single_embedding(text),
                ArrayType(FloatType())
            )
            
            result_df = parsed_df.withColumn(
                "embedding",
                embedding_udf(col("content"))
            )
            
            logger.info("✅ Kafka stream işleme konfigürasyonu tamamlandı")
            return result_df
            
        except Exception as e:
            logger.error(f"Kafka stream işleme hatası: {e}")
            raise EmbeddingProcessingError(f"Kafka stream processing failed: {e}")
    
    def _create_single_embedding(self, text: str) -> List[float]:
        """
        Tek metin için embedding oluştur (streaming için)
        
        Args:
            text: İşlenecek metin
            
        Returns:
            List[float]: Embedding vektörü
        """
        try:
            if not hasattr(self, '_model'):
                self._model = SentenceTransformer(self.model_name)
            
            if not text or not text.strip():
                return [0.0] * self.vector_size
            
            embedding = self._model.encode(text.strip())
            return embedding.tolist()
            
        except Exception as e:
            logger.error(f"Single embedding oluşturma hatası: {e}")
            return [0.0] * self.vector_size
    
    def write_to_qdrant(self, df: DataFrame, qdrant_config: Dict[str, Any]):
        """
        DataFrame'i Qdrant'a yaz
        
        Args:
            df: Yazılacak DataFrame
            qdrant_config: Qdrant konfigürasyonu
        """
        try:
            logger.info("Qdrant'a yazma işlemi başlatılıyor...")
            
            # DataFrame'i collect et (küçük batch'ler için)
            rows = df.collect()
            
            # Qdrant writer'ı import et
            from src.core.qdrant_writer import QdrantWriter
            
            qdrant_writer = QdrantWriter(qdrant_config)
            
            # Batch'ler halinde yaz
            batch_size = 100
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                
                embeddings_data = []
                for row in batch:
                    embeddings_data.append({
                        'vector': row['embedding'],
                        'metadata': {
                            'id': row.get('id', ''),
                            'content': row.get('content', ''),
                            'timestamp': row.get('timestamp', ''),
                            'source': 'spark_batch'
                        }
                    })
                
                # Async olmayan versiyonu kullan
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(qdrant_writer.write_embeddings(embeddings_data))
                finally:
                    loop.close()
                
                logger.info(f"Batch yazıldı: {len(batch)} kayıt")
            
            logger.info(f"✅ Qdrant'a yazma tamamlandı: {len(rows)} kayıt")
            
        except Exception as e:
            logger.error(f"Qdrant yazma hatası: {e}")
            raise EmbeddingProcessingError(f"Qdrant write failed: {e}")
    
    def run_batch_job(self, input_path: str, output_path: str, qdrant_config: Dict[str, Any]):
        """
        Batch job'ını çalıştır
        
        Args:
            input_path: Girdi dosya yolu
            output_path: Çıktı dosya yolu
            qdrant_config: Qdrant konfigürasyonu
        """
        try:
            logger.info(f"Batch job başlatılıyor: {input_path} -> {output_path}")
            
            if not self.spark:
                self.initialize_spark()
            
            # Girdi dosyasını oku
            input_df = self.spark.read.json(input_path)
            
            # Embedding'leri oluştur
            processed_df = self.process_dataframe(input_df)
            
            # Sonuçları kaydet
            processed_df.write.mode("overwrite").json(output_path)
            
            # Qdrant'a yaz
            self.write_to_qdrant(processed_df, qdrant_config)
            
            logger.info("✅ Batch job tamamlandı")
            
        except Exception as e:
            logger.error(f"Batch job hatası: {e}")
            raise EmbeddingProcessingError(f"Batch job failed: {e}")
    
    def stop(self):
        """
        Spark session'ını durdur
        """
        if self.spark:
            logger.info("Spark session durduruluyor...")
            self.spark.stop()
            self.spark = None
            logger.info("✅ Spark session durduruldu")

# Helper functions for UDF imports
from pyspark.sql.functions import collect_list, arrays_zip