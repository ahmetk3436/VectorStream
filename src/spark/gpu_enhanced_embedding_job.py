#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, explode, struct, collect_list, arrays_zip
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField, StringType
import numpy as np
from loguru import logger
import time

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig
from src.exceptions.embedding_exceptions import EmbeddingProcessingError
from src.spark.rapids_gpu_processor import RAPIDSGPUProcessor
from src.spark.embedding_job import SparkEmbeddingJob

class GPUEnhancedEmbeddingJob(SparkEmbeddingJob):
    """
    GPU-enhanced Spark embedding job with RAPIDS acceleration
    
    Bu sınıf SparkEmbeddingJob'ı extend ederek RAPIDS GPU acceleration ekler.
    GPU mevcut değilse otomatik olarak parent class'ın CPU implementasyonuna fallback yapar.
    """
    
    def __init__(self, spark_config: Dict[str, Any]):
        """
        GPU-enhanced embedding job'ını başlat
        
        Args:
            spark_config: Spark konfigürasyonu
        """
        super().__init__(spark_config)
        
        # GPU processor'ı başlat
        self.gpu_processor = RAPIDSGPUProcessor({
            'gpu': spark_config.get('gpu', {}),
            'embedding': {
                'model': self.model_name,
                'vector_size': self.vector_size,
                'batch_size': self.batch_size
            }
        })
        
        # Performance tracking
        self.performance_stats = {
            'gpu_processing_times': [],
            'cpu_processing_times': [],
            'total_processed': 0,
            'gpu_failures': 0,
            'cpu_fallbacks': 0
        }
        
        logger.info(f"GPU-Enhanced Embedding Job initialized - GPU available: {self.gpu_processor.gpu_enabled}")
    
    def initialize_spark(self) -> SparkSession:
        """
        GPU-optimized Spark session'ını başlat
        
        Returns:
            SparkSession: GPU-optimized Spark session
        """
        try:
            logger.info("GPU-optimized Spark session başlatılıyor...")
            
            spark_builder = SparkSession.builder \
                .appName("NewMind-AI-GPU-Enhanced-Embedding-Processor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            
            # GPU-specific configurations
            if self.gpu_processor.gpu_enabled:
                # RAPIDS SQL plugin configurations
                spark_builder = spark_builder \
                    .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
                    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
                    .config("spark.rapids.sql.enabled", "true") \
                    .config("spark.rapids.memory.gpu.pooling.enabled", "true")
                
                logger.info("GPU-specific Spark configurations applied")
            
            # Master URL'i ayarla
            master_url = self.spark_config.get('master', 'local[*]')
            spark_builder = spark_builder.master(master_url)
            
            # Executor konfigürasyonu - GPU için optimize edilmiş
            if self.gpu_processor.gpu_enabled:
                executor_memory = self.spark_config.get('executor_memory', '4g')  # More memory for GPU
                executor_cores = self.spark_config.get('executor_cores', '4')     # More cores for GPU
            else:
                executor_memory = self.spark_config.get('executor_memory', '2g')
                executor_cores = self.spark_config.get('executor_cores', '2')
            
            spark_builder = spark_builder \
                .config("spark.executor.memory", executor_memory) \
                .config("spark.executor.cores", executor_cores)
            
            # Driver konfigürasyonu
            driver_memory = self.spark_config.get('driver_memory', '2g')
            spark_builder = spark_builder.config("spark.driver.memory", driver_memory)
            
            self.spark = spark_builder.getOrCreate()
            
            # Log level'ı ayarla
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"✅ GPU-Enhanced Spark session başlatıldı: {self.spark.version}")
            logger.info(f"   Master: {master_url}")
            logger.info(f"   Executor Memory: {executor_memory}")
            logger.info(f"   Executor Cores: {executor_cores}")
            logger.info(f"   GPU Enabled: {self.gpu_processor.gpu_enabled}")
            
            return self.spark
            
        except Exception as e:
            logger.error(f"GPU-Enhanced Spark session başlatma hatası: {e}")
            # Fallback to regular Spark session
            logger.info("Falling back to regular Spark session...")
            return super().initialize_spark()
    
    def create_gpu_embedding_udf(self):
        """
        GPU-accelerated embedding oluşturma UDF'ini oluştur
        
        Returns:
            UDF: GPU-accelerated Spark UDF fonksiyonu
        """
        def create_embeddings_batch_gpu(texts: List[str]) -> List[List[float]]:
            """
            GPU kullanarak batch halinde embedding oluştur
            
            Args:
                texts: İşlenecek metinler
                
            Returns:
                List[List[float]]: Embedding'ler
            """
            try:
                start_time = time.time()
                
                # GPU processor'ı kullan
                embeddings = self.gpu_processor.create_embeddings_with_fallback(texts)
                
                processing_time = time.time() - start_time
                
                # Performance tracking
                if self.gpu_processor.gpu_enabled:
                    self.performance_stats['gpu_processing_times'].append(processing_time)
                else:
                    self.performance_stats['cpu_processing_times'].append(processing_time)
                    self.performance_stats['cpu_fallbacks'] += 1
                
                self.performance_stats['total_processed'] += len(texts)
                
                # Convert to list format
                return embeddings.tolist()
                
            except Exception as e:
                logger.error(f"GPU batch embedding oluşturma hatası: {e}")
                self.performance_stats['gpu_failures'] += 1
                
                # Ultimate fallback to parent class method
                try:
                    return super().create_embedding_udf()(texts)
                except:
                    # Return zero vectors as last resort
                    return [[0.0] * self.vector_size] * len(texts)
        
        # UDF'i tanımla
        return udf(
            create_embeddings_batch_gpu,
            ArrayType(ArrayType(FloatType()))
        )
    
    @circuit_breaker("gpu_spark_embedding_processing")
    def process_dataframe(self, df: DataFrame, text_column: str = "content") -> DataFrame:
        """
        GPU-accelerated DataFrame işleme
        
        Args:
            df: İşlenecek DataFrame
            text_column: Metin içeren sütun adı
            
        Returns:
            DataFrame: Embedding'ler eklenmiş DataFrame
        """
        try:
            start_time = time.time()
            row_count = df.count()
            logger.info(f"GPU-Enhanced DataFrame işleniyor: {row_count} satır")
            
            # GPU embedding UDF'ini kullan
            embedding_udf = self.create_gpu_embedding_udf()
            
            # Adaptive batch sizing based on GPU memory
            if self.gpu_processor.gpu_enabled:
                memory_info = self.gpu_processor.get_memory_usage()
                gpu_memory_gb = memory_info.get('gpu_memory', {}).get('free_gb', 1.0)
                
                # Adjust batch size based on available GPU memory
                if gpu_memory_gb > 8.0:
                    dynamic_batch_size = min(self.batch_size * 2, 5000)
                elif gpu_memory_gb > 4.0:
                    dynamic_batch_size = self.batch_size
                else:
                    dynamic_batch_size = max(self.batch_size // 2, 500)
                
                logger.info(f"Dynamic batch size: {dynamic_batch_size} (GPU memory: {gpu_memory_gb:.1f}GB)")
            else:
                dynamic_batch_size = self.batch_size
            
            # Batch'lere böl ve işle
            df_with_batch_id = df.withColumn(
                "batch_id", 
                (col("monotonically_increasing_id") / dynamic_batch_size).cast("int")
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
            
            processing_time = time.time() - start_time
            logger.info(f"✅ GPU-Enhanced DataFrame işlendi: {row_count} satır, {processing_time:.2f}s")
            
            return final_df
            
        except Exception as e:
            logger.error(f"GPU-Enhanced DataFrame işleme hatası: {e}")
            # Fallback to parent class method
            return super().process_dataframe(df, text_column)
    
    def benchmark_gpu_vs_cpu(self, sample_texts: List[str], iterations: int = 3) -> Dict[str, Any]:
        """
        GPU vs CPU performance karşılaştırması
        
        Args:
            sample_texts: Test metinleri
            iterations: Test tekrar sayısı
            
        Returns:
            Dict[str, Any]: Benchmark sonuçları
        """
        logger.info(f"GPU vs CPU benchmark başlatılıyor: {len(sample_texts)} metin, {iterations} iterasyon")
        
        return self.gpu_processor.benchmark_performance(sample_texts, iterations)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Performance istatistiklerini getir
        
        Returns:
            Dict[str, Any]: Performance istatistikleri
        """
        stats = self.performance_stats.copy()
        
        # Calculate averages
        if stats['gpu_processing_times']:
            stats['avg_gpu_time'] = sum(stats['gpu_processing_times']) / len(stats['gpu_processing_times'])
        else:
            stats['avg_gpu_time'] = 0.0
            
        if stats['cpu_processing_times']:
            stats['avg_cpu_time'] = sum(stats['cpu_processing_times']) / len(stats['cpu_processing_times'])
        else:
            stats['avg_cpu_time'] = 0.0
        
        # Add memory usage
        stats['memory_usage'] = self.gpu_processor.get_memory_usage()
        
        return stats
    
    def cleanup(self):
        """
        Kaynakları temizle
        """
        logger.info("GPU-Enhanced Embedding Job cleanup başlatılıyor...")
        
        # GPU processor cleanup
        if hasattr(self, 'gpu_processor'):
            self.gpu_processor.cleanup()
        
        # Parent cleanup
        super().cleanup()
        
        logger.info("✅ GPU-Enhanced Embedding Job cleanup tamamlandı")