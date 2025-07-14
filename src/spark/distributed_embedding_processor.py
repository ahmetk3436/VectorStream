#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Distributed Embedding Processor
Performans için executor-based embedding processing

Bu modül kullanıcının önerdiği çözümü uygular:
- foreachPartition ile executor'da embedding hesaplama
- Driver darboğazını ortadan kaldırma
- Paralel embedding processing
"""

from typing import Iterator, Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import pandas_udf, col, concat_ws, udf, current_timestamp, lit
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField, StringType
import pandas as pd
from loguru import logger


class DistributedEmbeddingProcessor:
    """
    Dağıtık embedding işleme sınıfı
    Executor'larda paralel embedding hesaplama yapar
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_name = config.get('embedding', {}).get('model_name', 'all-MiniLM-L6-v2')
        self.batch_size = config.get('embedding', {}).get('batch_size', 5000)
        
    def process_with_pandas_udf(self, df: DataFrame) -> DataFrame:
        """
        Pandas UDF kullanarak vectorized embedding processing
        Bu yaklaşım Arrow ile optimize edilmiştir
        """
        
        @pandas_udf(ArrayType(FloatType()))
        def create_embeddings_vectorized(texts: pd.Series) -> pd.Series:
            """
            Vectorized embedding creation using Pandas UDF
            Bu fonksiyon executor'larda çalışır
            """
            try:
                # Model'i lazy loading ile yükle
                from sentence_transformers import SentenceTransformer
                model = SentenceTransformer(self.model_name)
                
                # Batch processing
                text_list = texts.fillna("").tolist()
                if not text_list:
                    return pd.Series([[0.0] * 384] * len(texts))
                
                # Embeddings'i batch olarak hesapla
                embeddings = model.encode(
                    text_list,
                    batch_size=self.batch_size,
                    show_progress_bar=False,
                    convert_to_tensor=False
                )
                
                # Pandas Series olarak döndür
                return pd.Series([emb.tolist() for emb in embeddings])
                
            except Exception as e:
                logger.error(f"Embedding processing error: {e}")
                # Fallback: dummy embeddings
                return pd.Series([[0.0] * 384] * len(texts))
        
        # Content column'unu oluştur
        content_df = df.withColumn(
            "content",
            concat_ws(
                " ",
                col("event_type"),
                col("product.name"),
                col("product.description"),
                col("product.category"),
                col("search_query"),
            )
        )
        
        # Embeddings'i hesapla
        result_df = content_df.withColumn(
            "embedding",
            create_embeddings_vectorized(col("content"))
        )
        
        return result_df
    
    def process_with_foreach_partition(self, df: DataFrame, qdrant_config: Dict[str, Any]) -> None:
        """
        foreachPartition kullanarak executor'da embedding ve yazma işlemi
        Bu yaklaşım driver darboğazını tamamen ortadan kaldırır
        """
        
        def process_partition(partition_iter: Iterator[pd.DataFrame]) -> None:
            """
            Her partition'ı işle - executor'da çalışır
            """
            try:
                # Lazy imports - executor'da yükle
                from sentence_transformers import SentenceTransformer
                from src.core.qdrant_writer import QdrantWriter
                import asyncio
                
                # Model ve client'ı partition başında yükle
                model = SentenceTransformer(self.model_name)
                qdrant_client = QdrantWriter(qdrant_config)
                
                # Event loop oluştur
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                for pdf in partition_iter:
                    if pdf.empty:
                        continue
                    
                    # Content oluştur
                    pdf['content'] = (
                        pdf['event_type'].fillna('') + ' ' +
                        pdf['product.name'].fillna('') + ' ' +
                        pdf['product.description'].fillna('') + ' ' +
                        pdf['product.category'].fillna('')
                    ).str.strip()
                    
                    # Batch embedding
                    texts = pdf['content'].tolist()
                    embeddings = model.encode(
                        texts,
                        batch_size=self.batch_size,
                        show_progress_bar=False,
                        convert_to_tensor=False
                    )
                    
                    # Qdrant data hazırla
                    embeddings_data = []
                    for idx, (_, row) in enumerate(pdf.iterrows()):
                        embeddings_data.append({
                            "id": row.get('event_id', f"auto_{idx}"),
                            "vector": embeddings[idx].tolist(),
                            "payload": {
                                "event_id": row.get('event_id'),
                                "event_type": row.get('event_type'),
                                "user_id": row.get('user_id'),
                                "session_id": row.get('session_id'),
                                "timestamp": row.get('timestamp'),
                                "product_name": row.get('product.name'),
                                "product_description": row.get('product.description'),
                                "product_category": row.get('product.category'),
                                "content": row.get('content'),
                                "embedding_model": self.model_name,
                                "processed_by": "executor"
                            }
                        })
                    
                    # Async write to Qdrant
                    async def write_batch():
                        await qdrant_client.write_embeddings(embeddings_data, batch_size=2000)
                    
                    loop.run_until_complete(write_batch())
                    logger.info(f"✅ Executor: {len(embeddings_data)} embeddings processed and written")
                    
            except Exception as e:
                logger.error(f"Partition processing error: {e}")
                raise
        
        # mapInPandas kullanarak distributed processing
        schema = StructType([
            StructField("status", StringType(), True)
        ])
        
        def map_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            process_partition(iterator)
            yield pd.DataFrame([{"status": "completed"}])
        
        # Execute the distributed processing
        result_df = df.mapInPandas(map_partition, schema)
        result_df.collect()  # Trigger execution
        
        logger.info("✅ Distributed embedding processing completed")


def create_optimized_embedding_pipeline(df: DataFrame, config: Dict[str, Any], qdrant_config: Dict[str, Any]) -> DataFrame:
    """
    Optimize edilmiş embedding pipeline oluştur
    
    Args:
        df: Input DataFrame
        config: System configuration
        qdrant_config: Qdrant configuration
    
    Returns:
        Processed DataFrame with embeddings
    """
    processor = DistributedEmbeddingProcessor(config)
    
    # Pandas UDF ile vectorized processing
    return processor.process_with_pandas_udf(df)


def execute_distributed_processing(df: DataFrame, config: Dict[str, Any], qdrant_config: Dict[str, Any]) -> None:
    """
    Tamamen dağıtık processing - driver darboğazı yok
    
    Args:
        df: Input DataFrame
        config: System configuration
        qdrant_config: Qdrant configuration
    """
    processor = DistributedEmbeddingProcessor(config)
    processor.process_with_foreach_partition(df, qdrant_config)