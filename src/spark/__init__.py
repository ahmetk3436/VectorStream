#!/usr/bin/env python3
"""
Spark Integration Module

Bu modül NewMind AI sisteminin Spark entegrasyonunu sağlar.
Dağıtık embedding işleme, batch processing ve streaming için
gerekli sınıfları içerir.
"""

from .embedding_job import SparkEmbeddingJob
from .batch_processor import SparkBatchProcessor
from .kafka_spark_connector import KafkaSparkConnector
from .spark_cli import SparkCLI

__all__ = [
    'SparkEmbeddingJob',
    'SparkBatchProcessor', 
    'KafkaSparkConnector',
    'SparkCLI'
]

# Version info
__version__ = '1.0.0'
__author__ = 'NewMind AI Team'
__description__ = 'Spark integration for distributed embedding processing'