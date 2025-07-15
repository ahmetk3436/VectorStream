"""Application configuration management."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load application configuration from YAML file or environment variables.
    
    Args:
        config_path: Path to configuration file. If None, uses default locations.
        
    Returns:
        Configuration dictionary
    """
    config = {}
    
    default_config = {
        'spark': {
            'app_name': os.getenv('SPARK_APP_NAME', 'VectorStream-MLOps-Pipeline'),
            'master': os.getenv('SPARK_MASTER', 'local[*]'),
            'batch_interval': os.getenv('SPARK_BATCH_INTERVAL', '5 seconds'),
            'trigger_interval': os.getenv('SPARK_TRIGGER_INTERVAL', '500 milliseconds'),
            'max_offsets_per_trigger': int(os.getenv('SPARK_MAX_OFFSETS_PER_TRIGGER', '100000')),
            'checkpoint_location': os.getenv('SPARK_CHECKPOINT_LOCATION', '/tmp/spark-checkpoint-vectorstream'),
            'config': {
                'spark.sql.adaptive.enabled': os.getenv('SPARK_SQL_ADAPTIVE_ENABLED', 'true'),
                'spark.sql.adaptive.coalescePartitions.enabled': os.getenv('SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED', 'true'),
                'spark.sql.shuffle.partitions': os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '200'),
                'spark.executor.memory': os.getenv('SPARK_EXECUTOR_MEMORY', '8g'),
                'spark.driver.memory': os.getenv('SPARK_DRIVER_MEMORY', '4g'),
                'spark.executor.cores': os.getenv('SPARK_EXECUTOR_CORES', '8'),
                'spark.dynamicAllocation.enabled': os.getenv('SPARK_DYNAMIC_ALLOCATION_ENABLED', 'true'),
                'spark.dynamicAllocation.maxExecutors': os.getenv('SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS', '16'),
                'spark.serializer': os.getenv('SPARK_SERIALIZER', 'org.apache.spark.serializer.KryoSerializer')
            }
        },
        'embedding': {
            'model_name': os.getenv('EMBEDDING_MODEL_NAME', 'sentence-transformers/all-MiniLM-L6-v2'),
            'batch_size': int(os.getenv('EMBEDDING_BATCH_SIZE', '256')),
            'max_length': int(os.getenv('EMBEDDING_MAX_LENGTH', '512')),
            'device': os.getenv('EMBEDDING_DEVICE', 'cpu'),
            'normalize_embeddings': os.getenv('EMBEDDING_NORMALIZE', 'true').lower() == 'true',
            'use_fast_tokenizer': os.getenv('EMBEDDING_USE_FAST_TOKENIZER', 'true').lower() == 'true',
            'enable_caching': os.getenv('EMBEDDING_ENABLE_CACHING', 'true').lower() == 'true'
        },
        'qdrant': {
            'host': os.getenv('QDRANT_HOST', 'localhost'),
            'port': int(os.getenv('QDRANT_PORT', '6333')),
            'collection_name': os.getenv('QDRANT_COLLECTION_NAME', 'ecommerce_embeddings'),
            'vector_size': int(os.getenv('QDRANT_VECTOR_SIZE', '384')),
            'distance_metric': os.getenv('QDRANT_DISTANCE_METRIC', 'Cosine'),
            'api_key': os.getenv('QDRANT_API_KEY'),
            'timeout': int(os.getenv('QDRANT_TIMEOUT', '30'))
        },
        'kafka': {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'topic': os.getenv('KAFKA_TOPIC', 'ecommerce-events'),
            'group_id': os.getenv('KAFKA_GROUP_ID', 'vectorstream-consumer'),
            'auto_offset_reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
            'batch_size': int(os.getenv('KAFKA_BATCH_SIZE', '100')),
            'max_poll_records': int(os.getenv('KAFKA_MAX_POLL_RECORDS', '500')),
            'consumer_timeout_ms': int(os.getenv('KAFKA_CONSUMER_TIMEOUT_MS', '1000')),
            'enable_auto_commit': os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'true').lower() == 'true',
            'partitions': int(os.getenv('KAFKA_PARTITIONS', '16')),
            'replication_factor': int(os.getenv('KAFKA_REPLICATION_FACTOR', '1')),
            'auto_create_topic': os.getenv('KAFKA_AUTO_CREATE_TOPIC', 'true').lower() == 'true',
            'auto_configure_partitions': os.getenv('KAFKA_AUTO_CONFIGURE_PARTITIONS', 'true').lower() == 'true'
        },
        'api': {
            'host': os.getenv('API_HOST', '127.0.0.1'),
            'port': int(os.getenv('API_PORT', '8080'))
        },
        'paths': {
            'input_dir': os.getenv('DATA_INPUT_DIR', 'data/input'),
            'output_dir': os.getenv('DATA_OUTPUT_DIR', 'data/output'),
            'checkpoint_dir': os.getenv('DATA_CHECKPOINT_DIR', 'data/checkpoints'),
            'processed_dir': os.getenv('DATA_PROCESSED_DIR', 'data/processed'),
            'failed_dir': os.getenv('DATA_FAILED_DIR', 'data/failed')
        },
        'logging': {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'format': os.getenv('LOG_FORMAT', 'json'),
            'file_path': os.getenv('LOG_FILE_PATH', 'logs/vectorstream.log')
        },
        'performance': {
            'target_events_per_sec': int(os.getenv('PERFORMANCE_TARGET_EVENTS_PER_SEC', '1000')),
            'max_latency_seconds': int(os.getenv('PERFORMANCE_MAX_LATENCY_SECONDS', '30')),
            'batch_size': int(os.getenv('PERFORMANCE_BATCH_SIZE', '256'))
        },
        'rapids': {
            'enabled': os.getenv('RAPIDS_ENABLED', 'false').lower() == 'true',
            'gpu_memory_fraction': float(os.getenv('RAPIDS_GPU_MEMORY_FRACTION', '0.8')),
            'pool_size': int(os.getenv('RAPIDS_POOL_SIZE', '2048'))
        },
        'security': {
            'enable_ssl': os.getenv('SECURITY_ENABLE_SSL', 'false').lower() == 'true',
            'ssl_cert_path': os.getenv('SECURITY_SSL_CERT_PATH'),
            'ssl_key_path': os.getenv('SECURITY_SSL_KEY_PATH')
        }
    }
    
    config.update(default_config)
    
    if config_path:
        config_file = Path(config_path)
    else:
        possible_paths = [
            Path('config.yaml'),
            Path('config/config.yaml'),
            Path('src/config/config.yaml'),
            Path('config/app_config.yaml')
        ]
        config_file = None
        for path in possible_paths:
            if path.exists():
                config_file = path
                break
    
    if config_file and config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = yaml.safe_load(f)
                if file_config:
                    config.update(file_config)
                    logger.info(f"Configuration loaded from {config_file}")
        except Exception as e:
            logger.warning(f"Failed to load config from {config_file}: {e}")
    
    env_overrides = {
        'SPARK_MASTER': ('spark', 'master'),
        'QDRANT_HOST': ('qdrant', 'host'),
        'QDRANT_PORT': ('qdrant', 'port'),
        'KAFKA_BOOTSTRAP_SERVERS': ('kafka', 'bootstrap_servers'),
        'EMBEDDING_MODEL': ('embedding', 'model_name'),
        'LOG_LEVEL': ('logging', 'level')
    }
    
    for env_var, (section, key) in env_overrides.items():
        value = os.getenv(env_var)
        if value:
            if section not in config:
                config[section] = {}
            if key == 'port':
                try:
                    value = int(value)
                except ValueError:
                    logger.warning(f"Invalid port value in {env_var}: {value}")
                    continue
            config[section][key] = value
            logger.info(f"Configuration override from {env_var}: {section}.{key} = {value}")
    
    return config


def get_spark_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract Spark configuration from main config."""
    return config.get('spark', {})


def get_qdrant_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract Qdrant configuration from main config."""
    return config.get('qdrant', {})


def get_kafka_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract Kafka configuration from main config."""
    return config.get('kafka', {})


def get_embedding_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract embedding configuration from main config."""
    return config.get('embedding', {})


def get_paths_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract paths configuration from main config."""
    return config.get('paths', {})