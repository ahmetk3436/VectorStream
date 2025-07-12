"""Application configuration management."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load application configuration from YAML file or environment variables.
    
    Args:
        config_path: Path to configuration file. If None, uses default locations.
        
    Returns:
        Configuration dictionary
    """
    config = {}
    
    # Default configuration
    default_config = {
        'spark': {
            'app_name': 'NewMindAI-Spark',
            'master': 'local[*]',
            'config': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
            }
        },
        'embedding': {
            'model_name': 'sentence-transformers/all-MiniLM-L6-v2',
            'batch_size': 32,
            'max_length': 512
        },
        'qdrant': {
            'host': 'localhost',
            'port': 6333,
            'collection_name': 'embeddings',
            'vector_size': 384
        },
        'kafka': {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'embeddings',
            'group_id': 'newmind-ai'
        },
        'paths': {
            'input_dir': 'data/input',
            'output_dir': 'data/output',
            'checkpoint_dir': 'data/checkpoints',
            'processed_dir': 'data/processed',
            'failed_dir': 'data/failed'
        },
        'logging': {
            'level': 'INFO'
        }
    }
    
    config.update(default_config)
    
    # Try to load from config file
    if config_path:
        config_file = Path(config_path)
    else:
        # Try default locations
        possible_paths = [
            Path('config.yaml'),
            Path('config/config.yaml'),
            Path('src/config/config.yaml')
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
    
    # Override with environment variables
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
            # Convert port to int if it's a port
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