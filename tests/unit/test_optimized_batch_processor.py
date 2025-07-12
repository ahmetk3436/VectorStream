#!/usr/bin/env python3

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

import sys
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.spark.optimized_batch_processor import (
    OptimizedSparkBatchProcessor, 
    BatchConfig, 
    ProcessingMetrics
)
from src.exceptions.embedding_exceptions import EmbeddingProcessingError

class TestBatchConfig:
    """BatchConfig sınıfı testleri"""
    
    def test_batch_config_default_values(self):
        """Varsayılan değerleri test et"""
        config = BatchConfig()
        
        assert config.min_batch_size == 1000
        assert config.max_batch_size == 50000
        assert config.adaptive_batch_size == True
        assert config.max_parallel_files == 4
        assert config.enable_parallel_processing == True
        assert config.enable_caching == True
        assert config.cache_storage_level == "MEMORY_AND_DISK_SER"
        assert config.enable_compression == True
        assert config.compression_codec == "snappy"
        assert config.enable_metrics == True
        assert config.max_memory_usage_percent == 80.0
        assert config.memory_check_interval == 100
        assert config.metrics_interval == 60
        assert config.checkpoint_interval == 1000
        assert config.enable_incremental_checkpoints == True
    
    def test_batch_config_custom_values(self):
        """Özel değerleri test et"""
        config = BatchConfig(
            min_batch_size=50,
            max_batch_size=10000,
            adaptive_batch_size=False,
            max_parallel_files=8,
            enable_caching=False,
            enable_compression=False,
            compression_codec="snappy",
            enable_metrics=False,
            max_memory_usage_percent=70.0,
            memory_check_interval=10,
            enable_incremental_checkpoints=False
        )
        
        assert config.min_batch_size == 50
        assert config.max_batch_size == 10000
        assert config.adaptive_batch_size == False
        assert config.max_parallel_files == 8
        assert config.enable_caching == False
        assert config.enable_compression == False
        assert config.compression_codec == "snappy"
        assert config.enable_metrics == False
        assert config.max_memory_usage_percent == 70.0
        assert config.memory_check_interval == 10
        assert config.enable_incremental_checkpoints == False

class TestProcessingMetrics:
    """ProcessingMetrics sınıfı testleri"""
    
    def test_processing_metrics_initialization(self):
        """Metrics başlatma testi"""
        start_time = datetime.now()
        metrics = ProcessingMetrics(start_time=start_time)
        
        assert metrics.start_time == start_time
        assert metrics.end_time is None
        assert metrics.total_files == 0
        assert metrics.processed_files == 0
        assert metrics.failed_files == 0
        assert metrics.total_records == 0
        assert metrics.processed_records == 0
        assert metrics.failed_records == 0
        assert metrics.avg_processing_time_per_file == 0.0
        assert metrics.avg_processing_time_per_record == 0.0
        assert metrics.memory_usage_peak == 0.0
        assert metrics.cpu_usage_avg == 0.0
        assert metrics.throughput_records_per_second == 0.0

class TestOptimizedSparkBatchProcessor:
    """OptimizedSparkBatchProcessor sınıfı testleri"""
    
    @pytest.fixture
    def temp_dirs(self):
        """Geçici dizinler oluştur"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            input_path = temp_path / "input"
            output_path = temp_path / "output"
            checkpoint_path = temp_path / "checkpoints"
            failed_path = temp_path / "failed"
            metrics_path = temp_path / "metrics"
            
            input_path.mkdir()
            output_path.mkdir()
            checkpoint_path.mkdir()
            failed_path.mkdir()
            metrics_path.mkdir()
            
            yield {
                'input_path': str(input_path),
                'output_path': str(output_path),
                'checkpoint_path': str(checkpoint_path),
                'failed_path': str(failed_path),
                'metrics_path': str(metrics_path)
            }
    
    @pytest.fixture
    def batch_config(self):
        """Test batch config"""
        return BatchConfig(
            min_batch_size=10,
            max_batch_size=100,
            adaptive_batch_size=True,
            max_parallel_files=2,
            enable_caching=True,
            enable_compression=False,
            enable_metrics=True
        )
    
    @pytest.fixture
    def qdrant_config(self):
        """Test Qdrant config"""
        return {
            'host': 'localhost',
            'port': 6333,
            'collection_name': 'test_collection'
        }
    
    @pytest.fixture
    def mock_embedding_job(self):
        """Mock embedding job"""
        mock_job = Mock()
        mock_job.spark = Mock()
        mock_job.process_dataframe = Mock()
        mock_job.write_to_qdrant = Mock()
        mock_job.stop = Mock()
        return mock_job
    
    @patch('src.spark.optimized_batch_processor.SparkEmbeddingJob')
    def test_processor_initialization(self, mock_job_class, temp_dirs, batch_config, qdrant_config):
        """Processor başlatma testi"""
        mock_job_class.return_value = Mock()
        
        config = {
            'spark': {'app_name': 'test', 'master': 'local[1]'},
            'qdrant': qdrant_config,
            'batch': {
                'input_path': temp_dirs['input_path'],
                'output_path': temp_dirs['output_path'],
                'checkpoint_path': temp_dirs.get('checkpoint_path', temp_dirs['output_path']),
                'failed_path': temp_dirs.get('failed_path', temp_dirs['output_path'])
            }
        }
        
        processor = OptimizedSparkBatchProcessor(config)
        
        assert processor.input_path == temp_dirs['input_path']
        assert processor.output_path == temp_dirs['output_path']
        assert processor.qdrant_config == qdrant_config
        assert isinstance(processor.batch_config, BatchConfig)
        assert processor.current_batch_size == processor.batch_config.min_batch_size
        assert processor.current_metrics is None
        assert processor.stop_monitoring.is_set() == False
    
    def test_adaptive_batch_size_adjustment(self, temp_dirs, batch_config, qdrant_config):
        """Adaptive batch size ayarlama testi"""
        with patch('src.spark.optimized_batch_processor.SparkEmbeddingJob'):
            config = {
                'spark': {'app_name': 'test', 'master': 'local[1]'},
                'qdrant': qdrant_config,
                'batch': {
                    'input_path': temp_dirs['input_path'],
                    'output_path': temp_dirs['output_path'],
                    'checkpoint_path': temp_dirs.get('checkpoint_path', temp_dirs['output_path']),
                    'failed_path': temp_dirs.get('failed_path', temp_dirs['output_path'])
                }
            }
            processor = OptimizedSparkBatchProcessor(config)
            
            # Memory usage düşük - batch size artırılmalı
            processor._adjust_batch_size(memory_percent=30.0, cpu_percent=20.0)
            assert processor.current_batch_size > processor.batch_config.min_batch_size
            
            # Memory usage yüksek - batch size azaltılmalı
            processor._adjust_batch_size(memory_percent=85.0, cpu_percent=70.0)
            assert processor.current_batch_size <= processor.batch_config.max_batch_size
    
    def test_create_sample_data_file(self, temp_dirs):
        """Test verisi oluştur"""
        input_path = Path(temp_dirs['input_path'])
        test_file = input_path / "test_data.json"
        
        # Test verisi oluştur
        test_data = [
            {
                'id': 'doc_1',
                'content': 'Bu bir test metnidir.',
                'timestamp': datetime.now().isoformat()
            },
            {
                'id': 'doc_2', 
                'content': 'Bu ikinci test metnidir.',
                'timestamp': datetime.now().isoformat()
            }
        ]
        
        with open(test_file, 'w') as f:
            for item in test_data:
                f.write(json.dumps(item) + '\n')
        
        # Dosyanın oluşturulduğunu kontrol et
        assert test_file.exists()
        assert test_file.suffix == '.json'
    
    @patch('src.spark.optimized_batch_processor.SparkEmbeddingJob')
    def test_get_sorted_input_files(self, mock_job_class, temp_dirs, batch_config, qdrant_config):
        """Dosya sıralama testi"""
        mock_job_class.return_value = Mock()
        
        config = {
            'spark': {'app_name': 'test', 'master': 'local[1]'},
            'qdrant': qdrant_config,
            'batch': {
                'input_path': temp_dirs['input_path'],
                'output_path': temp_dirs['output_path'],
                'checkpoint_path': temp_dirs.get('checkpoint_path', temp_dirs['output_path']),
                'failed_path': temp_dirs.get('failed_path', temp_dirs['output_path'])
            }
        }
        
        processor = OptimizedSparkBatchProcessor(config)
        
        # Test dosyaları oluştur
        input_path = Path(temp_dirs['input_path'])
        
        # Küçük dosya
        small_file = input_path / "small.json"
        with open(small_file, 'w') as f:
            f.write('{"content": "small"}')
        
        # Büyük dosya
        large_file = input_path / "large.json"
        with open(large_file, 'w') as f:
            f.write('{"content": "' + 'x' * 1000 + '"}')
        
        # Dosyaları sırala
        sorted_files = processor._get_sorted_input_files("*.json")
        
        assert len(sorted_files) == 2
        # Küçük dosya önce gelmeli
        assert sorted_files[0][0].name == "small.json"
        assert sorted_files[1][0].name == "large.json"
    
    @patch('src.spark.optimized_batch_processor.SparkEmbeddingJob')
    def test_performance_report(self, mock_job_class, temp_dirs, batch_config, qdrant_config):
        """Performance raporu testi"""
        mock_job_class.return_value = Mock()
        
        config = {
            'spark': {'app_name': 'test', 'master': 'local[1]'},
            'qdrant': qdrant_config,
            'batch': {
                'input_path': temp_dirs['input_path'],
                'output_path': temp_dirs['output_path'],
                'checkpoint_path': temp_dirs.get('checkpoint_path', temp_dirs['output_path']),
                'failed_path': temp_dirs.get('failed_path', temp_dirs['output_path'])
            }
        }
        
        processor = OptimizedSparkBatchProcessor(config)
        
        # Performance raporu al (metrics dosyası olmadığı için no_metrics durumu bekleniyor)
        report = processor.get_performance_report()
        
        # Metrics dosyası yoksa no_metrics durumu döner
        if report.get('status') == 'no_metrics':
            assert 'message' in report
            assert report['message'] == 'Henüz metrics verisi yok'
        else:
            # Eğer metrics varsa bu alanları kontrol et
            assert 'batch_config' in report
            assert 'latest_metrics' in report
            assert 'system_info' in report
            assert 'stats' in report
    
    @patch('src.spark.optimized_batch_processor.SparkEmbeddingJob')
    def test_stop_processor(self, mock_job_class, temp_dirs, batch_config, qdrant_config):
        """Processor durdurma testi"""
        mock_embedding_job = Mock()
        mock_job_class.return_value = mock_embedding_job
        
        config = {
            'spark': {'app_name': 'test', 'master': 'local[1]'},
            'qdrant': qdrant_config,
            'batch': {
                'input_path': temp_dirs['input_path'],
                'output_path': temp_dirs['output_path'],
                'checkpoint_path': temp_dirs.get('checkpoint_path', temp_dirs['output_path']),
                'failed_path': temp_dirs.get('failed_path', temp_dirs['output_path'])
            }
        }
        
        processor = OptimizedSparkBatchProcessor(config)
        
        # Processor'ı durdur
        processor.stop()
        
        # Monitoring durdurulmalı
        assert processor.stop_monitoring.is_set() == True
        
        # Embedding job durdurulmalı
        mock_embedding_job.stop.assert_called_once()

if __name__ == '__main__':
    pytest.main([__file__, '-v'])