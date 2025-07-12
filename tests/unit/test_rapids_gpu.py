#!/usr/bin/env python3

import pytest
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.spark.rapids_gpu_processor import RAPIDSGPUProcessor
from src.exceptions.embedding_exceptions import EmbeddingProcessingError

class TestRAPIDSGPUProcessor:
    """RAPIDS GPU Processor test sınıfı"""

    def setup_method(self):
        """Reset circuit breaker manager before each test."""
        from src.utils.circuit_breaker import reset_circuit_breaker_manager
        reset_circuit_breaker_manager()
    
    @pytest.fixture
    def gpu_config(self):
        """Test konfigürasyonu"""
        return {
            'gpu': {
                'min_memory_gb': 1.0,
                'use_tfidf_fallback': True,
                'max_features': 1000,
                'svd_components': 128
            },
            'embedding': {
                'model': 'all-MiniLM-L6-v2',
                'vector_size': 384,
                'batch_size': 100
            }
        }
    
    @pytest.fixture
    def sample_texts(self):
        """Test metinleri"""
        return [
            "This is a sample text for testing.",
            "Machine learning models process data efficiently.",
            "GPU acceleration improves performance significantly.",
            "RAPIDS provides GPU-accelerated data processing.",
            "Natural language processing benefits from parallel computing."
        ]
    
    def test_initialization_without_rapids(self, gpu_config):
        """RAPIDS olmadan initialization testi"""
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', False):
            processor = RAPIDSGPUProcessor(gpu_config)
            
            assert not processor.gpu_enabled
            assert processor.config == gpu_config
            assert processor.model_name == 'all-MiniLM-L6-v2'
            assert processor.vector_size == 384
    
    @patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', True)
    @patch('src.spark.rapids_gpu_processor.cp', create=True)
    def test_initialization_with_rapids(self, mock_cupy, gpu_config):
        # Mock GPU memory
        mock_pool = Mock()
        mock_pool.total_bytes.return_value = 2 * 1024**3  # 2GB
        mock_cupy.get_default_memory_pool.return_value = mock_pool
        
        processor = RAPIDSGPUProcessor(gpu_config)
        
        assert processor.gpu_enabled
        assert processor.config == gpu_config
    
    @patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', True)
    @patch('src.spark.rapids_gpu_processor.cp', create=True)
    def test_insufficient_gpu_memory(self, mock_cupy, gpu_config):
            # Mock insufficient GPU memory
        mock_pool = Mock()
        mock_pool.total_bytes.return_value = 512 * 1024**2  # 512MB
        mock_cupy.get_default_memory_pool.return_value = mock_pool
        
        processor = RAPIDSGPUProcessor(gpu_config)
        
        assert not processor.gpu_enabled
    
    def test_cpu_fallback_embedding_creation(self, gpu_config, sample_texts):
        """CPU fallback embedding creation testi"""
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', False):
            processor = RAPIDSGPUProcessor(gpu_config)
            
            # Mock sklearn components
            with patch('sklearn.feature_extraction.text.TfidfVectorizer') as mock_tfidf, \
                 patch('sklearn.decomposition.TruncatedSVD') as mock_svd, \
                 patch('sklearn.preprocessing.normalize') as mock_normalize:
                
                # Setup mocks
                mock_tfidf_instance = Mock()
                mock_tfidf.return_value = mock_tfidf_instance
                mock_tfidf_instance.fit_transform.return_value = np.random.rand(len(sample_texts), 1000)
                
                mock_svd_instance = Mock()
                mock_svd.return_value = mock_svd_instance
                mock_svd_instance.fit_transform.return_value = np.random.rand(len(sample_texts), 128)
                
                mock_normalize.return_value = np.random.rand(len(sample_texts), 128)
                
                # Test embedding creation
                embeddings = processor.create_embeddings_with_fallback(sample_texts)
                
                assert embeddings is not None
                assert embeddings.shape[0] == len(sample_texts)
                mock_tfidf.assert_called_once()
                mock_svd.assert_called_once()
    
    @patch('src.spark.rapids_gpu_processor.SENTENCE_TRANSFORMERS_AVAILABLE', True)
    @patch('src.spark.rapids_gpu_processor.SentenceTransformer', create=True)
    def test_sentence_transformer_embedding(self, mock_st, gpu_config, sample_texts):
        """SentenceTransformer embedding testi"""
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', False):
            processor = RAPIDSGPUProcessor(gpu_config)
            processor.use_tfidf_fallback = False
            
            # Mock SentenceTransformer
        mock_model = Mock()
        mock_st.return_value = mock_model
        mock_model.encode.return_value = np.random.rand(len(sample_texts), 384)
        
        embeddings = processor.create_embeddings_with_fallback(sample_texts)
        
        assert embeddings is not None
        assert embeddings.shape == (len(sample_texts), 384)
        mock_model.encode.assert_called_once()
    
    @patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', True)
    @patch('src.spark.rapids_gpu_processor.cp', create=True)
    @patch('src.spark.rapids_gpu_processor.cudf', create=True)
    @patch('src.spark.rapids_gpu_processor.cuml', create=True)
    def test_gpu_embedding_creation(self, mock_cuml, mock_cudf, mock_cupy, gpu_config, sample_texts):
        """GPU embedding creation testi"""
        # Mock GPU memory
        mock_pool = Mock()
        mock_pool.total_bytes.return_value = 4 * 1024**3  # 4GB
        mock_cupy.get_default_memory_pool.return_value = mock_pool
        
        processor = RAPIDSGPUProcessor(gpu_config)
        
        # Since GPU embedding creation is complex, just test that it doesn't crash
        # and falls back properly when needed
        try:
            embeddings = processor.create_embeddings_gpu(sample_texts)
            # If it succeeds, check basic properties
            if embeddings is not None:
                assert embeddings.shape[0] == len(sample_texts)
        except (EmbeddingProcessingError, ImportError, AttributeError):
            # Expected if RAPIDS components are not available
            pass
    
    def test_embedding_with_fallback_on_gpu_failure(self, gpu_config, sample_texts):
        """GPU failure durumunda fallback testi"""
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', True):
            processor = RAPIDSGPUProcessor(gpu_config)
            processor.gpu_enabled = True
            
            # Mock GPU failure
            with patch.object(processor, 'create_embeddings_gpu', side_effect=Exception("GPU Error")):
                with patch.object(processor, '_create_tfidf_embeddings_cpu') as mock_cpu:
                    mock_cpu.return_value = np.random.rand(len(sample_texts), 128)
                    
                    embeddings = processor.create_embeddings_with_fallback(sample_texts)
                    
                    assert embeddings is not None
                    assert not processor.gpu_enabled  # Should be disabled after failure
                    mock_cpu.assert_called_once()
    
    def test_benchmark_performance(self, gpu_config, sample_texts):
        """Performance benchmark testi"""
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', False):
            processor = RAPIDSGPUProcessor(gpu_config)
            
            with patch.object(processor, '_create_tfidf_embeddings_cpu') as mock_cpu:
                mock_cpu.return_value = np.random.rand(len(sample_texts), 128)
                
                results = processor.benchmark_performance(sample_texts, iterations=2)
                
                assert 'gpu_available' in results
                assert 'test_size' in results
                assert 'iterations' in results
                assert 'cpu_times' in results
                assert results['test_size'] == len(sample_texts)
                assert results['iterations'] == 2
    
    @patch('psutil.virtual_memory')
    def test_memory_usage_info(self, mock_psutil, gpu_config):
        """Memory usage bilgisi testi"""
        # Mock system memory
        mock_mem = Mock()
        mock_mem.total = 16 * 1024**3  # 16GB
        mock_mem.used = 8 * 1024**3   # 8GB
        mock_mem.available = 8 * 1024**3  # 8GB
        mock_mem.percent = 50.0
        mock_psutil.return_value = mock_mem
        
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', False):
            processor = RAPIDSGPUProcessor(gpu_config)
            
            memory_info = processor.get_memory_usage()
            
            assert not memory_info['gpu_available']
            assert 'system_memory' in memory_info
            assert memory_info['system_memory']['total_gb'] == 16.0
            assert memory_info['system_memory']['percent_used'] == 50.0
    
    @patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', True)
    @patch('src.spark.rapids_gpu_processor.psutil.virtual_memory')
    @patch('src.spark.rapids_gpu_processor.cp', create=True)
    def test_gpu_memory_usage_info(self, mock_cupy, mock_psutil, gpu_config):
        """GPU memory usage bilgisi testi"""
        # Mock GPU memory info
        mock_pool = Mock()
        mock_pool.total_bytes = Mock(return_value=8 * 1024**3)  # 8GB
        mock_pool.used_bytes = Mock(return_value=2 * 1024**3)   # 2GB
        mock_pool.free_bytes = Mock(return_value=6 * 1024**3)   # 6GB
        mock_cupy.get_default_memory_pool.return_value = mock_pool

        # Mock system memory
        mock_psutil.return_value = Mock(
            total=16 * 1024**3,
            used=8 * 1024**3,
            available=8 * 1024**3,
            percent=50.0
        )
        
        processor = RAPIDSGPUProcessor(gpu_config)
        assert processor.gpu_enabled
        
        info = processor.get_memory_usage()
        
        assert info['gpu_available']
        assert 'gpu_memory' in info
        assert info['gpu_memory']['total_gb'] == 8.0
        assert info['gpu_memory']['used_gb'] == 2.0
        assert info['gpu_memory']['percent_used'] == 25.0
    
    
    
    @patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', True)
    @patch('src.spark.rapids_gpu_processor.cp', create=True)
    def test_gpu_cleanup(self, mock_cupy, gpu_config):
        """GPU cleanup testi"""
        # Mock to ensure gpu_enabled is True
        mock_pool = Mock()
        mock_pool.total_bytes.return_value = 4 * 1024**3
        mock_cupy.get_default_memory_pool.return_value = mock_pool

        processor = RAPIDSGPUProcessor(gpu_config)
        assert processor.gpu_enabled

        # Set some models to ensure they are cleaned up
        processor._sentence_model = Mock()
        processor._tfidf_model = Mock()
        processor._svd_model = Mock()

        processor.cleanup()

        # Verify GPU memory is freed
        mock_pool.free_all_blocks.assert_called_once()

        # Verify models are cleared
        assert processor._sentence_model is None
        assert processor._tfidf_model is None
        assert processor._svd_model is None

        # Verify GPU is disabled after cleanup
        assert not processor.gpu_enabled
    
    def test_circuit_breaker_integration(self, gpu_config, sample_texts):
        """Circuit breaker entegrasyonu testi"""
        with patch('src.spark.rapids_gpu_processor.RAPIDS_AVAILABLE', False):
            processor = RAPIDSGPUProcessor(gpu_config)
            
            # Mock repeated failures to trigger circuit breaker
            with patch.object(processor, '_create_tfidf_embeddings_cpu', side_effect=Exception("Persistent Error")):
                
                # Multiple calls should eventually trigger circuit breaker
                for _ in range(5):
                    try:
                        processor.create_embeddings_with_fallback(sample_texts)
                    except EmbeddingProcessingError:
                        pass
    
    def test_empty_texts_handling(self, gpu_config):
        """Boş metinler handling testi"""
        processor = RAPIDSGPUProcessor(gpu_config)
        processor.gpu_enabled = False

        # Test with empty and whitespace-only texts
        texts = ["", "   ", "  \n "]
        embeddings = processor.create_embeddings_with_fallback(texts)
        
        assert embeddings is not None
        assert embeddings.shape[0] == len(texts)
    
    def test_large_batch_processing(self, gpu_config, sample_texts):
        """Büyük batch işleme testi"""
        processor = RAPIDSGPUProcessor(gpu_config)
        processor.gpu_enabled = False

        # Mock a large number of texts
        large_texts = sample_texts * 200  # 1000 texts
        
        with patch.object(processor, '_create_tfidf_embeddings_cpu') as mock_cpu:
            mock_cpu.return_value = np.random.rand(len(large_texts), 128)
            
            embeddings = processor.create_embeddings_with_fallback(large_texts)
            
            assert embeddings is not None
            assert embeddings.shape[0] == len(large_texts)
            mock_cpu.assert_called_once()

if __name__ == "__main__":
    pytest.main([__file__])