#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import numpy as np
from loguru import logger
import time

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.exceptions.embedding_exceptions import EmbeddingProcessingError
from src.utils.circuit_breaker import circuit_breaker, CircuitBreakerConfig

import psutil

try:
    import cupy as cp
    import cudf
    import cuml
    from cuml.feature_extraction.text import TfidfVectorizer as cuTfidfVectorizer
    from cuml.decomposition import TruncatedSVD as cuTruncatedSVD
    from cuml.preprocessing import normalize as cu_normalize
    RAPIDS_AVAILABLE = True
    logger.info("✅ RAPIDS GPU acceleration available")
except ImportError as e:
    RAPIDS_AVAILABLE = False
    logger.warning(f"⚠️ RAPIDS not available, falling back to CPU: {e}")
    # Define dummy classes for type hinting if RAPIDS is not available
    class cp:
        @staticmethod
        def get_default_memory_pool():
            raise NotImplementedError
    import pandas as pd

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logger.warning("⚠️ SentenceTransformers not available")

class RAPIDSGPUProcessor:
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.gpu_config = config.get('gpu', {})
        self.embedding_config = config.get('embedding', {})
        

        self.gpu_enabled = self._check_gpu_availability()
        

        self.model_name = self.embedding_config.get('model', 'all-MiniLM-L6-v2')
        self.vector_size = self.embedding_config.get('vector_size', 384)
        self.batch_size = self.embedding_config.get('batch_size', 1000)
        

        self.use_tfidf_fallback = self.gpu_config.get('use_tfidf_fallback', True)
        self.max_features = self.gpu_config.get('max_features', 10000)
        self.svd_components = self.gpu_config.get('svd_components', 384)
        

        self.circuit_breaker_config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=30.0,
            timeout=120.0
        )
        

        self._sentence_model = None
        self._tfidf_model = None
        self._svd_model = None
        
        logger.info(f"RAPIDS GPU Processor initialized - GPU: {self.gpu_enabled}")
    
    def _check_gpu_availability(self) -> bool:
        if not RAPIDS_AVAILABLE:
            return False
        
        try:

            mempool = cp.get_default_memory_pool()
            total_bytes = mempool.total_bytes()
            

            min_memory_gb = self.gpu_config.get('min_memory_gb', 1.0)
            if total_bytes < min_memory_gb * 1024**3:
                logger.warning(f"Insufficient GPU memory: {total_bytes / 1024**3:.2f}GB < {min_memory_gb}GB")
                return False
            
            logger.info(f"GPU memory available: {total_bytes / 1024**3:.2f}GB")
            return True
            
        except Exception as e:
            logger.warning(f"GPU availability check failed: {e}")
            return False
    
    def _get_sentence_model(self):
        if self._sentence_model is None:
            if not SENTENCE_TRANSFORMERS_AVAILABLE:
                raise EmbeddingProcessingError("SentenceTransformers not available")
            
            logger.info(f"Loading SentenceTransformer model: {self.model_name}")
            self._sentence_model = SentenceTransformer(self.model_name)
        
        return self._sentence_model

    def process_embeddings_gpu(self, texts: List[str]) -> List[List[float]]:
        """
        Process embeddings using GPU acceleration.
        
        Args:
            texts: List of text strings to process
            
        Returns:
            List of embedding vectors
        """
        try:
            if not self.gpu_enabled:
                logger.warning("GPU not available, falling back to CPU")
                return self._process_embeddings_cpu(texts)
            

            import cudf
            df = cudf.DataFrame({'text': texts})
            

            embeddings = self._get_sentence_model().encode(
                texts,
                device='cuda' if self.gpu_enabled else 'cpu',
                batch_size=self.batch_size,
                show_progress_bar=False
            )
            

            if self.gpu_config.get('use_cuml_acceleration', False):
                embeddings = self._apply_cuml_acceleration(embeddings)
            
            return embeddings.tolist()
            
        except Exception as e:
            logger.error(f"GPU embedding processing failed: {str(e)}")
            return self._process_embeddings_cpu(texts)
    
    def _process_embeddings_cpu(self, texts: List[str]) -> List[List[float]]:
        """
        Process embeddings using CPU fallback.
        
        Args:
            texts: List of text strings to process
            
        Returns:
            List of embedding vectors
        """
        try:
            if self.use_tfidf_fallback:
                embeddings = self._create_tfidf_embeddings_cpu(texts)
            else:
                embeddings = self._create_sentence_embeddings(texts)
            
            return embeddings.tolist()
            
        except Exception as e:
            logger.error(f"CPU embedding processing failed: {str(e)}")
            raise EmbeddingProcessingError(f"CPU embedding failed: {e}")
    
    def _apply_cuml_acceleration(self, embeddings: np.ndarray) -> np.ndarray:
        """
        Apply cuML acceleration to embeddings.
        
        Args:
            embeddings: Input embeddings
            
        Returns:
            Accelerated embeddings
        """
        try:
            # Convert to cupy array for GPU processing
            gpu_embeddings = cp.asarray(embeddings)
            
            # Apply normalization using cuML
            normalized_embeddings = cu_normalize(gpu_embeddings, norm='l2')
            

            return cp.asnumpy(normalized_embeddings)
            
        except Exception as e:
            logger.warning(f"cuML acceleration failed, using original embeddings: {e}")
            return embeddings
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """
        Get current memory usage (system and GPU)
        
        Returns:
            Dict[str, Any]: Memory usage information
        """
        
        info = {'gpu_available': self.gpu_enabled}
        

        sys_mem = psutil.virtual_memory()
        info['system_memory'] = {
            'total_gb': sys_mem.total / 1024**3,
            'used_gb': sys_mem.used / 1024**3,
            'available_gb': sys_mem.available / 1024**3,
            'percent_used': sys_mem.percent
        }
        

        if self.gpu_enabled and RAPIDS_AVAILABLE:
            try:
                mempool = cp.get_default_memory_pool()
                total_bytes = mempool.total_bytes()
                used_bytes = mempool.used_bytes()
                
                info['gpu_memory'] = {
                    'total_gb': total_bytes / 1024**3,
                    'used_gb': used_bytes / 1024**3,
                    'percent_used': (used_bytes / total_bytes * 100) if total_bytes > 0 else 0
                }
            except Exception as e:
                logger.warning(f"Could not get GPU memory info: {e}")
                info['gpu_memory'] = {'error': str(e)}
        
        return info
    def _get_tfidf_model(self, fit_data: Optional[List[str]] = None):
        if self._tfidf_model is None:
            if self.gpu_enabled and RAPIDS_AVAILABLE:
                logger.info("Initializing cuML TF-IDF vectorizer")
                self._tfidf_model = cuTfidfVectorizer(
                    max_features=self.max_features,
                    stop_words='english',
                    lowercase=True,
                    ngram_range=(1, 2)
                )
            else:
                from sklearn.feature_extraction.text import TfidfVectorizer
                logger.info("Initializing sklearn TF-IDF vectorizer (CPU fallback)")
                self._tfidf_model = TfidfVectorizer(
                    max_features=self.max_features,
                    stop_words='english',
                    lowercase=True,
                    ngram_range=(1, 2)
                )
            

            if fit_data:
                logger.info(f"Fitting TF-IDF model with {len(fit_data)} samples")
                if self.gpu_enabled:
    
                    cudf_series = cudf.Series(fit_data)
                    self._tfidf_model.fit(cudf_series)
                else:
                    self._tfidf_model.fit(fit_data)
        
        return self._tfidf_model
    
    def _get_svd_model(self):
        if self._svd_model is None:
            if self.gpu_enabled:
                logger.info(f"Initializing cuML TruncatedSVD with {self.svd_components} components")
                self._svd_model = cuTruncatedSVD(n_components=self.svd_components)
            else:
                from sklearn.decomposition import TruncatedSVD
                logger.info(f"Initializing sklearn TruncatedSVD with {self.svd_components} components (CPU fallback)")
                self._svd_model = TruncatedSVD(n_components=self.svd_components)
        
        return self._svd_model
    
    @circuit_breaker("rapids_gpu_embedding")
    def create_embeddings_gpu(self, texts: List[str]) -> np.ndarray:
        if not self.gpu_enabled:
            raise EmbeddingProcessingError("GPU not available for embedding creation")
        
        try:
            start_time = time.time()
            

            valid_texts = [text.strip() if text else "" for text in texts]
            
            if self.use_tfidf_fallback:

                embeddings = self._create_tfidf_embeddings_gpu(valid_texts)
            else:

                embeddings = self._create_sentence_embeddings(valid_texts)
            
            processing_time = time.time() - start_time
            logger.info(f"GPU embedding creation completed: {len(texts)} texts in {processing_time:.2f}s")
            
            return embeddings
            
        except Exception as e:
            logger.error(f"GPU embedding creation failed: {e}")
            raise EmbeddingProcessingError(f"GPU embedding failed: {e}")
    
    def _create_tfidf_embeddings_gpu(self, texts: List[str]) -> np.ndarray:
        try:

            cudf_texts = cudf.Series(texts)
            

            tfidf_model = self._get_tfidf_model(texts)
            

            logger.info("Creating TF-IDF vectors on GPU")
            tfidf_matrix = tfidf_model.transform(cudf_texts)
            

            svd_model = self._get_svd_model()
            
            logger.info("Applying SVD dimensionality reduction on GPU")
            if not hasattr(svd_model, 'components_'):

                embeddings = svd_model.fit_transform(tfidf_matrix)
            else:
                embeddings = svd_model.transform(tfidf_matrix)
            

            embeddings = cu_normalize(embeddings, norm='l2')
            
            # Convert back to numpy
            if hasattr(embeddings, 'to_numpy'):
                return embeddings.to_numpy()
            else:
                return cp.asnumpy(embeddings)
            
        except Exception as e:
            logger.error(f"TF-IDF GPU embedding creation failed: {e}")

            return self._create_tfidf_embeddings_cpu(texts)
    
    def _create_tfidf_embeddings_cpu(self, texts: List[str]) -> np.ndarray:
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.decomposition import TruncatedSVD
            from sklearn.preprocessing import normalize
            
            logger.info("Creating TF-IDF embeddings on CPU (fallback)")
            

            tfidf = TfidfVectorizer(
                max_features=self.max_features,
                stop_words='english',
                lowercase=True,
                ngram_range=(1, 2)
            )
            
            tfidf_matrix = tfidf.fit_transform(texts)
            

            svd = TruncatedSVD(n_components=self.svd_components)
            embeddings = svd.fit_transform(tfidf_matrix)
            

            embeddings = normalize(embeddings, norm='l2')
            
            return embeddings
            
        except ValueError as e:
            if 'empty vocabulary' in str(e):
                logger.warning("TF-IDF vocabulary is empty, returning zero vectors.")
                return np.zeros((len(texts), self.svd_components))
            raise EmbeddingProcessingError(f"CPU TF-IDF embedding failed with ValueError: {e}")
        except Exception as e:
            logger.error(f"CPU TF-IDF embedding creation failed: {e}")
            raise EmbeddingProcessingError(f"CPU TF-IDF embedding failed: {e}")
    
    def _create_sentence_embeddings(self, texts: List[str]) -> np.ndarray:
        try:
            model = self._get_sentence_model()
            
            logger.info(f"Creating sentence embeddings with {self.model_name}")
            embeddings = model.encode(
                texts,
                batch_size=min(len(texts), self.batch_size),
                show_progress_bar=False
            )
            
            return embeddings
            
        except Exception as e:
            logger.error(f"Sentence embedding creation failed: {e}")
            raise EmbeddingProcessingError(f"Sentence embedding failed: {e}")

    def cleanup(self):
        """
        Clean up resources (models, GPU memory)
        """
        logger.info("Cleaning up RAPIDS GPU Processor resources")
        

        self._sentence_model = None
        self._tfidf_model = None
        self._svd_model = None
        

        if self.gpu_enabled and RAPIDS_AVAILABLE:
            try:
                mempool = cp.get_default_memory_pool()
                mempool.free_all_blocks()
                logger.info("Freed all GPU memory blocks")
            except Exception as e:
                logger.warning(f"Could not free GPU memory: {e}")
        

        self.gpu_enabled = False
    
    @circuit_breaker("rapids_embedding_fallback")
    def create_embeddings_with_fallback(self, texts: List[str]) -> np.ndarray:
        try:
            if self.gpu_enabled:
                try:
                    return self.create_embeddings_gpu(texts)
                except Exception as e:
                    logger.warning(f"GPU embedding failed, falling back to CPU: {e}")
                    self.gpu_enabled = False
            

            logger.info("Using CPU for embedding creation")
            if self.use_tfidf_fallback:
                return self._create_tfidf_embeddings_cpu(texts)
            else:
                return self._create_sentence_embeddings(texts)
            
        except Exception as e:
            logger.error(f"All embedding methods failed: {e}")
            raise EmbeddingProcessingError(f"Embedding creation failed: {e}")
    
    def benchmark_performance(self, test_texts: List[str], iterations: int = 3) -> Dict[str, Any]:
        results = {
            'gpu_available': self.gpu_enabled,
            'test_size': len(test_texts),
            'iterations': iterations,
            'gpu_times': [],
            'cpu_times': [],
            'gpu_avg': 0.0,
            'cpu_avg': 0.0,
            'speedup': 0.0
        }
        
        logger.info(f"Starting performance benchmark with {len(test_texts)} texts")
        

        if self.gpu_enabled:
            for i in range(iterations):
                start_time = time.time()
                try:
                    self.create_embeddings_gpu(test_texts)
                    gpu_time = time.time() - start_time
                    results['gpu_times'].append(gpu_time)
                    logger.info(f"GPU iteration {i+1}: {gpu_time:.2f}s")
                except Exception as e:
                    logger.error(f"GPU benchmark iteration {i+1} failed: {e}")
            
            if results['gpu_times']:
                results['gpu_avg'] = sum(results['gpu_times']) / len(results['gpu_times'])
        

        original_gpu_enabled = self.gpu_enabled
        self.gpu_enabled = False
        
        for i in range(iterations):
            start_time = time.time()
            try:
                if self.use_tfidf_fallback:
                    self._create_tfidf_embeddings_cpu(test_texts)
                else:
                    self._create_sentence_embeddings(test_texts)
                cpu_time = time.time() - start_time
                results['cpu_times'].append(cpu_time)
                logger.info(f"CPU iteration {i+1}: {cpu_time:.2f}s")
            except Exception as e:
                logger.error(f"CPU benchmark iteration {i+1} failed: {e}")
        
        self.gpu_enabled = original_gpu_enabled
        
        if results['cpu_times']:
            results['cpu_avg'] = sum(results['cpu_times']) / len(results['cpu_times'])
        

        if results['gpu_avg'] > 0 and results['cpu_avg'] > 0:
            results['speedup'] = results['cpu_avg'] / results['gpu_avg']
        
        logger.info(f"Benchmark completed - GPU: {results['gpu_avg']:.2f}s, CPU: {results['cpu_avg']:.2f}s, Speedup: {results['speedup']:.2f}x")
        
        return results
    
    def get_memory_usage(self) -> Dict[str, Any]:
        memory_info = {
            'gpu_available': self.gpu_enabled,
            'gpu_memory': {},
            'system_memory': {}
        }
        

        if self.gpu_enabled and RAPIDS_AVAILABLE:
            try:
                mempool = cp.get_default_memory_pool()
                
                total_bytes = mempool.total_bytes()
                used_bytes = mempool.used_bytes()
                free_bytes = mempool.free_bytes()
                
                memory_info['gpu_memory'] = {
                    'total_bytes': total_bytes,
                    'used_bytes': used_bytes,
                    'free_bytes': free_bytes,
                    'total_gb': total_bytes / 1024**3,
                    'used_gb': used_bytes / 1024**3,
                    'free_gb': free_bytes / 1024**3,
                    'percent_used': (used_bytes / total_bytes * 100) if total_bytes > 0 else 0.0
                }
            except Exception as e:
                logger.error(f"GPU memory info failed: {e}")
        

        try:
            import psutil
            mem = psutil.virtual_memory()
            
            memory_info['system_memory'] = {
                'total_bytes': mem.total,
                'used_bytes': mem.used,
                'free_bytes': mem.available,
                'total_gb': mem.total / 1024**3,
                'used_gb': mem.used / 1024**3,
                'free_gb': mem.available / 1024**3,
                'percent_used': mem.percent
            }
        except Exception as e:
            logger.error(f"System memory info failed: {e}")
        
        return memory_info