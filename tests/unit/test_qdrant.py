#!/usr/bin/env python3

import unittest
import asyncio
import numpy as np
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.core.qdrant_writer import QdrantWriter

class TestQdrantWriter(unittest.TestCase):
    """Qdrant Writer unit testleri"""
    
    def setUp(self):
        """Test setup"""
        self.qdrant_config = {
            'host': 'localhost',
            'port': 6333,
            'collection_name': 'test_embeddings',
            'vector_size': 384
        }
        
    def run_async_test(self, coro):
        """Async test helper"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
        
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_init(self, mock_client_class):
        """QdrantWriter initialization testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        writer = QdrantWriter(self.qdrant_config)
        
        # Verify client was created with correct parameters
        mock_client_class.assert_called_once_with(
            host=self.qdrant_config['host'],
            port=self.qdrant_config['port']
        )
        
        self.assertEqual(writer.client, mock_client_instance)
        self.assertEqual(writer.collection_name, self.qdrant_config['collection_name'])
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_initialize_collection_new(self, mock_client_class):
        """Yeni koleksiyon oluşturma testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        # Mock boş koleksiyon listesi
        mock_collections = MagicMock()
        mock_collections.collections = []
        mock_client_instance.get_collections.return_value = mock_collections
        
        writer = QdrantWriter(self.qdrant_config)
        self.run_async_test(writer.initialize_collection())
        
        mock_client_instance.get_collections.assert_called_once()
        mock_client_instance.create_collection.assert_called_once()
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_initialize_collection_existing(self, mock_client_class):
        """Mevcut koleksiyon testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        # Mock koleksiyon listesi - koleksiyon mevcut
        mock_collection = MagicMock()
        mock_collection.name = 'test_embeddings'
        mock_collections = MagicMock()
        mock_collections.collections = [mock_collection]
        mock_client_instance.get_collections.return_value = mock_collections
        
        writer = QdrantWriter(self.qdrant_config)
        self.run_async_test(writer.initialize_collection())
        
        mock_client_instance.get_collections.assert_called_once()
        mock_client_instance.create_collection.assert_not_called()
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_write_embeddings_single(self, mock_client_class):
        """Tek embedding yazma testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        writer = QdrantWriter(self.qdrant_config)
        
        embeddings = [{
            'vector': [0.1, 0.2, 0.3],
            'metadata': {'text': 'test'}
        }]
        
        result = self.run_async_test(writer.write_embeddings(embeddings))
        
        self.assertTrue(result)
        mock_client_instance.upsert.assert_called_once()
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_write_embeddings_multiple(self, mock_client_class):
        """Çoklu embedding yazma testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        writer = QdrantWriter(self.qdrant_config)
        
        embeddings = [
            {'vector': [0.1, 0.2, 0.3], 'metadata': {'text': 'test1'}},
            {'vector': [0.4, 0.5, 0.6], 'metadata': {'text': 'test2'}}
        ]
        
        result = self.run_async_test(writer.write_embeddings(embeddings))
        
        self.assertTrue(result)
        mock_client_instance.upsert.assert_called_once()
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_write_embeddings_error_handling(self, mock_client_class):
        """Embedding yazma hata yönetimi testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.upsert.side_effect = Exception("Connection error")
        
        writer = QdrantWriter(self.qdrant_config)
        
        # Test data
        test_embeddings = [{
            'vector': [0.1, 0.2, 0.3],
            'metadata': {'id': 'test_001', 'content': 'test content'}
        }]
        
        result = self.run_async_test(writer.write_embeddings(test_embeddings))
        
        # Verify error handling
        self.assertFalse(result)
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_search_similar(self, mock_client_class):
        """Benzer vektör arama testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        # Mock arama sonuçları
        mock_results = [MagicMock(), MagicMock()]
        mock_client_instance.search.return_value = mock_results
        
        writer = QdrantWriter(self.qdrant_config)
        
        query_vector = [0.1, 0.2, 0.3]
        results = self.run_async_test(writer.search_similar(query_vector, limit=5))
        
        self.assertEqual(results, mock_results)
        mock_client_instance.search.assert_called_once()
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_search_similar_empty_results(self, mock_client_class):
        """Boş arama sonucu testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        # Mock empty search results
        mock_client_instance.search.return_value = []
        
        writer = QdrantWriter(self.qdrant_config)
        
        # Test search
        query_vector = np.random.rand(384).tolist()
        results = self.run_async_test(writer.search_similar(query_vector))
        
        # Verify empty results
        self.assertEqual(len(results), 0)
    
    @patch('src.core.qdrant_writer.QdrantClient')
    def test_search_error_handling(self, mock_client_class):
        """Arama hatası yönetimi testi"""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.search.side_effect = Exception("Search error")
        
        writer = QdrantWriter(self.qdrant_config)
        
        # Test search
        query_vector = np.random.rand(384).tolist()
        results = self.run_async_test(writer.search_similar(query_vector))
        
        # Verify error handling
        self.assertEqual(results, [])

if __name__ == '__main__':
    unittest.main()