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

# Mock sentence_transformers module
sys.modules['sentence_transformers'] = MagicMock()
from src.core.embedding_processor import EmbeddingProcessor

class TestEmbeddingProcessor(unittest.TestCase):
    """Embedding Processor unit testleri"""
    
    def setUp(self):
        """Test setup"""
        self.embedding_config = {
            'model_name': 'sentence-transformers/all-MiniLM-L6-v2',
            'vector_size': 384,
            'batch_size': 32
        }
        
    def run_async_test(self, coro):
        """Async test helper"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_init(self, mock_sentence_transformer):
        """EmbeddingProcessor initialization testi"""
        processor = EmbeddingProcessor(self.embedding_config)
        
        self.assertEqual(processor.config, self.embedding_config)
        self.assertEqual(processor.model_name, 'sentence-transformers/all-MiniLM-L6-v2')
        self.assertEqual(processor.vector_size, 384)
        self.assertIsNone(processor.model)
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_create_embedding_single_text(self, mock_sentence_transformer):
        """Tek metin için embedding oluşturma testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        
        # Mock embedding output
        test_embedding = np.array([0.1, 0.2, 0.3])
        mock_model.encode.return_value = np.array([test_embedding])  # Batch format
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test single text
        test_text = "Bu bir test metnidir"
        result = self.run_async_test(processor.create_embedding(test_text))
        
        # Verify model.encode was called
        mock_model.encode.assert_called_once()
        
        # Verify result
        self.assertEqual(result, test_embedding.tolist())
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_create_embedding_multiple_texts(self, mock_sentence_transformer):
        """Çoklu metin için embedding oluşturma testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        
        # Mock embedding output
        test_embeddings = np.array([
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9]
        ])
        mock_model.encode.return_value = test_embeddings
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test multiple texts
        test_texts = [
            "Bu bir test metnidir",
            "İkinci test metni",
            "Üçüncü test metni"
        ]
        result = self.run_async_test(processor.create_embeddings(test_texts))
        
        # Verify model.encode was called correctly
        mock_model.encode.assert_called_once()
        
        # Verify result
        self.assertEqual(len(result), 3)
        for i, embedding in enumerate(result):
            self.assertEqual(embedding, test_embeddings[i].tolist())
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_create_embedding_empty_text(self, mock_sentence_transformer):
        """Boş metin için embedding oluşturma testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test empty text
        result = self.run_async_test(processor.create_embedding(""))
        
        # Boş text için None döndürülmeli
        self.assertIsNone(result)
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_create_embedding_none_text(self, mock_sentence_transformer):
        """None metin için embedding oluşturma testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test None text
        result = self.run_async_test(processor.create_embedding(None))
        
        # None text için None döndürülmeli
        self.assertIsNone(result)
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_process_message_with_content(self, mock_sentence_transformer):
        """Content içeren mesaj işleme testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        
        # Mock embedding output
        test_embedding = np.array([0.1, 0.2, 0.3])
        mock_model.encode.return_value = np.array([test_embedding])  # Batch format
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test message
        test_message = {
            'id': 'test_001',
            'content': 'Bu bir test mesajıdır',
            'timestamp': '2024-01-15T10:30:00Z',
            'metadata': {'source': 'test'}
        }
        
        result = self.run_async_test(processor.process_message(test_message))
        
        # Verify embedding was created
        mock_model.encode.assert_called_once()
        
        # Verify result structure
        self.assertIn('vector', result)
        self.assertIn('metadata', result)
        self.assertEqual(result['metadata']['id'], test_message['id'])
        self.assertEqual(result['metadata']['content'], test_message['content'])
        self.assertEqual(result['metadata']['timestamp'], test_message['timestamp'])
        self.assertEqual(result['vector'], test_embedding.tolist())
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_process_message_without_content(self, mock_sentence_transformer):
        """Content içermeyen mesaj işleme testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test message without content
        test_message = {
            'id': 'test_001',
            'timestamp': '2024-01-15T10:30:00Z',
            'metadata': {'source': 'test'}
        }
        
        result = self.run_async_test(processor.process_message(test_message))
        
        # Content yoksa None döndürülmeli
        self.assertIsNone(result)
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_process_message_empty_content(self, mock_sentence_transformer):
        """Boş content ile mesaj işleme testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        # Test message with empty content
        test_message = {
            'id': 'test_001',
            'content': '',
            'timestamp': '2024-01-15T10:30:00Z',
            'metadata': {'source': 'test'}
        }
        
        result = self.run_async_test(processor.process_message(test_message))
        
        # Boş content için None döndürülmeli
        self.assertIsNone(result)
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_batch_process_messages(self, mock_sentence_transformer):
        """Toplu mesaj işleme testi"""
        mock_model = MagicMock()
        test_embeddings = np.array([
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
            [0.7, 0.8, 0.9]
        ])
        mock_model.encode.return_value = test_embeddings
        mock_model.get_sentence_embedding_dimension.return_value = 384
        mock_sentence_transformer.return_value = mock_model
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        messages = [
            {
                'id': 'msg_001',
                'content': 'First message content',
                'timestamp': '2024-01-01T00:00:00Z'
            },
            {
                'id': 'msg_002', 
                'content': 'Second message content',
                'timestamp': '2024-01-01T00:01:00Z'
            },
            {
                'id': 'msg_003',
                'content': 'Third message content', 
                'timestamp': '2024-01-01T00:02:00Z'
            }
        ]
        
        results = self.run_async_test(processor.process_messages(messages))
        
        # Verify results
        self.assertEqual(len(results), 3)
        
        for i, result in enumerate(results):
            self.assertIsNotNone(result)
            self.assertIn('vector', result)
            self.assertIn('metadata', result)
            self.assertEqual(result['metadata']['id'], f'msg_00{i+1}')
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_model_loading_error(self, mock_sentence_transformer):
        """Model yükleme hatası testi"""
        # Mock model loading error
        mock_sentence_transformer.side_effect = Exception("Model loading failed")
        
        processor = EmbeddingProcessor(self.embedding_config)
        
        # initialize() method returns False on error, doesn't raise
        result = self.run_async_test(processor.initialize())
        
        # Verify initialization failed
        self.assertFalse(result)
    
    @patch('src.core.embedding_processor.SentenceTransformer')
    def test_encoding_error(self, mock_sentence_transformer):
        """Encoding hatası testi"""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        mock_model.get_sentence_embedding_dimension.return_value = 384
        
        # Mock encoding error
        mock_model.encode.side_effect = Exception("Encoding failed")
        
        processor = EmbeddingProcessor(self.embedding_config)
        processor.model = mock_model
        
        result = self.run_async_test(processor.create_embedding("test text"))
        
        # Encoding hatası durumunda None döndürülmeli
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()