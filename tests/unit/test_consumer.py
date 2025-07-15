#!/usr/bin/env python3

import unittest
import asyncio
import json
import time
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from src.config.kafka_config import KafkaConfig

class TestKafkaConsumer(unittest.TestCase):
    """Kafka Consumer unit testleri"""
    
    def setUp(self):
        """Test setup"""
        self.kafka_config = KafkaConfig(
            bootstrap_servers='localhost:9092',
            topic='test_topic',
            group_id='test_group',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.consumer = KafkaConsumer(self.kafka_config)
        
    def run_async_test(self, coro):
        """Async test helper"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    def test_init(self):
        """Consumer initialization testi"""
        self.assertEqual(self.consumer.cfg, self.kafka_config)
        self.assertIsNone(self.consumer.consumer)
        self.assertIsNone(self.consumer.handler)
        self.assertFalse(self.consumer.running)
    
    def test_set_message_handler(self):
        """Message handler set etme testi"""
        def dummy_handler(message):
            pass
        
        self.consumer.set_message_handler(dummy_handler)
        self.assertEqual(self.consumer.handler, dummy_handler)
    
    def test_start_consuming_without_handler(self):
        """Handler olmadan consume etme testi"""
        # Mock messages
        mock_message = Mock()
        mock_message.value.return_value = b'{"id": "test1", "content": "test message"}'
        mock_message.error.return_value = None
        
        # Set start_time to avoid None error
        self.consumer.start_time = time.time()
        
        # Test without handler - should not raise exception
        self.run_async_test(self.consumer._process_messages([mock_message]))
        
        # Verify message count increased
        self.assertEqual(self.consumer.msg_total, 1)
    
    def test_start_consuming_with_handler(self):
        """Handler ile consume etme testi"""
        # Mock messages
        mock_message1 = Mock()
        mock_message1.value.return_value = b'{"id": "test1", "content": "test message"}'
        mock_message1.error.return_value = None
        mock_message2 = Mock()
        mock_message2.value.return_value = b'{"id": "test2", "content": "another test"}'
        mock_message2.error.return_value = None
        
        # Set message handler
        processed_messages = []
        async def test_handler(messages):
            processed_messages.extend(messages)
        
        self.consumer.set_message_handler(test_handler)
        
        # Set start_time to avoid None error
        self.consumer.start_time = time.time()
        
        # Process messages
        self.run_async_test(self.consumer._process_messages([mock_message1, mock_message2]))
        
        # Verify messages were processed
        self.assertEqual(len(processed_messages), 2)
        self.assertEqual(processed_messages[0]['id'], 'test1')
        self.assertEqual(processed_messages[1]['id'], 'test2')
    
    @patch('src.core.kafka_consumer.KafkaConsumer._to_dlq')
    def test_invalid_json_handling(self, mock_to_dlq):
        """Geçersiz JSON mesaj handling testi"""
        # Mock invalid JSON message
        mock_message = Mock()
        mock_message.value.return_value = b'invalid json content'
        mock_message.error.return_value = None
        
        # Set message handler
        processed_messages = []
        async def test_handler(messages):
            processed_messages.extend(messages)
        
        self.consumer.set_message_handler(test_handler)
        
        # Set start_time to avoid None error
        self.consumer.start_time = time.time()
        
        # Mock _to_dlq to be async
        mock_to_dlq.return_value = asyncio.Future()
        mock_to_dlq.return_value.set_result(None)
        
        # Process invalid message
        self.run_async_test(self.consumer._process_messages([mock_message]))
        
        # Invalid JSON should be skipped, no messages processed
        self.assertEqual(len(processed_messages), 0)
        # DLQ should be called for invalid message
        mock_to_dlq.assert_called_once()
    
    def test_consumer_initialization(self):
        """Consumer başlangıç durumu testi"""
        self.assertIsNone(self.consumer.consumer)
        self.assertIsNone(self.consumer.handler)
        self.assertTrue(hasattr(self.consumer, 'running'))
    
    def test_close_without_consumer(self):
        """Consumer olmadan close etme testi"""
        # Should not raise any exception
        self.run_async_test(self.consumer.close())
    
    def test_close_with_consumer(self):
        """Consumer ile close etme testi"""
        # Mock consumer ekle
        mock_consumer = MagicMock()
        self.consumer.consumer = mock_consumer
        self.consumer.running = True
        
        self.run_async_test(self.consumer.close())
        
        # close() method only sets running to False, actual close happens in _close_consumer
        self.assertFalse(self.consumer.running)

if __name__ == '__main__':
    unittest.main()