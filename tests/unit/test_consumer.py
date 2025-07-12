#!/usr/bin/env python3

import unittest
import asyncio
import json
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from config.kafka_config import KafkaConfig

class TestKafkaConsumer(unittest.TestCase):
    """Kafka Consumer unit testleri"""
    
    def setUp(self):
        """Test setup"""
        self.kafka_config = KafkaConfig(
            bootstrap_servers=['localhost:9092'],
            topic='test_topic',
            group_id='test_group',
            auto_offset_reset='latest'
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
        self.assertEqual(self.consumer.config, self.kafka_config)
        self.assertIsNone(self.consumer.consumer)
        self.assertIsNone(self.consumer.message_handler)
    
    def test_set_message_handler(self):
        """Message handler set etme testi"""
        def dummy_handler(message):
            pass
        
        self.consumer.set_message_handler(dummy_handler)
        self.assertEqual(self.consumer.message_handler, dummy_handler)
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    def test_start_consuming_without_handler(self, mock_consumer_class):
        """Handler olmadan consume etme testi"""
        # Mock consumer instance
        mock_kafka_consumer = Mock()
        mock_consumer_class.return_value = mock_kafka_consumer
        
        # Mock messages
        mock_message = Mock()
        mock_message.value = b'{"id": "test1", "content": "test message"}'
        
        # Mock iteration with limited messages
        mock_kafka_consumer.__iter__ = Mock(return_value=iter([mock_message]))
        
        # Test without handler
        self.run_async_test(self.consumer.start_consuming())
        
        # Verify consumer was created
        mock_consumer_class.assert_called_once()
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    def test_start_consuming_with_handler(self, mock_consumer_class):
        """Handler ile consume etme testi"""
        # Mock consumer instance
        mock_kafka_consumer = Mock()
        mock_consumer_class.return_value = mock_kafka_consumer
        
        # Mock messages
        mock_message1 = Mock()
        mock_message1.value = b'{"id": "test1", "content": "test message"}'
        mock_message2 = Mock()
        mock_message2.value = b'{"id": "test2", "content": "another test"}'
        
        # Set message handler
        processed_messages = []
        async def test_handler(message):
            processed_messages.append(message)
            if len(processed_messages) >= 2:  # Stop after 2 messages
                self.consumer.running = False
        
        self.consumer.set_message_handler(test_handler)
        
        # Mock iteration with limited messages
        mock_kafka_consumer.__iter__ = Mock(return_value=iter([mock_message1, mock_message2]))
        
        # Start consuming
        self.run_async_test(self.consumer.start_consuming())
        
        # Verify messages were processed
        self.assertEqual(len(processed_messages), 2)
        self.assertEqual(processed_messages[0]['id'], 'test1')
        self.assertEqual(processed_messages[1]['id'], 'test2')
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    def test_invalid_json_handling(self, mock_consumer_class):
        """Geçersiz JSON mesaj handling testi"""
        # Mock consumer instance
        mock_kafka_consumer = Mock()
        mock_consumer_class.return_value = mock_kafka_consumer
        
        # Mock invalid JSON message
        mock_message = Mock()
        mock_message.value = b'invalid json content'
        
        # Set message handler
        processed_messages = []
        async def test_handler(message):
            processed_messages.append(message)
        
        self.consumer.set_message_handler(test_handler)
        
        # Mock iteration with single invalid message
        mock_kafka_consumer.__iter__ = Mock(return_value=iter([mock_message]))
        
        # Start consuming
        self.run_async_test(self.consumer.start_consuming())
        
        # Invalid JSON should be skipped, no messages processed
        self.assertEqual(len(processed_messages), 0)
    
    def test_consumer_initialization(self):
        """Consumer başlangıç durumu testi"""
        self.assertIsNone(self.consumer.consumer)
        self.assertIsNone(self.consumer.message_handler)
        self.assertFalse(self.consumer.running)
    
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
        
        mock_consumer.close.assert_called_once()
        self.assertFalse(self.consumer.running)

if __name__ == '__main__':
    unittest.main()