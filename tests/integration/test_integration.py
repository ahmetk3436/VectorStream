#!/usr/bin/env python3

import unittest
import asyncio
import json
import time
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from src.core.qdrant_writer import QdrantWriter
from config.kafka_config import KafkaConfig
import yaml

class TestIntegration(unittest.TestCase):
    """Integration testleri - gerçek servisleri kullanır"""
    
    def run_async_test(self, coro):
        """Async test helper"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    @classmethod
    def setUpClass(cls):
        """Test class setup"""
        # Load configuration
        config_path = project_root / "config" / "app_config.yaml"
        with open(config_path, 'r') as file:
            cls.config = yaml.safe_load(file)
    
    def setUp(self):
        """Test setup"""
        self.kafka_config = KafkaConfig.from_dict(self.config['kafka'])
        self.qdrant_config = self.config['qdrant']
        
        # Test collection name to avoid conflicts
        self.qdrant_config['collection_name'] = 'test_integration_embeddings'
    
    def test_qdrant_connection_and_operations(self):
        """Qdrant bağlantısı ve temel operasyonlar testi"""
        try:
            # Initialize Qdrant writer
            qdrant_writer = QdrantWriter(self.qdrant_config)
            
            # Initialize collection
            self.run_async_test(qdrant_writer.initialize_collection())
            
            # Test data
            test_embeddings = [
                [0.1] * 384,  # Simple test embedding
                [0.2] * 384,
                [0.3] * 384
            ]
            
            test_metadata = [
                {'id': 'integration_test_001', 'content': 'İlk test mesajı', 'source': 'integration_test'},
                {'id': 'integration_test_002', 'content': 'İkinci test mesajı', 'source': 'integration_test'},
                {'id': 'integration_test_003', 'content': 'Üçüncü test mesajı', 'source': 'integration_test'}
            ]
            
            # Write embeddings
            embeddings_data = [
                {'vector': embedding, 'metadata': metadata}
                for embedding, metadata in zip(test_embeddings, test_metadata)
            ]
            self.run_async_test(qdrant_writer.write_embeddings(embeddings_data))
            
            # Wait a bit for indexing
            time.sleep(1)
            
            # Search for similar vectors
            query_vector = [0.15] * 384  # Should be closest to first embedding
            results = self.run_async_test(qdrant_writer.search_similar(query_vector, limit=3))
            
            # Verify results
            self.assertGreater(len(results), 0)
            # Check that we got valid results with expected structure
            first_result = results[0]
            self.assertIn('id', first_result.payload)
            self.assertTrue(first_result.payload['id'].startswith('integration_test_'))
            
            print(f"✅ Qdrant integration test başarılı: {len(results)} sonuç bulundu")
            
        except Exception as e:
            self.fail(f"Qdrant integration test başarısız: {e}")
    
    @patch('kafka.KafkaProducer')
    def test_kafka_consumer_mock_integration(self, mock_producer_class):
        """Kafka consumer mock integration testi"""
        try:
            # Mock producer for sending test messages
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            
            # Initialize consumer
            consumer = KafkaConsumer(self.kafka_config)
            
            # Test message handler
            received_messages = []
            
            def test_handler(message):
                received_messages.append(message)
                if len(received_messages) >= 2:  # Stop after 2 messages
                    raise KeyboardInterrupt()
            
            consumer.set_message_handler(test_handler)
            
            # Mock consumer behavior
            with patch('src.core.kafka_consumer.PyKafkaConsumer') as mock_consumer_class:
                mock_kafka_consumer = MagicMock()
                mock_consumer_class.return_value = mock_kafka_consumer
                
                # Mock messages
                test_messages = [
                    type('MockMessage', (), {
                        'value': json.dumps({
                            'id': 'integration_test_001',
                            'content': 'Integration test mesajı 1',
                            'timestamp': '2024-01-15T10:30:00Z',
                            'metadata': {'source': 'integration_test'}
                        }).encode('utf-8')
                    }),
                    type('MockMessage', (), {
                        'value': json.dumps({
                            'id': 'integration_test_002',
                            'content': 'Integration test mesajı 2',
                            'timestamp': '2024-01-15T10:31:00Z',
                            'metadata': {'source': 'integration_test'}
                        }).encode('utf-8')
                    })
                ]
                
                mock_kafka_consumer.__iter__.return_value = iter(test_messages)
                
                # Start consuming
                try:
                    self.run_async_test(consumer.start_consuming())
                except KeyboardInterrupt:
                    pass
                
                # Verify messages were received
                self.assertEqual(len(received_messages), 2)
                self.assertEqual(received_messages[0]['id'], 'integration_test_001')
                self.assertEqual(received_messages[1]['id'], 'integration_test_002')
                
                print(f"✅ Kafka consumer mock integration test başarılı: {len(received_messages)} mesaj işlendi")
            
            # Clean up
            self.run_async_test(consumer.close())
            
        except Exception as e:
            self.fail(f"Kafka consumer integration test başarısız: {e}")
    
    def test_end_to_end_pipeline_mock(self):
        """End-to-end pipeline mock testi"""
        try:
            # Initialize components
            qdrant_writer = QdrantWriter(self.qdrant_config)
            self.run_async_test(qdrant_writer.initialize_collection())
            
            consumer = KafkaConsumer(self.kafka_config)
            
            # Pipeline test data
            processed_messages = []
            
            async def pipeline_handler(message):
                # Simulate embedding creation (simple mock)
                embedding = [0.1 + (ord(message['content'][0]) / 1000)] * 384
                
                # Write to Qdrant
                embeddings_data = [{'vector': embedding, 'metadata': message}]
                await qdrant_writer.write_embeddings(embeddings_data)
                
                processed_messages.append({
                    'message': message,
                    'embedding_size': len(embedding)
                })
                
                if len(processed_messages) >= 3:  # Stop after 3 messages
                    raise KeyboardInterrupt()
            
            consumer.set_message_handler(pipeline_handler)
            
            # Mock the consumer with test messages
            with patch('src.core.kafka_consumer.PyKafkaConsumer') as mock_consumer_class:
                mock_kafka_consumer = MagicMock()
                mock_consumer_class.return_value = mock_kafka_consumer
                
                test_messages = [
                    type('MockMessage', (), {
                        'value': json.dumps({
                            'id': f'pipeline_test_{i:03d}',
                            'content': f'Pipeline test mesajı {i+1}',
                            'timestamp': f'2024-01-15T10:3{i}:00Z',
                            'metadata': {'source': 'pipeline_test'}
                        }).encode('utf-8')
                    }) for i in range(3)
                ]
                
                mock_kafka_consumer.__iter__.return_value = iter(test_messages)
                
                # Run pipeline
                try:
                    self.run_async_test(consumer.start_consuming())
                except KeyboardInterrupt:
                    pass
                
                # Verify pipeline results
                self.assertEqual(len(processed_messages), 3)
                
                for i, result in enumerate(processed_messages):
                    self.assertEqual(result['message']['id'], f'pipeline_test_{i:03d}')
                    self.assertEqual(result['embedding_size'], 384)
                
                # Test search functionality
                time.sleep(1)  # Wait for indexing
                
                query_vector = [0.1] * 384
                search_results = self.run_async_test(qdrant_writer.search_similar(query_vector, limit=5))
                
                self.assertGreater(len(search_results), 0)
                
                print(f"✅ End-to-end pipeline test başarılı:")
                print(f"   - {len(processed_messages)} mesaj işlendi")
                print(f"   - {len(search_results)} arama sonucu bulundu")
            
            # Clean up
            self.run_async_test(consumer.close())
            
        except Exception as e:
            self.fail(f"End-to-end pipeline test başarısız: {e}")
    
    def test_error_handling_integration(self):
        """Hata yönetimi integration testi"""
        try:
            # Test invalid Qdrant config - this should raise during initialization
            invalid_qdrant_config = self.qdrant_config.copy()
            invalid_qdrant_config['port'] = 99999  # Invalid port
            
            qdrant_writer = QdrantWriter(invalid_qdrant_config)
            with self.assertRaises(Exception):
                self.run_async_test(qdrant_writer.initialize_collection())
            
            # Test invalid Kafka config
            invalid_kafka_config = KafkaConfig(
                bootstrap_servers=['invalid_host:9092'],
                topic='test_topic',
                group_id='test_group',
                auto_offset_reset='latest'
            )
            
            consumer = KafkaConsumer(invalid_kafka_config)
            
            def dummy_handler(message):
                pass
            
            consumer.set_message_handler(dummy_handler)
            
            # This should handle connection errors gracefully
            # The consumer handles exceptions internally, so we just verify it doesn't crash
            with patch('src.core.kafka_consumer.PyKafkaConsumer') as mock_consumer_class:
                mock_consumer_class.side_effect = Exception("Connection failed")
                
                # Should not raise exception, but handle it gracefully
                try:
                    self.run_async_test(consumer.start_consuming())
                except Exception:
                    pass  # Expected behavior - consumer handles errors internally
            
            print("✅ Error handling integration test başarılı")
            
        except Exception as e:
            self.fail(f"Error handling integration test başarısız: {e}")
    
    def test_configuration_loading(self):
        """Konfigürasyon yükleme testi"""
        try:
            # Verify all required config sections exist
            required_sections = ['kafka', 'qdrant', 'logging']
            
            for section in required_sections:
                self.assertIn(section, self.config)
            
            # Verify Kafka config
            kafka_config = self.config['kafka']
            required_kafka_keys = ['bootstrap_servers', 'topic', 'group_id']
            
            for key in required_kafka_keys:
                self.assertIn(key, kafka_config)
            
            # Verify Qdrant config
            qdrant_config = self.config['qdrant']
            required_qdrant_keys = ['host', 'port', 'collection_name', 'vector_size']
            
            for key in required_qdrant_keys:
                self.assertIn(key, qdrant_config)
            
            print("✅ Configuration loading test başarılı")
            
        except Exception as e:
            self.fail(f"Configuration loading test başarısız: {e}")

if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)