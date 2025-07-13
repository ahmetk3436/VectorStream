#!/usr/bin/env python3
"""
Dead Letter Queue Integration Tests

Bu modül DLQ sisteminin integration testlerini içerir.
"""

import unittest
import asyncio
import json
import time
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.utils.dlq_manager import DeadLetterQueueManager, DeadLetterMessage, FailureReason
from src.utils.dead_letter_queue import DeadLetterQueue, DLQConfig
from src.core.kafka_consumer import KafkaConsumer
from src.core.qdrant_writer import QdrantWriter
from config.kafka_config import KafkaConfig
import yaml


class TestDLQManagerIntegration(unittest.TestCase):
    """DLQ Manager integration testleri"""
    
    def run_async_test(self, coro):
        """Async test helper"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    def setUp(self):
        """Test setup"""
        # Create temporary directory for DLQ files
        self.temp_dir = tempfile.mkdtemp()
        
        # DLQ Manager config
        self.dlq_config = {
            'dead_letter_queue': {
                'path': self.temp_dir,
                'max_retries': 3,
                'retry_delays': [1, 2, 3],  # Short delays for testing
                'enable_retry': True,
                'batch_size': 10
            }
        }
        
        self.dlq_manager = DeadLetterQueueManager(self.dlq_config)
    
    def tearDown(self):
        """Test cleanup"""
        # Clean up temporary directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_dlq_manager_initialization(self):
        """DLQ Manager başlatma testi"""
        try:
            stats = self.dlq_manager.get_stats()
            
            self.assertIsInstance(stats, dict)
            self.assertIn('total_failed', stats)
            self.assertIn('in_retry_queue', stats)
            self.assertIn('ready_for_retry', stats)
            self.assertIn('failure_reasons', stats)
            self.assertIn('dlq_path', stats)
            self.assertIn('config', stats)
            
            # Check config values
            config = stats['config']
            self.assertEqual(config['max_retries'], 3)
            self.assertEqual(config['retry_delays'], [1, 2, 3])
            self.assertTrue(config['enable_retry'])
            self.assertEqual(config['batch_size'], 10)
            
            print("✅ DLQ Manager initialization test başarılı")
            
        except Exception as e:
            self.fail(f"DLQ Manager initialization test başarısız: {e}")
    
    def test_failed_message_handling(self):
        """Başarısız mesaj işleme testi"""
        try:
            test_message = {
                'id': 'test_message_001',
                'content': 'Test mesajı',
                'timestamp': '2024-01-15T10:00:00Z'
            }
            
            # Handle failed message
            result = self.run_async_test(
                self.dlq_manager.handle_failed_message(
                    test_message,
                    Exception("Test hatası"),
                    FailureReason.PROCESSING_ERROR,
                    "test-topic",
                    0,
                    123
                )
            )
            
            self.assertTrue(result)
            
            # Check stats
            stats = self.dlq_manager.get_stats()
            self.assertEqual(stats['in_retry_queue'], 1)
            self.assertEqual(stats['total_failed'], 0)
            
            # Check failure reasons
            self.assertIn('processing_error', stats['failure_reasons'])
            self.assertEqual(stats['failure_reasons']['processing_error'], 1)
            
            print("✅ Failed message handling test başarılı")
            
        except Exception as e:
            self.fail(f"Failed message handling test başarısız: {e}")
    
    def test_retry_mechanism(self):
        """Retry mekanizması testi"""
        try:
            test_message = {
                'id': 'test_retry_001',
                'content': 'Retry test mesajı',
                'timestamp': '2024-01-15T10:00:00Z'
            }
            
            # Add message to retry queue
            self.run_async_test(
                self.dlq_manager.handle_failed_message(
                    test_message,
                    Exception("İlk hata"),
                    FailureReason.PROCESSING_ERROR,
                    "test-topic",
                    0,
                    123
                )
            )
            
            # Wait for retry to be ready (short delay for testing)
            time.sleep(1.5)
            
            # Get retry candidates
            candidates = self.run_async_test(self.dlq_manager.get_retry_candidates())
            self.assertEqual(len(candidates), 1)
            
            # Test successful retry
            retry_count = 0
            async def mock_processor(message):
                nonlocal retry_count
                retry_count += 1
                return True  # Success
            
            processed = self.run_async_test(
                self.dlq_manager.process_retries(mock_processor)
            )
            
            self.assertEqual(processed, 1)
            self.assertEqual(retry_count, 1)
            
            # Check stats after successful retry
            stats = self.dlq_manager.get_stats()
            self.assertEqual(stats['in_retry_queue'], 0)
            self.assertEqual(stats['total_failed'], 0)
            
            print("✅ Retry mechanism test başarılı")
            
        except Exception as e:
            self.fail(f"Retry mechanism test başarısız: {e}")
    
    def test_permanent_failure(self):
        """Kalıcı başarısızlık testi"""
        try:
            test_message = {
                'id': 'test_permanent_001',
                'content': 'Permanent failure test',
                'timestamp': '2024-01-15T10:00:00Z'
            }
            
            # Simulate multiple failures to reach max retries
            for i in range(4):  # max_retries is 3, so 4th attempt should move to permanent failure
                result = self.run_async_test(
                    self.dlq_manager.handle_failed_message(
                        test_message,
                        Exception(f"Hata {i+1}"),
                        FailureReason.PROCESSING_ERROR,
                        "test-topic",
                        0,
                        123
                    )
                )
                
                if i < 3:
                    self.assertTrue(result)  # Should be scheduled for retry
                else:
                    self.assertFalse(result)  # Should be moved to permanent failure
            
            # Check final stats
            stats = self.dlq_manager.get_stats()
            self.assertEqual(stats['total_failed'], 1)
            self.assertEqual(stats['in_retry_queue'], 0)
            
            print("✅ Permanent failure test başarılı")
            
        except Exception as e:
            self.fail(f"Permanent failure test başarısız: {e}")
    
    def test_message_replay(self):
        """Mesaj replay testi"""
        try:
            test_message = {
                'id': 'test_replay_001',
                'content': 'Replay test mesajı',
                'timestamp': '2024-01-15T10:00:00Z'
            }
            
            # Create a failed message
            self.run_async_test(
                self.dlq_manager.handle_failed_message(
                    test_message,
                    Exception("Test hatası"),
                    FailureReason.PROCESSING_ERROR,
                    "test-topic",
                    0,
                    123
                )
            )
            
            # Get the DLQ message
            dlq_messages = self.run_async_test(self.dlq_manager.get_dlq_messages())
            self.assertEqual(len(dlq_messages), 1)
            
            dlq_message = dlq_messages[0]
            
            # Replay the message
            replay_result = self.run_async_test(
                self.dlq_manager.replay_message(dlq_message)
            )
            
            self.assertTrue(replay_result)
            
            # Check that message is removed from queues
            stats = self.dlq_manager.get_stats()
            self.assertEqual(stats['total_failed'], 0)
            self.assertEqual(stats['in_retry_queue'], 0)
            
            print("✅ Message replay test başarılı")
            
        except Exception as e:
            self.fail(f"Message replay test başarısız: {e}")
    
    def test_file_persistence(self):
        """Dosya kalıcılığı testi"""
        try:
            test_message = {
                'id': 'test_persistence_001',
                'content': 'Persistence test mesajı',
                'timestamp': '2024-01-15T10:00:00Z'
            }
            
            # Add message to DLQ
            self.run_async_test(
                self.dlq_manager.handle_failed_message(
                    test_message,
                    Exception("Test hatası"),
                    FailureReason.PROCESSING_ERROR,
                    "test-topic",
                    0,
                    123
                )
            )
            
            # Check that files are created
            failed_file = Path(self.temp_dir) / "failed_messages.jsonl"
            retry_file = Path(self.temp_dir) / "retry_queue.jsonl"
            
            self.assertTrue(retry_file.exists())
            
            # Create new DLQ manager to test loading
            new_dlq_manager = DeadLetterQueueManager(self.dlq_config)
            
            # Check that message was loaded
            stats = new_dlq_manager.get_stats()
            self.assertEqual(stats['in_retry_queue'], 1)
            
            print("✅ File persistence test başarılı")
            
        except Exception as e:
            self.fail(f"File persistence test başarısız: {e}")


class TestDLQKafkaIntegration(unittest.TestCase):
    """DLQ Kafka integration testleri"""
    
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
        self.dlq_config = DLQConfig(
            dlq_topic="test-dlq",
            kafka_bootstrap_servers=self.config['kafka']['bootstrap_servers']
        )
    
    @patch('src.utils.dead_letter_queue.KafkaProducer')
    @patch('src.utils.dead_letter_queue.KafkaConsumer')
    def test_dlq_kafka_integration(self, mock_consumer_class, mock_producer_class):
        """DLQ Kafka integration testi"""
        try:
            # Mock producer
            mock_producer = Mock()
            mock_future = Mock()
            mock_record_metadata = Mock()
            mock_record_metadata.topic = "test-dlq"
            mock_record_metadata.partition = 0
            mock_record_metadata.offset = 456
            
            mock_future.get.return_value = mock_record_metadata
            mock_producer.send.return_value = mock_future
            mock_producer_class.return_value = mock_producer
            
            # Mock consumer
            mock_consumer = Mock()
            mock_record = Mock()
            mock_record.value = {
                'original_topic': "test-topic",
                'original_partition': 0,
                'original_offset': 123,
                'original_key': "test-key",
                'original_value': "test-value",
                'failure_reason': "processing_error",
                'error_message': "Test hatası",
                'failed_at': "2024-01-15T10:00:00Z",
                'retry_count': 1,
                'original_headers': None
            }
            
            mock_consumer.poll.return_value = {
                ('test-dlq', 0): [mock_record]
            }
            mock_consumer_class.return_value = mock_consumer
            
            # Create DLQ instance
            dlq = DeadLetterQueue(self.dlq_config)
            
            # Test sending to DLQ
            result = self.run_async_test(
                dlq.send_to_dlq(
                    original_topic="test-topic",
                    original_partition=0,
                    original_offset=123,
                    original_key="test-key",
                    original_value="test-value",
                    failure_reason=FailureReason.PROCESSING_ERROR,
                    error_message="Test hatası"
                )
            )
            
            self.assertTrue(result)
            mock_producer.send.assert_called_once()
            
            # Test getting DLQ messages
            messages = dlq.get_dlq_messages()
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].original_topic, "test-topic")
            
            # Test message replay
            dlq_message = messages[0]
            replay_result = self.run_async_test(
                dlq.replay_message(dlq_message)
            )
            
            self.assertTrue(replay_result)
            
            # Clean up
            dlq.close()
            
            print("✅ DLQ Kafka integration test başarılı")
            
        except Exception as e:
            self.fail(f"DLQ Kafka integration test başarısız: {e}")


class TestDLQEndToEndIntegration(unittest.TestCase):
    """DLQ End-to-End integration testleri"""
    
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
        self.temp_dir = tempfile.mkdtemp()
        
        # Full system config
        self.system_config = {
            'dead_letter_queue': {
                'path': self.temp_dir,
                'max_retries': 2,
                'retry_delays': [1, 2],
                'enable_retry': True,
                'batch_size': 5
            }
        }
        
        self.kafka_config = KafkaConfig.from_dict(self.config['kafka'])
        self.qdrant_config = self.config['qdrant']
        self.qdrant_config['collection_name'] = 'test_dlq_integration'
    
    def tearDown(self):
        """Test cleanup"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    def test_end_to_end_dlq_flow(self, mock_consumer_class):
        """End-to-end DLQ akışı testi"""
        try:
            # Initialize components
            dlq_manager = DeadLetterQueueManager(self.system_config)
            qdrant_writer = QdrantWriter(self.qdrant_config)
            consumer = KafkaConsumer(self.kafka_config)
            
            # Initialize Qdrant collection
            self.run_async_test(qdrant_writer.initialize_collection())
            
            # Test data
            processed_messages = []
            failed_messages = []
            
            async def message_processor(message):
                """Mock message processor that fails for certain messages"""
                try:
                    if 'fail' in message.get('content', ''):
                        raise Exception("Simulated processing failure")
                    
                    # Simulate embedding creation
                    embedding = [0.1] * 384
                    embeddings_data = [{'vector': embedding, 'metadata': message}]
                    await qdrant_writer.write_embeddings(embeddings_data)
                    
                    processed_messages.append(message)
                    return True
                    
                except Exception as e:
                    # Handle failure with DLQ
                    await dlq_manager.handle_failed_message(
                        message,
                        e,
                        FailureReason.PROCESSING_ERROR,
                        "test-topic",
                        0,
                        len(failed_messages)
                    )
                    failed_messages.append(message)
                    return False
            
            # Mock Kafka consumer
            mock_kafka_consumer = MagicMock()
            test_messages = [
                type('MockMessage', (), {
                    'value': json.dumps({
                        'id': f'msg_{i:03d}',
                        'content': f'Test message {i}' if i % 3 != 0 else f'Test fail message {i}',
                        'timestamp': f'2024-01-15T10:{i:02d}:00Z'
                    }).encode('utf-8')
                }) for i in range(6)
            ]
            
            mock_kafka_consumer.__iter__.return_value = iter(test_messages)
            mock_consumer_class.return_value = mock_kafka_consumer
            
            # Set up consumer handler
            consumer.set_message_handler(message_processor)
            
            # Process messages
            message_count = 0
            try:
                async def consume_messages():
                    nonlocal message_count
                    async for message in consumer._consume_messages():
                        await message_processor(message)
                        message_count += 1
                        if message_count >= 6:
                            break
                
                self.run_async_test(consume_messages())
            except:
                pass
            
            # Check results
            self.assertEqual(len(processed_messages), 4)  # 2 failed out of 6
            self.assertEqual(len(failed_messages), 2)
            
            # Check DLQ stats
            stats = dlq_manager.get_stats()
            self.assertEqual(stats['in_retry_queue'], 2)
            
            # Test retry processing
            time.sleep(1.5)  # Wait for retry delay
            
            retry_processed = self.run_async_test(
                dlq_manager.process_retries(message_processor)
            )
            
            # Retry should still fail for messages with 'fail' in content
            self.assertEqual(retry_processed, 0)
            
            # Check final stats
            final_stats = dlq_manager.get_stats()
            self.assertGreater(final_stats['total_failed'], 0)
            
            print("✅ End-to-end DLQ flow test başarılı:")
            print(f"   - {len(processed_messages)} mesaj başarıyla işlendi")
            print(f"   - {len(failed_messages)} mesaj DLQ'ya gönderildi")
            print(f"   - Final DLQ stats: {final_stats}")
            
            # Clean up
            self.run_async_test(consumer.close())
            dlq_manager.close()
            
        except Exception as e:
            self.fail(f"End-to-end DLQ flow test başarısız: {e}")


class TestDLQErrorScenarios(unittest.TestCase):
    """DLQ hata senaryoları integration testleri"""
    
    def run_async_test(self, coro):
        """Async test helper"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    def setUp(self):
        """Test setup"""
        self.temp_dir = tempfile.mkdtemp()
        
        self.dlq_config = {
            'dead_letter_queue': {
                'path': self.temp_dir,
                'max_retries': 2,
                'retry_delays': [1, 2],
                'enable_retry': True,
                'batch_size': 5
            }
        }
    
    def tearDown(self):
        """Test cleanup"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_corrupted_dlq_files(self):
        """Bozuk DLQ dosyaları testi"""
        try:
            # Create corrupted files
            failed_file = Path(self.temp_dir) / "failed_messages.jsonl"
            retry_file = Path(self.temp_dir) / "retry_queue.jsonl"
            
            # Write invalid JSON
            with open(failed_file, 'w') as f:
                f.write("invalid json line\n")
                f.write('{"valid": "json"}\n')
            
            with open(retry_file, 'w') as f:
                f.write("another invalid line\n")
            
            # DLQ manager should handle corrupted files gracefully
            dlq_manager = DeadLetterQueueManager(self.dlq_config)
            
            # Should initialize without crashing
            stats = dlq_manager.get_stats()
            self.assertIsInstance(stats, dict)
            
            print("✅ Corrupted DLQ files test başarılı")
            
        except Exception as e:
            self.fail(f"Corrupted DLQ files test başarısız: {e}")
    
    def test_disk_space_exhaustion(self):
        """Disk alanı tükenmesi simülasyonu"""
        try:
            dlq_manager = DeadLetterQueueManager(self.dlq_config)
            
            # Mock file write to simulate disk space error
            original_open = open
            
            def mock_open(*args, **kwargs):
                if 'failed_messages.jsonl' in str(args[0]) and 'w' in args[1]:
                    raise OSError("No space left on device")
                return original_open(*args, **kwargs)
            
            with patch('builtins.open', side_effect=mock_open):
                # This should handle the error gracefully
                result = self.run_async_test(
                    dlq_manager.handle_failed_message(
                        {'id': 'test'},
                        Exception("Test error"),
                        FailureReason.PROCESSING_ERROR
                    )
                )
                
                # Should still return a result even if file save fails
                self.assertIsInstance(result, bool)
            
            print("✅ Disk space exhaustion test başarılı")
            
        except Exception as e:
            self.fail(f"Disk space exhaustion test başarısız: {e}")


if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)
