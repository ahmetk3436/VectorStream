"""Dead Letter Queue unit testleri"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

from src.utils.dead_letter_queue import (
    DeadLetterQueue,
    DeadLetterMessage,
    DLQConfig,
    FailureReason,
    get_dlq,
    reset_dlq
)


class TestDeadLetterMessage:
    """DeadLetterMessage testleri"""
    
    def test_create_message(self):
        """Mesaj oluşturma testi"""
        message = DeadLetterMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value="test-value",
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message="Test hatası",
            failed_at="2024-01-15T10:00:00Z",
            retry_count=1
        )
        
        assert message.original_topic == "test-topic"
        assert message.original_partition == 0
        assert message.original_offset == 123
        assert message.original_key == "test-key"
        assert message.original_value == "test-value"
        assert message.failure_reason == FailureReason.PROCESSING_ERROR
        assert message.error_message == "Test hatası"
        assert message.failed_at == "2024-01-15T10:00:00Z"
        assert message.retry_count == 1
    
    def test_to_dict(self):
        """Dict'e dönüştürme testi"""
        message = DeadLetterMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value="test-value",
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message="Test hatası",
            failed_at="2024-01-15T10:00:00Z",
            retry_count=1
        )
        
        data = message.to_dict()
        
        assert data['original_topic'] == "test-topic"
        assert data['failure_reason'] == "processing_error"
        assert data['retry_count'] == 1
    
    def test_from_dict(self):
        """Dict'ten oluşturma testi"""
        data = {
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
        
        message = DeadLetterMessage.from_dict(data)
        
        assert message.original_topic == "test-topic"
        assert message.failure_reason == FailureReason.PROCESSING_ERROR
        assert message.retry_count == 1


class TestDLQConfig:
    """DLQConfig testleri"""
    
    def test_default_config(self):
        """Varsayılan konfigürasyon testi"""
        config = DLQConfig()
        
        assert config.dlq_topic == "dead-letter-queue"
        assert config.max_retry_count == 3
        assert config.kafka_bootstrap_servers == "localhost:9092"
    
    def test_custom_config(self):
        """Özel konfigürasyon testi"""
        config = DLQConfig(
            dlq_topic="custom-dlq",
            max_retry_count=5,
            kafka_bootstrap_servers="custom:9092"
        )
        
        assert config.dlq_topic == "custom-dlq"
        assert config.max_retry_count == 5
        assert config.kafka_bootstrap_servers == "custom:9092"
    
    def test_producer_config(self):
        """Producer konfigürasyonu testi"""
        config = DLQConfig()
        producer_config = config.get_producer_config()
        
        assert producer_config['bootstrap_servers'] == "localhost:9092"
        assert producer_config['acks'] == 'all'
        assert producer_config['retries'] == 3
        assert 'value_serializer' in producer_config
        assert 'key_serializer' in producer_config
    
    def test_consumer_config(self):
        """Consumer konfigürasyonu testi"""
        config = DLQConfig()
        consumer_config = config.get_consumer_config()
        
        assert consumer_config['bootstrap_servers'] == "localhost:9092"
        assert consumer_config['auto_offset_reset'] == 'earliest'
        assert consumer_config['enable_auto_commit'] == True
        assert 'value_deserializer' in consumer_config
        assert 'key_deserializer' in consumer_config


class TestDeadLetterQueue:
    """DeadLetterQueue testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        self.config = DLQConfig(dlq_topic="test-dlq")
        self.dlq = DeadLetterQueue(self.config)
    
    def teardown_method(self):
        """Her test sonrası cleanup"""
        self.dlq.close()
    
    @patch('src.utils.dead_letter_queue.KafkaProducer')
    @pytest.mark.asyncio
    async def test_send_to_dlq_success(self, mock_producer_class):
        """DLQ'ya başarılı mesaj gönderme testi"""
        # Mock producer setup
        mock_producer = Mock()
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = "test-dlq"
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 456
        
        mock_future.get.return_value = mock_record_metadata
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        # Test
        result = await self.dlq.send_to_dlq(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value="test-value",
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message="Test hatası"
        )
        
        assert result == True
        mock_producer.send.assert_called_once()
        
        # Send çağrısının parametrelerini kontrol et
        call_args = mock_producer.send.call_args
        assert call_args[0][0] == "test-dlq"  # topic
        assert call_args[1]['key'] == "test-key"
        
        # Value'nun DeadLetterMessage olduğunu kontrol et
        sent_value = call_args[1]['value']
        assert sent_value['original_topic'] == "test-topic"
        assert sent_value['failure_reason'] == "processing_error"
    
    @patch('src.utils.dead_letter_queue.KafkaProducer')
    @pytest.mark.asyncio
    async def test_send_to_dlq_failure(self, mock_producer_class):
        """DLQ'ya mesaj gönderme hatası testi"""
        # Mock producer setup
        mock_producer = Mock()
        mock_producer.send.side_effect = Exception("Kafka hatası")
        mock_producer_class.return_value = mock_producer
        
        # Test
        result = await self.dlq.send_to_dlq(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value="test-value",
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message="Test hatası"
        )
        
        assert result == False
    
    @patch('src.utils.dead_letter_queue.KafkaConsumer')
    def test_get_dlq_messages_success(self, mock_consumer_class):
        """DLQ mesajları alma başarı testi"""
        # Mock consumer setup
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
        
        # Test
        messages = self.dlq.get_dlq_messages()
        
        assert len(messages) == 1
        assert messages[0].original_topic == "test-topic"
        assert messages[0].failure_reason == FailureReason.PROCESSING_ERROR
    
    @patch('src.utils.dead_letter_queue.KafkaConsumer')
    def test_get_dlq_messages_empty(self, mock_consumer_class):
        """DLQ mesajları alma boş sonuç testi"""
        # Mock consumer setup
        mock_consumer = Mock()
        mock_consumer.poll.return_value = {}
        mock_consumer_class.return_value = mock_consumer
        
        # Test
        messages = self.dlq.get_dlq_messages()
        
        assert len(messages) == 0
    
    @patch('src.utils.dead_letter_queue.KafkaProducer')
    @pytest.mark.asyncio
    async def test_replay_message_success(self, mock_producer_class):
        """Mesaj replay başarı testi"""
        # Mock producer setup
        mock_producer = Mock()
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = "test-topic"
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 789
        
        mock_future.get.return_value = mock_record_metadata
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        # Test mesajı
        dlq_message = DeadLetterMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value="test-value",
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message="Test hatası",
            failed_at="2024-01-15T10:00:00Z",
            retry_count=1
        )
        
        # Test
        result = await self.dlq.replay_message(dlq_message)
        
        assert result == True
        mock_producer.send.assert_called_once()
        
        # Send çağrısının parametrelerini kontrol et
        call_args = mock_producer.send.call_args
        assert call_args[0][0] == "test-topic"  # topic
        assert call_args[1]['key'] == "test-key"
        assert call_args[1]['value'] == b"test-value"
        
        # Headers'ı kontrol et
        headers = call_args[1]['headers']
        header_dict = {k: v.decode('utf-8') for k, v in headers}
        assert header_dict['dlq_replay'] == 'true'
        assert header_dict['original_failure_reason'] == 'processing_error'
    
    @patch('src.utils.dead_letter_queue.KafkaProducer')
    @pytest.mark.asyncio
    async def test_replay_message_custom_topic(self, mock_producer_class):
        """Mesaj replay özel topic testi"""
        # Mock producer setup
        mock_producer = Mock()
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = "custom-topic"
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 789
        
        mock_future.get.return_value = mock_record_metadata
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer
        
        # Test mesajı
        dlq_message = DeadLetterMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key="test-key",
            original_value="test-value",
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message="Test hatası",
            failed_at="2024-01-15T10:00:00Z",
            retry_count=1
        )
        
        # Test
        result = await self.dlq.replay_message(dlq_message, target_topic="custom-topic")
        
        assert result == True
        
        # Send çağrısının topic'ini kontrol et
        call_args = mock_producer.send.call_args
        assert call_args[0][0] == "custom-topic"  # topic
    
    @patch('src.utils.dead_letter_queue.KafkaConsumer')
    def test_get_dlq_stats(self, mock_consumer_class):
        """DLQ istatistikleri testi"""
        # Mock consumer setup
        mock_consumer = Mock()
        mock_consumer.partitions_for_topic.return_value = {0, 1}
        mock_consumer.assignment.return_value = [('test-dlq', 0)]
        mock_consumer.highwater.return_value = 100
        mock_consumer.position.return_value = 50
        mock_consumer_class.return_value = mock_consumer
        
        # Test
        stats = self.dlq.get_dlq_stats()
        
        assert 'total_messages' in stats
        assert 'partitions' in stats
    
    def test_close(self):
        """Kaynakları kapatma testi"""
        # Mock producer ve consumer
        mock_producer = Mock()
        mock_consumer = Mock()
        
        self.dlq._producer = mock_producer
        self.dlq._consumer = mock_consumer
        
        # Test
        self.dlq.close()
        
        mock_producer.close.assert_called_once()
        mock_consumer.close.assert_called_once()
        assert self.dlq._producer is None
        assert self.dlq._consumer is None


class TestGlobalDLQ:
    """Global DLQ fonksiyonları testleri"""
    
    def teardown_method(self):
        """Her test sonrası cleanup"""
        reset_dlq()
    
    def test_get_dlq_default(self):
        """Varsayılan DLQ alma testi"""
        dlq = get_dlq()
        
        assert isinstance(dlq, DeadLetterQueue)
        assert dlq.config.dlq_topic == "dead-letter-queue"
    
    def test_get_dlq_custom_config(self):
        """Özel konfigürasyon ile DLQ alma testi"""
        config = DLQConfig(dlq_topic="custom-dlq")
        dlq = get_dlq(config)
        
        assert isinstance(dlq, DeadLetterQueue)
        assert dlq.config.dlq_topic == "custom-dlq"
    
    def test_get_dlq_singleton(self):
        """DLQ singleton davranışı testi"""
        dlq1 = get_dlq()
        dlq2 = get_dlq()
        
        assert dlq1 is dlq2
    
    def test_reset_dlq(self):
        """DLQ sıfırlama testi"""
        dlq1 = get_dlq()
        reset_dlq()
        dlq2 = get_dlq()
        
        assert dlq1 is not dlq2


class TestFailureReason:
    """FailureReason enum testleri"""
    
    def test_failure_reasons(self):
        """Hata nedenleri testi"""
        assert FailureReason.PROCESSING_ERROR.value == "processing_error"
        assert FailureReason.TIMEOUT_ERROR.value == "timeout_error"
        assert FailureReason.VALIDATION_ERROR.value == "validation_error"
        assert FailureReason.CIRCUIT_BREAKER_OPEN.value == "circuit_breaker_open"
        assert FailureReason.UNKNOWN_ERROR.value == "unknown_error"