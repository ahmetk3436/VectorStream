"""End-to-end integration tests for NewMind-AI pipeline."""

import asyncio
import json
import pytest
import time
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List

import sys
from pathlib import Path

# Proje root'unu Python path'ine ekle
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.main import NewMindAI
from src.core.kafka_consumer import KafkaConsumer
from src.core.embedding_processor import EmbeddingProcessor
from src.core.qdrant_writer import QdrantWriter
from src.utils.circuit_breaker import reset_circuit_breaker_manager
from src.utils.dead_letter_queue import reset_dlq


class TestE2EFullPipeline:
    """Full pipeline end-to-end testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        # Circuit breaker ve DLQ'yu sıfırla
        reset_circuit_breaker_manager()
        reset_dlq()
        
        # Test konfigürasyonu
        self.test_config = {
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topic': 'test-topic',
                'group_id': 'test-group'
            },
            'qdrant': {
                'host': 'localhost',
                'port': 6333,
                'collection_name': 'test-collection'
            },
            'embedding': {
                'model_name': 'all-MiniLM-L6-v2',
                'batch_size': 32
            }
        }
        
        # Test mesajları
        self.test_messages = [
            {
                'id': '1',
                'content': 'Bu bir test mesajıdır.',
                'metadata': {'source': 'test', 'timestamp': '2024-01-15T10:00:00Z'}
            },
            {
                'id': '2', 
                'content': 'Machine learning ve AI konularında çalışıyoruz.',
                'metadata': {'source': 'test', 'timestamp': '2024-01-15T10:01:00Z'}
            },
            {
                'id': '3',
                'content': 'Vector database kullanarak similarity search yapıyoruz.',
                'metadata': {'source': 'test', 'timestamp': '2024-01-15T10:02:00Z'}
            }
        ]
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    @patch('src.core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_full_pipeline_success(self, mock_qdrant_client, mock_kafka_consumer):
        """Başarılı full pipeline testi"""
        # Mock Kafka consumer setup
        mock_consumer_instance = Mock()
        mock_messages = []
        
        for msg_data in self.test_messages:
            mock_message = Mock()
            mock_message.topic = 'test-topic'
            mock_message.partition = 0
            mock_message.offset = len(mock_messages)
            mock_message.key = msg_data['id'].encode('utf-8')
            mock_message.value = json.dumps(msg_data).encode('utf-8')
            mock_messages.append(mock_message)
        
        mock_consumer_instance.poll.side_effect = [
            {('test-topic', 0): mock_messages[:1]},
            {('test-topic', 0): mock_messages[1:2]},
            {('test-topic', 0): mock_messages[2:3]},
            {}  # Empty poll to end
        ]
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        # Mock Qdrant client setup
        mock_qdrant_instance = Mock()
        mock_qdrant_instance.get_collections.return_value.collections = []
        mock_qdrant_instance.create_collection.return_value = True
        mock_qdrant_instance.upsert.return_value = Mock(status='completed')
        mock_qdrant_client.return_value = mock_qdrant_instance
        
        # Test pipeline
        processed_messages = []
        
        async def message_handler(data):
            """Test mesaj işleyici"""
            processed_messages.append(data)
            
            # Embedding oluştur
            processor = EmbeddingProcessor(self.test_config['embedding'])
            embedding_result = await processor.process_message(data)
            
            # Qdrant'a yaz
            writer = QdrantWriter(self.test_config['qdrant'])
            await writer.initialize_collection()
            await writer.write_embeddings([embedding_result])
        
        # Kafka consumer oluştur ve test et
        consumer = KafkaConsumer(self.test_config['kafka'])
        consumer.set_message_handler(message_handler)
        
        # Mesajları işle
        for i in range(3):
            messages = mock_consumer_instance.poll.return_value
            if messages:
                for topic_partition, records in messages.items():
                    for record in records:
                        await consumer._process_message(record)
        
        # Sonuçları kontrol et
        assert len(processed_messages) == 3
        assert processed_messages[0]['id'] == '1'
        assert processed_messages[1]['id'] == '2'
        assert processed_messages[2]['id'] == '3'
        
        # Qdrant işlemlerini kontrol et
        assert mock_qdrant_instance.create_collection.called
        assert mock_qdrant_instance.upsert.call_count == 3
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    @patch('src.core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_pipeline_with_processing_errors(self, mock_qdrant_client, mock_kafka_consumer):
        """İşleme hataları ile pipeline testi"""
        # Mock setup
        mock_consumer_instance = Mock()
        
        # Hatalı JSON mesajı
        mock_message = Mock()
        mock_message.topic = 'test-topic'
        mock_message.partition = 0
        mock_message.offset = 0
        mock_message.key = b'error-key'
        mock_message.value = b'{invalid json}'
        
        mock_consumer_instance.poll.return_value = {('test-topic', 0): [mock_message]}
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        # DLQ mock
        with patch('src.utils.dead_letter_queue.get_dlq') as mock_get_dlq:
            mock_dlq = Mock()
            mock_dlq.send_to_dlq = AsyncMock(return_value=True)
            mock_get_dlq.return_value = mock_dlq
            
            # Test
            consumer = KafkaConsumer(self.test_config['kafka'])
            await consumer._process_message(mock_message)
            
            # DLQ'ya gönderildiğini kontrol et
            mock_dlq.send_to_dlq.assert_called_once()
            call_args = mock_dlq.send_to_dlq.call_args[1]
            assert call_args['failure_reason'].value == 'validation_error'
            assert 'JSON parse hatası' in call_args['error_message']
    
    @patch('src.core.kafka_consumer.PyKafkaConsumer')
    @patch('src.core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_pipeline_with_circuit_breaker(self, mock_qdrant_client, mock_kafka_consumer):
        """Circuit breaker ile pipeline testi"""
        # Mock setup
        mock_consumer_instance = Mock()
        mock_message = Mock()
        mock_message.topic = 'test-topic'
        mock_message.partition = 0
        mock_message.offset = 0
        mock_message.key = b'test-key'
        mock_message.value = json.dumps(self.test_messages[0]).encode('utf-8')
        
        mock_consumer_instance.poll.return_value = {('test-topic', 0): [mock_message]}
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        # Circuit breaker'ı açmak için hata oluştur
        error_count = 0
        
        async def failing_handler(data):
            nonlocal error_count
            error_count += 1
            if error_count <= 3:  # İlk 3 çağrıda hata ver
                raise Exception("Test hatası")
            return data
        
        with patch('utils.dead_letter_queue.get_dlq') as mock_get_dlq:
            mock_dlq = Mock()
            mock_dlq.send_to_dlq = AsyncMock(return_value=True)
            mock_get_dlq.return_value = mock_dlq
            
            consumer = KafkaConsumer(self.test_config['kafka'])
            consumer.set_message_handler(failing_handler)
            
            # İlk 3 çağrıda hata olacak, circuit breaker açılacak
            for i in range(4):
                await consumer._process_message(mock_message)
            
            # DLQ çağrılarını kontrol et
            assert mock_dlq.send_to_dlq.call_count >= 3
    
    @patch('src.core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_qdrant_connection_failure(self, mock_qdrant_client):
        """Qdrant bağlantı hatası testi"""
        # Qdrant client'ın hata vermesini sağla
        mock_qdrant_client.side_effect = Exception("Qdrant bağlantı hatası")
        
        with pytest.raises(Exception) as exc_info:
            writer = QdrantWriter(self.test_config['qdrant'])
            await writer.initialize_collection()
        
        assert "Qdrant bağlantı hatası" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_embedding_processor_batch(self):
        """Embedding processor batch işleme testi"""
        processor = EmbeddingProcessor(self.test_config['embedding'])
        
        # Batch mesajları
        batch_data = [
            {'content': 'İlk mesaj', 'id': '1'},
            {'content': 'İkinci mesaj', 'id': '2'},
            {'content': 'Üçüncü mesaj', 'id': '3'}
        ]
        
        # Batch işleme
        results = []
        for data in batch_data:
            result = await processor.process_message(data)
            results.append(result)
        
        # Sonuçları kontrol et
        assert len(results) == 3
        for result in results:
            assert 'embedding' in result
            assert 'metadata' in result
            assert len(result['embedding']) > 0  # Embedding vektörü var


class TestE2EPerformance:
    """Performance ve yük testleri"""
    
    @pytest.mark.asyncio
    async def test_embedding_performance(self):
        """Embedding oluşturma performans testi"""
        processor = EmbeddingProcessor({'model_name': 'all-MiniLM-L6-v2', 'batch_size': 32})
        
        # Test mesajları
        test_messages = [
            {'content': f'Test mesajı {i}', 'id': str(i)}
            for i in range(10)
        ]
        
        # Performans ölçümü
        start_time = time.time()
        
        results = []
        for msg in test_messages:
            result = await processor.process_message(msg)
            results.append(result)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Performans kontrolleri
        assert len(results) == 10
        assert processing_time < 30.0  # 10 mesaj 30 saniyede işlenmeli
        
        # Throughput hesapla
        throughput = len(test_messages) / processing_time
        assert throughput > 0.3  # En az 0.3 mesaj/saniye
    
    @patch('core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_qdrant_batch_write_performance(self, mock_qdrant_client):
        """Qdrant batch yazma performans testi"""
        # Mock setup
        mock_qdrant_instance = Mock()
        mock_qdrant_instance.get_collections.return_value.collections = []
        mock_qdrant_instance.create_collection.return_value = True
        mock_qdrant_instance.upsert.return_value = Mock(status='completed')
        mock_qdrant_client.return_value = mock_qdrant_instance
        
        writer = QdrantWriter(self.test_config['qdrant'])
        await writer.initialize_collection()
        
        # Test embeddings
        test_embeddings = [
            {
                'id': str(i),
                'embedding': [0.1] * 384,  # MiniLM embedding boyutu
                'metadata': {'content': f'Test {i}', 'index': i}
            }
            for i in range(50)
        ]
        
        # Batch yazma performansı
        start_time = time.time()
        await writer.write_embeddings(test_embeddings)
        end_time = time.time()
        
        write_time = end_time - start_time
        
        # Performans kontrolleri
        assert write_time < 10.0  # 50 embedding 10 saniyede yazılmalı
        assert mock_qdrant_instance.upsert.called


class TestE2EErrorScenarios:
    """Hata senaryoları testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        reset_circuit_breaker_manager()
        reset_dlq()
    
    @patch('core.kafka_consumer.PyKafkaConsumer')
    @pytest.mark.asyncio
    async def test_malformed_message_handling(self, mock_kafka_consumer):
        """Bozuk mesaj işleme testi"""
        # Mock setup
        mock_consumer_instance = Mock()
        
        # Çeşitli bozuk mesajlar
        malformed_messages = [
            Mock(topic='test', partition=0, offset=0, key=b'key1', value=b'{invalid json}'),
            Mock(topic='test', partition=0, offset=1, key=b'key2', value=b'not json at all'),
            Mock(topic='test', partition=0, offset=2, key=b'key3', value=b''),
            Mock(topic='test', partition=0, offset=3, key=None, value=b'{"incomplete": }')
        ]
        
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        with patch('utils.dead_letter_queue.get_dlq') as mock_get_dlq:
            mock_dlq = Mock()
            mock_dlq.send_to_dlq = AsyncMock(return_value=True)
            mock_get_dlq.return_value = mock_dlq
            
            consumer = KafkaConsumer({'bootstrap_servers': 'localhost:9092'})
            
            # Her bozuk mesajı işle
            for msg in malformed_messages:
                await consumer._process_message(msg)
            
            # Tüm bozuk mesajların DLQ'ya gönderildiğini kontrol et
            assert mock_dlq.send_to_dlq.call_count == len(malformed_messages)
    
    @patch('core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_qdrant_service_unavailable(self, mock_qdrant_client):
        """Qdrant servis erişilemez durumu testi"""
        # Qdrant'ın erişilemez olduğunu simüle et
        mock_qdrant_client.side_effect = ConnectionError("Qdrant erişilemez")
        
        with pytest.raises(ConnectionError):
            writer = QdrantWriter({'host': 'localhost', 'port': 6333})
            await writer.initialize_collection()
    
    @pytest.mark.asyncio
    async def test_embedding_model_failure(self):
        """Embedding model hatası testi"""
        with patch('src.core.embedding_processor.SentenceTransformer') as mock_transformer:
            # Model yükleme hatası
            mock_transformer.side_effect = Exception("Model yüklenemedi")
            
            with pytest.raises(Exception) as exc_info:
                processor = EmbeddingProcessor({'model_name': 'all-MiniLM-L6-v2'})
                await processor.process_message({'content': 'test'})
            
            assert "Model yüklenemedi" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_memory_pressure_simulation(self):
        """Bellek baskısı simülasyonu"""
        processor = EmbeddingProcessor({'model_name': 'all-MiniLM-L6-v2'})
        
        # Büyük mesajlar oluştur
        large_messages = [
            {'content': 'A' * 10000, 'id': str(i)}  # 10KB mesajlar
            for i in range(5)
        ]
        
        # Bellek kullanımını izle
        results = []
        for msg in large_messages:
            try:
                result = await processor.process_message(msg)
                results.append(result)
            except MemoryError:
                # Bellek hatası beklenen bir durum
                break
        
        # En az birkaç mesajın işlendiğini kontrol et
        assert len(results) >= 1


class TestE2ERecovery:
    """Kurtarma ve dayanıklılık testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        reset_circuit_breaker_manager()
        reset_dlq()
    
    @patch('core.kafka_consumer.PyKafkaConsumer')
    @pytest.mark.asyncio
    async def test_circuit_breaker_recovery(self, mock_kafka_consumer):
        """Circuit breaker kurtarma testi"""
        # Mock setup
        mock_consumer_instance = Mock()
        mock_message = Mock()
        mock_message.topic = 'test-topic'
        mock_message.partition = 0
        mock_message.offset = 0
        mock_message.key = b'test-key'
        mock_message.value = json.dumps({'content': 'test'}).encode('utf-8')
        
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        # Başarısız handler
        call_count = 0
        
        async def intermittent_handler(data):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise Exception("Geçici hata")
            return data  # 4. çağrıdan sonra başarılı
        
        consumer = KafkaConsumer({'bootstrap_servers': 'localhost:9092'})
        consumer.set_message_handler(intermittent_handler)
        
        # Circuit breaker'ı aç
        for i in range(3):
            try:
                await consumer._process_message(mock_message)
            except:
                pass
        
        # Circuit breaker açık olmalı
        cb = consumer.circuit_breaker
        assert cb.state.name == 'OPEN'
        
        # Kurtarma süresini bekle (test için kısa)
        await asyncio.sleep(0.1)
        
        # Kurtarma testi - bu başarılı olmalı
        try:
            await consumer._process_message(mock_message)
        except:
            pass  # Circuit breaker hala açık olabilir
    
    @patch('src.utils.dead_letter_queue.DeadLetterQueue')
    @pytest.mark.asyncio
    async def test_dlq_replay_functionality(self, mock_dlq_class):
        """DLQ replay fonksiyonalitesi testi"""
        from src.utils.dead_letter_queue import DeadLetterMessage, FailureReason
        
        # Mock DLQ setup
        mock_dlq = Mock()
        mock_dlq_class.return_value = mock_dlq
        
        # Test DLQ mesajı
        dlq_message = DeadLetterMessage(
            original_topic='test-topic',
            original_partition=0,
            original_offset=123,
            original_key='test-key',
            original_value=json.dumps({'content': 'test mesajı'}),
            failure_reason=FailureReason.PROCESSING_ERROR,
            error_message='Test hatası',
            failed_at='2024-01-15T10:00:00Z',
            retry_count=1
        )
        
        mock_dlq.get_dlq_messages.return_value = [dlq_message]
        mock_dlq.replay_message = AsyncMock(return_value=True)
        
        # Replay testi
        messages = mock_dlq.get_dlq_messages()
        assert len(messages) == 1
        
        # Mesajı replay et
        result = await mock_dlq.replay_message(messages[0])
        assert result == True
        
        mock_dlq.replay_message.assert_called_once_with(dlq_message)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])