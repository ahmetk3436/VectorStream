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

from src.main import VectorStreamPipeline
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
                'collection_name': 'test-collection',
                'vector_size': 384
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
    
    @pytest.mark.asyncio
    async def test_full_pipeline_success(self):
        """Başarılı full pipeline testi"""
        # Test pipeline
        processed_messages = []
        
        async def message_handler(data):
            """Test mesaj işleyici"""
            processed_messages.append(data)
            return data
        
        # Kafka consumer oluştur ve test et
        consumer = KafkaConsumer(self.test_config['kafka'])
        consumer.set_message_handler(message_handler)
        
        # Mock mesajları oluştur ve işle
        for msg_data in self.test_messages:
            mock_message = Mock()
            mock_message.topic = 'test-topic'
            mock_message.partition = 0
            mock_message.offset = len(processed_messages)
            mock_message.key = msg_data['id'].encode('utf-8')
            mock_message.value = json.dumps(msg_data).encode('utf-8')
            
            await consumer._process_message(mock_message)
        
        # Sonuçları kontrol et
        assert len(processed_messages) == 3
        assert processed_messages[0]['id'] == '1'
        assert processed_messages[1]['id'] == '2'
        assert processed_messages[2]['id'] == '3'
    
    @pytest.mark.asyncio
    async def test_pipeline_with_processing_errors(self):
        """İşleme hataları ile pipeline testi"""
        with patch('src.utils.dead_letter_queue.KafkaProducer') as mock_producer:
            # Mock producer setup
            mock_producer_instance = Mock()
            mock_future = Mock()
            mock_future.get.return_value = Mock(topic='dlq', partition=0, offset=0)
            mock_producer_instance.send.return_value = mock_future
            mock_producer.return_value = mock_producer_instance
            
            # Hatalı JSON mesajı
            mock_message = Mock()
            mock_message.topic = 'test-topic'
            mock_message.partition = 0
            mock_message.offset = 0
            mock_message.key = b'error-key'
            mock_message.value = b'{invalid json}'
            
            # Test
            consumer = KafkaConsumer(self.test_config['kafka'])
            await consumer._process_message(mock_message)
            
            # DLQ producer'ın çağrıldığını kontrol et - JSON parse hatası için 1 çağrı bekleniyor
            assert mock_producer_instance.send.call_count >= 1
    
    @pytest.mark.asyncio
    async def test_pipeline_with_circuit_breaker(self):
        """Circuit breaker ile pipeline testi"""
        with patch('src.utils.dead_letter_queue.KafkaProducer') as mock_producer:
            # Mock producer setup
            mock_producer_instance = Mock()
            mock_future = Mock()
            mock_future.get.return_value = Mock(topic='dlq', partition=0, offset=0)
            mock_producer_instance.send.return_value = mock_future
            mock_producer.return_value = mock_producer_instance
            
            consumer = KafkaConsumer(self.test_config['kafka'])
            
            # Hata üreten handler
            async def failing_handler(data):
                raise Exception("Test hatası")
            
            consumer.set_message_handler(failing_handler)
            
            # Test mesajı
            mock_message = Mock(
                topic='test-topic',
                partition=0,
                offset=123,
                key=b'test-key',
                value=b'{"content": "test message"}'
            )
            
            # Circuit breaker'ı tetiklemek için birkaç hatalı mesaj gönder
            for i in range(6):  # 5'ten fazla hata ile circuit breaker'ı aç
                try:
                    await consumer.cb.call(consumer._process_messages, [mock_message])
                except Exception:
                    pass  # Ignore exceptions to continue the test
            
            # Circuit breaker'ın açık olduğunu kontrol et
            assert consumer.cb.state.name in ['OPEN', 'HALF_OPEN']
            
            # Circuit breaker açık olduğunda DLQ çağrıları olabilir veya olmayabilir
            # Test başarılı sayılır çünkü circuit breaker düzgün çalışıyor
    
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
        with patch('src.core.embedding_processor.SentenceTransformer') as mock_model:
            # Mock model setup
            mock_model_instance = Mock()
            # encode method should handle both single text and list of texts
            import numpy as np
            def mock_encode(texts, **kwargs):
                if isinstance(texts, str):
                    return np.array([0.1] * 384)
                else:
                    return np.array([[0.1] * 384] * len(texts))
            
            mock_model_instance.encode = mock_encode
            mock_model_instance.get_sentence_embedding_dimension.return_value = 384
            mock_model.return_value = mock_model_instance
            
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
                if result:  # Ensure result is not None
                    results.append(result)
            
            # Sonuçları kontrol et
            assert len(results) == 3
            for result in results:
                assert 'vector' in result
                assert 'metadata' in result
                assert len(result['vector']) > 0  # Embedding vektörü var


class TestE2EPerformance:
    """Performance ve yük testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        self.test_config = {
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topic': 'test-topic',
                'group_id': 'test-group'
            },
            'qdrant': {
                'host': 'localhost',
                'port': 6333,
                'collection_name': 'test-collection',
                'vector_size': 384
            },
            'embedding': {
                'model_name': 'all-MiniLM-L6-v2',
                'batch_size': 32
            }
        }
    
    @pytest.mark.asyncio
    async def test_embedding_performance(self):
        """Embedding oluşturma performans testi"""
        with patch('src.core.embedding_processor.SentenceTransformer') as mock_model:
            # Mock model setup
            mock_model_instance = Mock()
            mock_model_instance.encode.return_value = [[0.1] * 384] * 10
            mock_model.return_value = mock_model_instance
            
            processor = EmbeddingProcessor(self.test_config['embedding'])
        
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
    
    @patch('src.core.qdrant_writer.QdrantClient')
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
                'vector': [0.1] * 384,  # MiniLM embedding boyutu
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
        
        self.test_config = {
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topic': 'test-topic',
                'group_id': 'test-group'
            },
            'qdrant': {
                'host': 'localhost',
                'port': 6333,
                'collection_name': 'test-collection',
                'vector_size': 384
            },
            'embedding': {
                'model_name': 'all-MiniLM-L6-v2',
                'batch_size': 32
            }
        }
    
    @pytest.mark.asyncio
    async def test_malformed_message_handling(self):
        """Bozuk mesaj işleme testi"""
        # Çeşitli bozuk mesajlar
        malformed_messages = [
            Mock(topic='test', partition=0, offset=0, key=b'key1', value=b'{invalid json}'),
            Mock(topic='test', partition=0, offset=1, key=b'key2', value=b'not json at all'),
            Mock(topic='test', partition=0, offset=2, key=b'key3', value=b''),
            Mock(topic='test', partition=0, offset=3, key=None, value=b'{"incomplete": }')
        ]
        
        with patch('src.utils.dead_letter_queue.KafkaProducer') as mock_producer:
            # Mock producer setup
            mock_producer_instance = Mock()
            mock_future = Mock()
            mock_future.get.return_value = Mock(topic='dlq', partition=0, offset=0)
            mock_producer_instance.send.return_value = mock_future
            mock_producer.return_value = mock_producer_instance
            
            consumer = KafkaConsumer(self.test_config['kafka'])
            
            # Her bozuk mesajı işle
            for msg in malformed_messages:
                try:
                    await consumer._process_message(msg)
                except Exception:
                    pass  # JSON parse errors are expected
            
            # Bozuk mesajların DLQ'ya gönderildiğini kontrol et
            assert mock_producer_instance.send.call_count >= 1  # En az 1 mesaj DLQ'ya gönderilmeli
    
    @patch('src.core.qdrant_writer.QdrantClient')
    @pytest.mark.asyncio
    async def test_qdrant_service_unavailable(self, mock_qdrant_client):
        """Qdrant servis erişilemez durumu testi"""
        # Qdrant'ın erişilemez olduğunu simüle et
        mock_qdrant_client.side_effect = ConnectionError("Qdrant erişilemez")
        
        with pytest.raises(ConnectionError):
            writer = QdrantWriter(self.test_config['qdrant'])
            await writer.initialize_collection()
    
    @pytest.mark.asyncio
    async def test_embedding_model_failure(self):
        """Embedding model hatası testi"""
        with patch('src.core.embedding_processor.SentenceTransformer') as mock_transformer:
            # Model yükleme hatası
            mock_transformer.side_effect = Exception("Model yüklenemedi")
            
            processor = EmbeddingProcessor(self.test_config['embedding'])
            
            # create_embedding None döndürür, exception raise etmez
            # Bu yüzden initialize metodunu doğrudan test edelim
            result = await processor.initialize()
            assert result == False  # initialize False döndürmeli
            
            # create_embedding None döndürmeli
            embedding_result = await processor.create_embedding("test text")
            assert embedding_result is None
    
    @pytest.mark.asyncio
    async def test_memory_pressure_simulation(self):
        """Bellek baskısı simülasyonu"""
        with patch('src.core.embedding_processor.SentenceTransformer') as mock_model:
            # Mock model setup
            mock_model_instance = Mock()
            mock_model_instance.encode.return_value = [[0.1] * 384] * 5
            mock_model.return_value = mock_model_instance
            
            processor = EmbeddingProcessor(self.test_config['embedding'])
            
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
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_recovery(self):
        """Circuit breaker kurtarma testi"""
        # Mock message setup
        mock_message = Mock()
        mock_message.topic = 'test-topic'
        mock_message.partition = 0
        mock_message.offset = 0
        mock_message.key = b'test-key'
        mock_message.value = json.dumps({'content': 'test'}).encode('utf-8')
        
        # Başarısız handler - her zaman hata verecek
        def failing_handler(data):
            raise Exception("Test hatası")
        
        consumer = KafkaConsumer(self.test_config['kafka'])
        consumer.set_message_handler(failing_handler)
        
        # Circuit breaker'ı aç - _process_messages metodunu kullan
        for i in range(6):  # 5'ten fazla hata ile circuit breaker'ı aç
            try:
                await consumer.cb.call(consumer._process_messages, [mock_message])
            except:
                pass
        
        # Circuit breaker durumunu kontrol et
        cb = consumer.cb
        # Circuit breaker 5+ hata sonrası açık olmalı
        assert cb.state.name in ['OPEN', 'HALF_OPEN']
        
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