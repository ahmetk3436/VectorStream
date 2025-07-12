import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from prometheus_client import CollectorRegistry, REGISTRY
from src.monitoring.prometheus_metrics import PrometheusMetrics, MetricsServer


class TestPrometheusMetrics:
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a separate registry for testing to avoid conflicts
        self.test_registry = CollectorRegistry()
        self.metrics = PrometheusMetrics(registry=self.test_registry)
    
    def test_prometheus_metrics_initialization(self):
        """Test PrometheusMetrics initialization."""
        self.setUp()
        
        # Check that all metrics are initialized
        assert self.metrics.system_info is not None
        assert self.metrics.kafka_messages_consumed is not None
        assert self.metrics.kafka_messages_processed is not None
        assert self.metrics.kafka_messages_failed is not None
        assert self.metrics.kafka_consumer_lag is not None
        assert self.metrics.kafka_connection_status is not None
        assert self.metrics.qdrant_operations is not None
        assert self.metrics.qdrant_operation_duration is not None
        assert self.metrics.qdrant_collection_points is not None
        assert self.metrics.qdrant_search_results is not None
        assert self.metrics.qdrant_connection_status is not None
        assert self.metrics.embedding_generation_duration is not None
        assert self.metrics.embedding_generation_total is not None
        assert self.metrics.system_cpu_usage is not None
        assert self.metrics.system_memory_usage is not None
        assert self.metrics.system_disk_usage is not None
        assert self.metrics.health_check_status is not None
        assert self.metrics.health_check_duration is not None
        assert self.metrics.application_uptime is not None
        assert self.metrics.error_rate is not None
    
    def test_record_kafka_message_consumed(self):
        """Test recording Kafka message consumption."""
        self.setUp()
        
        topic = "test-topic"
        partition = "0"
        
        # Record message consumption
        self.metrics.record_kafka_message_consumed(topic, partition)
        
        # Get metric value
        metric_value = self.metrics.kafka_messages_consumed.labels(
            topic=topic, partition=partition
        )._value._value
        
        assert metric_value == 1.0
    
    def test_record_kafka_message_processed(self):
        """Test recording Kafka message processing."""
        self.setUp()
        
        topic = "test-topic"
        status = "success"
        
        # Record message processing
        self.metrics.record_kafka_message_processed(topic, status)
        
        # Get metric value
        metric_value = self.metrics.kafka_messages_processed.labels(
            topic=topic, status=status
        )._value._value
        
        assert metric_value == 1.0
    
    def test_record_kafka_message_failed(self):
        """Test recording Kafka message failure."""
        self.setUp()
        
        topic = "test-topic"
        error_type = "processing_error"
        
        # Record message failure
        self.metrics.record_kafka_message_failed(topic, error_type)
        
        # Get metric value
        metric_value = self.metrics.kafka_messages_failed.labels(
            topic=topic, error_type=error_type
        )._value._value
        
        assert metric_value == 1.0
    
    def test_update_kafka_consumer_lag(self):
        """Test updating Kafka consumer lag."""
        self.setUp()
        
        topic = "test-topic"
        partition = "0"
        lag = 100
        
        # Update consumer lag
        self.metrics.update_kafka_consumer_lag(topic, partition, lag)
        
        # Get metric value
        metric_value = self.metrics.kafka_consumer_lag.labels(
            topic=topic, partition=partition
        )._value._value
        
        assert metric_value == 100.0
    
    def test_update_kafka_connection_status(self):
        """Test updating Kafka connection status."""
        self.setUp()
        
        # Update connection status to connected
        self.metrics.update_kafka_connection_status(True)
        
        # Get metric value
        metric_value = self.metrics.kafka_connection_status._value._value
        
        assert metric_value == 1.0
        
        # Update connection status to disconnected
        self.metrics.update_kafka_connection_status(False)
        
        # Get metric value
        metric_value = self.metrics.kafka_connection_status._value._value
        
        assert metric_value == 0.0
    
    def test_record_qdrant_operation(self):
        """Test recording Qdrant operation."""
        self.setUp()
        
        operation = "search"
        collection = "test-collection"
        status = "success"
        duration = 0.5
        
        # Record Qdrant operation
        self.metrics.record_qdrant_operation(operation, collection, status, duration)
        
        # Get operation counter value
        counter_value = self.metrics.qdrant_operations.labels(
            operation=operation, collection=collection, status=status
        )._value._value
        
        assert counter_value == 1.0
        
        # Check duration histogram
        histogram_samples = self.metrics.qdrant_operation_duration.labels(
            operation=operation, collection=collection
        ).collect()[0].samples
        
        # Find the _count sample
        count_sample = next((s for s in histogram_samples if s.name.endswith('_count')), None)
        assert count_sample is not None
        assert count_sample.value == 1.0
    
    def test_update_qdrant_collection_points(self):
        """Test updating Qdrant collection points."""
        self.setUp()
        
        collection = "test-collection"
        points = 1000
        
        # Update collection points
        self.metrics.update_qdrant_collection_points(collection, points)
        
        # Get metric value
        metric_value = self.metrics.qdrant_collection_points.labels(
            collection=collection
        )._value._value
        
        assert metric_value == 1000.0
    
    def test_record_qdrant_search_results(self):
        """Test recording Qdrant search results."""
        self.setUp()
        
        collection = "test-collection"
        results_count = 5
        
        # Record search results
        self.metrics.record_qdrant_search_results(collection, results_count)
        
        # Get metric value
        metric_value = self.metrics.qdrant_search_results.labels(
            collection=collection
        )._value._value
        
        assert metric_value == 5.0
    
    def test_update_qdrant_connection_status(self):
        """Test updating Qdrant connection status."""
        self.setUp()
        
        # Update connection status to connected
        self.metrics.update_qdrant_connection_status(True)
        
        # Get metric value
        metric_value = self.metrics.qdrant_connection_status._value._value
        
        assert metric_value == 1.0
        
        # Update connection status to disconnected
        self.metrics.update_qdrant_connection_status(False)
        
        # Get metric value
        metric_value = self.metrics.qdrant_connection_status._value._value
        
        assert metric_value == 0.0
    
    def test_record_embedding_generation(self):
        """Test recording embedding generation."""
        self.setUp()
        
        model = "test-model"
        status = "success"
        duration = 0.1
        
        # Record embedding generation
        self.metrics.record_embedding_generation(model, status, duration)
        
        # Get counter value
        counter_value = self.metrics.embedding_generation_total.labels(
            model=model, status=status
        )._value._value
        
        assert counter_value == 1.0
        
        # Check duration histogram
        histogram_samples = self.metrics.embedding_generation_duration.labels(
            model=model
        ).collect()[0].samples
        
        # Find the _count sample
        count_sample = next((s for s in histogram_samples if s.name.endswith('_count')), None)
        assert count_sample is not None
        assert count_sample.value == 1.0
    
    def test_update_system_metrics(self):
        """Test updating system metrics."""
        self.setUp()
        
        cpu_usage = 50.0
        memory_usage = 60.0
        disk_usage = 70.0
        
        # Update system metrics
        self.metrics.update_system_cpu_usage(cpu_usage)
        self.metrics.update_system_memory_usage(memory_usage)
        self.metrics.update_system_disk_usage(disk_usage)
        
        # Get metric values
        cpu_value = self.metrics.system_cpu_usage._value._value
        memory_value = self.metrics.system_memory_usage._value._value
        disk_value = self.metrics.system_disk_usage._value._value
        
        assert cpu_value == 50.0
        assert memory_value == 60.0
        assert disk_value == 70.0
    
    def test_record_health_check(self):
        """Test recording health check."""
        self.setUp()
        
        service = "kafka"
        status = "healthy"
        duration = 0.05
        
        # Record health check
        self.metrics.record_health_check(service, status, duration)
        
        # Get status gauge value
        status_value = self.metrics.health_check_status.labels(
            service=service, status=status
        )._value._value
        
        assert status_value == 1.0
        
        # Check duration histogram
        histogram_samples = self.metrics.health_check_duration.labels(
            service=service
        ).collect()[0].samples
        
        # Find the _count sample
        count_sample = next((s for s in histogram_samples if s.name.endswith('_count')), None)
        assert count_sample is not None
        assert count_sample.value == 1.0
    
    def test_update_application_uptime(self):
        """Test updating application uptime."""
        self.setUp()
        
        uptime = 3600.0  # 1 hour
        
        # Update application uptime
        self.metrics.update_application_uptime(uptime)
        
        # Get metric value
        metric_value = self.metrics.application_uptime._value._value
        
        assert metric_value == 3600.0
    
    def test_record_error(self):
        """Test recording error."""
        self.setUp()
        
        component = "kafka"
        error_type = "connection_error"
        
        # Record error
        self.metrics.record_error(component, error_type)
        
        # Get metric value
        metric_value = self.metrics.error_rate.labels(
            component=component, error_type=error_type
        )._value._value
        
        assert metric_value == 1.0


class TestMetricsServer:
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_registry = CollectorRegistry()
        self.server = MetricsServer(port=9091, registry=self.test_registry)
    
    def test_metrics_server_initialization(self):
        """Test MetricsServer initialization."""
        self.setUp()
        
        assert self.server.port == 9091
        assert self.server.registry == self.test_registry
        assert self.server.httpd is None
    
    @patch('src.monitoring.prometheus_metrics.start_http_server')
    def test_start_server(self, mock_start_http_server):
        """Test starting metrics server."""
        self.setUp()
        
        mock_httpd = Mock()
        mock_start_http_server.return_value = mock_httpd
        
        # Start server
        self.server.start()
        
        # Verify server was started
        mock_start_http_server.assert_called_once_with(9091, registry=self.test_registry)
        assert self.server.httpd == mock_httpd
    
    def test_stop_server_not_started(self):
        """Test stopping server when not started."""
        self.setUp()
        
        # Stop server that was never started
        self.server.stop()
        
        # Should not raise any exception
        assert self.server.httpd is None
    
    @patch('src.monitoring.prometheus_metrics.start_http_server')
    def test_stop_server_started(self, mock_start_http_server):
        """Test stopping server after starting."""
        self.setUp()
        
        mock_httpd = Mock()
        mock_start_http_server.return_value = mock_httpd
        
        # Start and then stop server
        self.server.start()
        self.server.stop()
        
        # Verify server was stopped
        mock_httpd.shutdown.assert_called_once()
        assert self.server.httpd is None
    
    @patch('src.monitoring.prometheus_metrics.start_http_server')
    def test_context_manager(self, mock_start_http_server):
        """Test using MetricsServer as context manager."""
        self.setUp()
        
        mock_httpd = Mock()
        mock_start_http_server.return_value = mock_httpd
        
        # Use as context manager
        with self.server:
            # Verify server was started
            mock_start_http_server.assert_called_once_with(9091, registry=self.test_registry)
            assert self.server.httpd == mock_httpd
        
        # Verify server was stopped
        mock_httpd.shutdown.assert_called_once()
        assert self.server.httpd is None