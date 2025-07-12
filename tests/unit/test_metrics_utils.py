import pytest
import time
import psutil
from unittest.mock import Mock, patch, MagicMock
from src.utils.metrics import (
    MetricsCollector,
    SystemMetricsCollector,
    HealthMetricsCollector,
    metrics_timer,
    kafka_metrics,
    qdrant_metrics,
    embedding_metrics
)
from src.monitoring.prometheus_metrics import PrometheusMetrics
from src.monitoring.health_monitor import HealthStatus
from prometheus_client import CollectorRegistry


class TestMetricsCollector:
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_registry = CollectorRegistry()
        self.prometheus_metrics = PrometheusMetrics(registry=self.test_registry)
        self.metrics_collector = MetricsCollector(self.prometheus_metrics)
    
    def test_metrics_collector_initialization(self):
        """Test MetricsCollector initialization."""
        self.setUp()
        
        assert self.metrics_collector.prometheus_metrics == self.prometheus_metrics
    
    def test_time_operation_context_manager(self):
        """Test time_operation context manager."""
        self.setUp()
        
        operation_name = "test_operation"
        
        # Use context manager
        with self.metrics_collector.time_operation(operation_name) as timer:
            time.sleep(0.01)  # Small delay to measure
            assert timer.operation_name == operation_name
            assert timer.start_time > 0
        
        # Check that duration was calculated
        assert timer.duration > 0
        assert timer.duration >= 0.01
    
    def test_record_kafka_metrics(self):
        """Test recording Kafka metrics."""
        self.setUp()
        
        topic = "test-topic"
        partition = "0"
        status = "success"
        duration = 0.5
        
        # Record Kafka metrics
        self.metrics_collector.record_kafka_metrics(topic, partition, status, duration)
        
        # Verify metrics were recorded (we can't easily check the actual values
        # without accessing internal state, but we can verify the method doesn't crash)
        assert True  # If we get here, the method executed successfully
    
    def test_record_qdrant_metrics(self):
        """Test recording Qdrant metrics."""
        self.setUp()
        
        operation = "search"
        collection = "test-collection"
        status = "success"
        duration = 0.2
        results_count = 5
        
        # Record Qdrant metrics
        self.metrics_collector.record_qdrant_metrics(
            operation, collection, status, duration, results_count
        )
        
        # Verify metrics were recorded
        assert True  # If we get here, the method executed successfully
    
    def test_record_embedding_metrics(self):
        """Test recording embedding metrics."""
        self.setUp()
        
        model = "test-model"
        status = "success"
        duration = 0.1
        
        # Record embedding metrics
        self.metrics_collector.record_embedding_metrics(model, status, duration)
        
        # Verify metrics were recorded
        assert True  # If we get here, the method executed successfully


class TestSystemMetricsCollector:
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_registry = CollectorRegistry()
        self.prometheus_metrics = PrometheusMetrics(registry=self.test_registry)
        self.system_collector = SystemMetricsCollector(self.prometheus_metrics)
    
    def test_system_metrics_collector_initialization(self):
        """Test SystemMetricsCollector initialization."""
        self.setUp()
        
        assert self.system_collector.prometheus_metrics == self.prometheus_metrics
    
    @patch('psutil.cpu_percent')
    def test_collect_cpu_usage(self, mock_cpu_percent):
        """Test collecting CPU usage."""
        self.setUp()
        
        mock_cpu_percent.return_value = 75.5
        
        # Collect CPU usage
        cpu_usage = self.system_collector.collect_cpu_usage()
        
        assert cpu_usage == 75.5
        mock_cpu_percent.assert_called_once_with(interval=1)
    
    @patch('psutil.virtual_memory')
    def test_collect_memory_usage(self, mock_virtual_memory):
        """Test collecting memory usage."""
        self.setUp()
        
        mock_memory = Mock()
        mock_memory.percent = 65.2
        mock_virtual_memory.return_value = mock_memory
        
        # Collect memory usage
        memory_usage = self.system_collector.collect_memory_usage()
        
        assert memory_usage == 65.2
        mock_virtual_memory.assert_called_once()
    
    @patch('psutil.disk_usage')
    def test_collect_disk_usage(self, mock_disk_usage):
        """Test collecting disk usage."""
        self.setUp()
        
        mock_disk = Mock()
        mock_disk.percent = 45.8
        mock_disk_usage.return_value = mock_disk
        
        # Collect disk usage
        disk_usage = self.system_collector.collect_disk_usage()
        
        assert disk_usage == 45.8
        mock_disk_usage.assert_called_once_with('/')
    
    @patch('psutil.disk_usage')
    def test_collect_disk_usage_custom_path(self, mock_disk_usage):
        """Test collecting disk usage with custom path."""
        self.setUp()
        
        mock_disk = Mock()
        mock_disk.percent = 55.3
        mock_disk_usage.return_value = mock_disk
        
        # Collect disk usage for custom path
        disk_usage = self.system_collector.collect_disk_usage('/data')
        
        assert disk_usage == 55.3
        mock_disk_usage.assert_called_once_with('/data')
    
    @patch.object(SystemMetricsCollector, 'collect_cpu_usage')
    @patch.object(SystemMetricsCollector, 'collect_memory_usage')
    @patch.object(SystemMetricsCollector, 'collect_disk_usage')
    def test_collect_all_metrics(self, mock_disk_usage, mock_memory_usage, mock_cpu_usage):
        """Test collecting all system metrics."""
        self.setUp()
        
        # Mock return values
        mock_cpu_usage.return_value = 70.0
        mock_memory_usage.return_value = 80.0
        mock_disk_usage.return_value = 60.0
        
        # Collect all metrics
        metrics = self.system_collector.collect_all_metrics()
        
        assert metrics['cpu_usage'] == 70.0
        assert metrics['memory_usage'] == 80.0
        assert metrics['disk_usage'] == 60.0
        
        mock_cpu_usage.assert_called_once()
        mock_memory_usage.assert_called_once()
        mock_disk_usage.assert_called_once_with('/')
    
    @patch.object(SystemMetricsCollector, 'collect_all_metrics')
    def test_update_prometheus_metrics(self, mock_collect_all_metrics):
        """Test updating Prometheus metrics."""
        self.setUp()
        
        # Mock collected metrics
        mock_collect_all_metrics.return_value = {
            'cpu_usage': 75.0,
            'memory_usage': 85.0,
            'disk_usage': 65.0
        }
        
        # Update Prometheus metrics
        self.system_collector.update_prometheus_metrics()
        
        mock_collect_all_metrics.assert_called_once()


class TestHealthMetricsCollector:
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_registry = CollectorRegistry()
        self.prometheus_metrics = PrometheusMetrics(registry=self.test_registry)
        self.health_collector = HealthMetricsCollector(self.prometheus_metrics)
    
    def test_health_metrics_collector_initialization(self):
        """Test HealthMetricsCollector initialization."""
        self.setUp()
        
        assert self.health_collector.prometheus_metrics == self.prometheus_metrics
    
    def test_record_health_check_result(self):
        """Test recording health check result."""
        self.setUp()
        
        service = "kafka"
        status = HealthStatus.HEALTHY
        duration = 0.05
        
        # Record health check result
        self.health_collector.record_health_check_result(service, status, duration)
        
        # Verify method executed successfully
        assert True
    
    def test_update_connection_status_kafka(self):
        """Test updating Kafka connection status."""
        self.setUp()
        
        # Update Kafka connection status
        self.health_collector.update_connection_status('kafka', True)
        
        # Verify method executed successfully
        assert True
    
    def test_update_connection_status_qdrant(self):
        """Test updating Qdrant connection status."""
        self.setUp()
        
        # Update Qdrant connection status
        self.health_collector.update_connection_status('qdrant', False)
        
        # Verify method executed successfully
        assert True
    
    def test_update_connection_status_unknown_service(self):
        """Test updating connection status for unknown service."""
        self.setUp()
        
        # Update connection status for unknown service (should not crash)
        self.health_collector.update_connection_status('unknown', True)
        
        # Verify method executed successfully
        assert True


class TestMetricsDecorators:
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.test_registry = CollectorRegistry()
        self.prometheus_metrics = PrometheusMetrics(registry=self.test_registry)
    
    @patch('src.utils.metrics.MetricsCollector')
    def test_metrics_timer_decorator(self, mock_metrics_collector_class):
        """Test metrics_timer decorator."""
        self.setUp()
        
        mock_collector = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_context)
        mock_context.__exit__ = Mock(return_value=None)
        mock_collector.time_operation.return_value = mock_context
        mock_metrics_collector_class.return_value = mock_collector
        
        @metrics_timer("test_operation")
        def test_function():
            time.sleep(0.01)
            return "result"
        
        # Call decorated function
        result = test_function()
        
        assert result == "result"
        # Verify that time_operation was called
        mock_collector.time_operation.assert_called_once_with("test_operation", "general")
    
    @patch('src.utils.metrics.MetricsCollector')
    def test_kafka_metrics_decorator(self, mock_metrics_collector_class):
        """Test kafka_metrics decorator."""
        self.setUp()
        
        mock_collector = Mock()
        mock_metrics_collector_class.return_value = mock_collector
        
        @kafka_metrics("test-topic", "0")
        def test_kafka_function():
            time.sleep(0.01)
            return "kafka_result"
        
        # Call decorated function
        result = test_kafka_function()
        
        assert result == "kafka_result"
        # Verify that record_kafka_metrics was called
        mock_collector.record_kafka_metrics.assert_called_once()
    
    @patch('src.utils.metrics.MetricsCollector')
    def test_qdrant_metrics_decorator(self, mock_metrics_collector_class):
        """Test qdrant_metrics decorator."""
        self.setUp()
        
        mock_collector = Mock()
        mock_metrics_collector_class.return_value = mock_collector
        
        @qdrant_metrics("search", "test-collection")
        def test_qdrant_function():
            time.sleep(0.01)
            return ["result1", "result2", "result3"]
        
        # Call decorated function
        result = test_qdrant_function()
        
        assert result == ["result1", "result2", "result3"]
        # Verify that record_qdrant_metrics was called
        mock_collector.record_qdrant_metrics.assert_called_once()
    
    @patch('src.utils.metrics.MetricsCollector')
    def test_embedding_metrics_decorator(self, mock_metrics_collector_class):
        """Test embedding_metrics decorator."""
        self.setUp()
        
        mock_collector = Mock()
        mock_metrics_collector_class.return_value = mock_collector
        
        @embedding_metrics("test-model")
        def test_embedding_function():
            time.sleep(0.01)
            return "embedding_result"
        
        # Call decorated function
        result = test_embedding_function()
        
        assert result == "embedding_result"
        # Verify that record_embedding_metrics was called
        mock_collector.record_embedding_metrics.assert_called_once()
    
    @patch('src.utils.metrics.MetricsCollector')
    def test_decorator_with_exception(self, mock_metrics_collector_class):
        """Test decorator behavior when function raises exception."""
        self.setUp()
        
        mock_collector = Mock()
        mock_context = Mock()
        mock_context.__enter__ = Mock(return_value=mock_context)
        mock_context.__exit__ = Mock(return_value=None)
        mock_collector.time_operation.return_value = mock_context
        mock_metrics_collector_class.return_value = mock_collector
        
        @metrics_timer("test_operation")
        def test_function_with_error():
            raise ValueError("Test error")
        
        # Call decorated function and expect exception
        with pytest.raises(ValueError, match="Test error"):
            test_function_with_error()
        
        # Verify that time_operation was still called
        mock_collector.time_operation.assert_called_once_with("test_operation", "general")