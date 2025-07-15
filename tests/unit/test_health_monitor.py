import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from src.monitoring.health_monitor import HealthMonitor, HealthStatus, HealthCheck
from src.config.kafka_config import KafkaConfig
from src.config.qdrant_config import QdrantConfig


class TestHealthMonitor:
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.kafka_config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            group_id="test-group"
        )
        self.qdrant_config = QdrantConfig(
            host="localhost",
            port=6333,
            collection_name="test-collection"
        )
        self.health_monitor = HealthMonitor(self.kafka_config, self.qdrant_config)
    
    def run_async_test(self, coro):
        """Helper method to run async tests."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    def test_health_status_enum(self):
        """Test HealthStatus enum values."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.DEGRADED.value == "degraded"
    
    def test_health_check_dataclass(self):
        """Test HealthCheck dataclass."""
        health_check = HealthCheck(
            service="test-service",
            status=HealthStatus.HEALTHY,
            message="Service is healthy",
            timestamp=1234567890.0
        )
        
        assert health_check.service == "test-service"
        assert health_check.status == HealthStatus.HEALTHY
        assert health_check.message == "Service is healthy"
        assert health_check.timestamp == 1234567890.0
    
    @patch('kafka.KafkaConsumer')
    def test_check_kafka_health_success(self, mock_kafka_consumer):
        """Test successful Kafka health check."""
        self.setUp()
        
        # Mock successful Kafka connection
        mock_consumer = Mock()
        mock_consumer.list_consumer_group_offsets.return_value = {}
        mock_kafka_consumer.return_value = mock_consumer
        
        result = self.run_async_test(self.health_monitor.check_kafka_health())
        
        assert result.service == "kafka"
        assert result.status == HealthStatus.HEALTHY
        assert "Kafka is healthy" in result.message
    
    @patch('kafka.KafkaConsumer')
    def test_check_kafka_health_failure(self, mock_kafka_consumer):
        """Test failed Kafka health check."""
        self.setUp()
        
        # Mock Kafka connection failure
        mock_kafka_consumer.side_effect = Exception("Connection failed")
        
        result = self.run_async_test(self.health_monitor.check_kafka_health())
        
        assert result.service == "kafka"
        assert result.status == HealthStatus.UNHEALTHY
        assert "Connection failed" in result.message
    
    @patch('qdrant_client.QdrantClient')
    def test_check_qdrant_health_success(self, mock_qdrant_client):
        """Test successful Qdrant health check."""
        self.setUp()
        
        # Mock successful Qdrant connection
        mock_client = Mock()
        mock_client.get_collections.return_value = Mock(collections=[])
        mock_qdrant_client.return_value = mock_client
        
        result = self.run_async_test(self.health_monitor.check_qdrant_health())
        
        assert result.service == "qdrant"
        assert result.status == HealthStatus.HEALTHY
        assert "Qdrant is healthy" in result.message
    
    @patch('qdrant_client.QdrantClient')
    def test_check_qdrant_health_failure(self, mock_qdrant_client):
        """Test failed Qdrant health check."""
        self.setUp()
        
        # Mock Qdrant connection failure
        mock_client = Mock()
        mock_client.get_collections.side_effect = Exception("Connection failed")
        mock_qdrant_client.return_value = mock_client
        
        result = self.run_async_test(self.health_monitor.check_qdrant_health())
        
        assert result.service == "qdrant"
        assert result.status == HealthStatus.UNHEALTHY
        assert "Connection failed" in result.message
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_check_system_health_healthy(self, mock_disk_usage, mock_virtual_memory, mock_cpu_percent):
        """Test system health check with healthy metrics."""
        self.setUp()
        
        # Mock healthy system metrics
        mock_cpu_percent.return_value = 50.0
        mock_virtual_memory.return_value = Mock(percent=60.0, available=8*1024**3)
        mock_disk_usage.return_value = Mock(percent=70.0)
        
        result = self.run_async_test(self.health_monitor.check_system_health())
        
        assert result.service == "system"
        assert result.status == HealthStatus.HEALTHY
        assert "System is healthy" in result.message
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_check_system_health_degraded(self, mock_disk_usage, mock_virtual_memory, mock_cpu_percent):
        """Test system health check with degraded metrics."""
        self.setUp()
        
        # Mock degraded system metrics (above 85% but below 95%)
        mock_cpu_percent.return_value = 90.0
        mock_virtual_memory.return_value = Mock(percent=90.0, available=2*1024**3)
        mock_disk_usage.return_value = Mock(percent=90.0)
        
        result = self.run_async_test(self.health_monitor.check_system_health())
        
        assert result.service == "system"
        assert result.status == HealthStatus.DEGRADED
        assert "System is degraded" in result.message
    
    @patch('psutil.cpu_percent')
    @patch('psutil.virtual_memory')
    @patch('psutil.disk_usage')
    def test_check_system_health_unhealthy(self, mock_disk_usage, mock_virtual_memory, mock_cpu_percent):
        """Test system health check with unhealthy metrics."""
        self.setUp()
        
        # Mock unhealthy system metrics (above 95%)
        mock_cpu_percent.return_value = 97.0
        mock_virtual_memory.return_value = Mock(percent=97.0, available=1*1024**3)
        mock_disk_usage.return_value = Mock(percent=97.0)
        
        result = self.run_async_test(self.health_monitor.check_system_health())
        
        assert result.service == "system"
        assert result.status == HealthStatus.UNHEALTHY
        assert "System is unhealthy" in result.message
    
    @patch.object(HealthMonitor, 'check_kafka_health')
    @patch.object(HealthMonitor, 'check_qdrant_health')
    @patch.object(HealthMonitor, 'check_system_health')
    def test_run_all_checks_all_healthy(self, mock_system_health, mock_qdrant_health, mock_kafka_health):
        """Test running all health checks with all services healthy."""
        self.setUp()
        
        # Mock all services as healthy
        mock_kafka_health.return_value = HealthCheck(
            service="kafka",
            status=HealthStatus.HEALTHY,
            message="Kafka is healthy",
            timestamp=1234567890.0
        )
        mock_qdrant_health.return_value = HealthCheck(
            service="qdrant",
            status=HealthStatus.HEALTHY,
            message="Qdrant is healthy",
            timestamp=1234567890.0
        )
        mock_system_health.return_value = HealthCheck(
            service="system",
            status=HealthStatus.HEALTHY,
            message="System is healthy",
            timestamp=1234567890.0
        )
        
        results = self.run_async_test(self.health_monitor.run_all_checks())
        
        assert len(results) == 3
        assert all(check.status == HealthStatus.HEALTHY for check in results)
    
    @patch.object(HealthMonitor, 'check_kafka_health')
    @patch.object(HealthMonitor, 'check_qdrant_health')
    @patch.object(HealthMonitor, 'check_system_health')
    def test_get_overall_status_healthy(self, mock_system_health, mock_qdrant_health, mock_kafka_health):
        """Test getting overall status when all services are healthy."""
        self.setUp()
        
        # Mock all services as healthy
        mock_kafka_health.return_value = HealthCheck(
            service="kafka",
            status=HealthStatus.HEALTHY,
            message="Kafka is healthy",
            timestamp=1234567890.0
        )
        mock_qdrant_health.return_value = HealthCheck(
            service="qdrant",
            status=HealthStatus.HEALTHY,
            message="Qdrant is healthy",
            timestamp=1234567890.0
        )
        mock_system_health.return_value = HealthCheck(
            service="system",
            status=HealthStatus.HEALTHY,
            message="System is healthy",
            timestamp=1234567890.0
        )
        
        status = self.run_async_test(self.health_monitor.get_overall_status())
        
        assert status == HealthStatus.HEALTHY
    
    @patch.object(HealthMonitor, 'check_kafka_health')
    @patch.object(HealthMonitor, 'check_qdrant_health')
    @patch.object(HealthMonitor, 'check_system_health')
    def test_get_overall_status_unhealthy(self, mock_system_health, mock_qdrant_health, mock_kafka_health):
        """Test getting overall status when one service is unhealthy."""
        self.setUp()
        
        # Mock one service as unhealthy
        mock_kafka_health.return_value = HealthCheck(
            service="kafka",
            status=HealthStatus.UNHEALTHY,
            message="Kafka is unhealthy",
            timestamp=1234567890.0
        )
        mock_qdrant_health.return_value = HealthCheck(
            service="qdrant",
            status=HealthStatus.HEALTHY,
            message="Qdrant is healthy",
            timestamp=1234567890.0
        )
        mock_system_health.return_value = HealthCheck(
            service="system",
            status=HealthStatus.HEALTHY,
            message="System is healthy",
            timestamp=1234567890.0
        )
        
        status = self.run_async_test(self.health_monitor.get_overall_status())
        
        assert status == HealthStatus.UNHEALTHY
    
    @patch.object(HealthMonitor, 'check_kafka_health')
    @patch.object(HealthMonitor, 'check_qdrant_health')
    @patch.object(HealthMonitor, 'check_system_health')
    def test_get_overall_status_degraded(self, mock_system_health, mock_qdrant_health, mock_kafka_health):
        """Test getting overall status when one service is degraded."""
        self.setUp()
        
        # Mock one service as degraded
        mock_kafka_health.return_value = HealthCheck(
            service="kafka",
            status=HealthStatus.HEALTHY,
            message="Kafka is healthy",
            timestamp=1234567890.0
        )
        mock_qdrant_health.return_value = HealthCheck(
            service="qdrant",
            status=HealthStatus.DEGRADED,
            message="Qdrant is degraded",
            timestamp=1234567890.0
        )
        mock_system_health.return_value = HealthCheck(
            service="system",
            status=HealthStatus.HEALTHY,
            message="System is healthy",
            timestamp=1234567890.0
        )
        
        status = self.run_async_test(self.health_monitor.get_overall_status())
        
        assert status == HealthStatus.DEGRADED