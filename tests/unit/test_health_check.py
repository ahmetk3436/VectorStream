import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
from src.utils.health_check import HealthCheckServer, run_health_check
from src.monitoring.health_monitor import HealthMonitor, HealthStatus, HealthCheck
from src.config.kafka_config import KafkaConfig
from src.config.qdrant_config import QdrantConfig


class TestHealthCheckServer:
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
        
        # Mock health monitor
        self.mock_health_monitor = Mock(spec=HealthMonitor)
        
        # Create health check server
        self.health_server = HealthCheckServer(
            kafka_config=self.kafka_config,
            qdrant_config=self.qdrant_config,
            health_monitor=self.mock_health_monitor
        )
        
        # Create test client
        self.client = TestClient(self.health_server.app)
    
    def test_health_check_server_initialization(self):
        """Test HealthCheckServer initialization."""
        self.setUp()
        
        assert self.health_server.kafka_config == self.kafka_config
        assert self.health_server.qdrant_config == self.qdrant_config
        assert self.health_server.health_monitor == self.mock_health_monitor
        assert self.health_server.app is not None
    
    def test_health_endpoint_healthy(self):
        """Test /health endpoint when all services are healthy."""
        self.setUp()
        
        # Mock health monitor to return healthy status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.HEALTHY
        )
        self.mock_health_monitor.run_all_checks = AsyncMock(
            return_value=[
                HealthCheck(
                    service="kafka",
                    status=HealthStatus.HEALTHY,
                    message="Kafka is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="qdrant",
                    status=HealthStatus.HEALTHY,
                    message="Qdrant is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="system",
                    status=HealthStatus.HEALTHY,
                    message="System is healthy",
                    timestamp=1234567890.0
                )
            ]
        )
        
        # Make request to health endpoint
        response = self.client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert len(data["checks"]) == 3
        assert all(check["status"] == "healthy" for check in data["checks"])
    
    def test_health_endpoint_unhealthy(self):
        """Test /health endpoint when one service is unhealthy."""
        self.setUp()
        
        # Mock health monitor to return unhealthy status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.UNHEALTHY
        )
        self.mock_health_monitor.run_all_checks = AsyncMock(
            return_value=[
                HealthCheck(
                    service="kafka",
                    status=HealthStatus.UNHEALTHY,
                    message="Kafka connection failed",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="qdrant",
                    status=HealthStatus.HEALTHY,
                    message="Qdrant is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="system",
                    status=HealthStatus.HEALTHY,
                    message="System is healthy",
                    timestamp=1234567890.0
                )
            ]
        )
        
        # Make request to health endpoint
        response = self.client.get("/health")
        
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "unhealthy"
        assert len(data["checks"]) == 3
        assert data["checks"][0]["status"] == "unhealthy"
    
    def test_health_endpoint_degraded(self):
        """Test /health endpoint when one service is degraded."""
        self.setUp()
        
        # Mock health monitor to return degraded status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.DEGRADED
        )
        self.mock_health_monitor.run_all_checks = AsyncMock(
            return_value=[
                HealthCheck(
                    service="kafka",
                    status=HealthStatus.HEALTHY,
                    message="Kafka is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="qdrant",
                    status=HealthStatus.DEGRADED,
                    message="Qdrant is degraded",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="system",
                    status=HealthStatus.HEALTHY,
                    message="System is healthy",
                    timestamp=1234567890.0
                )
            ]
        )
        
        # Make request to health endpoint
        response = self.client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert len(data["checks"]) == 3
        assert data["checks"][1]["status"] == "degraded"
    
    def test_liveness_endpoint_healthy(self):
        """Test /health/live endpoint when system is healthy."""
        self.setUp()
        
        # Mock health monitor to return healthy status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.HEALTHY
        )
        
        # Make request to liveness endpoint
        response = self.client.get("/health/live")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "alive"
        assert "timestamp" in data
    
    def test_liveness_endpoint_unhealthy(self):
        """Test /health/live endpoint when system is unhealthy."""
        self.setUp()
        
        # Mock health monitor to return unhealthy status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.UNHEALTHY
        )
        
        # Make request to liveness endpoint
        response = self.client.get("/health/live")
        
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "dead"
        assert "timestamp" in data
    
    def test_readiness_endpoint_ready(self):
        """Test /health/ready endpoint when system is ready."""
        self.setUp()
        
        # Mock health monitor to return healthy status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.HEALTHY
        )
        
        # Make request to readiness endpoint
        response = self.client.get("/health/ready")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"
        assert "timestamp" in data
    
    def test_readiness_endpoint_not_ready(self):
        """Test /health/ready endpoint when system is not ready."""
        self.setUp()
        
        # Mock health monitor to return unhealthy status
        self.mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.UNHEALTHY
        )
        
        # Make request to readiness endpoint
        response = self.client.get("/health/ready")
        
        assert response.status_code == 503
        data = response.json()
        assert data["status"] == "not ready"
        assert "timestamp" in data
    
    def test_kafka_health_endpoint_healthy(self):
        """Test /health/kafka endpoint when Kafka is healthy."""
        self.setUp()
        
        # Mock Kafka health check
        self.mock_health_monitor.check_kafka_health = AsyncMock(
            return_value=HealthCheck(
                service="kafka",
                status=HealthStatus.HEALTHY,
                message="Kafka is healthy",
                timestamp=1234567890.0
            )
        )
        
        # Make request to Kafka health endpoint
        response = self.client.get("/health/kafka")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "kafka"
        assert data["status"] == "healthy"
        assert data["message"] == "Kafka is healthy"
    
    def test_kafka_health_endpoint_unhealthy(self):
        """Test /health/kafka endpoint when Kafka is unhealthy."""
        self.setUp()
        
        # Mock Kafka health check
        self.mock_health_monitor.check_kafka_health = AsyncMock(
            return_value=HealthCheck(
                service="kafka",
                status=HealthStatus.UNHEALTHY,
                message="Kafka connection failed",
                timestamp=1234567890.0
            )
        )
        
        # Make request to Kafka health endpoint
        response = self.client.get("/health/kafka")
        
        assert response.status_code == 503
        data = response.json()
        assert data["service"] == "kafka"
        assert data["status"] == "unhealthy"
        assert data["message"] == "Kafka connection failed"
    
    def test_qdrant_health_endpoint_healthy(self):
        """Test /health/qdrant endpoint when Qdrant is healthy."""
        self.setUp()
        
        # Mock Qdrant health check
        self.mock_health_monitor.check_qdrant_health = AsyncMock(
            return_value=HealthCheck(
                service="qdrant",
                status=HealthStatus.HEALTHY,
                message="Qdrant is healthy",
                timestamp=1234567890.0
            )
        )
        
        # Make request to Qdrant health endpoint
        response = self.client.get("/health/qdrant")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "qdrant"
        assert data["status"] == "healthy"
        assert data["message"] == "Qdrant is healthy"
    
    def test_qdrant_health_endpoint_unhealthy(self):
        """Test /health/qdrant endpoint when Qdrant is unhealthy."""
        self.setUp()
        
        # Mock Qdrant health check
        self.mock_health_monitor.check_qdrant_health = AsyncMock(
            return_value=HealthCheck(
                service="qdrant",
                status=HealthStatus.UNHEALTHY,
                message="Qdrant connection failed",
                timestamp=1234567890.0
            )
        )
        
        # Make request to Qdrant health endpoint
        response = self.client.get("/health/qdrant")
        
        assert response.status_code == 503
        data = response.json()
        assert data["service"] == "qdrant"
        assert data["status"] == "unhealthy"
        assert data["message"] == "Qdrant connection failed"
    
    def test_system_health_endpoint_healthy(self):
        """Test /health/system endpoint when system is healthy."""
        self.setUp()
        
        # Mock system health check
        self.mock_health_monitor.check_system_health = AsyncMock(
            return_value=HealthCheck(
                service="system",
                status=HealthStatus.HEALTHY,
                message="System is healthy",
                timestamp=1234567890.0
            )
        )
        
        # Make request to system health endpoint
        response = self.client.get("/health/system")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "system"
        assert data["status"] == "healthy"
        assert data["message"] == "System is healthy"
    
    def test_system_health_endpoint_degraded(self):
        """Test /health/system endpoint when system is degraded."""
        self.setUp()
        
        # Mock system health check
        self.mock_health_monitor.check_system_health = AsyncMock(
            return_value=HealthCheck(
                service="system",
                status=HealthStatus.DEGRADED,
                message="System is degraded",
                timestamp=1234567890.0
            )
        )
        
        # Make request to system health endpoint
        response = self.client.get("/health/system")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "system"
        assert data["status"] == "degraded"
        assert data["message"] == "System is degraded"


class TestRunHealthCheck:
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
    
    def run_async_test(self, coro):
        """Helper method to run async tests."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    
    @patch('src.utils.health_check.HealthMonitor')
    def test_run_health_check_all_healthy(self, mock_health_monitor_class):
        """Test run_health_check function when all services are healthy."""
        self.setUp()
        
        # Mock health monitor
        mock_health_monitor = Mock()
        mock_health_monitor.run_all_checks = AsyncMock(
            return_value=[
                HealthCheck(
                    service="kafka",
                    status=HealthStatus.HEALTHY,
                    message="Kafka is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="qdrant",
                    status=HealthStatus.HEALTHY,
                    message="Qdrant is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="system",
                    status=HealthStatus.HEALTHY,
                    message="System is healthy",
                    timestamp=1234567890.0
                )
            ]
        )
        mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.HEALTHY
        )
        mock_health_monitor_class.return_value = mock_health_monitor
        
        # Run health check
        result = self.run_async_test(
            run_health_check(self.kafka_config, self.qdrant_config)
        )
        
        assert result["overall_status"] == "healthy"
        assert len(result["checks"]) == 3
        assert all(check["status"] == "healthy" for check in result["checks"])
    
    @patch('src.utils.health_check.HealthMonitor')
    def test_run_health_check_with_failures(self, mock_health_monitor_class):
        """Test run_health_check function when some services fail."""
        self.setUp()
        
        # Mock health monitor
        mock_health_monitor = Mock()
        mock_health_monitor.run_all_checks = AsyncMock(
            return_value=[
                HealthCheck(
                    service="kafka",
                    status=HealthStatus.UNHEALTHY,
                    message="Kafka connection failed",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="qdrant",
                    status=HealthStatus.HEALTHY,
                    message="Qdrant is healthy",
                    timestamp=1234567890.0
                ),
                HealthCheck(
                    service="system",
                    status=HealthStatus.DEGRADED,
                    message="System is degraded",
                    timestamp=1234567890.0
                )
            ]
        )
        mock_health_monitor.get_overall_status = AsyncMock(
            return_value=HealthStatus.UNHEALTHY
        )
        mock_health_monitor_class.return_value = mock_health_monitor
        
        # Run health check
        result = self.run_async_test(
            run_health_check(self.kafka_config, self.qdrant_config)
        )
        
        assert result["overall_status"] == "unhealthy"
        assert len(result["checks"]) == 3
        assert result["checks"][0]["status"] == "unhealthy"
        assert result["checks"][1]["status"] == "healthy"
        assert result["checks"][2]["status"] == "degraded"