import pytest
import asyncio
import time
import threading
import psutil
import random
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from enum import Enum

from src.core.kafka_consumer import KafkaConsumer
from src.core.qdrant_writer import QdrantWriter
from src.core.embedding_processor import EmbeddingProcessor
from src.utils.circuit_breaker import CircuitBreakerManager, reset_circuit_breaker_manager, circuit_breaker_manager, CircuitBreakerConfig, CircuitState
from src.utils.dead_letter_queue import reset_dlq
from src.utils.logger import setup_logger


class FailureType(Enum):
    """Types of failures to simulate"""
    SERVICE_UNAVAILABLE = "service_unavailable"
    NETWORK_TIMEOUT = "network_timeout"
    MEMORY_EXHAUSTION = "memory_exhaustion"
    CPU_EXHAUSTION = "cpu_exhaustion"
    DISK_FULL = "disk_full"
    RANDOM_EXCEPTION = "random_exception"
    SLOW_RESPONSE = "slow_response"
    PARTIAL_FAILURE = "partial_failure"


@dataclass
class ChaosConfig:
    """Configuration for chaos engineering tests"""
    failure_rate: float = 0.3  # 30% failure rate
    failure_duration_seconds: int = 5
    recovery_timeout_seconds: int = 10
    max_test_duration_seconds: int = 60
    enable_monitoring: bool = True
    failure_types: List[FailureType] = None
    
    def __post_init__(self):
        if self.failure_types is None:
            self.failure_types = [FailureType.SERVICE_UNAVAILABLE, FailureType.NETWORK_TIMEOUT]


class ChaosInjector:
    """Chaos injection utility for simulating various failures"""
    
    def __init__(self, config: ChaosConfig):
        self.config = config
        self.active_failures = set()
        self.failure_start_times = {}
        self.logger = setup_logger({'level': 'INFO'})
        
    def should_inject_failure(self) -> bool:
        """Determine if failure should be injected based on failure rate"""
        return random.random() < self.config.failure_rate
        
    def inject_service_failure(self, service_name: str):
        """Inject service unavailability"""
        if service_name not in self.active_failures:
            self.active_failures.add(service_name)
            self.failure_start_times[service_name] = time.time()
            self.logger.info(f"Injecting service failure for {service_name}")
            
    def inject_network_timeout(self, delay_seconds: float = 5.0):
        """Inject network timeout by adding delay"""
        self.logger.info(f"Injecting network timeout: {delay_seconds}s delay")
        time.sleep(delay_seconds)
        
    def inject_memory_pressure(self, size_mb: int = 100):
        """Simulate memory pressure"""
        self.logger.info(f"Injecting memory pressure: {size_mb}MB")
        # Simulate memory allocation
        data = bytearray(size_mb * 1024 * 1024)
        return data
        
    def inject_cpu_load(self, duration_seconds: int = 2):
        """Simulate CPU load"""
        self.logger.info(f"Injecting CPU load for {duration_seconds}s")
        end_time = time.time() + duration_seconds
        while time.time() < end_time:
            # CPU intensive operation
            sum(i * i for i in range(1000))
            
    def inject_random_exception(self):
        """Inject random exception"""
        exceptions = [
            ConnectionError("Connection lost"),
            TimeoutError("Operation timed out"),
            ValueError("Invalid data"),
            RuntimeError("Runtime error occurred")
        ]
        raise random.choice(exceptions)
        
    def is_service_failed(self, service_name: str) -> bool:
        """Check if service is currently failed"""
        if service_name not in self.active_failures:
            return False
            
        # Check if failure duration has passed
        if time.time() - self.failure_start_times[service_name] > self.config.failure_duration_seconds:
            self.recover_service(service_name)
            return False
            
        return True
        
    def recover_service(self, service_name: str):
        """Recover service from failure"""
        if service_name in self.active_failures:
            self.active_failures.remove(service_name)
            self.failure_start_times.pop(service_name, None)
            self.logger.info(f"Service {service_name} recovered")


class ResilienceMetrics:
    """Metrics collector for resilience testing"""
    
    def __init__(self):
        self.failure_count = 0
        self.recovery_count = 0
        self.total_requests = 0
        self.successful_requests = 0
        self.recovery_times = []
        self.error_types = {}
        
    def record_failure(self, error_type: str):
        """Record a failure occurrence"""
        self.failure_count += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        
    def record_recovery(self, recovery_time_seconds: float):
        """Record a recovery event"""
        self.recovery_count += 1
        self.recovery_times.append(recovery_time_seconds)
        
    def record_request(self, success: bool):
        """Record a request attempt"""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
            
    def get_success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
        
    def get_average_recovery_time(self) -> float:
        """Calculate average recovery time"""
        if not self.recovery_times:
            return 0.0
        return sum(self.recovery_times) / len(self.recovery_times)
        
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        return {
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failure_count': self.failure_count,
            'recovery_count': self.recovery_count,
            'success_rate': self.get_success_rate(),
            'average_recovery_time': self.get_average_recovery_time(),
            'error_types': self.error_types
        }


class ChaosTestRunner:
    """Main chaos engineering test runner"""
    
    def __init__(self, config: ChaosConfig):
        self.config = config
        self.chaos_injector = ChaosInjector(config)
        self.metrics = ResilienceMetrics()
        self.logger = setup_logger({'level': 'INFO'})
        
    async def run_service_failure_test(self, service_mock, service_name: str):
        """Test service failure and recovery"""
        self.logger.info(f"Starting service failure test for {service_name}")
        
        # Inject failure
        self.chaos_injector.inject_service_failure(service_name)
        
        # Configure mock to fail
        service_mock.side_effect = ConnectionError(f"{service_name} unavailable")
        
        start_time = time.time()
        
        # Test requests during failure
        for _ in range(10):
            try:
                await self._make_test_request(service_mock)
                self.metrics.record_request(True)
            except Exception as e:
                self.metrics.record_failure(type(e).__name__)
                self.metrics.record_request(False)
                
        # Wait for recovery
        await asyncio.sleep(self.config.failure_duration_seconds + 1)
        
        # Configure mock to succeed
        service_mock.side_effect = None
        service_mock.return_value = "success"
        
        # Test recovery
        recovery_start = time.time()
        for _ in range(5):
            try:
                await self._make_test_request(service_mock)
                self.metrics.record_request(True)
                recovery_time = time.time() - recovery_start
                self.metrics.record_recovery(recovery_time)
                break
            except Exception as e:
                self.metrics.record_failure(type(e).__name__)
                self.metrics.record_request(False)
                await asyncio.sleep(0.5)
                
    async def run_network_partition_test(self, service_mock):
        """Test network partition scenarios"""
        self.logger.info("Starting network partition test")
        
        # Simulate network partition with timeouts
        async def timeout_side_effect(*args, **kwargs):
            await asyncio.sleep(10)  # Long delay to simulate timeout
            raise TimeoutError("Network timeout")
            
        service_mock.side_effect = timeout_side_effect
        
        # Test requests during partition
        for _ in range(5):
            try:
                await asyncio.wait_for(self._make_test_request(service_mock), timeout=2)
                self.metrics.record_request(True)
            except (TimeoutError, asyncio.TimeoutError) as e:
                self.metrics.record_failure(type(e).__name__)
                self.metrics.record_request(False)
                
    async def run_resource_exhaustion_test(self):
        """Test resource exhaustion scenarios"""
        self.logger.info("Starting resource exhaustion test")
        
        # Test memory pressure
        try:
            memory_data = self.chaos_injector.inject_memory_pressure(50)
            self.metrics.record_request(True)
        except MemoryError as e:
            self.metrics.record_failure(type(e).__name__)
            self.metrics.record_request(False)
        finally:
            # Cleanup
            if 'memory_data' in locals():
                del memory_data
                
        # Test CPU load
        try:
            self.chaos_injector.inject_cpu_load(1)
            self.metrics.record_request(True)
        except Exception as e:
            self.metrics.record_failure(type(e).__name__)
            self.metrics.record_request(False)
            
    async def _make_test_request(self, service_mock):
        """Make a test request to the service"""
        if asyncio.iscoroutinefunction(service_mock):
            return await service_mock()
        else:
            return service_mock()


class TestChaosEngineering:
    """Chaos engineering test suite"""
    
    def setup_method(self):
        """Setup for each test"""
        reset_circuit_breaker_manager()
        reset_dlq()
        self.chaos_config = ChaosConfig(
            failure_rate=0.5,
            failure_duration_seconds=2,
            recovery_timeout_seconds=5,
            max_test_duration_seconds=30
        )
        self.test_runner = ChaosTestRunner(self.chaos_config)
        
    def test_chaos_config_creation(self):
        """Test chaos configuration creation"""
        config = ChaosConfig()
        assert config.failure_rate == 0.3
        assert config.failure_duration_seconds == 5
        assert config.recovery_timeout_seconds == 10
        assert len(config.failure_types) == 2
        
    def test_chaos_injector_initialization(self):
        """Test chaos injector initialization"""
        injector = ChaosInjector(self.chaos_config)
        assert injector.config == self.chaos_config
        assert len(injector.active_failures) == 0
        
    def test_service_failure_injection(self):
        """Test service failure injection"""
        injector = ChaosInjector(self.chaos_config)
        
        # Test failure injection
        injector.inject_service_failure("kafka")
        assert "kafka" in injector.active_failures
        assert injector.is_service_failed("kafka")
        
        # Test recovery
        injector.recover_service("kafka")
        assert "kafka" not in injector.active_failures
        assert not injector.is_service_failed("kafka")
        
    def test_network_timeout_injection(self):
        """Test network timeout injection"""
        injector = ChaosInjector(self.chaos_config)
        
        start_time = time.time()
        injector.inject_network_timeout(0.1)  # Short delay for test
        elapsed = time.time() - start_time
        
        assert elapsed >= 0.1
        
    def test_random_exception_injection(self):
        """Test random exception injection"""
        injector = ChaosInjector(self.chaos_config)
        
        with pytest.raises((ConnectionError, TimeoutError, ValueError, RuntimeError)):
            injector.inject_random_exception()
            
    def test_resilience_metrics(self):
        """Test resilience metrics collection"""
        metrics = ResilienceMetrics()
        
        # Record some events
        metrics.record_failure("ConnectionError")
        metrics.record_failure("TimeoutError")
        metrics.record_recovery(2.5)
        metrics.record_request(True)
        metrics.record_request(False)
        
        # Verify metrics
        assert metrics.failure_count == 2
        assert metrics.recovery_count == 1
        assert metrics.total_requests == 2
        assert metrics.successful_requests == 1
        assert metrics.get_success_rate() == 0.5
        assert metrics.get_average_recovery_time() == 2.5
        
        summary = metrics.get_metrics_summary()
        assert summary['total_requests'] == 2
        assert summary['success_rate'] == 0.5
        assert 'ConnectionError' in summary['error_types']
        
    @pytest.mark.asyncio
    async def test_service_failure_simulation(self):
        """Test service failure simulation"""
        mock_service = AsyncMock()
        
        await self.test_runner.run_service_failure_test(mock_service, "test_service")
        
        # Verify metrics were collected
        assert self.test_runner.metrics.total_requests > 0
        assert self.test_runner.metrics.failure_count > 0
        
    @pytest.mark.asyncio
    async def test_network_partition_simulation(self):
        """Test network partition simulation"""
        mock_service = AsyncMock()
        
        await self.test_runner.run_network_partition_test(mock_service)
        
        # Verify timeout failures were recorded
        assert self.test_runner.metrics.total_requests > 0
        assert 'TimeoutError' in self.test_runner.metrics.error_types
        
    @pytest.mark.asyncio
    async def test_resource_exhaustion_simulation(self):
        """Test resource exhaustion simulation"""
        await self.test_runner.run_resource_exhaustion_test()
        
        # Verify requests were made
        assert self.test_runner.metrics.total_requests > 0
        
    @pytest.mark.asyncio
    @patch('src.core.kafka_consumer.KafkaConsumer')
    async def test_kafka_resilience(self, mock_kafka_consumer):
        """Test Kafka service resilience"""
        mock_consumer = AsyncMock()
        mock_kafka_consumer.return_value = mock_consumer
        
        # Test with circuit breaker
        circuit_breaker = circuit_breaker_manager.get_circuit_breaker('kafka')
        if circuit_breaker is None:
            circuit_breaker = circuit_breaker_manager.create_circuit_breaker('kafka', CircuitBreakerConfig())
        
        # Simulate failures
        mock_consumer.consume_messages.side_effect = ConnectionError("Kafka unavailable")
        
        for _ in range(5):
            try:
                await circuit_breaker.call(mock_consumer.consume_messages)
                self.test_runner.metrics.record_request(True)
            except Exception as e:
                self.test_runner.metrics.record_failure(type(e).__name__)
                self.test_runner.metrics.record_request(False)
                
        # Verify circuit breaker opened
        assert circuit_breaker.state == CircuitState.OPEN
        assert self.test_runner.metrics.failure_count > 0
        
    @pytest.mark.asyncio
    @patch('src.core.qdrant_writer.QdrantWriter')
    async def test_qdrant_resilience(self, mock_qdrant_writer):
        """Test Qdrant service resilience"""
        mock_writer = AsyncMock()
        mock_qdrant_writer.return_value = mock_writer
        
        # Test with circuit breaker
        circuit_breaker = circuit_breaker_manager.get_circuit_breaker('qdrant')
        if circuit_breaker is None:
            circuit_breaker = circuit_breaker_manager.create_circuit_breaker('qdrant', CircuitBreakerConfig())
        
        # Simulate failures
        mock_writer.write_embeddings.side_effect = TimeoutError("Qdrant timeout")
        
        for _ in range(5):
            try:
                await circuit_breaker.call(mock_writer.write_embeddings, [])
                self.test_runner.metrics.record_request(True)
            except Exception as e:
                self.test_runner.metrics.record_failure(type(e).__name__)
                self.test_runner.metrics.record_request(False)
                
        # Verify circuit breaker behavior
        assert self.test_runner.metrics.failure_count > 0
        
    def test_failure_rate_calculation(self):
        """Test failure rate calculation"""
        injector = ChaosInjector(ChaosConfig(failure_rate=0.0))
        assert not injector.should_inject_failure()
        
        injector = ChaosInjector(ChaosConfig(failure_rate=1.0))
        assert injector.should_inject_failure()
        
    @pytest.mark.asyncio
    async def test_recovery_time_measurement(self):
        """Test recovery time measurement"""
        mock_service = AsyncMock()
        
        # Configure initial failure then success
        mock_service.side_effect = [ConnectionError("Failed")] * 3 + ["success"]
        
        start_time = time.time()
        
        for _ in range(4):
            try:
                await self.test_runner._make_test_request(mock_service)
                recovery_time = time.time() - start_time
                self.test_runner.metrics.record_recovery(recovery_time)
                break
            except Exception as e:
                self.test_runner.metrics.record_failure(type(e).__name__)
                await asyncio.sleep(0.1)
                
        assert self.test_runner.metrics.recovery_count > 0
        assert self.test_runner.metrics.get_average_recovery_time() > 0
        
    def test_chaos_config_validation(self):
        """Test chaos configuration validation"""
        # Test default values
        config = ChaosConfig()
        assert config.failure_rate == 0.3
        assert len(config.failure_types) == 2
        
        # Test custom values
        custom_config = ChaosConfig(
            failure_rate=0.8,
            failure_types=[FailureType.MEMORY_EXHAUSTION, FailureType.CPU_EXHAUSTION]
        )
        assert custom_config.failure_rate == 0.8
        assert len(custom_config.failure_types) == 2
        assert FailureType.MEMORY_EXHAUSTION in custom_config.failure_types
        
    @pytest.mark.asyncio
    async def test_comprehensive_chaos_scenario(self):
        """Test comprehensive chaos engineering scenario"""
        # Create multiple service mocks
        kafka_mock = AsyncMock()
        qdrant_mock = AsyncMock()
        
        # Run multiple chaos tests
        await self.test_runner.run_service_failure_test(kafka_mock, "kafka")
        await self.test_runner.run_service_failure_test(qdrant_mock, "qdrant")
        await self.test_runner.run_network_partition_test(kafka_mock)
        await self.test_runner.run_resource_exhaustion_test()
        
        # Verify comprehensive metrics
        metrics_summary = self.test_runner.metrics.get_metrics_summary()
        assert metrics_summary['total_requests'] > 0
        assert metrics_summary['failure_count'] > 0
        assert len(metrics_summary['error_types']) > 0
        
        # Verify resilience
        success_rate = metrics_summary['success_rate']
        assert 0 <= success_rate <= 1