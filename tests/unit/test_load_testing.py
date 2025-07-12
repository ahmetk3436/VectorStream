#!/usr/bin/env python3

import pytest
import asyncio
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from scripts.performance_test import LoadTestRunner, LoadTestConfig, PerformanceMetrics

class TestLoadTesting:
    """Load testing functionality tests"""
    
    @pytest.fixture
    def load_test_config(self):
        """Test configuration for load testing"""
        return LoadTestConfig(
            message_count=100,
            batch_size=10,
            concurrent_workers=2,
            test_duration_seconds=5,
            target_throughput=50,
            max_latency_ms=500,
            enable_monitoring=True
        )
    
    @pytest.fixture
    def mock_embedding_processor(self):
        """Mock embedding processor"""
        processor = Mock()
        processor.process_message = Mock(return_value=asyncio.Future())
        processor.process_message.return_value.set_result(None)
        return processor
    
    def test_load_test_config_creation(self, load_test_config):
        """Test load test configuration creation"""
        assert load_test_config.message_count == 100
        assert load_test_config.batch_size == 10
        assert load_test_config.concurrent_workers == 2
        assert load_test_config.enable_monitoring is True
    
    def test_load_test_runner_initialization(self, load_test_config):
        """Test load test runner initialization"""
        runner = LoadTestRunner(load_test_config)
        
        assert runner.config == load_test_config
        assert runner.test_results == []
        assert runner.monitoring_active is False
        assert 'cpu_samples' in runner.resource_metrics
        assert 'memory_samples' in runner.resource_metrics
        assert 'disk_samples' in runner.resource_metrics
    
    def test_generate_test_messages(self, load_test_config):
        """Test test message generation"""
        runner = LoadTestRunner(load_test_config)
        messages = runner.generate_test_messages(10)
        
        assert len(messages) == 10
        assert all('id' in msg for msg in messages)
        assert all('content' in msg for msg in messages)
        assert all('metadata' in msg for msg in messages)
        assert all('timestamp' in msg for msg in messages)
        
        # Check message content
        assert messages[0]['id'] == 'test_msg_0'
        assert 'test message number 0' in messages[0]['content']
        assert messages[0]['metadata']['test_batch'] == 0
    
    @pytest.mark.asyncio
    @patch('scripts.performance_test.EmbeddingProcessor')
    @patch('scripts.performance_test.SystemMetricsCollector')
    async def test_process_message_batch(self, mock_metrics, mock_processor_class, 
                                       load_test_config, mock_embedding_processor):
        """Test message batch processing"""
        mock_processor_class.return_value = mock_embedding_processor
        
        runner = LoadTestRunner(load_test_config)
        messages = runner.generate_test_messages(5)
        
        # Mock successful processing
        async def mock_process(msg):
            await asyncio.sleep(0.01)  # Simulate processing time
            return None
            
        mock_embedding_processor.process_message.side_effect = mock_process
        
        latencies = await runner.process_message_batch(messages, mock_embedding_processor)
        
        assert len(latencies) == 5
        assert all(lat > 0 for lat in latencies)  # All should be positive (successful)
        assert mock_embedding_processor.process_message.call_count == 5
    
    @pytest.mark.asyncio
    @patch('scripts.performance_test.EmbeddingProcessor')
    @patch('scripts.performance_test.SystemMetricsCollector')
    async def test_process_message_batch_with_failures(self, mock_metrics, mock_processor_class, 
                                                     load_test_config, mock_embedding_processor):
        """Test message batch processing with failures"""
        mock_processor_class.return_value = mock_embedding_processor
        
        runner = LoadTestRunner(load_test_config)
        messages = runner.generate_test_messages(3)
        
        # Mock processing with some failures
        async def mock_process_with_failure(msg):
            if 'test_msg_1' in msg['id']:
                raise Exception("Processing failed")
            await asyncio.sleep(0.01)
            return None
            
        mock_embedding_processor.process_message.side_effect = mock_process_with_failure
        
        latencies = await runner.process_message_batch(messages, mock_embedding_processor)
        
        assert len(latencies) == 3
        assert latencies[1] == -1  # Failed message
        assert latencies[0] > 0 and latencies[2] > 0  # Successful messages
    
    @pytest.mark.asyncio
    @patch('scripts.performance_test.EmbeddingProcessor')
    @patch('scripts.performance_test.SystemMetricsCollector')
    async def test_throughput_test(self, mock_metrics, mock_processor_class, 
                                 load_test_config):
        """Test throughput testing"""
        # Mock components
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        # Mock successful processing
        async def mock_process(msg):
            await asyncio.sleep(0.001)  # Very fast processing
            return None
            
        mock_processor.process_message.side_effect = mock_process
        
        # Mock metrics collector
        mock_metrics_instance = Mock()
        mock_metrics_instance.collect_cpu_usage.return_value = 50.0
        mock_metrics_instance.collect_memory_usage.return_value = 60.0
        mock_metrics_instance.collect_disk_usage.return_value = 70.0
        mock_metrics.return_value = mock_metrics_instance
        
        # Create a smaller config for faster testing
        small_config = LoadTestConfig(
            message_count=10,  # Much smaller for testing
            batch_size=5,
            concurrent_workers=1,  # Single worker to avoid threading issues
            test_duration_seconds=1,  # Short duration
            target_throughput=50,
            max_latency_ms=500,
            enable_monitoring=False  # Disable monitoring to avoid threading
        )
        
        runner = LoadTestRunner(small_config)
        
        # Mock monitoring completely to avoid threading issues
        runner.start_monitoring = Mock()
        runner.stop_monitoring = Mock()
        runner.resource_metrics = {
            'cpu_samples': [50.0],
            'memory_samples': [60.0],
            'disk_samples': [70.0]
        }
        
        metrics = await runner.run_throughput_test("test_throughput")
        
        assert isinstance(metrics, PerformanceMetrics)
        assert metrics.test_name == "test_throughput"
        assert metrics.total_messages == small_config.message_count
        assert metrics.successful_messages > 0
        assert metrics.throughput_msg_per_sec > 0
        assert metrics.duration_seconds > 0
        assert len(runner.test_results) == 1
    
    @pytest.mark.asyncio
    @patch('scripts.performance_test.EmbeddingProcessor')
    @patch('scripts.performance_test.SystemMetricsCollector')
    async def test_latency_test(self, mock_metrics, mock_processor_class, load_test_config):
        """Test latency testing"""
        # Mock components
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        async def mock_process(msg):
            await asyncio.sleep(0.001)
            return None
            
        mock_processor.process_message.side_effect = mock_process
        
        runner = LoadTestRunner(load_test_config)
        
        # Mock monitoring
        runner.start_monitoring = Mock()
        runner.stop_monitoring = Mock()
        runner.resource_metrics = {
            'cpu_samples': [45.0],
            'memory_samples': [55.0],
            'disk_samples': [65.0]
        }
        
        original_batch_size = load_test_config.batch_size
        metrics = await runner.run_latency_test("test_latency")
        
        assert isinstance(metrics, PerformanceMetrics)
        assert metrics.test_name == "test_latency"
        # Batch size should be restored
        assert load_test_config.batch_size == original_batch_size
    
    @pytest.mark.asyncio
    @patch('scripts.performance_test.EmbeddingProcessor')
    @patch('scripts.performance_test.SystemMetricsCollector')
    @patch('psutil.cpu_count')
    async def test_scalability_test(self, mock_cpu_count, mock_metrics, mock_processor_class, 
                                  load_test_config):
        """Test scalability testing"""
        mock_cpu_count.return_value = 8
        
        # Mock components
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        async def mock_process(msg):
            await asyncio.sleep(0.001)
            return None
            
        mock_processor.process_message.side_effect = mock_process
        
        runner = LoadTestRunner(load_test_config)
        
        # Mock monitoring
        runner.start_monitoring = Mock()
        runner.stop_monitoring = Mock()
        runner.resource_metrics = {
            'cpu_samples': [50.0],
            'memory_samples': [60.0],
            'disk_samples': [70.0]
        }
        
        original_workers = load_test_config.concurrent_workers
        results = await runner.run_scalability_test("test_scalability")
        
        assert isinstance(results, list)
        assert len(results) > 0
        assert all(isinstance(r, PerformanceMetrics) for r in results)
        # Worker count should be restored
        assert load_test_config.concurrent_workers == original_workers
    
    @patch('scripts.performance_test.EmbeddingProcessor')
    @patch('scripts.performance_test.SystemMetricsCollector')
    @patch('psutil.cpu_count')
    def test_stress_test(self, mock_cpu_count, mock_metrics, mock_processor_class, 
                        load_test_config):
        """Test stress testing"""
        mock_cpu_count.return_value = 8
        
        # Mock components
        mock_processor = Mock()
        mock_processor_class.return_value = mock_processor
        
        async def mock_process(msg):
            await asyncio.sleep(0.001)
            return None
            
        mock_processor.process_message.side_effect = mock_process
        
        runner = LoadTestRunner(load_test_config)
        
        # Mock monitoring
        runner.start_monitoring = Mock()
        runner.stop_monitoring = Mock()
        runner.resource_metrics = {
            'cpu_samples': [80.0, 85.0],
            'memory_samples': [75.0, 78.0],
            'disk_samples': [60.0, 62.0]
        }
        
        # Mock the async run_throughput_test method
        async def mock_run_throughput_test(test_name):
            return PerformanceMetrics(
                test_name=test_name,
                start_time="2024-01-01T00:00:00",
                end_time="2024-01-01T00:01:00",
                duration_seconds=60.0,
                total_messages=1000,
                successful_messages=950,
                failed_messages=50,
                throughput_msg_per_sec=15.8,
                avg_latency_ms=100.0,
                p95_latency_ms=200.0,
                p99_latency_ms=300.0,
                max_latency_ms=500.0,
                min_latency_ms=50.0,
                cpu_usage_avg=82.5,
                memory_usage_avg=76.5,
                memory_usage_peak=78.0,
                disk_usage_avg=61.0,
                error_rate=0.05,
                resource_utilization={},
                test_config={}
            )
        
        runner.run_throughput_test = mock_run_throughput_test
        
        original_count = load_test_config.message_count
        original_workers = load_test_config.concurrent_workers
        
        metrics = runner.run_stress_test("test_stress")
        
        assert isinstance(metrics, PerformanceMetrics)
        assert metrics.test_name == "test_stress"
        # Configuration should be restored
        assert load_test_config.message_count == original_count
        assert load_test_config.concurrent_workers == original_workers
    
    def test_generate_report_empty(self, load_test_config):
        """Test report generation with no results"""
        runner = LoadTestRunner(load_test_config)
        report = runner.generate_report()
        
        assert "error" in report
        assert report["error"] == "No test results available"
    
    @patch('psutil.cpu_count')
    @patch('psutil.virtual_memory')
    def test_generate_report_with_results(self, mock_memory, mock_cpu_count, load_test_config):
        """Test report generation with results"""
        mock_cpu_count.return_value = 4
        mock_memory.return_value.total = 8 * 1024**3  # 8GB
        
        runner = LoadTestRunner(load_test_config)
        
        # Add test results
        test_metrics = PerformanceMetrics(
            test_name="test_report",
            start_time="2024-01-01T00:00:00",
            end_time="2024-01-01T00:01:00",
            duration_seconds=60.0,
            total_messages=100,
            successful_messages=95,
            failed_messages=5,
            throughput_msg_per_sec=1.58,
            avg_latency_ms=50.0,
            p95_latency_ms=100.0,
            p99_latency_ms=150.0,
            max_latency_ms=200.0,
            min_latency_ms=25.0,
            cpu_usage_avg=60.0,
            memory_usage_avg=70.0,
            memory_usage_peak=75.0,
            disk_usage_avg=50.0,
            error_rate=0.05,
            resource_utilization={"cpu_samples": 60},
            test_config={"message_count": 100}
        )
        
        runner.test_results.append(test_metrics)
        
        report = runner.generate_report()
        
        assert "test_summary" in report
        assert "test_results" in report
        assert "performance_analysis" in report
        assert report["test_summary"]["total_tests"] == 1
        assert len(report["test_results"]) == 1
        assert "throughput_analysis" in report["performance_analysis"]
        assert "latency_analysis" in report["performance_analysis"]
        assert "reliability_analysis" in report["performance_analysis"]
        assert "recommendations" in report["performance_analysis"]
    
    def test_performance_analysis(self, load_test_config):
        """Test performance analysis"""
        runner = LoadTestRunner(load_test_config)
        
        # Add multiple test results
        results = [
            PerformanceMetrics(
                test_name=f"test_{i}",
                start_time="2024-01-01T00:00:00",
                end_time="2024-01-01T00:01:00",
                duration_seconds=60.0,
                total_messages=100,
                successful_messages=95,
                failed_messages=5,
                throughput_msg_per_sec=float(10 + i),
                avg_latency_ms=float(50 + i * 10),
                p95_latency_ms=100.0,
                p99_latency_ms=150.0,
                max_latency_ms=200.0,
                min_latency_ms=25.0,
                cpu_usage_avg=60.0,
                memory_usage_avg=70.0,
                memory_usage_peak=75.0,
                disk_usage_avg=50.0,
                error_rate=0.01 * i,
                resource_utilization={},
                test_config={}
            )
            for i in range(3)
        ]
        
        runner.test_results.extend(results)
        analysis = runner._analyze_performance()
        
        assert "throughput_analysis" in analysis
        assert "latency_analysis" in analysis
        assert "reliability_analysis" in analysis
        assert "recommendations" in analysis
        
        # Check throughput analysis
        throughput = analysis["throughput_analysis"]
        assert throughput["max_throughput"] == 12.0
        assert throughput["min_throughput"] == 10.0
        assert throughput["avg_throughput"] == 11.0
    
    def test_generate_recommendations(self, load_test_config):
        """Test recommendation generation"""
        runner = LoadTestRunner(load_test_config)
        
        # Test with poor performance metrics
        poor_metrics = PerformanceMetrics(
            test_name="poor_test",
            start_time="2024-01-01T00:00:00",
            end_time="2024-01-01T00:01:00",
            duration_seconds=60.0,
            total_messages=100,
            successful_messages=80,
            failed_messages=20,
            throughput_msg_per_sec=10.0,  # Below target of 50
            avg_latency_ms=1500.0,  # Above target of 500
            p95_latency_ms=2000.0,
            p99_latency_ms=3000.0,
            max_latency_ms=5000.0,
            min_latency_ms=100.0,
            cpu_usage_avg=90.0,  # High CPU
            memory_usage_avg=85.0,  # High memory
            memory_usage_peak=90.0,
            disk_usage_avg=50.0,
            error_rate=0.2,  # High error rate
            resource_utilization={},
            test_config={}
        )
        
        runner.test_results.append(poor_metrics)
        recommendations = runner._generate_recommendations()
        
        assert len(recommendations) > 0
        assert any("throughput" in rec.lower() for rec in recommendations)
        assert any("latency" in rec.lower() for rec in recommendations)
        assert any("error rate" in rec.lower() for rec in recommendations)
        assert any("cpu" in rec.lower() for rec in recommendations)
        assert any("memory" in rec.lower() for rec in recommendations)
    
    def test_save_report(self, load_test_config):
        """Test report saving"""
        runner = LoadTestRunner(load_test_config)
        
        # Add a test result
        test_metrics = PerformanceMetrics(
            test_name="test_save",
            start_time="2024-01-01T00:00:00",
            end_time="2024-01-01T00:01:00",
            duration_seconds=60.0,
            total_messages=100,
            successful_messages=95,
            failed_messages=5,
            throughput_msg_per_sec=1.58,
            avg_latency_ms=50.0,
            p95_latency_ms=100.0,
            p99_latency_ms=150.0,
            max_latency_ms=200.0,
            min_latency_ms=25.0,
            cpu_usage_avg=60.0,
            memory_usage_avg=70.0,
            memory_usage_peak=75.0,
            disk_usage_avg=50.0,
            error_rate=0.05,
            resource_utilization={},
            test_config={}
        )
        
        runner.test_results.append(test_metrics)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            runner.save_report(temp_file)
            
            # Verify file was created and contains valid JSON
            with open(temp_file, 'r') as f:
                report_data = json.load(f)
            
            assert "test_summary" in report_data
            assert "test_results" in report_data
            assert "performance_analysis" in report_data
            assert len(report_data["test_results"]) == 1
            
        finally:
            # Clean up
            Path(temp_file).unlink(missing_ok=True)
    
    def test_monitoring_start_stop(self, load_test_config):
        """Test monitoring start and stop"""
        load_test_config.enable_monitoring = True
        runner = LoadTestRunner(load_test_config)
        
        # Test monitoring start
        runner.start_monitoring()
        assert runner.monitoring_active is True
        
        # Test monitoring stop
        runner.stop_monitoring()
        assert runner.monitoring_active is False
    
    def test_monitoring_disabled(self, load_test_config):
        """Test monitoring when disabled"""
        load_test_config.enable_monitoring = False
        runner = LoadTestRunner(load_test_config)
        
        # Should not start monitoring
        runner.start_monitoring()
        assert runner.monitoring_active is False
        assert not hasattr(runner, 'monitor_thread')