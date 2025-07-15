#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Performance testing module for VectorStream Pipeline
"""

import asyncio
import time
import json
import threading
import psutil
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from pathlib import Path
import sys
from datetime import datetime
from loguru import logger

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.embedding_processor import EmbeddingProcessor


@dataclass
class LoadTestConfig:
    """Configuration for load testing"""
    message_count: int = 1000
    batch_size: int = 32
    concurrent_workers: int = 4
    test_duration_seconds: int = 60
    target_throughput: int = 100  # messages per second
    max_latency_ms: int = 1000
    enable_monitoring: bool = True


@dataclass
class PerformanceMetrics:
    """Performance test results"""
    test_name: str
    start_time: str
    end_time: str
    duration_seconds: float
    total_messages: int
    successful_messages: int
    failed_messages: int
    throughput_msg_per_sec: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    min_latency_ms: float
    cpu_usage_avg: float
    memory_usage_avg: float
    memory_usage_peak: float
    disk_usage_avg: float
    error_rate: float
    resource_utilization: Dict[str, Any]
    test_config: Dict[str, Any]


class SystemMetricsCollector:
    """System metrics collection"""
    
    def __init__(self):
        self.cpu_samples = []
        self.memory_samples = []
        self.disk_samples = []
        self.monitoring_active = False
        self.monitor_thread = None
    
    def start_monitoring(self, interval_seconds: float = 1.0):
        """Start system metrics monitoring"""
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop, 
            args=(interval_seconds,),
            daemon=True
        )
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop system metrics monitoring"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2.0)
    
    def _monitor_loop(self, interval_seconds: float):
        """Monitoring loop"""
        while self.monitoring_active:
            try:
                cpu = self.collect_cpu_usage()
                memory = self.collect_memory_usage()
                disk = self.collect_disk_usage()
                
                self.cpu_samples.append(cpu)
                self.memory_samples.append(memory)
                self.disk_samples.append(disk)
                
                time.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                break
    
    def collect_cpu_usage(self) -> float:
        """Collect CPU usage percentage"""
        return psutil.cpu_percent(interval=0.1)
    
    def collect_memory_usage(self) -> float:
        """Collect memory usage percentage"""
        return psutil.virtual_memory().percent
    
    def collect_disk_usage(self) -> float:
        """Collect disk usage percentage"""
        return psutil.disk_usage('/').percent
    
    def get_average_metrics(self) -> Dict[str, float]:
        """Get average metrics"""
        return {
            'cpu_avg': sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0.0,
            'memory_avg': sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0.0,
            'disk_avg': sum(self.disk_samples) / len(self.disk_samples) if self.disk_samples else 0.0
        }


class LoadTestRunner:
    """Load test runner for performance testing"""
    
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.test_results: List[PerformanceMetrics] = []
        self.monitoring_active = False
        self.resource_metrics = {
            'cpu_samples': [],
            'memory_samples': [],
            'disk_samples': []
        }
        self.metrics_collector = SystemMetricsCollector()
    
    def generate_test_messages(self, count: int) -> List[Dict[str, Any]]:
        """Generate test messages for load testing"""
        messages = []
        for i in range(count):
            message = {
                'id': f'test_msg_{i}',
                'content': f'This is a test message number {i} for performance testing. '
                          f'It contains some sample text to test embedding generation.',
                'metadata': {
                    'test_batch': i // self.config.batch_size,
                    'message_index': i,
                    'generated_at': datetime.now().isoformat()
                },
                'timestamp': datetime.now().isoformat()
            }
            messages.append(message)
        return messages
    
    async def process_message_batch(self, messages: List[Dict[str, Any]], 
                                  processor: EmbeddingProcessor) -> List[float]:
        """Process a batch of messages and return latencies"""
        latencies = []
        
        for message in messages:
            start_time = time.time()
            try:
                await processor.process_message(message)
                latency = (time.time() - start_time) * 1000  # Convert to milliseconds
                latencies.append(latency)
            except Exception as e:
                logger.error(f"Message processing failed: {e}")
                latencies.append(-1)  # Mark as failed
        
        return latencies
    
    def start_monitoring(self):
        """Start resource monitoring"""
        if self.config.enable_monitoring:
            self.metrics_collector.start_monitoring()
            self.monitoring_active = True
    
    def stop_monitoring(self):
        """Stop resource monitoring"""
        if self.monitoring_active:
            self.metrics_collector.stop_monitoring()
            self.monitoring_active = False
            
            # Copy metrics to resource_metrics for compatibility
            metrics = self.metrics_collector.get_average_metrics()
            self.resource_metrics['cpu_samples'] = [metrics['cpu_avg']]
            self.resource_metrics['memory_samples'] = [metrics['memory_avg']]
            self.resource_metrics['disk_samples'] = [metrics['disk_avg']]
    
    async def run_throughput_test(self, test_name: str) -> PerformanceMetrics:
        """Run throughput test"""
        logger.info(f"Starting throughput test: {test_name}")
        
        # Start monitoring
        self.start_monitoring()
        
        try:
            # Initialize embedding processor
            embedding_config = {
                'model_name': 'all-MiniLM-L6-v2',
                'batch_size': self.config.batch_size
            }
            processor = EmbeddingProcessor(embedding_config)
            
            # Generate test messages
            messages = self.generate_test_messages(self.config.message_count)
            
            # Process messages and measure performance
            start_time = time.time()
            all_latencies = []
            
            # Process in batches
            for i in range(0, len(messages), self.config.batch_size):
                batch = messages[i:i + self.config.batch_size]
                batch_latencies = await self.process_message_batch(batch, processor)
                all_latencies.extend(batch_latencies)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Calculate metrics
            successful_latencies = [lat for lat in all_latencies if lat > 0]
            failed_count = len([lat for lat in all_latencies if lat < 0])
            
            if successful_latencies:
                successful_latencies.sort()
                avg_latency = sum(successful_latencies) / len(successful_latencies)
                p95_index = int(len(successful_latencies) * 0.95)
                p99_index = int(len(successful_latencies) * 0.99)
                p95_latency = successful_latencies[p95_index] if p95_index < len(successful_latencies) else successful_latencies[-1]
                p99_latency = successful_latencies[p99_index] if p99_index < len(successful_latencies) else successful_latencies[-1]
                max_latency = max(successful_latencies)
                min_latency = min(successful_latencies)
            else:
                avg_latency = p95_latency = p99_latency = max_latency = min_latency = 0.0
            
            throughput = len(successful_latencies) / duration if duration > 0 else 0.0
            error_rate = failed_count / len(all_latencies) if all_latencies else 0.0
            
            # Stop monitoring and get resource metrics
            self.stop_monitoring()
            
            cpu_avg = sum(self.resource_metrics['cpu_samples']) / len(self.resource_metrics['cpu_samples']) if self.resource_metrics['cpu_samples'] else 0.0
            memory_avg = sum(self.resource_metrics['memory_samples']) / len(self.resource_metrics['memory_samples']) if self.resource_metrics['memory_samples'] else 0.0
            disk_avg = sum(self.resource_metrics['disk_samples']) / len(self.resource_metrics['disk_samples']) if self.resource_metrics['disk_samples'] else 0.0
            
            # Create performance metrics
            metrics = PerformanceMetrics(
                test_name=test_name,
                start_time=datetime.fromtimestamp(start_time).isoformat(),
                end_time=datetime.fromtimestamp(end_time).isoformat(),
                duration_seconds=duration,
                total_messages=len(all_latencies),
                successful_messages=len(successful_latencies),
                failed_messages=failed_count,
                throughput_msg_per_sec=throughput,
                avg_latency_ms=avg_latency,
                p95_latency_ms=p95_latency,
                p99_latency_ms=p99_latency,
                max_latency_ms=max_latency,
                min_latency_ms=min_latency,
                cpu_usage_avg=cpu_avg,
                memory_usage_avg=memory_avg,
                memory_usage_peak=max(self.resource_metrics['memory_samples']) if self.resource_metrics['memory_samples'] else 0.0,
                disk_usage_avg=disk_avg,
                error_rate=error_rate,
                resource_utilization={
                    'cpu_samples': self.resource_metrics['cpu_samples'],
                    'memory_samples': self.resource_metrics['memory_samples'],
                    'disk_samples': self.resource_metrics['disk_samples']
                },
                test_config={
                    'message_count': self.config.message_count,
                    'batch_size': self.config.batch_size,
                    'concurrent_workers': self.config.concurrent_workers
                }
            )
            
            self.test_results.append(metrics)
            
            logger.info(f"Throughput test completed: {test_name}")
            logger.info(f"  Messages processed: {len(successful_latencies)}/{len(all_latencies)}")
            logger.info(f"  Throughput: {throughput:.2f} msg/sec")
            logger.info(f"  Average latency: {avg_latency:.2f} ms")
            logger.info(f"  Error rate: {error_rate:.2%}")
            
            return metrics
            
        except Exception as e:
            self.stop_monitoring()
            logger.error(f"Throughput test failed: {e}")
            raise
    
    async def run_latency_test(self, test_name: str) -> PerformanceMetrics:
        """Run latency test"""
        logger.info(f"Starting latency test: {test_name}")
        
        # For latency test, use smaller batches and focus on response time
        latency_config = LoadTestConfig(
            message_count=min(100, self.config.message_count),
            batch_size=1,  # Process one by one for accurate latency measurement
            concurrent_workers=1,
            test_duration_seconds=self.config.test_duration_seconds,
            target_throughput=self.config.target_throughput,
            max_latency_ms=self.config.max_latency_ms,
            enable_monitoring=self.config.enable_monitoring
        )
        
        # Temporarily update config for latency test
        original_config = self.config
        self.config = latency_config
        
        try:
            result = await self.run_throughput_test(test_name)
            return result
        finally:
            self.config = original_config
    
    async def run_scalability_test(self, test_name: str) -> List[PerformanceMetrics]:
        """Run scalability test with different worker counts"""
        logger.info(f"Starting scalability test: {test_name}")
        
        import psutil
        cpu_count = psutil.cpu_count() or 4
        worker_counts = [1, 2, 4, min(8, cpu_count)]
        
        original_workers = self.config.concurrent_workers
        results = []
        
        try:
            for workers in worker_counts:
                self.config.concurrent_workers = workers
                metrics = await self.run_throughput_test(f"{test_name}_workers_{workers}")
                results.append(metrics)
                
            return results
        finally:
            self.config.concurrent_workers = original_workers
    
    def run_stress_test(self, test_name: str) -> PerformanceMetrics:
        """Run stress test with increased load"""
        logger.info(f"Starting stress test: {test_name}")
        
        import psutil
        cpu_count = psutil.cpu_count() or 4
        
        # Store original config
        original_count = self.config.message_count
        original_workers = self.config.concurrent_workers
        
        try:
            # Increase load for stress test
            self.config.message_count = original_count * 5
            self.config.concurrent_workers = min(cpu_count * 2, 16)
            
            # Run the stress test synchronously by creating a new event loop
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(self.run_throughput_test(test_name))
                return result
            finally:
                loop.close()
                
        finally:
            # Restore original config
            self.config.message_count = original_count
            self.config.concurrent_workers = original_workers
    
    def _analyze_performance(self) -> Dict[str, Any]:
        """Analyze performance metrics"""
        if not self.test_results:
            return {}
        
        throughputs = [m.throughput_msg_per_sec for m in self.test_results]
        latencies = [m.avg_latency_ms for m in self.test_results]
        error_rates = [m.error_rate for m in self.test_results]
        
        analysis = {
            'throughput_analysis': {
                'max_throughput': max(throughputs),
                'min_throughput': min(throughputs),
                'avg_throughput': sum(throughputs) / len(throughputs)
            },
            'latency_analysis': {
                'max_latency': max(latencies),
                'min_latency': min(latencies),
                'avg_latency': sum(latencies) / len(latencies)
            },
            'reliability_analysis': {
                'max_error_rate': max(error_rates),
                'min_error_rate': min(error_rates),
                'avg_error_rate': sum(error_rates) / len(error_rates)
            },
            'recommendations': self._generate_recommendations()
        }
        
        return analysis
    
    def _generate_recommendations(self) -> List[str]:
        """Generate performance recommendations"""
        if not self.test_results:
            return []
        
        recommendations = []
        latest_metrics = self.test_results[-1]
        
        # Throughput recommendations
        if latest_metrics.throughput_msg_per_sec < self.config.target_throughput:
            recommendations.append(
                f"Throughput ({latest_metrics.throughput_msg_per_sec:.1f} msg/sec) is below target "
                f"({self.config.target_throughput} msg/sec). Consider increasing batch size or worker count."
            )
        
        # Latency recommendations
        if latest_metrics.avg_latency_ms > self.config.max_latency_ms:
            recommendations.append(
                f"Average latency ({latest_metrics.avg_latency_ms:.1f} ms) exceeds target "
                f"({self.config.max_latency_ms} ms). Consider optimizing processing pipeline."
            )
        
        # Error rate recommendations
        if latest_metrics.error_rate > 0.05:  # 5% threshold
            recommendations.append(
                f"High error rate ({latest_metrics.error_rate:.1%}) detected. "
                "Review error handling and system stability."
            )
        
        # Resource recommendations
        if latest_metrics.cpu_usage_avg > 80:
            recommendations.append(
                f"High CPU usage ({latest_metrics.cpu_usage_avg:.1f}%). "
                "Consider scaling horizontally or optimizing CPU-intensive operations."
            )
        
        if latest_metrics.memory_usage_avg > 80:
            recommendations.append(
                f"High memory usage ({latest_metrics.memory_usage_avg:.1f}%). "
                "Consider increasing memory or optimizing memory usage."
            )
        
        return recommendations
    
    def save_report(self, filename: str):
        """Save performance report to file"""
        report = self.generate_report()
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Performance report saved to {filename}")
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate performance test report"""
        if not self.test_results:
            return {'error': 'No test results available'}
        
        analysis = self._analyze_performance()
        
        report = {
            'test_summary': {
                'total_tests': len(self.test_results),
                'test_config': {
                    'message_count': self.config.message_count,
                    'batch_size': self.config.batch_size,
                    'concurrent_workers': self.config.concurrent_workers,
                    'target_throughput': self.config.target_throughput
                }
            },
            'test_results': [],
            'performance_analysis': analysis
        }
        
        for metrics in self.test_results:
            result = {
                'test_name': metrics.test_name,
                'start_time': metrics.start_time,
                'end_time': metrics.end_time,
                'duration_seconds': metrics.duration_seconds,
                'performance': {
                    'throughput_msg_per_sec': metrics.throughput_msg_per_sec,
                    'avg_latency_ms': metrics.avg_latency_ms,
                    'p95_latency_ms': metrics.p95_latency_ms,
                    'p99_latency_ms': metrics.p99_latency_ms,
                    'error_rate': metrics.error_rate
                },
                'resources': {
                    'cpu_usage_avg': metrics.cpu_usage_avg,
                    'memory_usage_avg': metrics.memory_usage_avg,
                    'memory_usage_peak': metrics.memory_usage_peak,
                    'disk_usage_avg': metrics.disk_usage_avg
                },
                'messages': {
                    'total': metrics.total_messages,
                    'successful': metrics.successful_messages,
                    'failed': metrics.failed_messages
                }
            }
            report['test_results'].append(result)
        
        return report


async def main():
    """Main function for running performance tests"""
    config = LoadTestConfig(
        message_count=1000,
        batch_size=32,
        concurrent_workers=4,
        test_duration_seconds=60,
        target_throughput=100,
        max_latency_ms=1000,
        enable_monitoring=True
    )
    
    runner = LoadTestRunner(config)
    
    try:
        # Run throughput test
        await runner.run_throughput_test("baseline_throughput")
        
        # Run latency test
        await runner.run_latency_test("baseline_latency")
        
        # Generate report
        report = runner.generate_report()
        print(json.dumps(report, indent=2))
        
    except Exception as e:
        logger.error(f"Performance test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())