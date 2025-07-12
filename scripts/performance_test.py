#!/usr/bin/env python3
"""
Performance and Load Testing Script for NewMind-AI

This script provides comprehensive load testing capabilities including:
- Message throughput testing
- Latency measurement
- Resource utilization monitoring
- Scalability testing
- Performance benchmarking
"""

import asyncio
import json
import time
import statistics
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from pathlib import Path
import sys
import argparse
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.core.kafka_consumer import KafkaConsumer
from src.core.embedding_processor import EmbeddingProcessor
from src.core.qdrant_writer import QdrantWriter
from src.config.kafka_config import KafkaConfig
from src.config.qdrant_config import QdrantConfig
from src.utils.logger import setup_logger
from src.utils.metrics import SystemMetricsCollector
from src.monitoring.prometheus_metrics import PrometheusMetrics

logger = setup_logger({'level': 'INFO'})

@dataclass
class LoadTestConfig:
    """Load test configuration"""
    message_count: int = 1000
    batch_size: int = 100
    concurrent_workers: int = 5
    test_duration_seconds: int = 60
    ramp_up_seconds: int = 10
    target_throughput: int = 100  # messages per second
    max_latency_ms: int = 1000
    enable_monitoring: bool = True
    output_file: Optional[str] = None

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

class LoadTestRunner:
    """Main load testing runner"""
    
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.metrics_collector = SystemMetricsCollector()
        self.prometheus_metrics = PrometheusMetrics()
        self.test_results: List[PerformanceMetrics] = []
        self.monitoring_active = False
        self.resource_metrics = {
            'cpu_samples': [],
            'memory_samples': [],
            'disk_samples': []
        }
        
    def start_monitoring(self):
        """Start resource monitoring"""
        if not self.config.enable_monitoring:
            return
            
        self.monitoring_active = True
        
        def monitor():
            while self.monitoring_active:
                try:
                    cpu = self.metrics_collector.collect_cpu_usage()
                    memory = self.metrics_collector.collect_memory_usage()
                    disk = self.metrics_collector.collect_disk_usage()
                    
                    self.resource_metrics['cpu_samples'].append(cpu)
                    self.resource_metrics['memory_samples'].append(memory)
                    self.resource_metrics['disk_samples'].append(disk)
                    
                    time.sleep(1)  # Sample every second
                except Exception as e:
                    logger.warning(f"Monitoring error: {e}")
                    
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()
        
    def stop_monitoring(self):
        """Stop resource monitoring"""
        self.monitoring_active = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=2)
            
    def generate_test_messages(self, count: int) -> List[Dict[str, Any]]:
        """Generate test messages for load testing"""
        messages = []
        for i in range(count):
            message = {
                "id": f"test_msg_{i}",
                "content": f"This is test message number {i} for load testing. "
                          f"It contains some sample text to generate embeddings. "
                          f"Message timestamp: {datetime.now().isoformat()}",
                "metadata": {
                    "category": "test",
                    "priority": "normal",
                    "test_batch": i // self.config.batch_size
                },
                "timestamp": datetime.now().isoformat()
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
                # Process message
                await processor.process_message(message)
                latency = (time.time() - start_time) * 1000  # Convert to ms
                latencies.append(latency)
            except Exception as e:
                logger.error(f"Message processing failed: {e}")
                latencies.append(-1)  # Mark as failed
                
        return latencies
        
    async def run_throughput_test(self, test_name: str = "throughput_test") -> PerformanceMetrics:
        """Run throughput test"""
        logger.info(f"Starting throughput test: {test_name}")
        
        # Generate test messages
        messages = self.generate_test_messages(self.config.message_count)
        
        # Initialize components
        embedding_processor = EmbeddingProcessor({
            'model_name': 'all-MiniLM-L6-v2',
            'batch_size': self.config.batch_size
        })
        
        # Start monitoring
        start_time = datetime.now()
        self.start_monitoring()
        
        # Process messages with concurrent workers
        all_latencies = []
        successful_count = 0
        failed_count = 0
        
        batch_size = len(messages) // self.config.concurrent_workers
        batches = [messages[i:i + batch_size] for i in range(0, len(messages), batch_size)]
        
        test_start = time.time()
        
        with ThreadPoolExecutor(max_workers=self.config.concurrent_workers) as executor:
            futures = []
            for batch in batches:
                future = executor.submit(asyncio.run, self.process_message_batch(batch, embedding_processor))
                futures.append(future)
                
            for future in as_completed(futures):
                try:
                    batch_latencies = future.result()
                    for latency in batch_latencies:
                        if latency > 0:
                            all_latencies.append(latency)
                            successful_count += 1
                        else:
                            failed_count += 1
                except Exception as e:
                    logger.error(f"Batch processing failed: {e}")
                    failed_count += len(batches[0])  # Estimate failed count
                    
        test_end = time.time()
        duration = test_end - test_start
        
        # Stop monitoring
        self.stop_monitoring()
        end_time = datetime.now()
        
        # Calculate metrics
        throughput = successful_count / duration if duration > 0 else 0
        error_rate = failed_count / (successful_count + failed_count) if (successful_count + failed_count) > 0 else 0
        
        # Latency statistics
        if all_latencies:
            avg_latency = statistics.mean(all_latencies)
            p95_latency = statistics.quantiles(all_latencies, n=20)[18]  # 95th percentile
            p99_latency = statistics.quantiles(all_latencies, n=100)[98]  # 99th percentile
            max_latency = max(all_latencies)
            min_latency = min(all_latencies)
        else:
            avg_latency = p95_latency = p99_latency = max_latency = min_latency = 0
            
        # Resource utilization
        cpu_avg = statistics.mean(self.resource_metrics['cpu_samples']) if self.resource_metrics['cpu_samples'] else 0
        memory_avg = statistics.mean(self.resource_metrics['memory_samples']) if self.resource_metrics['memory_samples'] else 0
        memory_peak = max(self.resource_metrics['memory_samples']) if self.resource_metrics['memory_samples'] else 0
        disk_avg = statistics.mean(self.resource_metrics['disk_samples']) if self.resource_metrics['disk_samples'] else 0
        
        # Create performance metrics
        metrics = PerformanceMetrics(
            test_name=test_name,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            total_messages=self.config.message_count,
            successful_messages=successful_count,
            failed_messages=failed_count,
            throughput_msg_per_sec=throughput,
            avg_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            max_latency_ms=max_latency,
            min_latency_ms=min_latency,
            cpu_usage_avg=cpu_avg,
            memory_usage_avg=memory_avg,
            memory_usage_peak=memory_peak,
            disk_usage_avg=disk_avg,
            error_rate=error_rate,
            resource_utilization={
                'cpu_samples': len(self.resource_metrics['cpu_samples']),
                'memory_samples': len(self.resource_metrics['memory_samples']),
                'disk_samples': len(self.resource_metrics['disk_samples'])
            },
            test_config=asdict(self.config)
        )
        
        self.test_results.append(metrics)
        logger.info(f"Throughput test completed: {throughput:.2f} msg/sec, {error_rate:.2%} error rate")
        
        return metrics
        
    async def run_latency_test(self, test_name: str = "latency_test") -> PerformanceMetrics:
        """Run latency-focused test with smaller batches"""
        logger.info(f"Starting latency test: {test_name}")
        
        # Use smaller batches for latency testing
        original_batch_size = self.config.batch_size
        self.config.batch_size = min(10, self.config.batch_size)
        
        try:
            metrics = await self.run_throughput_test(test_name)
            return metrics
        finally:
            self.config.batch_size = original_batch_size
            
    async def run_scalability_test(self, test_name: str = "scalability_test") -> List[PerformanceMetrics]:
        """Run scalability test with varying worker counts"""
        logger.info(f"Starting scalability test: {test_name}")
        
        original_workers = self.config.concurrent_workers
        worker_counts = [1, 2, 4, 8, 16]
        results = []
        
        for worker_count in worker_counts:
            if worker_count > psutil.cpu_count():
                continue
                
            self.config.concurrent_workers = worker_count
            logger.info(f"Testing with {worker_count} workers")
            
            metrics = await self.run_throughput_test(f"{test_name}_workers_{worker_count}")
            results.append(metrics)
            
            # Reset resource metrics for next test
            self.resource_metrics = {
                'cpu_samples': [],
                'memory_samples': [],
                'disk_samples': []
            }
            
        self.config.concurrent_workers = original_workers
        return results
        
    def run_stress_test(self, test_name: str = "stress_test") -> PerformanceMetrics:
        """Run stress test with high load"""
        logger.info(f"Starting stress test: {test_name}")
        
        # Increase load for stress testing
        original_count = self.config.message_count
        original_workers = self.config.concurrent_workers
        
        self.config.message_count = min(10000, self.config.message_count * 10)
        self.config.concurrent_workers = min(psutil.cpu_count(), self.config.concurrent_workers * 2)
        
        try:
            metrics = asyncio.run(self.run_throughput_test(test_name))
            return metrics
        finally:
            self.config.message_count = original_count
            self.config.concurrent_workers = original_workers
            
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        if not self.test_results:
            return {"error": "No test results available"}
            
        report = {
            "test_summary": {
                "total_tests": len(self.test_results),
                "test_date": datetime.now().isoformat(),
                "system_info": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": psutil.virtual_memory().total / (1024**3),
                    "platform": sys.platform
                }
            },
            "test_results": [asdict(result) for result in self.test_results],
            "performance_analysis": self._analyze_performance()
        }
        
        return report
        
    def _analyze_performance(self) -> Dict[str, Any]:
        """Analyze performance results"""
        if not self.test_results:
            return {}
            
        throughputs = [r.throughput_msg_per_sec for r in self.test_results]
        latencies = [r.avg_latency_ms for r in self.test_results]
        error_rates = [r.error_rate for r in self.test_results]
        
        analysis = {
            "throughput_analysis": {
                "max_throughput": max(throughputs),
                "min_throughput": min(throughputs),
                "avg_throughput": statistics.mean(throughputs)
            },
            "latency_analysis": {
                "max_latency": max(latencies),
                "min_latency": min(latencies),
                "avg_latency": statistics.mean(latencies)
            },
            "reliability_analysis": {
                "max_error_rate": max(error_rates),
                "min_error_rate": min(error_rates),
                "avg_error_rate": statistics.mean(error_rates)
            },
            "recommendations": self._generate_recommendations()
        }
        
        return analysis
        
    def _generate_recommendations(self) -> List[str]:
        """Generate performance recommendations"""
        recommendations = []
        
        if not self.test_results:
            return recommendations
            
        avg_throughput = statistics.mean([r.throughput_msg_per_sec for r in self.test_results])
        avg_latency = statistics.mean([r.avg_latency_ms for r in self.test_results])
        avg_error_rate = statistics.mean([r.error_rate for r in self.test_results])
        avg_cpu = statistics.mean([r.cpu_usage_avg for r in self.test_results])
        avg_memory = statistics.mean([r.memory_usage_avg for r in self.test_results])
        
        if avg_throughput < self.config.target_throughput:
            recommendations.append(f"Throughput ({avg_throughput:.1f} msg/sec) is below target ({self.config.target_throughput} msg/sec). Consider increasing batch size or worker count.")
            
        if avg_latency > self.config.max_latency_ms:
            recommendations.append(f"Average latency ({avg_latency:.1f}ms) exceeds target ({self.config.max_latency_ms}ms). Consider optimizing processing pipeline.")
            
        if avg_error_rate > 0.01:  # 1%
            recommendations.append(f"Error rate ({avg_error_rate:.2%}) is high. Check error handling and system stability.")
            
        if avg_cpu > 80:
            recommendations.append(f"High CPU usage ({avg_cpu:.1f}%). Consider scaling horizontally or optimizing CPU-intensive operations.")
            
        if avg_memory > 80:
            recommendations.append(f"High memory usage ({avg_memory:.1f}%). Check for memory leaks or consider increasing available memory.")
            
        if not recommendations:
            recommendations.append("Performance is within acceptable limits. System is performing well.")
            
        return recommendations
        
    def save_report(self, filename: Optional[str] = None):
        """Save performance report to file"""
        if filename is None:
            filename = f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
        report = self.generate_report()
        
        output_path = Path(filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Performance report saved to: {output_path}")
        
def main():
    """Main function for CLI usage"""
    parser = argparse.ArgumentParser(description="NewMind-AI Performance Testing")
    parser.add_argument("--test-type", choices=["throughput", "latency", "scalability", "stress", "all"], 
                       default="all", help="Type of test to run")
    parser.add_argument("--message-count", type=int, default=1000, help="Number of messages to test")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--workers", type=int, default=5, help="Number of concurrent workers")
    parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
    parser.add_argument("--target-throughput", type=int, default=100, help="Target throughput (msg/sec)")
    parser.add_argument("--max-latency", type=int, default=1000, help="Maximum acceptable latency (ms)")
    parser.add_argument("--output", type=str, help="Output file for results")
    parser.add_argument("--no-monitoring", action="store_true", help="Disable resource monitoring")
    
    args = parser.parse_args()
    
    # Create test configuration
    config = LoadTestConfig(
        message_count=args.message_count,
        batch_size=args.batch_size,
        concurrent_workers=args.workers,
        test_duration_seconds=args.duration,
        target_throughput=args.target_throughput,
        max_latency_ms=args.max_latency,
        enable_monitoring=not args.no_monitoring,
        output_file=args.output
    )
    
    # Create test runner
    runner = LoadTestRunner(config)
    
    try:
        logger.info("Starting performance tests...")
        
        if args.test_type in ["throughput", "all"]:
            asyncio.run(runner.run_throughput_test())
            
        if args.test_type in ["latency", "all"]:
            asyncio.run(runner.run_latency_test())
            
        if args.test_type in ["scalability", "all"]:
            asyncio.run(runner.run_scalability_test())
            
        if args.test_type in ["stress", "all"]:
            runner.run_stress_test()
            
        # Generate and save report
        runner.save_report(args.output)
        
        # Print summary
        report = runner.generate_report()
        print("\n" + "="*50)
        print("PERFORMANCE TEST SUMMARY")
        print("="*50)
        print(f"Total tests: {report['test_summary']['total_tests']}")
        
        if 'performance_analysis' in report:
            analysis = report['performance_analysis']
            if 'throughput_analysis' in analysis:
                print(f"Max throughput: {analysis['throughput_analysis']['max_throughput']:.2f} msg/sec")
            if 'latency_analysis' in analysis:
                print(f"Avg latency: {analysis['latency_analysis']['avg_latency']:.2f} ms")
            if 'recommendations' in analysis:
                print("\nRecommendations:")
                for rec in analysis['recommendations']:
                    print(f"- {rec}")
                    
        logger.info("Performance testing completed successfully")
        
    except Exception as e:
        logger.error(f"Performance testing failed: {e}")
        sys.exit(1)
        
if __name__ == "__main__":
    main()