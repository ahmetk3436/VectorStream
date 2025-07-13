#!/usr/bin/env python3

import time
import functools
from typing import Callable, Any, Dict, Optional
from contextlib import contextmanager
from loguru import logger

from src.monitoring.prometheus_metrics import get_metrics

class MetricsCollector:
    """Metrics toplama yardımcı sınıfı"""
    
    def __init__(self, prometheus_metrics=None):
        self.prometheus_metrics = prometheus_metrics or get_metrics()
        self.metrics = self.prometheus_metrics  # backward compatibility
        
    class TimerContext:
        def __init__(self, operation_name: str, component: str, metrics_collector):
            self.operation_name = operation_name
            self.component = component
            self.metrics_collector = metrics_collector
            self.start_time = 0
            self.duration = 0
            self.success = True
            self.error_type = None
        
        def __enter__(self):
            self.start_time = time.time()
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.duration = time.time() - self.start_time
            if exc_type is not None:
                self.success = False
                self.error_type = exc_type.__name__
            
            # Component'e göre farklı metrikleri güncelle
            if self.component == "kafka":
                if self.success:
                    if hasattr(self.metrics_collector.metrics, 'record_kafka_message_processed'):
                        self.metrics_collector.metrics.record_kafka_message_processed(self.operation_name)
                else:
                    if hasattr(self.metrics_collector.metrics, 'record_kafka_message_failed'):
                        self.metrics_collector.metrics.record_kafka_message_failed(self.operation_name, self.error_type or "unknown")
            elif self.component == "qdrant":
                status = "success" if self.success else "error"
                if hasattr(self.metrics_collector.metrics, 'record_qdrant_operation'):
                    self.metrics_collector.metrics.record_qdrant_operation(self.operation_name, status, self.duration)
            elif self.component == "embedding":
                if self.success:
                    if hasattr(self.metrics_collector.metrics, 'record_embedding_processing'):
                        self.metrics_collector.metrics.record_embedding_processing(self.duration, self.operation_name)
                else:
                    if hasattr(self.metrics_collector.metrics, 'record_processing_error'):
                        self.metrics_collector.metrics.record_processing_error("embedding", self.error_type or "unknown")
            
            logger.debug(f"{self.component}.{self.operation_name} completed in {self.duration:.3f}s (success: {self.success})")
    
    def time_operation(self, operation_name: str, component: str = "general"):
        """Operasyon süresini ölç"""
        return self.TimerContext(operation_name, component, self)
    
    def record_kafka_metrics(self, topic: str, partition: str, status: str, duration: float):
        """Kafka metriklerini kaydet"""
        try:
            # Ensure correct labels for topic and status
            if hasattr(self.metrics, 'record_kafka_message_processed') and status == "success":
                self.metrics.record_kafka_message_processed(topic, status)
            elif hasattr(self.metrics, 'record_kafka_message_failed') and status != "success":
                self.metrics.record_kafka_message_failed(topic, status)
            logger.debug(f"Kafka metrics recorded - topic: {topic}, partition: {partition}, status: {status}, duration: {duration:.3f}s")
        except Exception as e:
            logger.error(f"Failed to record Kafka metrics: {str(e)}")
    
    def record_qdrant_metrics(self, operation: str, collection: str, status: str, duration: float, results_count: int = 0):
        """Qdrant metriklerini kaydet"""
        try:
            # Ensure correct signature: operation, collection, status, duration
            if hasattr(self.metrics, 'record_qdrant_operation'):
                self.metrics.record_qdrant_operation(operation, collection, status, duration)
            logger.debug(f"Qdrant metrics recorded - operation: {operation}, collection: {collection}, status: {status}, duration: {duration:.3f}s, results: {results_count}")
        except Exception as e:
            logger.error(f"Failed to record Qdrant metrics: {str(e)}")
    
    def record_embedding_metrics(self, model: str, status: str, duration: float):
        """Embedding metriklerini kaydet"""
        try:
            if hasattr(self.metrics, 'record_embedding_processing') and status == "success":
                self.metrics.record_embedding_processing(duration, model)
            elif hasattr(self.metrics, 'record_processing_error') and status != "success":
                self.metrics.record_processing_error("embedding", status)
            logger.debug(f"Embedding metrics recorded - model: {model}, status: {status}, duration: {duration:.3f}s")
        except Exception as e:
            logger.error(f"Failed to record embedding metrics: {str(e)}")

def metrics_timer(operation_name: str, component: str = "general"):
    """Decorator: Fonksiyon çalışma süresini ölç"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            with collector.time_operation(operation_name, component):
                return func(*args, **kwargs)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            with collector.time_operation(operation_name, component):
                return await func(*args, **kwargs)
        
        # Async fonksiyon mu kontrol et
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:  # CO_COROUTINE
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

def kafka_metrics(topic: str, partition: str = "0"):
    """Decorator: Kafka operasyonları için metrics"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                collector.record_kafka_metrics(topic, partition, "success", duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                collector.record_kafka_metrics(topic, partition, "error", duration)
                raise
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                collector.record_kafka_metrics(topic, partition, "success", duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                collector.record_kafka_metrics(topic, partition, "error", duration)
                raise
        
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

def qdrant_metrics(operation: str, collection: str = "default"):
    """Decorator: Qdrant operasyonları için metrics"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                results_count = len(result) if isinstance(result, (list, tuple)) else 1
                collector.record_qdrant_metrics(operation, collection, "success", duration, results_count)
                return result
            except Exception as e:
                duration = time.time() - start_time
                collector.record_qdrant_metrics(operation, collection, "error", duration, 0)
                raise
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                results_count = len(result) if isinstance(result, (list, tuple)) else 1
                collector.record_qdrant_metrics(operation, collection, "success", duration, results_count)
                return result
            except Exception as e:
                duration = time.time() - start_time
                collector.record_qdrant_metrics(operation, collection, "error", duration, 0)
                raise
        
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

def embedding_metrics(model: str):
    """Decorator: Embedding işlemleri için metrics"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                collector.record_embedding_metrics(model, "success", duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                collector.record_embedding_metrics(model, "error", duration)
                raise
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            collector = MetricsCollector()
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                collector.record_embedding_metrics(model, "success", duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                collector.record_embedding_metrics(model, "error", duration)
                raise
        
        if hasattr(func, '__code__') and func.__code__.co_flags & 0x80:
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

class SystemMetricsCollector:
    """Sistem metriklerini toplayan sınıf"""
    
    def __init__(self, prometheus_metrics=None):
        self.prometheus_metrics = prometheus_metrics or get_metrics()
        
    def collect_cpu_usage(self):
        """CPU kullanımını topla"""
        try:
            import psutil
            return psutil.cpu_percent(interval=1)
        except Exception as e:
            logger.error(f"Failed to collect CPU usage: {str(e)}")
            return 0.0
    
    def collect_memory_usage(self):
        """Memory kullanımını topla"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            return memory.percent
        except Exception as e:
            logger.error(f"Failed to collect memory usage: {str(e)}")
            return 0.0
    
    def collect_disk_usage(self, path='/'):
        """Disk kullanımını topla"""
        try:
            import psutil
            disk = psutil.disk_usage(path)
            return disk.percent
        except Exception as e:
            logger.error(f"Failed to collect disk usage: {str(e)}")
            return 0.0
    
    def collect_all_metrics(self):
        """Tüm sistem metriklerini topla"""
        return {
            'cpu_usage': self.collect_cpu_usage(),
            'memory_usage': self.collect_memory_usage(),
            'disk_usage': self.collect_disk_usage('/')
        }
    
    def update_prometheus_metrics(self):
        """Prometheus metriklerini güncelle"""
        try:
            metrics = self.collect_all_metrics()
            if hasattr(self.prometheus_metrics, 'update_system_metrics'):
                self.prometheus_metrics.update_system_metrics(
                    metrics['cpu_usage'],
                    metrics['memory_usage'],
                    metrics['disk_usage']
                )
            logger.debug(f"System metrics updated - CPU: {metrics['cpu_usage']}%, Memory: {metrics['memory_usage']}%, Disk: {metrics['disk_usage']}%")
            return metrics
        except Exception as e:
            logger.error(f"Failed to update prometheus metrics: {str(e)}")
            if hasattr(self.prometheus_metrics, 'record_processing_error'):
                self.prometheus_metrics.record_processing_error("system_metrics", type(e).__name__)
            return None
        
    def collect_system_metrics(self):
        """Sistem metriklerini topla ve güncelle (backward compatibility)"""
        return self.update_prometheus_metrics()

class HealthMetricsCollector:
    """Sağlık kontrolü metriklerini toplayan sınıf"""
    
    def __init__(self, prometheus_metrics=None):
        self.prometheus_metrics = prometheus_metrics or get_metrics()
        
    def record_health_check_result(self, service: str, status, duration: float, details: Optional[Dict] = None):
        """Sağlık kontrolü sonucunu kaydet"""
        # Handle both boolean and HealthStatus enum
        if hasattr(status, 'value'):
            # HealthStatus enum
            healthy = status.value.lower() == 'healthy'
            status_str = status.value.lower()
        else:
            # Boolean
            healthy = bool(status)
            status_str = "healthy" if healthy else "unhealthy"
        
        if hasattr(self.prometheus_metrics, 'record_health_check'):
            self.prometheus_metrics.record_health_check(service, healthy, duration)
        
        # Bağlantı durumlarını güncelle
        self.update_connection_status(service, healthy)
        
        # Detayları logla
        logger.info(f"Health check - {service}: {status_str} ({duration:.3f}s)")
        
        if details:
            logger.debug(f"Health check details for {service}: {details}")
    
    def update_connection_status(self, service: str, is_connected: bool):
        """Bağlantı durumunu güncelle"""
        try:
            if service == "kafka":
                if hasattr(self.prometheus_metrics, 'set_kafka_connection_status'):
                    self.prometheus_metrics.set_kafka_connection_status(is_connected)
            elif service == "qdrant":
                if hasattr(self.prometheus_metrics, 'set_qdrant_connection_status'):
                    self.prometheus_metrics.set_qdrant_connection_status(is_connected)
            
            logger.debug(f"Connection status updated for {service}: {is_connected}")
        except Exception as e:
            logger.error(f"Failed to update connection status for {service}: {str(e)}")

# Global collectors
_system_collector = None
_health_collector = None

def get_system_collector() -> SystemMetricsCollector:
    """Global sistem metrics collector'ını döndür"""
    global _system_collector
    if _system_collector is None:
        _system_collector = SystemMetricsCollector()
    return _system_collector

def get_health_collector() -> HealthMetricsCollector:
    """Global health metrics collector'ını döndür"""
    global _health_collector
    if _health_collector is None:
        _health_collector = HealthMetricsCollector()
    return _health_collector

# Convenience functions
def record_kafka_message(topic: str, partition: int = 0):
    """Kafka mesaj tüketimi kaydı"""
    metrics = get_metrics()
    metrics.record_kafka_message_consumed(topic, partition)

def record_kafka_lag(topic: str, partition: int, lag: int):
    """Kafka consumer lag kaydı"""
    metrics = get_metrics()
    metrics.set_kafka_consumer_lag(topic, partition, lag)

def record_qdrant_collection_size(collection: str, size: int):
    """Qdrant koleksiyon boyutu kaydı"""
    metrics = get_metrics()
    metrics.set_qdrant_collection_size(collection, size)

def record_error(component: str, error_type: str):
    """Genel hata kaydı"""
    metrics = get_metrics()
    metrics.record_processing_error(component, error_type)

if __name__ == "__main__":
    # Test için
    import asyncio
    import random
    
    @metrics_timer("test_operation", "general")
    def test_sync_function():
        time.sleep(random.uniform(0.1, 0.5))
        if random.random() < 0.1:  # %10 hata
            raise ValueError("Test error")
        return "success"
    
    @kafka_metrics("test-topic")
    async def test_kafka_operation():
        await asyncio.sleep(random.uniform(0.05, 0.2))
        if random.random() < 0.05:  # %5 hata
            raise ConnectionError("Kafka connection failed")
        return "message processed"
    
    @qdrant_metrics("write")
    async def test_qdrant_operation():
        await asyncio.sleep(random.uniform(0.01, 0.1))
        if random.random() < 0.03:  # %3 hata
            raise TimeoutError("Qdrant timeout")
        return "vector written"
    
    async def test_metrics():
        print("Testing metrics collection...")
        
        # System metrics
        system_collector = get_system_collector()
        system_metrics = system_collector.collect_system_metrics()
        print(f"System metrics: {system_metrics}")
        
        # Test operations
        for i in range(10):
            try:
                result = test_sync_function()
                print(f"Sync operation {i}: {result}")
            except Exception as e:
                print(f"Sync operation {i} failed: {e}")
            
            try:
                result = await test_kafka_operation()
                print(f"Kafka operation {i}: {result}")
            except Exception as e:
                print(f"Kafka operation {i} failed: {e}")
            
            try:
                result = await test_qdrant_operation()
                print(f"Qdrant operation {i}: {result}")
            except Exception as e:
                print(f"Qdrant operation {i} failed: {e}")
            
            await asyncio.sleep(0.1)
        
        # Metrics çıktısını göster
        metrics = get_metrics()
        print("\nMetrics output:")
        print(metrics.get_metrics())
    
    asyncio.run(test_metrics())