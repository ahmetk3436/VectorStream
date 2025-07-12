#!/usr/bin/env python3

import time
from typing import Dict, Any, Optional
from prometheus_client import (
    Counter, Gauge, Histogram, Summary, Info,
    CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST,
    start_http_server
)
from prometheus_client.exposition import MetricsHandler
from http.server import HTTPServer
import threading
from loguru import logger

class PrometheusMetrics:
    """Prometheus metrics collector"""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        self.setup_metrics()
        
    def setup_metrics(self):
        """Metrics tanımlarını oluştur"""
        
        # System Info
        self.system_info = Info(
            'newmind_ai_system_information',
            'System information',
            registry=self.registry
        )
        
        # Kafka Metrics
        self.kafka_messages_consumed = Counter(
            'newmind_ai_kafka_messages_consumed_total',
            'Total number of Kafka messages consumed',
            ['topic', 'partition'],
            registry=self.registry
        )
        
        self.kafka_messages_processed = Counter(
            'newmind_ai_kafka_messages_processed_total',
            'Total number of processed Kafka messages',
            ['topic', 'status'],
            registry=self.registry
        )
        
        self.kafka_messages_failed = Counter(
            'newmind_ai_kafka_messages_failed_total',
            'Total number of Kafka messages failed to process',
            ['topic', 'error_type'],
            registry=self.registry
        )
        
        self.kafka_consumer_lag = Gauge(
            'newmind_ai_kafka_consumer_lag',
            'Kafka consumer lag',
            ['topic', 'partition'],
            registry=self.registry
        )
        
        self.kafka_connection_status = Gauge(
            'newmind_ai_kafka_connection_status',
            'Kafka connection status (1=connected, 0=disconnected)',
            registry=self.registry
        )
        
        # Qdrant Metrics
        self.qdrant_operations = Counter(
            'newmind_ai_qdrant_operations_total',
            'Total number of Qdrant operations',
            ['operation', 'collection', 'status'],
            registry=self.registry
        )
        
        self.qdrant_operation_duration = Histogram(
            'newmind_ai_qdrant_operation_duration_seconds',
            'Duration of Qdrant operations',
            ['operation', 'collection'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        self.qdrant_collection_size = Gauge(
            'newmind_ai_qdrant_collection_size',
            'Number of vectors in Qdrant collection',
            ['collection'],
            registry=self.registry
        )
        
        self.qdrant_connection_status = Gauge(
            'newmind_ai_qdrant_connection_status',
            'Qdrant connection status (1=connected, 0=disconnected)',
            registry=self.registry
        )
        
        # Processing Metrics
        self.embedding_processing_duration = Histogram(
            'newmind_ai_embedding_processing_duration_seconds',
            'Duration of embedding processing',
            buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        self.embeddings_generated = Counter(
            'newmind_ai_embeddings_generated_total',
            'Total number of embeddings generated',
            ['model'],
            registry=self.registry
        )
        
        self.processing_errors = Counter(
            'newmind_ai_processing_errors_total',
            'Total number of processing errors',
            ['component', 'error_type'],
            registry=self.registry
        )
        
        # Error rate metrics
        self.error_rate = Counter(
            'newmind_ai_error_rate_total',
            'Total error rate by component and type',
            ['component', 'error_type'],
            registry=self.registry
        )
        
        # Additional Qdrant metrics
        self.qdrant_collection_points = Gauge(
            'newmind_ai_qdrant_collection_points',
            'Number of points in Qdrant collection',
            ['collection'],
            registry=self.registry
        )
        
        self.qdrant_search_results = Gauge(
            'newmind_ai_qdrant_search_results',
            'Number of search results returned',
            ['collection'],
            registry=self.registry
        )
        
        # Additional embedding metrics
        self.embedding_generation_duration = Histogram(
            'newmind_ai_embedding_generation_duration_seconds',
            'Duration of embedding generation',
            ['model'],
            buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
            registry=self.registry
        )
        
        self.embedding_generation_total = Counter(
            'newmind_ai_embedding_generation_total',
            'Total number of embeddings generated',
            ['model', 'status'],
            registry=self.registry
        )
        
        # System Metrics
        self.system_cpu_usage = Gauge(
            'newmind_ai_system_cpu_usage_percent',
            'System CPU usage percentage',
            registry=self.registry
        )
        
        self.system_memory_usage = Gauge(
            'newmind_ai_system_memory_usage_percent',
            'System memory usage percentage',
            registry=self.registry
        )
        
        self.system_disk_usage = Gauge(
            'newmind_ai_system_disk_usage_percent',
            'System disk usage percentage',
            registry=self.registry
        )
        
        # Health Check Metrics
        self.health_check_status = Gauge(
            'newmind_ai_health_check_status',
            'Health check status (1=healthy, 0=unhealthy)',
            ['service', 'status'],
            registry=self.registry
        )
        
        self.health_check_duration = Histogram(
            'newmind_ai_health_check_duration_seconds',
            'Duration of health checks',
            ['service'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
            registry=self.registry
        )
        
        # Application Metrics
        self.application_start_time = Gauge(
            'newmind_ai_application_start_time_seconds',
            'Application start time in Unix timestamp',
            registry=self.registry
        )
        
        self.application_uptime = Gauge(
            'newmind_ai_application_uptime_seconds',
            'Application uptime in seconds',
            registry=self.registry
        )
        
        # Set application start time
        self.application_start_time.set(time.time())
        
    def record_kafka_message_consumed(self, topic: str, partition: int):
        """Kafka mesaj tüketimi kaydı"""
        self.kafka_messages_consumed.labels(topic=topic, partition=partition).inc()
        
    def record_kafka_message_processed(self, topic: str, status: str = "success"):
        """Kafka mesaj işleme başarısı kaydı"""
        self.kafka_messages_processed.labels(topic=topic, status=status).inc()
        
    def record_kafka_message_failed(self, topic: str, error_type: str):
        """Kafka mesaj işleme hatası kaydı"""
        self.kafka_messages_failed.labels(topic=topic, error_type=error_type).inc()
        
    def set_kafka_consumer_lag(self, topic: str, partition: int, lag: int):
        """Kafka consumer lag güncelleme"""
        self.kafka_consumer_lag.labels(topic=topic, partition=partition).set(lag)
        
    def set_kafka_connection_status(self, connected: bool):
        """Kafka bağlantı durumu güncelleme"""
        self.kafka_connection_status.set(1 if connected else 0)
        
    def record_qdrant_operation(self, operation: str, collection: str, status: str, duration: float):
        """Qdrant operasyon kaydı"""
        self.qdrant_operations.labels(operation=operation, collection=collection, status=status).inc()
        self.qdrant_operation_duration.labels(operation=operation, collection=collection).observe(duration)
        
    def set_qdrant_collection_size(self, collection: str, size: int):
        """Qdrant koleksiyon boyutu güncelleme"""
        self.qdrant_collection_size.labels(collection=collection).set(size)
        
    def set_qdrant_connection_status(self, connected: bool):
        """Qdrant bağlantı durumu güncelleme"""
        self.qdrant_connection_status.set(1 if connected else 0)
        
    def record_embedding_processing(self, duration: float, model: str):
        """Embedding işleme kaydı"""
        self.embedding_processing_duration.observe(duration)
        self.embeddings_generated.labels(model=model).inc()
        
    def record_processing_error(self, component: str, error_type: str):
        """İşleme hatası kaydı"""
        self.processing_errors.labels(component=component, error_type=error_type).inc()
        
    def update_system_metrics(self, cpu_percent: float, memory_percent: float, disk_percent: float):
        """Sistem metriklerini güncelle"""
        self.system_cpu_usage.set(cpu_percent)
        self.system_memory_usage.set(memory_percent)
        self.system_disk_usage.set(disk_percent)
        
    def record_health_check(self, service: str, status: str, duration: float):
        """Sağlık kontrolü kaydı"""
        self.health_check_status.labels(service=service, status=status).set(1)
        self.health_check_duration.labels(service=service).observe(duration)
        
    def update_uptime(self):
        """Uygulama uptime güncelleme"""
        start_time = self.application_start_time._value._value
        uptime = time.time() - start_time
        self.application_uptime.set(uptime)
        
    def update_application_uptime(self):
        """Uygulama uptime güncelleme (alias)"""
        self.update_uptime()
        
    def record_error(self, component: str, error_type: str):
        """Hata kaydı (alias)"""
        self.record_processing_error(component, error_type)
        self.error_rate.labels(component=component, error_type=error_type).inc()
        

        
    def update_kafka_consumer_lag(self, topic: str, partition: str, lag: int):
        """Kafka consumer lag güncelleme (string partition ile)"""
        self.kafka_consumer_lag.labels(topic=topic, partition=partition).set(lag)
        
    def update_kafka_connection_status(self, connected: bool):
        """Kafka bağlantı durumu güncelleme (alias)"""
        self.set_kafka_connection_status(connected)
        

        
    def update_qdrant_collection_points(self, collection: str, points: int):
        """Qdrant koleksiyon nokta sayısı güncelleme"""
        self.qdrant_collection_points.labels(collection=collection).set(points)
        
    def record_qdrant_search_results(self, collection: str, results_count: int):
        """Qdrant arama sonuç sayısı kaydı"""
        self.qdrant_search_results.labels(collection=collection).set(results_count)
        
    def update_qdrant_connection_status(self, connected: bool):
        """Qdrant bağlantı durumu güncelleme (alias)"""
        self.set_qdrant_connection_status(connected)
        
    def record_embedding_generation(self, model: str, status: str, duration: float):
        """Embedding üretim kaydı"""
        self.embedding_generation_total.labels(model=model, status=status).inc()
        self.embedding_generation_duration.labels(model=model).observe(duration)
        
    def update_system_cpu_usage(self, cpu_percent: float):
        """CPU kullanımı güncelleme"""
        self.system_cpu_usage.set(cpu_percent)
        
    def update_system_memory_usage(self, memory_percent: float):
        """Bellek kullanımı güncelleme"""
        self.system_memory_usage.set(memory_percent)
        
    def update_system_disk_usage(self, disk_percent: float):
        """Disk kullanımı güncelleme"""
        self.system_disk_usage.set(disk_percent)
        

        
    def update_application_uptime(self, uptime: float):
        """Uygulama uptime güncelleme (parametre ile)"""
        self.application_uptime.set(uptime)
        
    def set_system_info(self, info: Dict[str, str]):
        """Sistem bilgilerini ayarla"""
        self.system_info.info(info)
        
    def get_metrics(self) -> str:
        """Prometheus formatında metrikleri döndür"""
        self.update_uptime()
        return generate_latest(self.registry).decode('utf-8')

class MetricsServer:
    """Prometheus metrics HTTP server"""
    
    def __init__(self, metrics: PrometheusMetrics = None, port: int = 9090, registry=None):
        self.metrics = metrics or PrometheusMetrics(registry=registry)
        self.port = port
        self.registry = registry
        self.server = None
        self.thread = None
        self.httpd = None
        
    def start(self):
        """Metrics server'ını başlat"""
        if self.httpd is None:
            self.httpd = start_http_server(self.port, registry=self.registry)
            logger.info(f"Starting Prometheus metrics server on port {self.port}")
        return self.httpd
        
    def stop(self):
        """Metrics server'ını durdur"""
        if self.httpd:
            self.httpd.shutdown()
            self.httpd = None
            logger.info("Prometheus metrics server stopped")
            
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()

# Global metrics instance
_metrics_instance = None

def get_metrics() -> PrometheusMetrics:
    """Global metrics instance'ını döndür"""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = PrometheusMetrics()
    return _metrics_instance

def init_metrics(system_info: Dict[str, str] = None) -> PrometheusMetrics:
    """Metrics sistemini başlat"""
    global _metrics_instance
    _metrics_instance = PrometheusMetrics()
    
    if system_info:
        _metrics_instance.set_system_info(system_info)
    
    logger.info("Prometheus metrics initialized")
    return _metrics_instance

if __name__ == "__main__":
    # Test için
    import asyncio
    import random
    
    async def test_metrics():
        metrics = init_metrics({
            'version': '1.0.0',
            'environment': 'development',
            'component': 'newmind-ai'
        })
        
        # Metrics server'ını başlat
        server = MetricsServer(metrics, port=9090)
        server.start()
        
        # Test verileri üret
        while True:
            # Kafka metrics
            metrics.record_kafka_message_consumed('test-topic', 0)
            if random.random() > 0.1:  # %90 başarı
                metrics.record_kafka_message_processed('test-topic')
            else:
                metrics.record_kafka_message_failed('test-topic', 'processing_error')
            
            # Qdrant metrics
            duration = random.uniform(0.01, 0.5)
            status = 'success' if random.random() > 0.05 else 'error'
            metrics.record_qdrant_operation('write', status, duration)
            
            # System metrics
            metrics.update_system_metrics(
                cpu_percent=random.uniform(10, 80),
                memory_percent=random.uniform(20, 70),
                disk_percent=random.uniform(30, 60)
            )
            
            await asyncio.sleep(1)
    
    asyncio.run(test_metrics())