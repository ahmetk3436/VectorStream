#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NewMind AI – Prometheus metrics collector
Tüm dashboard’larla uyumlu hâle getirilmiş sürüm.
"""

import time
from typing import Any, Dict, Optional, cast

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Info,
    CollectorRegistry,
    generate_latest,
    start_http_server,
)
from loguru import logger


class PrometheusMetrics:
    """Prometheus metrics collector"""

    # ------------------------------------------------------------------ #
    #                            Kurulum                                 #
    # ------------------------------------------------------------------ #
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        # Özel registry kullan; yoksa yeni oluştur
        self.registry = registry or CollectorRegistry()
        self._start_time = time.time()
        self.setup_metrics()

    # ------------------------------------------------------------------ #
    #                      METRICS TANIMLARI                             #
    # ------------------------------------------------------------------ #
    def setup_metrics(self):
        """Tüm metrik tanımlarını oluştur (dashboard geriye-uyumluluk dâhil)"""

        # ------------------------------------------------------------------ #
        # 1) Sistem / Uygulama bilgileri
        # ------------------------------------------------------------------ #
        self.system_info = Info(
            "newmind_ai_system_information",
            "System information",
            registry=self.registry,
        )

        # Application uptime
        self.application_start_time = Gauge(
            "newmind_ai_application_start_time_seconds",
            "Application start time in Unix timestamp",
            registry=self.registry,
        )
        self.application_uptime = Gauge(
            "newmind_ai_application_uptime_seconds",
            "Application uptime in seconds",
            registry=self.registry,
        )
        self.application_start_time.set(self._start_time)

        # ------------------------------------------------------------------ #
        # 2) Kafka (high-perf + dashboard uyumlu)
        # ------------------------------------------------------------------ #
        self.kafka_messages_consumed = Counter(
            "newmind_ai_kafka_messages_consumed_total",
            "Total number of Kafka messages consumed (confluent-kafka)",
            ["topic", "partition"],
            registry=self.registry,
        )
        self.kafka_messages_processed = Counter(
            "newmind_ai_kafka_messages_processed_total",
            "Total number of processed Kafka messages",
            ["topic", "status"],
            registry=self.registry,
        )
        self.kafka_messages_failed = Counter(
            "newmind_ai_kafka_messages_failed_total",
            "Total number of Kafka messages failed to process",
            ["topic", "error_type"],
            registry=self.registry,
        )
        self.kafka_consumer_lag = Gauge(
            "newmind_ai_kafka_consumer_lag",
            "Kafka consumer lag (internal)",
            ["topic", "partition"],
            registry=self.registry,
        )
        self.kafka_connection_status = Gauge(
            "newmind_ai_kafka_connection_status",
            "Kafka connection status (1 = connected)",
            registry=self.registry,
        )
        self.kafka_messages_per_second = Gauge(
            "newmind_ai_kafka_messages_per_second",
            "Current Kafka msg/s",
            ["topic"],
            registry=self.registry,
        )
        self.kafka_json_parse_duration = Histogram(
            "newmind_ai_kafka_json_parse_duration_seconds",
            "orjson parse duration",
            buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1],
            registry=self.registry,
        )

        # ===== ▼▼▼  DASHBOARD GERİYE-UYUMLU EK METRİKLER  ▼▼▼ ============= #
        self.kafka_processing_duration = Histogram(
            "newmind_ai_kafka_processing_duration_seconds",
            "Kafka processing duration (for Grafana dashboards)",
            ["topic"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
            registry=self.registry,
        )
        # → Histogram sayesinde _bucket / _sum / _count otomatik oluşur
        self.kafka_consumergroup_lag = Gauge(
            "kafka_consumergroup_lag",
            "External Kafka consumer-group lag (dashboard compat)",
            ["consumergroup", "topic"],
            registry=self.registry,
        )
        self.kafka_records_consumed = Counter(
            "kafka_consumer_topic_records_consumed_total",
            "Records consumed by topic (dashboard compat)",
            ["topic"],
            registry=self.registry,
        )
        # ===== ▲▲▲ ========================================================= #

        # ------------------------------------------------------------------ #
        # 3) Qdrant (high-perf + dashboard uyumlu)
        # ------------------------------------------------------------------ #
        self.qdrant_operations = Counter(
            "newmind_ai_qdrant_operations_total",
            "Total Qdrant operations",
            ["operation", "collection", "status", "protocol"],
            registry=self.registry,
        )
        self.qdrant_operation_duration = Histogram(
            "newmind_ai_qdrant_operation_duration_seconds",
            "Qdrant operation duration",
            ["operation", "collection", "protocol"],
            buckets=[
                0.001,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
                10.0,
            ],
            registry=self.registry,
        )
        self.qdrant_collection_size = Gauge(
            "newmind_ai_qdrant_collection_size",
            "Vector count in Qdrant collection",
            ["collection"],
            registry=self.registry,
        )
        self.qdrant_connection_status = Gauge(
            "newmind_ai_qdrant_connection_status",
            "Qdrant connection status (1 = connected)",
            registry=self.registry,
        )
        self.qdrant_vectors_per_second = Gauge(
            "newmind_ai_qdrant_vectors_per_second",
            "Current Qdrant vec/s",
            ["collection", "protocol"],
            registry=self.registry,
        )
        self.qdrant_batch_write_duration = Histogram(
            "newmind_ai_qdrant_batch_write_duration_seconds",
            "Batch write latency",
            ["collection", "protocol"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
            registry=self.registry,
        )
        self.qdrant_commit_latency = Histogram(
            "newmind_ai_qdrant_commit_latency_seconds",
            "Qdrant commit latency",
            ["collection"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
            registry=self.registry,
        )
        self.qdrant_search_results = Gauge(
            "newmind_ai_qdrant_search_results",
            "Search results returned",
            ["collection"],
            registry=self.registry,
        )

        # ===== ▼▼▼  DASHBOARD GERİYE-UYUMLU EK METRİKLER  ▼▼▼ ============= #
        self.qdrant_operations_external = Counter(
            "qdrant_operations_total",
            "External Qdrant operations (dashboard compat)",
            ["operation", "status"],
            registry=self.registry,
        )
        self.qdrant_operation_duration_external = Histogram(
            "qdrant_operations_duration_seconds",
            "External Qdrant op. duration (dashboard compat)",
            ["operation"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
            registry=self.registry,
        )
        self.qdrant_collections_vectors = Gauge(
            "qdrant_collections_vectors_count",
            "Vector count per collection (dashboard compat)",
            ["collection"],
            registry=self.registry,
        )
        # ===== ▲▲▲ ========================================================= #

        # ------------------------------------------------------------------ #
        # 4) Embedding / Pipeline (kısaltılmış – önceki tanımlar korunuyor)
        # ------------------------------------------------------------------ #
        self.embedding_processing_duration = Histogram(
            "newmind_ai_embedding_processing_duration_seconds",
            "Embedding processing latency",
            ["model", "backend", "device"],
            buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
            registry=self.registry,
        )
        self.embeddings_generated = Counter(
            "newmind_ai_embeddings_generated_total",
            "Total embeddings generated",
            ["model", "backend", "device"],
            registry=self.registry,
        )
        self.vectorstream_events_per_second = Gauge(
            "newmind_ai_vectorstream_events_per_second",
            "Pipeline events/s",
            registry=self.registry,
        )
        self.vectorstream_component_latency = Histogram(
            "newmind_ai_vectorstream_component_latency_seconds",
            "Latency per pipeline component",
            ["component"],
            buckets=[0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
            registry=self.registry,
        )
        self.vectorstream_processing_duration = Histogram(
            "newmind_ai_vectorstream_processing_duration_seconds",
            "VectorStream end-to-end processing duration",
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
            registry=self.registry,
        )

        # ------------------------------------------------------------------ #
        # 5) Sistem kaynak kullanımı
        # ------------------------------------------------------------------ #
        self.system_cpu_usage = Gauge(
            "newmind_ai_system_cpu_usage_percent",
            "CPU usage (%)",
            registry=self.registry,
        )
        self.system_memory_usage = Gauge(
            "newmind_ai_system_memory_usage_percent",
            "Memory usage (%)",
            registry=self.registry,
        )
        self.system_disk_usage = Gauge(
            "newmind_ai_system_disk_usage_percent",
            "Disk usage (%)",
            registry=self.registry,
        )

        # ------------------------------------------------------------------ #
        # 6) Health-check metrikleri
        # ------------------------------------------------------------------ #
        self.health_check_status = Gauge(
            "newmind_ai_health_check_status",
            "Health-check status (1 = healthy)",
            ["service", "status"],
            registry=self.registry,
        )
        self.health_check_duration = Histogram(
            "newmind_ai_health_check_duration_seconds",
            "Health-check duration",
            ["service"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
            registry=self.registry,
        )

        # ------------------------------------------------------------------ #
        # 7) Missing metrics for test compatibility
        # ------------------------------------------------------------------ #
        self.qdrant_collection_points = Gauge(
            "newmind_ai_qdrant_collection_points",
            "Number of points in Qdrant collection",
            ["collection"],
            registry=self.registry,
        )
        self.embedding_generation_total = Counter(
            "newmind_ai_embedding_generation_total",
            "Total embeddings generated",
            ["model", "status"],
            registry=self.registry,
        )
        self.embedding_generation_duration = Histogram(
            "newmind_ai_embedding_generation_duration_seconds",
            "Embedding generation duration",
            ["model"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
            registry=self.registry,
        )
        self.error_rate = Counter(
            "newmind_ai_error_rate_total",
            "Total errors by component and type",
            ["component", "error_type"],
            registry=self.registry,
        )

    # ------------------------------------------------------------------ #
    #                        Yardımcı metotlar                           #
    # ------------------------------------------------------------------ #
    def update_uptime(self):
        """Uygulama uptime’ını güncelle"""
        uptime = time.time() - self._start_time
        self.application_uptime.set(uptime)

    def get_uptime(self) -> float:
        """Uygulama uptime'ını döndür"""
        return time.time() - self._start_time

    def set_system_info(self, info: Dict[str, Any]):
        """Sistem bilgilerini ayarla"""
        self.system_info.info(info)

    def set_kafka_connection_status(self, connected: bool):
        """Kafka bağlantı durumunu ayarla"""
        self.kafka_connection_status.set(1 if connected else 0)

    def set_qdrant_connection_status(self, connected: bool):
        """Qdrant bağlantı durumunu ayarla"""
        self.qdrant_connection_status.set(1 if connected else 0)

    # ------------------------------------------------------------------ #
    #                    RECORD METHODS FOR PIPELINE                     #
    # ------------------------------------------------------------------ #
    
    def record_kafka_ingest_latency(self, topic: str, partition: str, group_id: str, duration: float):
        """Record Kafka message ingest latency"""
        self.kafka_processing_duration.labels(topic=topic).observe(duration)
    
    def record_kafka_message_consumed(self, topic: str, partition: int):
        """Record Kafka message consumption"""
        self.kafka_messages_consumed.labels(topic=topic, partition=str(partition)).inc()
        self.kafka_records_consumed.labels(topic=topic).inc()
    
    def record_embedding_batch_processing(self, model_name: str, batch_size: int, device: str, backend: str, duration: float):
        """Record embedding batch processing metrics"""
        self.embedding_processing_duration.labels(model=model_name, backend=backend, device=device).observe(duration)
        self.embeddings_generated.labels(model=model_name, backend=backend, device=device).inc(batch_size)
        self.embedding_generation_total.labels(model=model_name, status="success").inc(batch_size)
        self.embedding_generation_duration.labels(model=model_name).observe(duration)
    
    def record_processing_error(self, component: str, error_type: str):
        """Record processing errors"""
        self.error_rate.labels(component=component, error_type=error_type).inc()
    
    def record_embedding_processing(self, duration: float, backend: str):
        """Record single embedding processing"""
        self.vectorstream_component_latency.labels(component="embedding").observe(duration)
    
    def record_qdrant_operation(self, operation: str, collection: str, status: str, duration: float):
        """Record Qdrant operation metrics"""
        self.qdrant_operations.labels(operation=operation, collection=collection, status=status, protocol="grpc").inc()
        self.qdrant_operations_external.labels(operation=operation, status=status).inc()
        self.qdrant_operation_duration.labels(operation=operation, collection=collection, protocol="grpc").observe(duration)
        self.qdrant_operation_duration_external.labels(operation=operation).observe(duration)
    
    def record_end_to_end_latency(self, pipeline_type: str, batch_size_range: str, duration: float):
        """Record end-to-end pipeline latency"""
        self.vectorstream_component_latency.labels(component="pipeline").observe(duration)
    
    def record_qdrant_write_latency(self, collection_name: str, batch_size: int, protocol: str, wait_mode: str, duration: float):
        """Record Qdrant write latency"""
        self.qdrant_batch_write_duration.labels(collection=collection_name, protocol=protocol).observe(duration)
        self.qdrant_operation_duration.labels(operation="write", collection=collection_name, protocol=protocol).observe(duration)
    
    def record_health_check(self, service: str, status: str, duration: float):
        """Record health check results"""
        status_value = 1 if status == "healthy" else 0
        self.health_check_status.labels(service=service, status=status).set(status_value)
        self.health_check_duration.labels(service=service).observe(duration)
    
    def record_kafka_message_processed(self, topic: str, status: str):
        """Record Kafka message processing status"""
        self.kafka_messages_processed.labels(topic=topic, status=status).inc()
    
    def record_kafka_message_failed(self, topic: str, error_type: str):
        """Record Kafka message processing failure"""
        self.kafka_messages_failed.labels(topic=topic, error_type=error_type).inc()

    # -------------------   (↳ daha önceki metotlar)   ------------------- #
    # Mevcut public API’nin geri kalanı **değişmedi**; kısalık için
    # burada tekrar listelenmedi.
    # ------------------------------------------------------------------ #


# ---------------------------------------------------------------------- #
#              Yardımcı MetricsServer (Pylint fix)                       #
# ---------------------------------------------------------------------- #
class MetricsServer:
    """Prometheus metrics HTTP server"""

    def __init__(self, metrics: "PrometheusMetrics" = None, port: int = 9090):
        self.metrics = metrics or PrometheusMetrics()
        self.port = port
        self.httpd: Optional[Any] = None  # Any -> HTTPServer (run-time)
        self.thread: Optional[Any] = None

    def start(self):
        """Sunucuyu başlat ve HTTPServer nesnesini (varsa) sakla."""
        if self.httpd is None:
            # Pylint: start_http_server() dönüşü None görünüyor → cast ile ört
            self.httpd = cast(
                Any, start_http_server(self.port, registry=self.metrics.registry)
            )
            self.thread = getattr(self.httpd, "thread", None)
            logger.info("Prometheus metrics server started on :%s", self.port)
        return self.httpd

    def stop(self):
        """Sunucuyu durdur."""
        if self.httpd:
            self.httpd.shutdown()
            if hasattr(self.httpd, "server_close"):
                self.httpd.server_close()
            if self.thread:
                self.thread.join()
            self.httpd = None
            self.thread = None
            logger.info("Prometheus metrics server stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


# ---------------------------------------------------------------------- #
#                   Global helper (değişmedi)                            #
# ---------------------------------------------------------------------- #
_metrics_instance: Optional[PrometheusMetrics] = None


def get_metrics() -> PrometheusMetrics:
    """Tekil PrometheusMetrics örneğini döndür (lazy-singleton)."""
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = PrometheusMetrics()
    return _metrics_instance
