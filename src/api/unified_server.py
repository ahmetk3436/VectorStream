import asyncio
import gc
import platform
import psutil
import threading
import time
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from loguru import logger
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import uvicorn

from src.monitoring.prometheus_metrics import PrometheusMetrics
from src.monitoring.health_monitor import HealthMonitor
from src.utils.metrics import SystemMetricsCollector


class UnifiedServer:
    """
    Tüm servisleri tek FastAPI uygulamasında birleştirir:

    • /health (detaylı), /health/simple, /health/live, /health/ready
    • /metrics  (Prometheus)
    • /system   (sistem donanım + uptime bilgisi)
    • /system/gc (manuel garbage-collection tetikleyici)
    • /debug/config (hızlı debug konfigürasyonu)
    """

    def __init__(self, metrics: PrometheusMetrics, health_monitor: HealthMonitor):
        self.metrics = metrics
        self.health_monitor = health_monitor
        self.system_metrics_collector = SystemMetricsCollector(self.metrics)

        self.app = FastAPI(
            title="NewMind AI – Unified API Server",
            description=(
                "Health checks, Prometheus metrics ve yönetim endpoint’lerini birleştirir"
            ),
            version="1.0.0",
            docs_url="/docs",
            redoc_url="/redoc",
        )

        self.stop_metrics_update = threading.Event()
        self.metrics_update_thread: Optional[threading.Thread] = None

        self._setup_middleware()
        self._setup_routes()

    def _setup_middleware(self):
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def _setup_routes(self):
        """Tüm API rotalarını kaydeder"""

        @self.app.get("/")
        async def root():
            return {
                "service": "NewMind AI – Unified Server",
                "version": "1.0.0",
                "status": "running",
                "endpoints": {
                    "health": "/health",
                    "health_simple": "/health/simple",
                    "liveness": "/health/live",
                    "readiness": "/health/ready",
                    "metrics": "/metrics",
                    "system": "/system",
                    "docs": "/docs",
                },
            }

        @self.app.get("/health")
        async def health_check():
            try:
                health_checks = await self.health_monitor.run_all_checks()
                overall_status = await self.health_monitor.get_overall_status()

                services: Dict[str, Dict[str, Any]] = {}
                for check in health_checks:
                    info: Dict[str, Any] = {
                        "status": check.status.value,
                        "response_time_ms": check.response_time_ms,
                        "message": check.message,
                    }
                    if check.service == "system" and check.details:
                        info["details"] = check.details
                    services[check.service] = info

                return {
                    "status": overall_status.value,
                    "timestamp": time.time(),
                    "uptime_seconds": time.time() - getattr(
                        self.metrics, "_start_time", time.time()
                    ),
                    "services": services,
                }
            except Exception as exc:
                logger.error(f"Health check error: {exc}")
                raise HTTPException(status_code=500, detail=f"Health check failed: {exc}")

        @self.app.get("/health/simple")
        async def simple_health():
            try:
                health_checks = await self.health_monitor.run_all_checks()
                if any(ch.status.value != "healthy" for ch in health_checks):
                    raise HTTPException(status_code=503, detail="Service unhealthy")
                return {"status": "ok"}
            except HTTPException:
                raise
            except Exception as exc:
                logger.error(f"Simple health check error: {exc}")
                raise HTTPException(status_code=500, detail="Health check failed")

        @self.app.get("/health/live")
        async def liveness_probe():
            return {"status": "alive"}

        @self.app.get("/health/ready")
        async def readiness_probe():
            try:
                health_checks = await self.health_monitor.run_all_checks()
                if any(ch.status.value != "healthy" for ch in health_checks):
                    raise HTTPException(status_code=503, detail="Service not ready")
                return {"status": "ready"}
            except HTTPException:
                raise
            except Exception as exc:
                logger.error(f"Readiness check error: {exc}")
                raise HTTPException(status_code=500, detail="Readiness check failed")

        @self.app.get("/metrics")
        async def prometheus_metrics():
            """
            Prometheus metrics endpoint (özel registry ile)
            """
            try:
                self.system_metrics_collector.update_prometheus_metrics()
                self.metrics.update_uptime()

                metrics_data = generate_latest(self.metrics.registry)
                return Response(content=metrics_data, media_type=CONTENT_TYPE_LATEST)
            except Exception as exc:
                logger.error(f"/metrics endpoint error: {exc}")
                raise HTTPException(
                    status_code=500, detail=f"Metrics generation failed: {exc}"
                )

        @self.app.get("/system")
        async def system_info():
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage("/")

                return {
                    "system": {
                        "platform": platform.platform(),
                        "python_version": platform.python_version(),
                        "cpu_cores": psutil.cpu_count(),
                        "cpu_usage_percent": cpu_percent,
                        "memory": {
                            "total_gb": round(memory.total / 1024**3, 2),
                            "available_gb": round(memory.available / 1024**3, 2),
                            "used_percent": memory.percent,
                        },
                        "disk": {
                            "total_gb": round(disk.total / 1024**3, 2),
                            "free_gb": round(disk.free / 1024**3, 2),
                            "used_percent": round((disk.used / disk.total) * 100, 2),
                        },
                    },
                    "application": {
                        "uptime_seconds": self.metrics.get_uptime(),
                        "metrics_enabled": True,
                        "health_checks_enabled": True,
                    },
                }
            except Exception as exc:
                logger.error(f"System info error: {exc}")
                raise HTTPException(status_code=500, detail=f"System info failed: {exc}")

        @self.app.post("/system/gc")
        async def trigger_garbage_collection():
            try:
                collected = gc.collect()
                return {
                    "status": "completed",
                    "objects_collected": collected,
                    "timestamp": time.time(),
                }
            except Exception as exc:
                logger.error(f"GC trigger error: {exc}")
                raise HTTPException(status_code=500, detail=f"GC failed: {exc}")

        @self.app.get("/debug/config")
        async def debug_config():
            try:
                return {
                    "metrics_port": 9091,
                    "health_enabled": True,
                    "debug_mode": True,
                    "timestamp": time.time(),
                }
            except Exception as exc:
                logger.error(f"Debug config error: {exc}")
                raise HTTPException(status_code=500, detail=f"Debug config failed: {exc}")

    def start_background_metrics_update(self):
        """5 saniyede bir sistem + health metriklerini yeniler"""

        def update_metrics():
            while not self.stop_metrics_update.is_set():
                try:
                    self.system_metrics_collector.update_prometheus_metrics()
                    self.metrics.update_uptime()

                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(self._update_health_metrics())
                    finally:
                        loop.close()

                    time.sleep(5)
                except Exception as exc:
                    logger.error(f"Background metrics update error: {exc}")

        self.metrics_update_thread = threading.Thread(target=update_metrics, daemon=True)
        self.metrics_update_thread.start()
        logger.info("📊 BG metrics update thread started")

    async def _update_health_metrics(self):
        try:
            health_checks = await self.health_monitor.run_all_checks()
            for check in health_checks:
                self.metrics.record_health_check(
                    check.service, check.status.value, check.response_time_ms / 1000.0
                )
                if check.service == "kafka":
                    self.metrics.set_kafka_connection_status(
                        check.status.value == "healthy"
                    )
                elif check.service == "qdrant":
                    self.metrics.set_qdrant_connection_status(
                        check.status.value == "healthy"
                    )
        except Exception as exc:
            logger.error(f"Health metrics update error: {exc}")

    def start_server(self, host: str = "0.0.0.0", port: int = 8080):
        self.start_background_metrics_update()
        logger.info(f"🚀 Unified server listening on {host}:{port}")
        uvicorn.run(self.app, host=host, port=port, log_level="info", access_log=False)

    def stop(self):
        logger.info("🛑 Unified server shutting down…")
        self.stop_metrics_update.set()
        if self.metrics_update_thread and self.metrics_update_thread.is_alive():
            self.metrics_update_thread.join(timeout=5)
            logger.info("📊 BG metrics thread stopped")
