#!/usr/bin/env python3

import asyncio
import gc
import platform
import psutil
import threading
import time
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from loguru import logger
import uvicorn

from src.monitoring.prometheus_metrics import PrometheusMetrics
from src.monitoring.health_monitor import HealthMonitor
from src.utils.metrics import SystemMetricsCollector


class UnifiedServer:
    """
    Tüm servisleri tek FastAPI uygulaması altında toplayan unified server
    
    Bu sınıf şunları birleştirir:
    - Health Check API
    - Prometheus Metrics API  
    - System Monitoring
    - Future: Management API
    """
    
    def __init__(self, metrics: PrometheusMetrics, health_monitor: HealthMonitor):
        self.app = FastAPI(
            title="NewMind AI - Unified API Server",
            description="Health checks, metrics ve management API'lerini birleştiren tek endpoint",
            version="1.0.0",
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        self.metrics = metrics
        self.health_monitor = health_monitor
        self.system_metrics_collector = SystemMetricsCollector(self.metrics)
        
        # Background tasks
        self.stop_metrics_update = threading.Event()
        self.metrics_update_thread: Optional[threading.Thread] = None
        
        self._setup_middleware()
        self._setup_routes()
        
    def _setup_middleware(self):
        """Middleware'leri ayarla"""
        # CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
    def _setup_routes(self):
        """API route'larını ayarla"""
        
        @self.app.get("/")
        async def root():
            """Ana endpoint - sistem durumu özeti"""
            return {
                "service": "NewMind AI - Unified Server",
                "version": "1.0.0",
                "status": "running",
                "endpoints": {
                    "health": "/health",
                    "metrics": "/metrics", 
                    "system": "/system",
                    "docs": "/docs"
                }
            }
        
        @self.app.get("/health")
        async def health_check():
            """Detaylı health check endpoint"""
            try:
                health_checks = await self.health_monitor.run_all_checks()
                overall_status = await self.health_monitor.get_overall_status()
                
                services = {}
                
                for check in health_checks:
                    service_info = {
                        "status": check.status.value,
                        "response_time_ms": check.response_time_ms,
                        "message": check.message
                    }
                    
                    # Sistem için detay bilgileri ekle
                    if check.service == "system" and check.details:
                        service_info["details"] = check.details
                    
                    services[check.service] = service_info
                
                return {
                    "status": overall_status.value,
                    "timestamp": time.time(),
                    "uptime_seconds": time.time() - getattr(self.metrics, '_start_time', time.time()),
                    "services": services
                }
                
            except Exception as e:
                logger.error(f"Health check error: {e}")
                raise HTTPException(status_code=500, detail=f"Health check failed: {e}")
        
        @self.app.get("/health/simple")
        async def simple_health():
            """Basit health check - sadece OK/FAIL"""
            try:
                health_checks = await self.health_monitor.run_all_checks()
                
                for check in health_checks:
                    if check.status.value != "healthy":
                        raise HTTPException(status_code=503, detail="Service unhealthy")
                
                return {"status": "ok"}
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Simple health check error: {e}")
                raise HTTPException(status_code=500, detail="Health check failed")
        
        @self.app.get("/metrics")
        async def prometheus_metrics():
            """Prometheus metrics endpoint"""
            try:
                # Son metrikleri güncelle
                self.system_metrics_collector.update_prometheus_metrics()
                self.metrics.update_uptime()
                
                # Prometheus formatında metrics döndür
                metrics_data = generate_latest()
                return Response(
                    content=metrics_data,
                    media_type=CONTENT_TYPE_LATEST
                )
                
            except Exception as e:
                logger.error(f"Metrics endpoint error: {e}")
                raise HTTPException(status_code=500, detail=f"Metrics generation failed: {e}")
        
        @self.app.get("/system")
        async def system_info():
            """Sistem bilgileri endpoint"""
            try:
                # Sistem metrikleri
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                return {
                    "system": {
                        "platform": platform.platform(),
                        "python_version": platform.python_version(),
                        "cpu_cores": psutil.cpu_count(),
                        "cpu_usage_percent": cpu_percent,
                        "memory": {
                            "total_gb": round(memory.total / (1024**3), 2),
                            "available_gb": round(memory.available / (1024**3), 2),
                            "used_percent": memory.percent
                        },
                        "disk": {
                            "total_gb": round(disk.total / (1024**3), 2),
                            "free_gb": round(disk.free / (1024**3), 2),
                            "used_percent": round((disk.used / disk.total) * 100, 2)
                        }
                    },
                    "application": {
                        "uptime_seconds": self.metrics.get_uptime(),
                        "metrics_enabled": True,
                        "health_checks_enabled": True
                    }
                }
                
            except Exception as e:
                logger.error(f"System info error: {e}")
                raise HTTPException(status_code=500, detail=f"System info failed: {e}")
        
        @self.app.post("/system/gc")
        async def trigger_garbage_collection():
            """Garbage collection tetikle (debug amaçlı)"""
            try:
                collected = gc.collect()
                return {
                    "status": "completed",
                    "objects_collected": collected,
                    "timestamp": time.time()
                }
                
            except Exception as e:
                logger.error(f"GC trigger error: {e}")
                raise HTTPException(status_code=500, detail=f"GC failed: {e}")
        
        @self.app.get("/debug/config")
        async def debug_config():
            """Debug amaçlı config bilgileri (sensitive bilgiler hariç)"""
            try:
                return {
                    "metrics_port": 9091,
                    "health_enabled": True,
                    "debug_mode": True,
                    "timestamp": time.time()
                }
                
            except Exception as e:
                logger.error(f"Debug config error: {e}")
                raise HTTPException(status_code=500, detail=f"Debug config failed: {e}")
    
    def start_background_metrics_update(self):
        """Background metrics güncelleme thread'ini başlat"""
        def update_metrics():
            while not self.stop_metrics_update.is_set():
                try:
                    # Sistem metriklerini güncelle
                    self.system_metrics_collector.update_prometheus_metrics()
                    
                    # Uptime'ı güncelle
                    self.metrics.update_uptime()
                    
                    # Health check'leri çalıştır ve metrikleri güncelle
                    # Background thread'de çalıştığımız için async çağrıyı sync'e çevir
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(self._update_health_metrics())
                    except Exception as health_error:
                        logger.error(f"Health metrics update error: {health_error}")
                    finally:
                        if 'loop' in locals():
                            loop.close()
                    
                    time.sleep(5)  # Her 5 saniyede bir güncelle
                    
                except Exception as e:
                    logger.error(f"Background metrics update error: {e}")
                    
        self.metrics_update_thread = threading.Thread(target=update_metrics, daemon=True)
        self.metrics_update_thread.start()
        logger.info("📊 Background metrics update thread started")
    
    async def _update_health_metrics(self):
        """Health check metriklerini güncelle"""
        try:
            health_checks = await self.health_monitor.run_all_checks()
            
            for check in health_checks:
                self.metrics.record_health_check(
                    check.service,
                    check.status.value,
                    check.response_time_ms / 1000.0
                )
                
                # Bağlantı durumunu güncelle
                if check.service == "kafka":
                    self.metrics.set_kafka_connection_status(check.status.value == "healthy")
                elif check.service == "qdrant":
                    self.metrics.set_qdrant_connection_status(check.status.value == "healthy")
                    
        except Exception as e:
            logger.error(f"Health metrics update error: {e}")
    
    def start_server(self, host: str = "127.0.0.1", port: int = 8080):
        """FastAPI server'ını başlat"""
        # Background thread'i başlat
        self.start_background_metrics_update()
        
        # Server'ı başlat
        logger.info(f"🚀 Unified server starting on {host}:{port}")
        uvicorn.run(
            self.app, 
            host=host, 
            port=port,
            log_level="info",
            access_log=False  # Çok fazla log olmasın
        )
    
    def stop(self):
        """Server'ı durdur"""
        logger.info("🛑 Unified server stopping...")
        
        # Background thread'i durdur
        self.stop_metrics_update.set()
        if self.metrics_update_thread and self.metrics_update_thread.is_alive():
            self.metrics_update_thread.join(timeout=5)
            logger.info("📊 Background metrics thread stopped")
