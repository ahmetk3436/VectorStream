#!/usr/bin/env python3

import asyncio
import time
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
from loguru import logger

from src.monitoring.health_monitor import HealthMonitor, HealthStatus

class HealthCheckServer:
    """Health check HTTP server"""
    
    def __init__(self, kafka_config=None, qdrant_config=None, health_monitor=None):
        self.kafka_config = kafka_config
        self.qdrant_config = qdrant_config
        self.health_monitor = health_monitor
        self.app = FastAPI(title="NewMind AI Health Check", version="1.0.0")
        self.setup_routes()
        
    def setup_routes(self):
        """HTTP endpoint'lerini ayarla"""
        
        @self.app.get("/health")
        async def health_check():
            """Genel sağlık durumu endpoint'i"""
            try:
                # Tüm kontrolleri çalıştır
                checks = await self.health_monitor.run_all_checks()
                overall_status = await self.health_monitor.get_overall_status()
                
                # HTTP status code'unu belirle
                if overall_status == HealthStatus.HEALTHY:
                    status_code = 200
                elif overall_status == HealthStatus.DEGRADED:
                    status_code = 200  # Degraded durumda da 200 döndür ama uyarı ver
                else:
                    status_code = 503  # Service Unavailable
                
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "status": overall_status.value,
                        "checks": [
                            {
                                "service": check.service,
                                "status": check.status.value,
                                "message": check.message,
                                "timestamp": check.timestamp
                            }
                            for check in checks
                        ]
                    }
                )
                
            except Exception as e:
                logger.error(f"Health check endpoint error: {str(e)}")
                return JSONResponse(
                    status_code=500,
                    content={
                        "status": "error",
                        "message": f"Health check failed: {str(e)}"
                    }
                )
        
        @self.app.get("/health/live")
        async def liveness_probe():
            """Kubernetes liveness probe"""
            try:
                overall_status = await self.health_monitor.get_overall_status()
                
                if overall_status == HealthStatus.UNHEALTHY:
                    return JSONResponse(
                        status_code=503,
                        content={"status": "dead", "timestamp": time.time()}
                    )
                else:
                    return JSONResponse(
                        status_code=200,
                        content={"status": "alive", "timestamp": time.time()}
                    )
            except Exception:
                return JSONResponse(
                    status_code=200,
                    content={"status": "alive", "timestamp": time.time()}
                )
        
        @self.app.get("/health/ready")
        async def readiness_probe():
            """Kubernetes readiness probe"""
            try:
                # Tüm servislerin hazır olup olmadığını kontrol et
                await self.health_monitor.run_all_checks()
                overall_status = await self.health_monitor.get_overall_status()
                
                if overall_status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]:
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "ready",
                            "timestamp": time.time()
                        }
                    )
                else:
                    return JSONResponse(
                        status_code=503,
                        content={
                            "status": "not ready",
                            "timestamp": time.time()
                        }
                    )
                    
            except Exception as e:
                logger.error(f"Readiness probe error: {str(e)}")
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "not ready",
                        "timestamp": time.time()
                    }
                )
        
        @self.app.get("/health/kafka")
        async def kafka_health():
            """Kafka özel sağlık kontrolü"""
            try:
                kafka_check = await self.health_monitor.check_kafka_health()
                
                status_code = 200 if kafka_check.status == HealthStatus.HEALTHY else 503
                
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "service": kafka_check.service,
                        "status": kafka_check.status.value,
                        "message": kafka_check.message,
                        "timestamp": kafka_check.timestamp
                    }
                )
                
            except Exception as e:
                logger.error(f"Kafka health check error: {str(e)}")
                return JSONResponse(
                    status_code=503,
                    content={
                        "service": "kafka",
                        "status": "error",
                        "message": str(e)
                    }
                )
        
        @self.app.get("/health/qdrant")
        async def qdrant_health():
            """Qdrant özel sağlık kontrolü"""
            try:
                qdrant_check = await self.health_monitor.check_qdrant_health()
                
                status_code = 200 if qdrant_check.status == HealthStatus.HEALTHY else 503
                
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "service": qdrant_check.service,
                        "status": qdrant_check.status.value,
                        "message": qdrant_check.message,
                        "timestamp": qdrant_check.timestamp
                    }
                )
                
            except Exception as e:
                logger.error(f"Qdrant health check error: {str(e)}")
                return JSONResponse(
                    status_code=503,
                    content={
                        "service": "qdrant",
                        "status": "error",
                        "message": str(e)
                    }
                )
        
        @self.app.get("/health/system")
        async def system_health():
            """Sistem kaynaklarının sağlık kontrolü"""
            try:
                system_check = await self.health_monitor.check_system_health()
                
                status_code = 200 if system_check.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED] else 503
                
                return JSONResponse(
                    status_code=status_code,
                    content={
                        "service": system_check.service,
                        "status": system_check.status.value,
                        "message": system_check.message,
                        "timestamp": system_check.timestamp
                    }
                )
                
            except Exception as e:
                logger.error(f"System health check error: {str(e)}")
                return JSONResponse(
                    status_code=503,
                    content={
                        "service": "system",
                        "status": "error",
                        "message": str(e)
                    }
                )
    
    async def start_server(self, host: str = "0.0.0.0", port: int = 8080):
        """Health check server'ını başlat"""
        logger.info(f"Starting health check server on {host}:{port}")
        
        config = uvicorn.Config(
            app=self.app,
            host=host,
            port=port,
            log_level="info"
        )
        
        server = uvicorn.Server(config)
        await server.serve()

# Standalone health check fonksiyonu
async def run_health_check(kafka_config, qdrant_config) -> Dict[str, Any]:
    """Tek seferlik sağlık kontrolü çalıştır"""
    health_monitor = HealthMonitor(kafka_config, qdrant_config)
    health_checks = await health_monitor.run_all_checks()
    overall_status = await health_monitor.get_overall_status()
    
    return {
        "overall_status": overall_status.value,
        "checks": [
            {
                "service": check.service,
                "status": check.status.value,
                "message": check.message,
                "timestamp": check.timestamp
            }
            for check in health_checks
        ]
    }

if __name__ == "__main__":
    # Test için basit konfigürasyon
    test_config = {
        "kafka": {
            "host": "localhost",
            "port": 9092
        },
        "qdrant": {
            "host": "localhost",
            "port": 6333
        },
        "health_check_interval": 30
    }
    
    async def main():
        health_monitor = HealthMonitor(test_config)
        health_server = HealthCheckServer(test_config, health_monitor)
        
        # Health monitoring'i arka planda başlat
        monitoring_task = asyncio.create_task(health_monitor.start_monitoring())
        
        # HTTP server'ı başlat
        await health_server.start_server(port=8080)
    
    asyncio.run(main())