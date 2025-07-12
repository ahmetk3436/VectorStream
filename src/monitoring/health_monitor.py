#!/usr/bin/env python3

import asyncio
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import aiohttp
from loguru import logger

class HealthStatus(Enum):
    """Sağlık durumu enum'u"""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"

@dataclass
class HealthCheck:
    """Sağlık kontrolü sonucu"""
    service: str
    status: HealthStatus
    message: str
    timestamp: float
    response_time_ms: Optional[float] = None
    details: Optional[Dict[str, Any]] = None

class HealthMonitor:
    """Sistem sağlık durumu izleyicisi"""
    
    def __init__(self, kafka_config=None, qdrant_config=None):
        self.kafka_config = kafka_config
        self.qdrant_config = qdrant_config
        self.health_checks = {}
        self.last_check_time = 0
        self.check_interval = 30  # 30 saniye
        
    async def check_kafka_health(self) -> HealthCheck:
        """Kafka sağlık kontrolü"""
        start_time = time.time()
        
        try:
            from kafka import KafkaConsumer
            from kafka.errors import KafkaError
            
            if not self.kafka_config:
                raise Exception("Kafka config not provided")
            
            # Kafka bağlantısını test et
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_config.bootstrap_servers,
                request_timeout_ms=5000,
                api_version=(0, 10, 1)
            )
            
            # Test connection
            consumer.list_consumer_group_offsets()
            consumer.close()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheck(
                service="kafka",
                status=HealthStatus.HEALTHY,
                message="Kafka is healthy",
                timestamp=time.time(),
                response_time_ms=response_time,
                details={"bootstrap_servers": self.kafka_config.bootstrap_servers}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            logger.error(f"Kafka health check failed: {str(e)}")
            
            return HealthCheck(
                service="kafka",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed",
                timestamp=time.time(),
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    async def check_qdrant_health(self) -> HealthCheck:
        """Qdrant sağlık kontrolü"""
        start_time = time.time()
        
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http.exceptions import UnexpectedResponse
            
            if not self.qdrant_config:
                raise Exception("Qdrant config not provided")
            
            # Qdrant bağlantısını test et
            client = QdrantClient(
                host=self.qdrant_config.host,
                port=self.qdrant_config.port,
                timeout=5
            )
            
            # Koleksiyonları listele
            collections = client.get_collections()
            
            response_time = (time.time() - start_time) * 1000
            
            return HealthCheck(
                service="qdrant",
                status=HealthStatus.HEALTHY,
                message="Qdrant is healthy",
                timestamp=time.time(),
                response_time_ms=response_time,
                details={"collections_count": len(collections.collections)}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            logger.error(f"Qdrant health check failed: {str(e)}")
            
            return HealthCheck(
                service="qdrant",
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed",
                timestamp=time.time(),
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    async def check_system_health(self) -> HealthCheck:
        """Sistem kaynaklarını kontrol et"""
        start_time = time.time()
        
        try:
            import psutil
            
            # CPU ve memory kullanımını al
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            response_time = (time.time() - start_time) * 1000
            
            # Eşik değerleri kontrol et
            status = HealthStatus.HEALTHY
            messages = []
            
            if cpu_percent > 90 or memory.percent > 90 or disk.percent > 90:
                status = HealthStatus.UNHEALTHY
                messages.append("System is unhealthy")
            elif cpu_percent > 80 or memory.percent > 80 or disk.percent > 80:
                status = HealthStatus.DEGRADED
                messages.append("System is degraded")
            else:
                messages.append("System is healthy")
            
            message = messages[0] if messages else "System is healthy"
            
            return HealthCheck(
                service="system",
                status=status,
                message=message,
                timestamp=time.time(),
                response_time_ms=response_time,
                details={
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "disk_percent": disk.percent,
                    "memory_available_gb": round(memory.available / (1024**3), 2)
                }
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            logger.error(f"System health check failed: {str(e)}")
            
            return HealthCheck(
                service="system",
                status=HealthStatus.UNKNOWN,
                message=f"System check failed: {str(e)}",
                timestamp=time.time(),
                response_time_ms=response_time,
                details={"error": str(e)}
            )
    
    async def run_all_checks(self) -> list[HealthCheck]:
        """Tüm sağlık kontrollerini çalıştır"""
        logger.info("Running health checks...")
        
        # Paralel olarak tüm kontrolleri çalıştır
        tasks = [
            self.check_kafka_health(),
            self.check_qdrant_health(),
            self.check_system_health()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        health_checks = []
        for result in results:
            if isinstance(result, HealthCheck):
                health_checks.append(result)
            else:
                logger.error(f"Health check failed with exception: {result}")
        
        # Store as dict for internal use
        self.health_checks = {check.service: check for check in health_checks}
        self.last_check_time = time.time()
        
        return health_checks
    
    async def get_overall_status(self) -> HealthStatus:
        """Genel sistem durumunu döndür"""
        # Run health checks first to get current status
        health_checks = await self.run_all_checks()
        
        if not health_checks:
            return HealthStatus.UNKNOWN
        
        statuses = [check.status for check in health_checks]
        
        if any(status == HealthStatus.UNHEALTHY for status in statuses):
            return HealthStatus.UNHEALTHY
        elif any(status == HealthStatus.DEGRADED for status in statuses):
            return HealthStatus.DEGRADED
        elif all(status == HealthStatus.HEALTHY for status in statuses):
            return HealthStatus.HEALTHY
        else:
            return HealthStatus.UNKNOWN
    
    async def get_health_summary(self) -> Dict[str, Any]:
        """Sağlık durumu özetini döndür"""
        overall_status = await self.get_overall_status()
        
        return {
            "status": overall_status.value,
            "timestamp": time.time(),
            "last_check": self.last_check_time,
            "checks": [
                {
                    "service": check.service,
                    "status": check.status.value,
                    "message": check.message,
                    "timestamp": check.timestamp,
                    "response_time_ms": check.response_time_ms,
                    "details": check.details
                }
                for check in self.health_checks.values()
            ]
        }
    
    async def start_monitoring(self):
        """Sürekli sağlık izlemeyi başlat"""
        logger.info(f"Starting health monitoring with {self.check_interval}s interval")
        
        while True:
            try:
                await self.run_all_checks()
                
                # Sonuçları logla
                overall_status = await self.get_overall_status()
                logger.info(f"Health check completed - Overall status: {overall_status.value}")
                
                for name, check in self.health_checks.items():
                    logger.debug(f"{name}: {check.status.value} ({check.response_time_ms:.1f}ms) - {check.message}")
                
            except Exception as e:
                logger.error(f"Health monitoring error: {str(e)}")
            
            await asyncio.sleep(self.check_interval)