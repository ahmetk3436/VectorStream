#!/usr/bin/env python3

import asyncio
import time
from enum import Enum
from typing import Callable, Any, Optional, Dict
from dataclasses import dataclass
from loguru import logger

class CircuitState(Enum):
    """Circuit breaker durumları"""
    CLOSED = "closed"      # Normal çalışma
    OPEN = "open"          # Devre açık, istekler reddediliyor
    HALF_OPEN = "half_open" # Test aşaması

@dataclass
class CircuitBreakerConfig:
    """Circuit breaker konfigürasyonu"""
    failure_threshold: int = 5          # Başarısızlık eşiği
    recovery_timeout: float = 60.0      # Kurtarma süresi (saniye)
    expected_exception: type = Exception # Beklenen hata türü
    success_threshold: int = 3          # Half-open'da başarı eşiği
    timeout: float = 30.0               # İşlem timeout süresi

class CircuitBreakerError(Exception):
    """Circuit breaker hatası"""
    pass

class CircuitBreaker:
    """Circuit breaker pattern implementasyonu"""
    
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.next_attempt_time = 0
        
    def _should_attempt_reset(self) -> bool:
        """Reset denemesi yapılmalı mı kontrol et"""
        return (
            self.state == CircuitState.OPEN and
            time.time() >= self.next_attempt_time
        )
    
    def _record_success(self):
        """Başarılı işlem kaydı"""
        self.failure_count = 0
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self._close_circuit()
        elif self.state == CircuitState.OPEN:
            self._half_open_circuit()
    
    def _record_failure(self, exception: Exception):
        """Başarısız işlem kaydı"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            self._open_circuit()
        elif (
            self.state == CircuitState.CLOSED and
            self.failure_count >= self.config.failure_threshold
        ):
            self._open_circuit()
    
    def _close_circuit(self):
        """Devreyi kapat (normal durum)"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        logger.info(f"Circuit breaker '{self.name}' CLOSED durumuna geçti")
    
    def _open_circuit(self):
        """Devreyi aç (hata durumu)"""
        self.state = CircuitState.OPEN
        self.success_count = 0
        self.next_attempt_time = time.time() + self.config.recovery_timeout
        logger.warning(
            f"Circuit breaker '{self.name}' OPEN durumuna geçti. "
            f"Sonraki deneme: {self.config.recovery_timeout} saniye sonra"
        )
    
    def _half_open_circuit(self):
        """Devreyi yarı aç (test durumu)"""
        self.state = CircuitState.HALF_OPEN
        self.success_count = 0
        logger.info(f"Circuit breaker '{self.name}' HALF_OPEN durumuna geçti")
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Fonksiyonu circuit breaker ile çağır"""
        
        # Devre açıksa ve henüz deneme zamanı gelmemişse hata fırlat
        if self.state == CircuitState.OPEN:
            if not self._should_attempt_reset():
                raise CircuitBreakerError(
                    f"Circuit breaker '{self.name}' açık. "
                    f"Sonraki deneme: {self.next_attempt_time - time.time():.1f} saniye sonra"
                )
            else:
                self._half_open_circuit()
        
        try:
            # Timeout ile fonksiyonu çağır
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.config.timeout
                )
            else:
                result = func(*args, **kwargs)
            
            self._record_success()
            return result
            
        except self.config.expected_exception as e:
            self._record_failure(e)
            raise
        except asyncio.TimeoutError as e:
            self._record_failure(e)
            raise CircuitBreakerError(f"Timeout: {self.config.timeout}s") from e
        except Exception as e:
            # Beklenmeyen hatalar için de kayıt tut
            self._record_failure(e)
            raise
    
    def get_state(self) -> Dict[str, Any]:
        """Circuit breaker durumunu döndür"""
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'last_failure_time': self.last_failure_time,
            'next_attempt_time': self.next_attempt_time if self.state == CircuitState.OPEN else None,
            'config': {
                'failure_threshold': self.config.failure_threshold,
                'recovery_timeout': self.config.recovery_timeout,
                'success_threshold': self.config.success_threshold,
                'timeout': self.config.timeout
            }
        }

class CircuitBreakerManager:
    """Circuit breaker yöneticisi"""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
    
    def create_circuit_breaker(
        self, 
        name: str, 
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Yeni circuit breaker oluştur"""
        if config is None:
            config = CircuitBreakerConfig()
        
        circuit_breaker = CircuitBreaker(name, config)
        self.circuit_breakers[name] = circuit_breaker
        logger.info(f"Circuit breaker '{name}' oluşturuldu")
        return circuit_breaker
    
    def get_circuit_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """Circuit breaker'ı al"""
        return self.circuit_breakers.get(name)
    
    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """Tüm circuit breaker durumlarını al"""
        return {
            name: cb.get_state() 
            for name, cb in self.circuit_breakers.items()
        }
    
    def reset_circuit_breaker(self, name: str) -> bool:
        """Circuit breaker'ı sıfırla"""
        if name in self.circuit_breakers:
            self.circuit_breakers[name]._close_circuit()
            logger.info(f"Circuit breaker '{name}' sıfırlandı")
            return True
        return False

# Global circuit breaker manager
circuit_breaker_manager = CircuitBreakerManager()

def reset_circuit_breaker_manager():
    """Global circuit breaker manager'ı sıfırla (test amaçlı)"""
    global circuit_breaker_manager
    circuit_breaker_manager = CircuitBreakerManager()

# Decorator for easy usage
def circuit_breaker(
    name: str, 
    config: Optional[CircuitBreakerConfig] = None
):
    """Circuit breaker decorator"""
    def decorator(func):
        cb = circuit_breaker_manager.create_circuit_breaker(name, config)
        
        async def async_wrapper(*args, **kwargs):
            return await cb.call(func, *args, **kwargs)
        
        def sync_wrapper(*args, **kwargs):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(cb.call(func, *args, **kwargs))
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator