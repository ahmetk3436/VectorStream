#!/usr/bin/env python3

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

from src.utils.circuit_breaker import (
    CircuitBreaker, 
    CircuitBreakerConfig, 
    CircuitBreakerError, 
    CircuitState,
    CircuitBreakerManager,
    circuit_breaker
)

class TestCircuitBreaker:
    """Circuit breaker unit testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        self.config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=1.0,  # Test için kısa süre
            success_threshold=2,
            timeout=0.5
        )
        self.circuit_breaker = CircuitBreaker("test_cb", self.config)
    
    def test_initial_state(self):
        """İlk durum testi"""
        assert self.circuit_breaker.state == CircuitState.CLOSED
        assert self.circuit_breaker.failure_count == 0
        assert self.circuit_breaker.success_count == 0
    
    @pytest.mark.asyncio
    async def test_successful_call(self):
        """Başarılı çağrı testi"""
        async def success_func():
            return "success"
        
        result = await self.circuit_breaker.call(success_func)
        assert result == "success"
        assert self.circuit_breaker.state == CircuitState.CLOSED
        assert self.circuit_breaker.failure_count == 0
    
    @pytest.mark.asyncio
    async def test_failure_threshold(self):
        """Başarısızlık eşiği testi"""
        async def failing_func():
            raise Exception("Test hatası")
        
        # İlk 2 hata - devre hala kapalı
        for i in range(2):
            with pytest.raises(Exception):
                await self.circuit_breaker.call(failing_func)
            assert self.circuit_breaker.state == CircuitState.CLOSED
        
        # 3. hata - devre açılmalı
        with pytest.raises(Exception):
            await self.circuit_breaker.call(failing_func)
        assert self.circuit_breaker.state == CircuitState.OPEN
    
    @pytest.mark.asyncio
    async def test_circuit_open_behavior(self):
        """Açık devre davranışı testi"""
        # Devreyi açık duruma getir
        self.circuit_breaker.state = CircuitState.OPEN
        self.circuit_breaker.next_attempt_time = time.time() + 10  # 10 saniye sonra
        
        async def any_func():
            return "should not be called"
        
        # Devre açıkken çağrı yapılmamalı
        with pytest.raises(CircuitBreakerError):
            await self.circuit_breaker.call(any_func)
    
    @pytest.mark.asyncio
    async def test_half_open_to_closed(self):
        """Yarı açık'tan kapalı'ya geçiş testi"""
        # Devreyi yarı açık duruma getir
        self.circuit_breaker.state = CircuitState.HALF_OPEN
        
        async def success_func():
            return "success"
        
        # İlk başarı
        result = await self.circuit_breaker.call(success_func)
        assert result == "success"
        assert self.circuit_breaker.state == CircuitState.HALF_OPEN
        assert self.circuit_breaker.success_count == 1
        
        # İkinci başarı - devre kapalı olmalı
        result = await self.circuit_breaker.call(success_func)
        assert result == "success"
        assert self.circuit_breaker.state == CircuitState.CLOSED
        assert self.circuit_breaker.success_count == 0
        assert self.circuit_breaker.failure_count == 0
    
    @pytest.mark.asyncio
    async def test_half_open_to_open(self):
        """Yarı açık'tan açık'a geçiş testi"""
        # Devreyi yarı açık duruma getir
        self.circuit_breaker.state = CircuitState.HALF_OPEN
        
        async def failing_func():
            raise Exception("Test hatası")
        
        # Yarı açık durumda hata - tekrar açık olmalı
        with pytest.raises(Exception):
            await self.circuit_breaker.call(failing_func)
        assert self.circuit_breaker.state == CircuitState.OPEN
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self):
        """Timeout işleme testi"""
        async def slow_func():
            await asyncio.sleep(1.0)  # Config'de timeout 0.5s
            return "too slow"
        
        with pytest.raises((CircuitBreakerError, asyncio.TimeoutError)) as exc_info:
            await self.circuit_breaker.call(slow_func)
        
        # Timeout hatası olabilir veya CircuitBreakerError olabilir
        assert self.circuit_breaker.failure_count == 1
    
    @pytest.mark.asyncio
    async def test_recovery_after_timeout(self):
        """Timeout sonrası kurtarma testi"""
        # Devreyi açık duruma getir
        self.circuit_breaker.state = CircuitState.OPEN
        self.circuit_breaker.next_attempt_time = time.time() + 0.1  # 0.1 saniye sonra
        
        async def success_func():
            return "recovered"
        
        # Kısa süre bekle
        await asyncio.sleep(0.2)
        
        # Şimdi çağrı başarılı olmalı ve devre yarı açık olmalı
        result = await self.circuit_breaker.call(success_func)
        assert result == "recovered"
        assert self.circuit_breaker.state == CircuitState.HALF_OPEN
    
    def test_get_state(self):
        """Durum bilgisi alma testi"""
        state = self.circuit_breaker.get_state()
        
        assert state['name'] == 'test_cb'
        assert state['state'] == 'closed'
        assert state['failure_count'] == 0
        assert state['success_count'] == 0
        assert 'config' in state
        assert state['config']['failure_threshold'] == 3

class TestCircuitBreakerManager:
    """Circuit breaker manager testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        self.manager = CircuitBreakerManager()
    
    def test_create_circuit_breaker(self):
        """Circuit breaker oluşturma testi"""
        config = CircuitBreakerConfig(failure_threshold=5)
        cb = self.manager.create_circuit_breaker("test_cb", config)
        
        assert cb.name == "test_cb"
        assert cb.config.failure_threshold == 5
        assert "test_cb" in self.manager.circuit_breakers
    
    def test_get_circuit_breaker(self):
        """Circuit breaker alma testi"""
        cb = self.manager.create_circuit_breaker("test_cb")
        retrieved_cb = self.manager.get_circuit_breaker("test_cb")
        
        assert cb is retrieved_cb
        
        # Olmayan circuit breaker
        assert self.manager.get_circuit_breaker("nonexistent") is None
    
    def test_get_all_states(self):
        """Tüm durumları alma testi"""
        self.manager.create_circuit_breaker("cb1")
        self.manager.create_circuit_breaker("cb2")
        
        states = self.manager.get_all_states()
        
        assert len(states) == 2
        assert "cb1" in states
        assert "cb2" in states
        assert states["cb1"]["name"] == "cb1"
        assert states["cb2"]["name"] == "cb2"
    
    def test_reset_circuit_breaker(self):
        """Circuit breaker sıfırlama testi"""
        cb = self.manager.create_circuit_breaker("test_cb")
        
        # Devreyi açık duruma getir
        cb.state = CircuitState.OPEN
        cb.failure_count = 5
        
        # Sıfırla
        result = self.manager.reset_circuit_breaker("test_cb")
        
        assert result is True
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        
        # Olmayan circuit breaker
        result = self.manager.reset_circuit_breaker("nonexistent")
        assert result is False

class TestCircuitBreakerDecorator:
    """Circuit breaker decorator testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        # Global manager'ı kullan
        from src.utils.circuit_breaker import circuit_breaker_manager
        self.manager = circuit_breaker_manager
    
    @pytest.mark.asyncio
    async def test_async_decorator(self):
        """Async decorator testi"""
        config = CircuitBreakerConfig(failure_threshold=2)
        
        @circuit_breaker("async_test_decorator", config)
        async def async_func(value):
            if value == "fail":
                raise Exception("Test hatası")
            return f"success: {value}"
        
        # Başarılı çağrı
        result = await async_func("test")
        assert result == "success: test"
        
        # Başarısız çağrılar
        with pytest.raises(Exception):
            await async_func("fail")
        
        with pytest.raises(Exception):
            await async_func("fail")
        
        # Devre açık olmalı
        cb = self.manager.get_circuit_breaker("async_test_decorator")
        assert cb is not None
        assert cb.state == CircuitState.OPEN

if __name__ == "__main__":
    pytest.main([__file__, "-v"])