#!/usr/bin/env python3

import pytest
import asyncio
import time
from unittest.mock import Mock, patch
from src.utils.error_handler import (
    RetryPolicy, RetryManager, BackoffStrategy,
    retry_with_policy, retry_on_failure, retry_network_errors,
    retry_with_backoff, DEFAULT_RETRY_POLICY, AGGRESSIVE_RETRY_POLICY,
    CONSERVATIVE_RETRY_POLICY, NETWORK_RETRY_POLICY
)

class TestRetryPolicy:
    """RetryPolicy sınıfı testleri"""
    
    def test_default_policy(self):
        """Default policy testi"""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 60.0
        assert policy.backoff_strategy == BackoffStrategy.EXPONENTIAL
        assert policy.jitter is True
        assert policy.retryable_exceptions == [Exception]
        assert policy.non_retryable_exceptions == []
        assert policy.timeout is None
    
    def test_custom_policy(self):
        """Özel policy testi"""
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=2.0,
            max_delay=120.0,
            backoff_strategy=BackoffStrategy.LINEAR,
            jitter=False,
            retryable_exceptions=[ConnectionError],
            non_retryable_exceptions=[ValueError],
            timeout=30.0
        )
        assert policy.max_attempts == 5
        assert policy.base_delay == 2.0
        assert policy.max_delay == 120.0
        assert policy.backoff_strategy == BackoffStrategy.LINEAR
        assert policy.jitter is False
        assert policy.retryable_exceptions == [ConnectionError]
        assert policy.non_retryable_exceptions == [ValueError]
        assert policy.timeout == 30.0

class TestRetryManager:
    """RetryManager sınıfı testleri"""
    
    def setup_method(self):
        """Her test öncesi setup"""
        self.policy = RetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL
        )
        self.manager = RetryManager(self.policy)
    
    def test_fixed_backoff(self):
        """Fixed backoff testi"""
        policy = RetryPolicy(backoff_strategy=BackoffStrategy.FIXED, base_delay=2.0)
        manager = RetryManager(policy)
        
        assert manager.calculate_delay(1) == 2.0
        assert manager.calculate_delay(2) == 2.0
        assert manager.calculate_delay(3) == 2.0
    
    def test_linear_backoff(self):
        """Linear backoff testi"""
        policy = RetryPolicy(backoff_strategy=BackoffStrategy.LINEAR, base_delay=1.0)
        manager = RetryManager(policy)
        
        assert manager.calculate_delay(1) == 1.0
        assert manager.calculate_delay(2) == 2.0
        assert manager.calculate_delay(3) == 3.0
    
    def test_exponential_backoff(self):
        """Exponential backoff testi"""
        policy = RetryPolicy(backoff_strategy=BackoffStrategy.EXPONENTIAL, base_delay=1.0)
        manager = RetryManager(policy)
        
        assert manager.calculate_delay(1) == 1.0
        assert manager.calculate_delay(2) == 2.0
        assert manager.calculate_delay(3) == 4.0
        assert manager.calculate_delay(4) == 8.0
    
    def test_exponential_jitter_backoff(self):
        """Exponential jitter backoff testi"""
        policy = RetryPolicy(
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            base_delay=1.0,
            jitter=True
        )
        manager = RetryManager(policy)
        
        # Jitter nedeniyle tam değer kontrolü yapamayız, range kontrolü yapalım
        delay1 = manager.calculate_delay(1)
        delay2 = manager.calculate_delay(2)
        delay3 = manager.calculate_delay(3)
        
        assert 0.5 <= delay1 <= 1.0  # 1.0 * (0.5-1.0)
        assert 1.0 <= delay2 <= 2.0  # 2.0 * (0.5-1.0)
        assert 2.0 <= delay3 <= 4.0  # 4.0 * (0.5-1.0)
    
    def test_max_delay_limit(self):
        """Max delay limit testi"""
        policy = RetryPolicy(
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            base_delay=1.0,
            max_delay=5.0
        )
        manager = RetryManager(policy)
        
        assert manager.calculate_delay(1) == 1.0
        assert manager.calculate_delay(2) == 2.0
        assert manager.calculate_delay(3) == 4.0
        assert manager.calculate_delay(4) == 5.0  # Max delay limit
        assert manager.calculate_delay(5) == 5.0  # Max delay limit
    
    def test_should_retry_max_attempts(self):
        """Max attempts kontrolü testi"""
        assert self.manager.should_retry(Exception(), 1) is True
        assert self.manager.should_retry(Exception(), 2) is True
        assert self.manager.should_retry(Exception(), 3) is False  # Max attempts reached
    
    def test_should_retry_retryable_exceptions(self):
        """Retryable exceptions testi"""
        policy = RetryPolicy(
            max_attempts=3,
            retryable_exceptions=[ConnectionError, TimeoutError]
        )
        manager = RetryManager(policy)
        
        assert manager.should_retry(ConnectionError(), 1) is True
        assert manager.should_retry(TimeoutError(), 1) is True
        assert manager.should_retry(ValueError(), 1) is False  # Not in retryable list
    
    def test_should_retry_non_retryable_exceptions(self):
        """Non-retryable exceptions testi"""
        policy = RetryPolicy(
            max_attempts=3,
            retryable_exceptions=[Exception],
            non_retryable_exceptions=[ValueError, TypeError]
        )
        manager = RetryManager(policy)
        
        assert manager.should_retry(ConnectionError(), 1) is True
        assert manager.should_retry(ValueError(), 1) is False  # Non-retryable
        assert manager.should_retry(TypeError(), 1) is False  # Non-retryable

class TestRetryDecorators:
    """Retry decorator testleri"""
    
    def test_retry_with_policy_success(self):
        """Başarılı retry testi"""
        policy = RetryPolicy(max_attempts=3)
        
        @retry_with_policy(policy)
        def test_func():
            return "success"
        
        result = test_func()
        assert result == "success"
    
    def test_retry_with_policy_failure_then_success(self):
        """İlk başarısızlık sonra başarı testi"""
        policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        call_count = 0
        
        @retry_with_policy(policy)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 2
    
    def test_retry_with_policy_max_attempts_exceeded(self):
        """Max attempts aşıldığında hata fırlatma testi"""
        policy = RetryPolicy(max_attempts=2, base_delay=0.1)
        call_count = 0
        
        @retry_with_policy(policy)
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Always fails")
        
        with pytest.raises(ConnectionError):
            test_func()
        
        assert call_count == 2
    
    def test_retry_with_policy_non_retryable_exception(self):
        """Non-retryable exception testi"""
        policy = RetryPolicy(
            max_attempts=3,
            base_delay=0.1,
            retryable_exceptions=[ConnectionError],
            non_retryable_exceptions=[ValueError]
        )
        call_count = 0
        
        @retry_with_policy(policy)
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Non-retryable error")
        
        with pytest.raises(ValueError):
            test_func()
        
        assert call_count == 1  # Sadece bir kez çağrılmalı
    
    @pytest.mark.asyncio
    async def test_async_retry_with_policy(self):
        """Async retry testi"""
        policy = RetryPolicy(max_attempts=3, base_delay=0.1)
        call_count = 0
        
        @retry_with_policy(policy)
        async def test_async_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Connection failed")
            return "async success"
        
        result = await test_async_func()
        assert result == "async success"
        assert call_count == 2
    
    @pytest.mark.asyncio
    async def test_async_retry_timeout(self):
        """Async timeout testi"""
        policy = RetryPolicy(max_attempts=2, timeout=0.1)
        
        @retry_with_policy(policy)
        async def slow_func():
            await asyncio.sleep(0.2)  # Timeout'tan uzun
            return "success"
        
        with pytest.raises(asyncio.TimeoutError):
            await slow_func()
    
    def test_retry_on_failure_decorator(self):
        """retry_on_failure decorator testi"""
        call_count = 0
        
        @retry_on_failure(max_attempts=3, base_delay=0.1)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Fail once")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 2
    
    def test_retry_network_errors_decorator(self):
        """retry_network_errors decorator testi"""
        call_count = 0
        
        @retry_network_errors(max_attempts=3)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Network error")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 2
    
    def test_retry_network_errors_non_network_exception(self):
        """Network retry decorator ile non-network exception testi"""
        call_count = 0
        
        @retry_network_errors(max_attempts=3)
        def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not a network error")
        
        with pytest.raises(ValueError):
            test_func()
        
        assert call_count == 1  # Retry yapılmamalı
    
    def test_retry_with_backoff_decorator(self):
        """retry_with_backoff decorator testi"""
        call_count = 0
        start_time = time.time()
        
        @retry_with_backoff(
            strategy=BackoffStrategy.FIXED,
            max_attempts=3,
            base_delay=0.1
        )
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Fail twice")
            return "success"
        
        result = test_func()
        elapsed = time.time() - start_time
        
        assert result == "success"
        assert call_count == 3
        assert elapsed >= 0.2  # 2 retry * 0.1s delay

class TestPredefinedPolicies:
    """Önceden tanımlanmış policy testleri"""
    
    def test_default_retry_policy(self):
        """Default retry policy testi"""
        assert DEFAULT_RETRY_POLICY.max_attempts == 3
        assert DEFAULT_RETRY_POLICY.base_delay == 1.0
        assert DEFAULT_RETRY_POLICY.max_delay == 30.0
        assert DEFAULT_RETRY_POLICY.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER
    
    def test_aggressive_retry_policy(self):
        """Aggressive retry policy testi"""
        assert AGGRESSIVE_RETRY_POLICY.max_attempts == 5
        assert AGGRESSIVE_RETRY_POLICY.base_delay == 0.5
        assert AGGRESSIVE_RETRY_POLICY.max_delay == 60.0
        assert AGGRESSIVE_RETRY_POLICY.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER
    
    def test_conservative_retry_policy(self):
        """Conservative retry policy testi"""
        assert CONSERVATIVE_RETRY_POLICY.max_attempts == 2
        assert CONSERVATIVE_RETRY_POLICY.base_delay == 2.0
        assert CONSERVATIVE_RETRY_POLICY.max_delay == 10.0
        assert CONSERVATIVE_RETRY_POLICY.backoff_strategy == BackoffStrategy.FIXED
    
    def test_network_retry_policy(self):
        """Network retry policy testi"""
        assert NETWORK_RETRY_POLICY.max_attempts == 4
        assert NETWORK_RETRY_POLICY.base_delay == 1.0
        assert NETWORK_RETRY_POLICY.max_delay == 45.0
        assert NETWORK_RETRY_POLICY.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER
        assert NETWORK_RETRY_POLICY.timeout == 30.0
        assert ConnectionError in NETWORK_RETRY_POLICY.retryable_exceptions
        assert TimeoutError in NETWORK_RETRY_POLICY.retryable_exceptions
        assert OSError in NETWORK_RETRY_POLICY.retryable_exceptions
        assert ValueError in NETWORK_RETRY_POLICY.non_retryable_exceptions
        assert TypeError in NETWORK_RETRY_POLICY.non_retryable_exceptions

class TestBackwardCompatibility:
    """Backward compatibility testleri"""
    
    def test_handle_errors_decorator(self):
        """Eski handle_errors decorator testi"""
        from src.utils.error_handler import handle_errors
        
        call_count = 0
        
        @handle_errors(retry_count=2)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Fail once")
            return "success"
        
        result = test_func()
        assert result == "success"
        assert call_count == 2
    
    def test_handle_errors_with_fallback(self):
        """Fallback value ile handle_errors testi"""
        from src.utils.error_handler import handle_errors
        
        @handle_errors(retry_count=1, fallback_value="fallback")
        def test_func():
            raise Exception("Always fails")
        
        result = test_func()
        assert result == "fallback"

if __name__ == '__main__':
    pytest.main([__file__, '-v'])