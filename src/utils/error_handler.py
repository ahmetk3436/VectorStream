import functools
import asyncio
import time
import random
from typing import Callable, Any, Optional, List, Type, Union
from dataclasses import dataclass
from enum import Enum
from loguru import logger
from src.exceptions.base_exceptions import NewMindAIException

class BackoffStrategy(Enum):
    """Backoff stratejileri"""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"

@dataclass
class RetryPolicy:
    """Retry policy konfigürasyonu"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter: bool = True
    retryable_exceptions: List[Type[Exception]] = None
    non_retryable_exceptions: List[Type[Exception]] = None
    timeout: Optional[float] = None
    
    def __post_init__(self):
        if self.retryable_exceptions is None:
            self.retryable_exceptions = [Exception]
        if self.non_retryable_exceptions is None:
            self.non_retryable_exceptions = []

class RetryManager:
    """Gelişmiş retry yönetimi"""
    
    def __init__(self, policy: RetryPolicy):
        self.policy = policy
        
    def calculate_delay(self, attempt: int) -> float:
        """Attempt sayısına göre delay hesapla"""
        if self.policy.backoff_strategy == BackoffStrategy.FIXED:
            delay = self.policy.base_delay
        elif self.policy.backoff_strategy == BackoffStrategy.LINEAR:
            delay = self.policy.base_delay * attempt
        elif self.policy.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.policy.base_delay * (2 ** (attempt - 1))
        elif self.policy.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER:
            delay = self.policy.base_delay * (2 ** (attempt - 1))
            if self.policy.jitter:
                delay *= (0.5 + random.random() * 0.5)  # %50-100 jitter
        else:
            delay = self.policy.base_delay
            
        return min(delay, self.policy.max_delay)
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Retry yapılmalı mı kontrol et"""
        if attempt >= self.policy.max_attempts:
            return False
            
        # Non-retryable exceptions kontrolü
        for exc_type in self.policy.non_retryable_exceptions:
            if isinstance(exception, exc_type):
                return False
                
        # Retryable exceptions kontrolü
        for exc_type in self.policy.retryable_exceptions:
            if isinstance(exception, exc_type):
                return True
                
        return False

def retry_with_policy(policy: RetryPolicy):
    """Policy tabanlı retry decorator"""
    retry_manager = RetryManager(policy)
    
    def decorator(func: Callable):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, policy.max_attempts + 1):
                try:
                    if policy.timeout:
                        # Timeout kontrolü için signal kullanılabilir
                        start_time = time.time()
                        result = func(*args, **kwargs)
                        elapsed = time.time() - start_time
                        if elapsed > policy.timeout:
                            raise TimeoutError(f"Function {func.__name__} timed out after {elapsed:.2f}s")
                        return result
                    else:
                        return func(*args, **kwargs)
                        
                except Exception as e:
                    last_exception = e
                    logger.error(f"Attempt {attempt} failed for {func.__name__}: {str(e)}")
                    
                    if not retry_manager.should_retry(e, attempt):
                        logger.error(f"Not retrying {func.__name__} due to non-retryable exception or max attempts reached")
                        raise
                    
                    if attempt < policy.max_attempts:
                        delay = retry_manager.calculate_delay(attempt)
                        logger.info(f"Retrying {func.__name__} in {delay:.2f}s (attempt {attempt + 1}/{policy.max_attempts})")
                        time.sleep(delay)
            
            # Bu noktaya ulaşılmamalı ama güvenlik için
            if last_exception:
                raise last_exception
                
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, policy.max_attempts + 1):
                try:
                    if policy.timeout:
                        result = await asyncio.wait_for(func(*args, **kwargs), timeout=policy.timeout)
                        return result
                    else:
                        return await func(*args, **kwargs)
                        
                except Exception as e:
                    last_exception = e
                    logger.error(f"Attempt {attempt} failed for {func.__name__}: {str(e)}")
                    
                    if not retry_manager.should_retry(e, attempt):
                        logger.error(f"Not retrying {func.__name__} due to non-retryable exception or max attempts reached")
                        raise
                    
                    if attempt < policy.max_attempts:
                        delay = retry_manager.calculate_delay(attempt)
                        logger.info(f"Retrying {func.__name__} in {delay:.2f}s (attempt {attempt + 1}/{policy.max_attempts})")
                        await asyncio.sleep(delay)
            
            # Bu noktaya ulaşılmamalı ama güvenlik için
            if last_exception:
                raise last_exception
        
        # Async fonksiyon mu kontrol et
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator

# Önceden tanımlanmış retry policy'leri
DEFAULT_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    base_delay=1.0,
    max_delay=30.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER
)

AGGRESSIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=5,
    base_delay=0.5,
    max_delay=60.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER
)

CONSERVATIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=2,
    base_delay=2.0,
    max_delay=10.0,
    backoff_strategy=BackoffStrategy.FIXED
)

NETWORK_RETRY_POLICY = RetryPolicy(
    max_attempts=4,
    base_delay=1.0,
    max_delay=45.0,
    backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
    timeout=30.0,
    retryable_exceptions=[ConnectionError, TimeoutError, OSError],
    non_retryable_exceptions=[ValueError, TypeError]
)

# Backward compatibility için eski decorator
def handle_errors(retry_count: int = 0, fallback_value: Any = None):
    """Backward compatibility için eski retry decorator"""
    policy = RetryPolicy(
        max_attempts=retry_count + 1,
        base_delay=1.0,
        backoff_strategy=BackoffStrategy.FIXED
    )
    
    def decorator(func: Callable):
        @retry_with_policy(policy)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if fallback_value is not None:
                    logger.warning(f"Using fallback value for {func.__name__} due to: {str(e)}")
                    return fallback_value
                raise
        return wrapper
    return decorator

# Convenience decorators
def retry_on_failure(max_attempts: int = 3, base_delay: float = 1.0):
    """Basit retry decorator"""
    policy = RetryPolicy(
        max_attempts=max_attempts,
        base_delay=base_delay,
        backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER
    )
    return retry_with_policy(policy)

def retry_network_errors(max_attempts: int = 4):
    """Network hatalarında retry decorator"""
    return retry_with_policy(NETWORK_RETRY_POLICY._replace(max_attempts=max_attempts))

def retry_with_backoff(strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL_JITTER, 
                      max_attempts: int = 3, 
                      base_delay: float = 1.0,
                      max_delay: float = 60.0):
    """Özelleştirilebilir backoff ile retry decorator"""
    policy = RetryPolicy(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        backoff_strategy=strategy
    )
    return retry_with_policy(policy)