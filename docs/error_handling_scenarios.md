# Comprehensive Error Handling Documentation

## 🎯 Genel Bakış

Bu dokümantasyon VectorStream sisteminin kapsamlı error handling stratejilerini, failure recovery mekanizmalarını ve çeşitli hata senaryolarına yönelik çözümleri detaylandırır.

## 🚨 Error Classification

### 1. System-Level Errors

#### Infrastructure Failures
```python
class InfrastructureError(Exception):
    """Infrastructure-related errors"""
    pass

class DatabaseConnectionError(InfrastructureError):
    """Database connection failures"""
    def __init__(self, db_host, error_details):
        self.db_host = db_host
        self.error_details = error_details
        super().__init__(f"Database connection failed to {db_host}: {error_details}")

class KafkaConnectionError(InfrastructureError):
    """Kafka broker connection failures"""
    def __init__(self, broker_list, error_details):
        self.broker_list = broker_list
        self.error_details = error_details
        super().__init__(f"Kafka connection failed to {broker_list}: {error_details}")
```

#### Resource Exhaustion
```python
class ResourceExhaustionError(Exception):
    """Resource exhaustion errors"""
    pass

class OutOfMemoryError(ResourceExhaustionError):
    """Memory exhaustion"""
    def __init__(self, requested_mb, available_mb):
        self.requested_mb = requested_mb
        self.available_mb = available_mb
        super().__init__(
            f"Out of memory: requested {requested_mb}MB, available {available_mb}MB"
        )

class GPUMemoryError(ResourceExhaustionError):
    """GPU memory exhaustion"""
    def __init__(self, requested_mb, available_mb):
        self.requested_mb = requested_mb
        self.available_mb = available_mb
        super().__init__(
            f"GPU memory exhausted: requested {requested_mb}MB, available {available_mb}MB"
        )
```

### 2. Application-Level Errors

#### Processing Errors
```python
class ProcessingError(Exception):
    """Data processing errors"""
    pass

class EmbeddingProcessingError(ProcessingError):
    """Embedding generation failures"""
    def __init__(self, text_count, model_name, error_details):
        self.text_count = text_count
        self.model_name = model_name
        self.error_details = error_details
        super().__init__(
            f"Embedding processing failed for {text_count} texts using {model_name}: {error_details}"
        )

class ValidationError(ProcessingError):
    """Data validation failures"""
    def __init__(self, field_name, field_value, validation_rule):
        self.field_name = field_name
        self.field_value = field_value
        self.validation_rule = validation_rule
        super().__init__(
            f"Validation failed for {field_name}='{field_value}': {validation_rule}"
        )
```

#### Model Errors
```python
class ModelError(Exception):
    """ML model related errors"""
    pass

class ModelLoadingError(ModelError):
    """Model loading failures"""
    def __init__(self, model_name, model_path, error_details):
        self.model_name = model_name
        self.model_path = model_path
        self.error_details = error_details
        super().__init__(
            f"Failed to load model {model_name} from {model_path}: {error_details}"
        )

class ModelInferenceError(ModelError):
    """Model inference failures"""
    def __init__(self, model_name, input_size, error_details):
        self.model_name = model_name
        self.input_size = input_size
        self.error_details = error_details
        super().__init__(
            f"Model inference failed for {model_name} with input size {input_size}: {error_details}"
        )
```

## 🔄 Circuit Breaker Implementation

### Enhanced Circuit Breaker
```python
import time
import threading
from enum import Enum
from typing import Callable, Any, Optional
from dataclasses import dataclass
from loguru import logger

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    timeout: float = 30.0
    expected_exception: tuple = (Exception,)
    success_threshold: int = 3  # For half-open state

class AdvancedCircuitBreaker:
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.lock = threading.RLock()
        
        # Metrics
        self.total_calls = 0
        self.total_failures = 0
        self.total_timeouts = 0
        
    def call(self, func: Callable, *args, **kwargs) -> Any:
        with self.lock:
            self.total_calls += 1
            
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                    logger.info(f"Circuit breaker {self.name} moved to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker {self.name} is OPEN"
                    )
            
            try:
                # Execute with timeout
                result = self._execute_with_timeout(func, *args, **kwargs)
                self._on_success()
                return result
                
            except self.config.expected_exception as e:
                self._on_failure(e)
                raise
            except TimeoutError as e:
                self.total_timeouts += 1
                self._on_failure(e)
                raise
    
    def _execute_with_timeout(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with timeout"""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Function execution timed out after {self.config.timeout}s")
        
        # Set timeout
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(self.config.timeout))
        
        try:
            result = func(*args, **kwargs)
            signal.alarm(0)  # Cancel timeout
            return result
        finally:
            signal.signal(signal.SIGALRM, old_handler)
    
    def _on_success(self):
        """Handle successful execution"""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit breaker {self.name} moved to CLOSED")
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0
    
    def _on_failure(self, exception: Exception):
        """Handle failed execution"""
        self.total_failures += 1
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        logger.warning(
            f"Circuit breaker {self.name} failure {self.failure_count}/{self.config.failure_threshold}: {exception}"
        )
        
        if self.failure_count >= self.config.failure_threshold:
            self.state = CircuitState.OPEN
            logger.error(f"Circuit breaker {self.name} moved to OPEN")
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit breaker should attempt reset"""
        return (
            self.last_failure_time and
            time.time() - self.last_failure_time >= self.config.recovery_timeout
        )
    
    def get_metrics(self) -> dict:
        """Get circuit breaker metrics"""
        return {
            'name': self.name,
            'state': self.state.value,
            'total_calls': self.total_calls,
            'total_failures': self.total_failures,
            'total_timeouts': self.total_timeouts,
            'failure_rate': self.total_failures / max(self.total_calls, 1),
            'current_failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time
        }

class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open"""
    pass
```

## 🔧 Retry Mechanisms

### Exponential Backoff with Jitter
```python
import random
import time
from typing import Callable, Any, Type, Tuple

class RetryConfig:
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions

def retry_with_backoff(config: RetryConfig):
    """Decorator for retry with exponential backoff"""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts - 1:
                        logger.error(
                            f"Function {func.__name__} failed after {config.max_attempts} attempts: {e}"
                        )
                        raise
                    
                    # Calculate delay
                    delay = min(
                        config.base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    # Add jitter
                    if config.jitter:
                        delay *= (0.5 + random.random() * 0.5)
                    
                    logger.warning(
                        f"Function {func.__name__} attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    
                    time.sleep(delay)
            
            raise last_exception
        
        return wrapper
    return decorator

# Usage examples
@retry_with_backoff(RetryConfig(
    max_attempts=5,
    base_delay=1.0,
    max_delay=30.0,
    retryable_exceptions=(DatabaseConnectionError, TimeoutError)
))
def connect_to_database():
    # Database connection logic
    pass

@retry_with_backoff(RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    retryable_exceptions=(EmbeddingProcessingError,)
))
def process_embeddings(texts):
    # Embedding processing logic
    pass
```

## 🔄 Failure Recovery Strategies

### 1. Database Recovery
```python
class DatabaseRecoveryManager:
    def __init__(self, primary_db_url, replica_db_urls):
        self.primary_db_url = primary_db_url
        self.replica_db_urls = replica_db_urls
        self.current_db = primary_db_url
        self.failed_dbs = set()
        
    def get_healthy_connection(self):
        """Get connection to healthy database"""
        # Try primary first
        if self.primary_db_url not in self.failed_dbs:
            try:
                conn = self._test_connection(self.primary_db_url)
                self.current_db = self.primary_db_url
                return conn
            except Exception as e:
                logger.warning(f"Primary DB failed: {e}")
                self.failed_dbs.add(self.primary_db_url)
        
        # Try replicas
        for replica_url in self.replica_db_urls:
            if replica_url not in self.failed_dbs:
                try:
                    conn = self._test_connection(replica_url)
                    self.current_db = replica_url
                    logger.info(f"Switched to replica: {replica_url}")
                    return conn
                except Exception as e:
                    logger.warning(f"Replica {replica_url} failed: {e}")
                    self.failed_dbs.add(replica_url)
        
        raise DatabaseConnectionError("All databases failed", str(self.failed_dbs))
    
    def _test_connection(self, db_url):
        """Test database connection"""
        # Implementation specific to database type
        pass
    
    def recover_failed_connections(self):
        """Periodically test failed connections for recovery"""
        recovered = set()
        
        for db_url in self.failed_dbs.copy():
            try:
                self._test_connection(db_url)
                recovered.add(db_url)
                logger.info(f"Database recovered: {db_url}")
            except Exception:
                continue
        
        self.failed_dbs -= recovered
```

### 2. Kafka Recovery
```python
class KafkaRecoveryManager:
    def __init__(self, broker_list, consumer_config):
        self.broker_list = broker_list
        self.consumer_config = consumer_config
        self.consumer = None
        self.failed_brokers = set()
        
    def get_healthy_consumer(self):
        """Get consumer connected to healthy brokers"""
        healthy_brokers = [
            broker for broker in self.broker_list 
            if broker not in self.failed_brokers
        ]
        
        if not healthy_brokers:
            # Reset failed brokers and try again
            self.failed_brokers.clear()
            healthy_brokers = self.broker_list
        
        try:
            from kafka import KafkaConsumer
            self.consumer = KafkaConsumer(
                bootstrap_servers=healthy_brokers,
                **self.consumer_config
            )
            return self.consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise KafkaConnectionError(healthy_brokers, str(e))
    
    def handle_consumer_error(self, error):
        """Handle consumer errors and attempt recovery"""
        logger.error(f"Kafka consumer error: {error}")
        
        # Close current consumer
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None
        
        # Mark current brokers as failed
        self.failed_brokers.update(self.broker_list)
        
        # Wait before retry
        time.sleep(5)
        
        # Attempt to reconnect
        return self.get_healthy_consumer()
```

### 3. GPU Recovery
```python
class GPURecoveryManager:
    def __init__(self):
        self.gpu_available = self._check_gpu_availability()
        self.gpu_errors = 0
        self.max_gpu_errors = 3
        
    def execute_with_gpu_fallback(self, gpu_func, cpu_func, *args, **kwargs):
        """Execute function with GPU, fallback to CPU on failure"""
        if self.gpu_available and self.gpu_errors < self.max_gpu_errors:
            try:
                result = gpu_func(*args, **kwargs)
                self.gpu_errors = 0  # Reset error count on success
                return result
            except (GPUMemoryError, RuntimeError) as e:
                self.gpu_errors += 1
                logger.warning(
                    f"GPU execution failed ({self.gpu_errors}/{self.max_gpu_errors}): {e}"
                )
                
                if self.gpu_errors >= self.max_gpu_errors:
                    self.gpu_available = False
                    logger.error("GPU disabled due to repeated failures")
        
        # CPU fallback
        logger.info("Using CPU fallback")
        return cpu_func(*args, **kwargs)
    
    def _check_gpu_availability(self):
        """Check if GPU is available"""
        try:
            import torch
            return torch.cuda.is_available() or torch.backends.mps.is_available()
        except Exception:
            return False
    
    def attempt_gpu_recovery(self):
        """Attempt to recover GPU functionality"""
        if not self.gpu_available:
            try:
                # Clear GPU memory
                import torch
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                elif torch.backends.mps.is_available():
                    torch.mps.empty_cache()
                
                # Test GPU functionality
                if self._test_gpu_functionality():
                    self.gpu_available = True
                    self.gpu_errors = 0
                    logger.info("GPU functionality recovered")
                    
            except Exception as e:
                logger.warning(f"GPU recovery failed: {e}")
    
    def _test_gpu_functionality(self):
        """Test basic GPU functionality"""
        try:
            import torch
            if torch.cuda.is_available():
                x = torch.randn(10, 10).cuda()
                y = x @ x.T
                return True
            elif torch.backends.mps.is_available():
                x = torch.randn(10, 10).to('mps')
                y = x @ x.T
                return True
        except Exception:
            return False
        return False
```

## 📊 Error Monitoring and Alerting

### Error Metrics Collection
```python
class ErrorMetricsCollector:
    def __init__(self):
        self.error_counts = {}
        self.error_rates = {}
        self.last_errors = {}
        
    def record_error(self, error_type, error_message, component):
        """Record error occurrence"""
        key = f"{component}.{error_type}"
        
        # Update counts
        self.error_counts[key] = self.error_counts.get(key, 0) + 1
        
        # Store last error
        self.last_errors[key] = {
            'message': error_message,
            'timestamp': time.time(),
            'count': self.error_counts[key]
        }
        
        # Calculate error rate (errors per minute)
        self._calculate_error_rate(key)
        
        # Check if alert should be triggered
        self._check_alert_thresholds(key)
    
    def _calculate_error_rate(self, key):
        """Calculate error rate for the last minute"""
        current_time = time.time()
        window_start = current_time - 60  # 1 minute window
        
        # This is simplified - in practice, you'd use a time-series database
        recent_errors = self.error_counts.get(key, 0)
        self.error_rates[key] = recent_errors / 60  # errors per second
    
    def _check_alert_thresholds(self, key):
        """Check if error thresholds are exceeded"""
        error_rate = self.error_rates.get(key, 0)
        error_count = self.error_counts.get(key, 0)
        
        # Define thresholds
        thresholds = {
            'critical': {'rate': 10, 'count': 100},
            'warning': {'rate': 5, 'count': 50},
            'info': {'rate': 1, 'count': 10}
        }
        
        for severity, limits in thresholds.items():
            if error_rate >= limits['rate'] or error_count >= limits['count']:
                self._send_alert(key, severity, error_rate, error_count)
                break
    
    def _send_alert(self, key, severity, error_rate, error_count):
        """Send alert notification"""
        alert_message = (
            f"[{severity.upper()}] Error threshold exceeded for {key}: "
            f"Rate: {error_rate:.2f}/sec, Count: {error_count}"
        )
        
        logger.error(alert_message)
        
        # In practice, send to alerting system (Slack, PagerDuty, etc.)
        # self.alert_manager.send_alert(alert_message, severity)
```

### Health Check System
```python
class HealthCheckManager:
    def __init__(self):
        self.health_checks = {}
        self.check_results = {}
        
    def register_health_check(self, name, check_func, interval=30):
        """Register a health check function"""
        self.health_checks[name] = {
            'func': check_func,
            'interval': interval,
            'last_check': 0,
            'status': 'unknown'
        }
    
    def run_health_checks(self):
        """Run all registered health checks"""
        current_time = time.time()
        
        for name, check_info in self.health_checks.items():
            if current_time - check_info['last_check'] >= check_info['interval']:
                try:
                    result = check_info['func']()
                    self.check_results[name] = {
                        'status': 'healthy' if result else 'unhealthy',
                        'timestamp': current_time,
                        'details': result if isinstance(result, dict) else {}
                    }
                    check_info['status'] = 'healthy' if result else 'unhealthy'
                except Exception as e:
                    self.check_results[name] = {
                        'status': 'error',
                        'timestamp': current_time,
                        'error': str(e)
                    }
                    check_info['status'] = 'error'
                
                check_info['last_check'] = current_time
    
    def get_system_health(self):
        """Get overall system health status"""
        if not self.check_results:
            return {'status': 'unknown', 'checks': {}}
        
        healthy_checks = sum(
            1 for result in self.check_results.values() 
            if result['status'] == 'healthy'
        )
        total_checks = len(self.check_results)
        
        if healthy_checks == total_checks:
            overall_status = 'healthy'
        elif healthy_checks > total_checks / 2:
            overall_status = 'degraded'
        else:
            overall_status = 'unhealthy'
        
        return {
            'status': overall_status,
            'healthy_checks': healthy_checks,
            'total_checks': total_checks,
            'checks': self.check_results
        }

# Health check functions
def check_database_health():
    """Check database connectivity"""
    try:
        # Test database connection
        # conn = get_db_connection()
        # conn.execute("SELECT 1")
        return True
    except Exception:
        return False

def check_kafka_health():
    """Check Kafka connectivity"""
    try:
        # Test Kafka connection
        # producer = get_kafka_producer()
        # producer.send('health-check', b'ping')
        return True
    except Exception:
        return False

def check_gpu_health():
    """Check GPU availability"""
    try:
        import torch
        if torch.cuda.is_available():
            # Test GPU operation
            x = torch.randn(10, 10).cuda()
            y = x @ x.T
            return {'gpu_type': 'cuda', 'memory_gb': torch.cuda.get_device_properties(0).total_memory / 1024**3}
        elif torch.backends.mps.is_available():
            x = torch.randn(10, 10).to('mps')
            y = x @ x.T
            return {'gpu_type': 'mps'}
        else:
            return False
    except Exception:
        return False
```

## 🎯 Error Handling Best Practices

### 1. Graceful Degradation
```python
class GracefulDegradationManager:
    def __init__(self):
        self.degradation_levels = {
            'full': {'gpu': True, 'batch_size': 1000, 'workers': 8},
            'reduced': {'gpu': True, 'batch_size': 500, 'workers': 4},
            'minimal': {'gpu': False, 'batch_size': 100, 'workers': 2},
            'emergency': {'gpu': False, 'batch_size': 50, 'workers': 1}
        }
        self.current_level = 'full'
        
    def degrade_performance(self, error_type):
        """Degrade performance based on error type"""
        degradation_map = {
            GPUMemoryError: 'reduced',
            OutOfMemoryError: 'minimal',
            DatabaseConnectionError: 'emergency'
        }
        
        target_level = degradation_map.get(type(error_type), 'minimal')
        
        if self._should_degrade(target_level):
            self.current_level = target_level
            logger.warning(f"Performance degraded to {target_level} level")
            return self.degradation_levels[target_level]
        
        return self.degradation_levels[self.current_level]
    
    def _should_degrade(self, target_level):
        """Check if degradation is necessary"""
        level_order = ['emergency', 'minimal', 'reduced', 'full']
        current_index = level_order.index(self.current_level)
        target_index = level_order.index(target_level)
        
        return target_index < current_index
```

### 2. Error Context Preservation
```python
class ErrorContext:
    def __init__(self):
        self.context_stack = []
        
    def add_context(self, **kwargs):
        """Add context information"""
        self.context_stack.append(kwargs)
        
    def get_full_context(self):
        """Get complete error context"""
        full_context = {}
        for context in self.context_stack:
            full_context.update(context)
        return full_context
    
    def clear_context(self):
        """Clear context stack"""
        self.context_stack.clear()

# Context manager for error handling
class ErrorContextManager:
    def __init__(self, **context):
        self.context = context
        self.error_context = ErrorContext()
        
    def __enter__(self):
        self.error_context.add_context(**self.context)
        return self.error_context
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            # Enhance exception with context
            context = self.error_context.get_full_context()
            enhanced_message = f"{exc_val}\nContext: {context}"
            
            # Log enhanced error
            logger.error(enhanced_message)
            
            # Optionally re-raise with enhanced message
            # raise exc_type(enhanced_message) from exc_val
        
        self.error_context.clear_context()
        return False  # Don't suppress exceptions

# Usage example
with ErrorContextManager(component='embedding_processor', batch_id='batch_123'):
    with ErrorContextManager(operation='model_loading', model_name='all-MiniLM-L6-v2'):
        # Code that might fail
        load_model()
```

## 📋 Error Handling Checklist

### Development Phase
- [ ] Define custom exception hierarchy
- [ ] Implement circuit breakers for external dependencies
- [ ] Add retry mechanisms with exponential backoff
- [ ] Create comprehensive error logging
- [ ] Implement graceful degradation strategies
- [ ] Add health check endpoints
- [ ] Create error context preservation
- [ ] Implement fallback mechanisms

### Testing Phase
- [ ] Test all error scenarios
- [ ] Verify circuit breaker functionality
- [ ] Test retry mechanisms
- [ ] Validate fallback strategies
- [ ] Test resource exhaustion scenarios
- [ ] Verify error alerting
- [ ] Test recovery mechanisms
- [ ] Load test error handling

### Production Phase
- [ ] Monitor error rates and patterns
- [ ] Set up alerting thresholds
- [ ] Regular health check monitoring
- [ ] Error trend analysis
- [ ] Performance impact assessment
- [ ] Recovery time measurement
- [ ] Post-incident reviews
- [ ] Continuous improvement

## 🎯 Sonuç ve Öneriler

### Immediate Implementation
1. **Circuit Breakers**: Implement for all external dependencies
2. **Retry Logic**: Add exponential backoff for transient failures
3. **Health Checks**: Create comprehensive health monitoring
4. **Error Classification**: Standardize error types and handling

### Advanced Features
1. **Predictive Error Detection**: ML-based anomaly detection
2. **Auto-Recovery**: Automated recovery procedures
3. **Error Impact Analysis**: Understand error propagation
4. **Chaos Engineering**: Proactive failure testing

### Monitoring and Alerting
1. **Error Rate Monitoring**: Track error trends
2. **Alert Fatigue Prevention**: Smart alerting rules
3. **Error Budget**: SLI/SLO-based error management
4. **Root Cause Analysis**: Automated error correlation

---

*Bu dokümantasyon sürekli güncellenmekte ve yeni hata senaryoları keşfedildikçe genişletilmektedir.*