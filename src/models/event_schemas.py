#!/usr/bin/env python3
"""
Pydantic Event Schema Validation Models

Bu modül VectorStream sistemi için event schema validation sağlar.
JSON event'lerin doğruluğunu ve tutarlılığını garanti eder.
"""

from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator, root_validator
from loguru import logger
import json


class EventType(str, Enum):
    """Event türleri"""
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"
    ERROR_EVENT = "error_event"
    PERFORMANCE_METRIC = "performance_metric"
    EMBEDDING_REQUEST = "embedding_request"
    EMBEDDING_RESPONSE = "embedding_response"


class Priority(str, Enum):
    """Event öncelik seviyeleri"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class BaseEvent(BaseModel):
    """Temel event modeli"""
    
    event_id: str = Field(..., description="Unique event identifier")
    event_type: EventType = Field(..., description="Type of the event")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    source: str = Field(..., description="Event source system/component")
    priority: Priority = Field(default=Priority.MEDIUM, description="Event priority level")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    @validator('event_id')
    @classmethod
    def validate_event_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('event_id cannot be empty')
        return v.strip()
    
    @validator('source')
    @classmethod
    def validate_source(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('source cannot be empty')
        return v.strip()
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class UserActionEvent(BaseEvent):
    """Kullanıcı aksiyonu event'i"""
    
    event_type: EventType = Field(default=EventType.USER_ACTION, const=True)
    user_id: str = Field(..., description="User identifier")
    action: str = Field(..., description="Action performed by user")
    session_id: Optional[str] = Field(None, description="User session identifier")
    ip_address: Optional[str] = Field(None, description="User IP address")
    user_agent: Optional[str] = Field(None, description="User agent string")
    
    @validator('user_id')
    @classmethod
    def validate_user_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('user_id cannot be empty')
        return v.strip()
    
    @validator('action')
    @classmethod
    def validate_action(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('action cannot be empty')
        return v.strip()


class SystemEvent(BaseEvent):
    """Sistem event'i"""
    
    event_type: EventType = Field(default=EventType.SYSTEM_EVENT, const=True)
    component: str = Field(..., description="System component name")
    operation: str = Field(..., description="Operation performed")
    status: str = Field(..., description="Operation status")
    duration_ms: Optional[float] = Field(None, description="Operation duration in milliseconds")
    resource_usage: Optional[Dict[str, Any]] = Field(None, description="Resource usage metrics")
    
    @validator('component')
    @classmethod
    def validate_component(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('component cannot be empty')
        return v.strip()
    
    @validator('operation')
    @classmethod
    def validate_operation(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('operation cannot be empty')
        return v.strip()
    
    @validator('duration_ms')
    @classmethod
    def validate_duration(cls, v):
        if v is not None and v < 0:
            raise ValueError('duration_ms cannot be negative')
        return v


class ErrorEvent(BaseEvent):
    """Hata event'i"""
    
    event_type: EventType = Field(default=EventType.ERROR_EVENT, const=True)
    error_code: str = Field(..., description="Error code")
    error_message: str = Field(..., description="Error message")
    stack_trace: Optional[str] = Field(None, description="Stack trace")
    component: str = Field(..., description="Component where error occurred")
    severity: Priority = Field(default=Priority.HIGH, description="Error severity")
    
    @validator('error_code')
    @classmethod
    def validate_error_code(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('error_code cannot be empty')
        return v.strip()
    
    @validator('error_message')
    @classmethod
    def validate_error_message(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('error_message cannot be empty')
        return v.strip()


class PerformanceMetricEvent(BaseEvent):
    """Performans metrik event'i"""
    
    event_type: EventType = Field(default=EventType.PERFORMANCE_METRIC, const=True)
    metric_name: str = Field(..., description="Metric name")
    metric_value: float = Field(..., description="Metric value")
    metric_unit: str = Field(..., description="Metric unit (e.g., 'ms', 'MB', 'count')")
    component: str = Field(..., description="Component being measured")
    tags: Dict[str, str] = Field(default_factory=dict, description="Additional metric tags")
    
    @validator('metric_name')
    @classmethod
    def validate_metric_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('metric_name cannot be empty')
        return v.strip()
    
    @validator('metric_unit')
    @classmethod
    def validate_metric_unit(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('metric_unit cannot be empty')
        return v.strip()


class EmbeddingRequestEvent(BaseEvent):
    """Embedding request event'i"""
    
    event_type: EventType = Field(default=EventType.EMBEDDING_REQUEST, const=True)
    request_id: str = Field(..., description="Request identifier")
    text_count: int = Field(..., description="Number of texts to process")
    model_name: str = Field(..., description="Embedding model name")
    batch_size: Optional[int] = Field(None, description="Batch size for processing")
    use_gpu: bool = Field(default=False, description="Whether GPU acceleration is used")
    
    @validator('request_id')
    @classmethod
    def validate_request_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('request_id cannot be empty')
        return v.strip()
    
    @validator('text_count')
    @classmethod
    def validate_text_count(cls, v):
        if v <= 0:
            raise ValueError('text_count must be positive')
        return v
    
    @validator('model_name')
    @classmethod
    def validate_model_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('model_name cannot be empty')
        return v.strip()


class EmbeddingResponseEvent(BaseEvent):
    """Embedding response event'i"""
    
    event_type: EventType = Field(default=EventType.EMBEDDING_RESPONSE, const=True)
    request_id: str = Field(..., description="Request identifier")
    success: bool = Field(..., description="Whether request was successful")
    processing_time_ms: float = Field(..., description="Processing time in milliseconds")
    embeddings_count: int = Field(..., description="Number of embeddings generated")
    vector_dimension: int = Field(..., description="Embedding vector dimension")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    gpu_used: bool = Field(default=False, description="Whether GPU was used")
    memory_usage_mb: Optional[float] = Field(None, description="Memory usage in MB")
    
    @validator('request_id')
    @classmethod
    def validate_request_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('request_id cannot be empty')
        return v.strip()
    
    @validator('processing_time_ms')
    @classmethod
    def validate_processing_time(cls, v):
        if v < 0:
            raise ValueError('processing_time_ms cannot be negative')
        return v
    
    @validator('embeddings_count')
    @classmethod
    def validate_embeddings_count(cls, v):
        if v < 0:
            raise ValueError('embeddings_count cannot be negative')
        return v
    
    @validator('vector_dimension')
    @classmethod
    def validate_vector_dimension(cls, v):
        if v <= 0:
            raise ValueError('vector_dimension must be positive')
        return v


class EventValidator:
    """Event validation utility class"""
    
    @staticmethod
    def validate_json_event(json_data: Union[str, Dict[str, Any]]) -> BaseEvent:
        """
        JSON event'i validate et ve uygun Pydantic model'e dönüştür
        
        Args:
            json_data: JSON string veya dict
            
        Returns:
            BaseEvent: Validate edilmiş event object
            
        Raises:
            ValueError: Validation hatası
        """
        try:
            # JSON string'i dict'e dönüştür
            if isinstance(json_data, str):
                data = json.loads(json_data)
            else:
                data = json_data
            
            # Event type'a göre uygun model'i seç
            event_type = data.get('event_type')
            
            if event_type == EventType.USER_ACTION:
                return UserActionEvent(**data)
            elif event_type == EventType.SYSTEM_EVENT:
                return SystemEvent(**data)
            elif event_type == EventType.ERROR_EVENT:
                return ErrorEvent(**data)
            elif event_type == EventType.PERFORMANCE_METRIC:
                return PerformanceMetricEvent(**data)
            elif event_type == EventType.EMBEDDING_REQUEST:
                return EmbeddingRequestEvent(**data)
            elif event_type == EventType.EMBEDDING_RESPONSE:
                return EmbeddingResponseEvent(**data)
            else:
                # Fallback to base event
                return BaseEvent(**data)
                
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
        except Exception as e:
            raise ValueError(f"Event validation failed: {e}")
    
    @staticmethod
    def validate_batch_events(json_events: List[Union[str, Dict[str, Any]]]) -> List[BaseEvent]:
        """
        Batch event'leri validate et
        
        Args:
            json_events: JSON event'lerin listesi
            
        Returns:
            List[BaseEvent]: Validate edilmiş event'ler
            
        Raises:
            ValueError: Validation hatası
        """
        validated_events = []
        errors = []
        
        for i, event_data in enumerate(json_events):
            try:
                validated_event = EventValidator.validate_json_event(event_data)
                validated_events.append(validated_event)
            except ValueError as e:
                errors.append(f"Event {i}: {e}")
        
        if errors:
            raise ValueError(f"Batch validation failed: {'; '.join(errors)}")
        
        return validated_events
    
    @staticmethod
    def create_sample_events() -> Dict[str, BaseEvent]:
        """
        Test için örnek event'ler oluştur
        
        Returns:
            Dict[str, BaseEvent]: Örnek event'ler
        """
        return {
            'user_action': UserActionEvent(
                event_id="ua_001",
                user_id="user_123",
                action="search_query",
                source="web_app",
                session_id="sess_456",
                metadata={"query": "machine learning"}
            ),
            'system_event': SystemEvent(
                event_id="sys_001",
                component="embedding_processor",
                operation="batch_processing",
                status="completed",
                source="core_system",
                duration_ms=1250.5,
                resource_usage={"cpu_percent": 75.2, "memory_mb": 512}
            ),
            'error_event': ErrorEvent(
                event_id="err_001",
                error_code="EMB_001",
                error_message="GPU memory allocation failed",
                component="rapids_gpu_processor",
                source="gpu_system",
                severity=Priority.HIGH,
                stack_trace="Traceback..."
            ),
            'performance_metric': PerformanceMetricEvent(
                event_id="perf_001",
                metric_name="throughput",
                metric_value=101.57,
                metric_unit="msg/sec",
                component="embedding_processor",
                source="performance_monitor",
                tags={"test_type": "throughput", "workers": "8"}
            ),
            'embedding_request': EmbeddingRequestEvent(
                event_id="req_001",
                request_id="emb_req_123",
                text_count=1000,
                model_name="all-MiniLM-L6-v2",
                source="api_gateway",
                batch_size=200,
                use_gpu=True
            ),
            'embedding_response': EmbeddingResponseEvent(
                event_id="resp_001",
                request_id="emb_req_123",
                success=True,
                processing_time_ms=9850.2,
                embeddings_count=1000,
                vector_dimension=384,
                source="embedding_processor",
                gpu_used=True,
                memory_usage_mb=2048.5
            )
        }


if __name__ == "__main__":
    # Test event validation
    logger.info("Testing event schema validation...")
    
    # Create sample events
    sample_events = EventValidator.create_sample_events()
    
    for event_name, event in sample_events.items():
        logger.info(f"✅ {event_name}: {event.json()}")
    
    # Test JSON validation
    json_event = {
        "event_id": "test_001",
        "event_type": "user_action",
        "user_id": "test_user",
        "action": "test_action",
        "source": "test_system"
    }
    
    validated = EventValidator.validate_json_event(json_event)
    logger.info(f"✅ JSON validation successful: {validated.json()}")
    
    logger.info("Event schema validation tests completed!")