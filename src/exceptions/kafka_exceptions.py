from .base_exceptions import NewMindAIException

class KafkaConnectionError(NewMindAIException):
    """Kafka connection errors"""
    pass

class KafkaConsumerError(NewMindAIException):
    """Kafka consumer errors"""
    pass

class MessageDeserializationError(NewMindAIException):
    """Message deserialization errors"""
    pass