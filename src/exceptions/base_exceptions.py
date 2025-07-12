class NewMindAIException(Exception):
    def __init__(self, message: str, error_code: str = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

class ConfigurationError(NewMindAIException):
    """Configuration related errors"""
    pass

class DataProcessingError(NewMindAIException):
    """Data processing related errors"""
    pass