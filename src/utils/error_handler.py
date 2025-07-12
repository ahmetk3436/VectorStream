import functools
from typing import Callable, Any
from src.utils.logger import Logger
from src.exceptions.base_exceptions import NewMindAIException

logger = Logger().get_logger()

def handle_errors(retry_count: int = 0, fallback_value: Any = None):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retry_count + 1):
                try:
                    return func(*args, **kwargs)
                except NewMindAIException as e:
                    logger.error(f"Custom error in {func.__name__}: {e.message}")
                    if attempt == retry_count:
                        if fallback_value is not None:
                            return fallback_value
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    if attempt == retry_count:
                        if fallback_value is not None:
                            return fallback_value
                        raise NewMindAIException(f"Unexpected error: {str(e)}")
                
                if attempt < retry_count:
                    logger.info(f"Retrying {func.__name__} (attempt {attempt + 2})")
            
        return wrapper
    return decorator