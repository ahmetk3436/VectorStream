"""Embedding-related exceptions."""

from typing import Optional, Any, Dict


class EmbeddingError(Exception):
    """Base exception for embedding-related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class EmbeddingProcessingError(EmbeddingError):
    """Exception raised when embedding processing fails."""
    
    def __init__(self, message: str, file_path: Optional[str] = None, 
                 batch_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)
        self.file_path = file_path
        self.batch_id = batch_id


class EmbeddingModelError(EmbeddingError):
    """Exception raised when embedding model fails."""
    pass


class EmbeddingValidationError(EmbeddingError):
    """Exception raised when embedding validation fails."""
    pass


class EmbeddingStorageError(EmbeddingError):
    """Exception raised when embedding storage fails."""
    pass