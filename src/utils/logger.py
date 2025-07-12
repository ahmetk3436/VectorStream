from loguru import logger
import sys
from pathlib import Path

def setup_logger(config):
    """Logger'ı yapılandır"""
    # Mevcut logger'ları temizle
    logger.remove()
    
    # Console logger
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        level=config.get('level', 'INFO')
    )
    
    # File logger
    log_path = Path("logs")
    log_path.mkdir(exist_ok=True)
    
    logger.add(
        log_path / "app.log",
        rotation="10 MB",
        retention="7 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        level=config.get('level', 'INFO')
    )
    
    return logger