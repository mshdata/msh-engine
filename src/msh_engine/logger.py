"""
Logger module for msh-engine.

Provides a simple logger that writes to .msh/logs/ directory.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

# Log rotation constants
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5


def setup_logger(log_dir: Optional[str] = None, enable_file_logging: bool = True) -> logging.Logger:
    """
    Setup logger for msh-engine.
    
    Args:
        log_dir: Optional directory for log files. Defaults to .msh/logs/
        enable_file_logging: Whether to write to log files. Defaults to True.
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("msh_engine")
    logger.setLevel(logging.DEBUG)
    
    # Prevent duplicate handlers if logger is already configured
    if logger.handlers:
        return logger
    
    # Console handler (always enabled)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    if enable_file_logging:
        if log_dir is None:
            # Try to find .msh/logs/ in current directory or parent
            cwd = os.getcwd()
            log_dir = os.path.join(cwd, ".msh", "logs")
            
            # If not found, try parent directory (common when running from subdirectory)
            if not os.path.exists(os.path.dirname(log_dir)):
                parent_dir = os.path.dirname(cwd)
                parent_log_dir = os.path.join(parent_dir, ".msh", "logs")
                if os.path.exists(os.path.dirname(parent_log_dir)):
                    log_dir = parent_log_dir
        
        # Create logs directory if it doesn't exist
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        # File handler (rotates at configured size, keeps configured backups)
        log_file_path = os.path.join(log_dir, "msh_engine.log")
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Error-only file handler
        error_log_path = os.path.join(log_dir, "msh_engine.error.log")
        error_handler = RotatingFileHandler(
            error_log_path,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        logger.addHandler(error_handler)
    
    return logger


# Create default logger instance
logger = setup_logger()

