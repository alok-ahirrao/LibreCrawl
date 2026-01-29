"""
GMB Core Logger
Centralized logging with environment-based modes.

Modes (set via LOG_MODE env variable):
- 'debug': Verbose output for development
- 'info': Standard output (default)
- 'production': Errors only, minimal output
"""
import logging
import sys
from .config import config


def setup_logger(name: str = 'gmb_core') -> logging.Logger:
    """
    Create and configure a logger based on LOG_MODE environment variable.
    
    Usage:
        from .logger import log
        log.debug("This only shows in debug mode")
        log.info("This shows in debug and info modes")
        log.error("This always shows")
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Set level based on LOG_MODE
    mode = config.LOG_MODE
    
    if mode == 'debug':
        logger.setLevel(logging.DEBUG)
    elif mode == 'production':
        logger.setLevel(logging.ERROR)
    else:  # 'info' or default
        logger.setLevel(logging.INFO)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)  # Let logger level control what's shown
    
    # Create formatter based on mode
    if mode == 'debug':
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        formatter = logging.Formatter('[%(levelname)s] %(message)s')
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


# Create default logger instance
log = setup_logger()


# Convenience functions that match existing print patterns
def debug(msg: str):
    """Log debug message (only in debug mode)."""
    log.debug(msg)


def info(msg: str):
    """Log info message (debug and info modes)."""
    log.info(msg)


def warn(msg: str):
    """Log warning message."""
    log.warning(msg)


def error(msg: str):
    """Log error message (always shown)."""
    log.error(msg)
