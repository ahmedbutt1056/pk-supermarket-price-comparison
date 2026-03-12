"""
logger_setup.py — Sets up logging for the entire pipeline.
Logs go to both console and a log file.
A separate scraping_attempts log records EVERY HTTP request made by scrapers.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path


def _get_log_dir():
    """Return the default log directory, creating it if needed."""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def get_logger(name, log_dir=None):
    """
    Create a logger that writes to console AND a log file.
    
    Args:
        name: Name for the logger (usually module name)
        log_dir: Directory to save log files (optional)
    
    Returns:
        A configured logger object
    """
    logger = logging.getLogger(name)
    
    # Don't add handlers if they already exist
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # Format: timestamp - name - level - message
    formatter = logging.Formatter(
        "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # --- Console handler (shows INFO and above) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # --- File handler (shows DEBUG and above) ---
    if log_dir is None:
        log_dir = _get_log_dir()
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{today}.log"
    
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


def get_attempt_logger():
    """
    Return a dedicated logger that records EVERY scraping attempt.
    Each line logs: timestamp, store, URL, HTTP method, status code,
    response time, success/failure, error details.

    This satisfies the assignment requirement:
        "All scraping attempts must be logged."

    Writes to: logs/scraping_attempts_YYYY-MM-DD.log
    """
    name = "scraping_attempts"
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    log_dir = _get_log_dir()
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"scraping_attempts_{today}.log"

    # Structured format: easy to parse, easy to read
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Also show on console (WARNING+ only, to avoid flooding)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.WARNING)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # Write header on first creation
    logger.info("=" * 100)
    logger.info("SCRAPING ATTEMPTS LOG — Every HTTP request is recorded here")
    logger.info(f"Session started at {datetime.now().isoformat()}")
    logger.info("Fields: STORE | METHOD | URL | TRY | STATUS | TIME_MS | RESULT | DETAILS")
    logger.info("=" * 100)

    return logger
