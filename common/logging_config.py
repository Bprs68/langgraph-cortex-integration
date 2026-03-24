"""Centralized logging configuration."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler

_INITIALIZED = False


def setup_logging(
    level: int = logging.INFO,
    log_file: str = "cortex_langgraph.log",
) -> None:
    """Configure root logger with console + rotating file handlers.

    Safe to call multiple times — subsequent calls are no-ops.
    """
    global _INITIALIZED
    if _INITIALIZED:
        return
    _INITIALIZED = True

    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    root = logging.getLogger()
    root.setLevel(level)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Rotating file handler (5 MB, keep 3 backups)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Quiet noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
