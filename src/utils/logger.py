"""Centralised logging configuration using loguru."""

import os
import sys
from pathlib import Path

from loguru import logger as _root_logger

# Loguru's Logger type is not exported directly; use the bound type.
from loguru._logger import Logger  # type: ignore[import]

_configured = False
_LOG_DIR = Path("logs")


def _configure_logger() -> None:
    """Set up handlers once for the process lifetime."""
    global _configured
    if _configured:
        return

    _root_logger.remove()

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{extra[name]}</cyan> | "
        "<level>{message}</level>"
    )

    # Console handler
    _root_logger.add(sys.stderr, format=fmt, level=level, colorize=True)

    # File handler — rotates at 10 MB, keeps 7 days
    _LOG_DIR.mkdir(exist_ok=True)
    _root_logger.add(
        _LOG_DIR / "pipeline.log",
        format=fmt,
        level=level,
        rotation="10 MB",
        retention="7 days",
        colorize=False,
        enqueue=True,  # thread-safe writes
    )

    _configured = True


def get_logger(name: str) -> Logger:
    """Return a loguru logger bound to *name*.

    Args:
        name: Logical name for the logger (e.g. ``__name__``).

    Returns:
        A loguru Logger instance with ``name`` injected into every record.
    """
    _configure_logger()
    return _root_logger.bind(name=name)
