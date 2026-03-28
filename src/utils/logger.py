"""
utils/logger.py
===============
Structured JSON rotating file logger.
Every log line is a valid JSON object — easy to ingest into any log aggregator.

Usage:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Batch loaded", extra={"table": "customers", "rows": 500})
    logger.error("Schema fail", extra={"file": "orders.json", "missing": ["order_id"]})
"""

import logging
import json
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path


class _JsonFormatter(logging.Formatter):
    """
    Formats every log record as a single JSON line:
    {
        "timestamp": "2026-02-22T08:15:30.123456+00:00",
        "level":     "INFO",
        "module":    "src.ingestion.batch_ingestion",
        "message":   "Loaded customers.csv",
        "table":     "customers",
        "rows":      500
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }

        # Attach any extra kwargs passed via extra={...}
        _skip = {
            "name", "msg", "args", "levelname", "levelno", "pathname",
            "filename", "module", "exc_info", "exc_text", "stack_info",
            "lineno", "funcName", "created", "msecs", "relativeCreated",
            "thread", "threadName", "processName", "process", "message",
            "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in _skip:
                log_obj[key] = value

        # Append exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj, default=str, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger backed by a rotating JSON file handler.
    Calling get_logger with the same name returns the same logger (standard Python behaviour).
    """
    logger = logging.getLogger(name)

    # Don't add handlers more than once (important when module is imported multiple times)
    if logger.handlers:
        return logger

    # ── Import settings here (not at module level) to avoid circular imports ──
    try:
        from config.settings import settings
        log_dir = settings.logging and getattr(settings, "paths", None) and settings.paths.log_dir
        if not log_dir:
            log_dir = "logs"
        level_str = getattr(settings.logging, "level", "INFO")
        max_bytes = getattr(settings.logging, "max_bytes", 10_485_760)   # 10 MB
        backup_count = getattr(settings.logging, "backup_count", 5)
    except Exception:
        # Fallback defaults so logger works even before settings are loaded
        log_dir = "logs"
        level_str = "INFO"
        max_bytes = 10_485_760
        backup_count = 5

    # Resolve log directory relative to project root (where main.py lives)
    project_root = Path(__file__).resolve().parent.parent.parent
    log_path = project_root / log_dir
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / "pipeline.log"

    level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(level)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate output)
    logger.propagate = False

    return logger
