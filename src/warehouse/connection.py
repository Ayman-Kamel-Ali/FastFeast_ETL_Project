"""
warehouse/connection.py
=======================
PostgreSQL connection management with retry logic.
Provides a context manager for safe, auto-closing connections.

Usage:
    from src.warehouse.connection import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")

    # Connection is automatically closed / returned after the with block.
    # On DB error inside the block: transaction is rolled back, connection closed.
"""

import time
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Generator

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _get_db_config() -> dict:
    """Pull DB settings from config. Returns a plain dict for psycopg2.connect()."""
    try:
        from config.settings import settings
        return {
            "host":     settings.database.host,
            "port":     int(settings.database.port),
            "dbname":   settings.database.name,
            "user":     settings.database.user,
            "password": settings.database.password,
        }
    except Exception as exc:
        raise RuntimeError(f"Could not load database config: {exc}") from exc


def _get_retry_config() -> tuple[int, float]:
    """Return (max_retries, delay_seconds) from config."""
    try:
        from config.settings import settings
        retries = int(settings.database.connect_retries)
        delay   = float(settings.database.connect_retry_delay_sec)
        return retries, delay
    except Exception:
        return 3, 2.0


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager that yields an open psycopg2 connection.

    - Retries up to connect_retries times with connect_retry_delay_sec between attempts.
    - On success: commits on clean exit, rolls back on exception.
    - Always closes the connection when the with-block exits.
    - Raises RuntimeError if all retry attempts are exhausted.

    Example:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO ...")
        # auto-committed here
    """
    cfg            = _get_db_config()
    max_retries, delay = _get_retry_config()

    conn = None
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**cfg)
            # Use DictCursor by default so rows behave like dicts
            conn.cursor_factory = psycopg2.extras.RealDictCursor
            logger.info(
                "DB connection established",
                extra={"host": cfg["host"], "db": cfg["dbname"], "attempt": attempt},
            )
            break

        except psycopg2.OperationalError as exc:
            last_error = exc
            logger.warning(
                "DB connection failed — retrying",
                extra={
                    "attempt": attempt,
                    "max": max_retries,
                    "error": str(exc),
                    "retry_in_sec": delay if attempt < max_retries else 0,
                },
            )
            if attempt < max_retries:
                time.sleep(delay)

    if conn is None:
        from src.utils.alerter import send_alert_async
        send_alert_async(
            subject="Database connection failed",
            error_message=str(last_error),
            context={"host": cfg["host"], "db": cfg["dbname"], "attempts": max_retries},
        )
        raise RuntimeError(
            f"Could not connect to PostgreSQL after {max_retries} attempts: {last_error}"
        )

    try:
        yield conn
        conn.commit()
        logger.info("DB transaction committed")

    except Exception as exc:
        conn.rollback()
        logger.error("DB transaction rolled back", extra={"error": str(exc)})
        raise

    finally:
        conn.close()
        logger.info("DB connection closed")


def test_connection() -> bool:
    """
    Quick connectivity check. Returns True if the DB is reachable, False otherwise.
    Does NOT raise — safe to call at startup.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception as exc:
        logger.error("DB connectivity test failed", extra={"error": str(exc)})
        return False
