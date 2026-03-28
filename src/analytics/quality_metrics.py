"""
analytics/quality_metrics.py
==============================
Persists data quality statistics to the quality_log table after every
batch or stream file is processed.

One row per file per run. The quality_log table becomes the audit trail
that answers: "How healthy was our data on any given day?"

Usage:
    from src.analytics.quality_metrics import persist_metrics

    persist_metrics(
        conn       = conn,
        run_date   = "2026-02-22",
        table_name = "customers",
        source_file= "data/input/batch/2026-02-22/customers.csv",
        stats      = {
            "total":              510,
            "valid_records":      498,
            "rejected_records":   12,
            "duplicate_count":    10,
            "null_violations":    2,
            "invalid_format_count": 8,
            "orphan_count":       0,
            "orphan_rate":        0.0,
            "processing_latency_ms": 340,
            "status":             "success",
        }
    )
"""

from datetime import datetime, timezone
from typing import Optional

import psycopg2
from src.utils.logger import get_logger

logger = get_logger(__name__)


def persist_metrics(
    conn,
    run_date: str,
    table_name: str,
    stats: dict,
    source_file: Optional[str] = None,
) -> None:
    """
    Insert one quality_log row for the given file/table/run combination.

    Args:
        conn        : Open psycopg2 connection (caller manages transaction).
        run_date    : "YYYY-MM-DD" string.
        table_name  : Source table name (e.g. "customers", "orders").
        stats       : Dict with quality counts — see keys below.
                      Missing keys default to 0 / "success".
        source_file : Original file path for traceability.
    """
    # Normalise incoming stats — accept both data_validator and custom keys
    total      = stats.get("total",              stats.get("total_records",        0))
    valid      = stats.get("valid_records",      total - stats.get("total_rejected", 0))
    rejected   = stats.get("rejected_records",   stats.get("total_rejected",        0))
    dupes      = stats.get("duplicate_count",    stats.get("duplicates_removed",    0))
    nulls      = stats.get("null_violation_count", stats.get("null_violations",      0))
    fmt_errors = stats.get("invalid_format_count",
                            stats.get("invalid_email", 0)
                          + stats.get("invalid_phone", 0)
                          + stats.get("range_violations", 0)
                          + stats.get("date_violations", 0))
    orphans    = stats.get("orphan_count",        0)
    orphan_rate= stats.get("orphan_rate",         0.0)
    latency    = stats.get("processing_latency_ms", 0)
    status     = stats.get("status",             "success")

    sql = """
        INSERT INTO quality_log (
            run_date, run_timestamp, table_name, source_file,
            total_records, valid_records, rejected_records,
            duplicate_count, null_violation_count, invalid_format_count,
            orphan_count, orphan_rate, processing_latency_ms, status
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    values = (
        run_date,
        datetime.now(tz=timezone.utc),
        table_name,
        str(source_file) if source_file else None,
        total, valid, rejected,
        dupes, nulls, fmt_errors,
        orphans, orphan_rate, latency, status,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()
        logger.info(
            "Quality metrics persisted",
            extra={
                "table": table_name,
                "total": total,
                "valid": valid,
                "rejected": rejected,
                "orphan_rate": orphan_rate,
                "latency_ms": latency,
                "status": status,
            },
        )
    except Exception as exc:
        logger.error(
            "Failed to persist quality metrics",
            extra={"table": table_name, "error": str(exc)},
        )
        # Non-fatal — pipeline continues even if quality logging fails


def get_daily_summary(conn, run_date: str) -> list[dict]:
    """
    Retrieve all quality_log rows for a given date.
    Used by pdf_report.py to build the daily quality report.
    """
    sql = """
        SELECT *
        FROM quality_log
        WHERE run_date = %s
        ORDER BY run_timestamp
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (run_date,))
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.error(
            "Failed to fetch quality summary",
            extra={"run_date": run_date, "error": str(exc)},
        )
        return []


def get_file_success_rate(conn, run_date: str) -> float:
    """
    Return the proportion of files processed without error on a given date.
    Returns 1.0 if no records exist yet.
    """
    sql = """
        SELECT
            COUNT(*)                                       AS total,
            SUM(CASE WHEN status = 'success' THEN 1 END)  AS successful
        FROM quality_log
        WHERE run_date = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (run_date,))
            row = cur.fetchone()
        total      = row["total"]      if row else 0
        successful = row["successful"] if row else 0
        if not total:
            return 1.0
        return round((successful or 0) / total, 4)
    except Exception as exc:
        logger.error("Failed to compute file success rate", extra={"error": str(exc)})
        return 0.0
