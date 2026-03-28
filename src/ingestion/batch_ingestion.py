"""
ingestion/batch_ingestion.py
=============================
Processes the daily batch snapshot for a given date.

Reads all files from:   data/input/batch/YYYY-MM-DD/
Loads into:             PostgreSQL dim_* tables

Processing order matters — lookup tables must be loaded before entity tables
because entity tables hold FK references to lookup tables.

Order:
  Pass 1 — pure lookup tables (no FKs to other batch tables):
    cities, regions, segments, categories, teams,
    reason_categories, reasons, channels, priorities

  Pass 2 — entity tables (reference lookup tables):
    customers, restaurants, drivers, agents

For each file:
  1. Check file_tracker  → skip if already processed
  2. Read CSV / JSON
  3. Schema validate      → skip file on failure, alert
  4. Data validate        → drop bad rows, quarantine
  5. PII mask
  6. Upsert to warehouse
  7. Record quality_metrics
  8. Mark file as done in tracker

After all files processed:
  Returns a dict of known IDs (customer_ids, driver_ids, etc.)
  so stream_ingestion can use them for FK checks without re-querying.
"""

import time
from pathlib import Path
from typing import Optional

import pandas as pd

from src.ingestion.file_reader  import read_csv, read_json
from src.ingestion.file_tracker import FileTracker
from src.validation.schema_validator import validate_schema
from src.validation.data_validator   import validate_data
from src.validation.pii_handler      import mask_pii
from src.warehouse.loader            import upsert_by_source_name
from src.analytics.quality_metrics   import persist_metrics
from src.utils.quarantine            import quarantine_rows
from src.utils.alerter               import send_alert_async
from src.utils.logger                import get_logger

logger = get_logger(__name__)

# ── File manifest ──────────────────────────────────────────────────────────────
# (source_name, filename, format)  in load order
_BATCH_FILES = [
    # Pass 1 — lookup / reference tables
    ("cities",            "cities.json",            "json"),
    ("regions",           "regions.csv",             "csv"),
    ("segments",          "segments.csv",            "csv"),
    ("categories",        "categories.csv",          "csv"),
    ("teams",             "teams.csv",               "csv"),
    ("reason_categories", "reason_categories.csv",   "csv"),
    ("reasons",           "reasons.csv",             "csv"),
    ("channels",          "channels.csv",            "csv"),
    ("priorities",        "priorities.csv",          "csv"),
    # Pass 2 — entity tables
    ("customers",         "customers.csv",           "csv"),
    ("restaurants",       "restaurants.json",        "json"),
    ("drivers",           "drivers.csv",             "csv"),
    ("agents",            "agents.csv",              "csv"),
]


class BatchIngestor:
    """
    Orchestrates the full batch ingestion for one calendar date.
    Instantiate once per run; call run(date).
    """

    def __init__(self, conn):
        """
        Args:
            conn : Open psycopg2 connection — owned by the caller.
        """
        self._conn    = conn
        self._tracker = FileTracker()

        try:
            from config.settings import settings
            self._batch_dir     = settings.paths.batch_dir
            self._quarantine_dir = settings.paths.quarantine_dir
        except Exception:
            self._batch_dir      = "data/input/batch"
            self._quarantine_dir = "data/quarantine"

        # Project root (where main.py lives)
        self._root = Path(__file__).resolve().parent.parent.parent

    # ── Public API ─────────────────────────────────────────────────────────────

    def run(self, run_date: str) -> dict:
        """
        Process all batch files for run_date.

        Returns:
            Summary dict:  {
                "files_attempted": int,
                "files_succeeded": int,
                "files_skipped":   int,
                "total_rows_loaded": int,
                "total_rows_rejected": int,
            }
        """
        batch_dir = self._root / self._batch_dir / run_date

        if not batch_dir.exists():
            logger.error(
                "Batch directory not found",
                extra={"path": str(batch_dir), "run_date": run_date},
            )
            send_alert_async(
                subject="Batch directory missing",
                error_message=f"Expected batch dir not found: {batch_dir}",
                context={"run_date": run_date},
            )
            return self._empty_summary()

        logger.info("Batch ingestion started", extra={"run_date": run_date, "dir": str(batch_dir)})

        summary = self._empty_summary()

        for source_name, filename, fmt in _BATCH_FILES:
            file_path = batch_dir / filename
            self._process_file(
                file_path   = file_path,
                source_name = source_name,
                fmt         = fmt,
                run_date    = run_date,
                summary     = summary,
            )

        logger.info(
            "Batch ingestion complete",
            extra={"run_date": run_date, **summary},
        )
        return summary

    # ── Private helpers ────────────────────────────────────────────────────────

    def _process_file(
        self,
        file_path:   Path,
        source_name: str,
        fmt:         str,
        run_date:    str,
        summary:     dict,
    ) -> None:
        """Full pipeline for a single batch file."""

        summary["files_attempted"] += 1
        t_start = time.time()

        # ── 1. Idempotency check ─────────────────────────────────────────────
        if self._tracker.is_processed(file_path):
            summary["files_skipped"] += 1
            return

        # ── 2. Read file ─────────────────────────────────────────────────────
        df, read_err = (read_json if fmt == "json" else read_csv)(file_path)

        if read_err:
            logger.error(
                "File read failed — skipping",
                extra={"file": str(file_path), "error": read_err},
            )
            send_alert_async(
                subject=f"File parse error: {file_path.name}",
                error_message=read_err,
                context={"table": source_name, "run_date": run_date, "file": str(file_path)},
            )
            self._tracker.mark_done(file_path, status="failed")
            return

        if df.empty:
            logger.warning("File is empty — skipping load", extra={"file": str(file_path)})
            self._tracker.mark_done(file_path, status="skipped")
            summary["files_skipped"] += 1
            return

        # ── 3. Schema validation ─────────────────────────────────────────────
        df, schema_err = validate_schema(df, source_name)

        if schema_err:
            logger.error(
                "Schema validation failed — skipping file",
                extra={"file": str(file_path), "table": source_name, "error": schema_err},
            )
            send_alert_async(
                subject=f"Schema validation failed: {file_path.name}",
                error_message=schema_err,
                context={"table": source_name, "run_date": run_date, "file": str(file_path)},
            )
            self._tracker.mark_done(file_path, status="failed")
            self._record_metrics(run_date, source_name, file_path, {}, "failed")
            return

        # ── 4. Data validation ───────────────────────────────────────────────
        valid_df, rejected_df, val_stats = validate_data(df, source_name)

        if not rejected_df.empty:
            quarantine_rows(
                rejected_df,
                table_name  = source_name,
                reason      = "data_validation",
                run_date    = run_date,
                source_file = str(file_path),
            )

        if valid_df.empty:
            logger.warning(
                "All rows rejected after validation — nothing to load",
                extra={"file": str(file_path), "table": source_name},
            )
            self._tracker.mark_done(file_path, status="skipped",
                                    rows_rejected=len(rejected_df))
            self._record_metrics(run_date, source_name, file_path,
                                 val_stats, "skipped")
            summary["files_skipped"] += 1
            return

        # ── 5. PII masking ───────────────────────────────────────────────────
        masked_df = mask_pii(valid_df, source_name)

        # ── 6. Upsert to warehouse ───────────────────────────────────────────
        try:
            rows_loaded = upsert_by_source_name(self._conn, masked_df, source_name)
        except Exception as exc:
            logger.error(
                "Upsert failed",
                extra={"table": source_name, "file": str(file_path), "error": str(exc)},
            )
            send_alert_async(
                subject=f"DB write failed: {source_name}",
                error_message=str(exc),
                context={"table": source_name, "run_date": run_date, "file": str(file_path)},
            )
            self._tracker.mark_done(file_path, status="failed",
                                    rows_rejected=len(rejected_df))
            # Do NOT record quality metrics — load didn't complete
            return

        latency_ms = int((time.time() - t_start) * 1000)

        # ── 7. Quality metrics ───────────────────────────────────────────────
        merged_stats = {
            **val_stats,
            "valid_records":        rows_loaded,
            "rejected_records":     len(rejected_df),
            "processing_latency_ms": latency_ms,
            "status":               "success",
        }
        self._record_metrics(run_date, source_name, file_path, merged_stats, "success")

        # ── 8. Mark done ─────────────────────────────────────────────────────
        self._tracker.mark_done(
            file_path,
            status        = "success",
            rows_loaded   = rows_loaded,
            rows_rejected = len(rejected_df),
        )

        summary["files_succeeded"]   += 1
        summary["total_rows_loaded"] += rows_loaded
        summary["total_rows_rejected"] += len(rejected_df)

        logger.info(
            "Batch file processed",
            extra={
                "table":     source_name,
                "file":      file_path.name,
                "loaded":    rows_loaded,
                "rejected":  len(rejected_df),
                "latency_ms": latency_ms,
            },
        )

    def _record_metrics(
        self,
        run_date:    str,
        table_name:  str,
        file_path:   Path,
        stats:       dict,
        status:      str,
    ) -> None:
        """Write a quality_log row — never raises."""
        try:
            persist_metrics(
                conn        = self._conn,
                run_date    = run_date,
                table_name  = table_name,
                stats       = {**stats, "status": status},
                source_file = str(file_path),
            )
        except Exception as exc:
            logger.warning(
                "Quality metrics persist failed",
                extra={"table": table_name, "error": str(exc)},
            )

    @staticmethod
    def _empty_summary() -> dict:
        return {
            "files_attempted":    0,
            "files_succeeded":    0,
            "files_skipped":      0,
            "total_rows_loaded":  0,
            "total_rows_rejected": 0,
        }
