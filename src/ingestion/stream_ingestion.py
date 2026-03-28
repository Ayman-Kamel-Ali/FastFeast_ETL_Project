"""
ingestion/stream_ingestion.py
==============================
Continuously polls stream/YYYY-MM-DD/HH/ directories for new files,
processes them in the correct order (orders → tickets → ticket_events),
and loads valid rows into the warehouse fact tables.

Key behaviours:
  - Polls every stream_poll_interval_sec (configurable, default 30s)
  - Skips already-processed files (file_tracker)
  - Processes files within a slot in strict order:
      orders.json  →  tickets.csv  →  ticket_events.json
  - After tickets are loaded → triggers SLA refresh
  - After each file → persists quality metrics
  - Orphans are quarantined, never block the pipeline
  - Runs as a daemon thread — stops when the main thread exits

Usage (called by orchestrator.py):
    from src.ingestion.stream_ingestion import StreamWatcher
    watcher = StreamWatcher(conn, run_date="2026-02-22", validator=rv)
    watcher.start()   # spawns background thread
    watcher.stop()    # signals the thread to exit cleanly
"""

import time
import threading
from pathlib import Path
from typing import Optional

import pandas as pd

from src.ingestion.file_reader       import read_csv, read_json
from src.ingestion.file_tracker      import FileTracker
from src.validation.schema_validator import validate_schema
from src.validation.data_validator   import validate_data
from src.validation.pii_handler      import mask_pii
from src.validation.referential_validator import ReferentialValidator
from src.warehouse.loader            import upsert_by_source_name
from src.analytics.sla_calculator   import refresh_sla
from src.analytics.quality_metrics  import persist_metrics
from src.utils.quarantine           import quarantine_rows
from src.utils.alerter              import send_alert_async
from src.utils.logger               import get_logger

logger = get_logger(__name__)

# Files to process per hourly slot — ORDER IS MANDATORY
_STREAM_FILES = [
    ("orders",         "orders.json",         "json"),
    ("tickets",        "tickets.csv",         "csv"),
    ("ticket_events",  "ticket_events.json",  "json"),
]


class StreamWatcher:
    """
    Background daemon that polls the stream directory and processes new files.
    """

    def __init__(
        self,
        conn,
        run_date:  str,
        validator: ReferentialValidator,
    ):
        """
        Args:
            conn      : Open psycopg2 connection — long-lived, managed by orchestrator.
            run_date  : "YYYY-MM-DD" — which day's stream directory to watch.
            validator : Pre-loaded ReferentialValidator instance.
        """
        self._conn     = conn
        self._run_date = run_date
        self._rv       = validator
        self._tracker  = FileTracker()
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None

        try:
            from config.settings import settings
            self._stream_dir = settings.paths.stream_dir
            self._poll_sec   = int(settings.pipeline.stream_poll_interval_sec)
        except Exception:
            self._stream_dir = "data/input/stream"
            self._poll_sec   = 30

        self._root = Path(__file__).resolve().parent.parent.parent

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Spawn the watcher as a daemon background thread."""
        self._thread = threading.Thread(
            target   = self._watch_loop,
            name     = f"stream-watcher-{self._run_date}",
            daemon   = True,
        )
        self._thread.start()
        logger.info(
            "Stream watcher started",
            extra={"run_date": self._run_date, "poll_sec": self._poll_sec},
        )

    def stop(self) -> None:
        """Signal the watcher thread to stop after its current poll cycle."""
        self._stop_evt.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._poll_sec + 5)
        logger.info("Stream watcher stopped", extra={"run_date": self._run_date})

    def run_once(self) -> dict:
        """
        Run a single scan of all stream slots synchronously.
        Used by orchestrator for direct (non-threaded) invocation if needed.
        """
        return self._scan_all_slots()

    # ── Background loop ────────────────────────────────────────────────────────

    def _watch_loop(self) -> None:
        """Poll indefinitely until stop() is called."""
        logger.info("Stream watch loop running", extra={"run_date": self._run_date})
        while not self._stop_evt.is_set():
            try:
                self._scan_all_slots()
            except Exception as exc:
                # Loop must never crash — log and keep going
                logger.error(
                    "Unexpected error in stream watch loop",
                    extra={"error": str(exc)},
                )
            self._stop_evt.wait(timeout=self._poll_sec)

        logger.info("Stream watch loop exiting", extra={"run_date": self._run_date})

    # ── Slot scanning ──────────────────────────────────────────────────────────

    def _scan_all_slots(self) -> dict:
        """
        Scan all HH/ subdirectories under stream/YYYY-MM-DD/ and
        process any new files found.
        """
        stream_day_dir = self._root / self._stream_dir / self._run_date

        if not stream_day_dir.exists():
            return {}

        # Collect all HH/ dirs that exist, sorted ascending (process oldest first)
        slots = sorted(
            [d for d in stream_day_dir.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )

        total_summary: dict = {}
        tickets_loaded_this_scan = False

        for slot_dir in slots:
            slot_summary = self._process_slot(slot_dir)
            for k, v in slot_summary.items():
                total_summary[k] = total_summary.get(k, 0) + v
            if slot_summary.get("tickets_loaded", 0) > 0:
                tickets_loaded_this_scan = True

        # Refresh SLA after any ticket loads
        if tickets_loaded_this_scan:
            try:
                refresh_sla(self._conn, self._run_date)
            except Exception as exc:
                logger.error("SLA refresh failed", extra={"error": str(exc)})

        return total_summary

    def _process_slot(self, slot_dir: Path) -> dict:
        """
        Process one hourly slot directory.
        Files are always processed in the fixed order:
          orders → tickets → ticket_events
        """
        summary = {
            "orders_loaded":  0,
            "tickets_loaded": 0,
            "events_loaded":  0,
        }

        for source_name, filename, fmt in _STREAM_FILES:
            file_path = slot_dir / filename
            if not file_path.exists():
                continue
            if self._tracker.is_processed(file_path):
                continue

            rows = self._process_file(file_path, source_name, fmt, slot_dir.name)

            if source_name == "orders":
                summary["orders_loaded"]  += rows
                # Refresh order ID cache so tickets can reference newly loaded orders
                self._rv.refresh_order_ids()

            elif source_name == "tickets":
                summary["tickets_loaded"] += rows
                # Refresh ticket ID cache so events can reference them
                self._rv.refresh_ticket_ids()

            elif source_name == "ticket_events":
                summary["events_loaded"]  += rows

        return summary

    # ── Per-file pipeline ──────────────────────────────────────────────────────

    def _process_file(
        self,
        file_path:   Path,
        source_name: str,
        fmt:         str,
        hour_slot:   str,
    ) -> int:
        """
        Full validation + load pipeline for a single stream file.
        Returns number of rows loaded (0 on any failure).
        Never raises.
        """
        t_start = time.time()

        # ── Read ─────────────────────────────────────────────────────────────
        df, read_err = (read_json if fmt == "json" else read_csv)(file_path)

        if read_err:
            logger.error(
                "Stream file read failed",
                extra={"file": str(file_path), "error": read_err},
            )
            send_alert_async(
                subject=f"Stream parse error: {file_path.name}",
                error_message=read_err,
                context={"table": source_name, "slot": hour_slot,
                         "run_date": self._run_date, "file": str(file_path)},
            )
            self._tracker.mark_done(file_path, status="failed")
            return 0

        if df.empty:
            logger.info("Stream file empty — skipping", extra={"file": str(file_path)})
            self._tracker.mark_done(file_path, status="skipped")
            return 0

        # ── Schema validate ───────────────────────────────────────────────────
        df, schema_err = validate_schema(df, source_name)

        if schema_err:
            logger.error(
                "Stream schema validation failed",
                extra={"file": str(file_path), "error": schema_err},
            )
            send_alert_async(
                subject=f"Stream schema failed: {file_path.name}",
                error_message=schema_err,
                context={"table": source_name, "slot": hour_slot,
                         "run_date": self._run_date},
            )
            self._tracker.mark_done(file_path, status="failed")
            return 0

        # ── Data validate ─────────────────────────────────────────────────────
        valid_df, rejected_df, val_stats = validate_data(df, source_name)

        if not rejected_df.empty:
            quarantine_rows(
                rejected_df,
                table_name  = source_name,
                reason      = "data_validation",
                run_date    = self._run_date,
                source_file = str(file_path),
            )

        if valid_df.empty:
            self._tracker.mark_done(file_path, status="skipped",
                                    rows_rejected=len(rejected_df))
            self._write_metrics(source_name, file_path, val_stats,
                                orphan_stats={}, latency_ms=0, status="skipped")
            return 0

        # ── Referential integrity ─────────────────────────────────────────────
        valid_df, orphan_df, orphan_stats = self._check_referential(
            valid_df, source_name, file_path
        )

        if not orphan_df.empty:
            quarantine_rows(
                orphan_df,
                table_name  = source_name,
                reason      = "orphan_reference",
                run_date    = self._run_date,
                source_file = str(file_path),
            )

        if valid_df.empty:
            self._tracker.mark_done(file_path, status="skipped",
                                    rows_rejected=len(orphan_df))
            self._write_metrics(source_name, file_path, val_stats,
                                orphan_stats, latency_ms=0, status="skipped")
            return 0

        # ── PII mask ──────────────────────────────────────────────────────────
        masked_df = mask_pii(valid_df, source_name)

        # ── Upsert ───────────────────────────────────────────────────────────
        try:
            rows_loaded = upsert_by_source_name(self._conn, masked_df, source_name)
        except Exception as exc:
            logger.error(
                "Stream upsert failed",
                extra={"table": source_name, "file": str(file_path), "error": str(exc)},
            )
            send_alert_async(
                subject=f"Stream DB write failed: {source_name}",
                error_message=str(exc),
                context={"table": source_name, "slot": hour_slot,
                         "run_date": self._run_date},
            )
            self._tracker.mark_done(file_path, status="failed")
            return 0

        latency_ms = int((time.time() - t_start) * 1000)

        # ── Metrics + tracker ─────────────────────────────────────────────────
        self._write_metrics(
            source_name, file_path,
            {**val_stats,
             "valid_records":    rows_loaded,
             "rejected_records": len(rejected_df) + len(orphan_df)},
            orphan_stats, latency_ms, "success",
        )

        self._tracker.mark_done(
            file_path,
            status        = "success",
            rows_loaded   = rows_loaded,
            rows_rejected = len(rejected_df) + (len(orphan_df) if not orphan_df.empty else 0),
        )

        logger.info(
            "Stream file processed",
            extra={
                "table":      source_name,
                "slot":       hour_slot,
                "loaded":     rows_loaded,
                "rejected":   len(rejected_df),
                "orphans":    orphan_stats.get("orphan_count", 0),
                "latency_ms": latency_ms,
            },
        )
        return rows_loaded

    # ── Referential check dispatcher ───────────────────────────────────────────

    def _check_referential(
        self,
        df:          pd.DataFrame,
        source_name: str,
        file_path:   Path,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """Dispatch to the correct ReferentialValidator method."""
        if source_name == "orders":
            return self._rv.check_orders(
                df, run_date=self._run_date, source_file=str(file_path)
            )
        elif source_name == "tickets":
            return self._rv.check_tickets(
                df, run_date=self._run_date, source_file=str(file_path)
            )
        elif source_name == "ticket_events":
            return self._rv.check_events(
                df, run_date=self._run_date, source_file=str(file_path)
            )
        # Other tables don't need FK checks
        return df, pd.DataFrame(), {}

    # ── Quality metrics helper ─────────────────────────────────────────────────

    def _write_metrics(
        self,
        source_name:  str,
        file_path:    Path,
        val_stats:    dict,
        orphan_stats: dict,
        latency_ms:   int,
        status:       str,
    ) -> None:
        try:
            merged = {
                **val_stats,
                "orphan_count":           orphan_stats.get("orphan_count", 0),
                "orphan_rate":            orphan_stats.get("orphan_rate",  0.0),
                "processing_latency_ms":  latency_ms,
                "status":                 status,
            }
            persist_metrics(
                conn        = self._conn,
                run_date    = self._run_date,
                table_name  = source_name,
                stats       = merged,
                source_file = str(file_path),
            )
        except Exception as exc:
            logger.warning(
                "Stream quality metrics persist failed",
                extra={"table": source_name, "error": str(exc)},
            )
