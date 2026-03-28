"""
pipeline/orchestrator.py
=========================
Top-level wiring. Called by main.py.

Execution model:
  1. Verify DB connection
  2. Create / verify all warehouse tables (DDL)
  3. BatchIngestor.run(date)   — blocking, completes fully before stream starts
  4. Load known IDs into ReferentialValidator
  5. StreamWatcher.start()     — spawns background daemon thread
  6. Main thread stays alive (join / sleep loop) until SIGINT / KeyboardInterrupt
  7. On shutdown: StreamWatcher.stop(), final quality summary logged

Usage:
    from pipeline.orchestrator import Pipeline
    pipeline = Pipeline()
    pipeline.run("2026-02-22")
"""

import signal
import time

from src.warehouse.connection        import get_connection, test_connection
from src.warehouse.schema_ddl        import create_all_tables
from src.ingestion.batch_ingestion   import BatchIngestor
from src.ingestion.stream_ingestion  import StreamWatcher
from src.validation.referential_validator import ReferentialValidator
from src.analytics.quality_metrics   import get_daily_summary, get_file_success_rate
from src.utils.alerter               import send_alert_async
from src.utils.logger                import get_logger

logger = get_logger(__name__)


class Pipeline:
    """
    Owns one long-lived DB connection shared between batch and stream phases.
    The connection is held open for the lifetime of the run.
    """

    def run(self, run_date: str) -> None:
        """
        Execute the full pipeline for run_date.
        Blocks until KeyboardInterrupt or all stream files are exhausted.
        """
        logger.info("Pipeline starting", extra={"run_date": run_date})

        # ── Step 1: DB connectivity check ────────────────────────────────────
        if not test_connection():
            logger.error("Cannot reach database — aborting pipeline")
            send_alert_async(
                subject="Pipeline startup failed",
                error_message="Database is unreachable at startup",
                context={"run_date": run_date},
            )
            raise SystemExit(1)

        with get_connection() as conn:
            # ── Step 2: Ensure warehouse schema exists ────────────────────────
            logger.info("Verifying warehouse schema")
            create_all_tables(conn)

            # ── Step 3: Batch ingestion (blocking) ────────────────────────────
            logger.info("Starting batch ingestion", extra={"run_date": run_date})
            batch_ingestor = BatchIngestor(conn)
            batch_summary  = batch_ingestor.run(run_date)
            logger.info("Batch ingestion finished", extra={**batch_summary})

            # ── Step 4: Load known IDs for FK validation ──────────────────────
            logger.info("Loading known IDs for referential validation")
            rv = ReferentialValidator(conn)
            rv.load_known_ids()

            # ── Step 5: Start stream watcher (background thread) ──────────────
            watcher = StreamWatcher(conn=conn, run_date=run_date, validator=rv)
            watcher.start()

            # ── Step 6: Keep main thread alive ────────────────────────────────
            logger.info(
                "Pipeline running — waiting for stream data",
                extra={"run_date": run_date},
            )
            try:
                self._keepalive(watcher)
            except KeyboardInterrupt:
                logger.info("Shutdown signal received")

            # ── Step 7: Clean shutdown ────────────────────────────────────────
            watcher.stop()
            self._log_final_summary(conn, run_date)

        logger.info("Pipeline exited cleanly", extra={"run_date": run_date})

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _keepalive(watcher: StreamWatcher) -> None:
        """
        Block the main thread while the stream watcher runs.
        Wakes every 10 seconds to check if the watcher is still alive.
        Exits if the watcher thread dies unexpectedly.
        """
        while True:
            time.sleep(10)
            if watcher._thread and not watcher._thread.is_alive():
                logger.warning("Stream watcher thread died — exiting keepalive")
                break

    @staticmethod
    def _log_final_summary(conn, run_date: str) -> None:
        """Log a final quality summary for the day."""
        try:
            success_rate = get_file_success_rate(conn, run_date)
            rows = get_daily_summary(conn, run_date)
            total_records  = sum(r.get("total_records",  0) for r in rows)
            total_rejected = sum(r.get("rejected_records", 0) for r in rows)
            total_orphans  = sum(r.get("orphan_count",   0) for r in rows)

            logger.info(
                "Pipeline daily summary",
                extra={
                    "run_date":          run_date,
                    "file_success_rate": success_rate,
                    "total_records":     total_records,
                    "total_rejected":    total_rejected,
                    "total_orphans":     total_orphans,
                    "quality_log_rows":  len(rows),
                },
            )
        except Exception as exc:
            logger.warning("Could not log final summary", extra={"error": str(exc)})
