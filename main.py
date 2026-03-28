"""
main.py
=======
FastFeast Pipeline entry point.

Usage:
    # Run pipeline for a specific date
    python main.py --date 2026-02-22

    # Run and immediately exit after batch (no stream watching)
    python main.py --date 2026-02-22 --batch-only

    # Verify DB connection and schema, then exit
    python main.py --check

    # Generate a PDF quality report for a past date (bonus)
    python main.py --date 2026-02-22 --report-only
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).resolve().parent))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FastFeast near real-time micro-batch data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        type=str,
        help='Run date in YYYY-MM-DD format (e.g. --date 2026-02-22)',
    )
    parser.add_argument(
        "--batch-only",
        action="store_true",
        help="Process batch files only — do not start stream watcher",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Test DB connection and verify schema, then exit",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Generate PDF quality report for --date without running the pipeline",
    )
    return parser.parse_args()


def validate_date(date_str: str) -> str:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        print(f"[ERROR] Invalid date format: '{date_str}'. Use YYYY-MM-DD.")
        sys.exit(1)


def cmd_check() -> None:
    """Test DB connection and schema."""
    from src.warehouse.connection import test_connection, get_connection
    from src.warehouse.schema_ddl import create_all_tables
    from src.utils.logger import get_logger
    log = get_logger(__name__)

    print("Testing database connection ...")
    if not test_connection():
        print("[FAIL] Cannot reach database. Check pipeline_config.yaml and .env")
        sys.exit(1)

    print("[OK] Database reachable.")
    print("Verifying schema ...")
    with get_connection() as conn:
        create_all_tables(conn)
    print("[OK] All warehouse tables created / verified.")
    log.info("Startup check passed")


def cmd_batch_only(run_date: str) -> None:
    """Run batch ingestion only."""
    from src.warehouse.connection import get_connection
    from src.warehouse.schema_ddl import create_all_tables
    from src.ingestion.batch_ingestion import BatchIngestor
    from src.utils.logger import get_logger
    log = get_logger(__name__)

    log.info("Batch-only run started", extra={"run_date": run_date})
    with get_connection() as conn:
        create_all_tables(conn)
        ingestor = BatchIngestor(conn)
        summary  = ingestor.run(run_date)
    print(f"Batch complete: {summary}")
    log.info("Batch-only run complete", extra={"run_date": run_date, **summary})


def cmd_report_only(run_date: str) -> None:
    """Generate PDF report for a past date."""
    try:
        from src.reporting.pdf_report import generate_report
        from src.warehouse.connection import get_connection
        with get_connection() as conn:
            generate_report(conn, run_date)
        print(f"PDF report generated for {run_date}")
    except ImportError:
        print("[INFO] reporting/pdf_report.py not yet implemented.")


def cmd_run(run_date: str) -> None:
    """Full pipeline run (batch + continuous stream)."""
    from pipeline.orchestrator import Pipeline
    pipeline = Pipeline()
    pipeline.run(run_date)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    if args.check:
        cmd_check()
        sys.exit(0)

    if not args.date:
        print("[ERROR] --date is required unless using --check")
        sys.exit(1)

    run_date = validate_date(args.date)

    if args.report_only:
        cmd_report_only(run_date)
        sys.exit(0)

    if args.batch_only:
        cmd_batch_only(run_date)
        sys.exit(0)

    cmd_run(run_date)
