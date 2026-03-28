"""
utils/quarantine.py
===================
Writes rejected/invalid rows to timestamped CSV files under data/quarantine/.

Structure:
    data/quarantine/
    └── YYYY-MM-DD/
        ├── customers_null_violation_08-15-30.csv
        ├── orders_orphan_reference_09-00-01.csv
        └── tickets_invalid_format_09-00-01.csv

Each output file contains the original columns PLUS:
    rejection_reason    : str   — why the row was rejected
    rejection_timestamp : str   — ISO timestamp of quarantine write
    source_file         : str   — original file path that produced this row

Usage:
    from src.utils.quarantine import quarantine_rows
    quarantine_rows(bad_df, table_name="orders", reason="orphan_reference",
                    run_date="2026-02-22", source_file="stream/2026-02-22/09/orders.json")
"""

import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def quarantine_rows(
    df: pd.DataFrame,
    table_name: str,
    reason: str,
    run_date: str,
    source_file: Optional[str] = None,
) -> None:
    """
    Write a DataFrame of rejected rows to the quarantine directory.

    Args:
        df          : DataFrame containing the rows to quarantine.
                      Can be empty — function exits silently in that case.
        table_name  : Name of the source table (e.g. "customers", "orders").
        reason      : Short rejection reason tag, used in filename.
                      Use underscores, e.g. "null_violation", "orphan_reference",
                      "invalid_format", "schema_error".
        run_date    : Date string "YYYY-MM-DD" — determines quarantine subdirectory.
        source_file : Original file path for traceability (optional).
    """
    if df is None or df.empty:
        return

    try:
        from config.settings import settings
        quarantine_base = settings.paths.quarantine_dir
    except Exception:
        quarantine_base = "data/quarantine"

    # Build directory: data/quarantine/YYYY-MM-DD/
    project_root = Path(__file__).resolve().parent.parent.parent
    out_dir = project_root / quarantine_base / run_date
    out_dir.mkdir(parents=True, exist_ok=True)

    # Timestamped filename to avoid overwrites within the same day
    ts = datetime.now(tz=timezone.utc).strftime("%H-%M-%S")
    filename = f"{table_name}_{reason}_{ts}.csv"
    out_path = out_dir / filename

    # Annotate rows before writing
    annotated = df.copy()
    annotated["rejection_reason"] = reason
    annotated["rejection_timestamp"] = datetime.now(tz=timezone.utc).isoformat()
    if source_file:
        annotated["source_file"] = str(source_file)

    annotated.to_csv(out_path, index=False)

    logger.info(
        "Rows quarantined",
        extra={
            "table": table_name,
            "reason": reason,
            "count": len(df),
            "path": str(out_path),
        },
    )
