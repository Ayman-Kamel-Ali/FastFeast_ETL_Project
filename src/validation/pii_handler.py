"""
validation/pii_handler.py
=========================
Masks Personally Identifiable Information (PII) before any data reaches the warehouse.
Uses SHA-256 hashing: deterministic (same value → same hash), irreversible.

Columns masked per table:
  customers    : email → email_hash,  phone → phone_hash
  drivers      : driver_phone → phone_hash,  national_id → national_id_hash
  agents       : agent_email → email_hash,   agent_phone → phone_hash

The original columns are DROPPED and replaced with hashed versions.
Raw PII is never logged, never stored in the warehouse.

Usage:
    from src.validation.pii_handler import mask_pii
    masked_df = mask_pii(df, "customers")
"""

import hashlib
import pandas as pd
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── PII column mapping ─────────────────────────────────────────────────────────
# Format: { table_name: [ (source_col, hashed_col_name), ... ] }

_PII_MAP: dict[str, list[tuple[str, str]]] = {
    "customers": [
        ("email", "email_hash"),
        ("phone", "phone_hash"),
    ],
    "drivers": [
        ("driver_phone", "phone_hash"),
        ("national_id",  "national_id_hash"),
    ],
    "agents": [
        ("agent_email", "email_hash"),
        ("agent_phone", "phone_hash"),
    ],
}


def _sha256(value) -> Optional[str]:
    """
    Return SHA-256 hex digest of the string representation of value.
    Returns None if value is null/empty (preserves nullability).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def mask_pii(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Hash all PII columns for the given table.
    Returns a new DataFrame — the input is never modified.

    For tables with no PII (e.g. lookup tables), returns the DataFrame unchanged.
    """
    if df.empty:
        return df

    mappings = _PII_MAP.get(table_name)
    if not mappings:
        return df  # no PII in this table

    result = df.copy()
    masked_cols = []

    for src_col, dst_col in mappings:
        if src_col not in result.columns:
            logger.warning(
                "PII column not found in DataFrame — skipping",
                extra={"table": table_name, "column": src_col},
            )
            continue

        result[dst_col] = result[src_col].apply(_sha256)
        result = result.drop(columns=[src_col])
        masked_cols.append(f"{src_col} → {dst_col}")

    if masked_cols:
        logger.info(
            "PII masked",
            extra={"table": table_name, "masked": masked_cols},
        )

    return result
