"""
validation/data_validator.py
=============================
Row-level business rule validation.
Runs AFTER schema_validator (so we know all required columns exist).

Checks applied (in order):
  1. Duplicate detection     — deduplicate on PK, keep first
  2. Null enforcement        — drop rows where non_nullable fields are null
  3. Email format            — Egyptian/general regex
  4. Phone format            — Egyptian mobile regex (01X XXXXXXXX)
  5. Numeric range checks    — rating 1–5, rates 0–1, amounts >= 0, etc.
  6. Date validity           — drop rows where required date cols are NaT after coercion

Returns (valid_df, rejected_df, stats_dict) so the caller can:
  - Load valid_df into the warehouse
  - Quarantine rejected_df
  - Record stats_dict in quality_log

Usage:
    from src.validation.data_validator import validate_data
    valid_df, rejected_df, stats = validate_data(df, "customers")
"""

import re
import pandas as pd
from typing import Optional

from src.utils.logger import get_logger
from src.validation.schema_validator import SCHEMA_REGISTRY

logger = get_logger(__name__)

# ── Regex patterns ─────────────────────────────────────────────────────────────

# Standard email — user@domain.tld  (no spaces, valid TLD length)
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

# Egyptian mobile: 010 / 011 / 012 / 015 followed by exactly 8 digits
_PHONE_RE = re.compile(r"^01[0125]\d{8}$")

# ── Per-table numeric range rules ─────────────────────────────────────────────
# Format: { column_name: (min_value, max_value) }
# None means "no bound on that side"

_RANGE_RULES: dict[str, dict[str, tuple]] = {
    "customers":   {},   # no numeric range checks beyond non-null
    "restaurants": {
        "rating_avg":        (1.0, 5.0),
        "prep_time_avg_min": (1,   None),
    },
    "drivers": {
        "rating_avg":   (1.0, 5.0),
        "on_time_rate": (0.0, 1.0),
        "cancel_rate":  (0.0, 1.0),
        "completed_deliveries": (0, None),
    },
    "agents": {
        "resolution_rate": (0.0, 1.0),
        "csat_score":      (1.0, 5.0),
        "avg_handle_time_min": (1, None),
    },
    "reasons": {
        "severity_level":    (1, 5),
        "typical_refund_pct": (0.0, 1.0),
    },
    "orders": {
        "order_amount":    (0.0, None),
        "delivery_fee":    (0.0, None),
        "discount_amount": (0.0, None),
        "total_amount":    (0.0, None),
    },
    "tickets": {
        "refund_amount": (0.0, None),
    },
    "regions": {
        "delivery_base_fee": (0.0, None),
    },
}

# ── Per-table email / phone column names ──────────────────────────────────────

_EMAIL_COLS: dict[str, list[str]] = {
    "customers": ["email"],
    "agents":    ["agent_email"],
}

_PHONE_COLS: dict[str, list[str]] = {
    "customers": ["phone"],
    "drivers":   ["driver_phone"],
    "agents":    ["agent_phone"],
}

# ── Per-table required date columns (must not be NaT after coercion) ──────────

_REQUIRED_DATE_COLS: dict[str, list[str]] = {
    "orders":        ["order_created_at"],
    "tickets":       ["created_at"],
    "ticket_events": ["event_ts"],
}


# ── Main validator ─────────────────────────────────────────────────────────────

def validate_data(
    df: pd.DataFrame,
    table_name: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Apply all row-level business rules.

    Returns:
        valid_df    : rows that passed all checks
        rejected_df : rows that failed at least one check
                      (annotated with 'rejection_reason' column)
        stats       : {
            'total': int,
            'duplicates_removed': int,
            'null_violations': int,
            'invalid_email': int,
            'invalid_phone': int,
            'range_violations': int,
            'date_violations': int,
            'total_rejected': int,
        }
    """
    if df.empty:
        return df.copy(), pd.DataFrame(), _empty_stats()

    schema    = SCHEMA_REGISTRY.get(table_name, {})
    pk_cols   = _get_pk_cols(table_name)
    non_null  = schema.get("non_nullable", [])

    rejected_parts: list[pd.DataFrame] = []
    stats = _empty_stats()
    stats["total"] = len(df)

    working = df.copy()
    working["_rejection_reason"] = ""   # accumulates reasons for failed rows

    # ── 1. Deduplicate on PK ─────────────────────────────────────────────────
    if pk_cols:
        valid_pk_mask = working[pk_cols[0]].notna()
        before = len(working)
        working = working[valid_pk_mask].drop_duplicates(subset=pk_cols, keep="first")
        dupes = before - len(working)
        stats["duplicates_removed"] = dupes
        if dupes:
            logger.info(
                "Duplicates removed",
                extra={"table": table_name, "count": dupes},
            )

    # ── 2. Null enforcement ──────────────────────────────────────────────────
    if non_null:
        null_mask = working[non_null].isnull().any(axis=1)
        if null_mask.any():
            bad = working[null_mask].copy()
            bad["_rejection_reason"] = "null_violation"
            rejected_parts.append(bad)
            working = working[~null_mask]
            stats["null_violations"] = int(null_mask.sum())
            logger.warning(
                "Null violations",
                extra={"table": table_name, "count": stats["null_violations"],
                       "cols": non_null},
            )

    # ── 3. Email format ──────────────────────────────────────────────────────
    email_cols = _EMAIL_COLS.get(table_name, [])
    for col in email_cols:
        if col not in working.columns:
            continue
        # Only validate non-null values; null is handled by the null check above
        has_value = working[col].notna()
        bad_mask  = has_value & ~working[col].astype(str).str.match(_EMAIL_RE)
        if bad_mask.any():
            bad = working[bad_mask].copy()
            bad["_rejection_reason"] = f"invalid_email:{col}"
            rejected_parts.append(bad)
            working = working[~bad_mask]
            stats["invalid_email"] += int(bad_mask.sum())
            logger.warning(
                "Invalid email",
                extra={"table": table_name, "col": col, "count": int(bad_mask.sum())},
            )

    # ── 4. Phone format ──────────────────────────────────────────────────────
    phone_cols = _PHONE_COLS.get(table_name, [])
    for col in phone_cols:
        if col not in working.columns:
            continue
        has_value = working[col].notna()
        # Pandas reads all-numeric phone columns as int64, stripping the
        # leading zero (01012345678 → 1012345678).  Restore it by converting
        # to int then zero-padding to 11 digits before regex matching.
        def _normalize_phone(v):
            if pd.isna(v):
                return ""
            s = str(v).strip()
            # If pandas read it as a float (e.g. 1012345678.0), drop the .0
            if s.endswith(".0") and s[:-2].isdigit():
                s = s[:-2]
            # If it's all digits and 10 chars it lost its leading zero → pad
            if s.isdigit() and len(s) == 10:
                s = "0" + s
            return s
        normalized = working[col].apply(_normalize_phone)
        bad_mask   = has_value & ~normalized.str.match(_PHONE_RE)
        if bad_mask.any():
            bad = working[bad_mask].copy()
            bad["_rejection_reason"] = f"invalid_phone:{col}"
            rejected_parts.append(bad)
            working = working[~bad_mask]
            stats["invalid_phone"] += int(bad_mask.sum())
            logger.warning(
                "Invalid phone",
                extra={"table": table_name, "col": col, "count": int(bad_mask.sum())},
            )

    # ── 5. Numeric range checks ──────────────────────────────────────────────
    range_rules = _RANGE_RULES.get(table_name, {})
    for col, (lo, hi) in range_rules.items():
        if col not in working.columns:
            continue
        numeric_vals = pd.to_numeric(working[col], errors="coerce")
        bad_mask = pd.Series(False, index=working.index)
        if lo is not None:
            bad_mask |= numeric_vals < lo
        if hi is not None:
            bad_mask |= numeric_vals > hi
        # Also reject NaN that was produced by coercion (originally non-numeric)
        bad_mask |= numeric_vals.isna() & working[col].notna()

        if bad_mask.any():
            bad = working[bad_mask].copy()
            bad["_rejection_reason"] = f"range_violation:{col}"
            rejected_parts.append(bad)
            working = working[~bad_mask]
            stats["range_violations"] += int(bad_mask.sum())
            logger.warning(
                "Range violations",
                extra={"table": table_name, "col": col,
                       "count": int(bad_mask.sum()), "range": f"[{lo}, {hi}]"},
            )

    # ── 6. Required date columns must not be NaT ─────────────────────────────
    req_dates = _REQUIRED_DATE_COLS.get(table_name, [])
    for col in req_dates:
        if col not in working.columns:
            continue
        # At this point date cols are already coerced by schema_validator
        nat_mask = working[col].isna()
        if nat_mask.any():
            bad = working[nat_mask].copy()
            bad["_rejection_reason"] = f"invalid_date:{col}"
            rejected_parts.append(bad)
            working = working[~nat_mask]
            stats["date_violations"] += int(nat_mask.sum())
            logger.warning(
                "Invalid date in required column",
                extra={"table": table_name, "col": col, "count": int(nat_mask.sum())},
            )

    # ── Build rejected_df ────────────────────────────────────────────────────
    if rejected_parts:
        rejected_df = pd.concat(rejected_parts, ignore_index=True)
        # Rename internal tracking column to the public name
        rejected_df = rejected_df.rename(columns={"_rejection_reason": "rejection_reason"})
        # Drop the tracking column from valid rows too
        working = working.drop(columns=["_rejection_reason"], errors="ignore")
    else:
        rejected_df = pd.DataFrame()
        working = working.drop(columns=["_rejection_reason"], errors="ignore")

    stats["total_rejected"] = len(rejected_df)

    logger.info(
        "Data validation complete",
        extra={
            "table": table_name,
            "valid": len(working),
            "rejected": stats["total_rejected"],
            "stats": stats,
        },
    )
    return working, rejected_df, stats


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_pk_cols(table_name: str) -> list[str]:
    """Return the primary key column(s) for each table."""
    pk_map = {
        "cities":            ["city_id"],
        "regions":           ["region_id"],
        "segments":          ["segment_id"],
        "categories":        ["category_id"],
        "teams":             ["team_id"],
        "reason_categories": ["reason_category_id"],
        "reasons":           ["reason_id"],
        "channels":          ["channel_id"],
        "priorities":        ["priority_id"],
        "customers":         ["customer_id"],
        "restaurants":       ["restaurant_id"],
        "drivers":           ["driver_id"],
        "agents":            ["agent_id"],
        "orders":            ["order_id"],
        "tickets":           ["ticket_id"],
        "ticket_events":     ["event_id"],
    }
    return pk_map.get(table_name, [])


def _empty_stats() -> dict:
    return {
        "total":              0,
        "duplicates_removed": 0,
        "null_violations":    0,
        "invalid_email":      0,
        "invalid_phone":      0,
        "range_violations":   0,
        "date_violations":    0,
        "total_rejected":     0,
    }