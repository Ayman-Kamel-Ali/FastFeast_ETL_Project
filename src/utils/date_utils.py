"""
utils/date_utils.py
===================
Shared date/datetime parsing helpers used by data_validator and other modules.
All functions are pure (no side effects) and return None on parse failure
so callers can treat None as "invalid value".
"""

from datetime import datetime, date
from typing import Optional
import pandas as pd


# Values that always mean "no date" regardless of column
_NULL_SENTINELS = {"", "n/a", "na", "null", "none", "invalid", "nan"}


def parse_datetime(value) -> Optional[datetime]:
    """
    Try to parse a value as a datetime.
    Returns a datetime object on success, None on failure.

    Handles:
    - Already a datetime / pandas Timestamp
    - ISO strings: "2026-02-22 08:15:30", "2026-02-22T08:15:30"
    - Date-only strings: "2026-02-22"  → treated as midnight
    - Sentinel strings: "N/A", "invalid", "" → None
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day)

    s = str(value).strip()
    if s.lower() in _NULL_SENTINELS:
        return None

    # pandas is robust at parsing most common formats
    try:
        ts = pd.to_datetime(s, errors="raise")
        return ts.to_pydatetime()
    except Exception:
        return None


def parse_date(value) -> Optional[date]:
    """
    Try to parse a value as a date (no time component).
    Returns a date object on success, None on failure.
    """
    dt = parse_datetime(value)
    return dt.date() if dt is not None else None


def is_valid_datetime(value) -> bool:
    """Return True if value can be parsed as a datetime."""
    return parse_datetime(value) is not None


def is_valid_date(value) -> bool:
    """Return True if value can be parsed as a date."""
    return parse_date(value) is not None


def safe_parse_series(series: pd.Series) -> pd.Series:
    """
    Parse an entire pandas Series as datetimes.
    Invalid values become NaT (pandas null for timestamps).
    Equivalent to pd.to_datetime(series, errors='coerce') but with
    our sentinel list applied first.
    """
    # Replace known sentinel strings with None before parsing
    cleaned = series.apply(
        lambda v: None if (isinstance(v, str) and v.strip().lower() in _NULL_SENTINELS) else v
    )
    return pd.to_datetime(cleaned, errors="coerce")
