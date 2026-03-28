"""
ingestion/file_reader.py
========================
Safe file readers for CSV and JSON formats.
Both functions ALWAYS return a DataFrame — never raise.
On any parse error they return an empty DataFrame and log the issue.

The caller is responsible for checking if the returned DataFrame is empty
and deciding whether to quarantine / alert.

Usage:
    from src.ingestion.file_reader import read_csv, read_json
    df = read_csv("data/input/batch/2026-02-22/customers.csv")
    df = read_json("data/input/batch/2026-02-22/restaurants.json")
"""

import json
import pandas as pd
from pathlib import Path
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def read_csv(file_path: str | Path) -> tuple[pd.DataFrame, Optional[str]]:
    """
    Read a CSV file into a DataFrame.

    Returns:
        (df, error_message)
        - On success : (non-empty DataFrame, None)
        - On empty   : (empty DataFrame,     None)        — valid, just no data
        - On failure : (empty DataFrame,     error string)

    Handles:
        - Missing file
        - Completely empty file
        - Encoding issues (falls back UTF-8 → latin-1)
        - Malformed rows (bad_lines='skip')
    """
    path = Path(file_path)

    if not path.exists():
        msg = f"File not found: {path}"
        logger.error("CSV read failed", extra={"file": str(path), "error": msg})
        return pd.DataFrame(), msg

    if path.stat().st_size == 0:
        logger.warning("CSV file is empty", extra={"file": str(path)})
        return pd.DataFrame(), None

    # Try UTF-8 first, fall back to latin-1
    for encoding in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(
                path,
                encoding=encoding,
                on_bad_lines="skip",   # skip malformed rows, don't crash
                low_memory=False,
            )
            logger.info(
                "CSV read OK",
                extra={"file": str(path), "rows": len(df), "cols": list(df.columns)},
            )
            return df, None

        except pd.errors.EmptyDataError:
            logger.warning("CSV has no data rows", extra={"file": str(path)})
            return pd.DataFrame(), None

        except UnicodeDecodeError:
            continue  # try next encoding

        except Exception as exc:
            msg = str(exc)
            logger.error(
                "CSV read failed",
                extra={"file": str(path), "error": msg},
            )
            return pd.DataFrame(), msg

    # Both encodings failed
    msg = f"Could not decode file with UTF-8 or latin-1: {path}"
    logger.error("CSV read failed", extra={"file": str(path), "error": msg})
    return pd.DataFrame(), msg


def read_json(file_path: str | Path) -> tuple[pd.DataFrame, Optional[str]]:
    """
    Read a JSON file (array of objects) into a DataFrame.

    Returns:
        (df, error_message)  — same semantics as read_csv

    Handles:
        - Missing file
        - Empty file / empty array []
        - Malformed JSON
        - JSON object instead of array (wraps in list)
    """
    path = Path(file_path)

    if not path.exists():
        msg = f"File not found: {path}"
        logger.error("JSON read failed", extra={"file": str(path), "error": msg})
        return pd.DataFrame(), msg

    if path.stat().st_size == 0:
        logger.warning("JSON file is empty", extra={"file": str(path)})
        return pd.DataFrame(), None

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

    except json.JSONDecodeError as exc:
        msg = f"Malformed JSON: {exc}"
        logger.error("JSON read failed", extra={"file": str(path), "error": msg})
        return pd.DataFrame(), msg

    except Exception as exc:
        msg = str(exc)
        logger.error("JSON read failed", extra={"file": str(path), "error": msg})
        return pd.DataFrame(), msg

    # Normalise: wrap bare object in a list
    if isinstance(raw, dict):
        raw = [raw]

    if not isinstance(raw, list):
        msg = f"Expected JSON array, got {type(raw).__name__}"
        logger.error("JSON read failed", extra={"file": str(path), "error": msg})
        return pd.DataFrame(), msg

    if len(raw) == 0:
        logger.warning("JSON file contains empty array", extra={"file": str(path)})
        return pd.DataFrame(), None

    try:
        df = pd.DataFrame(raw)
        logger.info(
            "JSON read OK",
            extra={"file": str(path), "rows": len(df), "cols": list(df.columns)},
        )
        return df, None

    except Exception as exc:
        msg = f"Could not convert JSON to DataFrame: {exc}"
        logger.error("JSON read failed", extra={"file": str(path), "error": msg})
        return pd.DataFrame(), msg
