"""
warehouse/loader.py
===================
Idempotent upsert loader for all warehouse tables.
Uses PostgreSQL's INSERT … ON CONFLICT DO UPDATE (UPSERT) so re-running
the pipeline with the same data never creates duplicates.

Core function:
    upsert(conn, df, table_name, pk_cols)

The function:
  1. Drops rows where any PK column is null (can't upsert without a PK)
  2. Replaces pandas NaN / NaT with None so psycopg2 writes NULL correctly
  3. Builds a parameterised INSERT … ON CONFLICT … DO UPDATE statement
  4. Executes in a single executemany call (one round-trip)

Usage:
    from src.warehouse.loader import upsert
    with get_connection() as conn:
        rows_inserted = upsert(conn, df, "dim_customer", ["customer_id"])
"""

import pandas as pd
import psycopg2
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Map source DataFrame column names → warehouse table column names
# Only needed where they differ (e.g. stream files use 'ticket_events' but table is fact_ticket_event)
_TABLE_NAME_MAP: dict[str, str] = {
    "cities":            "dim_city",
    "regions":           "dim_region",
    "segments":          "dim_segment",
    "categories":        "dim_category",
    "teams":             "dim_team",
    "reason_categories": "dim_reason_category",
    "reasons":           "dim_reason",
    "channels":          "dim_channel",
    "priorities":        "dim_priority",
    "customers":         "dim_customer",
    "restaurants":       "dim_restaurant",
    "drivers":           "dim_driver",
    "agents":            "dim_agent",
    "orders":            "fact_order",
    "tickets":           "fact_ticket",
    "ticket_events":     "fact_ticket_event",
}

# PK columns per warehouse table name
_PK_MAP: dict[str, list[str]] = {
    "dim_city":              ["city_id"],
    "dim_region":            ["region_id"],
    "dim_segment":           ["segment_id"],
    "dim_category":          ["category_id"],
    "dim_team":              ["team_id"],
    "dim_reason_category":   ["reason_category_id"],
    "dim_reason":            ["reason_id"],
    "dim_channel":           ["channel_id"],
    "dim_priority":          ["priority_id"],
    "dim_customer":          ["customer_id"],
    "dim_restaurant":        ["restaurant_id"],
    "dim_driver":            ["driver_id"],
    "dim_agent":             ["agent_id"],
    "fact_order":            ["order_id"],
    "fact_ticket":           ["ticket_id"],
    "fact_ticket_event":     ["event_id"],
}


def _to_warehouse_table(source_name: str) -> str:
    """Resolve source table name (e.g. 'customers') to warehouse table (e.g. 'dim_customer')."""
    # Already a warehouse name (e.g. called directly with 'dim_customer')
    if source_name in _PK_MAP:
        return source_name
    if source_name in _TABLE_NAME_MAP:
        return _TABLE_NAME_MAP[source_name]
    raise ValueError(f"Unknown table: '{source_name}'. Not in loader table map.")


def _clean_df(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert DataFrame to a list of plain dicts suitable for psycopg2.
    Replaces pandas NA / NaT / nan with Python None so the DB gets NULL.
    """
    # Convert to object dtype to unify NaN handling, then replace
    records = df.where(pd.notnull(df), other=None).to_dict(orient="records")

    # psycopg2 doesn't understand pandas Timestamp — convert to Python datetime
    cleaned = []
    for row in records:
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                clean_row[k] = v.to_pydatetime() if not pd.isnull(v) else None
            elif v != v:   # NaN check (float NaN != float NaN)
                clean_row[k] = None
            else:
                clean_row[k] = v
        cleaned.append(clean_row)

    return cleaned


def upsert(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table_name: str,
    pk_cols: list[str] | None = None,
) -> int:
    """
    Upsert all rows from df into the given warehouse table.

    Args:
        conn       : Open psycopg2 connection (caller manages transaction).
        df         : DataFrame to load. Columns must match warehouse schema.
        table_name : Source name ('customers') or warehouse name ('dim_customer').
        pk_cols    : PK columns for ON CONFLICT clause. If None, looked up from _PK_MAP.

    Returns:
        Number of rows successfully upserted.

    Raises:
        ValueError  : Unknown table or no PK columns defined.
        psycopg2.*  : Re-raised DB errors (caller should catch and handle).
    """
    if df is None or df.empty:
        logger.info("Upsert skipped — empty DataFrame", extra={"table": table_name})
        return 0

    wh_table = _to_warehouse_table(table_name)

    if pk_cols is None:
        pk_cols = _PK_MAP.get(wh_table)
        if not pk_cols:
            raise ValueError(f"No PK defined for table '{wh_table}'")

    # Drop rows with null PKs — can't upsert without a conflict target
    before = len(df)
    df = df.dropna(subset=pk_cols)
    dropped_null_pk = before - len(df)
    if dropped_null_pk:
        logger.warning(
            "Dropped rows with null PK before upsert",
            extra={"table": wh_table, "count": dropped_null_pk},
        )

    if df.empty:
        return 0

    cols   = list(df.columns)
    values = _clean_df(df)

    # Build the parameterised SQL
    col_list      = ", ".join(f'"{c}"' for c in cols)
    placeholder   = ", ".join(f"%({c})s" for c in cols)
    pk_constraint = ", ".join(f'"{c}"' for c in pk_cols)

    # ON CONFLICT: update every non-PK column
    update_cols = [c for c in cols if c not in pk_cols]
    if update_cols:
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        conflict_clause = f"ON CONFLICT ({pk_constraint}) DO UPDATE SET {update_set}"
    else:
        # All columns are PKs — just skip duplicates
        conflict_clause = f"ON CONFLICT ({pk_constraint}) DO NOTHING"

    sql = f"""
        INSERT INTO {wh_table} ({col_list})
        VALUES ({placeholder})
        {conflict_clause}
    """

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, values, page_size=500)

        logger.info(
            "Upsert complete",
            extra={"table": wh_table, "rows": len(df)},
        )
        return len(df)

    except Exception as exc:
        logger.error(
            "Upsert failed",
            extra={"table": wh_table, "rows": len(df), "error": str(exc)},
        )
        raise


def upsert_by_source_name(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    source_table: str,
) -> int:
    """
    Convenience wrapper — resolves source name to warehouse name automatically.
    Used by batch_ingestion and stream_ingestion.
    """
    return upsert(conn, df, source_table)
