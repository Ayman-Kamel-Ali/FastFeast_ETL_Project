"""
validation/referential_validator.py
=====================================
FK orphan detection for stream files.
Splits each DataFrame into (valid_df, orphan_df) by checking whether
referenced IDs actually exist in the warehouse dimension tables.

Orphan logic per table:
  orders         → customer_id  in dim_customer
                   restaurant_id in dim_restaurant
                   driver_id     in dim_driver
                   region_id     in dim_region

  tickets        → order_id     in fact_order
                   agent_id     in dim_agent

  ticket_events  → ticket_id    in fact_ticket
                   agent_id     in dim_agent

The validator loads known IDs from the warehouse ONCE per run (cached in
memory as Python sets) to avoid one DB round-trip per row.

Usage:
    from src.validation.referential_validator import ReferentialValidator
    from src.warehouse.connection import get_connection

    with get_connection() as conn:
        rv = ReferentialValidator(conn)
        rv.load_known_ids()

    valid_orders, orphan_orders, orphan_stats = rv.check_orders(orders_df)
    valid_tickets, orphan_tickets, _          = rv.check_tickets(tickets_df)
    valid_events, orphan_events, _            = rv.check_events(events_df)
"""

import pandas as pd
from typing import Optional

from src.utils.logger import get_logger
from src.utils.alerter import send_alert_async

logger = get_logger(__name__)


class ReferentialValidator:
    """
    Loads known IDs from the warehouse once, then validates FK references
    for all stream tables without additional DB round-trips.
    """

    def __init__(self, conn):
        self._conn = conn
        # Sets of known IDs — populated by load_known_ids()
        self._customer_ids:   set = set()
        self._restaurant_ids: set = set()
        self._driver_ids:     set = set()
        self._region_ids:     set = set()
        self._agent_ids:      set = set()
        self._order_ids:      set = set()
        self._ticket_ids:     set = set()

    # ── ID loading ─────────────────────────────────────────────────────────────

    def load_known_ids(self) -> None:
        """
        Fetch all PKs from warehouse dimension and fact tables into memory.
        Call this once after batch ingestion completes so stream validation
        has a fresh view of what exists.
        """
        queries = {
            "_customer_ids":   "SELECT customer_id   FROM dim_customer",
            "_restaurant_ids": "SELECT restaurant_id FROM dim_restaurant",
            "_driver_ids":     "SELECT driver_id     FROM dim_driver",
            "_region_ids":     "SELECT region_id     FROM dim_region",
            "_agent_ids":      "SELECT agent_id      FROM dim_agent",
            "_order_ids":      "SELECT order_id      FROM fact_order",
            "_ticket_ids":     "SELECT ticket_id     FROM fact_ticket",
        }

        with self._conn.cursor() as cur:
            for attr, sql in queries.items():
                try:
                    cur.execute(sql)
                    rows = cur.fetchall()
                    # rows come back as RealDictRow — get the first (only) column value
                    id_set = {list(r.values())[0] for r in rows if list(r.values())[0] is not None}
                    setattr(self, attr, id_set)
                    logger.info(
                        "Known IDs loaded",
                        extra={"set": attr, "count": len(id_set)},
                    )
                except Exception as exc:
                    logger.warning(
                        "Could not load known IDs — FK checks will be skipped for this set",
                        extra={"set": attr, "error": str(exc)},
                    )

    def refresh_order_ids(self) -> None:
        """Reload order IDs after a batch of orders is inserted."""
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT order_id FROM fact_order")
                rows = cur.fetchall()
                self._order_ids = {list(r.values())[0] for r in rows}
            logger.info("Order IDs refreshed", extra={"count": len(self._order_ids)})
        except Exception as exc:
            logger.warning("Could not refresh order IDs", extra={"error": str(exc)})

    def refresh_ticket_ids(self) -> None:
        """Reload ticket IDs after a batch of tickets is inserted."""
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT ticket_id FROM fact_ticket")
                rows = cur.fetchall()
                self._ticket_ids = {list(r.values())[0] for r in rows}
            logger.info("Ticket IDs refreshed", extra={"count": len(self._ticket_ids)})
        except Exception as exc:
            logger.warning("Could not refresh ticket IDs", extra={"error": str(exc)})

    # ── Core split logic ───────────────────────────────────────────────────────

    @staticmethod
    def _split(
        df: pd.DataFrame,
        fk_col: str,
        known_ids: set,
        label: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split df into (valid, orphan) based on whether fk_col values are in known_ids.
        Rows with null FK are treated as orphans (can't be referenced).
        """
        if fk_col not in df.columns or not known_ids:
            # If known_ids is empty (table not loaded yet), pass all through
            # to avoid falsely quarantining everything on first run.
            if not known_ids:
                logger.warning(
                    "Known IDs set is empty — skipping FK check",
                    extra={"fk_col": fk_col, "label": label},
                )
            return df, pd.DataFrame()

        # Coerce FK column to the same type as known_ids for comparison
        # known_ids may contain ints or strings depending on the table
        sample_id = next(iter(known_ids)) if known_ids else None
        fk_series = df[fk_col]

        if sample_id is not None and isinstance(sample_id, int):
            fk_series = pd.to_numeric(fk_series, errors="coerce")

        null_mask  = fk_series.isna()
        valid_mask = fk_series.isin(known_ids) & ~null_mask
        orphan_mask = ~valid_mask

        valid_df  = df[valid_mask].copy()
        orphan_df = df[orphan_mask].copy()

        if not orphan_df.empty:
            orphan_df["orphan_fk_col"]   = fk_col
            orphan_df["orphan_fk_value"] = df.loc[orphan_mask, fk_col].astype(str)

        return valid_df, orphan_df

    # ── Per-table validators ───────────────────────────────────────────────────

    def check_orders(
        self, df: pd.DataFrame, run_date: str = "", source_file: str = ""
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        Validate FK references in orders:
          customer_id → dim_customer
          restaurant_id → dim_restaurant
          driver_id → dim_driver
          region_id → dim_region

        Returns (valid_df, orphan_df, stats)
        """
        return self._check_multi(
            df,
            checks=[
                ("customer_id",   self._customer_ids,   "orphan_customer"),
                ("restaurant_id", self._restaurant_ids, "orphan_restaurant"),
                ("driver_id",     self._driver_ids,     "orphan_driver"),
                ("region_id",     self._region_ids,     "orphan_region"),
            ],
            table="orders",
            run_date=run_date,
            source_file=source_file,
        )

    def check_tickets(
        self, df: pd.DataFrame, run_date: str = "", source_file: str = ""
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        Validate FK references in tickets:
          order_id → fact_order
          agent_id → dim_agent
        """
        return self._check_multi(
            df,
            checks=[
                ("order_id", self._order_ids,  "orphan_order"),
                ("agent_id", self._agent_ids,  "orphan_agent"),
            ],
            table="tickets",
            run_date=run_date,
            source_file=source_file,
        )

    def check_events(
        self, df: pd.DataFrame, run_date: str = "", source_file: str = ""
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        Validate FK references in ticket_events:
          ticket_id → fact_ticket
          agent_id  → dim_agent
        """
        return self._check_multi(
            df,
            checks=[
                ("ticket_id", self._ticket_ids, "orphan_ticket"),
                ("agent_id",  self._agent_ids,  "orphan_agent"),
            ],
            table="ticket_events",
            run_date=run_date,
            source_file=source_file,
        )

    def _check_multi(
        self,
        df: pd.DataFrame,
        checks: list[tuple[str, set, str]],
        table: str,
        run_date: str,
        source_file: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        Run multiple FK checks in sequence.
        A row is only in valid_df if it passes ALL checks.
        Orphan rows from any check are accumulated in orphan_df.
        """
        if df.empty:
            return df, pd.DataFrame(), _empty_orphan_stats()

        total = len(df)
        working   = df.copy()
        all_orphans: list[pd.DataFrame] = []

        for fk_col, known_ids, label in checks:
            working, orphans = self._split(working, fk_col, known_ids, label)
            if not orphans.empty:
                orphans["rejection_reason"] = label
                all_orphans.append(orphans)

        orphan_df = (
            pd.concat(all_orphans, ignore_index=True) if all_orphans else pd.DataFrame()
        )

        orphan_count = len(orphan_df)
        orphan_rate  = orphan_count / total if total > 0 else 0.0

        stats = {
            "total":        total,
            "valid":        len(working),
            "orphan_count": orphan_count,
            "orphan_rate":  round(orphan_rate, 4),
        }

        logger.info(
            "Referential validation complete",
            extra={"table": table, **stats},
        )

        # Alert if orphan rate exceeds configured threshold
        self._maybe_alert_orphan_rate(orphan_rate, table, run_date, source_file)

        return working, orphan_df, stats

    def _maybe_alert_orphan_rate(
        self, rate: float, table: str, run_date: str, source_file: str
    ) -> None:
        """Fire an async alert if orphan rate exceeds the configured threshold."""
        try:
            from config.settings import settings
            threshold = float(settings.thresholds.max_orphan_rate)
        except Exception:
            threshold = 0.05

        if rate > threshold:
            send_alert_async(
                subject=f"High orphan rate: {table}",
                error_message=(
                    f"Orphan rate {rate:.1%} exceeds threshold {threshold:.1%} "
                    f"for table '{table}'"
                ),
                context={
                    "table":       table,
                    "orphan_rate": f"{rate:.4f}",
                    "threshold":   f"{threshold:.4f}",
                    "run_date":    run_date,
                    "source_file": source_file,
                },
            )


def _empty_orphan_stats() -> dict:
    return {"total": 0, "valid": 0, "orphan_count": 0, "orphan_rate": 0.0}
