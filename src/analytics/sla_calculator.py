"""
analytics/sla_calculator.py
=============================
Computes SLA breach metrics entirely inside PostgreSQL — no Python math.

Called after every batch of tickets is loaded into fact_ticket.

Two responsibilities:
  1. UPDATE fact_ticket — fill in the four computed SLA columns for any
     rows where they are still NULL (i.e. freshly inserted rows).

  2. CREATE OR REPLACE VIEW — maintain analytics views that power
     the dashboard and PDF report.

SLA rules (from priorities table / config):
  first_response : ticket.first_response_at  ≤ ticket.sla_first_due_at
  resolution     : ticket.resolved_at        ≤ ticket.sla_resolve_due_at

Usage:
    from src.analytics.sla_calculator import refresh_sla

    with get_connection() as conn:
        refresh_sla(conn, run_date="2026-02-22")
"""

import psycopg2
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── SQL: update SLA columns for newly loaded tickets ──────────────────────────

_UPDATE_SLA_FLAGS = """
UPDATE fact_ticket
SET
    is_first_response_breached = (first_response_at > sla_first_due_at),
    is_resolution_breached     = (resolved_at > sla_resolve_due_at),
    first_response_seconds     = EXTRACT(
                                     EPOCH FROM (first_response_at - created_at)
                                 )::INTEGER,
    resolution_minutes         = ROUND(
                                     EXTRACT(EPOCH FROM (resolved_at - created_at)) / 60.0,
                                     2
                                 )
WHERE
    -- Only process rows where SLA flags haven't been calculated yet
    is_first_response_breached IS NULL
    -- Require timestamps to be present (data quality guard)
    AND first_response_at IS NOT NULL
    AND resolved_at        IS NOT NULL
    AND sla_first_due_at   IS NOT NULL
    AND sla_resolve_due_at IS NOT NULL
"""

# ── SQL: analytics views (re-created on every refresh) ────────────────────────

_VIEWS = [

    # Daily SLA summary
    """
    CREATE OR REPLACE VIEW v_sla_daily AS
    SELECT
        DATE(t.created_at)                                        AS day,
        COUNT(*)                                                  AS total_tickets,
        SUM(t.is_first_response_breached::INTEGER)                AS fr_breaches,
        ROUND(AVG(t.is_first_response_breached::FLOAT) * 100, 2) AS fr_breach_rate_pct,
        SUM(t.is_resolution_breached::INTEGER)                    AS res_breaches,
        ROUND(AVG(t.is_resolution_breached::FLOAT) * 100, 2)     AS res_breach_rate_pct,
        ROUND(AVG(t.first_response_seconds), 1)                   AS avg_first_response_sec,
        ROUND(AVG(t.resolution_minutes)::NUMERIC, 2)              AS avg_resolution_min,
        SUM(t.refund_amount)                                      AS total_refunds,
        COUNT(DISTINCT t.agent_id)                                AS agents_involved
    FROM fact_ticket t
    WHERE t.is_first_response_breached IS NOT NULL
    GROUP BY DATE(t.created_at)
    ORDER BY day DESC
    """,

    # SLA by restaurant
    """
    CREATE OR REPLACE VIEW v_sla_by_restaurant AS
    SELECT
        r.restaurant_id,
        r.restaurant_name,
        rc.category_name,
        rg.region_name,
        COUNT(t.ticket_id)                                        AS total_tickets,
        ROUND(AVG(t.is_resolution_breached::FLOAT) * 100, 2)     AS res_breach_rate_pct,
        ROUND(AVG(t.resolution_minutes)::NUMERIC, 2)              AS avg_resolution_min,
        SUM(t.refund_amount)                                      AS total_refunds
    FROM fact_ticket t
    JOIN dim_restaurant r  ON t.restaurant_id = r.restaurant_id
    JOIN dim_category   rc ON r.category_id   = rc.category_id
    JOIN dim_region     rg ON r.region_id     = rg.region_id
    WHERE t.is_resolution_breached IS NOT NULL
    GROUP BY r.restaurant_id, r.restaurant_name, rc.category_name, rg.region_name
    ORDER BY res_breach_rate_pct DESC
    """,

    # SLA by driver
    """
    CREATE OR REPLACE VIEW v_sla_by_driver AS
    SELECT
        d.driver_id,
        d.driver_name,
        d.vehicle_type,
        COUNT(t.ticket_id)                                        AS total_tickets,
        ROUND(AVG(t.is_resolution_breached::FLOAT) * 100, 2)     AS res_breach_rate_pct,
        SUM(t.refund_amount)                                      AS total_refunds,
        d.on_time_rate,
        d.cancel_rate
    FROM fact_ticket t
    JOIN dim_driver d ON t.driver_id = d.driver_id
    WHERE t.is_resolution_breached IS NOT NULL
    GROUP BY d.driver_id, d.driver_name, d.vehicle_type,
             d.on_time_rate, d.cancel_rate
    ORDER BY total_tickets DESC
    """,

    # SLA by city / region
    """
    CREATE OR REPLACE VIEW v_sla_by_region AS
    SELECT
        c.city_name,
        rg.region_name,
        COUNT(t.ticket_id)                                        AS total_tickets,
        ROUND(AVG(t.is_first_response_breached::FLOAT) * 100, 2) AS fr_breach_rate_pct,
        ROUND(AVG(t.is_resolution_breached::FLOAT) * 100, 2)     AS res_breach_rate_pct,
        SUM(t.refund_amount)                                      AS total_refunds
    FROM fact_ticket t
    JOIN fact_order   fo ON t.order_id   = fo.order_id
    JOIN dim_region   rg ON fo.region_id = rg.region_id
    JOIN dim_city     c  ON rg.city_id   = c.city_id
    WHERE t.is_resolution_breached IS NOT NULL
    GROUP BY c.city_name, rg.region_name
    ORDER BY res_breach_rate_pct DESC
    """,

    # Revenue impact summary
    """
    CREATE OR REPLACE VIEW v_revenue_impact AS
    SELECT
        DATE(t.created_at)                AS day,
        COUNT(t.ticket_id)                AS total_tickets,
        SUM(t.refund_amount)              AS total_refunds,
        COUNT(fo.order_id)                AS total_orders,
        SUM(fo.total_amount)              AS total_revenue,
        ROUND(
            SUM(t.refund_amount) /
            NULLIF(SUM(fo.total_amount), 0) * 100,
            2
        )                                 AS refund_pct_of_revenue,
        ROUND(
            COUNT(t.ticket_id)::FLOAT /
            NULLIF(COUNT(fo.order_id), 0) * 1000,
            2
        )                                 AS complaint_rate_per_1000_orders
    FROM fact_ticket t
    JOIN fact_order fo ON t.order_id = fo.order_id
    GROUP BY DATE(t.created_at)
    ORDER BY day DESC
    """,

    # Reopen rate
    """
    CREATE OR REPLACE VIEW v_reopen_rate AS
    SELECT
        DATE(t.created_at)                AS day,
        COUNT(DISTINCT t.ticket_id)       AS total_tickets,
        COUNT(DISTINCT
            CASE WHEN e.new_status = 'Reopened'
                 THEN e.ticket_id END
        )                                 AS reopened_tickets,
        ROUND(
            COUNT(DISTINCT CASE WHEN e.new_status = 'Reopened' THEN e.ticket_id END)
            ::FLOAT /
            NULLIF(COUNT(DISTINCT t.ticket_id), 0) * 100,
            2
        )                                 AS reopen_rate_pct
    FROM fact_ticket t
    LEFT JOIN fact_ticket_event e ON t.ticket_id = e.ticket_id
    GROUP BY DATE(t.created_at)
    ORDER BY day DESC
    """,
]


# ── Public API ─────────────────────────────────────────────────────────────────

def refresh_sla(conn, run_date: str) -> int:
    """
    1. UPDATE fact_ticket to fill in SLA breach flags for new rows.
    2. Recreate all analytics views.

    Args:
        conn     : Open psycopg2 connection.
        run_date : "YYYY-MM-DD" — used only for logging context.

    Returns:
        Number of ticket rows updated.
    """
    rows_updated = 0

    try:
        with conn.cursor() as cur:
            # Step 1: compute SLA flags
            cur.execute(_UPDATE_SLA_FLAGS)
            rows_updated = cur.rowcount
            logger.info(
                "SLA flags updated",
                extra={"run_date": run_date, "rows_updated": rows_updated},
            )

            # Step 2: refresh views
            for view_sql in _VIEWS:
                cur.execute(view_sql)

        conn.commit()
        logger.info(
            "SLA views refreshed",
            extra={"run_date": run_date, "views": len(_VIEWS)},
        )

    except Exception as exc:
        logger.error(
            "SLA refresh failed",
            extra={"run_date": run_date, "error": str(exc)},
        )
        # Non-fatal — pipeline continues; SLA can be recomputed on next cycle

    return rows_updated


def get_sla_summary(conn, run_date: str) -> dict:
    """
    Return a summary dict for the given date — used by pdf_report.py.
    Queries v_sla_daily directly.
    """
    sql = "SELECT * FROM v_sla_daily WHERE day = %s"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (run_date,))
            row = cur.fetchone()
        return dict(row) if row else {}
    except Exception as exc:
        logger.error("Failed to fetch SLA summary", extra={"error": str(exc)})
        return {}
