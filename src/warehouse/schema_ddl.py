"""
warehouse/schema_ddl.py
=======================
Full PostgreSQL DDL for the FastFeast star-schema data warehouse.

Tables:
  Dimensions (13): dim_city, dim_region, dim_segment, dim_category,
                   dim_team, dim_reason_category, dim_reason,
                   dim_channel, dim_priority, dim_customer,
                   dim_restaurant, dim_driver, dim_agent

  Facts (3):       fact_order, fact_ticket, fact_ticket_event

  Audit (1):       quality_log

All CREATE statements use IF NOT EXISTS — safe to call on every startup.
Foreign key constraints are defined but NOT enforced with DEFERRABLE —
the pipeline handles referential integrity itself before loading.

Usage:
    from src.warehouse.schema_ddl import create_all_tables
    with get_connection() as conn:
        create_all_tables(conn)
"""

import psycopg2
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── DDL statements (order matters — dims before facts) ─────────────────────────

_DDL_STATEMENTS = [

    # ── Dimension: city ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_city (
        city_id    INTEGER PRIMARY KEY,
        city_name  VARCHAR(100) NOT NULL,
        country    VARCHAR(100),
        timezone   VARCHAR(50)
    )
    """,

    # ── Dimension: region ────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_region (
        region_id         INTEGER PRIMARY KEY,
        region_name       VARCHAR(100) NOT NULL,
        city_id           INTEGER REFERENCES dim_city(city_id),
        delivery_base_fee NUMERIC(8, 2)
    )
    """,

    # ── Dimension: segment ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_segment (
        segment_id       INTEGER PRIMARY KEY,
        segment_name     VARCHAR(50) NOT NULL,
        discount_pct     INTEGER DEFAULT 0,
        priority_support BOOLEAN DEFAULT FALSE
    )
    """,

    # ── Dimension: category ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_category (
        category_id   INTEGER PRIMARY KEY,
        category_name VARCHAR(100) NOT NULL
    )
    """,

    # ── Dimension: team ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_team (
        team_id   INTEGER PRIMARY KEY,
        team_name VARCHAR(100) NOT NULL
    )
    """,

    # ── Dimension: reason_category ───────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_reason_category (
        reason_category_id INTEGER PRIMARY KEY,
        category_name      VARCHAR(100) NOT NULL
    )
    """,

    # ── Dimension: reason ────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_reason (
        reason_id          INTEGER PRIMARY KEY,
        reason_name        VARCHAR(200) NOT NULL,
        reason_category_id INTEGER REFERENCES dim_reason_category(reason_category_id),
        severity_level     INTEGER CHECK (severity_level BETWEEN 1 AND 5),
        typical_refund_pct NUMERIC(5, 3) CHECK (typical_refund_pct BETWEEN 0 AND 1)
    )
    """,

    # ── Dimension: channel ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_channel (
        channel_id   INTEGER PRIMARY KEY,
        channel_name VARCHAR(50) NOT NULL
    )
    """,

    # ── Dimension: priority ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_priority (
        priority_id           INTEGER PRIMARY KEY,
        priority_code         VARCHAR(10) NOT NULL,
        priority_name         VARCHAR(50) NOT NULL,
        sla_first_response_min INTEGER,
        sla_resolution_min     INTEGER
    )
    """,

    # ── Dimension: customer (PII masked) ─────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id INTEGER PRIMARY KEY,
        full_name   VARCHAR(200),
        email_hash  VARCHAR(64),
        phone_hash  VARCHAR(64),
        region_id   INTEGER REFERENCES dim_region(region_id),
        segment_id  INTEGER REFERENCES dim_segment(segment_id),
        signup_date DATE,
        gender      VARCHAR(10),
        created_at  TIMESTAMP,
        updated_at  TIMESTAMP
    )
    """,

    # ── Dimension: restaurant ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_restaurant (
        restaurant_id    INTEGER PRIMARY KEY,
        restaurant_name  VARCHAR(200),
        region_id        INTEGER REFERENCES dim_region(region_id),
        category_id      INTEGER REFERENCES dim_category(category_id),
        price_tier       VARCHAR(10),
        rating_avg       NUMERIC(3, 2),
        prep_time_avg_min INTEGER,
        is_active        BOOLEAN DEFAULT TRUE,
        created_at       TIMESTAMP,
        updated_at       TIMESTAMP
    )
    """,

    # ── Dimension: driver (PII masked) ───────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_driver (
        driver_id            INTEGER PRIMARY KEY,
        driver_name          VARCHAR(200),
        phone_hash           VARCHAR(64),
        national_id_hash     VARCHAR(64),
        region_id            INTEGER REFERENCES dim_region(region_id),
        shift                VARCHAR(20),
        vehicle_type         VARCHAR(20),
        hire_date            DATE,
        rating_avg           NUMERIC(3, 2),
        on_time_rate         NUMERIC(5, 3),
        cancel_rate          NUMERIC(5, 3),
        completed_deliveries INTEGER,
        is_active            BOOLEAN DEFAULT TRUE,
        created_at           TIMESTAMP,
        updated_at           TIMESTAMP
    )
    """,

    # ── Dimension: agent (PII masked) ────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_agent (
        agent_id             INTEGER PRIMARY KEY,
        agent_name           VARCHAR(200) NOT NULL,
        email_hash           VARCHAR(64),
        phone_hash           VARCHAR(64),
        team_id              INTEGER REFERENCES dim_team(team_id),
        skill_level          VARCHAR(20),
        hire_date            DATE,
        avg_handle_time_min  INTEGER,
        resolution_rate      NUMERIC(5, 3),
        csat_score           NUMERIC(4, 2),
        is_active            BOOLEAN DEFAULT TRUE,
        created_at           TIMESTAMP,
        updated_at           TIMESTAMP
    )
    """,

    # ── Fact: order ──────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_order (
        order_id         VARCHAR(36) PRIMARY KEY,
        customer_id      INTEGER REFERENCES dim_customer(customer_id),
        restaurant_id    INTEGER REFERENCES dim_restaurant(restaurant_id),
        driver_id        INTEGER REFERENCES dim_driver(driver_id),
        region_id        INTEGER REFERENCES dim_region(region_id),
        order_amount     NUMERIC(10, 2),
        delivery_fee     NUMERIC(8, 2),
        discount_amount  NUMERIC(8, 2),
        total_amount     NUMERIC(10, 2),
        order_status     VARCHAR(20),
        payment_method   VARCHAR(20),
        order_created_at TIMESTAMP,
        delivered_at     TIMESTAMP
    )
    """,

    # ── Fact: ticket ─────────────────────────────────────────────────────────
    # SLA breach flags and timing metrics are computed by sla_calculator.py
    # after load — they are NULL until that UPDATE runs.
    """
    CREATE TABLE IF NOT EXISTS fact_ticket (
        ticket_id                   VARCHAR(36) PRIMARY KEY,
        order_id                    VARCHAR(36) REFERENCES fact_order(order_id),
        customer_id                 INTEGER,
        driver_id                   INTEGER,
        restaurant_id               INTEGER,
        agent_id                    INTEGER REFERENCES dim_agent(agent_id),
        reason_id                   INTEGER REFERENCES dim_reason(reason_id),
        priority_id                 INTEGER REFERENCES dim_priority(priority_id),
        channel_id                  INTEGER REFERENCES dim_channel(channel_id),
        status                      VARCHAR(20),
        refund_amount               NUMERIC(10, 2),
        created_at                  TIMESTAMP,
        first_response_at           TIMESTAMP,
        resolved_at                 TIMESTAMP,
        sla_first_due_at            TIMESTAMP,
        sla_resolve_due_at          TIMESTAMP,
        is_first_response_breached  BOOLEAN,
        is_resolution_breached      BOOLEAN,
        first_response_seconds      INTEGER,
        resolution_minutes          NUMERIC(8, 2)
    )
    """,

    # ── Fact: ticket_event ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_ticket_event (
        event_id   VARCHAR(36) PRIMARY KEY,
        ticket_id  VARCHAR(36) REFERENCES fact_ticket(ticket_id),
        agent_id   INTEGER REFERENCES dim_agent(agent_id),
        event_ts   TIMESTAMP NOT NULL,
        old_status VARCHAR(20),
        new_status VARCHAR(20) NOT NULL,
        notes      TEXT
    )
    """,

    # ── Audit: quality_log ────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS quality_log (
        run_id               SERIAL PRIMARY KEY,
        run_date             DATE        NOT NULL,
        run_timestamp        TIMESTAMP   NOT NULL DEFAULT NOW(),
        table_name           VARCHAR(50) NOT NULL,
        source_file          TEXT,
        total_records        INTEGER     DEFAULT 0,
        valid_records        INTEGER     DEFAULT 0,
        rejected_records     INTEGER     DEFAULT 0,
        duplicate_count      INTEGER     DEFAULT 0,
        null_violation_count INTEGER     DEFAULT 0,
        invalid_format_count INTEGER     DEFAULT 0,
        orphan_count         INTEGER     DEFAULT 0,
        orphan_rate          NUMERIC(6, 4) DEFAULT 0,
        processing_latency_ms INTEGER    DEFAULT 0,
        status               VARCHAR(20) DEFAULT 'success'
    )
    """,

    # ── Indexes for common query patterns ────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_fact_order_customer   ON fact_order(customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_restaurant ON fact_order(restaurant_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_driver     ON fact_order(driver_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_order_created    ON fact_order(order_created_at)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_order     ON fact_ticket(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_agent     ON fact_ticket(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ticket_created   ON fact_ticket(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_ticket_event_ticket   ON fact_ticket_event(ticket_id)",
    "CREATE INDEX IF NOT EXISTS idx_quality_log_date      ON quality_log(run_date)",
]


def create_all_tables(conn: psycopg2.extensions.connection) -> None:
    """
    Execute all DDL statements against the given connection.
    Uses IF NOT EXISTS — completely safe to call on every pipeline startup.
    Raises on any DDL failure (unrecoverable — pipeline cannot proceed without schema).
    """
    with conn.cursor() as cur:
        for stmt in _DDL_STATEMENTS:
            try:
                cur.execute(stmt)
            except Exception as exc:
                logger.error(
                    "DDL execution failed",
                    extra={"statement_preview": stmt.strip()[:80], "error": str(exc)},
                )
                raise

    conn.commit()
    logger.info("All warehouse tables created / verified", extra={"table_count": 17})
