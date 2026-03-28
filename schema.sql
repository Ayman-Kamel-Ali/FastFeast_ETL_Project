-- ============================================================
-- FastFeast Data Warehouse — PostgreSQL Schema
-- ============================================================
-- Run this file once to create the full star-schema.
-- All statements use IF NOT EXISTS — safe to run multiple times.
--
-- Order:
--   1. Dimension tables (lookup → entity)
--   2. Fact tables
--   3. Audit table
--   4. Indexes
--   5. SLA analytics views
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_city (
    city_id    INTEGER PRIMARY KEY,
    city_name  VARCHAR(100) NOT NULL,
    country    VARCHAR(100),
    timezone   VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_id         INTEGER PRIMARY KEY,
    region_name       VARCHAR(100) NOT NULL,
    city_id           INTEGER REFERENCES dim_city(city_id),
    delivery_base_fee NUMERIC(8, 2)
);

CREATE TABLE IF NOT EXISTS dim_segment (
    segment_id       INTEGER PRIMARY KEY,
    segment_name     VARCHAR(50) NOT NULL,
    discount_pct     INTEGER DEFAULT 0,
    priority_support BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id   INTEGER PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id   INTEGER PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_reason_category (
    reason_category_id INTEGER PRIMARY KEY,
    category_name      VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_reason (
    reason_id          INTEGER PRIMARY KEY,
    reason_name        VARCHAR(200) NOT NULL,
    reason_category_id INTEGER REFERENCES dim_reason_category(reason_category_id),
    severity_level     INTEGER CHECK (severity_level BETWEEN 1 AND 5),
    typical_refund_pct NUMERIC(5, 3) CHECK (typical_refund_pct BETWEEN 0 AND 1)
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id   INTEGER PRIMARY KEY,
    channel_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_priority (
    priority_id            INTEGER PRIMARY KEY,
    priority_code          VARCHAR(10) NOT NULL,
    priority_name          VARCHAR(50) NOT NULL,
    sla_first_response_min INTEGER,
    sla_resolution_min     INTEGER
);

-- PII columns (email, phone) are SHA-256 hashed before insert
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
);

CREATE TABLE IF NOT EXISTS dim_restaurant (
    restaurant_id     INTEGER PRIMARY KEY,
    restaurant_name   VARCHAR(200),
    region_id         INTEGER REFERENCES dim_region(region_id),
    category_id       INTEGER REFERENCES dim_category(category_id),
    price_tier        VARCHAR(10),
    rating_avg        NUMERIC(3, 2),
    prep_time_avg_min INTEGER,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
);

-- PII columns (phone, national_id) are SHA-256 hashed before insert
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
);

-- PII columns (email, phone) are SHA-256 hashed before insert
CREATE TABLE IF NOT EXISTS dim_agent (
    agent_id            INTEGER PRIMARY KEY,
    agent_name          VARCHAR(200) NOT NULL,
    email_hash          VARCHAR(64),
    phone_hash          VARCHAR(64),
    team_id             INTEGER REFERENCES dim_team(team_id),
    skill_level         VARCHAR(20),
    hire_date           DATE,
    avg_handle_time_min INTEGER,
    resolution_rate     NUMERIC(5, 3),
    csat_score          NUMERIC(4, 2),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
);


-- ────────────────────────────────────────────────────────────
-- FACT TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_order (
    order_id         VARCHAR(36) PRIMARY KEY,   -- UUID from source
    customer_id      INTEGER REFERENCES dim_customer(customer_id),
    restaurant_id    INTEGER REFERENCES dim_restaurant(restaurant_id),
    driver_id        INTEGER REFERENCES dim_driver(driver_id),
    region_id        INTEGER REFERENCES dim_region(region_id),
    order_amount     NUMERIC(10, 2),
    delivery_fee     NUMERIC(8,  2),
    discount_amount  NUMERIC(8,  2),
    total_amount     NUMERIC(10, 2),
    order_status     VARCHAR(20),               -- Delivered / Cancelled / Refunded
    payment_method   VARCHAR(20),               -- card / cash / wallet
    order_created_at TIMESTAMP,
    delivered_at     TIMESTAMP                  -- NULL if not Delivered
);

-- SLA breach columns (is_first_response_breached, is_resolution_breached,
-- first_response_seconds, resolution_minutes) are NULL on insert and
-- populated by sla_calculator.py after each ticket batch is loaded.
CREATE TABLE IF NOT EXISTS fact_ticket (
    ticket_id                  VARCHAR(36) PRIMARY KEY,   -- UUID from source
    order_id                   VARCHAR(36) REFERENCES fact_order(order_id),
    customer_id                INTEGER,
    driver_id                  INTEGER,
    restaurant_id              INTEGER,
    agent_id                   INTEGER REFERENCES dim_agent(agent_id),
    reason_id                  INTEGER REFERENCES dim_reason(reason_id),
    priority_id                INTEGER REFERENCES dim_priority(priority_id),
    channel_id                 INTEGER REFERENCES dim_channel(channel_id),
    status                     VARCHAR(20),               -- Resolved / Closed
    refund_amount              NUMERIC(10, 2),
    created_at                 TIMESTAMP,
    first_response_at          TIMESTAMP,
    resolved_at                TIMESTAMP,
    sla_first_due_at           TIMESTAMP,                 -- created_at + 1 min
    sla_resolve_due_at         TIMESTAMP,                 -- created_at + 15 min
    -- Computed by sla_calculator.py after load:
    is_first_response_breached BOOLEAN,
    is_resolution_breached     BOOLEAN,
    first_response_seconds     INTEGER,
    resolution_minutes         NUMERIC(8, 2)
);

CREATE TABLE IF NOT EXISTS fact_ticket_event (
    event_id   VARCHAR(36) PRIMARY KEY,         -- UUID from source
    ticket_id  VARCHAR(36) REFERENCES fact_ticket(ticket_id),
    agent_id   INTEGER     REFERENCES dim_agent(agent_id),
    event_ts   TIMESTAMP   NOT NULL,
    old_status VARCHAR(20),                     -- NULL for the first event
    new_status VARCHAR(20) NOT NULL,            -- Open / InProgress / Resolved / Closed / Reopened
    notes      TEXT
);


-- ────────────────────────────────────────────────────────────
-- AUDIT TABLE
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS quality_log (
    run_id                SERIAL PRIMARY KEY,
    run_date              DATE        NOT NULL,
    run_timestamp         TIMESTAMP   NOT NULL DEFAULT NOW(),
    table_name            VARCHAR(50) NOT NULL,
    source_file           TEXT,
    total_records         INTEGER     DEFAULT 0,
    valid_records         INTEGER     DEFAULT 0,
    rejected_records      INTEGER     DEFAULT 0,
    duplicate_count       INTEGER     DEFAULT 0,
    null_violation_count  INTEGER     DEFAULT 0,
    invalid_format_count  INTEGER     DEFAULT 0,
    orphan_count          INTEGER     DEFAULT 0,
    orphan_rate           NUMERIC(6, 4) DEFAULT 0,
    processing_latency_ms INTEGER     DEFAULT 0,
    status                VARCHAR(20) DEFAULT 'success'  -- success / partial / failed / skipped
);


-- ────────────────────────────────────────────────────────────
-- INDEXES
-- ────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_order_customer    ON fact_order(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_restaurant  ON fact_order(restaurant_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_driver      ON fact_order(driver_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_region      ON fact_order(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_created     ON fact_order(order_created_at);
CREATE INDEX IF NOT EXISTS idx_fact_order_status      ON fact_order(order_status);

CREATE INDEX IF NOT EXISTS idx_fact_ticket_order      ON fact_ticket(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_agent      ON fact_ticket(agent_id);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_created    ON fact_ticket(created_at);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_status     ON fact_ticket(status);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_sla_fr     ON fact_ticket(is_first_response_breached);
CREATE INDEX IF NOT EXISTS idx_fact_ticket_sla_res    ON fact_ticket(is_resolution_breached);

CREATE INDEX IF NOT EXISTS idx_ticket_event_ticket    ON fact_ticket_event(ticket_id);
CREATE INDEX IF NOT EXISTS idx_ticket_event_ts        ON fact_ticket_event(event_ts);

CREATE INDEX IF NOT EXISTS idx_quality_log_date       ON quality_log(run_date);
CREATE INDEX IF NOT EXISTS idx_quality_log_table      ON quality_log(table_name);


-- ────────────────────────────────────────────────────────────
-- SLA ANALYTICS VIEWS
-- ────────────────────────────────────────────────────────────

-- Daily SLA summary
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
ORDER BY day DESC;


-- SLA breach rate by restaurant
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
ORDER BY res_breach_rate_pct DESC;


-- SLA breach rate by driver
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
GROUP BY d.driver_id, d.driver_name, d.vehicle_type, d.on_time_rate, d.cancel_rate
ORDER BY total_tickets DESC;


-- SLA breach rate by city / region
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
ORDER BY res_breach_rate_pct DESC;


-- Revenue & complaint impact by day
CREATE OR REPLACE VIEW v_revenue_impact AS
SELECT
    DATE(t.created_at)                                        AS day,
    COUNT(t.ticket_id)                                        AS total_tickets,
    SUM(t.refund_amount)                                      AS total_refunds,
    COUNT(fo.order_id)                                        AS total_orders,
    SUM(fo.total_amount)                                      AS total_revenue,
    ROUND(
        SUM(t.refund_amount) / NULLIF(SUM(fo.total_amount), 0) * 100, 2
    )                                                         AS refund_pct_of_revenue,
    ROUND(
        COUNT(t.ticket_id)::FLOAT / NULLIF(COUNT(fo.order_id), 0) * 1000, 2
    )                                                         AS complaint_rate_per_1000_orders
FROM fact_ticket t
JOIN fact_order fo ON t.order_id = fo.order_id
GROUP BY DATE(t.created_at)
ORDER BY day DESC;


-- Ticket reopen rate by day
CREATE OR REPLACE VIEW v_reopen_rate AS
SELECT
    DATE(t.created_at)                                        AS day,
    COUNT(DISTINCT t.ticket_id)                               AS total_tickets,
    COUNT(DISTINCT
        CASE WHEN e.new_status = 'Reopened' THEN e.ticket_id END
    )                                                         AS reopened_tickets,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.new_status = 'Reopened' THEN e.ticket_id END)
        ::FLOAT / NULLIF(COUNT(DISTINCT t.ticket_id), 0) * 100, 2
    )                                                         AS reopen_rate_pct
FROM fact_ticket t
LEFT JOIN fact_ticket_event e ON t.ticket_id = e.ticket_id
GROUP BY DATE(t.created_at)
ORDER BY day DESC;


-- ────────────────────────────────────────────────────────────
-- USEFUL QUERIES (reference / ad-hoc)
-- ────────────────────────────────────────────────────────────

-- Daily quality overview
-- SELECT table_name, total_records, valid_records, rejected_records,
--        orphan_count, ROUND(orphan_rate * 100, 2) AS orphan_pct,
--        processing_latency_ms, status
-- FROM quality_log
-- WHERE run_date = CURRENT_DATE
-- ORDER BY run_timestamp;

-- Overall SLA health check
-- SELECT * FROM v_sla_daily LIMIT 7;

-- Top 10 restaurants by refund amount
-- SELECT restaurant_name, total_refunds, res_breach_rate_pct
-- FROM v_sla_by_restaurant
-- ORDER BY total_refunds DESC
-- LIMIT 10;

-- Worst drivers by SLA breach rate
-- SELECT driver_name, vehicle_type, total_tickets, res_breach_rate_pct
-- FROM v_sla_by_driver
-- WHERE total_tickets >= 5
-- ORDER BY res_breach_rate_pct DESC
-- LIMIT 10;
