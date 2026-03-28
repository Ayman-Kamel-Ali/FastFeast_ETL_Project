# FastFeast Data Pipeline

Near real-time micro-batch data pipeline — OLTP → OLAP.  
Ingests mixed CSV/JSON files, validates data quality at 4 layers, loads a PostgreSQL star-schema warehouse, and computes SLA breach metrics entirely in SQL.

---

## Project Structure

```
fastfeast_pipeline/
├── config/
│   ├── settings.py              # Config loader (reads YAML + .env)
│   └── pipeline_config.yaml     # All tunable values — edit this, not the code
├── src/
│   ├── ingestion/
│   │   ├── file_reader.py       # Safe CSV / JSON readers
│   │   ├── file_tracker.py      # SQLite idempotency tracker
│   │   ├── batch_ingestion.py   # Daily dimension file processor
│   │   └── stream_ingestion.py  # Continuous micro-batch watcher
│   ├── validation/
│   │   ├── schema_validator.py  # Column presence + dtype checks (all 16 tables)
│   │   ├── data_validator.py    # Nulls, email, phone, numeric ranges, dates, dedup
│   │   ├── referential_validator.py  # FK orphan detection
│   │   └── pii_handler.py       # SHA-256 hash email / phone / national_id
│   ├── warehouse/
│   │   ├── connection.py        # psycopg2 context manager + retry
│   │   ├── schema_ddl.py        # CREATE TABLE DDL (13 dims + 3 facts + quality_log)
│   │   └── loader.py            # INSERT … ON CONFLICT upsert
│   ├── analytics/
│   │   ├── sla_calculator.py    # UPDATE SLA flags + CREATE VIEW in PostgreSQL
│   │   └── quality_metrics.py   # Persist run stats → quality_log table
│   ├── reporting/
│   │   └── pdf_report.py        # [BONUS] ReportLab PDF + email delivery
│   └── utils/
│       ├── logger.py            # Rotating JSON structured logger
│       ├── alerter.py           # Async failure email (daemon thread)
│       ├── quarantine.py        # Write rejected rows to data/quarantine/
│       └── date_utils.py        # Safe date parsing helpers
├── pipeline/
│   └── orchestrator.py          # Top-level wiring of all stages
├── scripts/                     # Provided simulation scripts — do not modify
│   ├── generate_master_data.py
│   ├── generate_batch_data.py
│   ├── generate_stream_data.py
│   ├── add_new_customers.py
│   ├── add_new_drivers.py
│   └── simulate_day.py
├── data/                        # Git-ignored — generated at runtime
│   ├── master/
│   ├── quarantine/
│   └── input/
│       ├── batch/
│       └── stream/
├── logs/                        # Git-ignored — pipeline.log written here
├── main.py                      # Entry point
├── requirements.txt
├── .env.example                 # Copy to .env and fill in credentials
└── file_tracker.db              # Auto-created SQLite (git-ignored)
```

---

## Setup

### 1. Clone and create virtual environment

```bash
git clone <your-repo-url>
cd fastfeast_pipeline
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Create PostgreSQL database

```sql
CREATE DATABASE fastfeast_dw;
CREATE USER pipeline_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE fastfeast_dw TO pipeline_user;
```

### 3. Configure credentials

```bash
cp .env.example .env
```

Edit `.env`:

```
DB_PASSWORD=your_postgres_password
ALERT_EMAIL=your_email@gmail.com
ALERT_PASSWORD=your_gmail_app_password
```

Edit `config/pipeline_config.yaml` if your PostgreSQL host/port/user differ from the defaults.

### 4. Verify connection

```bash
python main.py --check
# Expected output:
# [OK] Database reachable.
# [OK] All warehouse tables created / verified.
```

---

## Running the Pipeline

### Full run (batch + continuous stream watcher)

```bash
python main.py --date 2026-02-22
```

The pipeline will:

1. Process all `data/input/batch/2026-02-22/` files (blocking)
2. Start a background thread watching `data/input/stream/2026-02-22/`
3. Process new stream files as they arrive every 30 seconds
4. Press `Ctrl+C` to shut down cleanly

### Batch only (no stream watching)

```bash
python main.py --date 2026-02-22 --batch-only
```

---

## Simulating Data

### One-time master data setup

```bash
python scripts/generate_master_data.py
```

### Simulate a full day (recommended for testing)

```bash
# Terminal 1 — start the pipeline
python main.py --date 2026-02-22

# Terminal 2 — simulate data arriving throughout the day
python scripts/simulate_day.py --date 2026-02-22 --skip-master
```

`simulate_day.py` runs in the correct order:

1. Generates batch data
2. Randomly adds new customers/drivers (creating orphan scenarios)
3. Generates stream data for 8 hourly slots

### Manual step-by-step simulation

```bash
python scripts/generate_master_data.py
python scripts/generate_batch_data.py --date 2026-02-22
python scripts/add_new_customers.py --count 5    # creates orphan potential
python scripts/add_new_drivers.py --count 3
python scripts/generate_stream_data.py --date 2026-02-22 --hour 9
python scripts/generate_stream_data.py --date 2026-02-22 --hour 12
# ... repeat generate_stream_data for more hours
```

---

## Verifying Results

### Check warehouse row counts

```sql
SELECT 'dim_customer'    AS tbl, COUNT(*) FROM dim_customer    UNION ALL
SELECT 'dim_restaurant',          COUNT(*) FROM dim_restaurant  UNION ALL
SELECT 'dim_driver',              COUNT(*) FROM dim_driver      UNION ALL
SELECT 'fact_order',              COUNT(*) FROM fact_order      UNION ALL
SELECT 'fact_ticket',             COUNT(*) FROM fact_ticket     UNION ALL
SELECT 'fact_ticket_event',       COUNT(*) FROM fact_ticket_event;
```

### Check SLA breach metrics

```sql
SELECT * FROM v_sla_daily;
SELECT * FROM v_revenue_impact;
SELECT * FROM v_sla_by_restaurant LIMIT 10;
```

### Check data quality log

```sql
SELECT table_name, total_records, valid_records, rejected_records,
       orphan_count, orphan_rate, processing_latency_ms, status
FROM quality_log
ORDER BY run_timestamp DESC
LIMIT 20;
```

### Check quarantined rows

```bash
ls data/quarantine/2026-02-22/
# e.g.:
# customers_data_validation_09-00-01.csv
# orders_orphan_reference_09-00-15.csv
```

### Check pipeline logs

```bash
tail -f logs/pipeline.log | python3 -m json.tool
```

---

## Data Quality Checks Applied

| Layer | What is checked | On failure |
|---|---|---|
| Schema | Required columns present, no unknown columns | Skip whole file + alert |
| Null check | Non-nullable fields (customer_id, order_id, etc.) | Drop row + quarantine |
| Email format | `^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$` | Drop row + quarantine |
| Phone format | Egyptian mobile `^01[0125]\d{8}$` | Drop row + quarantine |
| Numeric ranges | rating 1–5, rates 0–1, amounts ≥ 0 | Drop row + quarantine |
| Date validity | Required timestamp columns must parse | Drop row + quarantine |
| Deduplication | PK-based, keep first occurrence | Remove duplicate silently |
| FK orphans | Orders reference existing customers/restaurants/drivers | Quarantine orphan rows |
| PII masking | email, phone, national_id → SHA-256 hash | Applied before any DB write |

---

## Fault Tolerance

The pipeline **never crashes** on data errors:

- File parse error → log + alert + skip file + continue
- Schema fail → log + alert + skip file + continue  
- Row validation fail → drop row + quarantine + continue
- FK orphan → quarantine orphan rows + load valid rows + continue
- DB write fail → log + alert + mark file as failed (will retry next run)
- Alert email fail → log the alert failure + continue

The only hard failure is an unreachable database at startup (`--check` will catch this).

---

## Idempotency

Re-running `python main.py --date 2026-02-22` twice produces identical warehouse state:

- **File level**: SQLite tracker skips files with the same path + MD5 hash
- **Row level**: `INSERT … ON CONFLICT DO UPDATE` in PostgreSQL
- **SLA flags**: `WHERE is_first_response_breached IS NULL` — only recalculates new rows

---

## Architecture Decisions

| Decision | Reason |
|---|---|
| PostgreSQL | Local/academic setup, psycopg2 lightweight, star schema maps naturally |
| Star schema | Clean dim/fact separation, SLA queries straightforward |
| SQLite for file tracking | Zero extra service, embedded, perfect for idempotency |
| SHA-256 for PII | Deterministic (same email → same hash), one-way, enables deduplication |
| Polling not inotify | Cross-platform, configurable interval, sufficient for micro-batch |
| SLA flags in OLAP | Source data explicitly omits them — computed post-load in SQL |
| Daemon thread for stream | Main thread controls lifecycle; stream thread dies on exit automatically |
| Daemon thread for alerts | Non-blocking — pipeline never waits for email delivery |

---

## Configuration Reference

All values in `config/pipeline_config.yaml`. Key settings:

| Setting | Default | Description |
|---|---|---|
| `database.host` | `localhost` | PostgreSQL host |
| `database.connect_retries` | `3` | Retry attempts on connection failure |
| `paths.stream_dir` | `data/input/stream` | Stream file root directory |
| `pipeline.stream_poll_interval_sec` | `30` | How often to scan for new stream files |
| `sla.first_response_minutes` | `1` | SLA threshold for first response |
| `sla.resolution_minutes` | `15` | SLA threshold for ticket resolution |
| `thresholds.max_orphan_rate` | `0.05` | Alert if orphan rate exceeds 5% |
| `alerts.enabled` | `true` | Set to `false` to disable email alerts |

---
