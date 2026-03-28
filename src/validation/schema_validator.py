"""
validation/schema_validator.py
===============================
Schema registry for all 16 input tables.
Validates that a DataFrame has the required columns before any further processing.

Each table entry defines:
  required_cols : list   — columns that MUST be present (file is rejected if any missing)
  optional_cols : list   — columns that may or may not appear (never cause rejection)
  non_nullable  : list   — subset of required_cols that must not be null (row-level drop)
  numeric_cols  : list   — columns that should be castable to float
  date_cols     : list   — columns that should be parseable as datetime

validate_schema() returns:
  (cleaned_df, error_info)
  - On success : (df with correct dtypes coerced, None)
  - On failure : (empty DataFrame, error string)   ← skip the whole file

Usage:
    from src.validation.schema_validator import validate_schema
    df, err = validate_schema(raw_df, "customers")
    if err:
        # file-level failure — skip, quarantine, alert
"""

import pandas as pd
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── Schema Registry ────────────────────────────────────────────────────────────
# Every table the pipeline touches is registered here.
# Keeping it in one place makes it trivial to update if source schemas change.

SCHEMA_REGISTRY: dict[str, dict] = {

    # ── Lookup / dimension tables (batch) ─────────────────────────────────────

    "cities": {
        "required_cols": ["city_id", "city_name", "country", "timezone"],
        "optional_cols": [],
        "non_nullable":  ["city_id", "city_name"],
        "numeric_cols":  ["city_id"],
        "date_cols":     [],
    },

    "regions": {
        "required_cols": ["region_id", "region_name", "city_id", "delivery_base_fee"],
        "optional_cols": [],
        "non_nullable":  ["region_id", "region_name", "city_id"],
        "numeric_cols":  ["region_id", "city_id", "delivery_base_fee"],
        "date_cols":     [],
    },

    "segments": {
        "required_cols": ["segment_id", "segment_name", "discount_pct", "priority_support"],
        "optional_cols": [],
        "non_nullable":  ["segment_id", "segment_name"],
        "numeric_cols":  ["segment_id", "discount_pct"],
        "date_cols":     [],
    },

    "categories": {
        "required_cols": ["category_id", "category_name"],
        "optional_cols": [],
        "non_nullable":  ["category_id", "category_name"],
        "numeric_cols":  ["category_id"],
        "date_cols":     [],
    },

    "teams": {
        "required_cols": ["team_id", "team_name"],
        "optional_cols": [],
        "non_nullable":  ["team_id", "team_name"],
        "numeric_cols":  ["team_id"],
        "date_cols":     [],
    },

    "reason_categories": {
        "required_cols": ["reason_category_id", "category_name"],
        "optional_cols": [],
        "non_nullable":  ["reason_category_id", "category_name"],
        "numeric_cols":  ["reason_category_id"],
        "date_cols":     [],
    },

    "reasons": {
        "required_cols": ["reason_id", "reason_name", "reason_category_id",
                          "severity_level", "typical_refund_pct"],
        "optional_cols": [],
        "non_nullable":  ["reason_id", "reason_name", "reason_category_id"],
        "numeric_cols":  ["reason_id", "reason_category_id", "severity_level",
                          "typical_refund_pct"],
        "date_cols":     [],
    },

    "channels": {
        "required_cols": ["channel_id", "channel_name"],
        "optional_cols": [],
        "non_nullable":  ["channel_id", "channel_name"],
        "numeric_cols":  ["channel_id"],
        "date_cols":     [],
    },

    "priorities": {
        "required_cols": ["priority_id", "priority_code", "priority_name",
                          "sla_first_response_min", "sla_resolution_min"],
        "optional_cols": [],
        "non_nullable":  ["priority_id", "priority_name"],
        "numeric_cols":  ["priority_id", "sla_first_response_min", "sla_resolution_min"],
        "date_cols":     [],
    },

    # ── Entity tables (batch) ──────────────────────────────────────────────────

    "customers": {
        "required_cols": ["customer_id", "full_name", "email", "phone",
                          "region_id", "segment_id", "signup_date",
                          "gender", "created_at", "updated_at"],
        "optional_cols": [],
        "non_nullable":  ["customer_id"],
        "numeric_cols":  ["customer_id", "region_id", "segment_id"],
        "date_cols":     ["signup_date", "created_at", "updated_at"],
    },

    "restaurants": {
        "required_cols": ["restaurant_id", "restaurant_name", "region_id",
                          "category_id", "price_tier", "rating_avg",
                          "prep_time_avg_min", "is_active",
                          "created_at", "updated_at"],
        "optional_cols": [],
        "non_nullable":  ["restaurant_id"],
        "numeric_cols":  ["restaurant_id", "region_id", "category_id",
                          "rating_avg", "prep_time_avg_min"],
        "date_cols":     ["created_at", "updated_at"],
    },

    "drivers": {
        "required_cols": ["driver_id", "driver_name", "driver_phone",
                          "national_id", "region_id", "shift",
                          "vehicle_type", "hire_date", "rating_avg",
                          "on_time_rate", "cancel_rate",
                          "completed_deliveries", "is_active",
                          "created_at", "updated_at"],
        "optional_cols": [],
        "non_nullable":  ["driver_id"],
        "numeric_cols":  ["driver_id", "region_id", "rating_avg",
                          "on_time_rate", "cancel_rate",
                          "completed_deliveries"],
        "date_cols":     ["hire_date", "created_at", "updated_at"],
    },

    "agents": {
        "required_cols": ["agent_id", "agent_name", "agent_email",
                          "agent_phone", "team_id", "skill_level",
                          "hire_date", "avg_handle_time_min",
                          "resolution_rate", "csat_score",
                          "is_active", "created_at", "updated_at"],
        "optional_cols": [],
        "non_nullable":  ["agent_id", "agent_name"],
        "numeric_cols":  ["agent_id", "team_id", "avg_handle_time_min",
                          "resolution_rate", "csat_score"],
        "date_cols":     ["hire_date", "created_at", "updated_at"],
    },

    # ── Fact / transaction tables (stream) ────────────────────────────────────

    "orders": {
        "required_cols": ["order_id", "customer_id", "restaurant_id",
                          "driver_id", "region_id", "order_amount",
                          "delivery_fee", "discount_amount", "total_amount",
                          "order_status", "payment_method",
                          "order_created_at", "delivered_at"],
        "optional_cols": [],
        "non_nullable":  ["order_id", "customer_id", "restaurant_id",
                          "driver_id", "order_status", "order_created_at"],
        "numeric_cols":  ["customer_id", "restaurant_id", "driver_id",
                          "region_id", "order_amount", "delivery_fee",
                          "discount_amount", "total_amount"],
        "date_cols":     ["order_created_at", "delivered_at"],
    },

    "tickets": {
        "required_cols": ["ticket_id", "order_id", "customer_id",
                          "driver_id", "restaurant_id", "agent_id",
                          "reason_id", "priority_id", "channel_id",
                          "status", "refund_amount",
                          "created_at", "first_response_at", "resolved_at",
                          "sla_first_due_at", "sla_resolve_due_at"],
        "optional_cols": [],
        "non_nullable":  ["ticket_id", "order_id", "agent_id",
                          "status", "created_at"],
        "numeric_cols":  ["customer_id", "driver_id", "restaurant_id",
                          "agent_id", "reason_id", "priority_id",
                          "channel_id", "refund_amount"],
        "date_cols":     ["created_at", "first_response_at", "resolved_at",
                          "sla_first_due_at", "sla_resolve_due_at"],
    },

    "ticket_events": {
        "required_cols": ["event_id", "ticket_id", "agent_id",
                          "event_ts", "old_status", "new_status", "notes"],
        "optional_cols": [],
        "non_nullable":  ["event_id", "ticket_id", "agent_id",
                          "event_ts", "new_status"],
        "numeric_cols":  ["agent_id"],
        "date_cols":     ["event_ts"],
    },
}


# ── Validator ──────────────────────────────────────────────────────────────────

def validate_schema(
    df: pd.DataFrame,
    table_name: str,
) -> tuple[pd.DataFrame, Optional[str]]:
    """
    Validate that df conforms to the registered schema for table_name.

    Steps:
      1. Check table is in registry
      2. Check all required_cols are present  ← file-level failure if not
      3. Drop completely unknown columns       ← don't load garbage columns
      4. Coerce numeric_cols to numeric        ← non-coercible → NaN
      5. Coerce date_cols to datetime          ← non-parseable → NaT

    Returns:
      (cleaned_df, None)       on success
      (empty DataFrame, msg)   on file-level failure (missing required columns)
    """
    if table_name not in SCHEMA_REGISTRY:
        msg = f"Unknown table '{table_name}' — not in schema registry"
        logger.error("Schema validation failed", extra={"table": table_name, "error": msg})
        return pd.DataFrame(), msg

    schema = SCHEMA_REGISTRY[table_name]
    required = schema["required_cols"]
    numeric  = schema["numeric_cols"]
    dates    = schema["date_cols"]

    # ── Step 1: Check required columns ───────────────────────────────────────
    present = set(df.columns)
    missing = [c for c in required if c not in present]

    if missing:
        msg = f"Missing required columns: {missing}"
        logger.error(
            "Schema validation failed — file skipped",
            extra={"table": table_name, "missing_cols": missing, "present_cols": list(present)},
        )
        return pd.DataFrame(), msg

    # ── Step 2: Drop columns not in schema at all ─────────────────────────────
    known = set(required) | set(schema.get("optional_cols", []))
    extra = [c for c in df.columns if c not in known]
    if extra:
        logger.warning(
            "Dropping unknown columns",
            extra={"table": table_name, "dropped": extra},
        )
        df = df.drop(columns=extra)

    df = df.copy()

    # ── Step 3: Coerce numeric columns ────────────────────────────────────────
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Step 4: Coerce date columns ───────────────────────────────────────────
    from src.utils.date_utils import safe_parse_series
    for col in dates:
        if col in df.columns:
            df[col] = safe_parse_series(df[col])

    logger.info(
        "Schema validation passed",
        extra={"table": table_name, "rows": len(df), "cols": list(df.columns)},
    )
    return df, None
