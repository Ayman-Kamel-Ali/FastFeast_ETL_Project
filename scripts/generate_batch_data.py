import os
import json
import random
import argparse
import pandas as pd
from datetime import datetime

MASTER_DIR = "data/master"
BATCH_DIR = "data/input/batch"


def load_master_data():
    """Load all master CSV files."""
    data = {}
    tables = [
        "cities", "regions", "segments", "categories", "teams",
        "reason_categories", "reasons", "channels", "priorities",
        "customers", "restaurants", "drivers", "agents"
    ]

    for table in tables:
        filepath = f"{MASTER_DIR}/{table}.csv"
        if os.path.exists(filepath):
            data[table] = pd.read_csv(filepath)
        else:
            raise FileNotFoundError(f"Master file not found: {filepath}\nRun generate_master_data.py first!")

    return data


def apply_daily_drift(df, column, drift_range, min_val, max_val):
    """Apply small daily changes to numeric columns to simulate real-world variation."""
    df = df.copy()

    def drift(v):
        if pd.isna(v):
            return v
        new_val = v + random.uniform(*drift_range)
        return round(max(min_val, min(max_val, new_val)), 3)

    df[column] = df[column].apply(drift)
    return df


def generate_batch(run_date, master_data):
    """Generate daily batch data with small drift from master."""

    batch = {}

    # Lookup tables - no changes (static reference data)
    batch["cities"] = master_data["cities"].copy()
    batch["regions"] = master_data["regions"].copy()
    batch["segments"] = master_data["segments"].copy()
    batch["categories"] = master_data["categories"].copy()
    batch["teams"] = master_data["teams"].copy()
    batch["reason_categories"] = master_data["reason_categories"].copy()
    batch["reasons"] = master_data["reasons"].copy()
    batch["channels"] = master_data["channels"].copy()
    batch["priorities"] = master_data["priorities"].copy()

    # Entity tables - apply daily drift to simulate real-world changes

    # Drivers: small metric drift + some active status flips
    drivers = master_data["drivers"].copy()
    drivers = apply_daily_drift(drivers, "rating_avg", (-0.05, 0.05), 1.0, 5.0)
    drivers = apply_daily_drift(drivers, "on_time_rate", (-0.01, 0.01), 0.5, 1.0)
    drivers = apply_daily_drift(drivers, "cancel_rate", (-0.005, 0.005), 0.0, 0.15)
    
    # Flip ~2% active status
    flip_count = max(1, int(0.02 * len(drivers)))
    for idx in random.sample(range(len(drivers)), min(flip_count, len(drivers))):
        drivers.at[idx, "is_active"] = not drivers.at[idx, "is_active"]
    batch["drivers"] = drivers

    # Restaurants: small rating drift
    restaurants = master_data["restaurants"].copy()
    restaurants = apply_daily_drift(restaurants, "rating_avg", (-0.03, 0.03), 2.5, 5.0)
    batch["restaurants"] = restaurants

    # Agents: small performance drift
    agents = master_data["agents"].copy()
    agents = apply_daily_drift(agents, "resolution_rate", (-0.01, 0.01), 0.6, 1.0)
    agents = apply_daily_drift(agents, "csat_score", (-0.05, 0.05), 3.0, 5.0)
    batch["agents"] = agents

    # Customers: no changes (customer data is relatively static)
    batch["customers"] = master_data["customers"].copy()

    return batch


def save_batch(run_date, batch):
    """Save batch data in specified formats."""

    out_dir = f"{BATCH_DIR}/{run_date}"
    os.makedirs(out_dir, exist_ok=True)

    # CSV files
    csv_tables = [
        "customers", "drivers", "agents", "regions", "reasons",
        "categories", "segments", "teams", "channels", "priorities", "reason_categories"
    ]

    for table in csv_tables:
        batch[table].to_csv(f"{out_dir}/{table}.csv", index=False)

    # JSON files
    json_tables = ["restaurants", "cities"]

    for table in json_tables:
        with open(f"{out_dir}/{table}.json", "w") as f:
            json.dump(batch[table].to_dict(orient="records"), f, indent=2)

    return out_dir


def main():
    parser = argparse.ArgumentParser(description="Generate daily batch data")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    run_date = args.date

    # Validate date format
    try:
        datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{run_date}'. Use YYYY-MM-DD")
        return

    # Set seed based on date for reproducibility
    random.seed(hash(run_date) & 0xFFFFFFFF)

    print("=" * 60)
    print(f"FastFeast Batch Data Generator - {run_date}")
    print("=" * 60)

    # Load master data
    print("\nLoading master data...")
    try:
        master_data = load_master_data()
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        return

    print(f"  Loaded {len(master_data)} tables from master")

    # Generate batch with daily drift
    print("\nApplying daily drift...")
    batch = generate_batch(run_date, master_data)

    # Save batch data
    out_dir = save_batch(run_date, batch)

    print(f"\nBatch data exported to: {out_dir}/")
    
    print("\n[CSV FILES]")
    for table in ["customers", "drivers", "agents", "regions", "reasons",
                  "categories", "segments", "teams", "channels", "priorities", "reason_categories"]:
        print(f"  {table}.csv: {len(batch[table])} records")

    print("\n[JSON FILES]")
    for table in ["restaurants", "cities"]:
        print(f"  {table}.json: {len(batch[table])} records")

    # Show summary
    print("\n[SUMMARY]")
    print(f"  Total customers in batch: {len(batch['customers'])}")
    print(f"  Total drivers in batch: {len(batch['drivers'])}")
    print(f"  Total restaurants in batch: {len(batch['restaurants'])}")
    print(f"  Total agents in batch: {len(batch['agents'])}")

    print("\n" + "=" * 60)
    print("NOTE: Any customers/drivers added AFTER this batch was generated")
    print("      will create ORPHAN references in stream data.")
    print("=" * 60)


if __name__ == "__main__":
    main()
