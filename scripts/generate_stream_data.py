import os
import json
import uuid
import random
import argparse
import pandas as pd
from datetime import datetime, timedelta

MASTER_DIR = "data/master"
BATCH_DIR = "data/input/batch"
STREAM_DIR = "data/input/stream"

# SLA Config
SLA_FIRST_RESPONSE_MIN = 1
SLA_RESOLUTION_MIN = 15


def load_batch_data(run_date):
    """Load batch data for the given date."""
    batch_path = f"{BATCH_DIR}/{run_date}"

    if not os.path.exists(batch_path):
        raise FileNotFoundError(
            f"Batch data not found: {batch_path}\n"
            f"Run: python generate_batch_data.py --date {run_date}"
        )

    data = {}

    # CSV files
    csv_files = [
        "customers", "drivers", "agents", "regions", "reasons",
        "categories", "segments", "teams", "channels", "priorities", "reason_categories"
    ]
    for table in csv_files:
        filepath = f"{batch_path}/{table}.csv"
        if os.path.exists(filepath):
            data[table] = pd.read_csv(filepath)

    # JSON files
    json_files = ["restaurants", "cities"]
    for table in json_files:
        filepath = f"{batch_path}/{table}.json"
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                data[table] = pd.DataFrame(json.load(f))

    return data


def load_master_metadata():
    """Load master metadata to detect new customers/drivers."""
    metadata_path = f"{MASTER_DIR}/metadata.json"
    
    if os.path.exists(metadata_path):
        with open(metadata_path, "r") as f:
            return json.load(f)
    
    return {"max_customer_id": 500, "max_driver_id": 100}


def get_quality_issue_rates():
    """Generate random quality issue rates for this run."""
    return {
        "duplicate_rate": random.uniform(0.005, 0.03),
        "null_rate": random.uniform(0.01, 0.05),
        "invalid_date_rate": random.uniform(0.005, 0.02),
        "negative_amount_rate": random.uniform(0.002, 0.01),
        "orphan_rate": random.uniform(0.01, 0.03),  # Intentional orphan references
    }


def introduce_data_quality_issues(df, issue_rates, id_column):
    """Introduce random data quality issues."""
    df = df.copy()

    if len(df) == 0:
        return df

    # Null issues
    null_count = int(len(df) * issue_rates["null_rate"])
    if null_count > 0:
        for _ in range(null_count):
            idx = random.randint(0, len(df) - 1)
            cols = [c for c in df.columns if c not in [id_column, "order_id", "ticket_id", "event_id"]]
            if cols:
                df.at[idx, random.choice(cols)] = None

    # Invalid date issues
    date_count = int(len(df) * issue_rates["invalid_date_rate"])
    if date_count > 0:
        date_cols = [c for c in df.columns if "at" in c.lower() or "date" in c.lower()]
        if date_cols:
            for _ in range(date_count):
                idx = random.randint(0, len(df) - 1)
                df.at[idx, random.choice(date_cols)] = random.choice(["N/A", "invalid", ""])

    # Negative amount issues
    neg_count = int(len(df) * issue_rates["negative_amount_rate"])
    if neg_count > 0:
        amount_cols = [c for c in df.columns if "amount" in c.lower() or "fee" in c.lower()]
        if amount_cols:
            for _ in range(neg_count):
                idx = random.randint(0, len(df) - 1)
                col = random.choice(amount_cols)
                try:
                    df[col] = df[col].astype(float)
                    df.at[idx, col] = -abs(random.uniform(10, 100))
                except:
                    pass

    # Duplicates
    dup_count = int(len(df) * issue_rates["duplicate_rate"])
    if dup_count > 0 and len(df) > 0:
        dup_indices = random.sample(range(len(df)), min(dup_count, len(df)))
        duplicates = df.iloc[dup_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)

    return df


def introduce_orphan_references(df, batch_data, master_metadata, issue_rates):
    """
    Introduce orphan references - orders referencing customers/drivers 
    that don't exist in the batch data.
    
    This simulates:
    1. New customers who signed up after batch was generated
    2. New drivers who onboarded after batch was generated
    3. Data corruption (invalid IDs)
    """
    df = df.copy()
    
    if len(df) == 0:
        return df, {"customer_orphans": 0, "driver_orphans": 0, "restaurant_orphans": 0}
    
    orphan_stats = {"customer_orphans": 0, "driver_orphans": 0, "restaurant_orphans": 0}
    
    # Get max IDs from batch (what the pipeline will see as valid)
    batch_customer_ids = set(batch_data["customers"]["customer_id"].dropna().astype(int).tolist())
    batch_driver_ids = set(batch_data["drivers"]["driver_id"].dropna().astype(int).tolist())
    batch_restaurant_ids = set(batch_data["restaurants"]["restaurant_id"].dropna().astype(int).tolist())
    
    # Get max IDs from master (includes new customers/drivers added after batch)
    master_max_customer = master_metadata.get("max_customer_id", 500)
    master_max_driver = master_metadata.get("max_driver_id", 100)
    
    orphan_count = int(len(df) * issue_rates["orphan_rate"])
    
    for _ in range(orphan_count):
        idx = random.randint(0, len(df) - 1)
        orphan_type = random.choice(["customer", "driver", "restaurant"])
        
        if orphan_type == "customer":
            # Reference a customer that exists in master but not in batch
            # OR a completely invalid ID
            if master_max_customer > max(batch_customer_ids):
                # New customer added after batch
                new_customer_id = random.randint(max(batch_customer_ids) + 1, master_max_customer + 50)
            else:
                # Invalid ID (data corruption)
                new_customer_id = random.randint(90000, 99999)
            
            df.at[idx, "customer_id"] = new_customer_id
            orphan_stats["customer_orphans"] += 1
            
        elif orphan_type == "driver":
            # Reference a driver that exists in master but not in batch
            if master_max_driver > max(batch_driver_ids):
                new_driver_id = random.randint(max(batch_driver_ids) + 1, master_max_driver + 20)
            else:
                new_driver_id = random.randint(90000, 99999)
            
            df.at[idx, "driver_id"] = new_driver_id
            orphan_stats["driver_orphans"] += 1
            
        else:
            # Invalid restaurant ID (data corruption only - restaurants don't get added mid-day)
            df.at[idx, "restaurant_id"] = random.randint(90000, 99999)
            orphan_stats["restaurant_orphans"] += 1
    
    return df, orphan_stats


def random_datetime_in_hour(base_date, hour):
    """Generate random datetime within a specific hour."""
    return base_date.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))


def generate_orders(base_datetime, hour, batch_data, master_metadata, issue_rates, num_orders=None):
    """Generate orders for the hour."""

    customers = batch_data["customers"]
    restaurants = batch_data["restaurants"]
    drivers = batch_data["drivers"]
    regions = batch_data["regions"]

    # Hour multipliers (more orders during meal times)
    hour_mult = {
        0: 0.2, 1: 0.1, 2: 0.05, 3: 0.05, 4: 0.05, 5: 0.1,
        6: 0.3, 7: 0.5, 8: 0.6, 9: 0.5, 10: 0.6, 11: 0.9,
        12: 1.5, 13: 1.4, 14: 1.0, 15: 0.7, 16: 0.6, 17: 0.8,
        18: 1.3, 19: 1.8, 20: 2.0, 21: 1.8, 22: 1.2, 23: 0.6
    }

    if num_orders is None:
        base_orders = random.randint(100, 200)
        num_orders = int(base_orders * hour_mult.get(hour, 1.0))

    # Filter active entities
    active_restaurants = restaurants[restaurants["is_active"] == True]
    active_drivers = drivers[drivers["is_active"] == True]
    valid_customers = customers[customers["region_id"].notna()]

    if active_restaurants.empty:
        active_restaurants = restaurants
    if active_drivers.empty:
        active_drivers = drivers
    if valid_customers.empty:
        valid_customers = customers

    orders = []

    for _ in range(num_orders):
        customer = valid_customers.sample(1).iloc[0]
        region_id = customer["region_id"]

        if pd.isna(region_id):
            region_id = random.choice(regions["region_id"].tolist())

        region = regions[regions["region_id"] == region_id]
        if region.empty:
            region = regions.sample(1)
        region = region.iloc[0]

        # Same city preference for restaurant and driver
        city_id = region["city_id"]
        city_regions = regions[regions["city_id"] == city_id]["region_id"].tolist()

        rest_pool = active_restaurants[active_restaurants["region_id"].isin(city_regions)]
        if rest_pool.empty:
            rest_pool = active_restaurants
        restaurant = rest_pool.sample(1).iloc[0]

        driver_pool = active_drivers[active_drivers["region_id"].isin(city_regions)]
        if driver_pool.empty:
            driver_pool = active_drivers
        driver = driver_pool.sample(1).iloc[0]

        created_at = random_datetime_in_hour(base_datetime, hour)

        # Order amount by restaurant price tier
        tier = restaurant.get("price_tier", "Mid")
        if tier == "Low":
            amount = round(random.uniform(40, 120), 2)
        elif tier == "High":
            amount = round(random.uniform(150, 500), 2)
        else:
            amount = round(random.uniform(80, 250), 2)

        delivery_fee = round(region["delivery_base_fee"] + random.uniform(-3, 5), 2)
        discount = round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2)

        status = random.choices(["Delivered", "Cancelled", "Refunded"], weights=[0.96, 0.03, 0.01])[0]

        # Calculate delivery time based on restaurant prep time and driver on-time rate
        prep = restaurant.get("prep_time_avg_min", 20)
        if pd.isna(prep):
            prep = 20
        on_time = driver.get("on_time_rate", 0.8)
        if pd.isna(on_time) or on_time <= 0:
            on_time = 0.8

        delivery_time = int((prep + random.randint(15, 35)) / on_time)
        delivered_at = created_at + timedelta(minutes=delivery_time) if status == "Delivered" else None

        orders.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": int(customer["customer_id"]),
            "restaurant_id": int(restaurant["restaurant_id"]),
            "driver_id": int(driver["driver_id"]),
            "region_id": int(region_id),
            "order_amount": amount,
            "delivery_fee": delivery_fee,
            "discount_amount": discount,
            "total_amount": round(amount + delivery_fee - discount, 2),
            "order_status": status,
            "payment_method": random.choice(["card", "cash", "wallet"]),
            "order_created_at": created_at.isoformat(sep=" "),
            "delivered_at": delivered_at.isoformat(sep=" ") if delivered_at else None
        })

    df = pd.DataFrame(orders)
    
    # Introduce orphan references FIRST (before other quality issues)
    df, orphan_stats = introduce_orphan_references(df, batch_data, master_metadata, issue_rates)
    
    # Then introduce other quality issues
    df = introduce_data_quality_issues(df, issue_rates, "order_id")
    
    return df, orphan_stats


def generate_tickets(base_datetime, hour, orders, batch_data, issue_rates, ticket_rate=0.12):
    """Generate tickets and events based on orders."""

    if orders.empty:
        return pd.DataFrame(), pd.DataFrame()

    reasons = batch_data["reasons"]
    agents = batch_data["agents"]
    channels = batch_data["channels"]
    priorities = batch_data["priorities"]

    num_tickets = int(len(orders) * ticket_rate)
    if num_tickets == 0:
        return pd.DataFrame(), pd.DataFrame()

    ticket_orders = orders.sample(min(num_tickets, len(orders)))

    active_agents = agents[agents["is_active"] == True]
    if active_agents.empty:
        active_agents = agents

    # SLA breach rates (vary per run to simulate real-world variation)
    fr_breach_rate = random.uniform(0.15, 0.35)
    res_breach_rate = random.uniform(0.20, 0.45)

    tickets = []
    events = []

    for _, order in ticket_orders.iterrows():
        ticket_id = str(uuid.uuid4())

        try:
            order_time = datetime.fromisoformat(str(order["order_created_at"]))
        except:
            order_time = base_datetime.replace(hour=hour)

        created_at = order_time + timedelta(minutes=random.randint(5, 120))

        # Select reason
        reason = reasons.sample(1).iloc[0]
        severity = reason["severity_level"]

        # Priority based on severity
        if severity >= 4:
            priority_id = random.choices([1, 2, 3, 4], weights=[0.4, 0.35, 0.2, 0.05])[0]
        elif severity >= 3:
            priority_id = random.choices([1, 2, 3, 4], weights=[0.2, 0.4, 0.3, 0.1])[0]
        else:
            priority_id = random.choices([1, 2, 3, 4], weights=[0.1, 0.25, 0.4, 0.25])[0]

        # Agent assignment based on priority
        if priority_id == 1:
            senior = active_agents[active_agents["skill_level"].isin(["Senior", "Lead"])]
            agent = senior.sample(1).iloc[0] if not senior.empty else active_agents.sample(1).iloc[0]
        else:
            agent = active_agents.sample(1).iloc[0]

        channel_id = random.choice(channels["channel_id"].tolist())

        # SLA due times
        sla_first_due = created_at + timedelta(minutes=SLA_FIRST_RESPONSE_MIN)
        sla_resolve_due = created_at + timedelta(minutes=SLA_RESOLUTION_MIN)

        # Determine if SLA will be breached
        breach_fr = random.random() < fr_breach_rate
        breach_res = random.random() < res_breach_rate

        # First response time
        if breach_fr:
            fr_seconds = random.randint(90, 600)  # 1.5-10 min (breached)
        else:
            fr_seconds = random.randint(10, 55)  # 10-55 sec (within SLA)
        first_response_at = created_at + timedelta(seconds=fr_seconds)

        # Resolution time
        if breach_res:
            res_minutes = random.randint(16, 60)  # Breached
        else:
            res_minutes = random.randint(2, 14)  # Within SLA

        # Adjust for agent skill
        agent_rate = agent.get("resolution_rate", 0.8)
        if pd.isna(agent_rate) or agent_rate <= 0:
            agent_rate = 0.8
        res_minutes = int(res_minutes / agent_rate)

        resolved_at = created_at + timedelta(minutes=res_minutes)
        if resolved_at <= first_response_at:
            resolved_at = first_response_at + timedelta(minutes=random.randint(1, 5))

        # Refund calculation
        total = order.get("total_amount", 100)
        if pd.isna(total):
            total = 100
        refund_pct = reason["typical_refund_pct"]
        refund = round(total * refund_pct * random.uniform(0.8, 1.0), 2) if random.random() < 0.4 else 0.0

        status = random.choice(["Resolved", "Closed"])

        tickets.append({
            "ticket_id": ticket_id,
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "driver_id": order["driver_id"],
            "restaurant_id": order["restaurant_id"],
            "agent_id": int(agent["agent_id"]),
            "reason_id": int(reason["reason_id"]),
            "priority_id": priority_id,
            "channel_id": channel_id,
            "status": status,
            "refund_amount": refund,
            "created_at": created_at.isoformat(sep=" "),
            "first_response_at": first_response_at.isoformat(sep=" "),
            "resolved_at": resolved_at.isoformat(sep=" "),
            "sla_first_due_at": sla_first_due.isoformat(sep=" "),
            "sla_resolve_due_at": sla_resolve_due.isoformat(sep=" ")
            # NOTE: SLA breach flags should be calculated in OLAP, not here
        })

        # Generate ticket events (lifecycle)
        events.append({
            "event_id": str(uuid.uuid4()),
            "ticket_id": ticket_id,
            "agent_id": int(agent["agent_id"]),
            "event_ts": created_at.isoformat(sep=" "),
            "old_status": None,
            "new_status": "Open",
            "notes": "Ticket created"
        })

        events.append({
            "event_id": str(uuid.uuid4()),
            "ticket_id": ticket_id,
            "agent_id": int(agent["agent_id"]),
            "event_ts": first_response_at.isoformat(sep=" "),
            "old_status": "Open",
            "new_status": "InProgress",
            "notes": "Agent responded"
        })

        events.append({
            "event_id": str(uuid.uuid4()),
            "ticket_id": ticket_id,
            "agent_id": int(agent["agent_id"]),
            "event_ts": resolved_at.isoformat(sep=" "),
            "old_status": "InProgress",
            "new_status": status,
            "notes": f"Ticket {status.lower()}"
        })

        # 8% chance of ticket being reopened
        if random.random() < 0.08:
            reopen = resolved_at + timedelta(minutes=random.randint(30, 180))
            final = reopen + timedelta(minutes=random.randint(15, 120))

            events.append({
                "event_id": str(uuid.uuid4()),
                "ticket_id": ticket_id,
                "agent_id": int(agent["agent_id"]),
                "event_ts": reopen.isoformat(sep=" "),
                "old_status": status,
                "new_status": "Reopened",
                "notes": "Customer reported issue not resolved"
            })
            events.append({
                "event_id": str(uuid.uuid4()),
                "ticket_id": ticket_id,
                "agent_id": int(agent["agent_id"]),
                "event_ts": final.isoformat(sep=" "),
                "old_status": "Reopened",
                "new_status": "Resolved",
                "notes": "Issue finally resolved"
            })

    tickets_df = pd.DataFrame(tickets)
    events_df = pd.DataFrame(events)

    # Introduce quality issues in tickets (not events - events should be clean)
    tickets_df = introduce_data_quality_issues(tickets_df, issue_rates, "ticket_id")

    return tickets_df, events_df


def main():
    parser = argparse.ArgumentParser(description="Generate micro-batch stream data for a specific hour")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--hour", type=int, required=True, help="Hour (0-23)")
    args = parser.parse_args()

    run_date = args.date
    hour = args.hour

    if hour < 0 or hour > 23:
        print("Error: Hour must be between 0 and 23")
        return

    # Random seed for varied results each run (realistic)
    random.seed()

    print("=" * 60)
    print(f"FastFeast Micro-Batch Data Generator - {run_date} Hour {hour:02d}")
    print("=" * 60)

    # Quality issue rates for this run
    issue_rates = get_quality_issue_rates()
    print("\nQuality Issue Rates (this run):")
    for k, v in issue_rates.items():
        print(f"  {k}: {v*100:.1f}%")

    # Load batch data
    print(f"\nLoading batch data for {run_date}...")
    try:
        batch_data = load_batch_data(run_date)
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        return

    # Load master metadata (to detect new customers/drivers)
    master_metadata = load_master_metadata()
    print(f"\nMaster metadata:")
    print(f"  max_customer_id: {master_metadata.get('max_customer_id', 'N/A')}")
    print(f"  max_driver_id: {master_metadata.get('max_driver_id', 'N/A')}")

    base_datetime = datetime.strptime(run_date, "%Y-%m-%d")

    # Generate orders
    print(f"\nGenerating orders...")
    orders, orphan_stats = generate_orders(base_datetime, hour, batch_data, master_metadata, issue_rates)

    # Generate tickets
    print("Generating tickets and events...")
    tickets, events = generate_tickets(base_datetime, hour, orders, batch_data, issue_rates)

    # Save files
    out_dir = f"{STREAM_DIR}/{run_date}/{hour:02d}"
    os.makedirs(out_dir, exist_ok=True)

    # Orders as JSON
    with open(f"{out_dir}/orders.json", "w") as f:
        json.dump(orders.to_dict(orient="records"), f, indent=2)

    # Tickets as CSV
    if not tickets.empty:
        tickets.to_csv(f"{out_dir}/tickets.csv", index=False)
    else:
        # Create empty file with headers
        pd.DataFrame(columns=[
            "ticket_id", "order_id", "customer_id", "driver_id", "restaurant_id",
            "agent_id", "reason_id", "priority_id", "channel_id", "status",
            "refund_amount", "created_at", "first_response_at", "resolved_at",
            "sla_first_due_at", "sla_resolve_due_at"
        ]).to_csv(f"{out_dir}/tickets.csv", index=False)

    # Events as JSON
    if not events.empty:
        with open(f"{out_dir}/ticket_events.json", "w") as f:
            json.dump(events.to_dict(orient="records"), f, indent=2)
    else:
        with open(f"{out_dir}/ticket_events.json", "w") as f:
            json.dump([], f)

    print(f"\nMicro-batch data exported to: {out_dir}/")
    print(f"\n[RESULTS]")
    print(f"  orders.json: {len(orders)} records")
    print(f"  tickets.csv: {len(tickets)} records")
    print(f"  ticket_events.json: {len(events)} records")

    print(f"\n[ORPHAN REFERENCES INTRODUCED]")
    print(f"  Customer orphans: {orphan_stats['customer_orphans']}")
    print(f"  Driver orphans: {orphan_stats['driver_orphans']}")
    print(f"  Restaurant orphans: {orphan_stats['restaurant_orphans']}")
    total_orphans = sum(orphan_stats.values())
    if len(orders) > 0:
        print(f"  Total orphan rate: {total_orphans/len(orders)*100:.1f}%")

    print("\n" + "=" * 60)
    print("NOTE: Orphan references simulate real-world scenarios where")
    print("      customers/drivers are added AFTER the daily batch.")
    print("      Your pipeline must detect and handle these gracefully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
