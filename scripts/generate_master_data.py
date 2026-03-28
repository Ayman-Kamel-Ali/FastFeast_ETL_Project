"""
FastFeast Master Data Generator
================================================
Creates base OLTP source tables in CSV format.
Run this ONCE before running batch or stream scripts.

Output: data/master/*.csv

Usage:
    python generate_master_data.py
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)

MASTER_DIR = "data/master"
os.makedirs(MASTER_DIR, exist_ok=True)

# Initial counts - these will grow as new customers/drivers are added
INITIAL_CUSTOMERS = 500
INITIAL_RESTAURANTS = 60
INITIAL_DRIVERS = 100
NUM_AGENTS = 30

# Egyptian names
FIRST_NAMES_MALE = [
    "Ahmed", "Mohamed", "Omar", "Youssef", "Ali", "Hassan", "Hussein", "Mahmoud",
    "Mostafa", "Ibrahim", "Khaled", "Tarek", "Amr", "Karim", "Sherif", "Hossam",
    "Ayman", "Walid", "Essam", "Adel", "Hazem", "Ramy", "Samy", "Ashraf",
    "Nabil", "Fadi", "Ziad", "Bassem", "Wael", "Hany"
]

FIRST_NAMES_FEMALE = [
    "Fatma", "Nour", "Sara", "Mariam", "Aya", "Yasmin", "Hana", "Dina",
    "Rania", "Mona", "Laila", "Noha", "Salma", "Heba", "Amira", "Dalia",
    "Eman", "Samar", "Mai", "Farida", "Nada", "Reem", "Shahd", "Jana",
    "Malak", "Hoda", "Nevine", "Sherine", "Nagla", "Abeer"
]

LAST_NAMES = [
    "El-Sayed", "Hassan", "Mohamed", "Ibrahim", "Ali", "Ahmed", "Mahmoud", "Mostafa",
    "Khalil", "Abdel-Rahman", "El-Masry", "Farouk", "Naguib", "Saleh", "Mansour", "Fathy",
    "Soliman", "Abdallah", "El-Din", "Shaker", "Rizk", "Gaber", "Tawfik", "Zaki"
]

RESTAURANT_NAMES = {
    1: ["Chicken House", "Crispy Wings", "Golden Chicken", "Chicken Palace", "Clucky's", "Wing Masters"],
    2: ["Pizza Corner", "Italiano", "Cheesy Bites", "Pizza Express", "Dough Masters", "Slice Heaven"],
    3: ["Burger Lab", "Grill House", "Patty Palace", "Burger Station", "Smash Kitchen", "Bun & Beef"],
    4: ["Wok & Roll", "Dragon Kitchen", "Sushi Bar", "Noodle House", "Eastern Flavors", "Bamboo Garden"],
    5: ["Ocean Catch", "Fish Market", "Sea Breeze", "Lobster House", "Neptune's", "Anchor Grill"],
    6: ["Smoke House", "BBQ Station", "Fire Grill", "Meat Masters", "Charcoal Kitchen", "Flame & Fork"],
    7: ["Koshary El Prince", "Koshary Station", "Egyptian Bowl", "Koshary House", "Traditional Koshary"],
    8: ["Sweet Tooth", "Sugar Rush", "Dessert Lab", "Candy Corner", "Cake House", "Treats & Sweets"],
    9: ["Green Bowl", "Fresh Kitchen", "Salad Bar", "Fit Meals", "Clean Eats", "Veggie House"],
    10: ["Shawarma King", "Wrap Station", "Levant Kitchen", "Shawarma House", "Rolling Grill"]
}

PREP_TIME_BY_CATEGORY = {
    1: (12, 25), 2: (15, 30), 3: (10, 20), 4: (15, 35), 5: (20, 40),
    6: (15, 35), 7: (8, 15), 8: (10, 20), 9: (10, 20), 10: (8, 18)
}


def gen_name(gender=None):
    """Generate an Egyptian name."""
    if gender is None:
        gender = random.choice(["male", "female"])
    first = random.choice(FIRST_NAMES_MALE if gender == "male" else FIRST_NAMES_FEMALE)
    return f"{first} {random.choice(LAST_NAMES)}"


def gen_phone(valid=True):
    """Generate Egyptian phone number."""
    if valid:
        return f"{random.choice(['010', '011', '012', '015'])}{random.randint(10000000, 99999999)}"
    return random.choice(["N/A", "", "abc12345", f"020{random.randint(10000000, 99999999)}"])


def gen_email(name, domain=None, valid=True):
    """Generate email address."""
    if not valid:
        return random.choice(["notanemail", "missing@domain", "@nodomain.com", "", "N/A"])
    if domain is None:
        domain = random.choice(["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"])
    parts = name.lower().replace("-", "").split()
    user = random.choice([
        f"{parts[0]}.{parts[-1]}",
        f"{parts[0]}{parts[-1]}",
        f"{parts[0]}.{parts[-1]}{random.randint(1, 99)}"
    ])
    return f"{user}@{domain}"


def rand_date(start_year=2019, end_year=2026):
    """Generate random date within range."""
    start = datetime(start_year, 1, 1)
    delta = datetime(end_year, 1, 1) - start
    return start + timedelta(days=random.randint(0, delta.days))


# ============================================================
# LOOKUP TABLES
# ============================================================

def gen_cities():
    """Generate cities lookup table."""
    return pd.DataFrame([
        {"city_id": 1, "city_name": "Cairo", "country": "Egypt", "timezone": "Africa/Cairo"},
        {"city_id": 2, "city_name": "Giza", "country": "Egypt", "timezone": "Africa/Cairo"},
        {"city_id": 3, "city_name": "Alexandria", "country": "Egypt", "timezone": "Africa/Cairo"},
    ])


def gen_regions():
    """Generate regions lookup table."""
    return pd.DataFrame([
        {"region_id": 1, "region_name": "Maadi", "city_id": 1, "delivery_base_fee": 16.89},
        {"region_id": 2, "region_name": "Nasr City", "city_id": 1, "delivery_base_fee": 13.94},
        {"region_id": 3, "region_name": "Heliopolis", "city_id": 1, "delivery_base_fee": 15.76},
        {"region_id": 4, "region_name": "Downtown", "city_id": 1, "delivery_base_fee": 15.75},
        {"region_id": 5, "region_name": "New Cairo", "city_id": 1, "delivery_base_fee": 17.61},
        {"region_id": 6, "region_name": "Zamalek", "city_id": 1, "delivery_base_fee": 14.37},
        {"region_id": 7, "region_name": "Dokki", "city_id": 1, "delivery_base_fee": 14.57},
        {"region_id": 8, "region_name": "6th October", "city_id": 2, "delivery_base_fee": 15.86},
        {"region_id": 9, "region_name": "Sheikh Zayed", "city_id": 2, "delivery_base_fee": 14.85},
        {"region_id": 10, "region_name": "Haram", "city_id": 2, "delivery_base_fee": 16.59},
        {"region_id": 11, "region_name": "Faisal", "city_id": 2, "delivery_base_fee": 14.12},
        {"region_id": 12, "region_name": "Mohandessin", "city_id": 2, "delivery_base_fee": 19.23},
        {"region_id": 13, "region_name": "Smouha", "city_id": 3, "delivery_base_fee": 10.07},
        {"region_id": 14, "region_name": "Gleem", "city_id": 3, "delivery_base_fee": 11.28},
        {"region_id": 15, "region_name": "Miami", "city_id": 3, "delivery_base_fee": 11.32},
        {"region_id": 16, "region_name": "Montaza", "city_id": 3, "delivery_base_fee": 13.95},
        {"region_id": 17, "region_name": "Sidi Gaber", "city_id": 3, "delivery_base_fee": 11.67},
        {"region_id": 18, "region_name": "Stanley", "city_id": 3, "delivery_base_fee": 13.74},
    ])


def gen_segments():
    """Generate customer segments lookup table."""
    return pd.DataFrame([
        {"segment_id": 1, "segment_name": "Regular", "discount_pct": 0, "priority_support": False},
        {"segment_id": 2, "segment_name": "VIP", "discount_pct": 10, "priority_support": True},
    ])


def gen_categories():
    """Generate restaurant categories lookup table."""
    return pd.DataFrame([
        {"category_id": 1, "category_name": "Fried Chicken"},
        {"category_id": 2, "category_name": "Pizza"},
        {"category_id": 3, "category_name": "Burgers"},
        {"category_id": 4, "category_name": "Asian"},
        {"category_id": 5, "category_name": "Seafood"},
        {"category_id": 6, "category_name": "Grill"},
        {"category_id": 7, "category_name": "Koshary"},
        {"category_id": 8, "category_name": "Dessert"},
        {"category_id": 9, "category_name": "Healthy"},
        {"category_id": 10, "category_name": "Shawarma"},
    ])


def gen_teams():
    """Generate support teams lookup table."""
    return pd.DataFrame([
        {"team_id": 1, "team_name": "General Support"},
        {"team_id": 2, "team_name": "Escalations"},
        {"team_id": 3, "team_name": "VIP Support"},
        {"team_id": 4, "team_name": "Technical"},
        {"team_id": 5, "team_name": "Refunds"},
    ])


def gen_reason_categories():
    """Generate complaint reason categories lookup table."""
    return pd.DataFrame([
        {"reason_category_id": 1, "category_name": "Delivery"},
        {"reason_category_id": 2, "category_name": "Food"},
        {"reason_category_id": 3, "category_name": "Payment"},
    ])


def gen_reasons():
    """Generate complaint reasons lookup table."""
    return pd.DataFrame([
        {"reason_id": 1, "reason_name": "Late Delivery", "reason_category_id": 1, "severity_level": 3, "typical_refund_pct": 0.15},
        {"reason_id": 2, "reason_name": "Wrong Item", "reason_category_id": 2, "severity_level": 3, "typical_refund_pct": 0.50},
        {"reason_id": 3, "reason_name": "Missing Items", "reason_category_id": 2, "severity_level": 2, "typical_refund_pct": 0.30},
        {"reason_id": 4, "reason_name": "Cold Food", "reason_category_id": 2, "severity_level": 2, "typical_refund_pct": 0.25},
        {"reason_id": 5, "reason_name": "Payment Issue", "reason_category_id": 3, "severity_level": 3, "typical_refund_pct": 1.00},
        {"reason_id": 6, "reason_name": "Driver Behavior", "reason_category_id": 1, "severity_level": 4, "typical_refund_pct": 0.20},
        {"reason_id": 7, "reason_name": "Order Never Arrived", "reason_category_id": 1, "severity_level": 5, "typical_refund_pct": 1.00},
        {"reason_id": 8, "reason_name": "Poor Food Quality", "reason_category_id": 2, "severity_level": 2, "typical_refund_pct": 0.40},
        {"reason_id": 9, "reason_name": "Packaging Damaged", "reason_category_id": 1, "severity_level": 1, "typical_refund_pct": 0.10},
        {"reason_id": 10, "reason_name": "Allergic Reaction Concern", "reason_category_id": 2, "severity_level": 5, "typical_refund_pct": 1.00},
    ])


def gen_channels():
    """Generate support channels lookup table."""
    return pd.DataFrame([
        {"channel_id": 1, "channel_name": "app"},
        {"channel_id": 2, "channel_name": "chat"},
        {"channel_id": 3, "channel_name": "phone"},
        {"channel_id": 4, "channel_name": "email"},
    ])


def gen_priorities():
    """Generate ticket priorities lookup table."""
    return pd.DataFrame([
        {"priority_id": 1, "priority_code": "P1", "priority_name": "Critical", "sla_first_response_min": 1, "sla_resolution_min": 15},
        {"priority_id": 2, "priority_code": "P2", "priority_name": "High", "sla_first_response_min": 1, "sla_resolution_min": 15},
        {"priority_id": 3, "priority_code": "P3", "priority_name": "Medium", "sla_first_response_min": 1, "sla_resolution_min": 15},
        {"priority_id": 4, "priority_code": "P4", "priority_name": "Low", "sla_first_response_min": 1, "sla_resolution_min": 15},
    ])


# ============================================================
# ENTITY TABLES
# ============================================================

def gen_customers(regions, start_id=1, count=INITIAL_CUSTOMERS):
    """Generate customers table."""
    region_ids = regions["region_id"].tolist()
    customers = []

    for i in range(count):
        customer_id = start_id + i
        gender = random.choice(["male", "female"])
        name = gen_name(gender)

        # Data quality issues (~5%)
        has_issue = random.random() < 0.05
        issue_type = random.choice(["null_name", "invalid_email", "invalid_phone", "null_region"]) if has_issue else None

        customers.append({
            "customer_id": customer_id,
            "full_name": None if issue_type == "null_name" else name,
            "email": gen_email(name or "Unknown", valid=(issue_type != "invalid_email")),
            "phone": gen_phone(valid=(issue_type != "invalid_phone")),
            "region_id": None if issue_type == "null_region" else random.choice(region_ids),
            "segment_id": 2 if random.random() < 0.10 else 1,
            "signup_date": rand_date(2020, 2026).date().isoformat(),
            "gender": gender,
            "created_at": rand_date(2020, 2026).isoformat(sep=" "),
            "updated_at": datetime.now().isoformat(sep=" ")
        })

    # Add duplicates (~2%)
    dup_count = int(count * 0.02)
    if dup_count > 0:
        for idx in random.sample(range(len(customers)), min(dup_count, len(customers))):
            customers.append(customers[idx].copy())

    random.shuffle(customers)
    return pd.DataFrame(customers)


def gen_restaurants(regions, categories):
    """Generate restaurants table."""
    region_ids = regions["region_id"].tolist()
    restaurants = []
    restaurant_id = 1

    for category_id in categories["category_id"].tolist():
        names = RESTAURANT_NAMES.get(category_id, ["Restaurant"])
        prep_range = PREP_TIME_BY_CATEGORY.get(category_id, (10, 25))

        for name in names:
            # Data quality issues (~3%)
            has_issue = random.random() < 0.03
            issue_type = random.choice(["null_name", "invalid_rating", "null_category"]) if has_issue else None

            restaurants.append({
                "restaurant_id": restaurant_id,
                "restaurant_name": None if issue_type == "null_name" else name,
                "region_id": random.choice(region_ids),
                "category_id": None if issue_type == "null_category" else category_id,
                "price_tier": random.choice(["Low", "Mid", "High"]),
                "rating_avg": random.choice([-1, 6, None]) if issue_type == "invalid_rating" else round(random.uniform(3.2, 4.9), 2),
                "prep_time_avg_min": random.randint(*prep_range),
                "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
                "created_at": rand_date(2019, 2024).isoformat(sep=" "),
                "updated_at": datetime.now().isoformat(sep=" ")
            })
            restaurant_id += 1

    return pd.DataFrame(restaurants)


def gen_drivers(regions, start_id=1, count=INITIAL_DRIVERS):
    """Generate drivers table."""
    region_ids = regions["region_id"].tolist()
    drivers = []

    for i in range(count):
        driver_id = start_id + i
        name = gen_name("male")
        vehicle = random.choice(["bike", "motorbike", "car"])
        on_time = round(random.uniform(0.80, 0.98) if vehicle == "bike" else random.uniform(0.70, 0.95), 3)
        hire_date = rand_date(2019, 2026)
        days = (datetime(2026, 2, 1) - hire_date).days

        # Data quality issues (~4%)
        has_issue = random.random() < 0.04
        issue_type = random.choice(["invalid_phone", "invalid_national_id", "null_name", "invalid_rate"]) if has_issue else None

        drivers.append({
            "driver_id": driver_id,
            "driver_name": None if issue_type == "null_name" else name,
            "driver_phone": gen_phone(valid=(issue_type != "invalid_phone")),
            "national_id": random.choice(["", "N/A", "123", None]) if issue_type == "invalid_national_id" else f"{random.randint(2, 3)}{random.randint(10000000000000, 99999999999999)}",
            "region_id": random.choice(region_ids),
            "shift": random.choice(["morning", "evening", "night"]),
            "vehicle_type": vehicle,
            "hire_date": hire_date.date().isoformat(),
            "rating_avg": round(random.uniform(3.5, 5.0), 2),
            "on_time_rate": random.choice([-0.5, 1.5, None]) if issue_type == "invalid_rate" else on_time,
            "cancel_rate": round(random.uniform(0.00, 0.08), 3),
            "completed_deliveries": int(days * random.uniform(5, 15) * random.uniform(0.6, 0.9)),
            "is_active": random.choices([True, False], weights=[0.92, 0.08])[0],
            "created_at": hire_date.isoformat(sep=" "),
            "updated_at": datetime.now().isoformat(sep=" ")
        })

    # Add duplicates (~1%)
    dup_count = int(count * 0.01)
    if dup_count > 0:
        for idx in random.sample(range(len(drivers)), min(dup_count, len(drivers))):
            drivers.append(drivers[idx].copy())

    return pd.DataFrame(drivers)


def gen_agents(teams):
    """Generate support agents table."""
    team_ids = teams["team_id"].tolist()
    agents = []

    for i in range(NUM_AGENTS):
        gender = random.choice(["male", "female"])
        name = gen_name(gender)
        hire_date = rand_date(2020, 2026)
        days = (datetime(2026, 2, 1) - hire_date).days

        # Skill level based on tenure
        if days > 1000:
            skill = random.choices(["Junior", "Mid", "Senior", "Lead"], weights=[0.1, 0.2, 0.5, 0.2])[0]
        elif days > 500:
            skill = random.choices(["Junior", "Mid", "Senior", "Lead"], weights=[0.2, 0.5, 0.25, 0.05])[0]
        else:
            skill = random.choices(["Junior", "Mid", "Senior", "Lead"], weights=[0.6, 0.3, 0.1, 0.0])[0]

        # Performance metrics based on skill
        metrics = {
            "Lead": (random.randint(3, 8), round(random.uniform(0.92, 0.99), 3), round(random.uniform(4.5, 5.0), 2)),
            "Senior": (random.randint(4, 10), round(random.uniform(0.88, 0.96), 3), round(random.uniform(4.2, 4.9), 2)),
            "Mid": (random.randint(6, 15), round(random.uniform(0.82, 0.92), 3), round(random.uniform(3.8, 4.5), 2)),
            "Junior": (random.randint(10, 25), round(random.uniform(0.75, 0.88), 3), round(random.uniform(3.5, 4.2), 2)),
        }
        handle_time, resolution_rate, csat = metrics[skill]

        # Data quality issues (~3%)
        has_issue = random.random() < 0.03
        issue_type = random.choice(["invalid_email", "invalid_phone", "null_team"]) if has_issue else None

        agents.append({
            "agent_id": i + 1,
            "agent_name": name,
            "agent_email": gen_email(name, "fastfeast.com", valid=(issue_type != "invalid_email")),
            "agent_phone": gen_phone(valid=(issue_type != "invalid_phone")),
            "team_id": None if issue_type == "null_team" else random.choice(team_ids),
            "skill_level": skill,
            "hire_date": hire_date.date().isoformat(),
            "avg_handle_time_min": handle_time,
            "resolution_rate": resolution_rate,
            "csat_score": csat,
            "is_active": random.choices([True, False], weights=[0.90, 0.10])[0],
            "created_at": hire_date.isoformat(sep=" "),
            "updated_at": datetime.now().isoformat(sep=" ")
        })

    return pd.DataFrame(agents)


def save_metadata(customers_count, drivers_count):
    """Save metadata for tracking max IDs."""
    metadata = {
        "max_customer_id": customers_count,
        "max_driver_id": drivers_count,
        "last_updated": datetime.now().isoformat()
    }
    
    import json
    with open(f"{MASTER_DIR}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    return metadata


def main():
    print("=" * 60)
    print("FastFeast Master Data Generator")
    print("=" * 60)

    # Lookup tables
    print("\n[LOOKUP TABLES]")

    cities = gen_cities()
    cities.to_csv(f"{MASTER_DIR}/cities.csv", index=False)
    print(f"  cities.csv: {len(cities)} records")

    regions = gen_regions()
    regions.to_csv(f"{MASTER_DIR}/regions.csv", index=False)
    print(f"  regions.csv: {len(regions)} records")

    segments = gen_segments()
    segments.to_csv(f"{MASTER_DIR}/segments.csv", index=False)
    print(f"  segments.csv: {len(segments)} records")

    categories = gen_categories()
    categories.to_csv(f"{MASTER_DIR}/categories.csv", index=False)
    print(f"  categories.csv: {len(categories)} records")

    teams = gen_teams()
    teams.to_csv(f"{MASTER_DIR}/teams.csv", index=False)
    print(f"  teams.csv: {len(teams)} records")

    reason_categories = gen_reason_categories()
    reason_categories.to_csv(f"{MASTER_DIR}/reason_categories.csv", index=False)
    print(f"  reason_categories.csv: {len(reason_categories)} records")

    reasons = gen_reasons()
    reasons.to_csv(f"{MASTER_DIR}/reasons.csv", index=False)
    print(f"  reasons.csv: {len(reasons)} records")

    channels = gen_channels()
    channels.to_csv(f"{MASTER_DIR}/channels.csv", index=False)
    print(f"  channels.csv: {len(channels)} records")

    priorities = gen_priorities()
    priorities.to_csv(f"{MASTER_DIR}/priorities.csv", index=False)
    print(f"  priorities.csv: {len(priorities)} records")

    # Entity tables
    print("\n[ENTITY TABLES]")

    customers = gen_customers(regions)
    customers.to_csv(f"{MASTER_DIR}/customers.csv", index=False)
    print(f"  customers.csv: {len(customers)} records")

    restaurants = gen_restaurants(regions, categories)
    restaurants.to_csv(f"{MASTER_DIR}/restaurants.csv", index=False)
    print(f"  restaurants.csv: {len(restaurants)} records")

    drivers = gen_drivers(regions)
    drivers.to_csv(f"{MASTER_DIR}/drivers.csv", index=False)
    print(f"  drivers.csv: {len(drivers)} records")

    agents = gen_agents(teams)
    agents.to_csv(f"{MASTER_DIR}/agents.csv", index=False)
    print(f"  agents.csv: {len(agents)} records")

    # Save metadata for tracking IDs
    metadata = save_metadata(INITIAL_CUSTOMERS, INITIAL_DRIVERS)
    print(f"\n[METADATA]")
    print(f"  metadata.json: max_customer_id={metadata['max_customer_id']}, max_driver_id={metadata['max_driver_id']}")

    print("\n" + "=" * 60)
    print(f"Master data saved to: {MASTER_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
