import os
import json
import random
import argparse
import pandas as pd
from datetime import datetime

MASTER_DIR = "data/master"

FIRST_NAMES_MALE = [
    "Ahmed", "Mohamed", "Omar", "Youssef", "Ali", "Hassan", "Hussein", "Mahmoud",
    "Mostafa", "Ibrahim", "Khaled", "Tarek", "Amr", "Karim", "Sherif", "Hossam",
    "Ayman", "Walid", "Essam", "Adel", "Hazem", "Ramy", "Samy", "Ashraf",
    "Nabil", "Fadi", "Ziad", "Bassem", "Wael", "Hany", "Tamer", "Yasser",
    "Hatem", "Osama", "Gamal", "Ehab", "Sameh", "Magdy", "Medhat", "Safwat"
]

LAST_NAMES = [
    "El-Sayed", "Hassan", "Mohamed", "Ibrahim", "Ali", "Ahmed", "Mahmoud", "Mostafa",
    "Khalil", "Abdel-Rahman", "El-Masry", "Farouk", "Naguib", "Saleh", "Mansour", "Fathy",
    "Soliman", "Abdallah", "El-Din", "Shaker", "Rizk", "Gaber", "Tawfik", "Zaki"
]


def gen_name():
    """Generate an Egyptian male name."""
    first = random.choice(FIRST_NAMES_MALE)
    return f"{first} {random.choice(LAST_NAMES)}"


def gen_phone(valid=True):
    """Generate Egyptian phone number."""
    if valid:
        return f"{random.choice(['010', '011', '012', '015'])}{random.randint(10000000, 99999999)}"
    return random.choice(["N/A", "", "abc12345"])


def gen_national_id(valid=True):
    """Generate Egyptian national ID."""
    if valid:
        return f"{random.randint(2, 3)}{random.randint(10000000000000, 99999999999999)}"
    return random.choice(["", "N/A", "123", None])


def load_metadata():
    """Load current metadata."""
    metadata_path = f"{MASTER_DIR}/metadata.json"
    
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(
            f"Metadata not found: {metadata_path}\n"
            f"Run generate_master_data.py first!"
        )
    
    with open(metadata_path, "r") as f:
        return json.load(f)


def save_metadata(metadata):
    """Save updated metadata."""
    metadata_path = f"{MASTER_DIR}/metadata.json"
    metadata["last_updated"] = datetime.now().isoformat()
    
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)


def load_regions():
    """Load regions for assigning to new drivers."""
    regions_path = f"{MASTER_DIR}/regions.csv"
    
    if not os.path.exists(regions_path):
        raise FileNotFoundError(f"Regions not found: {regions_path}")
    
    return pd.read_csv(regions_path)


def generate_new_drivers(start_id, count, regions, verbose=False):
    """Generate new drivers starting from start_id."""
    region_ids = regions["region_id"].tolist()
    drivers = []
    now = datetime.now()
    
    for i in range(count):
        driver_id = start_id + i
        name = gen_name()
        vehicle = random.choice(["bike", "motorbike", "car"])
        
        # New drivers start with average metrics
        on_time_rate = round(random.uniform(0.80, 0.90), 3)  # New drivers are decent
        
        # Data quality issues (~2% for new drivers)
        has_issue = random.random() < 0.02
        issue_type = random.choice(["invalid_phone", "invalid_national_id"]) if has_issue else None
        
        driver = {
            "driver_id": driver_id,
            "driver_name": name,
            "driver_phone": gen_phone(valid=(issue_type != "invalid_phone")),
            "national_id": gen_national_id(valid=(issue_type != "invalid_national_id")),
            "region_id": random.choice(region_ids),
            "shift": random.choice(["morning", "evening", "night"]),
            "vehicle_type": vehicle,
            "hire_date": now.date().isoformat(),
            "rating_avg": round(random.uniform(4.0, 4.5), 2),  # New drivers start with good ratings
            "on_time_rate": on_time_rate,
            "cancel_rate": round(random.uniform(0.00, 0.02), 3),  # Low cancel rate initially
            "completed_deliveries": 0,  # Brand new driver
            "is_active": True,  # New drivers are active
            "created_at": now.isoformat(sep=" "),
            "updated_at": now.isoformat(sep=" ")
        }
        
        drivers.append(driver)
        
        if verbose:
            print(f"  Created driver {driver_id}: {name} ({vehicle})")
    
    return pd.DataFrame(drivers)


def append_to_master(new_drivers):
    """Append new drivers to master drivers.csv."""
    drivers_path = f"{MASTER_DIR}/drivers.csv"
    
    if not os.path.exists(drivers_path):
        raise FileNotFoundError(f"Master drivers file not found: {drivers_path}")
    
    # Read existing
    existing = pd.read_csv(drivers_path)
    
    # Append new
    combined = pd.concat([existing, new_drivers], ignore_index=True)
    
    # Save back
    combined.to_csv(drivers_path, index=False)
    
    return len(existing), len(combined)


def main():
    parser = argparse.ArgumentParser(description="Add new drivers to master data")
    parser.add_argument("--count", type=int, required=True, help="Number of new drivers to add")
    parser.add_argument("--verbose", action="store_true", help="Show details of each driver created")
    args = parser.parse_args()
    
    count = args.count
    verbose = args.verbose
    
    if count <= 0:
        print("Error: Count must be positive")
        return
    
    # Random seed (no fixed seed - each run is different)
    random.seed()
    
    print("=" * 60)
    print(f"FastFeast New Drivers Generator")
    print("=" * 60)
    
    # Load current state
    print("\nLoading current state...")
    try:
        metadata = load_metadata()
        regions = load_regions()
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        return
    
    current_max = metadata.get("max_driver_id", 100)
    print(f"  Current max driver_id: {current_max}")
    
    # Generate new drivers
    start_id = current_max + 1
    print(f"\nGenerating {count} new drivers (IDs {start_id} to {start_id + count - 1})...")
    
    new_drivers = generate_new_drivers(start_id, count, regions, verbose)
    
    # Append to master file
    print("\nAppending to master drivers.csv...")
    old_count, new_count = append_to_master(new_drivers)
    print(f"  Previous count: {old_count}")
    print(f"  New count: {new_count}")
    
    # Update metadata
    new_max = current_max + count
    metadata["max_driver_id"] = new_max
    save_metadata(metadata)
    print(f"\nUpdated metadata:")
    print(f"  max_driver_id: {new_max}")
    
    print("\n" + "=" * 60)
    print("[SUMMARY]")
    print(f"  Added {count} new drivers")
    print(f"  Driver IDs: {start_id} to {start_id + count - 1}")
    print(f"  Total drivers in master: {new_count}")
    print("=" * 60)
    
    print("\n[IMPORTANT]")
    print("These drivers are NOT in any existing batch data!")
    print("If orders are assigned to these driver IDs, they will be ORPHANS.")
    print("Your pipeline must detect and handle orphan references.")
    print("=" * 60)


if __name__ == "__main__":
    main()
