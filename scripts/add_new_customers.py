import os
import json
import random
import argparse
import pandas as pd
from datetime import datetime, timedelta

MASTER_DIR = "data/master"


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
    return random.choice(["N/A", "", "abc12345"])


def gen_email(name, domain=None, valid=True):
    """Generate email address."""
    if not valid:
        return random.choice(["notanemail", "missing@domain", "@nodomain.com", ""])
    if domain is None:
        domain = random.choice(["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"])
    parts = name.lower().replace("-", "").split()
    user = random.choice([
        f"{parts[0]}.{parts[-1]}",
        f"{parts[0]}{parts[-1]}",
        f"{parts[0]}.{parts[-1]}{random.randint(1, 99)}"
    ])
    return f"{user}@{domain}"


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
    """Load regions for assigning to new customers."""
    regions_path = f"{MASTER_DIR}/regions.csv"
    
    if not os.path.exists(regions_path):
        raise FileNotFoundError(f"Regions not found: {regions_path}")
    
    return pd.read_csv(regions_path)


def generate_new_customers(start_id, count, regions, verbose=False):
    """Generate new customers starting from start_id."""
    region_ids = regions["region_id"].tolist()
    customers = []
    now = datetime.now()
    
    for i in range(count):
        customer_id = start_id + i
        gender = random.choice(["male", "female"])
        name = gen_name(gender)
        
        # New customers are less likely to have data quality issues (fresh data)
        # But still ~2% chance
        has_issue = random.random() < 0.02
        issue_type = random.choice(["invalid_email", "invalid_phone"]) if has_issue else None
        
        customer = {
            "customer_id": customer_id,
            "full_name": name,
            "email": gen_email(name, valid=(issue_type != "invalid_email")),
            "phone": gen_phone(valid=(issue_type != "invalid_phone")),
            "region_id": random.choice(region_ids),
            "segment_id": 1,  # New customers start as Regular
            "signup_date": now.date().isoformat(),
            "gender": gender,
            "created_at": now.isoformat(sep=" "),
            "updated_at": now.isoformat(sep=" ")
        }
        
        customers.append(customer)
        
        if verbose:
            print(f"  Created customer {customer_id}: {name}")
    
    return pd.DataFrame(customers)


def append_to_master(new_customers):
    """Append new customers to master customers.csv."""
    customers_path = f"{MASTER_DIR}/customers.csv"
    
    if not os.path.exists(customers_path):
        raise FileNotFoundError(f"Master customers file not found: {customers_path}")
    
    # Read existing
    existing = pd.read_csv(customers_path)
    
    # Append new
    combined = pd.concat([existing, new_customers], ignore_index=True)
    
    # Save back
    combined.to_csv(customers_path, index=False)
    
    return len(existing), len(combined)


def main():
    parser = argparse.ArgumentParser(description="Add new customers to master data")
    parser.add_argument("--count", type=int, required=True, help="Number of new customers to add")
    parser.add_argument("--verbose", action="store_true", help="Show details of each customer created")
    args = parser.parse_args()
    
    count = args.count
    verbose = args.verbose
    
    if count <= 0:
        print("Error: Count must be positive")
        return
    
    # Random seed (no fixed seed - each run is different)
    random.seed()
    
    print("=" * 60)
    print(f"FastFeast New Customers Generator")
    print("=" * 60)
    
    # Load current state
    print("\nLoading current state...")
    try:
        metadata = load_metadata()
        regions = load_regions()
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        return
    
    current_max = metadata.get("max_customer_id", 500)
    print(f"  Current max customer_id: {current_max}")
    
    # Generate new customers
    start_id = current_max + 1
    print(f"\nGenerating {count} new customers (IDs {start_id} to {start_id + count - 1})...")
    
    new_customers = generate_new_customers(start_id, count, regions, verbose)
    
    # Append to master file
    print("\nAppending to master customers.csv...")
    old_count, new_count = append_to_master(new_customers)
    print(f"  Previous count: {old_count}")
    print(f"  New count: {new_count}")
    
    # Update metadata
    new_max = current_max + count
    metadata["max_customer_id"] = new_max
    save_metadata(metadata)
    print(f"\nUpdated metadata:")
    print(f"  max_customer_id: {new_max}")
    
    print("\n" + "=" * 60)
    print("[SUMMARY]")
    print(f"  Added {count} new customers")
    print(f"  Customer IDs: {start_id} to {start_id + count - 1}")
    print(f"  Total customers in master: {new_count}")
    print("=" * 60)
    
    print("\n[IMPORTANT]")
    print("These customers are NOT in any existing batch data!")
    print("If orders reference these customer IDs, they will be ORPHANS.")
    print("Your pipeline must detect and handle orphan references.")
    print("=" * 60)


if __name__ == "__main__":
    main()
