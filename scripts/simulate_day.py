"""
FastFeast Day Simulation Script
================================
Simulates a full day of FastFeast operations by running all generators
in a realistic sequence.

Usage:
    python3 simulate_day.py --date 2026-02-22 --hours 8 9 12 14 18 20 --skip-master
"""

import os
import sys
import random
import argparse
import subprocess
from datetime import datetime


def run_command(cmd, description, verbose=False):
    """Run a command and handle output."""
    print(f"\n{'='*60}")
    print(f"[STEP] {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=not verbose, text=True)
    
    if result.returncode != 0:
        print(f"[ERROR] Command failed!")
        if not verbose and result.stderr:
            print(result.stderr)
        return False
    
    if verbose and result.stdout:
        print(result.stdout)
    
    print(f"[OK] {description} completed")
    return True


def check_master_data():
    """Check if master data exists."""
    return os.path.exists("data/master/metadata.json")


def main():
    parser = argparse.ArgumentParser(description="Simulate a full day of FastFeast operations")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--hours", type=int, nargs="+", default=[8, 10, 12, 14, 16, 18, 20, 22],
                        help="Hours to generate stream data for (default: 8 10 12 14 16 18 20 22)")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output from each script")
    parser.add_argument("--skip-master", action="store_true", help="Skip master data generation if it exists")
    args = parser.parse_args()
    
    run_date = args.date
    hours = sorted(args.hours)
    verbose = args.verbose
    
    # Validate date
    try:
        datetime.strptime(run_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{run_date}'. Use YYYY-MM-DD")
        return 1
    
    # Validate hours
    for h in hours:
        if h < 0 or h > 23:
            print(f"Error: Hour {h} is invalid. Must be 0-23")
            return 1
    
    print("=" * 60)
    print(f"FastFeast Day Simulation - {run_date}")
    print("=" * 60)
    print(f"Hours to simulate: {hours}")
    print(f"Verbose mode: {verbose}")
    
    # Track statistics
    stats = {
        "customers_added": 0,
        "drivers_added": 0,
        "hours_generated": 0,
        "orphan_opportunities": []
    }
    
    # Step 0: Generate master data if needed
    if not check_master_data():
        print("\n[INFO] Master data not found. Generating...")
        if not run_command(["python", "generate_master_data.py"], 
                          "Generate master data", verbose):
            return 1
    elif not args.skip_master:
        print("\n[INFO] Master data exists. Regenerating for fresh start...")
        if not run_command(["python", "generate_master_data.py"], 
                          "Regenerate master data", verbose):
            return 1
    else:
        print("\n[INFO] Using existing master data (--skip-master)")
    
    # Step 1: Generate batch data (morning snapshot)
    if not run_command(["python", "generate_batch_data.py", "--date", run_date],
                      f"Generate batch data for {run_date}", verbose):
        return 1
    
    # Step 2-N: Interleave new signups with stream generation
    # This creates realistic orphan scenarios
    
    for i, hour in enumerate(hours):
        # Before some hours, add new customers/drivers (creating orphan potential)
        if random.random() < 0.6:  # 60% chance before each hour
            new_customers = random.randint(2, 8)
            if not run_command(["python", "add_new_customers.py", "--count", str(new_customers)],
                              f"Add {new_customers} new customers (before hour {hour})", verbose):
                return 1
            stats["customers_added"] += new_customers
            stats["orphan_opportunities"].append(f"Customers before hour {hour}")
        
        if random.random() < 0.4:  # 40% chance for drivers
            new_drivers = random.randint(1, 4)
            if not run_command(["python", "add_new_drivers.py", "--count", str(new_drivers)],
                              f"Add {new_drivers} new drivers (before hour {hour})", verbose):
                return 1
            stats["drivers_added"] += new_drivers
            stats["orphan_opportunities"].append(f"Drivers before hour {hour}")
        
        # Generate stream data for this hour
        if not run_command(["python", "generate_stream_data.py", "--date", run_date, "--hour", str(hour)],
                          f"Generate stream data for hour {hour:02d}", verbose):
            return 1
        stats["hours_generated"] += 1
    
    # Final summary
    print("\n" + "=" * 60)
    print("SIMULATION COMPLETE")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
