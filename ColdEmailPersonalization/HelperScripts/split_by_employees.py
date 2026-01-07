#!/usr/bin/env python3
"""
Split city CSV files by employee count and add icebreakers.

Usage:
    python split_by_employees.py Atlanta
    python split_by_employees.py Atlanta Boston Chicago
    python split_by_employees.py --all
"""

import argparse
import os
import sys
import pandas as pd

# Directory containing the CSV files
MARKETING_AGENCIES_DIR = os.path.join(os.path.dirname(__file__), "MarketingAgencies")

# All available cities
ALL_CITIES = [
    "Atlanta", "Austin", "BayArea", "Boston", "Chicago", "Dallas",
    "General", "Houston", "LosAngeles", "Miami", "NewYork",
    "Philadelphia", "Phoenix", "SanDiego", "Seattle"
]

# Employee count buckets
BUCKETS = [
    ("1_50", 1, 50),
    ("51_100", 51, 100),
    ("101_250", 101, 250),
    ("251", 251, float('inf')),
]


def process_city(city: str) -> dict:
    """
    Process a single city: split by employee count and add icebreakers.

    Returns a dict with counts for each bucket.
    """
    base_file = os.path.join(MARKETING_AGENCIES_DIR, f"{city}.csv")
    icebreakers_file = os.path.join(MARKETING_AGENCIES_DIR, f"{city}_with_icebreakers.csv")

    # Check if files exist
    if not os.path.exists(base_file):
        print(f"Error: {base_file} not found")
        return {}

    if not os.path.exists(icebreakers_file):
        print(f"Warning: {icebreakers_file} not found, proceeding without icebreakers")
        icebreakers_df = None
    else:
        icebreakers_df = pd.read_csv(icebreakers_file)

    # Read the main CSV
    df = pd.read_csv(base_file)

    # Create email -> icebreaker lookup
    icebreaker_lookup = {}
    if icebreakers_df is not None:
        for _, row in icebreakers_df.iterrows():
            email = row.get('email', '')
            icebreaker = row.get('multiline_icebreaker', '')
            if email and pd.notna(email):
                icebreaker_lookup[email.lower().strip()] = icebreaker if pd.notna(icebreaker) else ''

    # Add icebreaker column by matching on email
    def get_icebreaker(email):
        if pd.isna(email):
            return ''
        return icebreaker_lookup.get(email.lower().strip(), '')

    df['icebreaker'] = df['Email'].apply(get_icebreaker)

    # Get employee counts column
    employees_col = '# Employees'

    results = {}

    for bucket_name, min_emp, max_emp in BUCKETS:
        # Filter rows by employee count
        mask = (df[employees_col] >= min_emp) & (df[employees_col] <= max_emp)
        bucket_df = df[mask].copy()

        # Output file path
        output_file = os.path.join(MARKETING_AGENCIES_DIR, f"{city}_{bucket_name}.csv")

        # Write to CSV
        bucket_df.to_csv(output_file, index=False)

        results[bucket_name] = len(bucket_df)
        print(f"  {city}_{bucket_name}.csv: {len(bucket_df)} rows")

    # Count rows with icebreakers
    icebreaker_count = df['icebreaker'].apply(lambda x: x != '' if pd.notna(x) else False).sum()
    print(f"  Total rows with icebreakers: {icebreaker_count}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Split city CSV files by employee count and add icebreakers"
    )
    parser.add_argument(
        'cities',
        nargs='*',
        help='City names to process (e.g., Atlanta Boston Chicago)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all available cities'
    )

    args = parser.parse_args()

    # Determine which cities to process
    if args.all:
        cities = ALL_CITIES
    elif args.cities:
        cities = args.cities
    else:
        print("Error: Please specify city names or use --all")
        print(f"Available cities: {', '.join(ALL_CITIES)}")
        sys.exit(1)

    # Validate city names
    for city in cities:
        if city not in ALL_CITIES:
            print(f"Warning: '{city}' is not in the known cities list. Attempting anyway...")

    print(f"Processing {len(cities)} cities...\n")

    total_results = {}
    for city in cities:
        print(f"Processing {city}:")
        results = process_city(city)
        total_results[city] = results
        print()

    # Print summary
    print("=" * 50)
    print("Summary:")
    print("=" * 50)
    for city, results in total_results.items():
        total = sum(results.values())
        print(f"{city}: {total} total rows split into {len(results)} files")


if __name__ == "__main__":
    main()
