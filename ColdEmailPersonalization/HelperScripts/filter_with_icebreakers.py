#!/usr/bin/env python3
"""
Filter split CSV files to keep only rows that have icebreakers.

Usage:
    python filter_with_icebreakers.py Atlanta
    python filter_with_icebreakers.py Atlanta Boston Chicago
    python filter_with_icebreakers.py --all
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

# File suffixes for the 4 split files
SUFFIXES = ["1_50", "51_100", "101_250", "251"]


def process_city(city: str) -> dict:
    """
    Filter all 4 split files for a city to keep only rows with icebreakers.

    Returns a dict with before/after counts for each file.
    """
    results = {}

    for suffix in SUFFIXES:
        filename = f"{city}_{suffix}.csv"
        filepath = os.path.join(MARKETING_AGENCIES_DIR, filename)

        if not os.path.exists(filepath):
            print(f"  Warning: {filename} not found, skipping")
            continue

        # Read the CSV
        df = pd.read_csv(filepath)
        before_count = len(df)

        # Filter to keep only rows with non-empty icebreakers
        df_filtered = df[df['icebreaker'].notna() & (df['icebreaker'] != '')]
        after_count = len(df_filtered)

        # Overwrite the file
        df_filtered.to_csv(filepath, index=False)

        removed = before_count - after_count
        results[suffix] = {
            'before': before_count,
            'after': after_count,
            'removed': removed
        }
        print(f"  {filename}: {before_count} -> {after_count} rows (removed {removed})")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Filter split CSV files to keep only rows with icebreakers"
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

    print(f"Filtering {len(cities)} cities to keep only rows with icebreakers...\n")

    total_removed = 0
    total_kept = 0

    for city in cities:
        print(f"Processing {city}:")
        results = process_city(city)

        city_removed = sum(r['removed'] for r in results.values())
        city_kept = sum(r['after'] for r in results.values())
        total_removed += city_removed
        total_kept += city_kept
        print()

    # Print summary
    print("=" * 50)
    print(f"Total: Kept {total_kept} rows, removed {total_removed} rows without icebreakers")


if __name__ == "__main__":
    main()
