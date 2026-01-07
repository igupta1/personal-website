#!/usr/bin/env python3
"""
Fix curly braces in icebreakers.

1. Replace generic placeholders with actual values:
   - {name} -> First Name
   - {ShortCompanyName} -> Company Name
   - {specific_niche_detail} -> "digital transformation"
   - {core_value_or_theme} -> "transparent growth"

2. Remove braces around personalized text (e.g., {Fromm} -> Fromm)

Usage:
    python fix_icebreaker_braces.py Atlanta
    python fix_icebreaker_braces.py Atlanta Boston Chicago
    python fix_icebreaker_braces.py --all
"""

import argparse
import os
import re
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


def fix_icebreaker(icebreaker: str, first_name: str, company_name: str) -> tuple[str, int]:
    """
    Fix curly braces in an icebreaker.

    Returns (fixed_icebreaker, number_of_fixes)
    """
    if pd.isna(icebreaker) or icebreaker == '':
        return icebreaker, 0

    fixes = 0
    result = icebreaker

    # 1. Replace known placeholders (case-insensitive)
    placeholders = {
        r'\{name\}': first_name if pd.notna(first_name) else '',
        r'\{ShortCompanyName\}': company_name if pd.notna(company_name) else '',
        r'\{specific_niche_detail\}': 'digital transformation',
        r'\{core_value_or_theme\}': 'transparent growth',
    }

    for pattern, replacement in placeholders.items():
        matches = len(re.findall(pattern, result, re.IGNORECASE))
        if matches > 0:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
            fixes += matches

    # 2. Remove braces around any remaining text (personalized content)
    # Match {anything} and replace with just "anything"
    remaining_braces = re.findall(r'\{([^}]+)\}', result)
    if remaining_braces:
        result = re.sub(r'\{([^}]+)\}', r'\1', result)
        fixes += len(remaining_braces)

    return result, fixes


def process_city(city: str) -> dict:
    """
    Fix icebreaker braces for all 4 split files of a city.

    Returns a dict with fix counts for each file.
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

        if 'icebreaker' not in df.columns:
            print(f"  Warning: {filename} has no 'icebreaker' column, skipping")
            continue

        total_fixes = 0
        rows_fixed = 0

        # Fix each row
        for idx, row in df.iterrows():
            fixed_icebreaker, num_fixes = fix_icebreaker(
                row['icebreaker'],
                row.get('First Name', ''),
                row.get('Company Name', '')
            )
            if num_fixes > 0:
                df.at[idx, 'icebreaker'] = fixed_icebreaker
                total_fixes += num_fixes
                rows_fixed += 1

        # Save the file
        df.to_csv(filepath, index=False)

        results[suffix] = {
            'rows_fixed': rows_fixed,
            'total_fixes': total_fixes
        }

        if total_fixes > 0:
            print(f"  {filename}: Fixed {total_fixes} braces in {rows_fixed} rows")
        else:
            print(f"  {filename}: No braces to fix")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Fix curly braces in icebreakers"
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

    print(f"Fixing icebreaker braces for {len(cities)} cities...\n")

    total_fixes = 0
    total_rows = 0

    for city in cities:
        print(f"Processing {city}:")
        results = process_city(city)

        city_fixes = sum(r['total_fixes'] for r in results.values())
        city_rows = sum(r['rows_fixed'] for r in results.values())
        total_fixes += city_fixes
        total_rows += city_rows
        print()

    # Print summary
    print("=" * 50)
    print(f"Total: Fixed {total_fixes} braces in {total_rows} rows")


if __name__ == "__main__":
    main()
