"""
MSPSalesDiscovery CLI — Find IT MSPs hiring for sales/marketing roles.

Usage:
    python -m MSPSalesDiscovery run [--dry-run] [--max-searches N] [-v]
    python -m MSPSalesDiscovery status
"""

import argparse
import asyncio
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set

from .config import Config
from .core.serpapi_client import SerpAPIJobClient
from .core.decision_maker import MSPDecisionMakerFinder
from .core.models import SerpJobListing, DecisionMakerResult

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# --- Seen Companies (JSON dedup) ---

def load_seen_companies(path: Path) -> Set[str]:
    """Load set of normalized company names already processed."""
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        return set(data.get("companies", []))
    except (json.JSONDecodeError, OSError):
        return set()


def save_seen_companies(path: Path, seen: Set[str]):
    """Save seen company names to JSON."""
    path.write_text(json.dumps({"companies": sorted(seen)}, indent=2))


# --- CSV Output ---

CSV_COLUMNS = [
    "Company Name",
    "Company Website",
    "Job Title",
    "Job Location",
    "Job URL",
    "Posted Date",
    "Decision Maker Name",
    "Decision Maker Title",
    "Decision Maker LinkedIn",
    "Employee Count",
    "Confidence",
    "Search Metro",
    "Discovered Date",
]


def write_csv(output_dir: Path, rows: List[Dict]) -> Path:
    """Append rows to the running CSV file. Creates the file with headers if new."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "msp_sales_leads.csv"

    file_exists = csv_path.exists()

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    return csv_path


# --- Main Pipeline ---

async def run_pipeline(config: Config, dry_run: bool = False, max_searches: int = None):
    """Execute the full MSP sales discovery pipeline."""
    start_time = datetime.now()
    max_s = max_searches or config.max_searches_per_run

    # Step 1: SerpAPI search
    print("\n" + "=" * 70)
    print("Step 1: Searching for sales/marketing roles at IT MSPs...")
    print("=" * 70)

    if not config.serpapi_api_key:
        print("Error: SERPAPI_API_KEY not set.")
        return

    client = SerpAPIJobClient(
        api_key=config.serpapi_api_key,
        max_searches=max_s,
    )

    todays_metros = SerpAPIJobClient.get_next_metros(
        all_metros=config.metro_areas,
        count=config.metros_per_run,
        state_path=config.metro_state_path,
    )
    print(f"  Today's metros: {', '.join(todays_metros)}")
    print(f"  Query: {config.search_query[:80]}...")

    all_listings = client.search_all(
        query=config.search_query,
        metro_areas=todays_metros,
    )
    print(f"  Found {len(all_listings)} unique listings across {client.searches_used} searches")

    if not all_listings:
        print("No listings found. Exiting.")
        return

    # Step 2: Dedup against seen companies
    print("\n" + "=" * 70)
    print("Step 2: Deduplicating against previously seen companies...")
    print("=" * 70)

    seen = load_seen_companies(config.seen_companies_path)
    new_listings: List[SerpJobListing] = []
    for listing in all_listings:
        norm_name = listing.company_name.lower().strip()
        if norm_name not in seen:
            new_listings.append(listing)

    skipped = len(all_listings) - len(new_listings)
    print(f"  {len(new_listings)} new companies, {skipped} already seen")

    if not new_listings:
        print("All companies already seen. Exiting.")
        return

    # Step 3: Gemini enrichment
    print("\n" + "=" * 70)
    print(f"Step 3: Gemini enrichment for {len(new_listings)} companies...")
    print("=" * 70)

    enriched_rows: List[Dict] = []

    if config.gemini_api_key:
        finder = MSPDecisionMakerFinder(
            api_key=config.gemini_api_key,
            model=config.gemini_model,
            batch_size=config.gemini_batch_size,
        )

        # Build unique company list (multiple listings may be same company)
        companies_seen_this_run: Dict[str, str] = {}
        companies_for_lookup: List[Dict[str, str]] = []
        for listing in new_listings:
            norm = listing.company_name.lower().strip()
            if norm not in companies_seen_this_run:
                companies_seen_this_run[norm] = listing.company_name
                companies_for_lookup.append({"company": listing.company_name})

        print(f"  Unique companies to enrich: {len(companies_for_lookup)}")

        dm_results = await finder.find_decision_makers(companies_for_lookup)

        # Build lookup: normalized company name -> DecisionMakerResult
        dm_by_company: Dict[str, DecisionMakerResult] = {}
        for dm in dm_results:
            dm_by_company[dm.company_name.lower().strip()] = dm

        # Filter and merge
        rejected_not_msp = 0
        rejected_too_large = 0
        for listing in new_listings:
            norm = listing.company_name.lower().strip()
            dm = dm_by_company.get(norm)

            if dm and not dm.is_verified_msp:
                rejected_not_msp += 1
                print(f"  REJECTED (not MSP): {listing.company_name} — {dm.not_found_reason or 'N/A'}")
                seen.add(norm)
                continue

            if dm and dm.employee_count and dm.employee_count > config.max_employee_count:
                rejected_too_large += 1
                print(f"  REJECTED (>{config.max_employee_count} employees): {listing.company_name} ({dm.employee_count})")
                seen.add(norm)
                continue

            # Accepted
            seen.add(norm)
            person_name = ""
            if dm and dm.person_name and "not confidently" not in dm.person_name.lower():
                person_name = dm.person_name

            row = {
                "Company Name": listing.company_name,
                "Company Website": (dm.company_website if dm else "") or "",
                "Job Title": listing.title,
                "Job Location": listing.location,
                "Job URL": listing.job_url or "",
                "Posted Date": listing.posting_date.isoformat() if listing.posting_date else listing.posted_at,
                "Decision Maker Name": person_name,
                "Decision Maker Title": (dm.title if dm else "") or "",
                "Decision Maker LinkedIn": (dm.source_url if dm else "") or "",
                "Employee Count": dm.employee_count if dm and dm.employee_count else "",
                "Confidence": (dm.confidence if dm else "") or "",
                "Search Metro": listing.search_metro or "",
                "Discovered Date": datetime.now().strftime("%Y-%m-%d"),
            }
            enriched_rows.append(row)

        print(f"\n  Accepted: {len(enriched_rows)}")
        print(f"  Rejected (not MSP): {rejected_not_msp}")
        print(f"  Rejected (too large): {rejected_too_large}")
    else:
        print("  Skipping (GEMINI_API_KEY not set) — writing raw listings without enrichment")
        for listing in new_listings:
            norm = listing.company_name.lower().strip()
            seen.add(norm)
            row = {
                "Company Name": listing.company_name,
                "Company Website": "",
                "Job Title": listing.title,
                "Job Location": listing.location,
                "Job URL": listing.job_url or "",
                "Posted Date": listing.posting_date.isoformat() if listing.posting_date else listing.posted_at,
                "Decision Maker Name": "",
                "Decision Maker Title": "",
                "Decision Maker LinkedIn": "",
                "Employee Count": "",
                "Confidence": "",
                "Search Metro": listing.search_metro or "",
                "Discovered Date": datetime.now().strftime("%Y-%m-%d"),
            }
            enriched_rows.append(row)

    # Step 4: Write CSV and update seen companies
    print("\n" + "=" * 70)
    print("Step 4: Writing results...")
    print("=" * 70)

    if enriched_rows and not dry_run:
        csv_path = write_csv(config.output_dir, enriched_rows)
        print(f"  Wrote {len(enriched_rows)} rows to {csv_path}")
    elif dry_run:
        print("  DRY RUN — would write:")
        for row in enriched_rows:
            dm_info = f" | DM: {row['Decision Maker Name']} ({row['Decision Maker Title']})" if row['Decision Maker Name'] else ""
            print(f"    {row['Company Name']}: {row['Job Title']}{dm_info}")
    else:
        print("  No rows to write.")

    if not dry_run:
        save_seen_companies(config.seen_companies_path, seen)
        print(f"  Updated seen companies ({len(seen)} total)")

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 70)
    print("RUN SUMMARY")
    print("=" * 70)
    print(f"  Duration:              {elapsed:.1f}s")
    print(f"  SerpAPI searches used: {client.searches_used}")
    print(f"  Total listings found:  {len(all_listings)}")
    print(f"  New companies:         {len(new_listings)}")
    print(f"  Leads written to CSV:  {len(enriched_rows)}")
    print(f"  Total seen companies:  {len(seen)}")
    print("=" * 70)


def cmd_run(args):
    setup_logging(args.verbose)
    config = Config.from_env()
    try:
        asyncio.run(run_pipeline(config, dry_run=args.dry_run, max_searches=args.max_searches))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def cmd_status(args):
    config = Config.from_env()

    print("=" * 70)
    print("MSP Sales Discovery — Status")
    print("=" * 70)

    # Seen companies
    seen = load_seen_companies(config.seen_companies_path)
    print(f"\nTotal seen companies: {len(seen)}")

    # Metro state
    if config.metro_state_path.exists():
        try:
            state = json.loads(config.metro_state_path.read_text())
            idx = state.get("next_index", 0)
            next_metros = [
                config.metro_areas[i % len(config.metro_areas)]
                for i in range(idx, idx + config.metros_per_run)
            ]
            print(f"Next metros: {', '.join(next_metros)}")
            runs_to_full_cycle = len(config.metro_areas) // config.metros_per_run
            print(f"Full cycle: {runs_to_full_cycle} runs ({len(config.metro_areas)} metros / {config.metros_per_run} per run)")
        except (json.JSONDecodeError, OSError):
            pass

    # Output file
    csv_path = config.output_dir / "msp_sales_leads.csv"
    if csv_path.exists():
        with open(csv_path) as fh:
            row_count = sum(1 for _ in fh) - 1
        print(f"\nCSV: {csv_path.name} — {row_count} leads")
    else:
        print("\nNo CSV file yet.")


def main():
    parser = argparse.ArgumentParser(
        prog="python -m MSPSalesDiscovery",
        description="Find IT MSPs hiring for sales/marketing roles",
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run MSP sales discovery pipeline")
    run_parser.add_argument("--dry-run", action="store_true", help="Preview without writing files")
    run_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    run_parser.add_argument("--max-searches", type=int, default=None, help="Override max searches (default: 2)")
    run_parser.set_defaults(func=cmd_run)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show status and stats")
    status_parser.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
