"""
ITMSPDiscovery CLI

Usage:
    python -m ITMSPDiscovery run [--dry-run] [--max-searches N]
    python -m ITMSPDiscovery status
    python -m ITMSPDiscovery upload [--api-key KEY] [--dry-run]
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime

from .config import Config
from .core.database import Database
from .core.orchestrator import ITMSPOrchestrator


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def cmd_run(args):
    """Run IT MSP lead discovery pipeline."""
    setup_logging(args.verbose)

    print("=" * 70)
    print(f"IT MSP Discovery Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if args.dry_run:
        print("DRY RUN MODE - No database writes will be made")

    config = Config.from_env()
    if args.skip_decision_makers:
        config.enable_decision_maker_lookup = False

    db = Database(config.db_path)

    orchestrator = ITMSPOrchestrator(
        config=config,
        database=db,
        dry_run=args.dry_run,
        max_searches=args.max_searches,
    )

    try:
        asyncio.run(orchestrator.run())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


def cmd_status(args):
    """Show current status and statistics."""
    config = Config.from_env()

    if not config.db_path.exists():
        print("No database found. Run 'run' first to initialize.")
        return

    db = Database(config.db_path)

    try:
        stats = db.get_statistics()

        print("=" * 70)
        print("IT MSP Discovery Status")
        print("=" * 70)
        print(f"\nCompanies tracked: {stats['total_companies']}")
        print(f"Active job listings: {stats['active_jobs']}")
        print(f"Decision makers found: {stats['total_decision_makers']}")
        print(f"Total seen listings (all-time): {stats['total_seen_listings']}")

        if stats["by_industry"]:
            print("\n--- Companies by Industry ---")
            for industry, count in stats["by_industry"].items():
                print(f"  {industry}: {count}")

        last = stats.get("last_run")
        if last:
            print(f"\n--- Last Run ({last['run_date']}) ---")
            print(f"  Searches used: {last['searches_used']}")
            print(f"  Unique listings: {last['unique_listings']}")
            print(f"  Companies stored: {last['companies_stored']}")
            print(f"  Decision makers found: {last['decision_makers_found']}")
        else:
            print("\nNo runs recorded yet.")
    finally:
        db.close()


def cmd_upload(args):
    """Export leads in website format and upload to Vercel Blob."""
    import json
    import requests

    config = Config.from_env()

    if not config.db_path.exists():
        print("No database found. Run 'run' first to initialize.")
        sys.exit(1)

    db = Database(config.db_path)

    try:
        companies = db.get_companies_for_upload(
            max_employee_count=config.max_employee_count
        )

        if not companies:
            print("No companies found with recent jobs. Run 'run' first.")
            sys.exit(1)

        # Transform to website lead format
        leads = []
        for row in companies:
            # Split decision maker name into first/last
            full_name = row["person_name"] or ""
            name_parts = full_name.strip().split()
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            emp_count = row["employee_count"] or 0
            category = "small"  # All are <=100 employees

            # Get active jobs for this company
            jobs = db.get_jobs_for_company(row["id"])

            # Find most recent posting date
            most_recent_date = ""
            if jobs:
                for job in jobs:
                    if job["posting_date"] and (
                        not most_recent_date
                        or job["posting_date"] > most_recent_date
                    ):
                        most_recent_date = job["posting_date"]

            # Create a lead entry for each job (frontend groups by company)
            if jobs:
                for job in jobs:
                    lead = {
                        "firstName": first_name,
                        "lastName": last_name,
                        "title": row["dm_title"] or "",
                        "companyName": row["company_name"] or "",
                        "email": "",
                        "website": row["website"] or "",
                        "location": job["location"] or "",
                        "companySize": (
                            f"{emp_count} employees" if emp_count else "Unknown"
                        ),
                        "category": category,
                        "industry": row["industry"] or "",
                        "employeeCount": emp_count,
                        "jobRole": job["title"] or "",
                        "jobLink": job["job_url"] or "",
                        "postingDate": job["posting_date"] or "",
                        "mostRecentPostingDate": most_recent_date,
                        "linkedinUrl": "",
                        "sourceUrl": row["source_url"] or "",
                        "confidence": row["confidence"] or "",
                        "isNewCompany": False,
                        "firstSeenDate": row["first_seen_date"] or "",
                        "verificationStatus": "unverified",
                    }
                    leads.append(lead)
            else:
                # Company with decision maker but no recent active jobs
                lead = {
                    "firstName": first_name,
                    "lastName": last_name,
                    "title": row["dm_title"] or "",
                    "companyName": row["company_name"] or "",
                    "email": "",
                    "website": row["website"] or "",
                    "location": "",
                    "companySize": (
                        f"{emp_count} employees" if emp_count else "Unknown"
                    ),
                    "category": category,
                    "industry": row["industry"] or "",
                    "employeeCount": emp_count,
                    "jobRole": "",
                    "jobLink": "",
                    "postingDate": "",
                    "mostRecentPostingDate": "",
                    "linkedinUrl": "",
                    "sourceUrl": row["source_url"] or "",
                    "confidence": row["confidence"] or "",
                    "isNewCompany": False,
                    "firstSeenDate": row["first_seen_date"] or "",
                    "verificationStatus": "unverified",
                }
                leads.append(lead)

        # Sort by most recent posting date
        leads.sort(
            key=lambda l: l.get("mostRecentPostingDate") or "",
            reverse=True,
        )

        unique_companies = len(set(l["companyName"] for l in leads))
        total_roles = len([l for l in leads if l["jobRole"]])
        print(
            f"Found {unique_companies} companies with "
            f"{total_roles} total roles to upload"
        )

        if args.dry_run:
            print("\nDRY RUN - Would upload the following leads:")
            for lead in leads[:10]:
                dm_info = ""
                if lead["firstName"]:
                    dm_info = f" | DM: {lead['firstName']} {lead['lastName']} ({lead['title']})"
                print(
                    f"  - {lead['companyName']}: {lead['jobRole']}{dm_info}"
                )
            if len(leads) > 10:
                print(f"  ... and {len(leads) - 10} more")
            return

        # Upload to Vercel Blob via API
        import os

        api_key = args.api_key or os.getenv("LEADS_UPLOAD_API_KEY")
        vercel_url = os.getenv("VERCEL_API_URL", "https://www.ishaangpta.com")
        api_url = args.api_url or f"{vercel_url}/api/upload-leads"

        if not api_key:
            print("Error: --api-key or LEADS_UPLOAD_API_KEY env var is required")
            sys.exit(1)

        payload = {
            "location": config.upload_location,
            "leads": leads,
        }

        response = requests.post(
            api_url,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": api_key,
            },
        )

        if response.status_code == 200:
            result = response.json()
            print(f"\nUpload successful!")
            print(f"  Message: {result.get('message')}")
            print(f"  Stats: {result.get('stats')}")
        else:
            print(f"\nUpload failed: {response.status_code}")
            print(f"  Response: {response.text}")
            sys.exit(1)

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        prog="python -m ITMSPDiscovery",
        description="IT MSP Lead Discovery System",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run IT MSP discovery pipeline")
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Do not write to database"
    )
    run_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    run_parser.add_argument(
        "--max-searches",
        type=int,
        default=None,
        help="Override max searches per run (default: 100)",
    )
    run_parser.add_argument(
        "--skip-decision-makers",
        action="store_true",
        help="Skip Gemini-based decision maker lookup",
    )
    run_parser.set_defaults(func=cmd_run)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show status")
    status_parser.set_defaults(func=cmd_status)

    # Upload command
    upload_parser = subparsers.add_parser(
        "upload", help="Upload leads to website (Vercel Blob)"
    )
    upload_parser.add_argument(
        "--api-key",
        required=False,
        help="API key for upload endpoint (LEADS_UPLOAD_API_KEY)",
    )
    upload_parser.add_argument(
        "--api-url",
        default=None,
        help="Upload API URL (default: from VERCEL_API_URL env)",
    )
    upload_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without uploading",
    )
    upload_parser.set_defaults(func=cmd_upload)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
