"""
MarketingJobDiscovery CLI

Usage:
    python -m MarketingJobDiscovery.cli run [--max-jobs N] [--dry-run]
    python -m MarketingJobDiscovery.cli status
    python -m MarketingJobDiscovery.cli export [--format csv|json] [--output FILE]
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

from .config import Config
from .core.database import Database
from .core.orchestrator import JobDiscoveryOrchestrator


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    # Reduce noise from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def cmd_run(args):
    """Run job discovery for all companies."""
    setup_logging(args.verbose)

    print("=" * 70)
    print(f"Job Discovery Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if args.dry_run:
        print("DRY RUN MODE - No database writes will be made")

    config = Config.from_env()
    if args.skip_decision_makers:
        config.enable_decision_maker_lookup = False
    if args.skip_email_lookup:
        config.enable_email_lookup = False
    db = Database(config.db_path)

    orchestrator = JobDiscoveryOrchestrator(
        config=config,
        database=db,
        max_jobs=args.max_jobs,
        dry_run=args.dry_run,
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
        print("No database found. Run 'cli.py run' first to initialize.")
        return

    db = Database(config.db_path)

    try:
        stats = db.get_statistics()

        print("=" * 70)
        print("Job Discovery Status")
        print("=" * 70)
        print(f"\nCompanies tracked: {stats['total_companies']}")
        print(f"Total active jobs: {stats['active_jobs']}")
        print(f"Marketing-relevant jobs: {stats['relevant_jobs']}")
        print(f"\nLast run: {stats['last_run_date'] or 'Never'}")
        print(f"New jobs (last run): {stats['last_run_new']}")
        print(f"Removed jobs (last run): {stats['last_run_removed']}")

        if stats["by_ats"]:
            print("\n--- Jobs by ATS Provider ---")
            for provider, count in sorted(
                stats["by_ats"].items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  {provider}: {count}")

        if stats["by_category"]:
            print("\n--- Jobs by Category ---")
            for category, count in sorted(
                stats["by_category"].items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  {category}: {count}")

        if stats["recent_changes"]:
            print("\n--- Recent Changes ---")
            for change in stats["recent_changes"][:10]:
                icon = "+" if change["type"] == "new" else "-"
                print(f"  [{icon}] {change['company']}: {change['title']}")
    finally:
        db.close()


def cmd_export(args):
    """Export jobs to CSV or JSON."""
    config = Config.from_env()

    if not config.db_path.exists():
        print("No database found. Run 'cli.py run' first to initialize.")
        sys.exit(1)

    db = Database(config.db_path)

    try:
        output_path = args.output or f"jobs_export.{args.format}"

        if args.format == "csv":
            db.export_to_csv(output_path, only_relevant=not args.all)
        else:
            db.export_to_json(output_path, only_relevant=not args.all)

        print(f"Exported to {output_path}")
    finally:
        db.close()


def cmd_upload(args):
    """Export leads in website format and upload to Vercel Blob."""
    import json
    import requests

    config = Config.from_env()

    if not config.db_path.exists():
        print("No database found. Run 'cli.py run' first to initialize.")
        sys.exit(1)

    db = Database(config.db_path)

    try:
        # Get companies with decision makers, sorted by most recent posting date
        cursor = db.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT
                c.id,
                c.name as company_name,
                c.domain,
                c.employee_count,
                dm.person_name,
                dm.title,
                dm.email,
                dm.linkedin_url,
                dm.source_url,
                dm.confidence,
                c.first_seen_date,
                c.last_csv_date,
                (SELECT MAX(j.posting_date) FROM jobs j WHERE j.company_id = c.id AND j.is_active = 1) as most_recent_posting
            FROM companies c
            JOIN decision_makers dm ON dm.company_id = c.id
            WHERE dm.person_name IS NOT NULL
            ORDER BY most_recent_posting DESC, c.urgency_score DESC
            """
        )
        companies = cursor.fetchall()

        if not companies:
            print("No decision makers found. Run 'cli.py run' first.")
            sys.exit(1)

        # Transform to website lead format
        leads = []
        for row in companies:
            # Split name into first/last
            full_name = row[4] or ""
            name_parts = full_name.strip().split()
            first_name = name_parts[0] if name_parts else ""
            last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            # Determine category based on employee count
            emp_count = row[3] or 0
            if emp_count <= 100:
                category = "small"
            elif emp_count <= 250:
                category = "medium"
            else:
                category = "large"

            # Get ALL active jobs for this company, excluding stale jobs
            cursor.execute(
                """
                SELECT title, job_url, posting_date, verification_status
                FROM jobs
                WHERE company_id = ? AND is_active = 1
                AND (verification_status IS NULL OR verification_status != 'stale')
                ORDER BY posting_date DESC, relevance_score DESC
                """,
                (row[0],),
            )
            jobs = cursor.fetchall()

            # Determine if this is a new company (first seen today)
            first_seen_date = row[10] if len(row) > 10 else None
            last_csv_date = row[11] if len(row) > 11 else None
            is_new_company = first_seen_date == last_csv_date if first_seen_date and last_csv_date else False

            # Find the most recent posting date for this company
            most_recent_date = ""
            if jobs:
                for job in jobs:
                    if job[2] and (not most_recent_date or job[2] > most_recent_date):
                        most_recent_date = job[2]

            # Create a lead entry for each job (frontend groups by company)
            if jobs:
                for job in jobs:
                    lead = {
                        "firstName": first_name,
                        "lastName": last_name,
                        "title": row[5] or "",
                        "companyName": row[1] or "",
                        "email": row[6] or "",
                        "website": f"https://{row[2]}" if row[2] else "",
                        "location": "",
                        "companySize": f"{emp_count} employees" if emp_count else "Unknown",
                        "category": category,
                        "jobRole": job[0] if job else "",
                        "jobLink": job[1] if job else "",
                        "postingDate": job[2] if job else "",
                        "mostRecentPostingDate": most_recent_date,
                        "linkedinUrl": row[7] or "",
                        "sourceUrl": row[8] or "",
                        "confidence": row[9] or "",
                        "isNewCompany": is_new_company,
                        "firstSeenDate": first_seen_date or "",
                        "verificationStatus": job[3] if len(job) > 3 and job[3] else "unverified",
                    }
                    leads.append(lead)
            else:
                # Company with decision maker but no active jobs
                lead = {
                    "firstName": first_name,
                    "lastName": last_name,
                    "title": row[5] or "",
                    "companyName": row[1] or "",
                    "email": row[6] or "",
                    "website": f"https://{row[2]}" if row[2] else "",
                    "location": "",
                    "companySize": f"{emp_count} employees" if emp_count else "Unknown",
                    "category": category,
                    "jobRole": "",
                    "jobLink": "",
                    "postingDate": "",
                    "mostRecentPostingDate": "",
                    "linkedinUrl": row[7] or "",
                    "sourceUrl": row[8] or "",
                    "confidence": row[9] or "",
                    "isNewCompany": is_new_company,
                    "firstSeenDate": first_seen_date or "",
                    "verificationStatus": "unverified",
                }
                leads.append(lead)

        # Sort leads by mostRecentPostingDate (newest first)
        # This ensures companies with recent job postings appear first
        leads.sort(
            key=lambda l: l.get("mostRecentPostingDate") or "",
            reverse=True
        )

        # Count unique companies and total job entries
        unique_companies = len(set(l["companyName"] for l in leads))
        total_roles = len([l for l in leads if l["jobRole"]])
        print(f"Found {unique_companies} companies with {total_roles} total roles to upload")

        # Count by category
        small = len([l for l in leads if l["category"] == "small"])
        medium = len([l for l in leads if l["category"] == "medium"])
        large = len([l for l in leads if l["category"] == "large"])
        print(f"  Small (≤100): {small} entries")
        print(f"  Medium (101-250): {medium} entries")
        print(f"  Large (251+): {large} entries")

        if args.dry_run:
            print("\nDRY RUN - Would upload the following leads:")
            for lead in leads[:5]:
                print(f"  - {lead['firstName']} {lead['lastName']} ({lead['title']}) at {lead['companyName']}")
            if len(leads) > 5:
                print(f"  ... and {len(leads) - 5} more")
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
            "location": args.location or "marketing-discovery",
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
        prog="python -m MarketingJobDiscovery.cli",
        description="Marketing Job Discovery System",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run job discovery")
    run_parser.add_argument(
        "--max-jobs",
        type=int,
        default=100,
        help="Maximum total jobs to process (default: 100)",
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Do not write to database"
    )
    run_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    run_parser.add_argument(
        "--skip-decision-makers",
        action="store_true",
        help="Skip Gemini-based decision maker lookup",
    )
    run_parser.add_argument(
        "--skip-email-lookup",
        action="store_true",
        help="Skip Apollo-based email lookup",
    )
    run_parser.set_defaults(func=cmd_run)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show status")
    status_parser.set_defaults(func=cmd_status)

    # Export command
    export_parser = subparsers.add_parser("export", help="Export jobs")
    export_parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Output format (default: csv)",
    )
    export_parser.add_argument("--output", "-o", help="Output file path")
    export_parser.add_argument(
        "--all", action="store_true", help="Include non-relevant jobs"
    )
    export_parser.set_defaults(func=cmd_export)

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
        default="https://www.ishaangpta.com/api/upload-leads",
        help="Upload API URL",
    )
    upload_parser.add_argument(
        "--location",
        default="marketing-discovery",
        help="Location identifier for the leads cache",
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
