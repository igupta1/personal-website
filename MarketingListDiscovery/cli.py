"""
MarketingListDiscovery CLI

Usage:
    python -m MarketingListDiscovery run [--dry-run] [--date YYYY-MM-DD]
    python -m MarketingListDiscovery status
    python -m MarketingListDiscovery export [--format csv|json] [--output FILE]
    python -m MarketingListDiscovery upload [--api-key KEY] [--dry-run]
    python -m MarketingListDiscovery enrich [--dry-run]
    python -m MarketingListDiscovery reset
"""

import argparse
import asyncio
import logging
import sys
from datetime import date, datetime
from pathlib import Path

from .config import Config
from .core.database import Database
from .core.orchestrator import ListDiscoveryOrchestrator


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
    """Run list discovery from GitHub source."""
    setup_logging(args.verbose)

    print("=" * 70)
    print(f"List Discovery Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if args.dry_run:
        print("DRY RUN MODE - No database writes will be made")

    # Parse target date if provided
    target_date = None
    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
            print(f"Target date: {target_date.isoformat()}")
        except ValueError:
            print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
            sys.exit(1)

    if args.include_all_days:
        print("Processing all companies from last 7 days")

    config = Config.from_env()
    if args.skip_decision_makers:
        config.enable_decision_maker_lookup = False
    if args.skip_email_lookup:
        config.enable_email_lookup = False
    db = Database(config.db_path)

    orchestrator = ListDiscoveryOrchestrator(
        config=config,
        database=db,
        dry_run=args.dry_run,
        target_date=target_date,
        include_all_days=args.include_all_days,
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
        seen_count = db.get_seen_companies_count()

        print("=" * 70)
        print("List Discovery Status")
        print("=" * 70)
        print(f"\nCompanies tracked: {stats['total_companies']}")
        print(f"Companies seen (all-time): {seen_count}")
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
        print("No database found. Run 'run' first to initialize.")
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
        print("No database found. Run 'run' first to initialize.")
        sys.exit(1)

    db = Database(config.db_path)

    try:
        # Get companies with recent jobs (<=7 days) and <=250 employees
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
                c.industry,
                (SELECT MAX(j.posting_date) FROM jobs j WHERE j.company_id = c.id AND j.is_active = 1) as most_recent_posting
            FROM companies c
            LEFT JOIN decision_makers dm ON dm.company_id = c.id
            WHERE (c.employee_count IS NULL OR c.employee_count <= 250)
              AND EXISTS (
                SELECT 1 FROM jobs j
                WHERE j.company_id = c.id AND j.is_active = 1
                  AND j.posting_date >= date('now', '-7 days')
              )
            ORDER BY most_recent_posting DESC, c.urgency_score DESC
            """
        )
        companies = cursor.fetchall()

        if not companies:
            print("No companies found. Run 'run' first.")
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
            else:
                category = "medium"

            # Get active jobs posted within last 7 days
            cursor.execute(
                """
                SELECT title, job_url, posting_date, verification_status
                FROM jobs
                WHERE company_id = ? AND is_active = 1
                AND posting_date >= date('now', '-7 days')
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
                        "industry": row[12] or "",
                        "employeeCount": emp_count,
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
                    "industry": row[12] or "",
                    "employeeCount": emp_count,
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
        print(f"  Small (<=100): {small} entries")
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


def cmd_enrich(args):
    """Backfill industry and employee count for existing companies using Gemini."""
    import json
    import re

    from google import genai
    from google.genai import types
    from .core.decision_maker import VALID_INDUSTRIES

    setup_logging(args.verbose)

    config = Config.from_env()

    if not config.db_path.exists():
        print("No database found. Run 'run' first to initialize.")
        sys.exit(1)

    if not config.gemini_api_key:
        print("Error: GEMINI_API_KEY environment variable is required")
        sys.exit(1)

    db = Database(config.db_path)

    try:
        cursor = db.conn.cursor()
        cursor.execute(
            "SELECT id, name, domain FROM companies WHERE industry IS NULL OR industry = ''"
        )
        companies = cursor.fetchall()

        if not companies:
            print("All companies already have industry data.")
            return

        print(f"Found {len(companies)} companies needing industry enrichment")

        if args.dry_run:
            for row in companies[:20]:
                print(f"  - {row[1]} ({row[2]})")
            if len(companies) > 20:
                print(f"  ... and {len(companies) - 20} more")
            return

        async def _run_enrichment():
            client = genai.Client(api_key=config.gemini_api_key)
            batch_size = config.gemini_batch_size or 5
            batches = [
                companies[i : i + batch_size]
                for i in range(0, len(companies), batch_size)
            ]

            enriched = 0
            for batch_idx, batch in enumerate(batches, 1):
                company_list = "\n".join(
                    f"- {row[1]} (website: {row[2]})" for row in batch
                )
                prompt = (
                    'You have access to Google Search grounding. For each company below, '
                    'determine:\n'
                    '1. The industry category (choose exactly one from: Home Services, '
                    'Healthcare, Legal, Financial Services, Food & Beverage, Real Estate, '
                    'Automotive, SaaS / Technology, Education, Fitness & Wellness, '
                    'Nonprofits, Professional Services, Retail / E-commerce, Other)\n'
                    '2. The approximate employee count (integer, use LinkedIn or other '
                    'public sources)\n\n'
                    'IMPORTANT: Return a JSON array with objects having these exact keys: '
                    '"company_name", "industry", "employee_count".\n\n'
                    f'Companies:\n{company_list}'
                )

                gen_config = types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.0,
                )

                try:
                    response = await client.aio.models.generate_content(
                        model=config.gemini_model or "gemini-2.5-flash",
                        contents=prompt,
                        config=gen_config,
                    )
                    raw_text = response.text

                    # Parse JSON from response
                    cleaned = raw_text.strip()
                    if cleaned.startswith("```"):
                        lines = cleaned.split("\n")
                        cleaned = "\n".join(lines[1:-1]).strip()

                    parsed = None
                    try:
                        parsed = json.loads(cleaned)
                    except json.JSONDecodeError:
                        match = re.search(r"\[[\s\S]*\]", raw_text)
                        if match:
                            try:
                                parsed = json.loads(match.group())
                            except json.JSONDecodeError:
                                pass

                    if not parsed:
                        print(f"  Batch {batch_idx}/{len(batches)}: Failed to parse response")
                        continue

                    # Update database
                    for entry in parsed:
                        name = entry.get("company_name", "")
                        industry = entry.get("industry", "")
                        emp_count = entry.get("employee_count")

                        if industry and industry not in VALID_INDUSTRIES:
                            industry = "Other"

                        if emp_count is not None:
                            try:
                                emp_count = int(emp_count)
                            except (ValueError, TypeError):
                                emp_count = None

                        # Find matching company in batch
                        matched_row = None
                        name_lower = name.lower().strip()
                        for row in batch:
                            if row[1].lower() == name_lower or name_lower in row[1].lower() or row[1].lower() in name_lower:
                                matched_row = row
                                break

                        if matched_row and industry:
                            updates = {"industry": industry}
                            if emp_count:
                                updates["employee_count"] = emp_count
                            set_clause = ", ".join(f"{k} = ?" for k in updates)
                            values = list(updates.values()) + [matched_row[0]]
                            cursor.execute(
                                f"UPDATE companies SET {set_clause} WHERE id = ?",
                                values,
                            )
                            enriched += 1

                    db.conn.commit()
                    print(f"  Batch {batch_idx}/{len(batches)}: enriched {len(parsed)} companies")

                except Exception as e:
                    print(f"  Batch {batch_idx}/{len(batches)}: Error - {e}")
                    continue

            return enriched

        enriched = asyncio.run(_run_enrichment())
        print(f"\nEnrichment complete: {enriched} companies updated")

    finally:
        db.close()


def cmd_reset(args):
    """Reset the seen companies table to allow re-processing."""
    config = Config.from_env()

    if not config.db_path.exists():
        print("No database found. Nothing to reset.")
        return

    db = Database(config.db_path)

    try:
        count = db.get_seen_companies_count()
        if count == 0:
            print("No seen companies to reset.")
            return

        if not args.force:
            response = input(
                f"This will reset {count} seen companies, "
                f"allowing them to be re-processed. Continue? [y/N] "
            )
            if response.lower() != "y":
                print("Cancelled.")
                return

        db.reset_seen_companies()
        print(f"Reset {count} seen companies.")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        prog="python -m MarketingListDiscovery",
        description="Marketing List Discovery System (GitHub-sourced)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run list discovery from GitHub")
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Do not write to database"
    )
    run_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    run_parser.add_argument(
        "--date",
        help="Target date to process (YYYY-MM-DD format, default: today)",
    )
    run_parser.add_argument(
        "--include-all-days",
        action="store_true",
        help="Process all companies from last 7 days, not just today",
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

    # Enrich command
    enrich_parser = subparsers.add_parser(
        "enrich", help="Backfill industry/employee data for existing companies"
    )
    enrich_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be enriched"
    )
    enrich_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    enrich_parser.set_defaults(func=cmd_enrich)

    # Reset command
    reset_parser = subparsers.add_parser(
        "reset", help="Reset seen companies to allow re-processing"
    )
    reset_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt",
    )
    reset_parser.set_defaults(func=cmd_reset)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
