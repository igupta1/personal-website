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

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
