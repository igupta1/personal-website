"""CLI interface for ColdEmailPersonalizer."""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .config import Config
from .pipeline import Pipeline, run_test


def main():
    parser = argparse.ArgumentParser(
        prog="ColdEmailPersonalizer",
        description="Personalize cold email subject lines and openers from a CSV of prospects.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- run command ---
    run_parser = subparsers.add_parser("run", help="Run the full personalization pipeline")
    run_parser.add_argument("-i", "--input", required=True, type=Path, help="Input CSV file")
    run_parser.add_argument("-o", "--output", type=Path, help="Output CSV file (default: {input}_personalized.csv)")
    run_parser.add_argument("-n", "--limit", type=int, default=0, help="Process only first N rows")
    run_parser.add_argument("--resume", action="store_true", help="Resume from checkpoint if exists")
    run_parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    run_parser.add_argument("--scrape-concurrency", type=int, default=None)
    run_parser.add_argument("--llm-concurrency", type=int, default=None)
    run_parser.add_argument("--dry-run", action="store_true", help="Scrape only, no LLM calls")

    # --- test command ---
    test_parser = subparsers.add_parser("test", help="Test on a few rows and print results")
    test_parser.add_argument("-i", "--input", required=True, type=Path, help="Input CSV file")
    test_parser.add_argument("--rows", type=int, default=5, help="Number of rows to test (default: 5)")
    test_parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command == "run":
        overrides = {
            "input_csv": args.input,
            "output_csv": args.output,
            "limit": args.limit,
            "resume": args.resume,
        }
        if args.scrape_concurrency is not None:
            overrides["scrape_concurrency"] = args.scrape_concurrency
        if args.llm_concurrency is not None:
            overrides["llm_concurrency"] = args.llm_concurrency

        config = Config.from_env(**overrides)

        if not config.gemini_api_key:
            print("Error: GEMINI_API_KEY not set. Set it in .env or as an environment variable.")
            sys.exit(1)

        pipeline = Pipeline(config)
        asyncio.run(pipeline.run())

    elif args.command == "test":
        config = Config.from_env(input_csv=args.input)

        if not config.gemini_api_key:
            print("Error: GEMINI_API_KEY not set. Set it in .env or as an environment variable.")
            sys.exit(1)

        asyncio.run(run_test(config, rows=args.rows))
