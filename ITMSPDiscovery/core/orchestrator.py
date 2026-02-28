"""Main orchestrator for the IT MSP discovery pipeline."""

import logging
from datetime import datetime
from typing import List, Dict, Any

from ..config import Config
from .database import Database
from .serpapi_client import SerpAPIJobClient
from .decision_maker import ITDecisionMakerFinder

logger = logging.getLogger(__name__)


class ITMSPOrchestrator:
    """
    Pipeline:
    1. SerpAPI Google Jobs search across metros
    2. Deduplicate listings (within run + across previous runs)
    3. Store companies and jobs in SQLite
    4. Gemini lookup for decision makers + employee count + industry
    5. Generate summary
    """

    def __init__(
        self,
        config: Config,
        database: Database,
        dry_run: bool = False,
        max_searches: int = None,
    ):
        self.config = config
        self.db = database
        self.dry_run = dry_run
        self.max_searches = max_searches or config.max_searches_per_run
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    async def run(self) -> Dict[str, Any]:
        """Execute the full IT MSP discovery pipeline."""
        start_time = datetime.now()

        # Step 1: SerpAPI search
        print("\n" + "=" * 70)
        print("Step 1: Searching for IT job postings via SerpAPI...")
        print("=" * 70)

        if not self.config.serpapi_api_key:
            print("Error: SERPAPI_API_KEY not set. Cannot search.")
            return {"error": "SERPAPI_API_KEY not set"}

        client = SerpAPIJobClient(
            api_key=self.config.serpapi_api_key,
            max_searches=self.max_searches,
        )

        # Pick today's metros from the rotation
        todays_metros = SerpAPIJobClient.get_next_metros(
            all_metros=self.config.metro_areas,
            count=self.config.metros_per_run,
            state_path=self.config.metro_state_path,
        )
        print(f"  Today's metros: {', '.join(todays_metros)}")

        all_listings = client.search_all(
            query=self.config.search_query,
            metro_areas=todays_metros,
        )
        raw_count = client.searches_used * 10  # Approximate
        print(
            f"  Found {len(all_listings)} unique listings "
            f"across {client.searches_used} searches"
        )

        if not all_listings:
            print("No listings found. Exiting.")
            return {"error": "No listings found"}

        # Step 2: Store in database (dedup against previous runs)
        print("\n" + "=" * 70)
        print("Step 2: Storing new listings in database...")
        print("=" * 70)

        new_count = 0
        companies_touched = set()

        for listing in all_listings:
            dedup_key = SerpAPIJobClient._dedup_key(
                listing.company_name, listing.title
            )

            if self.db.is_listing_seen(dedup_key):
                continue

            # Upsert company
            company_id = self.db.upsert_company(
                {"name": listing.company_name},
                run_id=self.run_id,
            )

            # Insert job
            if not self.dry_run:
                self.db.insert_job(
                    {
                        "title": listing.title,
                        "job_url": listing.job_url,
                        "location": listing.location,
                        "posting_date": (
                            listing.posting_date.isoformat()
                            if listing.posting_date
                            else None
                        ),
                        "posted_at_raw": listing.posted_at,
                        "source": listing.source,
                        "description_snippet": listing.description_snippet,
                        "search_metro": listing.search_metro,
                    },
                    company_id,
                )
                self.db.mark_listing_seen(
                    dedup_key, listing.company_name, listing.title, self.run_id
                )

            new_count += 1
            companies_touched.add(listing.company_name)

        print(
            f"  Stored {new_count} new listings "
            f"from {len(companies_touched)} companies"
        )
        if self.dry_run:
            print("  (DRY RUN - no database writes)")

        # Step 3: Gemini lookup for decision makers + enrichment
        dm_found = 0
        if (
            self.config.enable_decision_maker_lookup
            and self.config.gemini_api_key
        ):
            companies_needing_dm = self.db.get_companies_needing_decision_makers()
            if companies_needing_dm:
                print("\n" + "=" * 70)
                print(
                    f"Step 3: Looking up decision makers for "
                    f"{len(companies_needing_dm)} companies..."
                )
                print("=" * 70)

                finder = ITDecisionMakerFinder(
                    api_key=self.config.gemini_api_key,
                    model=self.config.gemini_model,
                    batch_size=self.config.gemini_batch_size,
                )

                companies_for_lookup = [
                    {"company": c["name"], "domain": c.get("domain") or ""}
                    for c in companies_needing_dm
                ]

                dm_results = await finder.find_decision_makers(companies_for_lookup)

                for dm in dm_results:
                    # Find the matching company in our list
                    company = next(
                        (
                            c
                            for c in companies_needing_dm
                            if c["name"] == dm.company_name
                        ),
                        None,
                    )
                    if not company:
                        continue

                    # Store decision maker
                    if (
                        dm.person_name
                        and "not confidently" not in dm.person_name.lower()
                    ):
                        if not self.dry_run:
                            self.db.upsert_decision_maker(
                                company_id=company["id"],
                                person_name=dm.person_name,
                                title=dm.title,
                                source_url=dm.source_url,
                                confidence=dm.confidence,
                            )
                        dm_found += 1
                        print(
                            f"  {dm.company_name}: {dm.person_name} "
                            f"({dm.title or 'N/A'})"
                        )

                    # Update enrichment data (employee count, industry)
                    if not self.dry_run and (dm.employee_count or dm.industry):
                        self.db.update_company_enrichment(
                            company["id"],
                            employee_count=dm.employee_count,
                            industry=dm.industry,
                        )

                print(f"\n  Found decision makers for {dm_found} companies")
            else:
                print("\nStep 3: All companies already have decision makers")
        elif not self.config.gemini_api_key:
            print("\nStep 3: Skipping (GEMINI_API_KEY not set)")
        else:
            print("\nStep 3: Skipping (decision maker lookup disabled)")

        # Step 4: Record run and print summary
        if not self.dry_run:
            self.db.record_run_snapshot(
                run_id=self.run_id,
                searches_used=client.searches_used,
                raw_listings=raw_count,
                unique_listings=len(all_listings),
                companies_stored=len(companies_touched),
                decision_makers_found=dm_found,
            )

        elapsed = (datetime.now() - start_time).total_seconds()
        self._print_summary(
            elapsed=elapsed,
            searches_used=client.searches_used,
            total_listings=len(all_listings),
            new_listings=new_count,
            companies=len(companies_touched),
            dm_found=dm_found,
        )

        return {
            "run_id": self.run_id,
            "elapsed_seconds": elapsed,
            "searches_used": client.searches_used,
            "total_listings": len(all_listings),
            "new_listings": new_count,
            "companies": len(companies_touched),
            "decision_makers_found": dm_found,
        }

    def _print_summary(self, **kwargs):
        """Print run summary."""
        print("\n" + "=" * 70)
        print("RUN SUMMARY")
        print("=" * 70)
        print(f"Duration: {kwargs['elapsed']:.1f}s")
        print(f"SerpAPI searches used: {kwargs['searches_used']}")
        print(f"Total unique listings: {kwargs['total_listings']}")
        print(f"New listings stored: {kwargs['new_listings']}")
        print(f"Companies touched: {kwargs['companies']}")
        print(f"Decision makers found: {kwargs['dm_found']}")
        print("=" * 70)
