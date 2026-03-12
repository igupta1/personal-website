"""Main orchestrator for the IT MSP discovery pipeline."""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any

import httpx

from ..config import Config
from .database import Database
from .serpapi_client import SerpAPIJobClient
from .decision_maker import ITDecisionMakerFinder
from .insight_generator import InsightGenerator
from .priority_classifier import PriorityClassifier
from .outreach_generator import OutreachGenerator
from .job_verifier import JobVerifier

logger = logging.getLogger(__name__)


class ITMSPOrchestrator:
    """
    Pipeline:
    1. SerpAPI Google Jobs search across metros
    2. Deduplicate listings (within run + across previous runs)
    3. Store companies and jobs in SQLite
    4. Gemini lookup for decision makers + employee count + industry
    5. Backfill LinkedIn URLs for DMs missing them
    6. Validate LinkedIn URLs
    7. Generate AI insights
    8. Classify priority tiers (P1-P5)
    9. Generate personalized outreach drafts
    10. Verify job URLs
    11. Record run snapshot
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

        # Step 3: Decision makers
        dm_found = await self._find_decision_makers_if_needed()

        # Step 4: LinkedIn URL backfill
        await self._backfill_linkedin_urls()

        # Step 5: LinkedIn URL validation
        await self._validate_linkedin_urls()

        # Step 6: Insights
        await self._generate_insights_if_needed()

        # Step 7: Priority classification
        await self._classify_priority_tiers()

        # Step 8: Outreach drafts
        await self._generate_outreach_if_needed()

        # Step 9: Job verification
        await self._verify_jobs_if_needed()

        # Step 10: Record run and print summary
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

    async def _find_decision_makers_if_needed(self) -> int:
        """Find decision makers for all upload-eligible companies missing one."""
        if not self.config.enable_decision_maker_lookup or not self.config.gemini_api_key:
            if self.config.enable_decision_maker_lookup and not self.config.gemini_api_key:
                print("\n--- Skipping decision makers (GEMINI_API_KEY not set) ---")
            else:
                print("\n--- Skipping decision makers (disabled) ---")
            return 0

        companies_needing_dm = self.db.get_companies_needing_dm_lookup(
            max_employee_count=self.config.max_employee_count
        )

        if not companies_needing_dm:
            print("\n--- All companies already have decision maker lookups ---")
            return 0

        print(
            f"\n--- Looking up decision makers for "
            f"{len(companies_needing_dm)} companies ---"
        )

        lookup_list = [
            {"company": c["name"], "domain": c.get("domain") or "", "company_id": c["id"]}
            for c in companies_needing_dm
        ]

        dm_found = 0
        try:
            finder = ITDecisionMakerFinder(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
                batch_size=self.config.gemini_batch_size,
            )
            dm_results = await finder.find_decision_makers(lookup_list)

            dm_by_company = {dm.company_name: dm for dm in dm_results}

            for company_info in lookup_list:
                company_id = company_info["company_id"]
                company_name = company_info["company"]
                dm = dm_by_company.get(company_name)

                if (
                    dm
                    and dm.person_name
                    and "not confidently" not in dm.person_name.lower()
                ):
                    dm_found += 1
                    if not self.dry_run:
                        self.db.upsert_decision_maker(
                            company_id=company_id,
                            person_name=dm.person_name,
                            title=dm.title,
                            source_url=dm.source_url,
                            confidence=dm.confidence,
                            linkedin_url=dm.linkedin_url,
                        )
                    print(
                        f"  {dm.company_name}: {dm.person_name} "
                        f"({dm.title or 'N/A'})"
                    )

                # Update enrichment data regardless
                if dm and not self.dry_run and (dm.employee_count or dm.industry):
                    self.db.update_company_enrichment(
                        company_id,
                        employee_count=dm.employee_count,
                        industry=dm.industry,
                    )

                # Mark attempt regardless of success/failure
                if not self.dry_run:
                    self.db.mark_dm_lookup_attempted(company_id)

            print(f"\n  Found decision makers for {dm_found}/{len(companies_needing_dm)} companies")

        except Exception as e:
            logger.error(f"Decision maker lookup failed: {e}")
            print(f"  Decision maker lookup failed: {e}")

        return dm_found

    async def _backfill_linkedin_urls(self):
        """Re-run DM lookup for existing decision makers missing LinkedIn URLs."""
        if not self.config.enable_decision_maker_lookup or not self.config.gemini_api_key:
            return

        companies = self.db.get_companies_needing_linkedin_url(
            max_employee_count=self.config.max_employee_count
        )
        if not companies:
            print("\n--- All decision makers already have LinkedIn URLs ---")
            return

        print(
            f"\n--- Backfilling LinkedIn URLs for "
            f"{len(companies)} decision makers ---"
        )

        lookup_list = [
            {"company": c["name"], "domain": c.get("domain") or "", "company_id": c["id"]}
            for c in companies
        ]

        try:
            finder = ITDecisionMakerFinder(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
                batch_size=self.config.gemini_batch_size,
            )
            dm_results = await finder.find_decision_makers(lookup_list)

            filled_count = 0
            dm_by_company = {dm.company_name: dm for dm in dm_results}

            for company_info in lookup_list:
                company_id = company_info["company_id"]
                company_name = company_info["company"]
                dm = dm_by_company.get(company_name)

                if dm and dm.person_name and dm.linkedin_url and not self.dry_run:
                    filled_count += 1
                    self.db.upsert_decision_maker(
                        company_id=company_id,
                        person_name=dm.person_name,
                        title=dm.title,
                        source_url=dm.source_url,
                        confidence=dm.confidence,
                        linkedin_url=dm.linkedin_url,
                    )

            print(f"  Filled {filled_count}/{len(companies)} LinkedIn URLs")

        except Exception as e:
            logger.error(f"LinkedIn URL backfill failed: {e}")
            print(f"  LinkedIn URL backfill failed: {e}")

    async def _validate_linkedin_urls(self):
        """Validate LinkedIn URLs by checking if they resolve to real profiles."""
        dms = self.db.get_decision_makers_with_linkedin_urls()
        if not dms:
            print("\n--- No LinkedIn URLs to validate ---")
            return

        print(f"\n--- Validating {len(dms)} LinkedIn URLs ---")

        cleared = 0
        browser_ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(10.0),
                headers={"User-Agent": browser_ua},
                follow_redirects=True,
            ) as client:
                for dm in dms:
                    url = dm["linkedin_url"]
                    try:
                        resp = await client.get(url)
                        body = resp.text[:5000].lower()

                        if resp.status_code == 404:
                            if not self.dry_run:
                                self.db.clear_linkedin_url(dm["id"])
                            cleared += 1
                            logger.info(f"Cleared invalid LinkedIn URL (404): {url}")
                        elif resp.status_code == 200 and (
                            "page not found" in body
                            or "profile is not available" in body
                            or "this page doesn" in body
                        ):
                            if not self.dry_run:
                                self.db.clear_linkedin_url(dm["id"])
                            cleared += 1
                            logger.info(f"Cleared invalid LinkedIn URL (not found): {url}")
                    except Exception as e:
                        logger.debug(f"LinkedIn validation error for {url}: {e}")

                    await asyncio.sleep(2)

            print(f"  Cleared {cleared}/{len(dms)} invalid LinkedIn URLs")
        except Exception as e:
            logger.error(f"LinkedIn URL validation failed: {e}")
            print(f"  LinkedIn URL validation failed: {e}")

    async def _generate_insights_if_needed(self):
        """Generate AI insights for companies that don't have one yet."""
        if not self.config.enable_insight_generation or not self.config.gemini_api_key:
            return

        companies_needing = self.db.get_companies_needing_insights(
            max_employee_count=self.config.max_employee_count
        )

        if not companies_needing:
            print("\n--- All companies already have insights ---")
            return

        print(
            f"\n--- Generating insights for "
            f"{len(companies_needing)} companies ---"
        )
        try:
            insight_input = []
            for comp in companies_needing:
                jobs = self.db.get_jobs_for_company(comp["id"])
                role_titles = [j["title"] for j in jobs]
                insight_input.append({
                    "company_name": comp["name"],
                    "domain": comp.get("domain") or "",
                    "roles": role_titles,
                })

            generator = InsightGenerator(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
                batch_size=10,
            )
            insights = await generator.generate_insights(insight_input)

            if not self.dry_run:
                for comp in companies_needing:
                    insight_text = insights.get(comp["name"])
                    if insight_text:
                        self.db.update_company_insight(comp["id"], insight_text)

            print(f"  Generated {len(insights)} insights")
        except Exception as e:
            logger.error(f"Insight generation failed: {e}")
            print(f"  Insight generation failed: {e}")

    async def _classify_priority_tiers(self):
        """Classify priority tiers for companies that don't have one yet."""
        if not self.config.enable_priority_classification or not self.config.gemini_api_key:
            return

        companies = self.db.get_companies_needing_priority_classification(
            max_employee_count=self.config.max_employee_count
        )

        if not companies:
            print("\n--- All companies already have priority tiers ---")
            return

        print(f"\n--- Classifying priority tiers for {len(companies)} companies ---")

        try:
            classifier_input = []
            for comp in companies:
                jobs = self.db.get_jobs_for_company(comp["id"])
                role_titles = [j["title"] for j in jobs]
                classifier_input.append({
                    "company_name": comp["name"],
                    "domain": comp.get("domain") or "",
                    "industry": comp.get("industry") or "",
                    "employee_count": comp.get("employee_count"),
                    "roles": role_titles,
                    "has_decision_maker": bool(comp.get("has_decision_maker")),
                })

            classifier = PriorityClassifier(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
                batch_size=10,
            )
            results = await classifier.classify_priorities(classifier_input)

            if not self.dry_run:
                for comp in companies:
                    result = results.get(comp["name"])
                    if result:
                        self.db.update_company_priority_tier(
                            comp["id"], result["priority_tier"]
                        )

            print(f"  Classified {len(results)} companies")
        except Exception as e:
            logger.error(f"Priority classification failed: {e}")
            print(f"  Priority classification failed: {e}")

    async def _generate_outreach_if_needed(self):
        """Generate personalized outreach drafts for companies missing them."""
        if not self.config.enable_outreach_generation or not self.config.gemini_api_key:
            return

        companies = self.db.get_companies_needing_outreach(
            max_employee_count=self.config.max_employee_count
        )

        if not companies:
            print("\n--- All companies already have outreach drafts ---")
            return

        print(f"\n--- Generating outreach drafts for {len(companies)} companies ---")

        try:
            outreach_input = []
            for comp in companies:
                jobs = self.db.get_jobs_for_company(comp["id"])
                role_titles = [j["title"] for j in jobs]
                outreach_input.append({
                    "company_name": comp["name"],
                    "domain": comp.get("domain") or "",
                    "roles": role_titles,
                    "company_id": comp["id"],
                })

            generator = OutreachGenerator(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
            )
            results = await generator.generate_outreach(outreach_input)

            if not self.dry_run:
                for comp in companies:
                    result = results.get(comp["name"])
                    if result:
                        self.db.update_company_outreach(
                            comp["id"],
                            result.get("summary", ""),
                            result.get("compliment", ""),
                            result.get("outreach_draft", ""),
                            result.get("role_classification", ""),
                        )

            generated = sum(1 for r in results.values() if r.get("outreach_draft"))
            print(f"  Generated {generated} outreach drafts")
        except Exception as e:
            logger.error(f"Outreach generation failed: {e}")
            print(f"  Outreach generation failed: {e}")

    async def _verify_jobs_if_needed(self):
        """Verify job URLs are still live using HEAD requests."""
        if not self.config.enable_job_verification:
            return

        jobs = self.db.get_jobs_for_verification()

        if not jobs:
            print("\n--- No jobs to verify ---")
            return

        print(f"\n--- Verifying {len(jobs)} job URLs ---")

        try:
            verifier = JobVerifier(
                timeout=self.config.job_verification_timeout,
                batch_size=self.config.job_verification_batch_size,
            )
            results = await verifier.verify_jobs(jobs)

            verified = stale = unverified = 0
            if not self.dry_run:
                for job_id, status in results:
                    self.db.update_job_verification(job_id, status)
                    if status == "verified":
                        verified += 1
                    elif status == "stale":
                        stale += 1
                    else:
                        unverified += 1

            print(f"  Verified: {verified}, Stale: {stale}, Unverified: {unverified}")
        except Exception as e:
            logger.error(f"Job verification failed: {e}")
            print(f"  Job verification failed: {e}")

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
