"""Main orchestrator for the list discovery pipeline."""

import asyncio
import hashlib
import logging
from datetime import date, datetime
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse
import httpx

from ..config import Config
from .database import Database
from .models import GitHubListing
from .decision_maker import DecisionMakerFinder
from .insight_generator import InsightGenerator
from ..scrapers.github_scraper import GitHubReadmeScraper

logger = logging.getLogger(__name__)


class ListDiscoveryOrchestrator:
    """
    Main orchestrator for the list discovery pipeline.

    Pipeline:
    1. Scrape GitHub README for today's job listings
    2. Filter out already-seen companies
    3. Store companies and their jobs directly from GitHub
    4. Enrich with decision makers and emails
    5. Mark companies as seen
    6. Generate summary report
    """

    def __init__(
        self,
        config: Config,
        database: Database,
        dry_run: bool = False,
        target_date: Optional[date] = None,
        include_all_days: bool = False,
    ):
        self.config = config
        self.db = database
        self.dry_run = dry_run
        self.target_date = target_date or date.today()
        self.include_all_days = include_all_days
        self.github_scraper = GitHubReadmeScraper(repo=config.github_repo)

    async def run(self) -> Dict[str, Any]:
        """Execute the full list discovery pipeline."""
        start_time = datetime.now()

        # Generate unique run_id for this daily run
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Initialize HTTP client
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        async with httpx.AsyncClient(
            limits=limits,
            timeout=httpx.Timeout(self.config.http_timeout),
            headers={"User-Agent": self.config.user_agent},
        ) as client:
            # Fetch listings from GitHub
            companies_with_jobs = await self._load_companies_from_github(client)

            if not companies_with_jobs:
                print("No new companies to process.")
                await self._find_decision_makers_if_needed()
                await self._generate_insights_if_needed()
                elapsed = (datetime.now() - start_time).total_seconds()
                return self._generate_summary([], elapsed)

            print(f"Found {len(companies_with_jobs)} new companies to process")

            results = []

            for i, (company, jobs) in enumerate(companies_with_jobs, 1):
                print(f"\n[{i}/{len(companies_with_jobs)}] {company['name']} ({len(jobs)} roles)")

                try:
                    result = self._store_company_and_jobs(company, jobs)
                    results.append(result)

                    for job in jobs:
                        print(f"  - {job.job_title}")

                    # Mark company as seen
                    if not self.dry_run:
                        self.db.mark_company_seen(
                            domain=company["domain"],
                            company_name=company["name"],
                            github_date=company["github_date"],
                            run_id=self.run_id,
                        )

                except Exception as e:
                    logger.error(f"Error processing {company['name']}: {e}")
                    results.append(
                        {
                            "company": company["name"],
                            "status": "error",
                            "error": str(e),
                        }
                    )

        await self._find_decision_makers_if_needed()

        await self._generate_insights_if_needed()

        # Generate summary
        elapsed = (datetime.now() - start_time).total_seconds()
        summary = self._generate_summary(results, elapsed)
        self._print_summary(summary)
        return summary

    async def _find_decision_makers_if_needed(self):
        """Find decision makers for all upload-eligible companies missing one."""
        if not self.config.enable_decision_maker_lookup or not self.config.gemini_api_key:
            if self.config.enable_decision_maker_lookup and not self.config.gemini_api_key:
                logger.warning("Decision maker lookup enabled but GEMINI_API_KEY not set.")
            return

        companies_needing_dm = self.db.get_companies_needing_dm_lookup()

        if not companies_needing_dm:
            print("\n--- All companies already have decision maker lookups ---")
            return

        print(
            f"\n--- Looking up decision makers for "
            f"{len(companies_needing_dm)} companies ---"
        )

        # Format for DecisionMakerFinder (expects 'company' and 'domain' keys)
        lookup_list = [
            {"company": c["name"], "domain": c.get("domain", ""), "company_id": c["id"]}
            for c in companies_needing_dm
        ]

        try:
            finder = DecisionMakerFinder(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
                batch_size=self.config.gemini_batch_size,
            )
            dm_results = await finder.find_decision_makers(lookup_list)

            found_count = 0
            dm_by_company = {dm.company_name: dm for dm in dm_results}

            for company_info in lookup_list:
                company_id = company_info["company_id"]
                company_name = company_info["company"]
                dm = dm_by_company.get(company_name)

                if dm and dm.person_name:
                    found_count += 1
                    if not self.dry_run:
                        self.db.upsert_decision_maker(
                            company_id=company_id,
                            person_name=dm.person_name,
                            title=dm.title,
                            source_url=dm.source_url,
                            confidence=dm.confidence,
                        )
                        # Update employee_count and industry if available
                        updates = {}
                        if dm.employee_count:
                            updates["employee_count"] = dm.employee_count
                        if dm.industry:
                            updates["industry"] = dm.industry
                        if updates:
                            set_clause = ", ".join(f"{k} = ?" for k in updates)
                            values = list(updates.values()) + [company_id]
                            self.db.conn.execute(
                                f"UPDATE companies SET {set_clause} WHERE id = ?",
                                values,
                            )
                            self.db.conn.commit()

                # Mark attempt regardless of success/failure
                if not self.dry_run:
                    self.db.mark_dm_lookup_attempted(company_id)

            print(f"  Found {found_count}/{len(companies_needing_dm)} decision makers")

        except Exception as e:
            logger.error(f"Decision maker lookup failed: {e}")
            print(f"  Decision maker lookup failed: {e}")

    async def _generate_insights_if_needed(self):
        """Generate AI insights for companies that don't have one yet."""
        if not self.config.enable_insight_generation or not self.config.gemini_api_key:
            return

        top_companies = self.db.get_companies_sorted_by_recency()
        top_company_ids = [c["id"] for c in top_companies]
        companies_needing_insights = self.db.get_companies_needing_insights(top_company_ids)

        if not companies_needing_insights:
            print("\n--- All top companies already have insights ---")
            return

        print(
            f"\n--- Generating insights for "
            f"{len(companies_needing_insights)} companies ---"
        )
        try:
            insight_input = []
            for comp in companies_needing_insights:
                jobs = self.db.get_jobs_for_company_by_id(comp["id"])
                role_titles = [j["title"] for j in jobs]
                insight_input.append({
                    "company_name": comp["name"],
                    "domain": comp["domain"],
                    "roles": role_titles,
                })

            generator = InsightGenerator(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
                batch_size=10,
            )
            insights = await generator.generate_insights(insight_input)

            if not self.dry_run:
                for comp in companies_needing_insights:
                    insight_text = insights.get(comp["name"])
                    if insight_text:
                        self.db.update_company_insight(comp["id"], insight_text)

            print(f"  Generated {len(insights)} insights")
        except Exception as e:
            logger.error(f"Insight generation failed: {e}")
            print(f"  Insight generation failed: {e}")

    async def _load_companies_from_github(
        self, client: httpx.AsyncClient
    ) -> List[tuple]:
        """Load new companies and their jobs from GitHub README.

        Returns list of (company_dict, [GitHubListing, ...]) tuples,
        grouped by company and filtered to only unseen companies.
        """
        if self.include_all_days:
            listings = await self.github_scraper.fetch_all_listings(client)
            print(f"Fetched {len(listings)} total listings from GitHub (all days)")
        else:
            listings = await self.github_scraper.fetch_todays_listings(
                client, target_date=self.target_date
            )
            print(
                f"Fetched {len(listings)} listings from GitHub "
                f"for {self.target_date.isoformat()}"
            )

        # Group listings by company domain
        companies: Dict[str, Dict] = {}  # domain -> company dict
        company_jobs: Dict[str, List[GitHubListing]] = {}  # domain -> listings

        for listing in listings:
            domain = listing.company_domain
            if domain not in companies:
                companies[domain] = {
                    "name": listing.company_name,
                    "domain": domain,
                    "website": listing.company_url,
                    "industry": "",
                    "keywords": "",
                    "technologies": "",
                    "employee_count": None,
                    "github_date": listing.date_posted.isoformat(),
                }
                company_jobs[domain] = []
            company_jobs[domain].append(listing)

        print(f"Unique companies: {len(companies)}")

        # Filter out already-seen companies
        result = []
        skipped = 0
        for domain, company in companies.items():
            if self.db.is_company_seen(domain):
                skipped += 1
                continue
            result.append((company, company_jobs[domain]))

        if skipped > 0:
            print(f"Skipped {skipped} already-seen companies")

        return result

    def _store_company_and_jobs(
        self, company: Dict, jobs: List[GitHubListing]
    ) -> Dict[str, Any]:
        """Store a company and its GitHub-sourced jobs in the database."""
        # Upsert company
        company_id, _ = self.db.upsert_company(company, run_id=self.run_id)

        # Store each job listing directly
        stored_count = 0
        job_details = []
        for listing in jobs:
            # Generate a stable external_id from the job URL
            external_id = hashlib.md5(listing.job_url.encode()).hexdigest()[:16]

            job_dict = {
                "external_id": external_id,
                "title": listing.job_title,
                "department": "",
                "location": listing.location,
                "description": "",
                "job_url": listing.job_url,
                "posting_date": listing.date_posted.isoformat(),
                "relevance_score": 80.0,  # All GitHub listings are marketing roles
                "matched_category": "general_marketing",
            }

            if not self.dry_run:
                self.db.insert_job(job_dict, company_id)
            stored_count += 1
            job_details.append({
                "title": listing.job_title,
                "posting_date": listing.date_posted.isoformat(),
            })

        # Update urgency score
        if not self.dry_run:
            self.db.update_company_urgency(company_id, stored_count)

        return {
            "company": company["name"],
            "company_id": company_id,
            "domain": company["domain"],
            "status": "success",
            "jobs_found": stored_count,
            "new_jobs": stored_count,
            "removed_jobs": 0,
            "job_details": job_details,
        }

    def _generate_summary(self, results: List[Dict], elapsed: float) -> Dict[str, Any]:
        """Generate run summary."""
        successful = [r for r in results if r.get("status") == "success"]

        return {
            "run_date": datetime.now().isoformat(),
            "elapsed_seconds": elapsed,
            "companies_processed": len(results),
            "companies_successful": len(successful),
            "total_jobs_found": sum(r.get("jobs_found", 0) for r in successful),
            "total_new_jobs": sum(r.get("new_jobs", 0) for r in successful),
            "total_removed_jobs": sum(r.get("removed_jobs", 0) for r in successful),
            "decision_makers_found": sum(
                1 for r in results
                if r.get("decision_maker", {}).get("person_name")
            ),
            "emails_found": sum(
                1 for r in results
                if r.get("decision_maker", {}).get("email")
            ),
            "by_status": self._count_by_key(results, "status"),
            "details": results,
        }

    def _count_by_key(self, items: List[Dict], key: str) -> Dict[str, int]:
        """Count items by a key."""
        counts: Dict[str, int] = {}
        for item in items:
            value = item.get(key, "unknown")
            counts[value] = counts.get(value, 0) + 1
        return counts

    def _print_summary(self, summary: Dict) -> None:
        """Print run summary to console."""
        print("\n" + "=" * 70)
        print("RUN SUMMARY")
        print("=" * 70)
        print(f"Duration: {summary['elapsed_seconds']:.1f}s")
        print(
            f"Companies: {summary['companies_successful']}/{summary['companies_processed']} successful"
        )
        print(f"Jobs Found: {summary['total_jobs_found']}")
        print(f"Decision Makers Found: {summary.get('decision_makers_found', 0)}")
        print(f"Emails Found: {summary.get('emails_found', 0)}")
        print("\n--- By Status ---")
        for status, count in summary["by_status"].items():
            print(f"  {status}: {count}")
        print("=" * 70)

        # Print jobs by company
        self._print_jobs_by_company(summary["details"])

    def _print_jobs_by_company(self, results: List[Dict]) -> None:
        """Print jobs grouped by company."""
        companies_with_jobs = [
            r for r in results
            if r.get("status") == "success" and r.get("jobs_found", 0) > 0
        ]

        if not companies_with_jobs:
            return

        companies_with_jobs.sort(key=lambda x: x.get("jobs_found", 0), reverse=True)

        print("\n" + "=" * 70)
        print("JOBS BY COMPANY")
        print("=" * 70)

        for result in companies_with_jobs:
            company_name = result.get("company", "Unknown")
            job_count = result.get("jobs_found", 0)
            job_details = result.get("job_details", [])

            print(f"\n{company_name} ({job_count} roles)")
            print("-" * 50)

            for job in job_details:
                title = job.get("title", "Unknown")
                posting_date = job.get("posting_date", "Unknown")
                print(f"  - {title} (Posted: {posting_date})")

            dm = result.get("decision_maker")
            if dm and dm.get("person_name"):
                print(
                    f"  Decision Maker: {dm['person_name']} "
                    f"- {dm.get('title', 'N/A')}"
                )
                if dm.get("email"):
                    print(f"    Email: {dm['email']}")
                if dm.get("linkedin_url"):
                    print(f"    LinkedIn: {dm['linkedin_url']}")

        print("\n" + "=" * 70)
