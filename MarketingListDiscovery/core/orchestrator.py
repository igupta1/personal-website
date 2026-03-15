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
from .priority_classifier import PriorityClassifier
from .outreach_generator import OutreachGenerator
from .relevancy_screener import RelevancyScreener
from ..scrapers.github_scraper import GitHubReadmeScraper

logger = logging.getLogger(__name__)


class ListDiscoveryOrchestrator:
    """
    Main orchestrator for the list discovery pipeline.

    Pipeline:
    1. Clean up stale data (>7 days old)
    2. Scrape GitHub README for today's job listings
    3. Filter out companies with no new jobs
    4. Store companies and their jobs directly from GitHub
    5. Enrich with decision makers
    6. Generate insights for new companies
    7. Mark companies as seen
    8. Generate summary report
    """

    # Maximum companies to process through expensive enrichment stages
    MAX_ENRICHMENT_COMPANIES = 100
    # Minimum relevancy score to proceed (filters out competitors, enterprises)
    MIN_RELEVANCY_SCORE = 40

    def __init__(
        self,
        config: Config,
        database: Database,
        dry_run: bool = False,
        target_date: Optional[date] = None,
        include_all_days: bool = False,
        max_companies: int = 0,
    ):
        self.config = config
        self.db = database
        self.dry_run = dry_run
        self.target_date = target_date or date.today()
        self.include_all_days = include_all_days
        self.max_companies = max_companies
        self.github_scraper = GitHubReadmeScraper(repo=config.github_repo)
        self._new_company_ids: List[int] = []
        self._errors: List[Dict[str, str]] = []

    async def run(self) -> Dict[str, Any]:
        """Execute the full list discovery pipeline."""
        start_time = datetime.now()

        # Generate unique run_id for this daily run
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Clean up stale data before processing
        if not self.dry_run:
            counts = self.db.cleanup_old_data()
            total = sum(counts.values())
            if total > 0:
                print(
                    f"Cleanup: removed {counts['jobs']} old jobs, "
                    f"{counts['companies']} companies, "
                    f"{counts['decision_makers']} decision makers, "
                    f"{counts['seen_companies']} seen entries"
                )

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

            # Early relevancy screening: score all companies, keep top N
            companies_with_jobs = await self._screen_relevancy(companies_with_jobs)

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
                    self._record_error("processing", f"{company['name']}: {e}")
                    results.append(
                        {
                            "company": company["name"],
                            "status": "error",
                            "error": str(e),
                        }
                    )

        await self._find_decision_makers_if_needed()

        await self._generate_insights_if_needed()

        await self._classify_priority_tiers()

        await self._generate_outreach_if_needed()

        # Generate summary
        elapsed = (datetime.now() - start_time).total_seconds()
        summary = self._generate_summary(results, elapsed)
        self._print_summary(summary)
        return summary

    async def _screen_relevancy(
        self, companies_with_jobs: List[tuple]
    ) -> List[tuple]:
        """Score all companies by relevancy and return only top N."""
        if not self.config.anthropic_api_key:
            logger.warning("No ANTHROPIC_API_KEY set, skipping relevancy screening")
            return companies_with_jobs

        print(f"\n--- Screening {len(companies_with_jobs)} companies for relevancy ---")

        screener_input = []
        for company, jobs in companies_with_jobs:
            screener_input.append({
                "company_name": company["name"],
                "domain": company["domain"],
                "roles": [j.job_title for j in jobs],
            })

        try:
            screener = RelevancyScreener(
                api_key=self.config.anthropic_api_key,
                model=self.config.anthropic_model,
                batch_size=10,
            )
            scores = await screener.screen_companies(screener_input)

            # Attach scores and sort descending
            scored = []
            for company, jobs in companies_with_jobs:
                result = scores.get(company["name"], {"score": 50, "reason": "Not scored"})
                scored.append((company, jobs, result["score"], result["reason"]))

            scored.sort(key=lambda x: x[2], reverse=True)

            # Print ranking
            for rank, (company, jobs, score, reason) in enumerate(scored, 1):
                marker = "*" if rank <= self.MAX_ENRICHMENT_COMPANIES else " "
                print(f"  {marker} #{rank} [{score}] {company['name']} - {reason}")

            # Filter by minimum score, then cap at top N
            qualified = [(c, j, s, r) for c, j, s, r in scored if s >= self.MIN_RELEVANCY_SCORE]
            disqualified = len(scored) - len(qualified)
            if disqualified > 0:
                print(f"\n  Disqualified {disqualified} companies below score {self.MIN_RELEVANCY_SCORE}")

            limit = self.MAX_ENRICHMENT_COMPANIES
            if self.max_companies > 0:
                limit = min(limit, self.max_companies)

            kept = qualified[:limit]
            dropped = len(qualified) - len(kept)
            if dropped > 0:
                print(f"  Capped to top {len(kept)} (dropped {dropped} above cutoff)")

            return [(company, jobs) for company, jobs, score, reason in kept]

        except Exception as e:
            logger.error(f"Relevancy screening failed: {e}")
            self._record_error("relevancy_screening", f"Screening failed: {e}")
            print(f"  Screening failed ({e}), proceeding with all companies")
            # Fall back to max_companies limit if set
            if self.max_companies > 0:
                return companies_with_jobs[:self.max_companies]
            return companies_with_jobs

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
            self._record_error("decision_makers", f"Decision maker lookup failed: {e}", critical=True)
            print(f"  Decision maker lookup failed: {e}")

    async def _generate_insights_if_needed(self):
        """Generate AI insights for newly added companies that don't have one yet."""
        if not self.config.enable_insight_generation or not self.config.anthropic_api_key:
            return

        # Only generate insights for companies processed in this run
        # Falls back to all companies with missing insights if no new ones
        companies_needing_insights = self.db.get_companies_with_missing_insights(
            self._new_company_ids if self._new_company_ids else None
        )

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
                api_key=self.config.anthropic_api_key,
                model=self.config.anthropic_model,
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
            self._record_error("insights", f"Insight generation failed: {e}")
            print(f"  Insight generation failed: {e}")

    async def _classify_priority_tiers(self):
        """Classify priority tiers for companies that don't have one yet."""
        if not self.config.enable_priority_classification or not self.config.anthropic_api_key:
            return

        companies = self.db.get_companies_needing_priority_classification(
            self._new_company_ids if self._new_company_ids else None
        )

        if not companies:
            print("\n--- All companies already have priority tiers ---")
            return

        print(f"\n--- Classifying priority tiers for {len(companies)} companies ---")

        try:
            classifier_input = []
            for comp in companies:
                jobs = self.db.get_jobs_for_company_by_id(comp["id"])
                role_titles = [j["title"] for j in jobs]
                classifier_input.append({
                    "company_name": comp["name"],
                    "domain": comp["domain"],
                    "industry": comp.get("industry") or "",
                    "employee_count": comp.get("employee_count"),
                    "roles": role_titles,
                    "has_decision_maker": bool(comp.get("has_decision_maker")),
                })

            classifier = PriorityClassifier(
                api_key=self.config.anthropic_api_key,
                model=self.config.anthropic_model,
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
            self._record_error("priority_classification", f"Priority classification failed: {e}")
            print(f"  Priority classification failed: {e}")

    async def _generate_outreach_if_needed(self):
        """Generate personalized outreach drafts for companies missing them."""
        if not self.config.enable_outreach_generation or not self.config.anthropic_api_key:
            return

        companies = self.db.get_companies_needing_outreach(
            self._new_company_ids if self._new_company_ids else None
        )

        if not companies:
            print("\n--- All companies already have outreach drafts ---")
            return

        print(f"\n--- Generating outreach drafts for {len(companies)} companies ---")

        try:
            outreach_input = []
            for comp in companies:
                jobs = self.db.get_jobs_for_company_by_id(comp["id"])
                role_titles = [j["title"] for j in jobs]
                outreach_input.append({
                    "company_name": comp["name"],
                    "domain": comp["domain"],
                    "roles": role_titles,
                    "company_id": comp["id"],
                })

            generator = OutreachGenerator(
                api_key=self.config.anthropic_api_key,
                model=self.config.anthropic_model,
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
            self._record_error("outreach", f"Outreach generation failed: {e}")
            print(f"  Outreach generation failed: {e}")

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

        if not listings:
            self._record_error("github_scrape", "No listings found from GitHub README", critical=True)

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

        # Filter out companies with no new jobs
        result = []
        skipped = 0
        for domain, company in companies.items():
            # Compute external_ids for all jobs (same MD5 logic as _store_company_and_jobs)
            job_external_ids = [
                hashlib.md5(listing.job_url.encode()).hexdigest()[:16]
                for listing in company_jobs[domain]
            ]
            if not self.db.has_new_jobs(domain, job_external_ids):
                skipped += 1
                continue
            result.append((company, company_jobs[domain]))

        if skipped > 0:
            print(f"Skipped {skipped} companies with no new jobs")

        return result

    def _store_company_and_jobs(
        self, company: Dict, jobs: List[GitHubListing]
    ) -> Dict[str, Any]:
        """Store a company and its GitHub-sourced jobs in the database."""
        # Upsert company
        company_id, _ = self.db.upsert_company(company, run_id=self.run_id)
        self._new_company_ids.append(company_id)

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

        # Reset DM lookup for re-enrichment
        if not self.dry_run:
            self.db.conn.execute(
                "UPDATE companies SET dm_lookup_attempted_at = NULL WHERE id = ?",
                (company_id,),
            )
            self.db.conn.commit()

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

    def _record_error(self, stage: str, message: str, critical: bool = False):
        """Record an error for end-of-run reporting."""
        self._errors.append({
            "stage": stage,
            "message": message,
            "severity": "critical" if critical else "warning",
        })

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
            "by_status": self._count_by_key(results, "status"),
            "details": results,
            "errors": self._errors,
            "has_critical_errors": any(
                e["severity"] == "critical" for e in self._errors
            ),
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
        print("\n--- By Status ---")
        for status, count in summary["by_status"].items():
            print(f"  {status}: {count}")

        if self._errors:
            print("\n--- Errors ---")
            for err in self._errors:
                prefix = "CRITICAL" if err["severity"] == "critical" else "WARNING"
                print(f"  [{prefix}] {err['stage']}: {err['message']}")
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

        print("\n" + "=" * 70)
