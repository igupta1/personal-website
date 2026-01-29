"""Main orchestrator for the job discovery pipeline."""

import asyncio
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from urllib.parse import urlparse
import httpx

from ..config import Config
from .database import Database
from .models import Company, JobPosting, ATSDetectionResult
from .caching import ATSDetectionCache
from .change_detector import ChangeDetector
from .relevance_scorer import RoleRelevanceScorer
from .decision_maker import DecisionMakerFinder
from .email_finder import ApolloEmailFinder
from ..ats.enhanced_detector import EnhancedATSDetector
from ..ats.greenhouse import GreenhouseClient
from ..ats.lever import LeverClient
from ..ats.ashby import AshbyClient
from ..ats.workable import WorkableClient
from ..ats.jobvite import JobviteClient
from ..ats.smartrecruiters import SmartRecruitersClient
from ..ats.recruitee import RecruiteeClient
from ..ats.breezyhr import BreezyHRClient
from ..ats.personio import PersonioClient
from ..scrapers.robots_checker import RobotsChecker

logger = logging.getLogger(__name__)


class JobDiscoveryOrchestrator:
    """
    Main orchestrator for the job discovery pipeline.

    Pipeline:
    1. Load companies from Apollo CSV
    2. For each company:
       a. Check robots.txt compliance
       b. Detect ATS platform (cached)
       c. Fetch jobs via ATS API
       d. Score jobs for marketing relevance
       e. Detect changes (new/removed)
       f. Update database
    3. Generate summary report
    """

    ATS_CLIENTS = {
        "greenhouse": GreenhouseClient,
        "lever": LeverClient,
        "ashby": AshbyClient,
        "workable": WorkableClient,
        "jobvite": JobviteClient,
        "smartrecruiters": SmartRecruitersClient,
        "recruitee": RecruiteeClient,
        "breezyhr": BreezyHRClient,
        "personio": PersonioClient,
    }

    def __init__(
        self,
        config: Config,
        database: Database,
        max_jobs: int = 100,
        dry_run: bool = False,
    ):
        self.config = config
        self.db = database
        self.max_jobs = max_jobs
        self.dry_run = dry_run

        self.relevance_scorer = RoleRelevanceScorer(
            relevance_threshold=config.relevance_threshold
        )

        self.ats_cache = ATSDetectionCache(
            database.conn, ttl_days=config.ats_cache_ttl_days
        )

    async def run(self) -> Dict[str, Any]:
        """Execute the full job discovery pipeline."""
        start_time = datetime.now()

        # Load companies from CSV
        companies = self._load_companies_from_csv()
        logger.info(f"Loaded {len(companies)} companies from Apollo CSV")
        print(f"Loaded {len(companies)} companies")

        # Initialize HTTP client with rate limiting
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        async with httpx.AsyncClient(
            limits=limits,
            timeout=httpx.Timeout(self.config.http_timeout),
            headers={"User-Agent": self.config.user_agent},
        ) as client:
            self.ats_detector = EnhancedATSDetector(client)
            self.robots_checker = RobotsChecker(client)

            results = []
            total_jobs_processed = 0

            for i, company in enumerate(companies, 1):
                if total_jobs_processed >= self.max_jobs:
                    logger.info(f"Reached max jobs limit ({self.max_jobs})")
                    break

                print(f"\n[{i}/{len(companies)}] Processing: {company['name']}")

                try:
                    result = await self._process_company(client, company)
                    results.append(result)

                    if result.get("status") == "success":
                        total_jobs_processed += result.get("jobs_found", 0)
                        print(
                            f"  -> {result.get('ats', 'unknown')} | "
                            f"{result.get('jobs_found', 0)} jobs | "
                            f"+{result.get('new_jobs', 0)} new | "
                            f"-{result.get('removed_jobs', 0)} removed"
                        )
                    elif result.get("status") == "linkedin_only":
                        linkedin_slug = result.get("linkedin_slug", "unknown")
                        print(f"  -> linkedin_only (slug: {linkedin_slug})")
                    else:
                        print(f"  -> {result.get('status', 'error')}")

                    # Rate limiting between companies
                    await asyncio.sleep(self.config.delay_between_companies)

                except Exception as e:
                    logger.error(f"Error processing {company['name']}: {e}")
                    results.append(
                        {
                            "company": company["name"],
                            "status": "error",
                            "error": str(e),
                        }
                    )

        # Decision Maker Lookup (after all companies processed)
        if self.config.enable_decision_maker_lookup and self.config.gemini_api_key:
            successful_with_jobs = [
                r
                for r in results
                if r.get("status") == "success" and r.get("jobs_found", 0) > 0
            ]

            if successful_with_jobs:
                print(
                    f"\n--- Looking up decision makers for "
                    f"{len(successful_with_jobs)} companies ---"
                )
                try:
                    finder = DecisionMakerFinder(
                        api_key=self.config.gemini_api_key,
                        model=self.config.gemini_model,
                        batch_size=self.config.gemini_batch_size,
                    )
                    dm_results = await finder.find_decision_makers(
                        successful_with_jobs
                    )

                    # Attach results to corresponding company result dicts
                    dm_by_company = {dm.company_name: dm for dm in dm_results}
                    for result in results:
                        company_name = result.get("company")
                        if company_name in dm_by_company:
                            dm = dm_by_company[company_name]
                            result["decision_maker"] = {
                                "person_name": dm.person_name,
                                "title": dm.title,
                                "source_url": dm.source_url,
                                "confidence": dm.confidence,
                                "not_found_reason": dm.not_found_reason,
                            }

                    # Store in database if not dry run
                    if not self.dry_run:
                        for dm in dm_results:
                            if not dm.person_name:
                                continue
                            company_result = next(
                                (
                                    r
                                    for r in results
                                    if r.get("company") == dm.company_name
                                ),
                                None,
                            )
                            if company_result:
                                self.db.upsert_decision_maker(
                                    company_id=company_result["company_id"],
                                    person_name=dm.person_name,
                                    title=dm.title,
                                    source_url=dm.source_url,
                                    confidence=dm.confidence,
                                )

                except Exception as e:
                    logger.error(f"Decision maker lookup failed: {e}")
                    print(f"  Decision maker lookup failed: {e}")

        elif self.config.enable_decision_maker_lookup and not self.config.gemini_api_key:
            logger.warning(
                "Decision maker lookup enabled but GEMINI_API_KEY not set. "
                "Skipping."
            )

        # Apollo Email Lookup (after decision makers are found)
        if self.config.enable_email_lookup and self.config.apollo_api_key:
            # Collect decision makers that were found
            dm_results_for_email = [
                r
                for r in results
                if r.get("decision_maker", {}).get("person_name")
            ]

            if dm_results_for_email:
                print(
                    f"\n--- Looking up emails for "
                    f"{len(dm_results_for_email)} decision makers ---"
                )
                try:
                    from .models import DecisionMakerResult as DMR

                    dm_objects = [
                        DMR(
                            company_name=r["company"],
                            person_name=r["decision_maker"]["person_name"],
                            title=r["decision_maker"].get("title"),
                        )
                        for r in dm_results_for_email
                    ]

                    email_finder = ApolloEmailFinder(
                        api_key=self.config.apollo_api_key,
                        batch_size=self.config.apollo_batch_size,
                    )
                    email_results = await email_finder.find_emails(
                        dm_objects, results
                    )

                    # Attach email results to company result dicts
                    email_by_company = {
                        er.company_name: er for er in email_results
                    }
                    for result in results:
                        company_name = result.get("company")
                        if company_name in email_by_company:
                            er = email_by_company[company_name]
                            dm = result.get("decision_maker", {})
                            dm["email"] = er.email
                            dm["linkedin_url"] = er.linkedin_url

                    # Persist emails to database if not dry run
                    if not self.dry_run:
                        for er in email_results:
                            if not er.email:
                                continue
                            company_result = next(
                                (
                                    r
                                    for r in results
                                    if r.get("company") == er.company_name
                                ),
                                None,
                            )
                            if company_result and company_result.get("decision_maker"):
                                dm = company_result["decision_maker"]
                                self.db.upsert_decision_maker(
                                    company_id=company_result["company_id"],
                                    person_name=dm.get("person_name", ""),
                                    title=dm.get("title"),
                                    source_url=dm.get("source_url"),
                                    confidence=dm.get("confidence"),
                                    email=er.email,
                                    linkedin_url=er.linkedin_url,
                                )

                except Exception as e:
                    logger.error(f"Apollo email lookup failed: {e}")
                    print(f"  Apollo email lookup failed: {e}")

        elif self.config.enable_email_lookup and not self.config.apollo_api_key:
            logger.warning(
                "Email lookup enabled but APOLLO_API_KEY not set. Skipping."
            )

        # Generate summary
        elapsed = (datetime.now() - start_time).total_seconds()
        summary = self._generate_summary(results, elapsed)

        self._print_summary(summary)

        return summary

    def _load_companies_from_csv(self) -> List[Dict]:
        """Load companies from Apollo export CSV."""
        csv_path = self.config.input_csv_path
        companies = []

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Extract domain from website URL
                website = row.get("Website", "")
                domain = self._extract_domain(website)

                if not domain:
                    logger.warning(f"Skipping company with no domain: {row.get('Company Name', 'Unknown')}")
                    continue

                companies.append(
                    {
                        "name": row.get("Company Name", ""),
                        "domain": domain,
                        "website": website,
                        "industry": row.get("Industry", ""),
                        "keywords": row.get("Keywords", ""),
                        "technologies": row.get("Technologies", ""),
                        "employee_count": self._parse_employee_count(
                            row.get("# Employees", "")
                        ),
                    }
                )

        return companies

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        if not url:
            return None
        url = url if url.startswith("http") else f"https://{url}"
        parsed = urlparse(url)
        return parsed.netloc.replace("www.", "")

    def _parse_employee_count(self, value: str) -> Optional[int]:
        """Parse employee count from string."""
        if not value:
            return None
        try:
            return int(value.replace(",", ""))
        except ValueError:
            return None

    async def _process_company(
        self, client: httpx.AsyncClient, company: Dict
    ) -> Dict[str, Any]:
        """Process a single company through the pipeline."""
        logger.info(f"Processing: {company['name']} ({company['domain']})")

        # Step 1: Ensure company exists in database
        company_id = self.db.upsert_company(company)

        # Step 2: Check robots.txt for domain
        primary_url = f"https://{company['domain']}/careers"
        if not await self.robots_checker.can_fetch(primary_url):
            logger.warning(f"Blocked by robots.txt: {company['domain']}")
            return {"company": company["name"], "status": "blocked_robots"}

        # Step 3: Detect ATS using enhanced detector (API probing first)
        ats_result = await self._detect_ats_for_company(client, company)

        # Handle linkedin_only classification
        if ats_result.provider == "linkedin_only":
            logger.info(f"LinkedIn-only for {company['name']}")
            # Store the LinkedIn info in database
            if not self.dry_run:
                self.db.update_company_ats(
                    company_id, "linkedin_only", ats_result.board_token
                )
            return {
                "company": company["name"],
                "status": "linkedin_only",
                "linkedin_slug": ats_result.board_token,
            }

        if not ats_result.provider or ats_result.provider == "unknown":
            logger.info(f"Unknown ATS for {company['name']}")
            return {"company": company["name"], "status": "unknown_ats"}

        # Step 5: Fetch jobs from ATS
        ats_client_class = self.ATS_CLIENTS.get(ats_result.provider)
        if not ats_client_class:
            return {
                "company": company["name"],
                "status": "unsupported_ats",
                "ats": ats_result.provider,
            }

        ats_client = ats_client_class(client, ats_result.board_token)

        try:
            jobs = await ats_client.fetch_jobs()
        except Exception as e:
            logger.error(f"Failed to fetch jobs from {ats_result.provider}: {e}")
            return {
                "company": company["name"],
                "status": "fetch_error",
                "ats": ats_result.provider,
                "error": str(e),
            }

        # Step 6: Score for marketing relevance
        relevant_jobs = []
        for job in jobs:
            result = self.relevance_scorer.score(job.title, job.description or "")
            if result.is_relevant:
                job_dict = {
                    "external_id": job.external_id,
                    "title": job.title,
                    "department": job.department,
                    "location": job.location,
                    "description": job.description,
                    "job_url": job.job_url,
                    "posting_date": (
                        job.posting_date.isoformat() if job.posting_date else None
                    ),
                    "relevance_score": result.score,
                    "matched_category": result.matched_category,
                }
                relevant_jobs.append(job_dict)

        # Step 7: Detect changes
        change_detector = ChangeDetector(self.db.conn)
        change_report = change_detector.detect_changes(
            company_id, company["name"], relevant_jobs
        )

        # Step 8: Apply changes (unless dry run)
        if not self.dry_run:
            change_detector.apply_changes(change_report, relevant_jobs)
            self.db.update_company_ats(
                company_id, ats_result.provider, ats_result.board_token
            )
            # Update urgency score (count of marketing jobs)
            self.db.update_company_urgency(company_id, len(relevant_jobs))

        # Collect job details for reporting
        job_details = [
            {
                "title": job["title"],
                "posting_date": job.get("posting_date"),
            }
            for job in relevant_jobs
        ]

        # Collect removed job details from change report
        removed_job_details = [
            {"title": job.title, "external_id": job.external_id}
            for job in change_report.removed_jobs
        ]

        return {
            "company": company["name"],
            "company_id": company_id,
            "domain": company["domain"],
            "status": "success",
            "ats": ats_result.provider,
            "total_jobs": len(jobs),
            "jobs_found": len(relevant_jobs),
            "new_jobs": len(change_report.new_jobs),
            "removed_jobs": len(change_report.removed_jobs),
            "job_details": job_details,
            "removed_job_details": removed_job_details,
        }

    async def _detect_ats_for_company(
        self,
        client: httpx.AsyncClient,
        company: Dict,
    ) -> ATSDetectionResult:
        """Detect ATS for a company using the enhanced detector."""
        domain = company["domain"]

        # Check cache first
        cached = self.ats_cache.get(domain)
        if cached:
            return ATSDetectionResult(
                provider=cached["provider"],
                board_token=cached["board_token"],
                confidence=1.0,
                detection_method="cache",
            )

        # Use enhanced detector with smart token generation and API probing
        result = await self.ats_detector.detect(
            company_name=company["name"],
            domain=domain,
            technologies=company.get("technologies", ""),
        )

        # Cache successful detections (including linkedin_only)
        if result.provider and result.provider != "unknown":
            self.ats_cache.set(domain, result.provider, result.board_token)

        return result

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
                1
                for r in results
                if r.get("decision_maker", {}).get("person_name")
            ),
            "emails_found": sum(
                1
                for r in results
                if r.get("decision_maker", {}).get("email")
            ),
            "by_status": self._count_by_key(results, "status"),
            "by_ats": self._count_by_key(successful, "ats"),
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
        print(f"New Jobs: {summary['total_new_jobs']}")
        print(f"Removed Jobs: {summary['total_removed_jobs']}")
        print(f"Decision Makers Found: {summary.get('decision_makers_found', 0)}")
        print(f"Emails Found: {summary.get('emails_found', 0)}")
        print("\n--- By Status ---")
        for status, count in summary["by_status"].items():
            print(f"  {status}: {count}")
        print("\n--- By ATS ---")
        for ats, count in summary["by_ats"].items():
            print(f"  {ats}: {count}")
        print("=" * 70)

        # Print detailed marketing jobs by company (sorted by urgency/job count)
        self._print_jobs_by_company(summary["details"])

    def _print_jobs_by_company(self, results: List[Dict]) -> None:
        """Print detailed marketing jobs grouped by company, sorted by urgency."""
        # Filter to only successful results with jobs
        companies_with_jobs = [
            r for r in results
            if r.get("status") == "success" and r.get("jobs_found", 0) > 0
        ]

        if not companies_with_jobs:
            return

        # Sort by number of marketing jobs (urgency) descending
        companies_with_jobs.sort(key=lambda x: x.get("jobs_found", 0), reverse=True)

        print("\n" + "=" * 70)
        print("MARKETING JOBS BY COMPANY (Sorted by Urgency)")
        print("=" * 70)

        for result in companies_with_jobs:
            company_name = result.get("company", "Unknown")
            job_count = result.get("jobs_found", 0)
            job_details = result.get("job_details", [])

            print(f"\n{company_name} ({job_count} marketing roles)")
            print("-" * 50)

            # Print active jobs with posting dates
            for job in job_details:
                title = job.get("title", "Unknown")
                date = job.get("posting_date")
                if date:
                    # Parse ISO date to readable format (YYYY-MM-DD)
                    try:
                        parsed = datetime.fromisoformat(date)
                        date_str = parsed.strftime("%Y-%m-%d")
                    except:
                        date_str = date[:10] if len(date) >= 10 else date
                    print(f"  - {title} (Posted: {date_str})")
                else:
                    print(f"  - {title} (Posted: Unknown)")

            # Print removed jobs if any
            removed_details = result.get("removed_job_details", [])
            if removed_details:
                print(f"\n  Recently Removed ({len(removed_details)} roles):")
                for job in removed_details:
                    print(f"    × {job.get('title', 'Unknown')}")

            # Print decision maker if available
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
                if dm.get("source_url"):
                    print(f"    Source: {dm['source_url']}")
                if dm.get("confidence"):
                    print(f"    Confidence: {dm['confidence']}")
            elif dm and dm.get("not_found_reason"):
                print(f"  Decision Maker: {dm['not_found_reason']}")

        print("\n" + "=" * 70)
