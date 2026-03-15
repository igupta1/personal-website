"""Main orchestrator for the IT MSP discovery pipeline."""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any

from ..config import Config
from .database import Database
from .serpapi_client import SerpAPIJobClient
from .decision_maker import ITDecisionMakerFinder
from .insight_and_priority import InsightAndPriorityGenerator
from .outreach_generator import (
    OutreachGenerator, _classify_role, _clean_role_title, _a_or_an,
    _pick_closing, _MSP_CLOSINGS, _NON_MSP_CLOSINGS, _strip_em_dashes,
)
from .job_verifier import JobVerifier
from .relevancy_screener import RelevancyScreener

logger = logging.getLogger(__name__)


class ITMSPOrchestrator:
    """
    Pipeline:
    1. SerpAPI Google Jobs search across metros
    2. Deduplicate listings (within run + across previous runs)
    3. Store companies and jobs in SQLite
    4. Early relevancy screening (Claude)
    5. Gemini lookup for decision makers + employee count + industry
    6. Early employee_count filter (>100 screened out)
    7. Generate insights + classify priority tiers (Claude, combined)
    8. Generate personalized outreach drafts (Claude, batched)
    9. Assign P5 fallback outreach (no LLM)
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

        # Build today's cluster schedule (3 clusters with rotating metro counts)
        schedule = SerpAPIJobClient.get_cluster_schedule(
            all_metros=self.config.metro_areas,
            queries=self.config.search_queries,
            rotation_patterns=self.config.cluster_rotation_patterns,
            state_path=self.config.metro_state_path,
        )
        for query, metros in schedule:
            label = query[:50].strip("()")
            print(f"  {label}  →  {', '.join(metros)}")

        all_listings = client.search_all(query_metro_pairs=schedule)
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

        # Step 3: Early relevancy screening
        await self._screen_relevancy()

        # Step 4: Decision makers
        dm_found = await self._find_decision_makers_if_needed()

        # Step 5: Early employee_count filter
        self._filter_large_companies()

        # Step 6: Insights + Priority (combined)
        await self._generate_insights_and_priorities()

        # Step 7: Outreach drafts
        await self._generate_outreach_if_needed()

        # Step 8: P5 fallback outreach
        self._assign_p5_fallback_outreach()

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

    async def _screen_relevancy(self):
        """Screen companies for relevancy before expensive enrichment."""
        if not self.config.enable_relevancy_screening or not self.config.anthropic_api_key:
            if self.config.enable_relevancy_screening and not self.config.anthropic_api_key:
                print("\n--- Skipping relevancy screening (ANTHROPIC_API_KEY not set) ---")
            else:
                print("\n--- Skipping relevancy screening (disabled) ---")
            return

        companies = self.db.get_companies_needing_relevancy_screening()

        if not companies:
            print("\n--- All companies already screened for relevancy ---")
            return

        print(f"\n" + "=" * 70)
        print(f"Step 3: Screening {len(companies)} companies for relevancy...")
        print("=" * 70)

        # Build input with job data for richer context
        screener_input = []
        for comp in companies:
            jobs = self.db.get_jobs_for_company(comp["id"])
            roles = [j["title"] for j in jobs]
            location = jobs[0]["location"] if jobs else ""
            snippets = [j.get("description_snippet", "") for j in jobs if j.get("description_snippet")]
            snippet = snippets[0] if snippets else ""

            screener_input.append({
                "company_name": comp["name"],
                "roles": roles,
                "location": location,
                "description_snippet": snippet,
            })

        try:
            screener = RelevancyScreener(
                api_key=self.config.anthropic_api_key,
                model=self.config.anthropic_model,
                batch_size=15,
            )
            results = await screener.screen_companies(screener_input)

            # Sort by score descending for display
            scored = []
            for comp in companies:
                result = results.get(comp["name"], {"score": 50, "reason": "No score returned"})
                scored.append((comp, result["score"], result["reason"]))
            scored.sort(key=lambda x: x[1], reverse=True)

            kept = 0
            screened_out = 0
            threshold = self.config.relevancy_screening_threshold

            for comp, score, reason in scored:
                is_screened_out = score < threshold
                if not self.dry_run:
                    self.db.update_company_relevancy(
                        comp["id"], score, reason, is_screened_out
                    )
                if is_screened_out:
                    screened_out += 1
                    print(f"  [-] {comp['name']}: {score}/100 - {reason}")
                else:
                    kept += 1
                    print(f"  [+] {comp['name']}: {score}/100 - {reason}")

            print(f"\n  Kept {kept} companies, screened out {screened_out}")

        except Exception as e:
            logger.error(f"Relevancy screening failed: {e}")
            print(f"  Relevancy screening failed: {e}")

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
                        )
                    print(
                        f"  {dm.company_name}: {dm.person_name} "
                        f"({dm.title or 'N/A'})"
                    )

                # Update enrichment data regardless
                if dm and not self.dry_run and (dm.employee_count or dm.industry or dm.website):
                    self.db.update_company_enrichment(
                        company_id,
                        employee_count=dm.employee_count,
                        industry=dm.industry,
                        website=dm.website,
                    )

                # Mark attempt regardless of success/failure
                if not self.dry_run:
                    self.db.mark_dm_lookup_attempted(company_id)

            print(f"\n  Found decision makers for {dm_found}/{len(companies_needing_dm)} companies")

        except Exception as e:
            logger.error(f"Decision maker lookup failed: {e}")
            print(f"  Decision maker lookup failed: {e}")

        return dm_found

    def _filter_large_companies(self):
        """Screen out companies with employee_count > max threshold after DM lookup."""
        if self.dry_run:
            return
        cursor = self.db.conn.cursor()
        cursor.execute(
            "UPDATE companies SET screened_out = 1 "
            "WHERE employee_count IS NOT NULL AND employee_count > ? "
            "AND (screened_out IS NULL OR screened_out = 0)",
            (self.config.max_employee_count,),
        )
        filtered = cursor.rowcount
        self.db.conn.commit()
        if filtered:
            print(f"\n--- Filtered out {filtered} companies with >{self.config.max_employee_count} employees ---")

    async def _generate_insights_and_priorities(self):
        """Generate insights and classify priority tiers in a single Claude call."""
        if not self.config.enable_insight_generation and not self.config.enable_priority_classification:
            return
        if not self.config.anthropic_api_key:
            print("\n--- Skipping insights+priority (ANTHROPIC_API_KEY not set) ---")
            return

        # Collect companies needing either insight or priority
        companies_needing_insight = set()
        companies_needing_priority = set()

        if self.config.enable_insight_generation:
            for c in self.db.get_companies_needing_insights(
                max_employee_count=self.config.max_employee_count
            ):
                companies_needing_insight.add(c["id"])

        if self.config.enable_priority_classification:
            for c in self.db.get_companies_needing_priority_classification(
                max_employee_count=self.config.max_employee_count
            ):
                companies_needing_priority.add(c["id"])

        all_ids = companies_needing_insight | companies_needing_priority
        if not all_ids:
            print("\n--- All companies already have insights and priority tiers ---")
            return

        print(f"\n--- Generating insights + priorities for {len(all_ids)} companies ---")

        try:
            # Build input list
            generator_input = []
            for company_id in all_ids:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    "SELECT c.id, c.name, c.domain, c.industry, c.employee_count, "
                    "CASE WHEN dm.id IS NOT NULL THEN 1 ELSE 0 END as has_decision_maker "
                    "FROM companies c LEFT JOIN decision_makers dm ON dm.company_id = c.id "
                    "WHERE c.id = ?",
                    (company_id,),
                )
                row = cursor.fetchone()
                if not row:
                    continue

                jobs = self.db.get_jobs_for_company(row["id"])
                role_titles = [j["title"] for j in jobs]
                generator_input.append({
                    "company_name": row["name"],
                    "domain": row["domain"] or "",
                    "industry": row["industry"] or "",
                    "employee_count": row["employee_count"],
                    "roles": role_titles,
                    "has_decision_maker": bool(row["has_decision_maker"]),
                    "_id": row["id"],
                })

            generator = InsightAndPriorityGenerator(
                api_key=self.config.anthropic_api_key,
                model=self.config.anthropic_model,
                batch_size=10,
            )
            results = await generator.generate(generator_input)

            if not self.dry_run:
                for item in generator_input:
                    result = results.get(item["company_name"])
                    if result:
                        if item["_id"] in companies_needing_insight and result.get("insight"):
                            self.db.update_company_insight(item["_id"], result["insight"])
                        if item["_id"] in companies_needing_priority and result.get("priority_tier"):
                            self.db.update_company_priority_tier(item["_id"], result["priority_tier"])

            print(f"  Generated insights + priorities for {len(results)} companies")
        except Exception as e:
            logger.error(f"Insight+priority generation failed: {e}")
            print(f"  Insight+priority generation failed: {e}")

    async def _generate_outreach_if_needed(self):
        """Generate personalized outreach drafts for companies missing them."""
        if not self.config.enable_outreach_generation or not self.config.anthropic_api_key:
            return

        # Clear stale outreach where scraping failed (no compliment)
        if not self.dry_run:
            cursor = self.db.conn.cursor()
            cursor.execute("""
                UPDATE companies SET outreach_draft = NULL, compliment = NULL,
                       website_summary = NULL, role_classification = NULL
                WHERE outreach_draft IS NOT NULL AND outreach_draft != ''
                  AND (compliment IS NULL OR compliment = '' OR compliment = 'none')
            """)
            cleared = cursor.rowcount
            self.db.conn.commit()
            if cleared:
                print(f"\n--- Cleared {cleared} stale outreach drafts (missing compliments) ---")

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
                    "domain": comp.get("website") or comp.get("domain") or "",
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
            print(f"  Outreach generation failed: {e}")

    def _assign_p5_fallback_outreach(self):
        """Assign generic outreach drafts to P5 companies without using LLM."""
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT c.id, c.name, c.domain FROM companies c
            WHERE c.priority_tier = 'P5'
            AND (c.outreach_draft IS NULL OR c.outreach_draft = '')
            AND EXISTS (SELECT 1 FROM jobs j WHERE j.company_id = c.id AND j.is_active = 1)
        """)
        p5_companies = cursor.fetchall()
        if not p5_companies:
            return

        count = 0
        for row in p5_companies:
            company_id, name = row["id"], row["name"]
            jobs = self.db.get_jobs_for_company(company_id)
            role_title = jobs[0]["title"] if jobs else "IT role"
            role_class = _classify_role(role_title)
            clean_role = _clean_role_title(role_title)
            a_role = _a_or_an(clean_role)
            closing = _pick_closing(
                _MSP_CLOSINGS if role_class == "msp_replaceable" else _NON_MSP_CLOSINGS,
                name,
            )
            draft = _strip_em_dashes(f"Noticed you're looking for {a_role}. {closing}")
            if not self.dry_run:
                self.db.update_company_outreach(company_id, "", "", draft, role_class)
            count += 1

        if count:
            print(f"  Assigned {count} generic outreach drafts to P5 companies")

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
