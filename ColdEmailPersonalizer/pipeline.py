"""Pipeline orchestrator: CSV in -> scrape -> personalize -> validate -> CSV out."""

import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

from .config import Config
from .scraper import normalize_domain, scrape_all_websites
from .personalizer import Personalizer
from .validator import validate, sanitize

logger = logging.getLogger(__name__)


class Pipeline:
    """End-to-end cold email personalization pipeline."""

    def __init__(self, config: Config):
        self.config = config
        self.checkpoint_path = config.input_csv.parent / f"{config.input_csv.stem}_checkpoint.json"
        self.checkpoint_data: Dict[str, Any] = {
            "scrape_results": {},
            "personalization_results": {},
        }
        self._setup_error_log()

    def _setup_error_log(self) -> None:
        """Set up a dedicated error log file for scrape/LLM failures."""
        self.error_log_path = self.config.input_csv.parent / f"{self.config.input_csv.stem}_errors.log"
        self.error_logger = logging.getLogger("ColdEmailPersonalizer.errors")
        self.error_logger.setLevel(logging.INFO)
        # Remove existing handlers to avoid duplicates on re-run
        self.error_logger.handlers.clear()
        fh = logging.FileHandler(self.error_log_path, mode="w")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
        self.error_logger.addHandler(fh)

    def _load_checkpoint(self) -> None:
        """Load checkpoint from disk if it exists."""
        if self.config.resume and self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, "r") as f:
                    self.checkpoint_data = json.load(f)
                scrape_count = len(self.checkpoint_data.get("scrape_results", {}))
                pers_count = len(self.checkpoint_data.get("personalization_results", {}))
                print(f"  Resumed checkpoint: {scrape_count} scrapes, {pers_count} personalizations")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
                self.checkpoint_data = {"scrape_results": {}, "personalization_results": {}}

    def _save_checkpoint(self) -> None:
        """Save checkpoint to disk."""
        try:
            with open(self.checkpoint_path, "w") as f:
                json.dump(self.checkpoint_data, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    async def run(self) -> Dict[str, int]:
        """Run the full pipeline.

        Returns:
            Stats dict with keys: total, ok, scrape_failed, llm_failed, validation_failed
        """
        stats = {"total": 0, "ok": 0, "scrape_failed": 0, "llm_failed": 0, "validation_failed": 0, "needs_manual_review": 0}

        # --- Read CSV ---
        print(f"Reading {self.config.input_csv}...")
        df = pd.read_csv(self.config.input_csv, encoding="utf-8-sig")

        required_cols = ["First Name", "Company Name", "Website"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if self.config.limit > 0:
            df = df.head(self.config.limit)

        stats["total"] = len(df)
        print(f"  {len(df)} rows to process")

        # --- Load checkpoint ---
        self._load_checkpoint()

        # --- Build domain map and deduplicate ---
        websites = []
        for _, row in df.iterrows():
            url = str(row.get("Website", "")).strip()
            if url and url.lower() not in ("nan", ""):
                websites.append(url)

        unique_domains = set()
        for url in websites:
            d = normalize_domain(url)
            if d:
                unique_domains.add(d)

        # Remove already-scraped domains from the to-scrape list
        cached_domains = set(self.checkpoint_data.get("scrape_results", {}).keys())
        domains_to_scrape = unique_domains - cached_domains
        print(f"  {len(unique_domains)} unique domains ({len(cached_domains)} cached, {len(domains_to_scrape)} to scrape)")

        # --- Phase 1: Scrape ---
        if domains_to_scrape:
            print(f"\nPhase 1: Scraping {len(domains_to_scrape)} websites...")
            # Build URL list from domains that need scraping
            domain_urls = []
            for url in websites:
                d = normalize_domain(url)
                if d in domains_to_scrape:
                    domain_urls.append(url)
                    domains_to_scrape.discard(d)  # Only add one URL per domain

            new_scrapes = await scrape_all_websites(
                domain_urls,
                concurrency=self.config.scrape_concurrency,
                timeout=self.config.scrape_timeout,
                subpage_delay=self.config.subpage_delay,
                max_subpages=self.config.max_subpages,
                homepage_char_limit=self.config.homepage_char_limit,
                subpage_char_limit=self.config.subpage_char_limit,
            )

            # Merge into checkpoint and log failures
            for domain, result in new_scrapes.items():
                self.checkpoint_data["scrape_results"][domain] = result
                if result.get("error"):
                    self.error_logger.info(f"SCRAPE_FAIL domain={domain} error={result['error']}")
            self._save_checkpoint()

            scrape_ok = sum(1 for r in new_scrapes.values() if not r.get("error"))
            scrape_fail = len(new_scrapes) - scrape_ok
            print(f"  Scraping done: {scrape_ok} succeeded, {scrape_fail} failed")
        else:
            print("\nPhase 1: All websites already scraped (using checkpoint)")

        scrape_cache = self.checkpoint_data["scrape_results"]

        # --- Phase 2: Personalize ---
        # Build prospect list, skipping already-personalized rows
        cached_personalizations = self.checkpoint_data.get("personalization_results", {})
        prospects_to_process = []
        for idx, row in df.iterrows():
            row_key = str(idx)
            if row_key in cached_personalizations:
                continue

            first_name = str(row.get("First Name", "")).strip()
            company_name = str(row.get("Company Name", "")).strip()
            website = str(row.get("Website", "")).strip()

            if not first_name or not company_name:
                cached_personalizations[row_key] = {
                    "subject": None, "opener": None, "error": "missing_name_or_company"
                }
                continue

            domain = normalize_domain(website) if website and website.lower() != "nan" else ""
            scrape_result = scrape_cache.get(domain, {})

            if scrape_result.get("error") and not scrape_result.get("homepage_text"):
                cached_personalizations[row_key] = {
                    "subject": None, "opener": None, "error": "scrape_failed"
                }
                stats["scrape_failed"] += 1
                continue

            prospects_to_process.append({
                "row_index": idx,
                "first_name": first_name,
                "company_name": company_name,
                "website": website,
            })

        already_done = len(cached_personalizations) - len([
            v for v in cached_personalizations.values()
            if v.get("error") in ("missing_name_or_company", "scrape_failed")
        ])

        print(f"\nPhase 2: Personalizing {len(prospects_to_process)} prospects ({already_done} cached)...")

        if prospects_to_process:
            personalizer = Personalizer(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
            )

            def on_complete(row_idx, result):
                cached_personalizations[str(row_idx)] = result
                self._save_checkpoint()
                if result.get("error"):
                    # Find the prospect info for this row
                    prospect = next((p for p in prospects_to_process if p["row_index"] == row_idx), {})
                    self.error_logger.info(
                        f"LLM_FAIL row={row_idx} company={prospect.get('company_name', '?')} "
                        f"error={result['error']}"
                    )

            new_results = await personalizer.personalize_all(
                prospects_to_process,
                scrape_cache,
                concurrency=self.config.llm_concurrency,
                on_complete=on_complete,
            )

            # Results are already saved via on_complete callback
            self.checkpoint_data["personalization_results"] = cached_personalizations
            self._save_checkpoint()

        # --- Phase 2.5: Same-domain subject dedup ---
        all_results = self.checkpoint_data["personalization_results"]

        # Group results by domain to find duplicate subjects
        domain_rows: Dict[str, List[tuple]] = {}  # domain -> [(row_idx, subject)]
        for idx, row in df.iterrows():
            row_key = str(idx)
            result = all_results.get(row_key, {})
            subject = result.get("subject", "")
            if not subject or result.get("error"):
                continue
            website = str(row.get("Website", "")).strip()
            domain = normalize_domain(website) if website and website.lower() != "nan" else ""
            if not domain:
                continue
            if domain not in domain_rows:
                domain_rows[domain] = []
            domain_rows[domain].append((idx, subject))

        # Find duplicates within same domain
        dedup_retry_prospects = []
        for domain, entries in domain_rows.items():
            if len(entries) <= 1:
                continue
            subject_groups: Dict[str, List[int]] = {}
            for row_idx, subject in entries:
                normalized_subj = subject.lower().strip()
                if normalized_subj not in subject_groups:
                    subject_groups[normalized_subj] = []
                subject_groups[normalized_subj].append(row_idx)
            for subject, row_indices in subject_groups.items():
                if len(row_indices) <= 1:
                    continue
                # Keep the first, retry the rest with avoid_subject
                for row_idx in row_indices[1:]:
                    row = df.loc[row_idx]
                    dedup_retry_prospects.append({
                        "row_index": row_idx,
                        "first_name": str(row.get("First Name", "")).strip(),
                        "company_name": str(row.get("Company Name", "")).strip(),
                        "website": str(row.get("Website", "")).strip(),
                        "avoid_subject": subject,
                    })

        if dedup_retry_prospects:
            print(f"\nPhase 2.5: Deduplicating {len(dedup_retry_prospects)} same-domain duplicate subjects...")
            personalizer = Personalizer(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
            )
            dedup_results = await personalizer.personalize_all(
                dedup_retry_prospects,
                scrape_cache,
                concurrency=self.config.llm_concurrency,
            )
            for row_idx, result in dedup_results.items():
                row_key = str(row_idx)
                if result.get("error") or not result.get("subject") or not result.get("opener"):
                    # Dedup retry failed — mark as needs_manual_review
                    all_results[row_key]["error"] = "needs_manual_review"
                    self.error_logger.info(
                        f"DEDUP_FAIL row={row_idx} company={df.loc[row_idx].get('Company Name', '?')} "
                        f"error=dedup_retry_failed"
                    )
                else:
                    subject = result.get("subject", "")
                    opener = result.get("opener", "")
                    subject, opener = sanitize(subject, opener)
                    row = df.loc[row_idx]
                    first_name = str(row.get("First Name", "")).strip()
                    last_name = str(row.get("Last Name", "")).strip()
                    is_valid, issues = validate(subject, opener, first_name, last_name)
                    if is_valid:
                        result["subject"] = subject
                        result["opener"] = opener
                        all_results[row_key] = result
                    else:
                        # Dedup retry produced invalid output — mark as needs_manual_review
                        all_results[row_key]["error"] = "needs_manual_review"
                        self.error_logger.info(
                            f"DEDUP_VALIDATION_FAIL row={row_idx} company={df.loc[row_idx].get('Company Name', '?')} "
                            f"subject=\"{subject}\" opener=\"{opener}\" issues={issues}"
                        )
            self._save_checkpoint()

        # --- Phase 3: Validate + Retry (up to max_retries rounds) ---
        print(f"\nPhase 3: Validating results...")

        # Initial validation pass
        failed_row_indices = set()
        for idx, row in df.iterrows():
            row_key = str(idx)
            result = all_results.get(row_key, {})

            if result.get("error"):
                continue

            subject = result.get("subject", "")
            opener = result.get("opener", "")

            if not subject or not opener:
                continue

            # Sanitize first
            subject, opener = sanitize(subject, opener)
            result["subject"] = subject
            result["opener"] = opener

            first_name = str(row.get("First Name", "")).strip()
            last_name = str(row.get("Last Name", "")).strip()
            is_valid, issues = validate(subject, opener, first_name, last_name)

            if not is_valid:
                rejects = [i for i in issues if i.startswith("REJECT:")]
                logger.info(f"Row {idx} ({row.get('Company Name', '')}): {rejects}")
                self.error_logger.info(
                    f"VALIDATION_FAIL row={idx} company={row.get('Company Name', '')} "
                    f"subject=\"{subject}\" opener=\"{opener}\" issues={rejects}"
                )
                result["validation_issues"] = issues
                failed_row_indices.add(idx)

        # Retry loop (up to max_retries rounds)
        for retry_round in range(self.config.max_retries):
            if not failed_row_indices:
                break

            retry_prospects = []
            for idx in failed_row_indices:
                row = df.loc[idx]
                retry_prospects.append({
                    "row_index": idx,
                    "first_name": str(row.get("First Name", "")).strip(),
                    "company_name": str(row.get("Company Name", "")).strip(),
                    "website": str(row.get("Website", "")).strip(),
                })

            print(f"  Retry {retry_round + 1}/{self.config.max_retries}: {len(retry_prospects)} failed validations...")
            personalizer = Personalizer(
                api_key=self.config.gemini_api_key,
                model=self.config.gemini_model,
            )

            retry_results = await personalizer.personalize_all(
                retry_prospects,
                scrape_cache,
                concurrency=self.config.llm_concurrency,
            )

            still_failed = set()
            for row_idx, result in retry_results.items():
                if result.get("error"):
                    still_failed.add(row_idx)
                    continue
                subject = result.get("subject", "")
                opener = result.get("opener", "")
                if subject and opener:
                    subject, opener = sanitize(subject, opener)
                    result["subject"] = subject
                    result["opener"] = opener
                    row = df.loc[row_idx]
                    first_name = str(row.get("First Name", "")).strip()
                    last_name = str(row.get("Last Name", "")).strip()
                    is_valid, issues = validate(subject, opener, first_name, last_name)
                    if is_valid:
                        all_results[str(row_idx)] = result
                    else:
                        still_failed.add(row_idx)
                else:
                    still_failed.add(row_idx)

            failed_row_indices = still_failed

        # Mark remaining failures as needs_manual_review
        for idx in failed_row_indices:
            row_key = str(idx)
            all_results[row_key]["error"] = "needs_manual_review"
            self.error_logger.info(
                f"MANUAL_REVIEW row={idx} company={df.loc[idx].get('Company Name', '?')} "
                f"reason=failed_all_{self.config.max_retries}_retries"
            )

        self._save_checkpoint()

        # --- Phase 4: Write output CSV ---
        print(f"\nPhase 4: Writing output CSV...")
        ai_subjects = []
        ai_openers = []
        ai_statuses = []

        for idx, row in df.iterrows():
            row_key = str(idx)
            result = all_results.get(row_key, {})

            subject = result.get("subject") or ""
            opener = result.get("opener") or ""
            error = result.get("error")

            if error:
                if str(error) == "needs_manual_review":
                    status = "needs_manual_review"
                    stats["needs_manual_review"] += 1
                elif "scrape" in str(error) or "insufficient" in str(error):
                    status = "scrape_failed"
                    stats["scrape_failed"] += 1
                else:
                    status = "llm_failed"
                    stats["llm_failed"] += 1
            elif not subject or not opener:
                status = "llm_failed"
                stats["llm_failed"] += 1
            else:
                first_name = str(row.get("First Name", "")).strip()
                last_name = str(row.get("Last Name", "")).strip()
                is_valid, issues = validate(subject, opener, first_name, last_name)
                if is_valid:
                    status = "ok"
                    stats["ok"] += 1
                else:
                    status = "validation_failed"
                    stats["validation_failed"] += 1

            ai_subjects.append(subject)
            ai_openers.append(opener)
            ai_statuses.append(status)

        df["ai_subject"] = ai_subjects
        df["ai_opener"] = ai_openers
        df["ai_status"] = ai_statuses

        df.to_csv(self.config.output_csv, index=False, encoding="utf-8-sig")

        # --- Summary ---
        print(f"\nDone! Output: {self.config.output_csv}")
        print(f"  Total:               {stats['total']}")
        print(f"  OK:                  {stats['ok']}")
        print(f"  Scrape failed:       {stats['scrape_failed']}")
        print(f"  LLM failed:          {stats['llm_failed']}")
        print(f"  Validation failed:   {stats['validation_failed']}")
        print(f"  Needs manual review: {stats['needs_manual_review']}")
        print(f"  Error log:           {self.error_log_path}")

        return stats


async def run_test(config: Config, rows: int = 5) -> None:
    """Run the pipeline on a small number of rows and print results to stdout."""
    config.limit = rows
    pipeline = Pipeline(config)
    # Don't use checkpoint for test runs
    pipeline.checkpoint_path = Path("/dev/null")

    print(f"=== TEST MODE: Processing {rows} rows ===\n")

    # Read CSV
    df = pd.read_csv(config.input_csv, encoding="utf-8-sig").head(rows)

    # Collect websites
    websites = []
    for _, row in df.iterrows():
        url = str(row.get("Website", "")).strip()
        if url and url.lower() != "nan":
            websites.append(url)

    # Scrape
    print("Scraping...")
    from .scraper import scrape_all_websites, build_content_summary
    scrape_cache = await scrape_all_websites(
        websites,
        concurrency=config.scrape_concurrency,
        timeout=config.scrape_timeout,
        subpage_delay=config.subpage_delay,
        max_subpages=config.max_subpages,
        homepage_char_limit=config.homepage_char_limit,
        subpage_char_limit=config.subpage_char_limit,
    )

    # Personalize
    print("Personalizing...\n")
    personalizer = Personalizer(
        api_key=config.gemini_api_key,
        model=config.gemini_model,
    )

    for idx, row in df.iterrows():
        first_name = str(row.get("First Name", "")).strip()
        last_name = str(row.get("Last Name", "")).strip()
        company_name = str(row.get("Company Name", "")).strip()
        website = str(row.get("Website", "")).strip()

        domain = normalize_domain(website) if website else ""
        scrape_result = scrape_cache.get(domain, {})
        content = build_content_summary(scrape_result)

        result = await personalizer.personalize_one(first_name, company_name, content)

        subject = result.get("subject", "")
        opener = result.get("opener", "")

        if subject and opener:
            subject, opener = sanitize(subject, opener)
            is_valid, issues = validate(subject, opener, first_name, last_name)
        else:
            is_valid = False
            issues = [result.get("error", "no output")]

        print(f"--- Row {idx + 1}: {first_name} at {company_name} ---")
        print(f"  Website:  {website}")
        print(f"  Scraped:  {'Yes' if scrape_result.get('homepage_text') else 'No'} ({len(content)} chars)")
        print(f"  Subject:  {subject}")
        print(f"  Opener:   {opener}")
        print(f"  Valid:    {is_valid}")
        if issues:
            print(f"  Issues:   {issues}")
        print()
