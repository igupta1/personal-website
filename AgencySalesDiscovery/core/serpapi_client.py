"""SerpAPI Google Jobs client for agency sales role discovery."""

import json
import logging
import re
from datetime import date, timedelta
from pathlib import Path
from typing import List, Set, Optional

from serpapi import GoogleSearch

from .models import SerpJobListing

logger = logging.getLogger(__name__)


class SerpAPIJobClient:
    """
    Search for sales/marketing job postings at marketing agencies using SerpAPI's Google Jobs API.

    Uses a single combined query across rotating metro areas,
    deduplicates results by (company_name_normalized, title_normalized).
    """

    def __init__(self, api_key: str, max_searches: int = 2):
        self.api_key = api_key
        self.max_searches = max_searches
        self.searches_used = 0

    @staticmethod
    def get_next_metros(
        all_metros: List[str], count: int, state_path: Path
    ) -> List[str]:
        """
        Pick the next `count` metros from the rotation and advance the index.

        State is stored in a JSON file: {"next_index": N}
        Wraps around when reaching the end of the list.
        """
        next_index = 0
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text())
                next_index = state.get("next_index", 0)
            except (json.JSONDecodeError, OSError):
                next_index = 0

        selected = []
        total = len(all_metros)
        for i in range(count):
            selected.append(all_metros[(next_index + i) % total])

        new_index = (next_index + count) % total
        state_path.write_text(json.dumps({"next_index": new_index}))

        return selected

    def search_all(
        self,
        query: str,
        metro_areas: List[str],
    ) -> List[SerpJobListing]:
        """
        Search the query across the given metro areas, deduplicate results.

        With daily rotation: 1 query x 2 metros = 2 searches per run.
        """
        seen: Set[str] = set()
        all_listings: List[SerpJobListing] = []

        for metro in metro_areas:
            if self.searches_used >= self.max_searches:
                logger.warning(
                    f"Search budget exhausted ({self.max_searches}). Stopping."
                )
                return all_listings

            listings = self._search_one(query, metro)
            self.searches_used += 1

            for listing in listings:
                dedup_key = self._dedup_key(listing.company_name, listing.title)
                if dedup_key not in seen:
                    seen.add(dedup_key)
                    listing.search_metro = metro
                    all_listings.append(listing)

            logger.info(
                f"[{self.searches_used}] {metro}: {len(listings)} results, "
                f"{len(all_listings)} unique total"
            )

        return all_listings

    def _search_one(self, query: str, location: str) -> List[SerpJobListing]:
        """Execute a single SerpAPI Google Jobs search."""
        params = {
            "engine": "google_jobs",
            "q": query,
            "location": location,
            "chips": "date_posted:week",
            "api_key": self.api_key,
        }

        try:
            search = GoogleSearch(params)
            results = search.get_dict()

            if "error" in results:
                logger.error(f"SerpAPI error for {location}: {results['error']}")
                return []

            jobs = results.get("jobs_results", [])
            return [self._parse_listing(job) for job in jobs]
        except Exception as e:
            logger.error(f"SerpAPI search failed for {location}: {e}")
            return []

    def _parse_listing(self, job: dict) -> SerpJobListing:
        """Parse a single SerpAPI job result into a SerpJobListing."""
        apply_url = None
        source = None
        apply_options = job.get("apply_options", [])
        if apply_options:
            apply_url = apply_options[0].get("link")
            source = apply_options[0].get("title")

        posted_at = ""
        extensions = job.get("detected_extensions", {})
        if isinstance(extensions, dict):
            posted_at = extensions.get("posted_at", "")

        return SerpJobListing(
            title=job.get("title", ""),
            company_name=job.get("company_name", ""),
            location=job.get("location", ""),
            posted_at=posted_at,
            posting_date=self._parse_posted_at(posted_at),
            job_url=apply_url,
            source=source,
            description_snippet=(job.get("description", "") or "")[:500],
            raw_data=job,
        )

    @staticmethod
    def _parse_posted_at(posted_at: str) -> Optional[date]:
        """Convert '2 days ago', 'today', '1 week ago' to a date object."""
        if not posted_at:
            return None
        text = posted_at.lower().strip()
        today = date.today()

        if "today" in text or "just" in text or "hour" in text:
            return today
        if "yesterday" in text:
            return today - timedelta(days=1)

        match = re.search(r"(\d+)\s*day", text)
        if match:
            return today - timedelta(days=int(match.group(1)))

        match = re.search(r"(\d+)\s*week", text)
        if match:
            return today - timedelta(weeks=int(match.group(1)))

        return None

    @staticmethod
    def _dedup_key(company_name: str, title: str) -> str:
        """Create a normalized dedup key from company + title."""
        cn = company_name.lower().strip()
        t = title.lower().strip()
        return f"{cn}|||{t}"
