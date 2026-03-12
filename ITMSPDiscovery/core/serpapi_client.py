"""SerpAPI Google Jobs client for IT role discovery."""

import json
import logging
import re
from datetime import date, timedelta
from pathlib import Path
from typing import List, Set, Optional, Tuple

from serpapi import GoogleSearch

from .models import SerpJobListing

logger = logging.getLogger(__name__)


class SerpAPIJobClient:
    """
    Search for IT job postings using SerpAPI's Google Jobs API.

    Uses a single combined query across rotating metro areas,
    deduplicates results by (company_name_normalized, title_normalized).
    """

    def __init__(self, api_key: str, max_searches: int = 3):
        self.api_key = api_key
        self.max_searches = max_searches
        self.searches_used = 0

    @staticmethod
    def get_cluster_schedule(
        all_metros: List[str],
        queries: List[str],
        rotation_patterns: List[List[int]],
        state_path: Path,
    ) -> List[Tuple[str, List[str]]]:
        """
        Build a per-cluster metro schedule and advance rotation state.

        Each cluster has its own independent metro rotation index so every
        cluster cycles through all metros over ~7 days. The "short" cluster
        (2 metros instead of 3) rotates each day via cluster_rotation.

        State file schema:
            {"cluster_metro_indices": [0, 0, 0], "cluster_rotation": 0}

        Returns:
            List of (query, [metros]) tuples — one per cluster.
        """
        num_clusters = len(queries)
        total_metros = len(all_metros)

        # Read state (handle old format gracefully)
        indices = [0] * num_clusters
        cluster_rotation = 0
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text())
                if "cluster_metro_indices" in state:
                    saved = state["cluster_metro_indices"]
                    # Ensure list is right length
                    indices = (saved + [0] * num_clusters)[:num_clusters]
                    cluster_rotation = state.get("cluster_rotation", 0)
                # Old format {"next_index": N} — start fresh
            except (json.JSONDecodeError, OSError):
                pass

        # Get today's pattern
        pattern = rotation_patterns[cluster_rotation % len(rotation_patterns)]

        # Build schedule: each cluster picks its metros independently
        schedule: List[Tuple[str, List[str]]] = []
        for i, query in enumerate(queries):
            count = pattern[i] if i < len(pattern) else 2
            metros = []
            for j in range(count):
                metros.append(all_metros[(indices[i] + j) % total_metros])
            schedule.append((query, metros))
            # Advance this cluster's index
            indices[i] = (indices[i] + count) % total_metros

        # Save state
        state_path.write_text(json.dumps({
            "cluster_metro_indices": indices,
            "cluster_rotation": cluster_rotation + 1,
        }))

        return schedule

    def search_all(
        self,
        query_metro_pairs: List[Tuple[str, List[str]]],
    ) -> List[SerpJobListing]:
        """
        Search each query cluster against its assigned metros, deduplicate results.

        3 clusters with rotating metro counts = 8 searches per run.
        """
        seen: Set[str] = set()
        all_listings: List[SerpJobListing] = []

        for query_idx, (query, metros) in enumerate(query_metro_pairs, 1):
            for metro in metros:
                if self.searches_used >= self.max_searches:
                    logger.warning(
                        f"Search budget exhausted ({self.max_searches}). Stopping."
                    )
                    return all_listings

                listings = self._search_one(query, metro)
                self.searches_used += 1

                new_in_batch = 0
                for listing in listings:
                    dedup_key = self._dedup_key(listing.company_name, listing.title)
                    if dedup_key not in seen:
                        seen.add(dedup_key)
                        listing.search_metro = metro
                        all_listings.append(listing)
                        new_in_batch += 1

                logger.info(
                    f"[{self.searches_used}] Q{query_idx} {metro}: "
                    f"{len(listings)} results, {new_in_batch} new, "
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
            # no_cache deliberately NOT set - use cached results to save credits
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
        # Extract apply link from apply_options
        apply_url = None
        source = None
        apply_options = job.get("apply_options", [])
        if apply_options:
            apply_url = apply_options[0].get("link")
            source = apply_options[0].get("title")

        # Extract posted_at from detected_extensions
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

        # "X days ago"
        match = re.search(r"(\d+)\s*day", text)
        if match:
            return today - timedelta(days=int(match.group(1)))

        # "X weeks ago"
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
