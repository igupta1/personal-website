"""Lever ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class LeverClient(BaseATSClient):
    """
    Lever Public Postings API client.

    API: GET https://api.lever.co/v0/postings/{company}

    Returns all active postings. No auth required.
    Pagination via 'offset' parameter if >50 jobs.
    """

    def get_api_endpoint(self) -> str:
        return f"https://api.lever.co/v0/postings/{self.board_token}"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from Lever API with pagination."""
        all_jobs = []
        offset = 0
        limit = 50

        while True:
            endpoint = self.get_api_endpoint()
            params = {"mode": "json", "limit": limit, "offset": offset}

            try:
                response = await self.client.get(endpoint, params=params)

                if response.status_code == 404:
                    return []

                response.raise_for_status()
                postings = response.json()

                if not postings:
                    break

                for posting in postings:
                    categories = posting.get("categories", {})
                    job = JobPosting(
                        external_id=posting.get("id", ""),
                        title=posting.get("text", ""),
                        department=categories.get("department") or categories.get("team"),
                        location=categories.get("location"),
                        description=posting.get("descriptionPlain", ""),
                        job_url=posting.get("hostedUrl", ""),
                        posting_date=self.parse_timestamp_ms(posting.get("createdAt")),
                        raw_data=posting,
                    )
                    all_jobs.append(job)

                if len(postings) < limit:
                    break

                offset += limit

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return []
                raise

        return all_jobs
