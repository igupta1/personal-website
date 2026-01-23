"""Workable ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class WorkableClient(BaseATSClient):
    """
    Workable Widget API client.

    API: GET https://apply.workable.com/api/v1/widget/accounts/{company}

    Returns jobs in widget-friendly format.
    May require company subdomain discovery.
    """

    def get_api_endpoint(self) -> str:
        return f"https://apply.workable.com/api/v1/widget/accounts/{self.board_token}"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from Workable Widget API."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)

            if response.status_code == 404:
                return []

            response.raise_for_status()
            data = response.json()

            jobs = []
            for job_data in data.get("jobs", []):
                shortcode = job_data.get("shortcode", "")
                job = JobPosting(
                    external_id=shortcode,
                    title=job_data.get("title", ""),
                    department=job_data.get("department"),
                    location=self._format_location(job_data),
                    description=job_data.get("description", ""),
                    job_url=f"https://apply.workable.com/{self.board_token}/j/{shortcode}/",
                    posting_date=self.parse_date_ymd(job_data.get("published_on")),
                    raw_data=job_data,
                )
                jobs.append(job)

            return jobs

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise

    def _format_location(self, job_data: dict) -> str:
        """Format location from job data."""
        parts = [
            job_data.get("city"),
            job_data.get("state"),
            job_data.get("country"),
        ]
        return ", ".join(filter(None, parts))
