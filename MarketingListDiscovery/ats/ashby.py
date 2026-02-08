"""Ashby ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class AshbyClient(BaseATSClient):
    """
    Ashby Public Job Board API client.

    API: GET https://api.ashbyhq.com/posting-api/job-board/{company}

    Returns job info including departments and locations.
    No authentication required.
    """

    def get_api_endpoint(self) -> str:
        return f"https://api.ashbyhq.com/posting-api/job-board/{self.board_token}"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from Ashby API."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)

            if response.status_code == 404:
                return []

            response.raise_for_status()
            data = response.json()

            jobs = []
            for job_data in data.get("jobs", []):
                job = JobPosting(
                    external_id=job_data.get("id", ""),
                    title=job_data.get("title", ""),
                    department=job_data.get("departmentName"),
                    location=job_data.get("locationName"),
                    description=job_data.get("descriptionHtml", "") or job_data.get("descriptionPlain", ""),
                    job_url=job_data.get("jobUrl", ""),
                    posting_date=self.parse_iso_date(job_data.get("publishedDate")),
                    raw_data=job_data,
                )
                jobs.append(job)

            return jobs

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise
