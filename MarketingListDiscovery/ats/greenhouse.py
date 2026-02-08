"""Greenhouse ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class GreenhouseClient(BaseATSClient):
    """
    Greenhouse Public Job Board API client.

    API: GET https://api.greenhouse.io/v1/boards/{board_token}/jobs?content=true

    No authentication required for public job boards.
    Rate limit: ~20 requests/second (be conservative)
    """

    def get_api_endpoint(self) -> str:
        return f"https://api.greenhouse.io/v1/boards/{self.board_token}/jobs"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from Greenhouse API."""
        endpoint = self.get_api_endpoint()
        params = {"content": "true"}  # Include job description

        try:
            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            jobs = []
            for job_data in data.get("jobs", []):
                job = JobPosting(
                    external_id=str(job_data.get("id")),
                    title=job_data.get("title", ""),
                    department=self._extract_department(job_data),
                    location=self._extract_location(job_data),
                    description=job_data.get("content", ""),
                    job_url=job_data.get("absolute_url", ""),
                    posting_date=self.parse_iso_date(job_data.get("updated_at")),
                    raw_data=job_data,
                )
                jobs.append(job)

            return jobs

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []  # Board not found
            raise

    def _extract_department(self, job_data: dict) -> Optional[str]:
        """Extract department name from job data."""
        departments = job_data.get("departments", [])
        if departments:
            return departments[0].get("name")
        return None

    def _extract_location(self, job_data: dict) -> Optional[str]:
        """Extract location from job data."""
        location = job_data.get("location", {})
        return location.get("name")
