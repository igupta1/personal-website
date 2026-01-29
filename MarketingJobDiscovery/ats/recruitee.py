"""Recruitee ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class RecruiteeClient(BaseATSClient):
    """
    Recruitee Public Careers API client.

    API: GET https://{company_slug}.recruitee.com/api/offers/

    No authentication required for public job listings.
    """

    def get_api_endpoint(self) -> str:
        return f"https://{self.board_token}.recruitee.com/api/offers/"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from Recruitee API."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)
            response.raise_for_status()
            data = response.json()

            jobs = []
            for job_data in data.get("offers", []):
                job = JobPosting(
                    external_id=str(job_data.get("id", "")),
                    title=job_data.get("title", ""),
                    department=self._extract_department(job_data),
                    location=job_data.get("location", ""),
                    description=job_data.get("description", ""),
                    job_url=self._build_job_url(job_data),
                    posting_date=self.parse_iso_date(job_data.get("created_at")),
                    raw_data=job_data,
                )
                jobs.append(job)

            return jobs

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []  # Company not found
            raise

    def _extract_department(self, job_data: dict) -> Optional[str]:
        """Extract department name from job data."""
        department = job_data.get("department")
        if department:
            return department
        return None

    def _build_job_url(self, job_data: dict) -> str:
        """Build the public job URL."""
        careers_url = job_data.get("careers_url")
        if careers_url:
            return careers_url
        # Fallback: construct URL from company and job ID
        job_id = job_data.get("id", "")
        return f"https://{self.board_token}.recruitee.com/o/{job_id}"
