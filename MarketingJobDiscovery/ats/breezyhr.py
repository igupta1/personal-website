"""BreezyHR ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class BreezyHRClient(BaseATSClient):
    """
    BreezyHR Public Jobs API client.

    API: GET https://{company_slug}.breezy.hr/json

    No authentication required for public job listings.
    """

    def get_api_endpoint(self) -> str:
        return f"https://{self.board_token}.breezy.hr/json"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from BreezyHR API."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)
            response.raise_for_status()
            data = response.json()

            jobs = []
            # BreezyHR returns array of positions directly
            if isinstance(data, list):
                for job_data in data:
                    job = JobPosting(
                        external_id=str(job_data.get("id", "")),
                        title=job_data.get("name", ""),
                        department=self._extract_department(job_data),
                        location=self._extract_location(job_data),
                        description=job_data.get("description", ""),
                        job_url=job_data.get("url", ""),
                        posting_date=self.parse_iso_date(job_data.get("published_date")),
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
            return department.get("name") if isinstance(department, dict) else str(department)
        return None

    def _extract_location(self, job_data: dict) -> Optional[str]:
        """Extract location from job data."""
        location = job_data.get("location")
        if location:
            if isinstance(location, dict):
                city = location.get("city", "")
                state = location.get("state", "")
                country = location.get("country", "")
                parts = [p for p in [city, state, country] if p]
                return ", ".join(parts) if parts else None
            return str(location)
        return None
