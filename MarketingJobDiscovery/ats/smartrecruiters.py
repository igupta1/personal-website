"""SmartRecruiters ATS API client."""

from typing import List, Optional
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class SmartRecruitersClient(BaseATSClient):
    """
    SmartRecruiters Public Job API client.

    API: GET https://api.smartrecruiters.com/v1/companies/{company_id}/postings

    No authentication required for public job postings.
    """

    def get_api_endpoint(self) -> str:
        return f"https://api.smartrecruiters.com/v1/companies/{self.board_token}/postings"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from SmartRecruiters API."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)
            response.raise_for_status()
            data = response.json()

            jobs = []
            for job_data in data.get("content", []):
                job = JobPosting(
                    external_id=str(job_data.get("id", "")),
                    title=job_data.get("name", ""),
                    department=self._extract_department(job_data),
                    location=self._extract_location(job_data),
                    description=self._get_job_description(job_data),
                    job_url=self._build_job_url(job_data),
                    posting_date=self.parse_iso_date(job_data.get("releasedDate")),
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
        department = job_data.get("department", {})
        return department.get("label") if department else None

    def _extract_location(self, job_data: dict) -> Optional[str]:
        """Extract location from job data."""
        location = job_data.get("location", {})
        if location:
            city = location.get("city", "")
            country = location.get("country", "")
            if city and country:
                return f"{city}, {country}"
            return city or country
        return None

    def _get_job_description(self, job_data: dict) -> str:
        """Get job description - may need separate API call for full content."""
        # SmartRecruiters listing endpoint doesn't include full description
        # Return empty for now, could be enhanced to fetch individual job details
        return ""

    def _build_job_url(self, job_data: dict) -> str:
        """Build the public job URL."""
        ref = job_data.get("ref", "")
        if ref:
            return ref
        # Fallback: construct URL from company and job ID
        job_id = job_data.get("id", "")
        return f"https://jobs.smartrecruiters.com/{self.board_token}/{job_id}"
