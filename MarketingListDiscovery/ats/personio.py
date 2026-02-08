"""Personio ATS API client."""

from typing import List, Optional
import httpx
from bs4 import BeautifulSoup

from .base import BaseATSClient
from ..core.models import JobPosting


class PersonioClient(BaseATSClient):
    """
    Personio Public Jobs client.

    Note: Personio doesn't have a public JSON API.
    We fetch the HTML careers page and parse it.

    URL: https://{company_slug}.jobs.personio.de/
    """

    def get_api_endpoint(self) -> str:
        return f"https://{self.board_token}.jobs.personio.de/"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Fetch jobs from Personio careers page (HTML scraping)."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)
            response.raise_for_status()

            # Personio uses HTML, not JSON
            # We'll do basic HTML parsing to extract job links
            jobs = []
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find job listings (this is a simplified approach)
            # Personio's structure may vary, so this is a best-effort implementation
            job_links = soup.find_all('a', class_='position-link') or soup.find_all('a', href=lambda h: h and '/job/' in h)

            for link in job_links:
                title = link.get_text(strip=True)
                job_url = link.get('href', '')

                # Make URL absolute if relative
                if job_url and not job_url.startswith('http'):
                    job_url = f"{endpoint.rstrip('/')}/{job_url.lstrip('/')}"

                # Extract ID from URL (e.g., /job/123456)
                external_id = job_url.split('/')[-1] if job_url else ""

                job = JobPosting(
                    external_id=external_id,
                    title=title,
                    department=None,  # Not easily extractable from listing page
                    location=None,  # Would need to fetch individual job pages
                    description="",  # Would need to fetch individual job pages
                    job_url=job_url,
                    posting_date=None,
                    raw_data={"title": title, "url": job_url},
                )
                jobs.append(job)

            return jobs

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []  # Company not found
            raise
        except Exception:
            # If HTML parsing fails, return empty list rather than crashing
            return []
