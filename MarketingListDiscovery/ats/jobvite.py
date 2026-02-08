"""Jobvite ATS XML feed client."""

import xml.etree.ElementTree as ET
from typing import List, Optional
from datetime import datetime
from email.utils import parsedate_to_datetime
import httpx

from .base import BaseATSClient
from ..core.models import JobPosting


class JobviteClient(BaseATSClient):
    """
    Jobvite XML Feed client.

    Feed: http://jobs.jobvite.com/{company}/feed.xml

    Returns RSS/XML feed of job postings.
    """

    def get_api_endpoint(self) -> str:
        return f"https://jobs.jobvite.com/{self.board_token}/feed.xml"

    async def fetch_jobs(self) -> List[JobPosting]:
        """Parse Jobvite XML feed."""
        endpoint = self.get_api_endpoint()

        try:
            response = await self.client.get(endpoint)

            if response.status_code == 404:
                return []

            response.raise_for_status()

            # Parse XML
            root = ET.fromstring(response.text)

            jobs = []
            # Jobvite uses RSS format with items in channel
            for item in root.findall(".//item"):
                job = JobPosting(
                    external_id=self._get_text(item, "jvid") or self._get_text(item, "guid", ""),
                    title=self._get_text(item, "title", ""),
                    department=self._get_text(item, "category"),
                    location=self._get_text(item, "location") or self._get_text(item, "jv:location"),
                    description=self._get_text(item, "description", ""),
                    job_url=self._get_text(item, "link", ""),
                    posting_date=self._parse_rss_date(self._get_text(item, "pubDate")),
                    raw_data={"xml": ET.tostring(item, encoding="unicode")},
                )
                jobs.append(job)

            return jobs

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise
        except ET.ParseError:
            return []

    def _get_text(
        self, element: ET.Element, tag: str, default: Optional[str] = None
    ) -> Optional[str]:
        """Get text content from child element."""
        # Try with namespace prefix
        for prefix in ["", "jv:", "{http://www.jobvite.com/}"]:
            child = element.find(f"{prefix}{tag}")
            if child is not None and child.text:
                return child.text.strip()
        return default

    def _parse_rss_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse RSS date format (RFC 822)."""
        if not date_str:
            return None
        try:
            return parsedate_to_datetime(date_str)
        except (ValueError, TypeError):
            return None
