"""Base ATS client interface."""

from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime
import httpx

from ..core.models import JobPosting


class BaseATSClient(ABC):
    """Abstract base class for ATS API clients."""

    def __init__(self, http_client: httpx.AsyncClient, board_token: str):
        """
        Initialize ATS client.

        Args:
            http_client: Async HTTP client for making requests
            board_token: Company identifier for the ATS (e.g., company slug)
        """
        self.client = http_client
        self.board_token = board_token

    @abstractmethod
    async def fetch_jobs(self) -> List[JobPosting]:
        """
        Fetch all job postings from the ATS.

        Returns:
            List of JobPosting objects
        """
        pass

    @abstractmethod
    def get_api_endpoint(self) -> str:
        """
        Return the API endpoint URL for this ATS.

        Returns:
            Full URL to the jobs API endpoint
        """
        pass

    @staticmethod
    def parse_iso_date(date_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO format date string."""
        if not date_str:
            return None
        try:
            # Handle various ISO formats
            date_str = date_str.replace("Z", "+00:00")
            return datetime.fromisoformat(date_str)
        except ValueError:
            return None

    @staticmethod
    def parse_timestamp_ms(ts: Optional[int]) -> Optional[datetime]:
        """Parse millisecond timestamp."""
        if ts:
            return datetime.fromtimestamp(ts / 1000)
        return None

    @staticmethod
    def parse_date_ymd(date_str: Optional[str]) -> Optional[datetime]:
        """Parse YYYY-MM-DD date string."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None
