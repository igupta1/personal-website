"""robots.txt compliance checker."""

import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from typing import Dict, Optional
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)


class RobotsChecker:
    """
    Check robots.txt compliance before scraping.

    Uses Python's built-in robotparser with caching.
    """

    def __init__(self, http_client: httpx.AsyncClient, cache_ttl_hours: int = 24):
        """
        Initialize robots checker.

        Args:
            http_client: Async HTTP client for fetching robots.txt
            cache_ttl_hours: How long to cache robots.txt files
        """
        self.client = http_client
        self.cache_ttl = timedelta(hours=cache_ttl_hours)
        self._cache: Dict[str, dict] = {}
        self.user_agent = "MarketingJobDiscovery"

    async def can_fetch(self, url: str) -> bool:
        """
        Check if we can fetch the given URL per robots.txt.

        Args:
            url: URL to check

        Returns:
            True if allowed, False if disallowed
        """
        parsed = urlparse(url)
        domain = parsed.netloc

        # Get or fetch robots.txt
        parser = await self._get_robots_parser(domain, parsed.scheme)

        if parser is None:
            # If we couldn't fetch robots.txt, assume allowed
            return True

        return parser.can_fetch(self.user_agent, url)

    def get_crawl_delay(self, domain: str) -> Optional[float]:
        """
        Get crawl delay for a domain if specified.

        Args:
            domain: Domain to check

        Returns:
            Crawl delay in seconds, or None if not specified
        """
        cache_entry = self._cache.get(domain)
        if cache_entry and cache_entry.get("parser"):
            delay = cache_entry["parser"].crawl_delay(self.user_agent)
            return delay
        return None

    async def _get_robots_parser(
        self, domain: str, scheme: str = "https"
    ) -> Optional[RobotFileParser]:
        """Get or fetch robots.txt parser for a domain."""
        # Check cache
        cache_entry = self._cache.get(domain)
        if cache_entry:
            if datetime.now() < cache_entry["expires_at"]:
                return cache_entry["parser"]

        # Fetch robots.txt
        robots_url = f"{scheme}://{domain}/robots.txt"

        try:
            response = await self.client.get(robots_url, timeout=10.0)

            if response.status_code == 200:
                parser = RobotFileParser()
                parser.parse(response.text.split("\n"))

                # Cache it
                self._cache[domain] = {
                    "parser": parser,
                    "expires_at": datetime.now() + self.cache_ttl,
                }

                return parser
            else:
                # robots.txt not found or error - allow all
                logger.debug(f"robots.txt not found for {domain}: {response.status_code}")
                self._cache[domain] = {
                    "parser": None,
                    "expires_at": datetime.now() + self.cache_ttl,
                }
                return None

        except httpx.HTTPError as e:
            logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
            # Cache the failure
            self._cache[domain] = {
                "parser": None,
                "expires_at": datetime.now() + timedelta(hours=1),  # Shorter TTL for failures
            }
            return None

    def clear_cache(self):
        """Clear the robots.txt cache."""
        self._cache.clear()
