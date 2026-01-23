"""ATS detection from company careers pages."""

import re
import logging
from typing import Optional, List, Tuple
from urllib.parse import urlparse
import httpx

from ..core.models import ATSDetectionResult

logger = logging.getLogger(__name__)


class ATSDetector:
    """Detect ATS platform from company careers page."""

    # URL patterns that indicate specific ATS platforms
    # Each pattern should capture the board token as group 1
    URL_PATTERNS = {
        "greenhouse": [
            r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)",
            r"job-boards\.greenhouse\.io/([a-zA-Z0-9_-]+)",
            r"boards-api\.greenhouse\.io/v1/boards/([a-zA-Z0-9_-]+)",
        ],
        "lever": [
            r"jobs\.lever\.co/([a-zA-Z0-9_-]+)",
            r"api\.lever\.co/v0/postings/([a-zA-Z0-9_-]+)",
        ],
        "ashby": [
            r"jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)",
            r"api\.ashbyhq\.com/posting-api/job-board/([a-zA-Z0-9_-]+)",
        ],
        "workable": [
            r"apply\.workable\.com/([a-zA-Z0-9_-]+)",
            r"([a-zA-Z0-9_-]+)\.workable\.com",
        ],
        "smartrecruiters": [
            r"careers\.smartrecruiters\.com/([a-zA-Z0-9_-]+)",
            r"jobs\.smartrecruiters\.com/([a-zA-Z0-9_-]+)",
        ],
        "jobvite": [
            r"jobs\.jobvite\.com/([a-zA-Z0-9_-]+)",
            r"app\.jobvite\.com/[^/]+/([a-zA-Z0-9_-]+)",
        ],
    }

    # HTML signatures in page source
    HTML_SIGNATURES = {
        "greenhouse": [
            "greenhouse.io",
            "grnhse_app",
            "boards-api.greenhouse.io",
            "boards.greenhouse.io",
        ],
        "lever": [
            "jobs.lever.co",
            "lever-jobs-iframe",
            "api.lever.co",
        ],
        "ashby": [
            "ashbyhq.com",
            "jobs.ashbyhq.com",
            "ashby_embed",
        ],
        "workable": [
            "workable.com",
            "apply.workable.com",
            "whr-embed",
        ],
        "smartrecruiters": [
            "smartrecruiters.com",
            "smrtr.io",
        ],
        "jobvite": [
            "jobs.jobvite.com",
            "jobvite.com",
            "jvi-",
        ],
    }

    def __init__(self, http_client: httpx.AsyncClient):
        self.client = http_client

    async def detect(self, careers_url: str) -> ATSDetectionResult:
        """
        Detect ATS from careers page URL.

        Detection order:
        1. Check URL pattern (fastest, most reliable)
        2. Fetch page and check HTML signatures
        3. Check for redirects to ATS domains
        4. Probe known API endpoints
        """
        # Step 1: URL pattern matching
        result = self._detect_from_url(careers_url)
        if result.provider and result.confidence >= 0.9:
            logger.info(f"Detected {result.provider} from URL pattern: {careers_url}")
            return result

        # Step 2: Fetch page content
        try:
            response = await self.client.get(
                careers_url, follow_redirects=True, timeout=15.0
            )

            # Check if redirected to ATS domain
            final_url = str(response.url)
            if final_url != careers_url:
                redirect_result = self._detect_from_url(final_url)
                if redirect_result.provider:
                    redirect_result = ATSDetectionResult(
                        provider=redirect_result.provider,
                        board_token=redirect_result.board_token,
                        confidence=redirect_result.confidence,
                        detection_method="redirect",
                    )
                    logger.info(
                        f"Detected {redirect_result.provider} from redirect: {final_url}"
                    )
                    return redirect_result

            # Check HTML signatures
            html_result = self._detect_from_html(response.text, careers_url)
            if html_result.provider:
                logger.info(
                    f"Detected {html_result.provider} from HTML signature: {careers_url}"
                )
                return html_result

        except httpx.HTTPError as e:
            logger.warning(f"Failed to fetch {careers_url}: {e}")

        # Step 3: API probing (try common board tokens derived from domain)
        domain = urlparse(careers_url).netloc.replace("www.", "")
        company_slug = domain.split(".")[0]
        probe_result = await self._probe_ats_apis(company_slug)
        if probe_result.provider:
            logger.info(f"Detected {probe_result.provider} from API probe: {company_slug}")
            return probe_result

        return ATSDetectionResult(
            provider="unknown",
            board_token=None,
            confidence=0.0,
            detection_method="none",
        )

    def _detect_from_url(self, url: str) -> ATSDetectionResult:
        """Extract ATS provider and board token from URL pattern."""
        for provider, patterns in self.URL_PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, url, re.IGNORECASE)
                if match:
                    return ATSDetectionResult(
                        provider=provider,
                        board_token=match.group(1),
                        confidence=1.0,
                        detection_method="url_pattern",
                    )
        return ATSDetectionResult(None, None, 0.0, "url_pattern")

    def _detect_from_html(self, html: str, url: str) -> ATSDetectionResult:
        """Detect ATS from HTML signatures and extract board token."""
        html_lower = html.lower()

        for provider, signatures in self.HTML_SIGNATURES.items():
            for sig in signatures:
                if sig.lower() in html_lower:
                    # Try to extract board token from embedded URLs/scripts
                    board_token = self._extract_board_token(html, provider)
                    return ATSDetectionResult(
                        provider=provider,
                        board_token=board_token,
                        confidence=0.8 if board_token else 0.5,
                        detection_method="html_signature",
                    )

        return ATSDetectionResult(None, None, 0.0, "html_signature")

    def _extract_board_token(self, html: str, provider: str) -> Optional[str]:
        """Extract board token from HTML for specific provider."""
        patterns = self.URL_PATTERNS.get(provider, [])
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    async def _probe_ats_apis(self, company_slug: str) -> ATSDetectionResult:
        """Probe ATS APIs with common company slug variations."""
        slugs_to_try = list(
            set(
                [
                    company_slug,
                    company_slug.lower(),
                    company_slug.replace("-", ""),
                    company_slug.replace("_", ""),
                    company_slug.replace("-", "").lower(),
                ]
            )
        )

        probe_endpoints = {
            "greenhouse": "https://api.greenhouse.io/v1/boards/{}/jobs",
            "lever": "https://api.lever.co/v0/postings/{}",
            "ashby": "https://api.ashbyhq.com/posting-api/job-board/{}",
        }

        for provider, endpoint_template in probe_endpoints.items():
            for slug in slugs_to_try:
                endpoint = endpoint_template.format(slug)
                try:
                    response = await self.client.get(endpoint, timeout=5.0)
                    if response.status_code == 200:
                        # Verify it has actual job data
                        data = response.json()
                        if self._has_valid_jobs(data, provider):
                            return ATSDetectionResult(
                                provider=provider,
                                board_token=slug,
                                confidence=0.95,
                                detection_method="api_probe",
                            )
                except (httpx.HTTPError, ValueError):
                    continue

        return ATSDetectionResult(None, None, 0.0, "api_probe")

    def _has_valid_jobs(self, data: dict, provider: str) -> bool:
        """Check if API response contains actual job data (not just empty arrays)."""
        if provider == "greenhouse":
            return "jobs" in data and len(data.get("jobs", [])) > 0
        elif provider == "lever":
            return isinstance(data, list) and len(data) > 0
        elif provider == "ashby":
            return "jobs" in data and len(data.get("jobs", [])) > 0
        return False

    async def detect_with_cache(
        self, domain: str, careers_url: str, cache
    ) -> ATSDetectionResult:
        """
        Detect ATS with caching support.

        Args:
            domain: Company domain for cache key
            careers_url: URL to detect ATS from
            cache: ATS detection cache instance
        """
        # Check cache first
        cached = cache.get(domain)
        if cached:
            return ATSDetectionResult(
                provider=cached["provider"],
                board_token=cached["board_token"],
                confidence=1.0,
                detection_method="cache",
            )

        # Detect
        result = await self.detect(careers_url)

        # Cache if found
        if result.provider and result.provider != "unknown":
            cache.set(domain, result.provider, result.board_token)

        return result
