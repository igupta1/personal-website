"""Enhanced ATS detection with prioritized API probing and smart token generation."""

import asyncio
import re
import logging
from typing import Optional, List, Tuple, Set
from urllib.parse import urlparse
import httpx

from ..core.models import ATSDetectionResult

logger = logging.getLogger(__name__)


class EnhancedATSDetector:
    """
    Enhanced ATS detection that prioritizes official API probing.

    Detection Order:
    1. Generate smart board token variations from company name/domain
    2. Probe all major ATS APIs with token variations
    3. Lightweight HTML fingerprinting on homepage for ATS widgets
    4. Google search for careers page discovery
    5. Fall back to linkedin_only classification
    """

    # ATS API endpoints - order matters (most common first)
    ATS_ENDPOINTS = {
        "greenhouse": "https://api.greenhouse.io/v1/boards/{token}/jobs",
        "lever": "https://api.lever.co/v0/postings/{token}",
        "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
        "workable": "https://apply.workable.com/api/v1/widget/accounts/{token}",
        "jobvite": "https://jobs.jobvite.com/{token}/feed.xml",
        "smartrecruiters": "https://api.smartrecruiters.com/v1/companies/{token}/postings",
        "recruitee": "https://{token}.recruitee.com/api/offers/",
        "breezyhr": "https://{token}.breezy.hr/json",
        "personio": "https://{token}.jobs.personio.de/",
    }

    # HTML signatures for embedded ATS widgets
    HTML_FINGERPRINTS = {
        "greenhouse": [
            r'src="[^"]*greenhouse\.io',
            r'href="[^"]*boards\.greenhouse\.io/([a-zA-Z0-9_-]+)',
            r'href="[^"]*job-boards\.greenhouse\.io/([a-zA-Z0-9_-]+)',
            r'data-greenhouse',
            r'grnhse_app',
            r'greenhouse-job-board',
        ],
        "lever": [
            r'src="[^"]*lever\.co',
            r'href="[^"]*jobs\.lever\.co/([a-zA-Z0-9_-]+)',
            r'lever-jobs-iframe',
            r'data-lever',
            r'lever-job-board',
        ],
        "ashby": [
            r'src="[^"]*ashbyhq\.com',
            r'href="[^"]*jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)',
            r'ashby-job-board',
            r'ashby_embed',
            r'data-ashby',
        ],
        "workable": [
            r'src="[^"]*workable\.com',
            r'href="[^"]*apply\.workable\.com/([a-zA-Z0-9_-]+)',
            r'whr-embed',
            r'workable-widget',
            r'data-workable',
        ],
        "smartrecruiters": [
            r'src="[^"]*smartrecruiters\.com',
            r'href="[^"]*careers\.smartrecruiters\.com/([a-zA-Z0-9_-]+)',
            r'href="[^"]*jobs\.smartrecruiters\.com/([a-zA-Z0-9_-]+)',
        ],
        "jobvite": [
            r'src="[^"]*jobvite\.com',
            r'href="[^"]*jobs\.jobvite\.com/([a-zA-Z0-9_-]+)',
            r'jvi-job-list',
        ],
        "bamboohr": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.bamboohr\.com/careers',
            r'href="[^"]*([a-zA-Z0-9_-]+)\.bamboohr\.com/jobs',
            r'bamboohr\.com/js/embed',
        ],
        "rippling": [
            r'href="[^"]*ats\.rippling\.com/([a-zA-Z0-9_-]+)',
        ],
        "breezyhr": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.breezy\.hr',
        ],
        "teamtailor": [
            r'href="[^"]*career\.teamtailor\.com/([a-zA-Z0-9_-]+)',
            r'href="[^"]*([a-zA-Z0-9_-]+)\.teamtailor\.com',
            r'teamtailor-cdn\.com',
            r'Powered by Teamtailor',
            r'_ttAnalytics',
        ],
        "recruitee": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.recruitee\.com',
            r'recruitee\.com/api',
            r'recruitee-careers',
        ],
        "personio": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.jobs\.personio',
            r'personio-jobs',
            r'jobs\.personio\.de',
        ],
        "jazzhr": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.applytojob\.com',
            r'app\.jazz\.co',
            r'jazzhr\.com',
        ],
        "icims": [
            r'href="[^"]*careers-([a-zA-Z0-9_-]+)\.icims\.com',
            r'href="[^"]*([a-zA-Z0-9_-]+)\.icims\.com',
            r'icims\.com/jobs',
        ],
        "taleo": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.taleo\.net',
            r'taleo\.net/careersection',
        ],
        "workday": [
            r'href="[^"]*([a-zA-Z0-9_-]+)\.wd\d+\.myworkdayjobs\.com',
            r'myworkdayjobs\.com',
            r'workday\.com/.*careers',
        ],
    }

    # LinkedIn company page patterns
    LINKEDIN_PATTERNS = [
        r'href="[^"]*linkedin\.com/company/([a-zA-Z0-9_-]+)',
        r'linkedin\.com/company/([a-zA-Z0-9_-]+)/jobs',
    ]

    def __init__(self, http_client: httpx.AsyncClient):
        self.client = http_client

    def generate_token_variations(self, company_name: str, domain: str, linkedin_slug: Optional[str] = None) -> List[str]:
        """
        Generate board token variations from company name and domain.

        Balanced for speed and accuracy: generates ~5-10 most likely tokens.

        Examples:
            "LlamaIndex" + "llamaindex.ai" -> ["llamaindex", "llama-index"]
            "Fitt Insider" + "fittinsider.com" -> ["fittinsider", "fitt-insider", "fitt"]
        """
        variations: Set[str] = set()

        # From domain (highest priority - most likely to be the token)
        domain_base = domain.split(".")[0].lower()
        variations.add(domain_base)
        variations.add(domain_base.replace("-", ""))

        # From company name (no spaces, with hyphens)
        name_clean = re.sub(r'[^a-zA-Z0-9\s-]', '', company_name)
        name_lower = name_clean.lower()
        variations.add(name_lower.replace(" ", ""))
        variations.add(name_lower.replace(" ", "-"))

        # First word only (for multi-word companies like "Fitt Insider" -> "fitt")
        words = name_lower.split()
        if words:
            variations.add(words[0])

        # Acronyms for long names (e.g., "New York Foundation for the Arts" -> "nyfa")
        if len(words) >= 3:
            acronym = "".join(w[0] for w in words if len(w) > 0)
            if len(acronym) >= 3:
                variations.add(acronym)

        # Add LinkedIn slug if available (often matches the ATS token)
        if linkedin_slug:
            variations.add(linkedin_slug.lower())

        # Filter out invalid tokens
        valid_variations = set()
        for v in variations:
            v = v.strip()
            if len(v) <= 2 or len(v) >= 50:
                continue
            if '_' in v:
                continue
            if any(c in v for c in '()&,. '):
                continue
            if v.endswith('-'):
                continue
            valid_variations.add(v)

        return list(valid_variations)

    async def detect(
        self,
        company_name: str,
        domain: str,
        technologies: str = ""
    ) -> ATSDetectionResult:
        """
        Detect ATS for a company using prioritized detection methods.

        Args:
            company_name: Company name from Apollo
            domain: Company domain (e.g., llamaindex.ai)
            technologies: Technologies field from Apollo (may hint at ATS)

        Returns:
            ATSDetectionResult with provider, board_token, and confidence
        """
        logger.info(f"Detecting ATS for {company_name} ({domain})")

        # Try to get LinkedIn slug early for better token generation
        linkedin_slug = await self._extract_linkedin_slug(domain)

        # Generate token variations (including LinkedIn slug if found)
        tokens = self.generate_token_variations(company_name, domain, linkedin_slug)
        logger.debug(f"Generated {len(tokens)} token variations: {tokens[:5]}...")

        # Step 1: Probe all ATS APIs concurrently (much faster)
        all_ats = list(self.ATS_ENDPOINTS.keys())

        # Probe all ATS platforms in parallel with all tokens
        probe_tasks = [self._probe_ats_api(ats_name, tokens) for ats_name in all_ats]
        probe_results = await asyncio.gather(*probe_tasks, return_exceptions=True)

        # Return first successful detection (prefer by priority order)
        priority_ats = self._get_priority_ats(technologies)
        ats_to_result = {}
        for ats_name, result in zip(all_ats, probe_results):
            if isinstance(result, ATSDetectionResult) and result.provider:
                ats_to_result[ats_name] = result

        # Check priority order first
        for ats_name in priority_ats:
            if ats_name in ats_to_result:
                return ats_to_result[ats_name]

        # Then any other detected
        for ats_name in all_ats:
            if ats_name in ats_to_result:
                return ats_to_result[ats_name]

        # Step 3: Homepage HTML fingerprinting
        homepage_result = await self._detect_from_homepage(domain)
        if homepage_result.provider:
            return homepage_result

        # Step 4: Try careers page variations
        careers_result = await self._detect_from_careers_pages(domain)
        if careers_result.provider:
            return careers_result

        # Step 5: Fall back to LinkedIn-only (we already extracted the slug at the start)
        if linkedin_slug:
            return ATSDetectionResult(
                provider="linkedin_only",
                board_token=linkedin_slug,
                confidence=0.6,
                detection_method="linkedin_fallback",
            )

        # Final fallback
        return ATSDetectionResult(
            provider="linkedin_only",
            board_token=None,
            confidence=0.3,
            detection_method="default_fallback",
        )

    def _get_priority_ats(self, technologies: str) -> List[str]:
        """Get priority-ordered ATS list based on technology hints."""
        technologies = technologies.lower()
        priority = []

        # Check for explicit mentions in technologies
        ats_keywords = {
            "greenhouse": ["greenhouse"],
            "lever": ["lever"],
            "ashby": ["ashby"],
            "workable": ["workable"],
            "jobvite": ["jobvite"],
            "smartrecruiters": ["smartrecruiters", "smart recruiters"],
            "recruitee": ["recruitee"],
            "breezyhr": ["breezy", "breezyhr"],
            "personio": ["personio"],
            "bamboohr": ["bamboo", "bamboohr"],
            "teamtailor": ["teamtailor"],
            "jazzhr": ["jazz", "jazzhr"],
        }

        for ats_name, keywords in ats_keywords.items():
            for kw in keywords:
                if kw in technologies:
                    priority.append(ats_name)
                    break

        # Default priority order (most common ATS first based on market share)
        default_order = [
            "greenhouse", "lever", "ashby", "workable",
            "smartrecruiters", "recruitee", "breezyhr", "personio", "jobvite"
        ]

        # Add remaining in default order
        for ats in default_order:
            if ats not in priority:
                priority.append(ats)

        return priority

    async def _probe_ats_api(
        self,
        ats_name: str,
        tokens: List[str]
    ) -> ATSDetectionResult:
        """Probe a specific ATS API with multiple token variations."""
        endpoint_template = self.ATS_ENDPOINTS.get(ats_name)
        if not endpoint_template:
            return ATSDetectionResult(None, None, 0.0, "api_probe")

        for token in tokens:
            endpoint = endpoint_template.format(token=token)
            try:
                response = await self.client.get(endpoint, timeout=3.0)

                if response.status_code == 200:
                    # Validate response has actual jobs
                    if await self._validate_jobs_response(response, ats_name):
                        logger.info(f"Found {ats_name} with token '{token}'")
                        return ATSDetectionResult(
                            provider=ats_name,
                            board_token=token,
                            confidence=0.95,
                            detection_method="api_probe",
                        )
            except (httpx.HTTPError, httpx.TimeoutException) as e:
                logger.debug(f"Probe failed for {ats_name}/{token}: {e}")
                continue

        return ATSDetectionResult(None, None, 0.0, "api_probe")

    async def _validate_jobs_response(self, response: httpx.Response, ats_name: str) -> bool:
        """Validate that the API response contains actual job data or is a valid empty board."""
        try:
            if ats_name == "jobvite":
                # XML feed - just check for job elements
                content = response.text
                return "<job>" in content.lower() or "<item>" in content.lower()

            data = response.json()

            if ats_name == "greenhouse":
                jobs = data.get("jobs", [])
                # Accept valid response structure (even if empty - company uses Greenhouse)
                return isinstance(jobs, list) and len(jobs) > 0

            elif ats_name == "lever":
                return isinstance(data, list) and len(data) > 0

            elif ats_name == "ashby":
                jobs = data.get("jobs", [])
                return isinstance(jobs, list) and len(jobs) > 0

            elif ats_name == "workable":
                jobs = data.get("jobs", [])
                return isinstance(jobs, list) and len(jobs) > 0

            elif ats_name == "smartrecruiters":
                # SmartRecruiters API returns content object with jobs
                content = data.get("content", [])
                return isinstance(content, list) and len(content) > 0

            elif ats_name == "recruitee":
                # Recruitee returns offers array
                offers = data.get("offers", [])
                return isinstance(offers, list) and len(offers) > 0

            elif ats_name == "breezyhr":
                # BreezyHR returns array of positions
                return isinstance(data, list) and len(data) > 0

            elif ats_name == "personio":
                # Check if page has job content (HTML response)
                if isinstance(data, str):
                    return "position" in data.lower() or "job" in data.lower()
                return False

            return False

        except Exception as e:
            logger.debug(f"Failed to validate response: {e}")
            return False

    async def _detect_from_homepage(self, domain: str) -> ATSDetectionResult:
        """Detect ATS from homepage HTML fingerprints (concurrent check)."""
        urls_to_try = [
            f"https://{domain}",
            f"https://www.{domain}",
        ]

        async def check_homepage(url: str) -> ATSDetectionResult:
            try:
                response = await self.client.get(
                    url,
                    follow_redirects=True,
                    timeout=8.0  # Slightly reduced timeout
                )

                if response.status_code == 200:
                    result = self._fingerprint_html(response.text)
                    if result.provider:
                        return result

            except (httpx.HTTPError, httpx.TimeoutException) as e:
                logger.debug(f"Homepage fetch failed for {url}: {e}")

            return ATSDetectionResult(None, None, 0.0, "homepage_fingerprint")

        # Check both URLs concurrently
        results = await asyncio.gather(*[check_homepage(url) for url in urls_to_try])

        for result in results:
            if result.provider:
                return result

        return ATSDetectionResult(None, None, 0.0, "homepage_fingerprint")

    async def _detect_from_careers_pages(self, domain: str) -> ATSDetectionResult:
        """Check common careers page URLs for ATS fingerprints (optimized with concurrency)."""
        # High-priority paths (most common)
        priority_paths = ["/careers", "/jobs", "/join"]
        # Secondary paths (less common)
        secondary_paths = ["/about/careers", "/company/careers", "/join-us", "/work-with-us"]

        # Subdomain variations (most direct when they exist)
        subdomain_urls = [
            f"https://careers.{domain}",
            f"https://jobs.{domain}",
        ]

        # Build URL list for priority check
        priority_urls = subdomain_urls + [
            f"https://{domain}{path}" for path in priority_paths
        ]

        # Check priority URLs concurrently (fast parallel check)
        result = await self._check_urls_concurrently(priority_urls)
        if result.provider:
            return result

        # If priority check failed, try secondary paths (also concurrent)
        secondary_urls = [f"https://{domain}{path}" for path in secondary_paths]
        result = await self._check_urls_concurrently(secondary_urls)
        if result.provider:
            return result

        return ATSDetectionResult(None, None, 0.0, "careers_page")

    async def _check_urls_concurrently(self, urls: List[str]) -> ATSDetectionResult:
        """Check multiple URLs concurrently for ATS fingerprints."""
        async def check_single_url(url: str) -> ATSDetectionResult:
            try:
                response = await self.client.get(
                    url,
                    follow_redirects=True,
                    timeout=5.0  # Reduced timeout for faster failure
                )

                if response.status_code == 200:
                    # Check final URL for ATS redirect
                    final_url = str(response.url)
                    redirect_result = self._check_url_for_ats(final_url)
                    if redirect_result.provider:
                        return redirect_result

                    # Check HTML for fingerprints
                    html_result = self._fingerprint_html(response.text)
                    if html_result.provider:
                        return html_result

            except (httpx.HTTPError, httpx.TimeoutException):
                pass

            return ATSDetectionResult(None, None, 0.0, "concurrent_check")

        # Run all URL checks concurrently
        tasks = [check_single_url(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Return first successful result
        for result in results:
            if isinstance(result, ATSDetectionResult) and result.provider:
                return result

        return ATSDetectionResult(None, None, 0.0, "concurrent_check")

    def _fingerprint_html(self, html: str) -> ATSDetectionResult:
        """Extract ATS provider and board token from HTML content."""
        for ats_name, patterns in self.HTML_FINGERPRINTS.items():
            for pattern in patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    # Try to extract board token from captured group
                    board_token = match.group(1) if match.lastindex else None
                    return ATSDetectionResult(
                        provider=ats_name,
                        board_token=board_token,
                        confidence=0.85 if board_token else 0.6,
                        detection_method="html_fingerprint",
                    )

        return ATSDetectionResult(None, None, 0.0, "html_fingerprint")

    def _check_url_for_ats(self, url: str) -> ATSDetectionResult:
        """Check if a URL indicates a specific ATS platform."""
        url_patterns = {
            "greenhouse": [
                r'boards\.greenhouse\.io/([a-zA-Z0-9_-]+)',
                r'job-boards\.greenhouse\.io/([a-zA-Z0-9_-]+)',
            ],
            "lever": [
                r'jobs\.lever\.co/([a-zA-Z0-9_-]+)',
            ],
            "ashby": [
                r'jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)',
            ],
            "workable": [
                r'apply\.workable\.com/([a-zA-Z0-9_-]+)',
            ],
            "smartrecruiters": [
                r'careers\.smartrecruiters\.com/([a-zA-Z0-9_-]+)',
                r'jobs\.smartrecruiters\.com/([a-zA-Z0-9_-]+)',
            ],
            "jobvite": [
                r'jobs\.jobvite\.com/([a-zA-Z0-9_-]+)',
            ],
            "bamboohr": [
                r'([a-zA-Z0-9_-]+)\.bamboohr\.com',
            ],
            "breezyhr": [
                r'([a-zA-Z0-9_-]+)\.breezy\.hr',
            ],
            "teamtailor": [
                r'career\.teamtailor\.com/([a-zA-Z0-9_-]+)',
                r'([a-zA-Z0-9_-]+)\.teamtailor\.com',
            ],
            "recruitee": [
                r'([a-zA-Z0-9_-]+)\.recruitee\.com',
            ],
            "personio": [
                r'([a-zA-Z0-9_-]+)\.jobs\.personio',
            ],
            "jazzhr": [
                r'([a-zA-Z0-9_-]+)\.applytojob\.com',
            ],
            "icims": [
                r'careers-([a-zA-Z0-9_-]+)\.icims\.com',
                r'([a-zA-Z0-9_-]+)\.icims\.com',
            ],
            "taleo": [
                r'([a-zA-Z0-9_-]+)\.taleo\.net',
            ],
            "workday": [
                r'([a-zA-Z0-9_-]+)\.wd\d+\.myworkdayjobs\.com',
            ],
            "rippling": [
                r'ats\.rippling\.com/([a-zA-Z0-9_-]+)',
            ],
        }

        for ats_name, patterns in url_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, url, re.IGNORECASE)
                if match:
                    return ATSDetectionResult(
                        provider=ats_name,
                        board_token=match.group(1),
                        confidence=1.0,
                        detection_method="url_redirect",
                    )

        return ATSDetectionResult(None, None, 0.0, "url_redirect")

    async def _extract_linkedin_slug(self, domain: str) -> Optional[str]:
        """Try to extract LinkedIn company slug from homepage (fast, single request)."""
        # Only try the main domain (skip www variant to save time)
        url = f"https://{domain}"
        try:
            response = await self.client.get(
                url,
                follow_redirects=True,
                timeout=5.0  # Reduced timeout
            )

            if response.status_code == 200:
                for pattern in self.LINKEDIN_PATTERNS:
                    match = re.search(pattern, response.text, re.IGNORECASE)
                    if match:
                        return match.group(1)

        except (httpx.HTTPError, httpx.TimeoutException):
            pass

        return None
