"""Website scraping with priority-based link selection and domain normalization."""

import asyncio
import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import warnings

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logger = logging.getLogger(__name__)

# Paths to skip when extracting internal links
SKIP_PATHS = {
    "#", "javascript:", ".pdf", ".zip", ".png", ".jpg", ".jpeg", ".gif", ".svg",
    "/login", "/signin", "/signup", "/register", "/api", "/cdn", "/cart", "/checkout",
    "/account", "/admin", "/wp-admin", "/wp-login", "/feed", "/rss",
}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# Priority scores for subpage paths (higher = more valuable for personalization)
LINK_PRIORITY = {
    "/about": 10, "/about-us": 10, "/who-we-are": 10,
    "/services": 9, "/what-we-do": 9, "/solutions": 9, "/capabilities": 9,
    "/work": 8, "/portfolio": 8, "/case-studies": 8, "/case-study": 8, "/projects": 8, "/results": 8,
    "/blog": 7, "/insights": 7, "/resources": 7, "/news": 7, "/articles": 7,
    "/clients": 6, "/industries": 6, "/sectors": 6, "/verticals": 6,
    "/team": 5, "/people": 5, "/leadership": 5,
}

# Boilerplate patterns to strip from extracted text
_BOILERPLATE_RE = re.compile(
    r"(?i)("
    r"all rights reserved|"
    r"©\s*\d{4}|"
    r"cookie policy|privacy policy|terms of service|terms & conditions|"
    r"follow us on|connect with us|"
    r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b|"  # phone numbers
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"  # emails
    r")"
)


def normalize_domain(url: str) -> str:
    """Normalize a URL to a canonical domain for deduplication.

    Strips protocol, www., and trailing slash.
    'https://www.growthspark.com/' -> 'growthspark.com'
    """
    domain = url.strip()
    domain = domain.replace("https://", "").replace("http://", "")
    domain = domain.lstrip("www.")
    domain = domain.rstrip("/")
    return domain.lower()


def _extract_text(html: str) -> str:
    """Extract clean text from HTML, removing scripts/styles/nav/footer."""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def _condense_text(text: str, char_limit: int) -> str:
    """Strip boilerplate, short lines, and collapse whitespace, then truncate."""
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        line = line.strip()
        if len(line) < 5:
            continue
        if _BOILERPLATE_RE.search(line):
            continue
        cleaned.append(line)
    result = "\n".join(cleaned)
    # Collapse multiple whitespace
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result[:char_limit]


def _extract_internal_links(html: str, base_url: str, domain: str) -> List[str]:
    """Extract unique internal links from HTML."""
    soup = BeautifulSoup(html, "lxml")
    seen = set()
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()

        if any(href.lower().startswith(skip) or skip in href.lower() for skip in SKIP_PATHS):
            continue

        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        if domain not in parsed.netloc:
            continue

        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if normalized.rstrip("/") == base_url.rstrip("/"):
            continue

        if normalized not in seen:
            seen.add(normalized)
            links.append(normalized)

    return links


def _prioritize_links(links: List[str], max_links: int = 5) -> List[str]:
    """Sort links by priority score, preferring informative pages."""
    def score(url: str) -> int:
        path = urlparse(url).path.rstrip("/").lower()
        # Check exact path match first
        if path in LINK_PRIORITY:
            return LINK_PRIORITY[path]
        # Check if path starts with a priority prefix
        for prefix, s in LINK_PRIORITY.items():
            if path.startswith(prefix):
                return s
        return 1

    scored = sorted(links, key=score, reverse=True)
    return scored[:max_links]


def strip_em_dashes(text: str) -> str:
    """Replace em dashes and en dashes with ' - '."""
    return text.replace("\u2014", " - ").replace("\u2013", " - ")


async def scrape_website(
    url: str,
    timeout: float = 10.0,
    subpage_delay: float = 0.5,
    max_subpages: int = 5,
    homepage_char_limit: int = 2000,
    subpage_char_limit: int = 1000,
) -> Dict:
    """Scrape a company website: homepage + up to max_subpages prioritized subpages.

    Returns dict with keys: homepage_text, subpage_texts, error
    """
    domain = normalize_domain(url)
    result = {"homepage_text": "", "subpage_texts": [], "error": None}

    async with httpx.AsyncClient(
        headers=BROWSER_HEADERS, timeout=timeout, follow_redirects=True
    ) as client:
        # Fetch homepage
        homepage_url = f"https://{domain}"
        try:
            resp = await client.get(homepage_url)
            resp.raise_for_status()
        except Exception:
            try:
                homepage_url = f"http://{domain}"
                resp = await client.get(homepage_url)
                resp.raise_for_status()
            except Exception as e:
                result["error"] = str(e)
                logger.warning(f"Failed to fetch {domain}: {e}")
                return result

        homepage_html = resp.text
        homepage_text = _extract_text(homepage_html)
        result["homepage_text"] = _condense_text(homepage_text, homepage_char_limit)

        # Extract and prioritize internal links
        internal_links = _extract_internal_links(homepage_html, homepage_url, domain)
        subpage_links = _prioritize_links(internal_links, max_subpages)

        # Scrape subpages
        for link in subpage_links:
            await asyncio.sleep(subpage_delay)
            try:
                resp = await client.get(link)
                resp.raise_for_status()
                page_text = _extract_text(resp.text)
                condensed = _condense_text(page_text, subpage_char_limit)
                if len(condensed) > 50:
                    result["subpage_texts"].append(condensed)
            except Exception as e:
                logger.debug(f"Failed to fetch subpage {link}: {e}")

    # Check minimum content threshold (200 words)
    all_text = result["homepage_text"] + " " + " ".join(result.get("subpage_texts", []))
    word_count = len(all_text.split())
    if word_count < 200:
        result["error"] = f"insufficient_content: only {word_count} words (min 200)"
        result["homepage_text"] = ""
        result["subpage_texts"] = []

    return result


async def scrape_all_websites(
    urls: List[str],
    concurrency: int = 20,
    **scrape_kwargs,
) -> Dict[str, Dict]:
    """Scrape all websites concurrently with a semaphore.

    Args:
        urls: List of website URLs (will be deduplicated by normalized domain).
        concurrency: Max concurrent scrapes.
        **scrape_kwargs: Passed to scrape_website().

    Returns:
        Dict mapping normalized_domain -> scrape result.
    """
    # Deduplicate by normalized domain
    domain_to_url = {}
    for url in urls:
        domain = normalize_domain(url)
        if domain and domain not in domain_to_url:
            domain_to_url[domain] = url

    semaphore = asyncio.Semaphore(concurrency)
    results: Dict[str, Dict] = {}
    completed = 0
    total = len(domain_to_url)

    async def _scrape_one(domain: str, url: str):
        nonlocal completed
        async with semaphore:
            try:
                result = await scrape_website(url, **scrape_kwargs)
                results[domain] = result
            except Exception as e:
                logger.error(f"Scrape failed for {domain}: {e}")
                results[domain] = {"homepage_text": "", "subpage_texts": [], "error": str(e)}
            completed += 1
            if completed % 50 == 0 or completed == total:
                print(f"  Scraped {completed}/{total} websites")

    await asyncio.gather(*[_scrape_one(d, u) for d, u in domain_to_url.items()])
    return results


def build_content_summary(scrape_result: Dict) -> str:
    """Build a content summary string from a scrape result for the LLM prompt."""
    parts = []
    if scrape_result.get("homepage_text"):
        parts.append(scrape_result["homepage_text"])
    for subpage_text in scrape_result.get("subpage_texts", []):
        parts.append(subpage_text)
    return "\n\n---\n\n".join(parts) if parts else ""
