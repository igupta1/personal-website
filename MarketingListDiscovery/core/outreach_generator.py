"""Personalized outreach draft generation using website scraping + Anthropic Claude."""

import asyncio
import json
import logging
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

SUMMARIZE_AND_COMPLIMENT_PROMPT = (
    "You're provided markdown scrapes of a company's homepage and up to 3 subpages. "
    "Do two things:\n\n"
    "1. Write a two-paragraph summary of what this company does, who they serve, and "
    "anything notable about their positioning, values, or recent activity.\n\n"
    "2. Based on that summary, generate a single compliment sentence about this company "
    "that could open a cold email.\n\n"
    'Return as JSON: {"summary": "your summary here", "compliment": "your compliment here"}.\n\n'
    "Summary rules: Be comprehensive but concise. Focus on specifics that make this company unique.\n\n"
    "Compliment rules: One sentence only. Reference something specific and non-obvious from the summary. "
    "Do NOT say generic things like 'Love your website' or 'Great company'. "
    "Write in a matter-of-fact, peer-to-peer tone. Do NOT use gushing language like "
    "'It's genuinely impressive,' 'It's really cool,' 'It's fantastic how,' or "
    "'I was really impressed.' State the observation directly without filler praise. "
    "For example, instead of 'It's genuinely impressive how EXL achieves a 90% success rate' "
    "write 'EXL's 90% success rate on enterprise AI initiatives is a strong proof point.' "
    "Shorten company names where natural. "
    "Do NOT use em dashes (\u2014) anywhere.\n\n"
    'If content is empty or unusable, return {"summary": "none", "compliment": "none"}'
)

OUTREACH_AGENCY_WITH_COMPLIMENT = (
    "{compliment} Noticed you're looking for {a_role}. Before you commit to a "
    "full-time hire, would it be worth exploring whether an agency could deliver "
    "the same results at a fraction of the cost?"
)

OUTREACH_AGENCY_WITHOUT_COMPLIMENT = (
    "Noticed you're looking for {a_role}. Before you commit to a full-time hire, "
    "would it be worth exploring whether an agency could deliver the same results "
    "at a fraction of the cost?"
)

OUTREACH_NON_AGENCY_WITH_COMPLIMENT = (
    "{compliment} Noticed you're looking for {a_role}. If your team is also "
    "stretched thin on digital marketing or content, that's something an agency "
    "could take off your plate while you focus on hiring for the in-person side."
)

OUTREACH_NON_AGENCY_WITHOUT_COMPLIMENT = (
    "Noticed you're looking for {a_role}. If your team is also stretched thin on "
    "digital marketing or content, that's something an agency could take off your "
    "plate while you focus on hiring for the in-person side."
)

OUTREACH_AGENCY_COMPANY_WITH_COMPLIMENT = (
    "{compliment} Noticed you're hiring for {a_role}. If you ever need overflow "
    "support or white-label help on client work, would it be worth a quick conversation?"
)

OUTREACH_AGENCY_COMPANY_WITHOUT_COMPLIMENT = (
    "Noticed you're hiring for {a_role}. If you ever need overflow support or "
    "white-label help on client work, would it be worth a quick conversation?"
)

# Keywords for role classification (checked case-insensitively against role title)
_NOT_AGENCY_KEYWORDS = [
    "brand ambassador", "promotions representative", "field marketing",
    "event coordinator", "canvasser", "door hanger", "retail",
    "on-call", "shift", "community outreach", "merchandising", "sales",
]

_AGENCY_KEYWORDS = [
    "marketing coordinator", "social media", "content strategist",
    "content coordinator", "seo", "paid media", "paid search",
    "digital marketing", "pr ", "public relations", "comms ",
    "communications", "growth marketing", "email marketing",
    "ecommerce", "e-commerce", "retention marketing", "web content",
    "digital merchandising", "market analyst", "marketing analyst",
    "marketing associate", "copywriter", "marketing specialist",
    "marketing manager", "media planning", "media planner",
    "ad operations", "advertising operations",
]


def _classify_role(role_title: str) -> str:
    """Classify a role as agency_replaceable or not_agency_replaceable."""
    lower = role_title.lower()
    for kw in _NOT_AGENCY_KEYWORDS:
        if kw in lower:
            return "not_agency_replaceable"
    return "agency_replaceable"

# Paths to skip when extracting internal links
SKIP_PATHS = {
    "#", "javascript:", ".pdf", ".zip", ".png", ".jpg", ".jpeg", ".gif", ".svg",
    "/login", "/signin", "/signup", "/register", "/api", "/cdn", "/cart", "/checkout",
    "/account", "/admin", "/wp-admin", "/wp-login", "/feed", "/rss",
}

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Full browser headers to reduce 403 rejections
BROWSER_HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
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


def _strip_em_dashes(text: str) -> str:
    """Replace em dashes and en dashes with ' - '."""
    return text.replace("\u2014", " - ").replace("\u2013", " - ")


_ROLE_KEYWORDS = [
    "coordinator", "manager", "specialist", "director", "analyst",
    "associate", "ambassador", "writer", "strategist", "planner",
    "lead", "head", "vp", "chief", "officer", "editor", "designer",
    "developer", "engineer", "assistant", "intern", "executive",
    "representative", "consultant", "supervisor", "administrator",
]


def _clean_role_title(role_title: str) -> str:
    """Strip location, parentheticals, and junk from role titles.

    Examples:
        "Digital Audience Operations Support(New York, NY)" -> "Digital Audience Operations Support"
        "Gretchen - Energy - Sports Drink - Brand Ambassador - Promoter - Weekly Pay" -> "Brand Ambassador"
        "Social Media Coordinator- Beauty" -> "Social Media Coordinator"
        "Associate, Growth & Go-To-Market Strategy" -> "Associate, Growth & Go-To-Market Strategy"
    """
    # Step 1: Remove parenthetical content
    cleaned = re.sub(r'\s*\(.*?\)\s*', '', role_title).strip()

    # Step 2: Handle dash-separated junk titles (3+ segments)
    segments = [s.strip() for s in cleaned.split(" - ") if s.strip()]
    if len(segments) >= 3:
        # Find segment containing a role keyword
        for seg in segments:
            seg_lower = seg.lower()
            if any(kw in seg_lower for kw in _ROLE_KEYWORDS):
                cleaned = seg
                break
        else:
            # No keyword match — use longest segment as best guess
            cleaned = max(segments, key=len)

    # Step 3: Strip trailing "- Category" suffix, but protect hyphenated
    # compounds like "Go-To-Market" where the dash is part of the word
    if not re.search(r'\w-\w', cleaned):
        cleaned = re.sub(r'\s*-\s*\w+(\s+\w+)?$', '', cleaned).strip()

    return cleaned if cleaned else role_title


def _a_or_an(role_title: str) -> str:
    """Return 'a' or 'an' based on whether the role title starts with a vowel sound."""
    word = role_title.lstrip().split()[0] if role_title.strip() else ""

    # Handle acronyms (all-uppercase words like UGC, SEO, HR, FBI)
    # Letters whose names start with vowel sounds: A E F H I L M N O R S X
    if word.isupper() and len(word) >= 2:
        vowel_sound_letters = set("AEFHILMNORSX")
        if word[0] in vowel_sound_letters:
            return f"an {role_title}"
        return f"a {role_title}"

    first_char = role_title.lstrip().lower()[:1]
    if first_char in ('a', 'e', 'i', 'o', 'u'):
        return f"an {role_title}"
    return f"a {role_title}"


_AGENCY_COMPANY_KEYWORDS = [
    "marketing agency", "advertising agency", "pr agency", "pr firm",
    "media agency", "creative agency", "digital agency", "communications agency",
    "communications firm", "marketing firm", "ad agency", "branding agency",
    "full-service agency", "marketing services firm", "advertising firm",
    "digital marketing agency", "social media agency", "content agency",
    "performance marketing agency", "media buying agency",
]


def _is_marketing_agency(summary: str) -> bool:
    """Detect if company is a marketing/advertising/PR agency from its summary."""
    if not summary or summary == "none":
        return False
    lower = summary.lower()
    return any(kw in lower for kw in _AGENCY_COMPANY_KEYWORDS)


class OutreachGenerator:
    """Generate personalized outreach drafts by scraping company websites and using Anthropic Claude."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-6",
    ):
        self.model = model
        self.client = AsyncAnthropic(api_key=api_key, max_retries=3)

    async def generate_outreach(
        self, companies: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, str]]:
        """
        Generate outreach drafts for a list of companies.

        Args:
            companies: List of dicts with keys:
                - company_name (str)
                - domain (str)
                - roles (list of str): job titles being hired

        Returns:
            Dict mapping company_name -> {summary, compliment, outreach_draft}
        """
        results: Dict[str, Dict[str, str]] = {}

        logger.info(f"Generating outreach for {len(companies)} companies")

        for i, company in enumerate(companies):
            name = company["company_name"]
            domain = company.get("domain", "")
            roles = company.get("roles", [])
            role_title = roles[0] if roles else "marketing role"
            role_class = _classify_role(role_title)

            try:
                print(f"  [{i+1}/{len(companies)}] {name}: scraping {domain}...")
                raw_text = await self._scrape_company(domain)

                if raw_text and len(raw_text.strip()) > 100:
                    print(f"  [{i+1}/{len(companies)}] {name}: summarizing + generating compliment...")
                    summary, compliment = await self._summarize_and_compliment(raw_text)
                else:
                    summary = "none"
                    compliment = "none"

                is_agency_co = _is_marketing_agency(summary)
                if is_agency_co:
                    role_class = "agency_company"
                    print(f"  [{i+1}/{len(companies)}] {name}: detected as agency, using agency template")

                draft = self._assemble_draft(compliment, role_title, role_class, is_agency_co)

                results[name] = {
                    "summary": summary or "none",
                    "compliment": compliment or "none",
                    "outreach_draft": draft,
                    "role_classification": role_class,
                }
                print(f"  [{i+1}/{len(companies)}] {name}: done")

            except Exception as e:
                logger.error(f"Outreach generation failed for {name}: {e}")
                draft = self._assemble_draft("none", role_title, role_class)
                results[name] = {
                    "summary": "none",
                    "compliment": "none",
                    "outreach_draft": draft,
                    "role_classification": role_class,
                }

            if i < len(companies) - 1:
                await asyncio.sleep(1.0)

        return results

    async def _scrape_company(self, domain: str) -> str:
        """Scrape company homepage + up to 3 subpages, return concatenated text."""
        all_text = []

        async with httpx.AsyncClient(
            headers=BROWSER_HEADERS, timeout=10.0, follow_redirects=True
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
                    logger.warning(f"Failed to fetch {domain}: {e}")
                    return ""

            homepage_html = resp.text
            homepage_text = self._extract_text(homepage_html)
            all_text.append(homepage_text[:3000])

            # Extract internal links
            internal_links = self._extract_internal_links(homepage_html, homepage_url, domain)
            subpage_links = internal_links[:3]

            # Scrape subpages
            for link in subpage_links:
                await asyncio.sleep(1.0)
                try:
                    resp = await client.get(link)
                    resp.raise_for_status()
                    page_text = self._extract_text(resp.text)
                    all_text.append(page_text[:3000])
                except Exception as e:
                    logger.debug(f"Failed to fetch subpage {link}: {e}")

        concatenated = "\n\n---\n\n".join(all_text)
        return concatenated[:10000]

    @staticmethod
    def _extract_text(html: str) -> str:
        """Extract clean text from HTML, removing scripts/styles/nav/footer."""
        soup = BeautifulSoup(html, "lxml")
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "noscript"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)

    @staticmethod
    def _extract_internal_links(html: str, base_url: str, domain: str) -> List[str]:
        """Extract unique internal links from HTML."""
        soup = BeautifulSoup(html, "lxml")
        seen = set()
        links = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()

            # Skip non-content paths
            if any(href.lower().startswith(skip) or skip in href.lower() for skip in SKIP_PATHS):
                continue

            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)

            # Only keep same-domain links
            if domain not in parsed.netloc:
                continue

            # Normalize: strip fragment and query
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if normalized.rstrip("/") == base_url.rstrip("/"):
                continue

            if normalized not in seen:
                seen.add(normalized)
                links.append(normalized)

        return links

    async def _summarize_and_compliment(self, raw_text: str) -> tuple:
        """Summarize company website and generate compliment in a single LLM call.

        Returns:
            Tuple of (summary, compliment) strings.
        """
        prompt = f"{SUMMARIZE_AND_COMPLIMENT_PROMPT}\n\n---\n\n{raw_text}"

        try:
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_response = response.content[0].text
            if not raw_response:
                return "none", "none"

            summary = self._parse_json_field(raw_response, "summary")
            compliment = self._parse_json_field(raw_response, "compliment")
            compliment = _strip_em_dashes(compliment) if compliment and compliment != "none" else "none"
            return summary, compliment
        except Exception as e:
            logger.error(f"Summarize+compliment call failed: {e}")
            return "none", "none"

    @staticmethod
    def _parse_json_field(raw_text: str, field: str) -> str:
        """Parse a JSON response for a specific field, with fallbacks."""
        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1]).strip()

        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict) and field in parsed:
                return parsed[field]
        except json.JSONDecodeError:
            pass

        # Regex fallback
        pattern = rf'"{field}"\s*:\s*"((?:[^"\\]|\\.)*)\"'
        match = re.search(pattern, raw_text)
        if match:
            return match.group(1).replace('\\"', '"')

        return "none"

    @staticmethod
    def _assemble_draft(
        compliment: str,
        role_title: str,
        role_classification: str = "agency_replaceable",
        is_agency_company: bool = False,
    ) -> str:
        """Assemble the final outreach draft from compliment, role title, and role classification."""
        has_compliment = compliment and compliment != "none"

        clean_role = _clean_role_title(role_title)
        a_role = _a_or_an(clean_role)

        if is_agency_company:
            if has_compliment:
                draft = OUTREACH_AGENCY_COMPANY_WITH_COMPLIMENT.format(compliment=compliment, a_role=a_role)
            else:
                draft = OUTREACH_AGENCY_COMPANY_WITHOUT_COMPLIMENT.format(a_role=a_role)
        elif role_classification == "agency_replaceable":
            if has_compliment:
                draft = OUTREACH_AGENCY_WITH_COMPLIMENT.format(compliment=compliment, a_role=a_role)
            else:
                draft = OUTREACH_AGENCY_WITHOUT_COMPLIMENT.format(a_role=a_role)
        else:
            if has_compliment:
                draft = OUTREACH_NON_AGENCY_WITH_COMPLIMENT.format(compliment=compliment, a_role=a_role)
            else:
                draft = OUTREACH_NON_AGENCY_WITHOUT_COMPLIMENT.format(a_role=a_role)

        return _strip_em_dashes(draft)
