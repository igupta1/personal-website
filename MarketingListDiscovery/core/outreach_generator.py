"""Personalized outreach draft generation using website scraping + Gemini."""

import asyncio
import json
import logging
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

SUMMARIZE_PROMPT = (
    "You're provided markdown scrapes of a company's homepage and up to 3 subpages. "
    "Write a two-paragraph summary of what this company does, who they serve, and "
    "anything notable about their positioning, values, or recent activity. "
    'Return as JSON: {"summary": "your summary here"}. '
    "Rules: Be comprehensive but concise. Focus on specifics that make this company unique. "
    'If content is empty or unusable, return {"summary": "none"}'
)

COMPLIMENT_PROMPT = (
    "You're a sales copywriter. Given a company summary, generate a single compliment "
    "sentence about this company that could open a cold email. "
    'Return as JSON: {"compliment": "your compliment here"}. '
    "Rules: One sentence only. Reference something specific and non-obvious from the summary. "
    "Do NOT say generic things like 'Love your website' or 'Great company'. "
    "Write in a matter-of-fact, peer-to-peer tone. Do NOT use gushing language like "
    "'It's genuinely impressive,' 'It's really cool,' 'It's fantastic how,' or "
    "'I was really impressed.' State the observation directly without filler praise. "
    "For example, instead of 'It's genuinely impressive how EXL achieves a 90% success rate' "
    "write 'EXL's 90% success rate on enterprise AI initiatives is a strong proof point.' "
    "Shorten company names where natural. "
    "Do NOT use em dashes (\u2014) anywhere. "
    'If the summary is \'none\', return {"compliment": "none"}'
)

OUTREACH_AGENCY_WITH_COMPLIMENT = (
    "{compliment} Noticed you're looking for a {role}. Before you commit to a "
    "full-time hire, would it be worth exploring whether an agency could deliver "
    "the same results at a fraction of the cost?"
)

OUTREACH_AGENCY_WITHOUT_COMPLIMENT = (
    "Noticed you're looking for a {role}. Before you commit to a full-time hire, "
    "would it be worth exploring whether an agency could deliver the same results "
    "at a fraction of the cost?"
)

OUTREACH_NON_AGENCY_WITH_COMPLIMENT = (
    "{compliment} Noticed you're looking for a {role}. If your team is also "
    "stretched thin on digital marketing or content, that's something an agency "
    "could take off your plate while you focus on hiring for the in-person side."
)

OUTREACH_NON_AGENCY_WITHOUT_COMPLIMENT = (
    "Noticed you're looking for a {role}. If your team is also stretched thin on "
    "digital marketing or content, that's something an agency could take off your "
    "plate while you focus on hiring for the in-person side."
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


def _strip_em_dashes(text: str) -> str:
    """Replace em dashes and en dashes with ' - '."""
    return text.replace("\u2014", " - ").replace("\u2013", " - ")


class OutreachGenerator:
    """Generate personalized outreach drafts by scraping company websites and using Gemini."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "gemini-2.5-flash",
    ):
        self.model = model
        client_kwargs = {}
        if api_key:
            client_kwargs["api_key"] = api_key
        self.client = genai.Client(**client_kwargs)

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
                    print(f"  [{i+1}/{len(companies)}] {name}: summarizing...")
                    summary = await self._summarize(raw_text)
                else:
                    summary = "none"

                if summary and summary != "none":
                    print(f"  [{i+1}/{len(companies)}] {name}: generating compliment...")
                    compliment = await self._generate_compliment(summary)
                else:
                    compliment = "none"

                draft = self._assemble_draft(compliment, role_title, role_class)

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
        headers = {"User-Agent": BROWSER_USER_AGENT}

        async with httpx.AsyncClient(
            headers=headers, timeout=10.0, follow_redirects=True
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

    async def _summarize(self, raw_text: str) -> str:
        """Summarize company website content via Gemini."""
        prompt = f"{SUMMARIZE_PROMPT}\n\n---\n\n{raw_text}"
        config = types.GenerateContentConfig(temperature=0.3)

        try:
            response = await self.client.aio.models.generate_content(
                model=self.model,
                contents=prompt,
                config=config,
            )
            if not response.text:
                return "none"
            return self._parse_json_field(response.text, "summary")
        except Exception as e:
            logger.error(f"Summarize call failed: {e}")
            return "none"

    async def _generate_compliment(self, summary: str) -> str:
        """Generate a compliment sentence from the company summary via Gemini."""
        prompt = f"{COMPLIMENT_PROMPT}\n\nCompany summary:\n{summary}"
        config = types.GenerateContentConfig(temperature=0.3)

        try:
            response = await self.client.aio.models.generate_content(
                model=self.model,
                contents=prompt,
                config=config,
            )
            if not response.text:
                return "none"
            compliment = self._parse_json_field(response.text, "compliment")
            return _strip_em_dashes(compliment) if compliment else "none"
        except Exception as e:
            logger.error(f"Compliment call failed: {e}")
            return "none"

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
    def _assemble_draft(compliment: str, role_title: str, role_classification: str = "agency_replaceable") -> str:
        """Assemble the final outreach draft from compliment, role title, and role classification."""
        has_compliment = compliment and compliment != "none"
        is_agency = role_classification == "agency_replaceable"

        if is_agency and has_compliment:
            draft = OUTREACH_AGENCY_WITH_COMPLIMENT.format(compliment=compliment, role=role_title)
        elif is_agency:
            draft = OUTREACH_AGENCY_WITHOUT_COMPLIMENT.format(role=role_title)
        elif has_compliment:
            draft = OUTREACH_NON_AGENCY_WITH_COMPLIMENT.format(compliment=compliment, role=role_title)
        else:
            draft = OUTREACH_NON_AGENCY_WITHOUT_COMPLIMENT.format(role=role_title)

        return _strip_em_dashes(draft)
