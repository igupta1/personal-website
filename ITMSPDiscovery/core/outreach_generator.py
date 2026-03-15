"""Personalized outreach draft generation using website scraping + Anthropic Claude for IT MSP pipeline."""

import asyncio
import hashlib
import json
import logging
import re
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from anthropic import AsyncAnthropic

logger = logging.getLogger(__name__)

# Common misspellings in IT job titles from postings
_ROLE_TYPO_MAP = {
    "adminstrator": "Administrator", "administator": "Administrator",
    "technican": "Technician", "technicain": "Technician",
    "cordinator": "Coordinator", "coodinator": "Coordinator",
    "coordinater": "Coordinator",
    "specalist": "Specialist", "specilaist": "Specialist",
    "managr": "Manager", "mananger": "Manager", "managment": "Management",
    "enginer": "Engineer", "direcor": "Director", "directr": "Director",
    "analst": "Analyst", "anaylst": "Analyst",
    "supervisr": "Supervisor", "assitant": "Assistant", "assisstant": "Assistant",
    "adminstration": "Administration",
}

SUMMARIZE_AND_COMPLIMENT_PROMPT = (
    "You're provided markdown scrapes of a company's homepage and up to 3 subpages. "
    "Do two things:\n\n"
    "1. Write a two-paragraph summary of what this company does, who they serve, and "
    "anything notable about their positioning, values, or recent activity.\n\n"
    "2. Based on that summary, generate a single compliment sentence about this company "
    "that could open a cold email.\n\n"
    'Return as JSON: {"summary": "your summary here", "compliment": "your compliment here"}.\n\n'
    "Summary rules: Be comprehensive but concise. Focus on specifics that make this company unique.\n\n"
    "Compliment rules:\n"
    "- MUST be under 20 words. Trim to the core insight. No filler.\n"
    "- Reference something specific and non-obvious from the summary.\n"
    "- Start with a varied, conversational opener. Rotate naturally among styles like:\n"
    '  "Was looking at your site and noticed..."\n'
    '  "Came across your [specific thing] and..."\n'
    '  "Saw that [company] recently..."\n'
    '  "Interesting approach with..."\n'
    '  "Cool that [company]..."\n'
    "  Do NOT always use the same opener. Each compliment should feel fresh.\n"
    "- Do NOT say generic things like 'Love your website' or 'Great company'.\n"
    "- Write in a matter-of-fact, peer-to-peer tone. Do NOT use gushing language like "
    "'It's genuinely impressive,' 'It's really cool,' 'It's fantastic how,' or "
    "'I was really impressed.' State the observation directly without filler praise.\n"
    "- Shorten company names where natural.\n"
    "- Do NOT use em dashes (\u2014) anywhere.\n\n"
    'If content is empty or unusable, return {"summary": "none", "compliment": "none"}'
)

_MSP_CLOSINGS = [
    "Before you commit to a full-time hire, would it be worth exploring whether "
    "a managed IT provider could handle that workload at a predictable monthly cost?",
    "Might be worth chatting with a managed IT provider before locking in a full-time salary "
    "and benefits package for this.",
    "Depending on scope, a managed IT provider might be able to cover this at a fraction of "
    "what a full-time hire would cost.",
    "Have you considered whether a managed IT provider could handle this without the overhead "
    "of a full-time hire?",
    "Worth asking whether this is a full-time role or something a managed IT provider could "
    "own for less.",
]

_NON_MSP_CLOSINGS = [
    "If your team is also stretched thin on day-to-day IT support, help desk, or security, "
    "that's something a managed provider could take off your plate while you focus on "
    "the strategic hire.",
    "If the day-to-day IT support side is also a gap, a managed provider could handle that "
    "while you build out the leadership team.",
    "While you're building out IT leadership, a managed provider could quietly handle the "
    "support workload without adding more headcount.",
]


def _pick_closing(variations: list, company_name: str) -> str:
    """Deterministically pick a closing variation based on company name hash."""
    idx = int(hashlib.md5(company_name.encode()).hexdigest(), 16) % len(variations)
    return variations[idx]


# Keywords for role classification (checked case-insensitively against role title)
_NOT_MSP_KEYWORDS = [
    "cio", "vp of technology", "vp technology", "chief information officer",
    "it architect", "it director", "director of it", "director of technology",
    "chief technology officer", "cto",
]

_MSP_KEYWORDS = [
    "it manager", "help desk", "systems administrator", "sysadmin",
    "network admin", "network administrator", "it support", "it technician",
    "it coordinator", "desktop support", "it specialist", "network engineer",
    "it associate", "it analyst", "tech support", "technical support",
]


def _classify_role(role_title: str) -> str:
    """Classify a role as msp_replaceable or not_msp_replaceable."""
    # Fix typos before classifying
    fixed = _fix_role_typos(role_title)
    lower = fixed.lower()
    for kw in _NOT_MSP_KEYWORDS:
        if kw in lower:
            return "not_msp_replaceable"
    return "msp_replaceable"


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
    "associate", "administrator", "technician", "engineer", "supervisor",
    "lead", "head", "vp", "chief", "officer", "assistant", "intern",
    "architect", "support", "consultant", "operator",
]


def _fix_role_typos(title: str) -> str:
    """Fix common misspellings in role titles from job postings."""
    words = title.split()
    fixed = []
    for word in words:
        # Check lowercase version against typo map
        lookup = word.lower().rstrip("s,.:;")
        if lookup in _ROLE_TYPO_MAP:
            # Preserve trailing chars (e.g. plural 's')
            suffix = word[len(lookup):]
            fixed.append(_ROLE_TYPO_MAP[lookup] + suffix)
        else:
            fixed.append(word)
    return " ".join(fixed)


def _clean_role_title(role_title: str) -> str:
    """Strip location, parentheticals, junk, and fix typos in role titles.

    Examples:
        "Systems Administrator(New York, NY)" -> "Systems Administrator"
        "Acme - IT - Help Desk Technician - Full Time" -> "Help Desk Technician"
        "Network Engineer- Enterprise" -> "Network Engineer"
        "IT Manager - Norton Shores Area" -> "IT Manager"
        "IT Support Specialist - Part time, .5 FTE" -> "IT Support Specialist"
        "Information TechnologyManager" -> "Information Technology Manager"
        "IT Adminstrator" -> "IT Administrator"
    """
    # Step 1: Remove parenthetical content
    cleaned = re.sub(r'\s*\(.*?\)\s*', '', role_title).strip()

    # Step 1b: Fix missing spaces before uppercase (e.g. "TechnologyManager")
    cleaned = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned)

    # Step 2: Split on " - " and find the role keyword segment
    segments = [s.strip() for s in cleaned.split(" - ") if s.strip()]
    if len(segments) >= 2:
        # Find segment containing a role keyword
        for seg in segments:
            seg_lower = seg.lower()
            if any(kw in seg_lower for kw in _ROLE_KEYWORDS):
                cleaned = seg
                break
        else:
            # No keyword match — use first segment as best guess
            cleaned = segments[0]

    # Step 3: Strip trailing "- Category" suffix, but protect hyphenated
    # compounds where the dash is part of the word
    if not re.search(r'\w-\w', cleaned):
        cleaned = re.sub(r'\s*-\s*.*$', '', cleaned).strip()

    # Step 4: Fix common typos
    cleaned = _fix_role_typos(cleaned)

    return cleaned if cleaned else role_title


def _a_or_an(role_title: str) -> str:
    """Return 'a' or 'an' based on whether the role title starts with a vowel sound."""
    word = role_title.lstrip().split()[0] if role_title.strip() else ""

    # Handle acronyms (all-uppercase words like IT, MSP, HR)
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

        Phase 1: Scrape all company websites in parallel (semaphore-limited).
        Phase 2: Batch summarize+compliment via Claude (3 companies per call).
        Phase 3: Assemble final drafts deterministically.

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

        # Phase 1: Parallel scraping
        print(f"  Scraping {len(companies)} company websites...")
        scraped = await self._scrape_all_companies(companies)

        # Phase 2: Batched summarization
        companies_with_text = [
            c for c in companies
            if scraped.get(c["company_name"]) and len(scraped[c["company_name"]].strip()) > 100
        ]
        companies_without_text = [
            c for c in companies if c not in companies_with_text
        ]

        summaries: Dict[str, tuple] = {}  # name -> (summary, compliment)

        if companies_with_text:
            print(f"  Summarizing {len(companies_with_text)} companies in batches...")
            summaries = await self._batch_summarize(companies_with_text, scraped)

        # Phase 3: Assemble drafts
        for company in companies:
            name = company["company_name"]
            roles = company.get("roles", [])
            role_title = roles[0] if roles else "IT role"
            role_class = _classify_role(role_title)

            summary, compliment = summaries.get(name, ("none", "none"))

            draft = self._assemble_draft(compliment, role_title, role_class, name)

            results[name] = {
                "summary": summary or "none",
                "compliment": compliment or "none",
                "outreach_draft": draft,
                "role_classification": role_class,
            }

        print(f"  Generated {len(results)} outreach drafts")
        return results

    async def _scrape_all_companies(
        self, companies: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Scrape all company websites in parallel with a concurrency limit."""
        semaphore = asyncio.Semaphore(5)
        scraped: Dict[str, str] = {}

        async def _scrape_one(company: Dict[str, Any]):
            name = company["company_name"]
            domain = company.get("domain", "")
            async with semaphore:
                try:
                    text = await self._scrape_company(domain)
                    scraped[name] = text
                except Exception as e:
                    logger.error(f"Scrape failed for {name}: {e}")
                    scraped[name] = ""

        await asyncio.gather(*[_scrape_one(c) for c in companies])
        return scraped

    async def _batch_summarize(
        self,
        companies: List[Dict[str, Any]],
        scraped: Dict[str, str],
    ) -> Dict[str, tuple]:
        """Summarize + generate compliments in batches of 3 companies per Claude call."""
        batch_size = 3
        results: Dict[str, tuple] = {}

        batches = [
            companies[i : i + batch_size]
            for i in range(0, len(companies), batch_size)
        ]

        for batch_idx, batch in enumerate(batches, 1):
            try:
                # Build multi-company prompt
                sections = []
                for c in batch:
                    name = c["company_name"]
                    text = scraped.get(name, "")
                    sections.append(f"=== COMPANY: {name} ===\n{text}")

                combined_text = "\n\n".join(sections)
                prompt = (
                    f"{SUMMARIZE_AND_COMPLIMENT_PROMPT}\n\n"
                    f"There are {len(batch)} companies below, separated by '=== COMPANY: name ===' markers. "
                    f"Return a JSON ARRAY with one object per company. Each object must have: "
                    f'"company_name", "summary", "compliment".\n\n---\n\n{combined_text}'
                )

                response = await self.client.messages.create(
                    model=self.model,
                    max_tokens=2048,
                    temperature=0.3,
                    messages=[{"role": "user", "content": prompt}],
                )
                raw_response = response.content[0].text

                # Parse array response
                parsed = self._try_parse_json_array(raw_response)
                if parsed:
                    batch_names = {c["company_name"] for c in batch}
                    for entry in parsed:
                        name = entry.get("company_name", "")
                        matched = self._match_company_name(name, batch_names)
                        if matched:
                            summary = entry.get("summary", "none")
                            compliment = entry.get("compliment", "none")
                            compliment = _strip_em_dashes(compliment) if compliment and compliment != "none" else "none"
                            results[matched] = (summary, compliment)

                # Fill in any missing companies from the batch
                for c in batch:
                    if c["company_name"] not in results:
                        results[c["company_name"]] = ("none", "none")

            except Exception as e:
                logger.error(f"Batch summarize failed for batch {batch_idx}: {e}")
                for c in batch:
                    if c["company_name"] not in results:
                        results[c["company_name"]] = ("none", "none")

        return results

    @staticmethod
    def _try_parse_json_array(raw_text: str) -> Optional[List[Dict]]:
        """Try to parse a JSON array from response text."""
        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            cleaned = "\n".join(lines[1:-1]).strip()
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        match = re.search(r"\[[\s\S]*\]", raw_text)
        if match:
            try:
                parsed = json.loads(match.group())
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
        # Fallback: try parsing as single object and wrap in array
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                return [parsed]
        except json.JSONDecodeError:
            pass
        return None

    @staticmethod
    def _match_company_name(name: str, candidates: set) -> Optional[str]:
        """Match company name from response to batch list."""
        if not name:
            return None
        name_lower = name.lower().strip()
        for candidate in candidates:
            if candidate.lower() == name_lower:
                return candidate
            if name_lower in candidate.lower() or candidate.lower() in name_lower:
                return candidate
        return None

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

    @staticmethod
    def _assemble_draft(
        compliment: str,
        role_title: str,
        role_classification: str = "msp_replaceable",
        company_name: str = "",
    ) -> str:
        """Assemble the final outreach draft from compliment, role title, and role classification."""
        has_compliment = compliment and compliment != "none"

        clean_role = _clean_role_title(role_title)
        a_role = _a_or_an(clean_role)

        if role_classification == "msp_replaceable":
            closing = _pick_closing(_MSP_CLOSINGS, company_name)
        else:
            closing = _pick_closing(_NON_MSP_CLOSINGS, company_name)

        opener = f"Noticed you're looking for {a_role}. "

        if has_compliment:
            draft = f"{compliment} {opener}{closing}"
        else:
            draft = f"{opener}{closing}"

        return _strip_em_dashes(draft)
