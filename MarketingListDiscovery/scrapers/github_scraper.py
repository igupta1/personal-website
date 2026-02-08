"""GitHub README scraper for jobright-ai marketing new grad listings."""

import re
import logging
from datetime import date, datetime
from typing import List, Optional, Sequence
from urllib.parse import urlparse

import httpx

from ..core.models import GitHubListing

logger = logging.getLogger(__name__)

# Regex to extract markdown links: **[Text](URL)**
BOLD_LINK_RE = re.compile(r'\*\*\[(.+?)\]\((.+?)\)\*\*')


class GitHubReadmeScraper:
    """Scrapes job listings from the jobright-ai GitHub README."""

    def __init__(self, repo: str = "jobright-ai/2026-Marketing-New-Grad"):
        self.repo = repo
        self.api_url = f"https://api.github.com/repos/{repo}/readme"

    async def fetch_todays_listings(
        self,
        client: httpx.AsyncClient,
        target_date: Optional[date] = None,
    ) -> List[GitHubListing]:
        """Fetch and parse listings for a specific date (defaults to today)."""
        if target_date is None:
            target_date = date.today()

        readme_content = await self._fetch_readme(client)
        if not readme_content:
            return []

        all_listings = self._parse_table(readme_content)
        filtered = [l for l in all_listings if l.date_posted == target_date]

        logger.info(
            f"Parsed {len(all_listings)} total listings, "
            f"{len(filtered)} for {target_date.isoformat()}"
        )
        return filtered

    async def fetch_all_listings(
        self,
        client: httpx.AsyncClient,
    ) -> List[GitHubListing]:
        """Fetch and parse all listings currently in the README (last ~7 days)."""
        readme_content = await self._fetch_readme(client)
        if not readme_content:
            return []

        all_listings = self._parse_table(readme_content)
        logger.info(f"Parsed {len(all_listings)} total listings")
        return all_listings

    async def _fetch_readme(self, client: httpx.AsyncClient) -> Optional[str]:
        """Fetch the raw README content from GitHub API."""
        try:
            response = await client.get(
                self.api_url,
                headers={
                    "Accept": "application/vnd.github.raw+json",
                    "User-Agent": "MarketingListDiscovery/1.0",
                },
            )
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as e:
            logger.error(f"GitHub API error: {e.response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch README: {e}")
            return None

    def _parse_table(self, content: str) -> List[GitHubListing]:
        """Parse the markdown table from README content."""
        lines = content.split("\n")

        # Find table boundaries
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if "TABLE_START" in line:
                start_idx = i
            elif "TABLE_END" in line:
                end_idx = i
                break

        if start_idx is None or end_idx is None:
            logger.error("Could not find TABLE_START/TABLE_END markers")
            return []

        # Extract table rows (skip header, separator, and blank lines)
        table_lines = []
        for line in lines[start_idx + 1 : end_idx]:
            stripped = line.strip()
            if (
                stripped
                and stripped.startswith("|")
                and "Company" not in stripped
                and "-----" not in stripped
            ):
                table_lines.append(stripped)

        listings = []
        current_company_name = None
        current_company_url = None
        current_company_domain = None

        for line in table_lines:
            parsed = self._parse_row(
                line, current_company_name, current_company_url, current_company_domain
            )
            if parsed:
                listings.append(parsed)
                current_company_name = parsed.company_name
                current_company_url = parsed.company_url
                current_company_domain = parsed.company_domain

        return listings

    def _split_table_row(self, line: str) -> List[str]:
        """Split a markdown table row by | while respecting markdown links.

        Markdown links like [text](url) can contain | inside them,
        so we can't naively split by |. Instead we track bracket depth.
        """
        cells = []
        current = []
        bracket_depth = 0
        paren_depth = 0

        # Strip leading/trailing |
        line = line.strip()
        if line.startswith("|"):
            line = line[1:]
        if line.endswith("|"):
            line = line[:-1]

        for ch in line:
            if ch == "[":
                bracket_depth += 1
                current.append(ch)
            elif ch == "]":
                bracket_depth -= 1
                current.append(ch)
            elif ch == "(":
                paren_depth += 1
                current.append(ch)
            elif ch == ")":
                paren_depth -= 1
                current.append(ch)
            elif ch == "|" and bracket_depth == 0 and paren_depth == 0:
                cells.append("".join(current).strip())
                current = []
            else:
                current.append(ch)

        # Don't forget the last cell
        if current:
            cells.append("".join(current).strip())

        return cells

    def _parse_row(
        self,
        line: str,
        prev_company_name: Optional[str],
        prev_company_url: Optional[str],
        prev_company_domain: Optional[str],
    ) -> Optional[GitHubListing]:
        """Parse a single table row into a GitHubListing."""
        cells = self._split_table_row(line)

        if len(cells) < 5:
            logger.debug(f"Skipping row with {len(cells)} cells: {line[:80]}")
            return None

        company_cell = cells[0]
        job_cell = cells[1]
        location = cells[2]
        work_model = cells[3]
        date_str = cells[4]

        # Parse company (or use previous for ↳ rows)
        if "↳" in company_cell:
            company_name = prev_company_name
            company_url = prev_company_url
            company_domain = prev_company_domain
        else:
            match = BOLD_LINK_RE.search(company_cell)
            if match:
                company_name = match.group(1)
                company_url = match.group(2)
                company_domain = self._extract_domain(company_url)
            else:
                # Plain text company name (no link)
                company_name = company_cell.replace("**", "").strip()
                company_url = ""
                company_domain = ""

        if not company_name or not company_domain:
            logger.debug(f"Skipping row with no company/domain: {line[:80]}")
            return None

        # Parse job title and URL
        job_match = BOLD_LINK_RE.search(job_cell)
        if job_match:
            job_title = job_match.group(1)
            job_url = job_match.group(2)
        else:
            job_title = job_cell.replace("**", "").strip()
            job_url = ""

        # Parse date
        date_posted = self._parse_date(date_str)
        if not date_posted:
            logger.debug(f"Skipping row with unparseable date '{date_str}': {line[:80]}")
            return None

        return GitHubListing(
            company_name=company_name,
            company_url=company_url,
            company_domain=company_domain,
            job_title=job_title,
            job_url=job_url,
            location=location,
            work_model=work_model,
            date_posted=date_posted,
        )

    # Domains that are not actual company websites
    SKIP_DOMAINS = {"linkedin.com", "github.com", "twitter.com", "facebook.com"}

    def _extract_domain(self, url: str) -> str:
        """Extract domain from a company URL.

        Returns empty string for social media profiles (LinkedIn, etc.)
        since those can't be used for ATS detection.
        """
        if not url:
            return ""
        # Ensure URL has a scheme
        if not url.startswith("http"):
            url = f"https://{url}"
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace("www.", "")
            # Skip social media profiles — not real company domains
            if domain in self.SKIP_DOMAINS:
                return ""
            return domain
        except Exception:
            return ""

    def _parse_date(self, date_str: str) -> Optional[date]:
        """Parse date string like 'Feb 07' into a date object.

        Assumes current year. Handles Dec->Jan rollover by checking if
        the parsed date is more than 30 days in the future.
        """
        date_str = date_str.strip()
        if not date_str:
            return None

        current_year = date.today().year
        try:
            parsed = datetime.strptime(f"{date_str} {current_year}", "%b %d %Y")
            result = parsed.date()

            # Handle year rollover: if the date is >30 days in the future,
            # it's likely from last year (e.g., "Dec 28" parsed in January)
            days_ahead = (result - date.today()).days
            if days_ahead > 30:
                result = result.replace(year=current_year - 1)

            return result
        except ValueError:
            return None
