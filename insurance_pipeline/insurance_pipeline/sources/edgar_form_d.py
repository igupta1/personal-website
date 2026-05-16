"""SEC EDGAR Form D filings source.

Form D is the federal filing for private securities offerings —
companies raising money under Reg D exemptions. For an independent
insurance agent, a fresh Form D means:
- D&O exposure (board now has fiduciary duty to investors)
- EPLI exposure (the company is hiring with fresh capital)
- Sometimes cyber + key-person life

Free, keyless, no Cloudflare. Hits EDGAR's ``getcurrent`` Atom feed
filtered to ``type=D``. SEC's fair-use policy asks scrapers to
identify with a contact email in the User-Agent.

Heavy noise filter: Form D is dominated by VC funds, partnerships,
and investment vehicles ("Acme Ventures III LP", "X Capital
Partners"). The ``_FINANCIAL_ENTITY_RE`` regex drops the obvious
filers so what surfaces is operating-company offerings only.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser

from insurance_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

_EDGAR_GETCURRENT_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=D&count=100&output=atom"
)
_USER_AGENT = (
    "ishaan-personal-website insurance-lead-magnet/0.1 (ishaangpta@g.ucla.edu)"
)

# Atom title format: "D - <Company Name> (cik) (Filer)"
_TITLE_RE = re.compile(
    r"^\s*D\s*-\s*(?P<name>.+?)\s*\(\d+\)\s*\(Filer\)\s*$",
    re.IGNORECASE,
)

# Drop obvious financial-vehicle names. These file Form D constantly
# and aren't insurance buyers (they invest, they don't operate).
_FINANCIAL_ENTITY_RE = re.compile(
    r"\b("
    r"venture[s]?|capital|partner[s]?|partnership|"
    r"holdings?|investors?|invest|investment[s]?|"
    r"reit|trust|"
    r"fund[s]?|funding|"
    r"opportunity|opportunities|"
    r"l\.?p\.?$|l\.?l\.?p\.?$|"
    r"asset\s+management|management\s+l\.?p\.?|"
    r"family\s+office"
    r")\b",
    re.IGNORECASE,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_rss_date(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _is_operating_company(name: str) -> bool:
    return not _FINANCIAL_ENTITY_RE.search(name)


def _extract_company_name(title: str) -> str | None:
    m = _TITLE_RE.match(title)
    if m is None:
        return None
    return m.group("name").strip() or None


def fetch(*, since: datetime, limit: int | None = None) -> list[LeadCandidate]:
    captured_at = _utcnow()

    try:
        # feedparser supports request_headers for User-Agent override.
        feed = feedparser.parse(_EDGAR_GETCURRENT_URL, request_headers={"User-Agent": _USER_AGENT})
    except Exception:
        _log.exception("edgar form-d fetch failed")
        return []

    candidates: list[LeadCandidate] = []
    for entry in feed.entries:
        # Form-type comes through as a category term; the feed sometimes
        # returns mixed types even with &type=D, so we re-filter here.
        terms = {t.get("term", "").upper() for t in (entry.get("tags") or [])}
        if "D" not in terms:
            # Also accept entries that match the title prefix (some
            # feedparser versions don't populate `tags` for atom).
            if not (entry.get("title") or "").strip().lower().startswith("d -"):
                continue

        title = (entry.get("title") or "").strip()
        company = _extract_company_name(title)
        if not company:
            continue
        if not _is_operating_company(company):
            _log.info("edgar: skipping financial-entity filer: %r", company)
            continue

        updated = entry.get("updated") or entry.get("published")
        updated_dt = _parse_rss_date(updated)
        if updated_dt and updated_dt < since:
            continue

        candidates.append(
            LeadCandidate(
                name=company,
                domain=None,
                initial_signal=Signal(
                    type=SignalType.FUNDING_RAISED,
                    source=SourceName.FUNDING,
                    captured_at=captured_at,
                    payload={
                        "title": title,
                        "filing_type": "Form D",
                        "filed_on": (updated_dt.date().isoformat()
                                     if updated_dt else ""),
                        "link": str(entry.get("link") or ""),
                    },
                ),
            )
        )

    if limit is not None:
        candidates = candidates[:limit]
    return candidates
