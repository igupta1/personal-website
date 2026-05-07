import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

_TECHCRUNCH_FEED = "https://techcrunch.com/category/startups/feed/"
_PRNEWSWIRE_FEED = (
    "https://www.prnewswire.com/rss/financial-services-latest-news/"
    "financial-services-latest-news-list.rss"
)

# Title-pattern filter for RSS sources. The PR Newswire financial-services
# feed mixes funding announcements with class-action notices, regulatory
# filings, M&A, etc. — only let through titles that look like actual
# funding events.
_FUNDING_TITLE_PATTERN = re.compile(
    r"\b(raise[sd]?|secur(?:es|ed)|clos(?:es|ed)|"
    r"funding|series\s+[a-h]\b|seed(?:\s+round)?\b|"
    r"investment|round|backed|capital\s+from)\b",
    re.IGNORECASE,
)
_NON_FUNDING_PATTERN = re.compile(
    r"\b(class\s+action|lawsuit|investigation|encourag(?:es|ing)|"
    r"shareholder|inquire|complaint|fraud|securities\s+fraud|"
    r"recall(?:ed|s)?|reminder)\b",
    re.IGNORECASE,
)

# Strip "(CIK 0001234567)" suffixes that some upstream feeds attach to
# company names. Defensive — the SEC EDGAR fetcher that historically emitted
# these has been removed, but the helper costs nothing and protects against
# any future feed leaking similar markers.
_CIK_SUFFIX = re.compile(r"\s*\(CIK\s+\d+\)\s*$", re.IGNORECASE)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_rss_date(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _clean_company_name(name: str) -> str:
    return _CIK_SUFFIX.sub("", name).strip()


def _is_funding_title(title: str) -> bool:
    """True if a feed-entry title looks like a funding announcement."""
    if not title:
        return False
    if _NON_FUNDING_PATTERN.search(title):
        return False
    return bool(_FUNDING_TITLE_PATTERN.search(title))


def _fetch_from_rss(feed_url: str, since: datetime) -> list[LeadCandidate]:
    captured_at = _utcnow()
    candidates: list[LeadCandidate] = []
    try:
        feed = feedparser.parse(feed_url)
    except Exception:
        _log.exception("rss fetch failed: %s", feed_url)
        return []
    for entry in feed.entries:
        title = str(entry.get("title") or "").strip()
        if not _is_funding_title(title):
            continue
        published = entry.get("published")
        published_dt = _parse_rss_date(published)
        if published_dt and published_dt < since:
            continue
        # Per M2 plan: feed-entry title used verbatim as company name in v1.
        # M4 enrichment (with llm.py) will extract the actual company and
        # merge duplicates.
        candidates.append(
            LeadCandidate(
                name=_clean_company_name(title),
                domain=None,
                initial_signal=Signal(
                    type=SignalType.FUNDING_RAISED,
                    source=SourceName.FUNDING,
                    captured_at=captured_at,
                    payload={
                        "feed_title": title,
                        "feed_url": feed_url,
                        "link": str(entry.get("link") or ""),
                        "published": published or "",
                    },
                ),
            )
        )
    return candidates


def _fetch_from_techcrunch(since: datetime) -> list[LeadCandidate]:
    return _fetch_from_rss(_TECHCRUNCH_FEED, since)


def _fetch_from_prnewswire(since: datetime) -> list[LeadCandidate]:
    return _fetch_from_rss(_PRNEWSWIRE_FEED, since)


def fetch(*, since: datetime, limit: int | None = None) -> list[LeadCandidate]:
    candidates: list[LeadCandidate] = []
    fetchers = (
        ("techcrunch", _fetch_from_techcrunch),
        ("prnewswire", _fetch_from_prnewswire),
    )
    for name, fetcher in fetchers:
        try:
            candidates.extend(fetcher(since))
        except Exception:
            _log.exception("fetcher %s failed entirely", name)
    if limit is not None:
        candidates = candidates[:limit]
    return candidates
