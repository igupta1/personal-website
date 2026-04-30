import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser
import requests

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

_SEC_EDGAR_API = "https://efts.sec.gov/LATEST/search-index"
_TECHCRUNCH_FEED = "https://techcrunch.com/category/startups/feed/"
_PRNEWSWIRE_FEED = (
    "https://www.prnewswire.com/rss/financial-services-latest-news/"
    "financial-services-latest-news-list.rss"
)

_HEADERS = {"User-Agent": "MSP Lead Magnet Pipeline (contact@example.com)"}


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


def _fetch_from_sec_edgar(since: datetime) -> list[LeadCandidate]:
    captured_at = _utcnow()
    candidates: list[LeadCandidate] = []
    try:
        response = requests.get(
            _SEC_EDGAR_API,
            params={
                "q": "",
                "dateRange": "custom",
                "startdt": since.date().isoformat(),
                "enddt": captured_at.date().isoformat(),
                "forms": "D",
            },
            headers=_HEADERS,
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        _log.exception("sec edgar fetch failed")
        return []

    for hit in data.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        names = source.get("display_names") or []
        if not names:
            continue
        company = str(names[0]).strip()
        if not company:
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
                        "form": "D",
                        "filing_date": str(source.get("file_date") or ""),
                        "accession": str(source.get("adsh") or ""),
                    },
                ),
            )
        )
    return candidates


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
        if not title:
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
                name=title,
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
        ("sec_edgar", _fetch_from_sec_edgar),
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
