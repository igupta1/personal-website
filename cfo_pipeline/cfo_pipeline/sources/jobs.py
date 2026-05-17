"""Job-board source for the CFO pipeline.

Two outputs from one fetch:

1. ``LeadCandidate`` rows with ``JOB_POSTED_FINANCE_LEAD`` — the
   primary buying signal. Generated when the posting title is for a
   role one rung below CFO (Controller, VP / Head / Director of
   Finance, Accounting / Finance Manager).
2. ``Disqualifier`` rows — generated when the posting title is for a
   *full-time* Chief Financial Officer. Those companies are buying a
   CFO, not a fractional one, so per spec they're dropped entirely
   from the dashboard. The disqualifier is sticky: a CFO posting on
   day 1 still blocks a Form D filing on day 10 (via
   ``db.disqualified``).

Where headcount is available from the job posting (Indeed's
``company_num_employees`` field, surfaced by JobSpy as the same name
or as ``company_employees_label``), it's carried on the candidate so
the SMB cap can short-circuit enrichment for obviously-oversized
companies.

Backends: JobSpy (Indeed + LinkedIn scrape) + Adzuna API. No HN —
Hacker News' Who's Hiring threads index tech roles, which is the
wrong demographic for fractional-CFO buyers.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import jobspy
import requests

from cfo_pipeline.models import (
    Disqualifier,
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

# Search queries. The Controller / VP-Finance set is the primary buy
# signal; "Chief Financial Officer" is queried separately so we can
# write disqualifiers for those companies. "CFO" alone matches noisy
# titles ("Chief Marketing Officer (CMO)" can hit on substring engines)
# so we use the spelled-out form on the query side.
_FINANCE_LEAD_QUERIES: tuple[str, ...] = (
    "Controller",
    "VP Finance",
    "Head of Finance",
    "Director of Finance",
    "Accounting Manager",
    "Finance Manager",
)
_CFO_QUERIES: tuple[str, ...] = (
    "Chief Financial Officer",
)

# Title classifier. Title text comes back messy ("Senior Controller,
# Manufacturing — Greater Boston (Remote)") so we use word-boundary
# regexes rather than exact matches.
_CONTROLLER_RE = re.compile(r"\b(controller|comptroller)\b", re.IGNORECASE)
_VP_FINANCE_RE = re.compile(
    r"\b(?:vp|vice\s+president|head|director)\s+of\s+finance\b",
    re.IGNORECASE,
)
_VP_FINANCE_ALT_RE = re.compile(
    r"\b(?:vp|vice\s+president)\s+finance\b",
    re.IGNORECASE,
)
_FINANCE_DIRECTOR_RE = re.compile(
    r"\bfinance\s+director\b",
    re.IGNORECASE,
)
_FINANCE_MANAGER_RE = re.compile(
    r"\b(?:accounting|finance)\s+manager\b",
    re.IGNORECASE,
)
_HEAD_OF_ACCOUNTING_RE = re.compile(
    r"\b(?:head|director)\s+of\s+accounting\b",
    re.IGNORECASE,
)

# CFO disqualifier. Detects "Chief Financial Officer" or stand-alone
# "CFO" (as a word, not a substring) — but excludes part-time variants
# explicitly. A company hiring a Fractional / Interim / Part-time CFO
# is the opposite of disqualified; they're already in market for
# what's being sold.
_CFO_TITLE_RE = re.compile(
    r"\b(chief\s+financial\s+officer|cfo)\b",
    re.IGNORECASE,
)
_PART_TIME_QUALIFIER_RE = re.compile(
    r"\b(fractional|interim|part[\s-]?time|outsourced|contract|temp|temporary|consultant|consulting|advisory)\b",
    re.IGNORECASE,
)

# Recruiter / staffing firm names — same problem the MSP pipeline
# hits. A "Robert Half" job posting for a Controller is a posting on
# BEHALF of an unnamed client; the lead would be the staffing firm,
# not the actual hiring company. Useless for outreach.
_RECRUITER_NAME_PATTERN = re.compile(
    r"\b(staffing|recruit(?:ing|er|ers|ment)|"
    r"personnel\s+services?|talent\s+(?:group|agency|partners|solutions)|"
    r"\btalent$|robert\s+half|aerotek|kelly\s+services|adecco|"
    r"randstad|manpower|teksystems|insight\s+global)",
    re.IGNORECASE,
)

# Indeed's company_employees_label / JobSpy's company_num_employees
# returns strings like "1 to 10", "11 to 50", "201 to 500", "10,001+".
# Parse to an integer upper bound; None when unparseable.
_HEADCOUNT_BAND_RE = re.compile(
    r"^\s*(\d[\d,]*)\s*(?:to|-|–)\s*(\d[\d,]*)\s*\+?\s*$",
    re.IGNORECASE,
)
_HEADCOUNT_PLUS_RE = re.compile(
    r"^\s*(\d[\d,]*)\s*\+\s*$",
)

_ADZUNA_API = "https://api.adzuna.com/v1/api/jobs/us/search/1"


def _is_recruiter_name(name: str) -> bool:
    return bool(_RECRUITER_NAME_PATTERN.search(name))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _is_finance_lead_title(title: str) -> bool:
    """True when the posting is for a finance lead one rung below CFO.
    Always called AFTER ``_is_cfo_disqualifier_title`` returns False,
    so we don't double-classify CFO postings as both signal and
    disqualifier (a "VP Finance and CFO" posting hits the CFO branch
    first)."""
    if not title:
        return False
    return bool(
        _CONTROLLER_RE.search(title)
        or _VP_FINANCE_RE.search(title)
        or _VP_FINANCE_ALT_RE.search(title)
        or _FINANCE_DIRECTOR_RE.search(title)
        or _FINANCE_MANAGER_RE.search(title)
        or _HEAD_OF_ACCOUNTING_RE.search(title)
    )


def _is_cfo_disqualifier_title(title: str) -> bool:
    """True when the title is for a full-time CFO. False when it's a
    fractional / interim / part-time variant — those companies are
    explicitly the buyer of what's being sold, not disqualified."""
    if not title:
        return False
    if not _CFO_TITLE_RE.search(title):
        return False
    if _PART_TIME_QUALIFIER_RE.search(title):
        return False
    return True


def _parse_headcount_label(label: str | None) -> int | None:
    """Parse Indeed's company size band into an integer upper bound.
    Returns None when the label is unknown / unparseable. We pick the
    upper bound (rather than midpoint) so the SMB cap stays conservative
    — a "11 to 50" band returns 50, which is exactly the cap line in
    the spec."""
    if not label:
        return None
    s = str(label).strip()
    if not s or s.lower() in ("unknown", "n/a", "none"):
        return None

    m = _HEADCOUNT_BAND_RE.match(s)
    if m:
        try:
            return int(m.group(2).replace(",", ""))
        except ValueError:
            return None

    m = _HEADCOUNT_PLUS_RE.match(s)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            return None

    # Plain integer string.
    try:
        return int(s.replace(",", ""))
    except ValueError:
        return None


def _read_jobspy_headcount(row: Any) -> int | None:
    """JobSpy versions vary on the field name; check both."""
    for field in ("company_num_employees", "company_employees_label", "company_size"):
        val = row.get(field) if hasattr(row, "get") else None
        if val is not None and str(val) and str(val).lower() != "nan":
            parsed = _parse_headcount_label(str(val))
            if parsed is not None:
                return parsed
    return None


def _make_finance_candidate(
    *,
    company: str,
    title: str,
    url: str,
    date_posted: str,
    site: str,
    captured_at: datetime,
    headcount: int | None,
) -> LeadCandidate:
    return LeadCandidate(
        name=company,
        domain=None,
        headcount=headcount,
        initial_signal=Signal(
            type=SignalType.JOB_POSTED_FINANCE_LEAD,
            source=SourceName.JOBS,
            captured_at=captured_at,
            payload={
                "title": title,
                "url": url,
                "date_posted": date_posted,
                "site": site,
            },
        ),
    )


def _make_cfo_disqualifier(
    *, company: str, title: str, site: str, url: str
) -> Disqualifier:
    return Disqualifier(
        name=company,
        reason="open_full_time_cfo_posting",
        source=SourceName.JOBS,
        payload={"title": title, "site": site, "url": url},
    )


def _fetch_from_jobspy(
    since: datetime, *, queries: tuple[str, ...]
) -> tuple[list[LeadCandidate], list[Disqualifier]]:
    captured_at = _utcnow()
    hours_old = max(1, int((captured_at - since).total_seconds() / 3600))
    candidates: list[LeadCandidate] = []
    disqualifiers: list[Disqualifier] = []
    for query in queries:
        try:
            df = jobspy.scrape_jobs(
                site_name=["indeed", "linkedin"],
                search_term=query,
                location="United States",
                results_wanted=10,
                hours_old=hours_old,
                country_indeed="usa",
            )
        except Exception:
            _log.exception("jobspy query failed: %s", query)
            continue
        if df is None or len(df) == 0:
            continue
        for _, row in df.iterrows():
            title = str(row.get("title") or "").strip()
            company = str(row.get("company") or "").strip()
            if not title or not company:
                continue
            if _is_recruiter_name(company):
                continue

            url = str(row.get("job_url") or "")
            date_posted = str(row.get("date_posted") or "")
            site = str(row.get("site") or "")

            if _is_cfo_disqualifier_title(title):
                disqualifiers.append(
                    _make_cfo_disqualifier(
                        company=company, title=title, site=site, url=url,
                    )
                )
                continue
            if _is_finance_lead_title(title):
                headcount = _read_jobspy_headcount(row)
                candidates.append(
                    _make_finance_candidate(
                        company=company,
                        title=title,
                        url=url,
                        date_posted=date_posted,
                        site=site,
                        captured_at=captured_at,
                        headcount=headcount,
                    )
                )
    return candidates, disqualifiers


def _fetch_from_adzuna(
    since: datetime, *, queries: tuple[str, ...]
) -> tuple[list[LeadCandidate], list[Disqualifier]]:
    app_id = os.environ.get("ADZUNA_APP_ID")
    app_key = os.environ.get("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        _log.warning("ADZUNA_APP_ID/ADZUNA_APP_KEY not set; skipping Adzuna")
        return [], []

    captured_at = _utcnow()
    max_days_old = max(1, (captured_at - since).days)
    candidates: list[LeadCandidate] = []
    disqualifiers: list[Disqualifier] = []
    for query in queries:
        try:
            response = requests.get(
                _ADZUNA_API,
                params={
                    "app_id": app_id,
                    "app_key": app_key,
                    "what": query,
                    "max_days_old": max_days_old,
                    "results_per_page": 20,
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except Exception:
            _log.exception("adzuna query failed: %s", query)
            continue
        for item in data.get("results", []):
            title = str(item.get("title") or "").strip()
            company = str((item.get("company") or {}).get("display_name") or "").strip()
            if not title or not company:
                continue
            if _is_recruiter_name(company):
                continue

            url = str(item.get("redirect_url") or "")
            date_posted = str(item.get("created") or "")

            if _is_cfo_disqualifier_title(title):
                disqualifiers.append(
                    _make_cfo_disqualifier(
                        company=company, title=title, site="adzuna", url=url,
                    )
                )
                continue
            if _is_finance_lead_title(title):
                candidates.append(
                    _make_finance_candidate(
                        company=company,
                        title=title,
                        url=url,
                        date_posted=date_posted,
                        site="adzuna",
                        captured_at=captured_at,
                        headcount=None,  # Adzuna doesn't expose a size field.
                    )
                )
    return candidates, disqualifiers


def fetch(
    *, since: datetime, limit: int | None = None
) -> tuple[list[LeadCandidate], list[Disqualifier]]:
    """Returns (finance-lead candidates, CFO disqualifiers).

    Two-return signature differs from the funding/edgar sources because
    the jobs source is the only one that produces disqualifiers. The
    daily_run runner branches on the return shape.
    """
    candidates: list[LeadCandidate] = []
    disqualifiers: list[Disqualifier] = []

    fetchers: tuple[
        tuple[str, Any, tuple[str, ...]], ...
    ] = (
        ("jobspy_finance_leads", _fetch_from_jobspy, _FINANCE_LEAD_QUERIES),
        ("jobspy_cfo_disqualifiers", _fetch_from_jobspy, _CFO_QUERIES),
        ("adzuna_finance_leads", _fetch_from_adzuna, _FINANCE_LEAD_QUERIES),
        ("adzuna_cfo_disqualifiers", _fetch_from_adzuna, _CFO_QUERIES),
    )
    for name, fetcher, queries in fetchers:
        try:
            c, d = fetcher(since, queries=queries)
            candidates.extend(c)
            disqualifiers.extend(d)
        except Exception:
            _log.exception("fetcher %s failed entirely", name)

    if limit is not None:
        candidates = candidates[:limit]
    return candidates, disqualifiers
