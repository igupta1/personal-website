"""Insurance-flavored fork of `sources/jobs.py`.

Same three job-board fetchers (JobSpy / Adzuna / HN Algolia), same
recruiter filter, same per-source isolation — only the query list and
the title classifier differ. Emits four insurance-buying SignalTypes:

- ``JOB_OPS_ROLE``     — office / operations / admin hires (renewal-handler turnover)
- ``JOB_BLUE_COLLAR``  — warehouse / production / construction (workers-comp trigger)
- ``JOB_FLEET_ROLE``   — drivers / dispatchers / fleet managers (commercial-auto trigger)
- ``JOB_FINANCE_OPS``  — CFO / controller / HR director (the actual buyer)

Fork rather than parameterize: the classifier regex shape is different
enough that a shared abstraction would be more noise than payoff for
v1. Revisit if a third jobs-source niche appears.
"""

import logging
import os
import re
from datetime import datetime, timezone

import jobspy
import requests

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

# Search-term list fed to all three APIs. Broader than the classifier
# patterns — we want the API to return plausibly-relevant rows that the
# classifier then categorizes (or drops as None).
_JOB_QUERIES = (
    "office manager",
    "operations coordinator",
    "warehouse worker",
    "production worker",
    "construction laborer",
    "CDL driver",
    "delivery driver",
    "fleet manager",
    "controller",
    "HR manager",
    "benefits manager",
)

# Classifier patterns. Order matters in `_classify_job_title`: finance
# execs first (the actual buyers), then fleet, then blue-collar, then
# the broader ops/admin bucket so "fleet manager" doesn't accidentally
# fall into JOB_OPS_ROLE via the "manager" suffix.

_FINANCE_OPS_PATTERN = re.compile(
    r"\b("
    r"cfo|chief\s+financial\s+officer|"
    r"controller|"
    r"director\s+of\s+finance|finance\s+director|vp\s+of\s+finance|"
    r"director\s+of\s+(?:hr|human\s+resources)|hr\s+director|"
    r"head\s+of\s+(?:people|hr|human\s+resources)|"
    r"(?:hr|human\s+resources|people)\s+(?:manager|business\s+partner)|"
    r"benefits\s+(?:manager|director|administrator)"
    r")\b",
    re.IGNORECASE,
)

_FLEET_PATTERN = re.compile(
    r"\b("
    r"cdl|owner[-\s]?operator|"
    r"(?:delivery|route|truck|otr|local|class\s+a|class\s+b)\s+driver|"
    r"driver\s+(?:cdl|class\s+a|class\s+b)|"
    r"fleet\s+(?:manager|coordinator|supervisor|director)|"
    r"dispatcher|dispatch\s+coordinator|"
    r"transportation\s+(?:manager|coordinator|supervisor)"
    r")\b",
    re.IGNORECASE,
)

_BLUE_COLLAR_PATTERN = re.compile(
    r"\b("
    r"warehouse\s+(?:worker|associate|operator|laborer|clerk)|"
    r"production\s+(?:worker|associate|operator|technician)|"
    r"manufacturing\s+(?:technician|operator|associate|worker)|"
    r"machine\s+operator|"
    r"general\s+laborer|construction\s+(?:laborer|worker)|"
    r"forklift\s+operator|"
    r"order\s+picker|picker[-\s]?packer|"
    r"assembler|assembly\s+(?:worker|operator|technician)"
    r")\b",
    re.IGNORECASE,
)

_OPS_ROLE_PATTERN = re.compile(
    r"\b("
    r"office\s+(?:manager|coordinator|administrator)|"
    r"operations\s+(?:coordinator|administrator|specialist|associate)|"
    r"administrative\s+(?:assistant|coordinator|specialist)|"
    r"executive\s+assistant|admin\s+assistant"
    r")\b",
    re.IGNORECASE,
)

_ADZUNA_API = "https://api.adzuna.com/v1/api/jobs/us/search/1"
_HN_ALGOLIA_API = "https://hn.algolia.com/api/v1/search_by_date"

_HN_COMPANY_PATTERN = re.compile(r"\s+at\s+(.+?)(?:\s*[-–|(]|\s*$)", re.IGNORECASE)

# Same recruiter-name filter as `sources/jobs.py` — staffing firms
# repost roles on behalf of unnamed clients, so the lead would be the
# recruiter, not the actual hiring company. Duplicated rather than
# imported across modules so each source stays self-contained.
_RECRUITER_NAME_PATTERN = re.compile(
    r"\b(staffing|recruit(?:ing|er|ers|ment)|"
    r"personnel\s+services?|talent\s+(?:group|agency|partners|solutions)|"
    r"\btalent$)",
    re.IGNORECASE,
)


def _is_recruiter_name(name: str) -> bool:
    return bool(_RECRUITER_NAME_PATTERN.search(name))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _classify_job_title(title: str) -> SignalType | None:
    # Order matters: finance execs first (the buyer), fleet before
    # blue-collar (so "delivery driver" doesn't get swept into BC by
    # a generic "worker" pattern), ops/admin last (broadest fallback).
    if _FINANCE_OPS_PATTERN.search(title):
        return SignalType.JOB_FINANCE_OPS
    if _FLEET_PATTERN.search(title):
        return SignalType.JOB_FLEET_ROLE
    if _BLUE_COLLAR_PATTERN.search(title):
        return SignalType.JOB_BLUE_COLLAR
    if _OPS_ROLE_PATTERN.search(title):
        return SignalType.JOB_OPS_ROLE
    return None


def _extract_company_from_hn_title(title: str) -> str | None:
    m = _HN_COMPANY_PATTERN.search(title)
    if m:
        return m.group(1).strip() or None
    return None


def _fetch_from_jobspy(since: datetime) -> list[LeadCandidate]:
    captured_at = _utcnow()
    hours_old = max(1, int((captured_at - since).total_seconds() / 3600))
    candidates: list[LeadCandidate] = []
    for query in _JOB_QUERIES:
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
            sig_type = _classify_job_title(title)
            if sig_type is None:
                continue
            if _is_recruiter_name(company):
                continue
            candidates.append(
                LeadCandidate(
                    name=company,
                    domain=None,
                    initial_signal=Signal(
                        type=sig_type,
                        source=SourceName.JOBS,
                        captured_at=captured_at,
                        payload={
                            "title": title,
                            "url": str(row.get("job_url") or ""),
                            "date_posted": str(row.get("date_posted") or ""),
                            "site": str(row.get("site") or ""),
                        },
                    ),
                )
            )
    return candidates


def _fetch_from_adzuna(since: datetime) -> list[LeadCandidate]:
    app_id = os.environ.get("ADZUNA_APP_ID")
    app_key = os.environ.get("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        _log.warning("ADZUNA_APP_ID/ADZUNA_APP_KEY not set; skipping Adzuna")
        return []

    captured_at = _utcnow()
    max_days_old = max(1, (captured_at - since).days)
    candidates: list[LeadCandidate] = []
    for query in _JOB_QUERIES:
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
            sig_type = _classify_job_title(title)
            if sig_type is None:
                continue
            if _is_recruiter_name(company):
                continue
            candidates.append(
                LeadCandidate(
                    name=company,
                    domain=None,
                    initial_signal=Signal(
                        type=sig_type,
                        source=SourceName.JOBS,
                        captured_at=captured_at,
                        payload={
                            "title": title,
                            "url": str(item.get("redirect_url") or ""),
                            "date_posted": str(item.get("created") or ""),
                            "site": "adzuna",
                        },
                    ),
                )
            )
    return candidates


def _fetch_from_hn(since: datetime) -> list[LeadCandidate]:
    captured_at = _utcnow()
    since_ts = int(since.replace(tzinfo=timezone.utc).timestamp())
    candidates: list[LeadCandidate] = []
    for query in _JOB_QUERIES:
        try:
            response = requests.get(
                _HN_ALGOLIA_API,
                params={
                    "query": query,
                    "tags": "story",
                    "numericFilters": f"created_at_i>{since_ts}",
                    "hitsPerPage": 20,
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except Exception:
            _log.exception("hn algolia query failed: %s", query)
            continue
        for hit in data.get("hits", []):
            title = str(hit.get("title") or "").strip()
            if not title:
                continue
            company = _extract_company_from_hn_title(title)
            if not company:
                continue
            sig_type = _classify_job_title(title)
            if sig_type is None:
                continue
            if _is_recruiter_name(company):
                continue
            candidates.append(
                LeadCandidate(
                    name=company,
                    domain=None,
                    initial_signal=Signal(
                        type=sig_type,
                        source=SourceName.JOBS,
                        captured_at=captured_at,
                        payload={
                            "title": title,
                            "url": str(hit.get("url") or ""),
                            "date_posted": str(hit.get("created_at") or ""),
                            "site": "hn_algolia",
                        },
                    ),
                )
            )
    return candidates


def fetch(*, since: datetime, limit: int | None = None) -> list[LeadCandidate]:
    candidates: list[LeadCandidate] = []
    fetchers = (
        ("jobspy", _fetch_from_jobspy),
        ("adzuna", _fetch_from_adzuna),
        ("hn", _fetch_from_hn),
    )
    for name, fetcher in fetchers:
        try:
            candidates.extend(fetcher(since))
        except Exception:
            _log.exception("fetcher %s failed entirely", name)
    if limit is not None:
        candidates = candidates[:limit]
    return candidates
