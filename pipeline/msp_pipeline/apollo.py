"""Apollo.io decision-maker enrichment.

Three-step lookup per company:

1. ``organizations/enrich`` (free) by domain, with ``mixed_companies/search``
   by name as fallback — gets us Apollo's ``organization_id``.
2. ``mixed_people/api_search`` (free) filtered by DM-priority titles → list
   of obfuscated person records. We pick the best candidate by IT/security
   focus + seniority and grab their Apollo ``id``.
3. ``people/match`` by that ``id`` — returns the unlocked record (full name,
   verified work email, LinkedIn URL, location). This is the call that may
   consume a credit on some Apollo plans.

Required env var: ``APOLLO_API_KEY``. When unset, ``is_configured()`` returns
False and the daily-run stage skips silently — pipeline still works, just
falls back to the existing Gemini-derived ``dm_name`` / ``dm_title``.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import requests
from pydantic import BaseModel

log = logging.getLogger(__name__)

_BASE = "https://api.apollo.io/api/v1"
_TIMEOUT = 20

# Title filter sent to Apollo's people-search. OR-matched as substrings.
# Order doesn't matter to Apollo; our own ranking re-orders the response.
_DM_TITLES: tuple[str, ...] = (
    "Chief Information Officer", "CIO",
    "Chief Technology Officer", "CTO",
    "Chief Information Security Officer", "CISO",
    "VP of Information Technology", "VP of Technology", "Vice President of IT",
    "Director of Information Technology", "Director of IT", "IT Director",
    "Head of IT", "IT Manager", "Head of Engineering",
    # Small-business fallbacks — no tech exec exists, so the COO/founder is the buyer.
    "Chief Operating Officer", "COO",
    "President", "Owner",
    "Founder", "Co-Founder",
    "Chief Executive Officer", "CEO",
)


class Result(BaseModel):
    """Outcome of one Apollo lookup. ``org_found`` controls whether we mark
    the lead so we don't retry; ``dm_found`` controls whether we update the
    DM columns."""

    org_found: bool = False
    dm_found: bool = False
    dm_name: str | None = None
    dm_title: str | None = None
    dm_email: str | None = None
    dm_linkedin_url: str | None = None
    apollo_person_id: str | None = None
    headcount: int | None = None  # Apollo's estimated_num_employees, when present


def is_configured() -> bool:
    return bool(os.environ.get("APOLLO_API_KEY"))


def _headers(api_key: str) -> dict[str, str]:
    return {
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "Accept": "application/json",
    }


def _enrich_org_by_domain(api_key: str, domain: str) -> dict[str, Any] | None:
    try:
        r = requests.post(
            f"{_BASE}/organizations/enrich",
            params={"domain": domain},
            headers=_headers(api_key),
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("organization")
    except requests.RequestException as exc:
        log.warning("apollo enrich-by-domain %r failed: %s", domain, exc)
        return None


def _search_org_by_name(api_key: str, name: str) -> dict[str, Any] | None:
    try:
        r = requests.post(
            f"{_BASE}/mixed_companies/search",
            json={"q_organization_name": name, "page": 1, "per_page": 1},
            headers=_headers(api_key),
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        orgs = r.json().get("organizations") or []
        return orgs[0] if orgs else None
    except requests.RequestException as exc:
        log.warning("apollo search-by-name %r failed: %s", name, exc)
        return None


def _search_people(api_key: str, org_id: str) -> list[dict[str, Any]]:
    try:
        r = requests.post(
            f"{_BASE}/mixed_people/api_search",
            json={
                "organization_ids": [org_id],
                "person_titles": list(_DM_TITLES),
                "page": 1,
                "per_page": 5,
            },
            headers=_headers(api_key),
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("people") or []
    except requests.RequestException as exc:
        log.warning("apollo api_search org_id=%s failed: %s", org_id, exc)
        return []


def _match_person(api_key: str, person_id: str) -> dict[str, Any] | None:
    try:
        r = requests.post(
            f"{_BASE}/people/match",
            json={"id": person_id},
            headers=_headers(api_key),
            timeout=_TIMEOUT,
        )
        r.raise_for_status()
        return r.json().get("person")
    except requests.RequestException as exc:
        log.warning("apollo people/match id=%s failed: %s", person_id, exc)
        return None


# Seniority bucket → priority weight. Apollo's `seniority` field is a fixed
# enum; missing buckets fall to 0. Matches what we'd want at an SMB:
# c-suite/founder beats VP beats director beats manager.
_SENIORITY_WEIGHT: dict[str, int] = {
    "c_suite": 100,
    "founder": 90,
    "owner": 90,
    "partner": 70,
    "vp": 60,
    "head": 50,
    "director": 45,
    "manager": 30,
    "senior": 15,
}

# Long phrases — safe to substring-match, won't accidentally hit other words.
_IT_PHRASES: tuple[str, ...] = (
    "technology", "security", "information",
    "engineering", "infrastructure", "devops",
)

# Short abbreviations need word-boundary matching, otherwise "cto" hits
# "dire**cto**r" and "cio" hits "asso**cio**ate". Pre-compiled for speed.
_IT_ABBREV_RE = re.compile(r"\b(cto|cio|ciso|it)\b", re.IGNORECASE)


def _score_person(person: dict[str, Any]) -> int:
    """Higher = better DM candidate. Combines seniority bucket + IT focus."""
    title = (person.get("title") or "").lower()
    seniority = (person.get("seniority") or "").lower()
    score = _SENIORITY_WEIGHT.get(seniority, 0)
    if any(k in title for k in _IT_PHRASES) or _IT_ABBREV_RE.search(title):
        score += 100
    return score


def _pick_best(people: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not people:
        return None
    return max(people, key=_score_person)


def find_decision_maker(name: str, domain: str | None) -> Result:
    """Run the three-step Apollo lookup. Never raises on Apollo errors —
    returns an empty Result on any HTTP failure so the caller can decide
    whether to mark or retry."""
    api_key = os.environ.get("APOLLO_API_KEY")
    if not api_key:
        raise RuntimeError("APOLLO_API_KEY not set")

    org = None
    if domain:
        org = _enrich_org_by_domain(api_key, domain)
    if not org:
        org = _search_org_by_name(api_key, name)
    if not org or not org.get("id"):
        return Result()

    org_id = str(org["id"])
    headcount = org.get("estimated_num_employees")
    if isinstance(headcount, str):
        try:
            headcount = int(headcount)
        except ValueError:
            headcount = None

    people = _search_people(api_key, org_id)
    chosen = _pick_best(people)
    if chosen is None or not chosen.get("id"):
        return Result(org_found=True, headcount=headcount)

    person_id = str(chosen["id"])
    matched = _match_person(api_key, person_id)
    if matched is None:
        # Match failed (network / quota). Don't mark dm_found — we'd retry
        # next night. But we did find the org, so headcount is still useful.
        return Result(
            org_found=True,
            apollo_person_id=person_id,
            headcount=headcount,
        )

    full_name = matched.get("name")
    title = matched.get("title") or chosen.get("title")
    email = matched.get("email")
    linkedin_url = matched.get("linkedin_url")

    return Result(
        org_found=True,
        dm_found=bool(full_name),
        dm_name=full_name,
        dm_title=title,
        dm_email=email,
        dm_linkedin_url=linkedin_url,
        apollo_person_id=person_id,
        headcount=headcount,
    )
