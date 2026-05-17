"""Apollo.io decision-maker enrichment for the CFO pipeline.

Three-step lookup (org enrich → people search → people match), same
shape as the other pipelines' Apollo modules but with founder-first
title preferences.

The buyer of a fractional CFO is the operator who currently signs the
checks — the founder, president, or managing partner — NOT the
controller or finance manager the company is hiring (that person is
the thing being bought, not the buyer) and NOT a CFO (there shouldn't
be one; if Apollo surfaces one, that's a strong signal the lead has
graduated past the fractional stage).
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

# Title filter sent to Apollo's people-search. Founder / president
# pinned to the top because almost every sub-50-person target is owner-
# operated. Finance-titled people are deliberately NOT in this list —
# Controllers and CFOs at a prospect company are the role being hired
# or the disqualifier, not the buyer.
_DM_TITLES: tuple[str, ...] = (
    "Founder", "Co-Founder",
    "Owner",
    "President",
    "Chief Executive Officer", "CEO",
    "Managing Partner", "Managing Director",
    "Chief Operating Officer", "COO",
)


class Result(BaseModel):
    org_found: bool = False
    dm_found: bool = False
    dm_name: str | None = None
    dm_title: str | None = None
    dm_email: str | None = None
    dm_linkedin_url: str | None = None
    apollo_person_id: str | None = None
    headcount: int | None = None
    has_full_time_cfo: bool = False  # True when Apollo also surfaces a CFO at this org.


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


def _search_people(api_key: str, org_id: str, titles: list[str]) -> list[dict[str, Any]]:
    try:
        r = requests.post(
            f"{_BASE}/mixed_people/api_search",
            json={
                "organization_ids": [org_id],
                "person_titles": titles,
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


_SENIORITY_WEIGHT: dict[str, int] = {
    "founder": 100,
    "owner": 100,
    "c_suite": 90,
    "partner": 70,
    "vp": 50,
    "head": 40,
    "director": 35,
    "manager": 20,
    "senior": 10,
}

# Operator / founder keywords — the fractional-CFO buyer.
_OPERATOR_PHRASES: tuple[str, ...] = (
    "founder", "co-founder", "cofounder", "president", "managing partner",
    "managing director", "owner",
)
_CEO_RE = re.compile(r"\b(ceo|chief executive)\b", re.IGNORECASE)
_COO_RE = re.compile(r"\b(coo|chief operating)\b", re.IGNORECASE)

# Roles that are NOT the fractional-CFO buyer — the existing CFO (if
# Apollo surfaces one we want to know, but they shouldn't be the DM)
# and the finance lead currently being hired.
_NEGATIVE_FINANCE_PHRASES: tuple[str, ...] = (
    "cfo", "chief financial", "controller", "comptroller",
    "vp finance", "vp of finance", "director of finance",
)

_DISQUALIFYING_TITLE_KEYWORDS: tuple[str, ...] = (
    "recruitment", "recruiting", "talent acquisition",
    "marketing", "communications",
    "sales", "business development",
    "medical officer", "clinical",
    "diversity", "equity and inclusion",
    "facilities",
    "intern", "internship",
)


def _score_person(person: dict[str, Any]) -> int:
    """Higher = better fractional-CFO DM. Founder / CEO at the top;
    finance-titled people get heavily *down*-weighted because at a
    prospect company they're either the disqualifier (already-CFO) or
    the role being hired (controller-or-below)."""
    title = (person.get("title") or "").lower()
    seniority = (person.get("seniority") or "").lower()
    score = _SENIORITY_WEIGHT.get(seniority, 0)
    if any(k in title for k in _OPERATOR_PHRASES):
        score += 70
    elif _CEO_RE.search(title):
        score += 60
    elif _COO_RE.search(title):
        score += 30
    if any(k in title for k in _NEGATIVE_FINANCE_PHRASES):
        score -= 80
    if any(k in title for k in _DISQUALIFYING_TITLE_KEYWORDS):
        score -= 1000
    return score


def _pick_best(people: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not people:
        return None
    best = max(people, key=_score_person)
    if _score_person(best) < 0:
        return None
    return best


def _people_include_cfo(people: list[dict[str, Any]]) -> bool:
    for p in people:
        title = (p.get("title") or "").lower()
        if "cfo" in title or "chief financial" in title:
            return True
    return False


def _sanitize_title(title: str | None) -> str | None:
    if not title:
        return None
    if len(title) > 80 or "|" in title:
        return None
    return title.strip()


def find_decision_maker(name: str, domain: str | None) -> Result:
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

    # Two searches: one for the operator DM, one for a CFO sanity-check
    # (used by daily_run as a late disqualifier — if Apollo says the
    # company has a CFO, it's not a fractional-CFO prospect).
    operator_people = _search_people(api_key, org_id, list(_DM_TITLES))
    cfo_people = _search_people(
        api_key, org_id, ["Chief Financial Officer", "CFO"]
    )
    has_full_time_cfo = _people_include_cfo(cfo_people)

    chosen = _pick_best(operator_people)
    if chosen is None or not chosen.get("id"):
        return Result(
            org_found=True,
            headcount=headcount,
            has_full_time_cfo=has_full_time_cfo,
        )

    person_id = str(chosen["id"])
    matched = _match_person(api_key, person_id)
    if matched is None:
        return Result(
            org_found=True,
            apollo_person_id=person_id,
            headcount=headcount,
            has_full_time_cfo=has_full_time_cfo,
        )

    full_name = matched.get("name")
    title = _sanitize_title(matched.get("title")) or _sanitize_title(chosen.get("title"))
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
        has_full_time_cfo=has_full_time_cfo,
    )
