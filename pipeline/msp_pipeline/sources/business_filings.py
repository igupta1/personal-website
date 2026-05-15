"""New-business-filed source for the insurance niche.

v1 uses OpenCorporates' public Search API as a single fetcher across
the three target jurisdictions (FL, CO, WA). This deviates from the
plan's "per-state bulk file fetchers" because OpenCorporates ships
the same normalized shape for every US state — one parser, one set of
fixtures, easier to reason about. Per-state bulk pulls remain a v2
option if OpenCorporates rate-limits become painful.

Auth: `OPENCORPORATES_API_KEY` is optional. Without it we get the
free public tier (heavily rate-limited; useful for demos and small
nightly pulls). With it we get whatever the user's plan allows. The
fetcher silently returns [] on any HTTP failure so the daily pipeline
keeps running — the leads from the jobs / funding sources are
unaffected.

Recency: the module-level `_MAX_FILING_AGE_DAYS = 60` clamp inside
``fetch`` caps the effective start date. Keeps the source-contract
`since` parameter intact (every other source obeys it the same way)
while preventing a `--reenrich` backfill from pulling years of
ancient filings.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import requests

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

_OPENCORPORATES_API = "https://api.opencorporates.com/v0.4/companies/search"

# Hard ceiling on how far back into the past `fetch` will look,
# regardless of what `since` the caller passes. 60 days is the
# half-life-adjacent window for "this entity is still in its buying-
# decision phase" — anything older has already shopped insurance once.
_MAX_FILING_AGE_DAYS = 60

# OpenCorporates jurisdiction codes for the v1 target states. Add to
# this tuple to expand coverage; OpenCorporates indexes all 50 US
# states under `us_<state>` codes.
_TARGET_JURISDICTIONS: tuple[str, ...] = ("us_fl", "us_co", "us_wa")

_PER_PAGE = 30
_TIMEOUT_S = 30


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _state_from_jurisdiction(jurisdiction: str) -> str:
    """`us_fl` -> `FL`. Defensive: returns the whole code uppercased
    when the format isn't the expected `us_<code>`."""
    if "_" in jurisdiction:
        return jurisdiction.rsplit("_", 1)[-1].upper()
    return jurisdiction.upper()


def _fetch_from_opencorporates(
    jurisdiction: str, since: datetime
) -> list[LeadCandidate]:
    captured_at = _utcnow()
    state = _state_from_jurisdiction(jurisdiction)
    since_iso = since.date().isoformat()
    today_iso = captured_at.date().isoformat()

    params: dict[str, str | int] = {
        "jurisdiction_code": jurisdiction,
        "incorporation_date": f"{since_iso}:{today_iso}",
        "inactive": "false",
        "order": "incorporation_date",
        "per_page": _PER_PAGE,
    }
    api_key = os.environ.get("OPENCORPORATES_API_KEY")
    if api_key:
        params["api_token"] = api_key

    try:
        response = requests.get(
            _OPENCORPORATES_API,
            params=params,
            timeout=_TIMEOUT_S,
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        _log.exception("opencorporates fetch failed: %s", jurisdiction)
        return []

    companies = ((data.get("results") or {}).get("companies")) or []
    candidates: list[LeadCandidate] = []
    for entry in companies:
        company = entry.get("company") or {}
        name = (company.get("name") or "").strip()
        if not name:
            continue

        filed_on = company.get("incorporation_date") or ""
        # Defensive: if OpenCorporates returns a filing outside the
        # requested window (some entries have a different date semantic),
        # drop it locally so the scoring decay starts from the right age.
        if filed_on:
            try:
                filed_dt = datetime.fromisoformat(filed_on)
                if filed_dt.replace(tzinfo=None) < since:
                    continue
            except ValueError:
                pass  # leave it through; scoring will handle stale dates

        candidates.append(
            LeadCandidate(
                name=name,
                domain=None,
                initial_signal=Signal(
                    type=SignalType.NEW_BUSINESS_FILED,
                    source=SourceName.FILINGS,
                    captured_at=captured_at,
                    payload={
                        "state": state,
                        "filing_type": company.get("company_type") or "Entity",
                        "filed_on": filed_on,
                        "registered_agent": (
                            company.get("registered_agent_name") or ""
                        ),
                        "opencorporates_url": company.get("opencorporates_url") or "",
                    },
                ),
            )
        )
    return candidates


def fetch(*, since: datetime, limit: int | None = None) -> list[LeadCandidate]:
    # Clamp `since` so a stale parameter or a `--reenrich` backfill
    # can't sweep in filings older than _MAX_FILING_AGE_DAYS.
    effective_since = max(
        since, _utcnow() - timedelta(days=_MAX_FILING_AGE_DAYS)
    )

    candidates: list[LeadCandidate] = []
    for jurisdiction in _TARGET_JURISDICTIONS:
        try:
            candidates.extend(
                _fetch_from_opencorporates(jurisdiction, effective_since)
            )
        except Exception:
            _log.exception("fetcher %s failed entirely", jurisdiction)

    if limit is not None:
        candidates = candidates[:limit]
    return candidates
