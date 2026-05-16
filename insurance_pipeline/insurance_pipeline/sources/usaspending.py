"""USAspending.gov federal contract awards source.

Free, keyless, no rate limit (within reason). New federal contract
recipients need insurance triggers:
- Cyber + professional liability (for any federal data work)
- E&O (services contractors)
- D&O (corporate governance step-up after a meaningful gov contract)

Filters to small/mid-tier awards ($10K-$500K) and the last 60 days,
which dodges the Boeing/Lockheed enterprise tier while catching real
SMB federal contractors.

Emits FUNDING_RAISED — same scoring weight as Form D / TechCrunch
funding (25). A separate GOV_CONTRACT_AWARDED type could land later
if we want distinct scoring; for v1, reusing the bucket keeps scope
tight.

Skips government entities by name pattern — many "COUNTY OF X" /
"CITY OF X" winners are not insurance buyers.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

import requests

from insurance_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

_USASPENDING_URL = (
    "https://api.usaspending.gov/api/v2/search/spending_by_award/"
)
_MAX_FILING_AGE_DAYS = 60
_TIMEOUT_S = 60
_LIMIT_PER_PAGE = 100

# Award-amount window. Below 10K is too small to trigger meaningful
# insurance need; above 500K usually means an established mid-market
# contractor that already has coverage. Sweet spot is 25-200K for SMB
# first-federal-contract triggers.
_MIN_AWARD = 25_000
_MAX_AWARD = 500_000

# Government / public-sector winners — not insurance buyers in the
# agent-prospect sense. Filter at parse time.
_GOV_ENTITY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(?:CITY|COUNTY|STATE|TOWN|TOWNSHIP|VILLAGE) OF\b", re.IGNORECASE),
    re.compile(r"^(?:U\.?S\.?|UNITED STATES|US ARMY|US NAVY|US AIR FORCE)\b", re.IGNORECASE),
    re.compile(r"^DEPARTMENT OF\b", re.IGNORECASE),
    re.compile(r"\b(?:UNIVERSITY|COLLEGE)\b", re.IGNORECASE),
    re.compile(r"\bSCHOOL\s+DISTRICT\b", re.IGNORECASE),
    re.compile(r"\bPUBLIC\s+SCHOOLS?\b", re.IGNORECASE),
    re.compile(r"\b(?:AUTHORITY|COMMISSION|BOARD)\s*$", re.IGNORECASE),
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _is_gov_entity(name: str) -> bool:
    return any(p.search(name) for p in _GOV_ENTITY_PATTERNS)


def fetch(*, since: datetime, limit: int | None = None) -> list[LeadCandidate]:
    captured_at = _utcnow()
    effective_since = max(
        since, captured_at - timedelta(days=_MAX_FILING_AGE_DAYS)
    )

    body = {
        "filters": {
            "time_period": [{
                "start_date": effective_since.date().isoformat(),
                "end_date": captured_at.date().isoformat(),
                "date_type": "action_date",
            }],
            "award_type_codes": ["A", "B", "C", "D"],  # contracts
            "award_amounts": [{
                "lower_bound": _MIN_AWARD,
                "upper_bound": _MAX_AWARD,
            }],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Award Date",
            "Description",
            "awarding_agency_name",
            "naics_code",
            "naics_description",
            "Place of Performance State Code",
        ],
        "sort": "Award Date",
        "order": "desc",
        "limit": _LIMIT_PER_PAGE,
        "page": 1,
    }

    try:
        resp = requests.post(
            _USASPENDING_URL,
            json=body,
            timeout=_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        _log.exception("usaspending fetch failed")
        return []

    results = data.get("results") or []
    if not isinstance(results, list):
        _log.warning("usaspending: non-list results")
        return []

    candidates: list[LeadCandidate] = []
    seen_names: set[str] = set()
    for row in results:
        name = (row.get("Recipient Name") or "").strip()
        if not name:
            continue
        if name in seen_names:
            # Same recipient may appear multiple times if they won
            # multiple contracts in the window. Keep one.
            continue
        if _is_gov_entity(name):
            continue
        seen_names.add(name)

        award_date = row.get("Award Date") or ""
        award_amount = row.get("Award Amount") or 0
        naics_desc = (row.get("naics_description") or "").strip()
        agency = (row.get("awarding_agency_name") or "").strip()
        state = (row.get("Place of Performance State Code") or "").strip()

        candidates.append(
            LeadCandidate(
                name=name,
                domain=None,
                initial_signal=Signal(
                    type=SignalType.FUNDING_RAISED,
                    source=SourceName.FUNDING,
                    captured_at=captured_at,
                    payload={
                        "filing_type": "Federal contract",
                        "filed_on": str(award_date)[:10],
                        "amount_usd": float(award_amount) if award_amount else None,
                        "title": f"{agency} contract: {naics_desc}".strip(": "),
                        "naics": naics_desc,
                        "agency": agency,
                        "state": state,
                    },
                ),
            )
        )

    if limit is not None:
        candidates = candidates[:limit]
    return candidates
