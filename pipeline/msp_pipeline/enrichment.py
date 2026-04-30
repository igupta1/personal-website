"""Enrichment pass: industry, headcount, country (US-only filter), city/state.

One Gemini grounded call per lead for the lookup (headcount + city + state +
country), one OpenAI structured call for industry classification. Non-US
leads are deleted. Re-enrichment is signal-aware: skip when industry is set
and no source signal has arrived since the last ``ENRICHMENT_RUN`` marker.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel

from msp_pipeline import db, llm
from msp_pipeline.models import Lead, Signal, SignalType, SourceName

log = logging.getLogger(__name__)


# --- Industry vocabulary ---------------------------------------------------


class Industry(str, Enum):
    SOFTWARE_SAAS = "software_saas"
    FINTECH = "fintech"
    HEALTHCARE = "healthcare"
    ECOMMERCE_RETAIL = "ecommerce_retail"
    MANUFACTURING = "manufacturing"
    LOGISTICS_TRANSPORT = "logistics_transport"
    REAL_ESTATE = "real_estate"
    LEGAL_PROFESSIONAL = "legal_professional"
    EDUCATION = "education"
    MEDIA_ENTERTAINMENT = "media_entertainment"
    HOSPITALITY_FOOD = "hospitality_food"
    GOVERNMENT_NONPROFIT = "government_nonprofit"
    CONSTRUCTION = "construction"
    OTHER = "other"


# --- Pure helpers ---------------------------------------------------------


_BANDS: tuple[tuple[int, str], ...] = (
    (10, "1-10"),
    (50, "11-50"),
    (200, "51-200"),
    (1000, "201-1000"),
    (5000, "1001-5000"),
)


def compute_band(headcount: int | None) -> str | None:
    if headcount is None:
        return None
    for upper, label in _BANDS:
        if headcount < upper:
            return label
    return "5000+"


def _round_to_10(n: int) -> int:
    return int(round(n, -1))


def _summarize_signals(lead: Lead) -> str:
    types_by_source: dict[SourceName, list[str]] = {}
    for sig in lead.signals:
        if sig.source == SourceName.COMPUTED:
            continue
        types_by_source.setdefault(sig.source, []).append(sig.type.value)
    if not types_by_source:
        return "(no signals)"
    parts = []
    for source, types in types_by_source.items():
        unique = sorted(set(types))
        parts.append(f"{source.value} ({', '.join(unique)})")
    return "; ".join(parts)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- Combined Gemini lookup -----------------------------------------------


_LOOKUP_PROMPT = """\
Look up the company "{name}" on the web. Reply with EXACTLY these lines and
nothing else (use "unknown" when you cannot determine a value):

HEADCOUNT: <integer employee count or "unknown">
CITY: <city name or "unknown">
STATE: <2-letter US state code (CA, NY, ...) or "unknown">
COUNTRY: <2-letter ISO country code (US, GB, CA, ...) or "unknown">
"""

_FIELD_RE = {
    "headcount": re.compile(r"^HEADCOUNT:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "city": re.compile(r"^CITY:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "state": re.compile(r"^STATE:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "country": re.compile(r"^COUNTRY:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
}

_HEADCOUNT_NUM_RE = re.compile(r"\b(\d{1,3}(?:,\d{3})*|\d+)\b")


class _Lookup(BaseModel):
    headcount: int | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None


def _parse_field(raw: str, field: str) -> str | None:
    m = _FIELD_RE[field].search(raw)
    if m is None:
        return None
    val = m.group(1).strip()
    if not val or val.lower() == "unknown":
        return None
    return val


def _parse_headcount(raw_value: str | None) -> int | None:
    if raw_value is None:
        return None
    m = _HEADCOUNT_NUM_RE.search(raw_value)
    if m is None:
        return None
    return _round_to_10(int(m.group(1).replace(",", "")))


def lookup_company(lead: Lead) -> _Lookup:
    raw = llm.call_gemini(_LOOKUP_PROMPT.format(name=lead.name))
    headcount_raw = _parse_field(raw, "headcount")
    city = _parse_field(raw, "city")
    state_raw = _parse_field(raw, "state")
    country_raw = _parse_field(raw, "country")
    return _Lookup(
        headcount=_parse_headcount(headcount_raw),
        city=city,
        state=state_raw.upper() if state_raw else None,
        country=country_raw.upper() if country_raw else None,
    )


# --- OpenAI industry classification ---------------------------------------


_INDUSTRY_PROMPT = """\
Classify the company "{name}" into exactly one of these industry tags:
{tags}

Recent signals about this company: {signals}

Respond with the single best-fitting tag. Use "other" only when no other tag
reasonably applies.
"""


class _IndustryOut(BaseModel):
    industry: Industry


def classify_industry(lead: Lead) -> Industry:
    tags = ", ".join(t.value for t in Industry)
    prompt = _INDUSTRY_PROMPT.format(
        name=lead.name,
        tags=tags,
        signals=_summarize_signals(lead),
    )
    out = llm.call_openai(prompt, response_model=_IndustryOut)
    return out.industry


# --- Orchestrator ---------------------------------------------------------


def _last_enrichment_at(lead: Lead) -> datetime | None:
    times = [s.captured_at for s in lead.signals if s.type == SignalType.ENRICHMENT_RUN]
    return max(times) if times else None


def _has_signal_after(lead: Lead, cutoff: datetime) -> bool:
    return any(
        s.captured_at > cutoff
        for s in lead.signals
        if s.type != SignalType.ENRICHMENT_RUN
    )


def _should_skip(lead: Lead, force: bool) -> bool:
    if force:
        return False
    if lead.industry is None:
        return False
    last = _last_enrichment_at(lead)
    if last is None:
        return False
    return not _has_signal_after(lead, last)


def enrich(conn: sqlite3.Connection, lead: Lead, *, force: bool = False) -> bool:
    if lead.id is None:
        raise ValueError("enrich() requires a persisted lead (lead.id is None)")

    if _should_skip(lead, force):
        log.debug("enrich: skip lead %d (%s) — no new signals since last run",
                  lead.id, lead.name)
        return True

    lookup = lookup_company(lead)

    if lookup.country != "US":
        log.info("enrich: deleting non-US lead %d (%s) — country=%r",
                 lead.id, lead.name, lookup.country)
        db.delete_lead(conn, lead.id)
        return False

    if lookup.city or lookup.state:
        db.append_signal(
            conn,
            lead.id,
            Signal(
                type=SignalType.LOCATION_CAPTURED,
                source=SourceName.COMPUTED,
                captured_at=_utcnow(),
                payload={"city": lookup.city, "state": lookup.state},
            ),
        )

    industry = classify_industry(lead)

    db.update_lead(
        conn,
        lead.id,
        industry=industry.value,
        headcount=lookup.headcount,
        country="US",
    )

    db.append_signal(
        conn,
        lead.id,
        Signal(
            type=SignalType.ENRICHMENT_RUN,
            source=SourceName.COMPUTED,
            captured_at=_utcnow(),
            payload={},
        ),
    )

    return True
