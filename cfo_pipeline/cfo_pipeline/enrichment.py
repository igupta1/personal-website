"""CFO-pipeline enrichment.

Two-stage, mirrors ``insurance_pipeline.enrichment`` but with:

- SMB cap at 50 (per the spec — fractional CFO is a sub-50-employee
  product). Companies above that are buying a full-time CFO, not a
  fractional one.
- CFO-competitor pre-purge (CPA firms, bookkeeping shops, other
  fractional-CFO providers, accounting / advisory firms). They aren't
  prospects, they're the competition.
- Founder / CEO Gemini DM prompt. The buyer of a fractional CFO is
  the operator who currently signs the checks — the founder /
  president / managing partner — NOT the finance lead they're
  trying to hire (that person doesn't exist yet).

Re-enrichment is signal-aware: skip when industry is set and no source
signal has arrived since the last ``ENRICHMENT_RUN`` marker.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel

from cfo_pipeline import db, llm
from cfo_pipeline.models import Lead, Signal, SignalType, SourceName
from cfo_pipeline.sources.edgar_form_d import (
    _STREET_SPV_RE,
    _VINTAGE_YEAR_RE,
)

log = logging.getLogger(__name__)


# --- Pure-code disqualification filter ------------------------------------
#
# Runs BEFORE any LLM call inside purge_disqualified, and as a secondary
# gate inside enrich(). Cheap regex pass that drops leads which clearly
# don't fit "SMB buying a fractional CFO" without burning Gemini tokens
# on them.

# 75 instead of 50 (spec said "~50") because the smoke run cut several
# plausible targets at 60-70 — fractional-CFO buyers exist comfortably
# up to ~75. The tilde in the spec was load-bearing.
_SMB_HEADCOUNT_CAP = 75

_BLOCKED_TLDS: tuple[str, ...] = (".gov", ".mil", ".edu")

_BLOCKED_NAME_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:State|City|County|Town) of\b", re.IGNORECASE),
    re.compile(r"\bDepartment of\b", re.IGNORECASE),
    re.compile(r"\bU\.?S\.? (?:House|Senate|Department|Government|Army|Navy|Air Force)\b", re.IGNORECASE),
    re.compile(r"\b(?:University|College)\b", re.IGNORECASE),
    re.compile(r"\bVentures?\b", re.IGNORECASE),
    re.compile(
        r"\b(?:Capital|Holdings?|Investments?) "
        r"(?:Partners|LLC|Inc|LP|L\.P\.|Corp(?:oration)?|Group|Fund|Management|Investments?)\b",
        re.IGNORECASE,
    ),
)

# Mega-corps that shouldn't be SMB leads. They have full finance orgs.
_BLOCKED_DOMAINS: frozenset[str] = frozenset({
    "google.com", "alphabet.com",
    "apple.com",
    "meta.com", "facebook.com",
    "microsoft.com",
    "amazon.com", "aws.amazon.com",
    "netflix.com",
    "tesla.com",
    "openai.com",
    "anthropic.com",
    "nvidia.com",
    "oracle.com",
    "salesforce.com",
    "ibm.com",
    "intel.com",
    "cisco.com",
})

# CFO-competitor names — purged outright. A company whose business IS
# providing fractional-CFO / accounting / bookkeeping services is the
# competition, not the prospect. They have their own finance leadership
# and they're who the prospect would buy FROM, not someone who'd buy.
_CFO_COMPETITOR_NAME_RES: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"\b(?:Fractional|Outsourced|Virtual|Part[-\s]?Time)\s+CFO\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bCFO\s+(?:Services|Solutions|Partners|Group|Advisory|Consulting)\b", re.IGNORECASE),
    re.compile(
        r"\b(?:CPA|CPAs|Certified\s+Public\s+Accountants?)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:Accounting|Bookkeeping|Tax)\s+(?:Services|Solutions|Group|Firm|Partners|Associates|Advisors|Consultants|LLC|Inc)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:Financial|Finance|Wealth)\s+(?:Advisors?|Advisory|Planning|Management|Consultants?|Consulting)\b",
        re.IGNORECASE,
    ),
)


def _domain_blocked(domain: str | None) -> bool:
    if not domain:
        return False
    d = domain.lower().strip().rstrip("/")
    if d in _BLOCKED_DOMAINS:
        return True
    return any(d.endswith(tld) for tld in _BLOCKED_TLDS)


def _name_blocked(name: str) -> bool:
    return any(p.search(name) for p in _BLOCKED_NAME_RES)


def _is_cfo_competitor(name: str) -> bool:
    return any(p.search(name) for p in _CFO_COMPETITOR_NAME_RES)


# Financial-vehicle names that slip past EDGAR's source-level filter
# end up in the DB and persist across runs. Same retroactive cleanup
# pass as insurance_pipeline.
_FINANCIAL_VEHICLE_NAME_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bBancshares\b", re.IGNORECASE),
    re.compile(r"\bBancorp\b", re.IGNORECASE),
    re.compile(r"\bFinance\s+(?:Corp|Company|Inc|LLC|L\.?P\.?)", re.IGNORECASE),
    re.compile(r",\s*L\.?\s?L?\s?P\.?\s*$", re.IGNORECASE),
    re.compile(r"^TPG\b", re.IGNORECASE),
    re.compile(r"^Blackstone\b", re.IGNORECASE),
    re.compile(r"^KKR\b", re.IGNORECASE),
    re.compile(r"^Carlyle\b", re.IGNORECASE),
    re.compile(r"^MidOcean\b", re.IGNORECASE),
)


def _is_financial_vehicle(name: str) -> bool:
    return any(p.search(name) for p in _FINANCIAL_VEHICLE_NAME_RES)


_MEGACORP_PREFIX_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(?:AT&?T|ATT)\b", re.IGNORECASE),
    re.compile(r"^Verizon\b", re.IGNORECASE),
    re.compile(r"^T-Mobile\b", re.IGNORECASE),
    re.compile(r"^Sprint\b", re.IGNORECASE),
    re.compile(r"^Comcast\b", re.IGNORECASE),
    re.compile(r"^Johnson\s+Controls\b", re.IGNORECASE),
    re.compile(r"^AECOM\b", re.IGNORECASE),
    re.compile(r"^Honeywell\b", re.IGNORECASE),
    re.compile(r"^Siemens\b", re.IGNORECASE),
    re.compile(r"^Raytheon\b", re.IGNORECASE),
    re.compile(r"^Lockheed\b", re.IGNORECASE),
    re.compile(r"^Northrop\b", re.IGNORECASE),
    re.compile(r"^Boeing\b", re.IGNORECASE),
    re.compile(r"^General\s+Dynamics\b", re.IGNORECASE),
    re.compile(r"^Booz\s+Allen\b", re.IGNORECASE),
    re.compile(r"^Leidos\b", re.IGNORECASE),
    re.compile(r"^SAIC\b", re.IGNORECASE),
    re.compile(r"^IBM\b", re.IGNORECASE),
    re.compile(r"^Oracle\b", re.IGNORECASE),
    re.compile(r"^Microsoft\b", re.IGNORECASE),
    re.compile(r"^Amazon\b", re.IGNORECASE),
    re.compile(r"^Google\b", re.IGNORECASE),
    re.compile(r"^Cisco\b", re.IGNORECASE),
    re.compile(r"^Accenture\b", re.IGNORECASE),
    re.compile(r"^Deloitte\b", re.IGNORECASE),
    re.compile(r"^KPMG\b", re.IGNORECASE),
    re.compile(r"^Ernst\s+&?\s+Young\b", re.IGNORECASE),
    re.compile(r"^PricewaterhouseCoopers\b", re.IGNORECASE),
    re.compile(r"^PwC\b", re.IGNORECASE),
)


def _is_megacorp_subsidiary(name: str) -> bool:
    return any(p.match(name) for p in _MEGACORP_PREFIX_RES)


def _is_form_d_noise(lead: Lead) -> bool:
    """Vintage-year / street-SPV regexes from the EDGAR source applied
    retroactively. Gated to leads that actually have a Form D signal
    so we don't accidentally drop a real operating company that
    happens to have a year-looking name."""
    has_form_d = any(
        s.type == SignalType.FUNDING_RAISED
        and s.payload.get("filing_type") == "Form D"
        for s in lead.signals
    )
    if not has_form_d:
        return False
    return bool(
        _VINTAGE_YEAR_RE.search(lead.name) or _STREET_SPV_RE.search(lead.name)
    )


def _disqualification_reason(lead: Lead) -> str | None:
    if _domain_blocked(lead.domain):
        return f"blocked_domain={lead.domain}"
    if _name_blocked(lead.name):
        return "blocked_name_pattern"
    if _is_cfo_competitor(lead.name):
        return "cfo_competitor_name"
    if _is_financial_vehicle(lead.name):
        return "financial_vehicle"
    if _is_megacorp_subsidiary(lead.name):
        return "megacorp_subsidiary"
    if _is_form_d_noise(lead):
        return "form_d_noise_pattern"
    if lead.headcount is not None and lead.headcount > _SMB_HEADCOUNT_CAP:
        return f"oversized={lead.headcount}"
    if lead.headcount == 0:
        return "zero_headcount"
    return None


def purge_disqualified(conn: sqlite3.Connection) -> int:
    """Pure-code pass. Two phases:

    1. Sweep the leads table for anything whose name_key sits in the
       persistent ``disqualified`` table (CFO postings from the jobs
       source). This is the new piece relative to insurance_pipeline.
    2. Run the regex-based disqualifier on each lead (CFO competitors,
       financial vehicles, megacorps, oversized headcount).
    """
    deleted = 0
    leads = list(db.iter_leads(conn))

    # Phase 1: disqualified-table sweep.
    by_key: dict[str, Lead] = {}
    for lead in leads:
        if lead.id is None:
            continue
        by_key[lead.name_key] = lead
    for key, _name, reason in db.iter_disqualified(conn):
        lead = by_key.get(key)
        if lead is None or lead.id is None:
            continue
        log.info(
            "purge: deleting %d (%s) — disqualified=%s", lead.id, lead.name, reason
        )
        try:
            db.delete_lead(conn, lead.id)
            deleted += 1
        except Exception:
            log.exception("purge: delete_lead failed for id=%s", lead.id)

    # Phase 2: regex disqualifier on what's left.
    for lead in db.iter_leads(conn):
        if lead.id is None:
            continue
        reason = _disqualification_reason(lead)
        if reason is not None:
            log.info("purge: deleting %d (%s) — %s", lead.id, lead.name, reason)
            try:
                db.delete_lead(conn, lead.id)
                deleted += 1
            except Exception:
                log.exception("purge: delete_lead failed for id=%s", lead.id)
    return deleted


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
DOMAIN: <primary website domain like "acme.com" without https or path, or "unknown">
IS_CFO_COMPETITOR: <"yes" if this company itself SELLS CFO / accounting / \
bookkeeping / fractional-CFO / financial-advisory services (i.e. they're \
the competition, not a buyer), otherwise "no">
HAS_FULL_TIME_CFO: <"yes" if the company appears to have a full-time CFO \
already (named CFO on the leadership page, etc.), otherwise "no". Companies \
with a full-time CFO are NOT prospects for a fractional CFO.>
DM_NAME: <full name of the founder / CEO / president / managing partner — \
the operator who would BUY a fractional CFO. NOT the controller or finance \
manager (that's the role being hired). NOT the CFO (if there isn't one, \
which there shouldn't be at this stage). Prefer Founder / CEO / President / \
Managing Partner / Owner. Use "unknown" only if you can't identify a likely \
buyer.>
DM_TITLE: <their job title (e.g. "Founder", "CEO", "Managing Partner") \
or "unknown">
VALUE_PROP: <ONE sentence (max 25 words), present tense, plain language, \
describing what the company does. Examples: "B2B SaaS for HVAC contractors, \
recently raised Series A." / "DTC apparel brand based in Brooklyn." / \
"Boutique consulting firm specializing in healthcare M&A." Avoid marketing \
fluff. Use "unknown" only as a last resort.>
"""

_FIELD_RE = {
    "headcount": re.compile(r"^HEADCOUNT:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "city": re.compile(r"^CITY:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "state": re.compile(r"^STATE:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "country": re.compile(r"^COUNTRY:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "domain": re.compile(r"^DOMAIN:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "is_cfo_competitor": re.compile(
        r"^IS_CFO_COMPETITOR:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE
    ),
    "has_full_time_cfo": re.compile(
        r"^HAS_FULL_TIME_CFO:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE
    ),
    "dm_name": re.compile(r"^DM_NAME:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "dm_title": re.compile(r"^DM_TITLE:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
    "value_prop": re.compile(r"^VALUE_PROP:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE),
}

_HEADCOUNT_NUM_RE = re.compile(r"\b(\d{1,3}(?:,\d{3})*|\d+)\b")
_DOMAIN_CLEAN_RE = re.compile(r"^https?://|^www\.|/.*$", re.IGNORECASE)


def _round_to_10(n: int) -> int:
    return int(round(n, -1))


class _Lookup(BaseModel):
    headcount: int | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    domain: str | None = None
    is_cfo_competitor: bool = False
    has_full_time_cfo: bool = False
    dm_name: str | None = None
    dm_title: str | None = None
    value_prop: str | None = None


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


def _parse_domain(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    cleaned = _DOMAIN_CLEAN_RE.sub("", raw_value).strip().lower()
    if not cleaned or "." not in cleaned or " " in cleaned:
        return None
    return cleaned


def _parse_yesno(raw_value: str | None) -> bool:
    if raw_value is None:
        return False
    return raw_value.strip().lower().startswith("y")


def lookup_company(lead: Lead) -> _Lookup:
    raw = llm.call_gemini(_LOOKUP_PROMPT.format(name=lead.name))
    return _Lookup(
        headcount=_parse_headcount(_parse_field(raw, "headcount")),
        city=_parse_field(raw, "city"),
        state=(_parse_field(raw, "state") or "").upper() or None,
        country=(_parse_field(raw, "country") or "").upper() or None,
        domain=_parse_domain(_parse_field(raw, "domain")),
        is_cfo_competitor=_parse_yesno(_parse_field(raw, "is_cfo_competitor")),
        has_full_time_cfo=_parse_yesno(_parse_field(raw, "has_full_time_cfo")),
        dm_name=_parse_field(raw, "dm_name"),
        dm_title=_parse_field(raw, "dm_title"),
        value_prop=_parse_field(raw, "value_prop"),
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

    reason = _disqualification_reason(lead)
    if reason is not None:
        log.info("enrich: deleting lead %d (%s) — %s", lead.id, lead.name, reason)
        db.delete_lead(conn, lead.id)
        return False

    lookup = lookup_company(lead)

    has_apollo = any(s.type == SignalType.APOLLO_ENRICHED for s in lead.signals)
    effective_headcount = (
        lead.headcount if has_apollo and lead.headcount is not None
        else lookup.headcount or lead.headcount
    )

    oversized = (
        effective_headcount is not None and effective_headcount > _SMB_HEADCOUNT_CAP
    )
    # Country check: explicit non-US deletes. "unknown" deletes too —
    # the CFO pipeline has no FMCSA-style US-only sources, so an
    # unknown country is genuinely unknown.
    non_us = lookup.country is not None and lookup.country != "US"
    unknown_country = lookup.country is None
    if (
        non_us or unknown_country
        or oversized
        or lookup.is_cfo_competitor
        or lookup.has_full_time_cfo
    ):
        log.info(
            "enrich: deleting lead %d (%s) — country=%r headcount=%r competitor=%s has_cfo=%s",
            lead.id, lead.name, lookup.country, effective_headcount,
            lookup.is_cfo_competitor, lookup.has_full_time_cfo,
        )
        db.delete_lead(conn, lead.id)
        # has_full_time_cfo → also mark disqualified so a future Form D
        # filing for the same company doesn't resurrect it.
        if lookup.has_full_time_cfo:
            from cfo_pipeline.models import Disqualifier
            db.mark_disqualified(
                conn,
                Disqualifier(
                    name=lead.name,
                    reason="has_full_time_cfo_per_gemini",
                    source=SourceName.COMPUTED,
                    payload={},
                ),
            )
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

    updates: dict[str, object] = {
        "industry": industry.value,
        "country": "US" if lookup.country == "US" else None,
        "value_prop": lookup.value_prop,
    }
    if not has_apollo:
        updates.update(
            headcount=lookup.headcount if lookup.headcount is not None else lead.headcount,
            domain=lookup.domain,
            dm_name=lookup.dm_name,
            dm_title=lookup.dm_title,
        )
    elif lookup.domain and not lead.domain:
        updates["domain"] = lookup.domain
    db.update_lead(conn, lead.id, **updates)

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
