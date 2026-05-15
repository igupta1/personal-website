"""Per-niche insight copy generation.

One OpenAI structured call per niche per lead. Returns a ``Copy`` Pydantic
with one field: ``insight`` — a single third-person sentence summarizing
the buying-moment, shown above the signal list on each card. Pure — does
not write to the DB. The ``daily_run.py`` orchestrator handles persistence
and re-generation gating.

Outreach email bodies were removed from the lead magnet; the file is kept
under the ``outreach`` name so existing imports / call sites stay stable,
but it now generates only the insight blurb.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from msp_pipeline import llm
from msp_pipeline.models import Lead, NicheName, Signal, SignalType


class Copy(BaseModel):
    insight: str = Field(min_length=10, max_length=160)


_NICHE_FRAMING: dict[NicheName, str] = {
    NicheName.IT_MSP: (
        "You are summarizing leads for a salesperson at an IT managed "
        "service provider (MSP) selling help desk, sysadmin, and general "
        "IT support to growing SMBs (10-250 employees)."
    ),
    NicheName.MSSP: (
        "You are summarizing leads for a salesperson at a managed security "
        "service provider (MSSP) selling SOC monitoring, vulnerability "
        "management, and compliance support to regulated SMBs (10-250 "
        "employees)."
    ),
    NicheName.CLOUD: (
        "You are summarizing leads for a salesperson at a cloud "
        "consultancy that helps SMBs (10-250 employees) plan, migrate, "
        "and optimize their AWS / GCP / Azure footprint."
    ),
    NicheName.INSURANCE: (
        "You are summarizing leads for a salesperson at an independent "
        "insurance agency that sells commercial lines (general "
        "liability, workers compensation, commercial auto, property, "
        "D&O / EPLI, group benefits) to SMBs (10-250 employees) and "
        "newly-formed entities. The agent prospects on buying "
        "triggers — new entity registrations, blue-collar / fleet "
        "hiring, new finance or HR leadership, fresh funding."
    ),
}


_PROMPT_TEMPLATE = """\
{framing}

Lead profile:
- Company: {name}
- What they do: {value_prop}
- Industry: {industry}
- Headcount: {headcount}
- Location: {location}
- Heat score for this niche: {score:.0f}/100

Recent signals (most recent first):
{signals}

Write ONE sentence (<= 140 chars), THIRD PERSON, naming the specific
buying moment that makes this lead a fit (the actual job title, the
funding amount/round, the agency that received the breach disclosure,
etc.). Use the company name or pronouns like "they" / "the company" —
NEVER "you" or "your". This text is shown to a salesperson scanning a
list of leads, not to the lead being pitched.

Avoid generic phrasing like "growing companies" or "highlighting the
need for IT support". Lead with the concrete signal.
"""


def generate(
    lead: Lead,
    niche: NicheName,
    score: float,
    *,
    model: str = "gpt-4o-mini",
) -> Copy:
    prompt = _PROMPT_TEMPLATE.format(
        framing=_NICHE_FRAMING[niche],
        name=lead.name,
        value_prop=lead.value_prop or "unknown",
        industry=lead.industry or "unknown",
        headcount=_describe_headcount(lead.headcount),
        location=_describe_location(lead),
        score=score,
        signals=_describe_signals(lead),
    )
    return llm.call_openai(prompt, response_model=Copy, model=model)


# --- Private helpers -------------------------------------------------------


_SCORING_SIGNAL_TYPES = frozenset(
    {
        SignalType.JOB_IT_SUPPORT,
        SignalType.JOB_IT_LEADERSHIP,
        SignalType.JOB_SECURITY,
        SignalType.JOB_CLOUD_DEVOPS,
        SignalType.JOB_OPS_ROLE,
        SignalType.JOB_BLUE_COLLAR,
        SignalType.JOB_FLEET_ROLE,
        SignalType.JOB_FINANCE_OPS,
        SignalType.EXEC_HIRED,
        SignalType.FUNDING_RAISED,
        SignalType.BREACH_DISCLOSED,
        SignalType.NEW_BUSINESS_FILED,
    }
)

_SIGNAL_PAYLOAD_FIELDS: dict[SignalType, tuple[str, ...]] = {
    SignalType.JOB_IT_SUPPORT: ("title", "location"),
    SignalType.JOB_IT_LEADERSHIP: ("title", "location"),
    SignalType.JOB_SECURITY: ("title", "location"),
    SignalType.JOB_CLOUD_DEVOPS: ("title", "location"),
    SignalType.JOB_OPS_ROLE: ("title", "location"),
    SignalType.JOB_BLUE_COLLAR: ("title", "location"),
    SignalType.JOB_FLEET_ROLE: ("title", "location"),
    SignalType.JOB_FINANCE_OPS: ("title", "location"),
    SignalType.EXEC_HIRED: ("title", "location"),
    SignalType.FUNDING_RAISED: ("title", "amount_usd", "round"),
    SignalType.BREACH_DISCLOSED: ("disclosed_on", "records_affected", "title"),
    SignalType.NEW_BUSINESS_FILED: ("state", "filing_type", "filed_on"),
}


def _describe_headcount(n: int | None) -> str:
    if n is None:
        return "unknown"
    return f"~{n} employees"


def _describe_location(lead: Lead) -> str:
    locs = sorted(
        (s for s in lead.signals if s.type == SignalType.LOCATION_CAPTURED),
        key=lambda s: s.captured_at,
        reverse=True,
    )
    if not locs:
        return lead.country or "unknown"
    payload = locs[0].payload
    city = payload.get("city")
    state = payload.get("state")
    if city and state:
        return f"{city}, {state}"
    if state:
        return state
    if city:
        return city
    return lead.country or "unknown"


def _format_signal(sig: Signal, days_ago: int) -> str:
    fields = _SIGNAL_PAYLOAD_FIELDS.get(sig.type, ())
    parts: list[str] = []
    for field in fields:
        val = sig.payload.get(field)
        if val:
            parts.append(f"{field}={val!r}")
    detail = f" - {'; '.join(parts)}" if parts else ""
    return f"- {sig.type.value} ({days_ago}d ago){detail}"


def _describe_signals(
    lead: Lead, *, limit: int = 6, now: datetime | None = None
) -> str:
    relevant = [s for s in lead.signals if s.type in _SCORING_SIGNAL_TYPES]
    relevant.sort(key=lambda s: s.captured_at, reverse=True)
    if not relevant:
        return "(no recent signals on file)"
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    return "\n".join(
        _format_signal(s, max(0, (now - s.captured_at).days))
        for s in relevant[:limit]
    )
