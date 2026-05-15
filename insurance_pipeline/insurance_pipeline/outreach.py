"""Insight copy generation for the insurance pipeline.

One OpenAI structured call per lead → ``Copy.insight`` (single
third-person sentence shown above the signal list on each card). Pure;
``daily_run`` handles persistence + regeneration gating.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from insurance_pipeline import llm
from insurance_pipeline.models import Lead, Signal, SignalType


class Copy(BaseModel):
    insight: str = Field(min_length=10, max_length=160)


_FRAMING = (
    "You are summarizing leads for a salesperson at an independent "
    "insurance agency that sells commercial lines (general "
    "liability, workers compensation, commercial auto, property, "
    "D&O / EPLI, group benefits) to SMBs (10-500 employees) and "
    "newly-formed entities. The agent prospects on buying triggers — "
    "new motor-carrier authorities, new business registrations, "
    "construction permits, fresh funding."
)


_PROMPT_TEMPLATE = """\
{framing}

Lead profile:
- Company: {name}
- What they do: {value_prop}
- Industry: {industry}
- Headcount: {headcount}
- Location: {location}
- Heat score: {score:.0f}/100

Recent signals (most recent first):
{signals}

Write ONE sentence (<= 140 chars), THIRD PERSON, naming the specific
buying moment that makes this lead a fit — the actual filing type, the
fleet size and authority date, the permit value, etc. Use the company
name or pronouns like "they" / "the company" — NEVER "you" or "your".
This text is shown to a salesperson scanning a list of leads, not to
the lead being pitched.

Avoid generic phrasing like "growing companies" or "highlighting the
need for insurance". Lead with the concrete signal.
"""


def generate(
    lead: Lead,
    score: float,
    *,
    model: str = "gpt-4o-mini",
) -> Copy:
    prompt = _PROMPT_TEMPLATE.format(
        framing=_FRAMING,
        name=lead.name,
        value_prop=lead.value_prop or "unknown",
        industry=lead.industry or "unknown",
        headcount=_describe_headcount(lead.headcount),
        location=_describe_location(lead),
        score=score,
        signals=_describe_signals(lead),
    )
    return llm.call_openai(prompt, response_model=Copy, model=model)


_SCORING_SIGNAL_TYPES = frozenset(
    {
        SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        SignalType.NEW_BUSINESS_FILED,
        SignalType.OSHA_INSPECTION_RECORDED,
        SignalType.BUILDING_PERMIT_ISSUED,
        SignalType.FUNDING_RAISED,
    }
)

_SIGNAL_PAYLOAD_FIELDS: dict[SignalType, tuple[str, ...]] = {
    SignalType.NEW_MOTOR_CARRIER_AUTHORITY: (
        "usdot", "fleet_size_power_units", "drivers", "issue_date",
    ),
    SignalType.NEW_BUSINESS_FILED: ("state", "filing_type", "filed_on"),
    SignalType.OSHA_INSPECTION_RECORDED: ("naics", "citations", "penalty"),
    SignalType.BUILDING_PERMIT_ISSUED: ("job_type", "estimated_cost"),
    SignalType.FUNDING_RAISED: ("title", "amount_usd", "round"),
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
