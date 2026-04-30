"""Per-niche outreach copy generation.

One OpenAI structured call per niche per lead. Returns a ``Copy`` Pydantic
with two fields: ``insight`` (one sentence) and ``outreach`` (3–5 sentence
email body). Pure — does not write to the DB. The ``daily_run.py``
orchestrator (M7) handles persistence and re-generation gating.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from msp_pipeline import llm
from msp_pipeline.models import Lead, NicheName, Signal, SignalType


class Copy(BaseModel):
    insight: str = Field(min_length=10, max_length=240)
    outreach: str = Field(min_length=80, max_length=1500)


_NICHE_FRAMING: dict[NicheName, str] = {
    NicheName.IT_MSP: (
        "You write outbound copy for an IT managed service provider (MSP) "
        "that sells help desk, sysadmin, and general IT support to growing "
        "small and mid-sized businesses (10-250 employees)."
    ),
    NicheName.MSSP: (
        "You write outbound copy for a managed security service provider "
        "(MSSP) that sells SOC monitoring, vuln management, and compliance "
        "support to regulated SMBs (10-250 employees)."
    ),
    NicheName.CLOUD: (
        "You write outbound copy for a cloud consultancy that helps SMBs "
        "(10-250 employees) plan, migrate, and optimize their AWS / GCP / "
        "Azure footprint."
    ),
}


_PROMPT_TEMPLATE = """\
{framing}

Lead profile:
- Company name: {name}
- Short form to use in the email body: "{short_name}"
- Industry: {industry}
- Approximate headcount: {headcount}
- Location: {location}
- Heat score for this niche: {score:.0f}/100

Recent signals (most recent first):
{signals}

Write two pieces of copy:
1. **insight** - ONE sentence (<= 220 chars) explaining the most compelling
   reason this lead is a fit. Lead with the strongest recent signal,
   referencing concrete details from it (job title, funding round, etc.).
2. **outreach** - a 3-5 sentence outbound email body (no subject line, no
   greeting, no signature) that:
   - opens by naming a *specific* detail from one recent signal - the
     actual job title, the funding amount/round, the breach context,
     etc. (do NOT just say "I saw you posted a job" - say "I saw the
     Senior Security Engineer posting"),
   - addresses the company by the short form "{short_name}" (not the
     full legal name) - e.g. "Hope Q3 is going well at {short_name}",
   - frames how your service addresses what that signal implies,
   - ends with a soft, low-pressure CTA (e.g. a 15-min intro call).

Tone & style - write like a human salesperson, not like an AI:
- AVOID these AI-tell openers: "I hope this email finds you well",
  "I came across", "I noticed", "I wanted to reach out", "Just wanted
  to touch base", "I trust you're doing well".
- Prefer specific, conversational openers grounded in the signal.
- Professional B2B tone. No emojis. No exclamation marks. Contractions
  are fine ("you're", "we've").
- Do NOT fabricate facts not in the profile or signals - if a detail
  isn't there, don't invent it.

Special handling for breach signals:
- If a recent signal is a breach disclosure, treat it with EMPATHY,
  never urgency or alarm. The reader has already lived through the
  breach; the goal is to build trust as a future partner, not to
  exploit a vulnerable moment. Phrases like "we know how stressful a
  disclosure can be" or "no pitch - just wanted to introduce ourselves
  in case it's useful down the line" land better than urgency.
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
        short_name=_short_name(lead.name),
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
        SignalType.EXEC_HIRED,
        SignalType.FUNDING_RAISED,
        SignalType.BREACH_DISCLOSED,
    }
)

_GENERIC_LEADING_WORDS = frozenset({"the", "a", "an"})

_SIGNAL_PAYLOAD_FIELDS: dict[SignalType, tuple[str, ...]] = {
    SignalType.JOB_IT_SUPPORT: ("title", "location"),
    SignalType.JOB_IT_LEADERSHIP: ("title", "location"),
    SignalType.JOB_SECURITY: ("title", "location"),
    SignalType.JOB_CLOUD_DEVOPS: ("title", "location"),
    SignalType.EXEC_HIRED: ("title", "location"),
    SignalType.FUNDING_RAISED: ("title", "amount_usd", "round"),
    SignalType.BREACH_DISCLOSED: ("disclosed_on", "records_affected", "title"),
}


def _short_name(name: str) -> str:
    tokens = name.strip().split()
    if not tokens:
        return name
    first = tokens[0]
    if first.lower() in _GENERIC_LEADING_WORDS and len(tokens) > 1:
        first = tokens[1]
    if first.endswith("'s") or first.endswith("’s"):
        first = first[:-2]
    return first


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
