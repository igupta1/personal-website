"""Insight copy generation for the CFO pipeline.

One OpenAI structured call per lead → ``Copy.insight`` (single
third-person sentence shown above the signal list on each card).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field

from cfo_pipeline import llm
from cfo_pipeline.models import Lead, Signal, SignalType


class Copy(BaseModel):
    insight: str = Field(min_length=10, max_length=160)


_FRAMING = (
    "You are summarizing leads for a fractional CFO. The product: "
    "part-time CFO services for sub-50-employee US companies. The "
    "buyer is the founder / CEO who has financial complexity (raised "
    "money, hiring a controller, taking on a board) but isn't big "
    "enough to justify a $300K full-time CFO. Buying triggers: a "
    "Controller / VP Finance posting (they need finance leadership "
    "but are sizing down from CFO), a recent Form D / Series A "
    "(fresh cash, new board reporting obligations)."
)


_INSIGHT_STYLES: tuple[str, ...] = (
    "Open with the specific finance hire they're making (Controller, "
    "VP Finance, Accounting Manager). Name the title in the first "
    "three words. Then the implication for fractional-CFO timing.",

    "Lead with the funding moment (Form D filed, Series A closed) "
    "and the reporting / board-prep load it creates. Frame the "
    "fractional CFO as the bridge before a full-time hire makes sense.",

    "Frame the urgency window — fresh hire postings get bound to a "
    "candidate within 6-10 weeks. Why call this week vs. next month.",

    "Highlight what's distinctive about this company's stage or "
    "vertical. Stage-aware: 'Seed-stage [vertical] company in "
    "[state]...', 'Post-Series-A [vertical] hiring its first "
    "Controller...'.",

    "Open with the decision maker (founder / CEO / managing "
    "partner), then the buying-window signal, then the fractional-"
    "CFO angle.",

    "Action-first verb opener. 'Hiring a Controller for...', "
    "'Closed seed round...', 'Filed Form D after...'. Then the "
    "implication.",
)


_PROMPT_TEMPLATE = """\
{framing}

STYLE FOR THIS LEAD: {style}

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
buying moment that makes this lead a fit for a fractional CFO — the
actual posting title, the filing type, the round size if known. Use
the company name or pronouns like "they" / "the company" — NEVER
"you" or "your". This text is shown to the fractional CFO scanning a
list of leads.

Hard rules — the LLM grading this rejects insights that violate any:
1. Do NOT start the sentence with the company name.
2. Do NOT use these phrases: "recently posted", "recently filed",
   "just filed", "effective [date]", "indicating a need for",
   "in need of a CFO".
3. Do NOT use generic filler: "growing companies", "presents an
   opportunity", "may indicate", "ideal candidate".
4. Do NOT use marketing-fluff hedge language: "positioned for",
   "poised for", "may require", "well-positioned", "primed for",
   "potential need for".
5. Output ENGLISH ONLY.
6. Do NOT echo "0 days ago" or "0d ago" — use "today" / "yesterday"
   for fresh signals.
7. DO lead with a concrete detail from the signal payload (the title
   they're hiring for, the funding round, the filing type).
8. DO follow the STYLE directive above.
"""


def generate(
    lead: Lead,
    score: float,
    *,
    model: str = "gpt-4o-mini",
) -> Copy:
    seed = lead.id if lead.id is not None else hash(lead.name)
    style = _INSIGHT_STYLES[seed % len(_INSIGHT_STYLES)]

    prompt = _PROMPT_TEMPLATE.format(
        framing=_FRAMING,
        style=style,
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
        SignalType.JOB_POSTED_FINANCE_LEAD,
        SignalType.FUNDING_RAISED,
    }
)

_SIGNAL_PAYLOAD_FIELDS: dict[SignalType, tuple[str, ...]] = {
    SignalType.JOB_POSTED_FINANCE_LEAD: ("title", "date_posted", "site"),
    SignalType.FUNDING_RAISED: ("filing_type", "filed_on", "feed_title"),
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


def _humanize_days_ago(days_ago: int) -> str:
    if days_ago <= 0:
        return "today"
    if days_ago == 1:
        return "yesterday"
    return f"{days_ago}d ago"


def _format_signal(sig: Signal, days_ago: int) -> str:
    fields = _SIGNAL_PAYLOAD_FIELDS.get(sig.type, ())
    parts: list[str] = []
    for field in fields:
        val = sig.payload.get(field)
        if val:
            parts.append(f"{field}={val!r}")
    detail = f" - {'; '.join(parts)}" if parts else ""
    return f"- {sig.type.value} ({_humanize_days_ago(days_ago)}){detail}"


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


# --- Trigger classifier (used by daily_run for the UI filter chip) -------


def trigger_type(sig: Signal) -> str:
    """Coarse classifier for UI filtering. Mirrors insurance's
    ``policy_fit.trigger_type`` so the LeadsPage trigger dropdown
    works the same way."""
    if sig.type == SignalType.JOB_POSTED_FINANCE_LEAD:
        return "finance_hire"
    if sig.type == SignalType.FUNDING_RAISED:
        ft = sig.payload.get("filing_type")
        if ft == "Form D":
            return "form_d"
        return "funding_event"
    return "other"
