"""Per-niche outreach copy generation.

One OpenAI structured call per niche per lead. Returns a ``Copy`` Pydantic
with two fields: ``insight`` (one sentence) and ``outreach`` (3–5 sentence
email body). Pure — does not write to the DB. The ``daily_run.py``
orchestrator (M7) handles persistence and re-generation gating.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from msp_pipeline import llm
from msp_pipeline.models import Lead, NicheName, Signal, SignalType


class Copy(BaseModel):
    insight: str = Field(min_length=10, max_length=160)
    # Tight cap so the email is genuinely send-as-is. ~3 short sentences
    # land in 350-550 chars; 600 caps the long tail without forcing one-liners.
    outreach: str = Field(min_length=80, max_length=600)


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
- Company: {name} (short form: "{short_name}")
- What they do: {value_prop}
- Industry: {industry}
- Headcount: {headcount}
- Location: {location}
- Likely decision maker: {dm_block}
- Heat score for this niche: {score:.0f}/100

Recent signals (most recent first):
{signals}

Write two pieces of copy.

**insight** — ONE sentence (<= 140 chars), THIRD PERSON, naming the
specific buying moment (job title, breach context, funding round, etc.).
Use the company name or "they" / "the company" — never "you" / "your".
This is shown to a salesperson, not the prospect.

**outreach** — an email the salesperson can copy, paste, and send AS-IS.

Structure — TWO parts, separated by a blank line:

(a) GREETING LINE — REQUIRED when a decision maker is named above.
    Format: "Hi <first_name>," using the actual first name from the
    "Likely decision maker" line (e.g. for "Philip Hu, Owner" write
    "Hi Philip,"). Take only the first word of the name. If the
    decision maker is unknown, OMIT this line entirely AND do NOT
    substitute a generic greeting — no "Hi there,", no "Hello,",
    no "Hi,". Start directly with sentence 1 of the body.

(b) BODY — 2-3 sentences, ~60-90 words total. Hard rules:
    - Sentence 1 names the SPECIFIC signal detail (the actual title,
      the dollar amount, the agency that received the breach
      disclosure) and ties it to what the company does using the
      "What they do" field. Example: "Saw the Director of IT posting
      at {short_name} — running a 200-person insurance brokerage,
      that role can make or break ticket SLAs."
    - Sentence 2: one concrete way your service eases that pressure,
      grounded in their actual business model from "What they do".
    - Sentence 3 (optional): soft CTA like "Worth a 15-minute intro?"
      or "Happy to share what we did for a similar firm if useful."
    - NO sign-off, NO "Best," NO signature — the salesperson adds
      their own. End on the CTA sentence.

Banned openers (AI tells): "I hope this email finds you well", "I came
across", "I noticed", "I wanted to reach out", "Just wanted to touch
base", "I trust you're doing well", "Hope Q3 is going well at...". Use
specific, signal-grounded language instead.

Banned forever:
- Template placeholders like "[Your Company Name]", "[Customer]",
  "[Date]", "[Your MSSP]" — any "[Word]" form. Write concrete text.
- Exclamation marks.
- Emojis.
- Fabricating facts not in the profile or signals.

Breach signals get EMPATHY, not urgency. The recipient is already
stressed. Lead with "Saw the disclosure — no pitch, just wanted to
introduce ourselves in case it's useful down the road" rather than
hammering on the vulnerability.
"""


# Detects unfilled template-style placeholders in generated copy. The model
# occasionally hallucinates "[Your Company Name]" / "[Customer]" / "[Date]"
# patterns from its training data. We reject those rather than ship them.
_PLACEHOLDER_RE = re.compile(r"\[[A-Z][A-Za-z][A-Za-z\s]{1,40}\]")


def _describe_dm(lead: Lead) -> str:
    if lead.dm_name and lead.dm_title:
        return f"{lead.dm_name}, {lead.dm_title}"
    if lead.dm_name:
        return lead.dm_name
    return "unknown — open without a personal greeting"


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
        value_prop=lead.value_prop or "unknown",
        industry=lead.industry or "unknown",
        headcount=_describe_headcount(lead.headcount),
        location=_describe_location(lead),
        dm_block=_describe_dm(lead),
        score=score,
        signals=_describe_signals(lead),
    )
    out = llm.call_openai(prompt, response_model=Copy, model=model)
    for field, text in (("insight", out.insight), ("outreach", out.outreach)):
        match = _PLACEHOLDER_RE.search(text)
        if match:
            raise llm.LLMError(
                f"outreach.generate: copy.{field} contains template "
                f"placeholder {match.group(0)!r}"
            )
    return out


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
