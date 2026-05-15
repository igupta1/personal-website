"""Per-niche signal-only scorer.

Score = clamp(sum(signal_weight × recency_decay), 0, 100), per niche.
No LLM. Pure deterministic. Industry and headcount live on the lead row
(written by ``enrichment.py``) but are UI filters, not score inputs.
"""

from __future__ import annotations

from datetime import datetime, timezone

from msp_pipeline.models import Lead, NicheName, SignalType

HALF_LIFE_DAYS = 30.0
SCORE_MIN = 0.0
SCORE_MAX = 100.0

SIGNAL_WEIGHTS: dict[NicheName, dict[SignalType, float]] = {
    NicheName.IT_MSP: {
        # Rebalanced so every "primary" buying signal scores in the same
        # 33-39 single-fresh-signal range. Previously breach disclosures
        # at weight 40 swept the top 30 simply because state AGs report
        # them in volume; other signal types couldn't compete. The page
        # is a lead magnet — variety beats homogeneity.
        SignalType.JOB_IT_LEADERSHIP: 40,  # strongest MSP-buying signal
        SignalType.BREACH_DISCLOSED: 35,   # urgent IT pain (MSSP also high)
        SignalType.JOB_IT_SUPPORT: 35,     # growing IT team — MSP supplements
        SignalType.FUNDING_RAISED: 35,     # new cash, IT scale-up
        SignalType.EXEC_HIRED: 35,         # CISO/CIO/CTO triggers vendor eval
        SignalType.JOB_SECURITY: 15,       # primarily MSSP territory
        SignalType.JOB_CLOUD_DEVOPS: 15,   # primarily Cloud territory
    },
    NicheName.MSSP: {
        SignalType.BREACH_DISCLOSED: 45,
        SignalType.JOB_SECURITY: 40,
        SignalType.FUNDING_RAISED: 22,
        SignalType.JOB_IT_LEADERSHIP: 18,
        SignalType.EXEC_HIRED: 15,
        SignalType.JOB_CLOUD_DEVOPS: 12,
        SignalType.JOB_IT_SUPPORT: 10,
    },
    NicheName.CLOUD: {
        SignalType.JOB_CLOUD_DEVOPS: 40,
        SignalType.FUNDING_RAISED: 28,
        SignalType.JOB_IT_LEADERSHIP: 18,
        SignalType.EXEC_HIRED: 15,
        SignalType.JOB_SECURITY: 12,
        SignalType.JOB_IT_SUPPORT: 10,
        SignalType.BREACH_DISCLOSED: 8,
    },
}


def score(lead: Lead, *, now: datetime | None = None) -> dict[NicheName, float]:
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {niche: _score_niche(lead, niche, now) for niche in NicheName}


def _score_niche(lead: Lead, niche: NicheName, now: datetime) -> float:
    weights = SIGNAL_WEIGHTS[niche]
    total = 0.0
    for sig in lead.signals:
        w = weights.get(sig.type)
        if w is None:
            continue
        total += w * _decay(sig.captured_at, now)
    return max(SCORE_MIN, min(SCORE_MAX, total))


def _decay(captured_at: datetime, now: datetime) -> float:
    days = max(0.0, (now - captured_at).total_seconds() / 86400.0)
    return 0.5 ** (days / HALF_LIFE_DAYS)
