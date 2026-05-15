"""Single-niche scorer for the insurance pipeline.

Score = clamp(sum(signal_weight × recency_decay), 0, 100). No LLM,
pure deterministic, 30-day half-life carried over from msp_pipeline.

Insurance carriers / brokers / MGAs / TPAs are purged pre-scoring in
``enrichment.purge_disqualified``, not score-zeroed here. Different
architecture from v1; same goal.
"""

from __future__ import annotations

from datetime import datetime, timezone

from insurance_pipeline.models import Lead, SignalType

HALF_LIFE_DAYS = 30.0
SCORE_MIN = 0.0
SCORE_MAX = 100.0

SIGNAL_WEIGHTS: dict[SignalType, float] = {
    # Tier 1: federal commercial-auto mandate. Every new motor carrier
    # MUST carry commercial auto by federal rule — strongest signal we
    # can ingest.
    SignalType.NEW_MOTOR_CARRIER_AUTHORITY: 50,
    # Tier 1: a brand-new entity needs the starter pack — general
    # liability, often workers comp, sometimes property.
    SignalType.NEW_BUSINESS_FILED: 45,
    # Tier 2: construction contractor exposure — WC + GL + builders
    # risk. Source not yet built; weight defined for forward-compat.
    SignalType.BUILDING_PERMIT_ISSUED: 30,
    # Tier 2 / optional reuse: fresh funding → hiring → group benefits
    # + D&O scale-up. Source not yet built.
    SignalType.FUNDING_RAISED: 25,
    # Tier 2: OSHA inspection is a re-rate trigger, not a coverage
    # need (WC is mandatory in 49 states). Soft signal — weighted
    # below new-entity filings by design.
    SignalType.OSHA_INSPECTION_RECORDED: 20,
}


def score(lead: Lead, *, now: datetime | None = None) -> float:
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    total = 0.0
    for sig in lead.signals:
        w = SIGNAL_WEIGHTS.get(sig.type)
        if w is None:
            continue
        total += w * _decay(sig.captured_at, now)
    return max(SCORE_MIN, min(SCORE_MAX, total))


def _decay(captured_at: datetime, now: datetime) -> float:
    days = max(0.0, (now - captured_at).total_seconds() / 86400.0)
    return 0.5 ** (days / HALF_LIFE_DAYS)
