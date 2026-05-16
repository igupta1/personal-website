"""Single-niche scorer for the insurance pipeline.

Score = clamp(sum(signal_weight × recency_decay), 0, 100). No LLM,
pure deterministic, 30-day half-life carried over from msp_pipeline.

Insurance carriers / brokers / MGAs / TPAs are purged pre-scoring in
``enrichment.purge_disqualified``, not score-zeroed here.

Some signal weights are payload-aware. The most important case: an
FMCSA motor carrier authority for a 50-truck fleet is roughly 50× the
premium value of a 1-truck owner-operator, and the dashboard should
reflect that. ``_signal_weight`` handles the per-signal lookup; flat
weights live in SIGNAL_WEIGHTS as fallbacks.
"""

from __future__ import annotations

from datetime import datetime, timezone

from insurance_pipeline.models import Lead, Signal, SignalType

HALF_LIFE_DAYS = 30.0
SCORE_MIN = 0.0
SCORE_MAX = 100.0

# Flat-weight fallbacks. NEW_MOTOR_CARRIER_AUTHORITY's value here is
# what a single signal scores when fleet size is unknown — see
# `_signal_weight` for the scaled curve when payload data is present.
SIGNAL_WEIGHTS: dict[SignalType, float] = {
    SignalType.NEW_MOTOR_CARRIER_AUTHORITY: 30,
    SignalType.NEW_BUSINESS_FILED: 45,
    SignalType.BUILDING_PERMIT_ISSUED: 30,
    SignalType.FUNDING_RAISED: 25,
    SignalType.OSHA_INSPECTION_RECORDED: 20,
}


def _fmcsa_weight(power_units: int) -> float:
    """Linear in fleet size up to a 50-truck saturation point.
    - 1 truck   → 22  (owner-operator, ~$3K policy)
    - 5 trucks  → 30  (small operator)
    - 10 trucks → 38  (mid-small fleet)
    - 25 trucks → 60  (real commission territory)
    - 50+ trucks → 100 (clamped; sweet spot and above)
    Negative or zero power_units → flat fallback weight."""
    if power_units <= 0:
        return float(SIGNAL_WEIGHTS[SignalType.NEW_MOTOR_CARRIER_AUTHORITY])
    return min(100.0, 20 + power_units * 1.6)


def _signal_weight(sig: Signal) -> float:
    if sig.type == SignalType.NEW_MOTOR_CARRIER_AUTHORITY:
        try:
            power_units = int(sig.payload.get("fleet_size_power_units") or 0)
        except (TypeError, ValueError):
            power_units = 0
        return _fmcsa_weight(power_units)
    return float(SIGNAL_WEIGHTS.get(sig.type, 0))


def score(lead: Lead, *, now: datetime | None = None) -> float:
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    total = 0.0
    for sig in lead.signals:
        w = _signal_weight(sig)
        if w == 0:
            continue
        total += w * _decay(sig.captured_at, now)
    return max(SCORE_MIN, min(SCORE_MAX, total))


def _decay(captured_at: datetime, now: datetime) -> float:
    days = max(0.0, (now - captured_at).total_seconds() / 86400.0)
    return 0.5 ** (days / HALF_LIFE_DAYS)
