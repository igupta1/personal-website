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


def _fmcsa_weight(
    power_units: int, drivers: int, days_since_issue: int
) -> float:
    """FMCSA composite weight: fleet size + recency bonus.

    Fleet size uses ``max(power_units, drivers, 1)`` because the FMCSA
    census often reports ``power_units=0`` for carriers that haven't
    filed MCS-150 yet — using drivers as a fallback gives us
    differentiation for those carriers instead of collapsing them all
    to the flat fallback weight.

    - size=1 (owner-operator), fresh (≤7d) → 22 + 20 = 42
    - size=5 (small operator), fresh        → 28 + 20 = 48
    - size=25 (real fleet), fresh           → 60 + 20 = 80
    - size=25, 50d old                      → 60
    - size=50+, fresh                       → 100 (clamped)

    Recency bonus codifies the buying-intent decay: the agent's window
    is the first 30 days after authority, when the carrier is actively
    shopping coverage. After 30d most have bound a policy.
    """
    size = max(power_units, drivers, 1)
    size_weight = min(60.0, 20.0 + size * 1.6)
    if days_since_issue <= 7:
        size_weight += 20.0
    elif days_since_issue <= 30:
        size_weight += 10.0
    return min(100.0, size_weight)


def _safe_int(v: object) -> int:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return 0


def _days_since(iso_date: str | None, now: datetime) -> int:
    if not iso_date:
        return 9999
    try:
        d = datetime.fromisoformat(str(iso_date)[:10])
    except (TypeError, ValueError):
        return 9999
    return max(0, (now - d).days)


def _signal_weight(sig: Signal, now: datetime) -> float:
    if sig.type == SignalType.NEW_MOTOR_CARRIER_AUTHORITY:
        power_units = _safe_int(sig.payload.get("fleet_size_power_units"))
        drivers = _safe_int(sig.payload.get("drivers"))
        age = _days_since(sig.payload.get("issue_date"), now)
        return _fmcsa_weight(power_units, drivers, age)
    return float(SIGNAL_WEIGHTS.get(sig.type, 0))


def score(lead: Lead, *, now: datetime | None = None) -> float:
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    total = 0.0
    for sig in lead.signals:
        w = _signal_weight(sig, now)
        if w == 0:
            continue
        total += w * _decay(sig.captured_at, now)
    return max(SCORE_MIN, min(SCORE_MAX, total))


def _decay(captured_at: datetime, now: datetime) -> float:
    days = max(0.0, (now - captured_at).total_seconds() / 86400.0)
    return 0.5 ** (days / HALF_LIFE_DAYS)
