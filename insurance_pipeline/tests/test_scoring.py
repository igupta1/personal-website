"""Scoring tests for the insurance pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from insurance_pipeline.models import Lead, Signal, SignalType, SourceName
from insurance_pipeline.scoring import SIGNAL_WEIGHTS, score


def _now() -> datetime:
    return datetime(2026, 5, 15, 12, 0, 0)


def _signal(
    *,
    type: SignalType,
    captured_at: datetime,
    source: SourceName = SourceName.FMCSA,
    payload: dict[str, Any] | None = None,
) -> Signal:
    return Signal(type=type, source=source, captured_at=captured_at, payload=payload or {})


def _lead(*signals: Signal) -> Lead:
    return Lead(name="Test Co", name_key="test", signals=list(signals))


def test_blank_lead_scores_zero() -> None:
    assert score(_lead(), now=_now()) == 0.0


def test_single_fresh_signal_full_weight() -> None:
    # FMCSA score is fleet-size-aware (see fmcsa_scoring tests below).
    # A signal with no fleet info falls back to SIGNAL_WEIGHTS = 30.
    lead = _lead(_signal(type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY, captured_at=_now()))
    assert score(lead, now=_now()) == 30.0

    lead = _lead(_signal(type=SignalType.NEW_BUSINESS_FILED, captured_at=_now()))
    assert score(lead, now=_now()) == 45.0


def test_fmcsa_scoring_scales_by_fleet_size() -> None:
    # 1-truck owner-operator → low ~$3K policy → score 22
    # 25-truck fleet → real commission → score 60
    # 50+ truck → saturation → 100
    # formula: min(100, 20 + power_units * 1.6)
    for power_units, expected in [(1, 21.6), (5, 28), (10, 36), (25, 60), (50, 100), (100, 100)]:
        lead = _lead(_signal(
            type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
            captured_at=_now(),
            payload={"fleet_size_power_units": power_units},
        ))
        actual = score(lead, now=_now())
        assert abs(actual - expected) < 0.5, (
            f"expected {expected} for {power_units} power units, got {actual}"
        )


def test_fmcsa_missing_fleet_size_uses_flat_fallback() -> None:
    lead = _lead(_signal(
        type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        captured_at=_now(),
        payload={},
    ))
    # Falls back to SIGNAL_WEIGHTS = 30.
    assert score(lead, now=_now()) == 30.0


def test_recency_decay_halves_at_one_half_life() -> None:
    fresh = _lead(_signal(type=SignalType.NEW_BUSINESS_FILED, captured_at=_now()))
    stale = _lead(
        _signal(
            type=SignalType.NEW_BUSINESS_FILED,
            captured_at=_now() - timedelta(days=30),
        )
    )
    f = score(fresh, now=_now())
    s = score(stale, now=_now())
    assert abs(s - f / 2) < 0.01 * f


def test_score_clamps_to_100() -> None:
    # All weighted signal types at once → sums past 100, must clamp.
    lead = _lead(*[
        _signal(type=t, captured_at=_now())
        for t in SIGNAL_WEIGHTS
    ])
    assert sum(SIGNAL_WEIGHTS.values()) > 100, (
        "sanity: weights should sum past 100 so the clamp matters"
    )
    assert score(lead, now=_now()) == 100.0


def test_non_scoring_signals_ignored() -> None:
    lead = _lead(
        _signal(
            type=SignalType.LOCATION_CAPTURED,
            captured_at=_now(),
            source=SourceName.COMPUTED,
            payload={"city": "Miami", "state": "FL"},
        ),
        _signal(
            type=SignalType.ENRICHMENT_RUN,
            captured_at=_now(),
            source=SourceName.COMPUTED,
        ),
    )
    assert score(lead, now=_now()) == 0.0
