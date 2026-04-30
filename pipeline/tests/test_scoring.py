from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from msp_pipeline import db
from msp_pipeline.models import (
    Lead,
    LeadCandidate,
    NicheName,
    Signal,
    SignalType,
    SourceName,
)
from msp_pipeline.scoring import (
    SIGNAL_WEIGHTS,
    score,
)


def _now() -> datetime:
    return datetime(2026, 5, 1, 12, 0, 0)


def _signal(
    *,
    type: SignalType,
    captured_at: datetime,
    source: SourceName = SourceName.JOBS,
    payload: dict[str, Any] | None = None,
) -> Signal:
    return Signal(
        type=type,
        source=source,
        captured_at=captured_at,
        payload=payload or {},
    )


def _lead(*signals: Signal) -> Lead:
    return Lead(name="Test Co", name_key="test", signals=list(signals))


def test_score_returns_three_niches() -> None:
    result = score(_lead(), now=_now())
    assert set(result.keys()) == set(NicheName)
    for v in result.values():
        assert 0.0 <= v <= 100.0


def test_score_zero_for_blank_lead() -> None:
    result = score(_lead(), now=_now())
    assert result == {n: 0.0 for n in NicheName}


def test_single_fresh_signal_scores_full_weight() -> None:
    lead = _lead(_signal(type=SignalType.BREACH_DISCLOSED, captured_at=_now()))
    result = score(lead, now=_now())
    assert result[NicheName.MSSP] == 45.0
    assert result[NicheName.IT_MSP] == 18.0
    assert result[NicheName.CLOUD] == 8.0


def test_recency_decay_halves_at_one_half_life() -> None:
    fresh_lead = _lead(_signal(type=SignalType.JOB_SECURITY, captured_at=_now()))
    stale_lead = _lead(
        _signal(type=SignalType.JOB_SECURITY, captured_at=_now() - timedelta(days=30))
    )
    fresh = score(fresh_lead, now=_now())[NicheName.MSSP]
    stale = score(stale_lead, now=_now())[NicheName.MSSP]
    assert abs(stale - fresh / 2) < 0.01 * fresh


def test_recency_decay_floors_at_zero_for_future_dates() -> None:
    future = _now() + timedelta(days=10)
    lead = _lead(_signal(type=SignalType.BREACH_DISCLOSED, captured_at=future))
    result = score(lead, now=_now())
    assert result[NicheName.MSSP] == 45.0


def test_score_clamps_to_100() -> None:
    lead = _lead(*[
        _signal(type=t, captured_at=_now())
        for t in (
            SignalType.JOB_IT_SUPPORT,
            SignalType.JOB_IT_LEADERSHIP,
            SignalType.JOB_SECURITY,
            SignalType.JOB_CLOUD_DEVOPS,
            SignalType.EXEC_HIRED,
            SignalType.FUNDING_RAISED,
            SignalType.BREACH_DISCLOSED,
        )
    ])
    result = score(lead, now=_now())
    for niche, val in result.items():
        raw_sum = sum(SIGNAL_WEIGHTS[niche].values())
        assert raw_sum > 100.0, f"sanity: {niche} weights should sum past 100"
        assert val == 100.0


def test_unknown_signal_types_ignored() -> None:
    lead = _lead(
        _signal(
            type=SignalType.LOCATION_CAPTURED,
            captured_at=_now(),
            source=SourceName.COMPUTED,
            payload={"city": "Boston", "state": "MA"},
        ),
        _signal(
            type=SignalType.ENRICHMENT_RUN,
            captured_at=_now(),
            source=SourceName.COMPUTED,
        ),
    )
    result = score(lead, now=_now())
    assert result == {n: 0.0 for n in NicheName}


def _candidate(name: str) -> LeadCandidate:
    return LeadCandidate(
        name=name,
        initial_signal=_signal(
            type=SignalType.JOB_SECURITY,
            captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
        ),
    )


def test_score_is_pure_no_db_writes(tmp_path: Path) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    inserted = db.upsert_lead(conn, _candidate("Pure Co"))
    assert inserted.id is not None

    score(inserted)

    refreshed = db.get_lead(conn, lead_id=inserted.id)
    assert refreshed is not None
    assert refreshed.it_msp_score is None
    assert refreshed.mssp_score is None
    assert refreshed.cloud_score is None
