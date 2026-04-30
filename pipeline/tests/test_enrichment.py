from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from msp_pipeline import db, enrichment
from msp_pipeline.enrichment import (
    Industry,
    _IndustryOut,
    _Lookup,
    _round_to_10,
    classify_industry,
    compute_band,
    enrich,
    lookup_company,
)
from msp_pipeline.models import (
    Lead,
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _signal(
    *,
    type: SignalType = SignalType.JOB_IT_SUPPORT,
    source: SourceName = SourceName.JOBS,
    captured_at: datetime | None = None,
    payload: dict[str, Any] | None = None,
) -> Signal:
    return Signal(
        type=type,
        source=source,
        captured_at=captured_at or _now(),
        payload=payload or {},
    )


def _candidate(name: str) -> LeadCandidate:
    return LeadCandidate(name=name, initial_signal=_signal())


def _lookup_response(
    *, headcount: str = "120", city: str = "Boston", state: str = "MA", country: str = "US"
) -> str:
    return (
        f"HEADCOUNT: {headcount}\n"
        f"CITY: {city}\n"
        f"STATE: {state}\n"
        f"COUNTRY: {country}\n"
    )


# --- Pure helpers ----------------------------------------------------------


@pytest.mark.parametrize(
    "headcount,expected",
    [
        (None, None),
        (5, "1-10"),
        (10, "11-50"),
        (50, "51-200"),
        (4999, "1001-5000"),
        (5000, "5000+"),
        (100000, "5000+"),
    ],
)
def test_compute_band(headcount: int | None, expected: str | None) -> None:
    assert compute_band(headcount) == expected


@pytest.mark.parametrize(
    "n,expected",
    [(0, 0), (4, 0), (7, 10), (23, 20), (1247, 1250), (5000, 5000)],
)
def test_round_to_10(n: int, expected: int) -> None:
    assert _round_to_10(n) == expected


# --- lookup_company --------------------------------------------------------


def test_lookup_company_parses_full_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(return_value=_lookup_response(headcount="1247")),
    )
    lead = Lead(name="Acme Inc", name_key="acme", signals=[_signal()])
    result = lookup_company(lead)
    assert result == _Lookup(headcount=1250, city="Boston", state="MA", country="US")


def test_lookup_company_handles_unknowns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(
            return_value=_lookup_response(
                headcount="unknown", city="unknown", state="unknown", country="unknown"
            )
        ),
    )
    lead = Lead(name="Mystery Co", name_key="mystery", signals=[_signal()])
    result = lookup_company(lead)
    assert result == _Lookup(headcount=None, city=None, state=None, country=None)


# --- classify_industry -----------------------------------------------------


def test_classify_industry_returns_enum(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_call_openai(prompt: str, **kwargs: Any) -> _IndustryOut:
        captured["prompt"] = prompt
        captured["kwargs"] = kwargs
        return _IndustryOut(industry=Industry.FINTECH)

    monkeypatch.setattr(enrichment.llm, "call_openai", fake_call_openai)
    lead = Lead(
        name="Stripe",
        name_key="stripe",
        signals=[_signal(type=SignalType.JOB_SECURITY)],
    )
    result = classify_industry(lead)
    assert result is Industry.FINTECH
    assert "Stripe" in captured["prompt"]
    assert "job_posted_security" in captured["prompt"]
    assert captured["kwargs"]["response_model"] is _IndustryOut


# --- enrich() orchestrator -------------------------------------------------


def _insert_lead(conn: Any, name: str) -> Lead:
    lead = db.upsert_lead(conn, _candidate(name))
    assert lead.id is not None
    return lead


def test_enrich_skips_when_industry_set_and_no_new_signals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "Already Enriched Co")
    assert lead.id is not None
    db.update_lead(conn, lead.id, industry="fintech", country="US")
    db.append_signal(
        conn,
        lead.id,
        _signal(type=SignalType.ENRICHMENT_RUN, source=SourceName.COMPUTED),
    )

    refreshed = db.get_lead(conn, lead_id=lead.id)
    assert refreshed is not None

    gemini_mock = MagicMock()
    openai_mock = MagicMock()
    monkeypatch.setattr(enrichment.llm, "call_gemini", gemini_mock)
    monkeypatch.setattr(enrichment.llm, "call_openai", openai_mock)

    assert enrich(conn, refreshed) is True
    gemini_mock.assert_not_called()
    openai_mock.assert_not_called()


def test_enrich_re_runs_when_new_signal_after_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "Stale Enrich Co")
    assert lead.id is not None
    db.update_lead(conn, lead.id, industry="fintech", country="US")
    base = _now()
    db.append_signal(
        conn,
        lead.id,
        _signal(
            type=SignalType.ENRICHMENT_RUN,
            source=SourceName.COMPUTED,
            captured_at=base,
        ),
    )
    db.append_signal(
        conn,
        lead.id,
        _signal(
            type=SignalType.FUNDING_RAISED,
            source=SourceName.FUNDING,
            captured_at=base + timedelta(days=1),
        ),
    )

    refreshed = db.get_lead(conn, lead_id=lead.id)
    assert refreshed is not None

    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(return_value=_lookup_response(headcount="80")),
    )
    monkeypatch.setattr(
        enrichment.llm,
        "call_openai",
        MagicMock(return_value=_IndustryOut(industry=Industry.SOFTWARE_SAAS)),
    )

    assert enrich(conn, refreshed) is True

    after = db.get_lead(conn, lead_id=lead.id)
    assert after is not None
    assert after.industry == "software_saas"
    assert after.headcount == 80
    enrichment_runs = [s for s in after.signals if s.type == SignalType.ENRICHMENT_RUN]
    assert len(enrichment_runs) == 2


def test_enrich_writes_us_lead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "New US Co")

    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(return_value=_lookup_response(headcount="120")),
    )
    monkeypatch.setattr(
        enrichment.llm,
        "call_openai",
        MagicMock(return_value=_IndustryOut(industry=Industry.FINTECH)),
    )

    assert enrich(conn, lead) is True
    assert lead.id is not None

    after = db.get_lead(conn, lead_id=lead.id)
    assert after is not None
    assert after.industry == "fintech"
    assert after.headcount == 120
    assert after.country == "US"
    assert after.updated_at is not None
    assert lead.updated_at is not None
    assert after.updated_at >= lead.updated_at

    location_signals = [
        s for s in after.signals if s.type == SignalType.LOCATION_CAPTURED
    ]
    assert len(location_signals) == 1
    assert location_signals[0].source == SourceName.COMPUTED
    assert location_signals[0].payload == {"city": "Boston", "state": "MA"}

    enrichment_runs = [
        s for s in after.signals if s.type == SignalType.ENRICHMENT_RUN
    ]
    assert len(enrichment_runs) == 1
    assert enrichment_runs[0].source == SourceName.COMPUTED


def test_enrich_deletes_non_us_lead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "London Co")
    assert lead.id is not None

    openai_mock = MagicMock()
    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(return_value=_lookup_response(country="GB")),
    )
    monkeypatch.setattr(enrichment.llm, "call_openai", openai_mock)

    assert enrich(conn, lead) is False
    assert db.get_lead(conn, lead_id=lead.id) is None
    openai_mock.assert_not_called()


def test_enrich_skips_location_signal_when_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "No Loc Co")
    assert lead.id is not None

    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(
            return_value=_lookup_response(
                headcount="50", city="unknown", state="unknown", country="US"
            )
        ),
    )
    monkeypatch.setattr(
        enrichment.llm,
        "call_openai",
        MagicMock(return_value=_IndustryOut(industry=Industry.OTHER)),
    )

    assert enrich(conn, lead) is True
    after = db.get_lead(conn, lead_id=lead.id)
    assert after is not None
    location_signals = [
        s for s in after.signals if s.type == SignalType.LOCATION_CAPTURED
    ]
    assert location_signals == []


def test_enrich_persists_headcount_rounding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "Rounding Co")
    assert lead.id is not None

    monkeypatch.setattr(
        enrichment.llm,
        "call_gemini",
        MagicMock(return_value=_lookup_response(headcount="1247")),
    )
    monkeypatch.setattr(
        enrichment.llm,
        "call_openai",
        MagicMock(return_value=_IndustryOut(industry=Industry.SOFTWARE_SAAS)),
    )

    enrich(conn, lead)
    after = db.get_lead(conn, lead_id=lead.id)
    assert after is not None
    assert after.headcount == 1250


def test_enrich_force_re_enriches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    lead = _insert_lead(conn, "Force Co")
    assert lead.id is not None
    db.update_lead(conn, lead.id, industry="fintech", country="US")
    db.append_signal(
        conn,
        lead.id,
        _signal(type=SignalType.ENRICHMENT_RUN, source=SourceName.COMPUTED),
    )

    refreshed = db.get_lead(conn, lead_id=lead.id)
    assert refreshed is not None

    gemini_mock = MagicMock(return_value=_lookup_response(headcount="500"))
    openai_mock = MagicMock(return_value=_IndustryOut(industry=Industry.HEALTHCARE))
    monkeypatch.setattr(enrichment.llm, "call_gemini", gemini_mock)
    monkeypatch.setattr(enrichment.llm, "call_openai", openai_mock)

    assert enrich(conn, refreshed, force=True) is True
    gemini_mock.assert_called_once()
    openai_mock.assert_called_once()

    after = db.get_lead(conn, lead_id=lead.id)
    assert after is not None
    assert after.industry == "healthcare"
    assert after.headcount == 500
