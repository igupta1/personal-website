from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from msp_pipeline import daily_run, db, enrichment
from msp_pipeline.models import (
    Lead,
    LeadCandidate,
    NicheName,
    Signal,
    SignalType,
    SourceName,
)
from msp_pipeline.outreach import Copy


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _signal(
    type: SignalType,
    captured_at: datetime,
    *,
    source: SourceName = SourceName.JOBS,
    payload: dict[str, Any] | None = None,
) -> Signal:
    return Signal(
        type=type,
        source=source,
        captured_at=captured_at,
        payload=payload or {},
    )


def _candidate(
    name: str,
    sig_type: SignalType = SignalType.JOB_SECURITY,
    source: SourceName = SourceName.JOBS,
) -> LeadCandidate:
    return LeadCandidate(
        name=name,
        initial_signal=_signal(sig_type, _now(), source=source),
    )


def test_main_dry_run_makes_no_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "leads.db"
    out_path = tmp_path / "leads.json"

    fetch_mock = MagicMock(return_value=[_candidate("Test Co")])
    monkeypatch.setattr(daily_run.jobs, "fetch", fetch_mock)
    monkeypatch.setattr(daily_run.funding, "fetch", fetch_mock)
    monkeypatch.setattr(daily_run.breaches, "fetch", fetch_mock)

    enrich_mock = MagicMock()
    generate_mock = MagicMock()
    monkeypatch.setattr("msp_pipeline.enrichment.enrich", enrich_mock)
    monkeypatch.setattr("msp_pipeline.outreach.generate", generate_mock)

    rc = daily_run.main(
        [
            "--dry-run",
            "--db-path",
            str(db_path),
            "--output-path",
            str(out_path),
        ]
    )
    assert rc == 0
    assert not db_path.exists()
    assert not out_path.exists()
    enrich_mock.assert_not_called()
    generate_mock.assert_not_called()
    assert fetch_mock.call_count == 3


def test_main_end_to_end_smoke(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "leads.db"
    out_path = tmp_path / "leads.json"

    monkeypatch.setattr(
        daily_run.jobs,
        "fetch",
        MagicMock(return_value=[_candidate("Acme Co", SignalType.JOB_SECURITY)]),
    )
    monkeypatch.setattr(
        daily_run.funding,
        "fetch",
        MagicMock(
            return_value=[
                _candidate("Beta Inc", SignalType.FUNDING_RAISED, SourceName.FUNDING)
            ]
        ),
    )
    monkeypatch.setattr(
        daily_run.breaches,
        "fetch",
        MagicMock(
            return_value=[
                _candidate("Gamma LLC", SignalType.BREACH_DISCLOSED, SourceName.BREACHES)
            ]
        ),
    )

    monkeypatch.setattr(
        "msp_pipeline.enrichment.lookup_company",
        MagicMock(
            return_value=enrichment._Lookup(
                headcount=100, city="Boston", state="MA", country="US"
            )
        ),
    )
    monkeypatch.setattr(
        "msp_pipeline.enrichment.classify_industry",
        MagicMock(return_value=enrichment.Industry.FINTECH),
    )
    monkeypatch.setattr(
        "msp_pipeline.outreach.generate",
        MagicMock(return_value=Copy(insight="x" * 30, outreach="y" * 200)),
    )

    rc = daily_run.main(
        ["--db-path", str(db_path), "--output-path", str(out_path)]
    )
    assert rc == 0
    assert db_path.exists()
    assert out_path.exists()

    payload = json.loads(out_path.read_text())
    assert "generated_at" in payload
    assert set(payload["niches"].keys()) == {"it_msp", "mssp", "cloud"}
    for leads in payload["niches"].values():
        assert len(leads) == 3
        for lead in leads:
            assert set(lead.keys()) >= {
                "name",
                "industry",
                "headcount",
                "country",
                "city",
                "state",
                "score",
                "insight",
                "outreach",
                "signals",
            }
            assert lead["country"] == "US"
            assert lead["city"] == "Boston"
            assert lead["state"] == "MA"


def test_per_source_failure_isolated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "leads.db"
    out_path = tmp_path / "leads.json"

    monkeypatch.setattr(
        daily_run.jobs,
        "fetch",
        MagicMock(side_effect=RuntimeError("jobs down")),
    )
    monkeypatch.setattr(
        daily_run.funding,
        "fetch",
        MagicMock(
            return_value=[
                _candidate("Funding Co", SignalType.FUNDING_RAISED, SourceName.FUNDING)
            ]
        ),
    )
    monkeypatch.setattr(
        daily_run.breaches,
        "fetch",
        MagicMock(
            return_value=[
                _candidate("Breach Co", SignalType.BREACH_DISCLOSED, SourceName.BREACHES)
            ]
        ),
    )
    monkeypatch.setattr(
        "msp_pipeline.enrichment.lookup_company",
        MagicMock(
            return_value=enrichment._Lookup(
                headcount=80, city=None, state=None, country="US"
            )
        ),
    )
    monkeypatch.setattr(
        "msp_pipeline.enrichment.classify_industry",
        MagicMock(return_value=enrichment.Industry.OTHER),
    )
    monkeypatch.setattr(
        "msp_pipeline.outreach.generate",
        MagicMock(return_value=Copy(insight="x" * 30, outreach="y" * 200)),
    )

    rc = daily_run.main(
        ["--db-path", str(db_path), "--output-path", str(out_path)]
    )
    assert rc == 0
    payload = json.loads(out_path.read_text())
    names = {lead["name"] for lead in payload["niches"]["it_msp"]}
    assert names == {"Funding Co", "Breach Co"}


def test_copy_regen_threshold_gates_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = db.init_db(tmp_path / "leads.db")

    cold = db.upsert_lead(conn, _candidate("Cold Co", SignalType.JOB_IT_SUPPORT))
    assert cold.id is not None
    db.update_lead(conn, cold.id, industry="other", headcount=80, country="US")

    hot = db.upsert_lead(
        conn, _candidate("Hot Co", SignalType.BREACH_DISCLOSED, SourceName.BREACHES)
    )
    assert hot.id is not None
    db.append_signal(conn, hot.id, _signal(SignalType.JOB_SECURITY, _now()))
    db.update_lead(conn, hot.id, industry="healthcare", headcount=80, country="US")

    generate_mock = MagicMock(
        return_value=Copy(insight="x" * 30, outreach="y" * 200)
    )
    monkeypatch.setattr("msp_pipeline.outreach.generate", generate_mock)

    rescored, copy_calls = daily_run._rescore_and_regen_copy(
        conn, [cold.id, hot.id], model="gpt-4o-mini"
    )
    assert len(rescored) == 2
    assert copy_calls == 1
    called_names = {call.args[0].name for call in generate_mock.call_args_list}
    assert called_names == {"Hot Co"}

    generate_mock.reset_mock()
    rescored2, copy_calls2 = daily_run._rescore_and_regen_copy(
        conn, [cold.id, hot.id], model="gpt-4o-mini"
    )
    assert copy_calls2 == 0


def test_json_output_shape(tmp_path: Path) -> None:
    conn = db.init_db(tmp_path / "leads.db")

    a = db.upsert_lead(conn, _candidate("Alpha"))
    b = db.upsert_lead(conn, _candidate("Beta"))
    c = db.upsert_lead(conn, _candidate("Gamma"))
    assert a.id is not None and b.id is not None and c.id is not None

    db.update_lead(conn, a.id, it_msp_score=80.0)
    db.update_lead(conn, b.id, it_msp_score=30.0)

    output = daily_run._build_output(conn)
    assert "generated_at" in output
    assert set(output["niches"].keys()) == {"it_msp", "mssp", "cloud"}

    it_leads = output["niches"]["it_msp"]
    assert len(it_leads) == 3
    assert [lead["name"] for lead in it_leads] == ["Alpha", "Beta", "Gamma"]
    assert it_leads[0]["score"] == 80.0
    assert it_leads[2]["score"] is None


def test_lead_to_json_signals_filtered_and_capped() -> None:
    base = _now()
    sigs: list[Signal] = []
    for i in range(10):
        sigs.append(
            _signal(
                SignalType.JOB_IT_SUPPORT,
                base - timedelta(days=i),
                payload={"title": f"role-{i}"},
            )
        )
    sigs.append(
        _signal(
            SignalType.LOCATION_CAPTURED,
            base,
            source=SourceName.COMPUTED,
            payload={"city": "Boston", "state": "MA"},
        )
    )
    sigs.append(
        _signal(
            SignalType.ENRICHMENT_RUN,
            base,
            source=SourceName.COMPUTED,
        )
    )

    lead = Lead(name="Test", name_key="test", signals=sigs)
    out = daily_run._lead_to_json(lead, NicheName.IT_MSP, now=base)
    assert len(out["signals"]) == 6
    types = {s["type"] for s in out["signals"]}
    assert "location_captured" not in types
    assert "enrichment_run" not in types


def test_lead_to_json_pulls_city_state_from_signal() -> None:
    base = _now()
    lead_with_loc = Lead(
        name="Loc Co",
        name_key="loc",
        signals=[
            _signal(
                SignalType.LOCATION_CAPTURED,
                base,
                source=SourceName.COMPUTED,
                payload={"city": "Austin", "state": "TX"},
            )
        ],
    )
    out = daily_run._lead_to_json(lead_with_loc, NicheName.IT_MSP, now=base)
    assert out["city"] == "Austin"
    assert out["state"] == "TX"

    lead_no_loc = Lead(name="Bare Co", name_key="bare", signals=[])
    out2 = daily_run._lead_to_json(lead_no_loc, NicheName.IT_MSP, now=base)
    assert out2["city"] is None
    assert out2["state"] is None
