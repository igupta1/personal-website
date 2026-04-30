import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from msp_pipeline.db import (
    _name_key,
    append_signal,
    get_lead,
    init_db,
    iter_leads,
    update_lead,
    upsert_lead,
)
from msp_pipeline.models import (
    LeadCandidate,
    NicheName,
    Signal,
    SignalType,
    SourceName,
)


def _candidate(
    name: str,
    *,
    signal_type: SignalType = SignalType.JOB_IT_SUPPORT,
    source: SourceName = SourceName.JOBS,
    payload: dict[str, Any] | None = None,
) -> LeadCandidate:
    return LeadCandidate(
        name=name,
        initial_signal=Signal(
            type=signal_type,
            source=source,
            captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
            payload=payload or {},
        ),
    )


def _signal() -> Signal:
    return Signal(
        type=SignalType.JOB_IT_SUPPORT,
        source=SourceName.JOBS,
        captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
        payload={},
    )


def test_init_db_idempotent(tmp_path: Path) -> None:
    p = tmp_path / "leads.db"
    init_db(p)
    init_db(p)


def test_upsert_inserts_new_then_merges_fuzzy(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")

    a = upsert_lead(conn, _candidate("Acme Inc"))
    assert a.id is not None
    assert a.name_key == "acme"
    assert len(a.signals) == 1

    b = upsert_lead(conn, _candidate("Acme Inc."))
    assert b.id == a.id
    assert len(b.signals) == 2

    c = upsert_lead(conn, _candidate("Acme Industries"))
    assert c.id != a.id
    assert len(c.signals) == 1


def test_append_signal_preserves_prior(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    a = upsert_lead(conn, _candidate("Foo Co"))
    assert a.id is not None

    sig2 = Signal(
        type=SignalType.FUNDING_RAISED,
        source=SourceName.FUNDING,
        captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
        payload={"amount_usd": 1_000_000},
    )
    append_signal(conn, a.id, sig2)

    result = get_lead(conn, lead_id=a.id)
    assert result is not None
    assert len(result.signals) == 2
    assert result.signals[0].type == SignalType.JOB_IT_SUPPORT
    assert result.signals[1].type == SignalType.FUNDING_RAISED
    assert result.signals[1].payload == {"amount_usd": 1_000_000}


def test_iter_leads_orders_by_niche_score(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    a = upsert_lead(conn, _candidate("Alpha Co"))
    b = upsert_lead(conn, _candidate("Beta Co"))
    c = upsert_lead(conn, _candidate("Gamma Co"))
    assert a.id is not None and b.id is not None and c.id is not None

    update_lead(conn, a.id, it_msp_score=50.0)
    update_lead(conn, b.id, it_msp_score=80.0)

    results = list(iter_leads(conn, niche=NicheName.IT_MSP))
    assert len(results) == 3
    scores = [r.it_msp_score for r in results]
    assert scores == [80.0, 50.0, None]


def test_update_lead_persists_and_bumps_updated_at(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    a = upsert_lead(conn, _candidate("Delta Co"))
    assert a.id is not None and a.updated_at is not None

    time.sleep(0.01)
    update_lead(conn, a.id, industry="saas", headcount=120, country="US")

    result = get_lead(conn, lead_id=a.id)
    assert result is not None
    assert result.industry == "saas"
    assert result.headcount == 120
    assert result.country == "US"
    assert result.updated_at is not None
    assert result.updated_at > a.updated_at


def test_get_lead_requires_exactly_one_arg(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    with pytest.raises(ValueError):
        get_lead(conn)
    with pytest.raises(ValueError):
        get_lead(conn, lead_id=1, name_key="acme")


def test_iter_leads_min_score_filter(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    a = upsert_lead(conn, _candidate("Alpha Co"))
    b = upsert_lead(conn, _candidate("Beta Co"))
    c = upsert_lead(conn, _candidate("Gamma Co"))
    assert a.id is not None and b.id is not None and c.id is not None
    update_lead(conn, a.id, it_msp_score=50.0)
    update_lead(conn, b.id, it_msp_score=80.0)
    update_lead(conn, c.id, it_msp_score=30.0)

    results = list(iter_leads(conn, niche=NicheName.IT_MSP, min_score=50.0))
    assert [r.it_msp_score for r in results] == [80.0, 50.0]


def test_iter_leads_limit(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    upsert_lead(conn, _candidate("Alpha Co"))
    upsert_lead(conn, _candidate("Beta Co"))
    upsert_lead(conn, _candidate("Gamma Co"))
    results = list(iter_leads(conn, limit=2))
    assert len(results) == 2


def test_iter_leads_min_score_without_niche_raises(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    with pytest.raises(ValueError):
        list(iter_leads(conn, min_score=50.0))


def test_update_lead_rejects_unknown_field(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    a = upsert_lead(conn, _candidate("Foo Co"))
    assert a.id is not None
    with pytest.raises(ValueError):
        update_lead(conn, a.id, secret_field="x")


def test_update_lead_raises_on_missing_id(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    with pytest.raises(ValueError):
        update_lead(conn, 99999, industry="saas")


def test_append_signal_raises_on_missing_id(tmp_path: Path) -> None:
    conn = init_db(tmp_path / "leads.db")
    with pytest.raises(ValueError):
        append_signal(conn, 99999, _signal())


def test_name_key_unicode_and_legal_suffixes() -> None:
    assert _name_key("Café Inc") == "cafe"
    assert _name_key("Acme Corporation") == "acme"
    assert _name_key("Acme & Co, LLC") == "acme"
    assert _name_key("naïve Ltd") == "naive"
    with pytest.raises(ValueError):
        _name_key("Inc.")
    with pytest.raises(ValueError):
        _name_key("LLC")
    with pytest.raises(ValueError):
        _name_key("")
