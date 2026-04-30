from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)
from msp_pipeline.sources import breaches as breaches_module

_FIXTURES = Path(__file__).parent / "fixtures" / "breaches"


def _make_candidate(name: str) -> LeadCandidate:
    return LeadCandidate(
        name=name,
        initial_signal=Signal(
            type=SignalType.BREACH_DISCLOSED,
            source=SourceName.BREACHES,
            captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
            payload={},
        ),
    )


def test_breaches_hhs_parses_fixture() -> None:
    csv_text = (_FIXTURES / "hhs.csv").read_text()
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.text = csv_text

    since = datetime(2020, 1, 1)
    with patch.object(breaches_module.requests, "get", return_value=response):
        candidates = breaches_module._fetch_from_hhs(since)

    names = {c.name for c in candidates}
    assert names == {"Tango Hospital", "Uniform Medical Group"}
    assert all(c.initial_signal.type == SignalType.BREACH_DISCLOSED for c in candidates)


def test_breaches_ca_ag_parses_fixture() -> None:
    html = (_FIXTURES / "ca_ag.html").read_text()
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.text = html

    since = datetime(2020, 1, 1)
    with patch.object(breaches_module.requests, "get", return_value=response):
        candidates = breaches_module._fetch_from_ca_ag(since)

    names = {c.name for c in candidates}
    assert names == {"Victor Financial", "Whiskey Tech Inc"}


def test_breaches_me_ag_parses_fixture() -> None:
    html = (_FIXTURES / "me_ag.html").read_text()
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.text = html

    since = datetime(2020, 1, 1)
    with patch.object(breaches_module.requests, "get", return_value=response):
        candidates = breaches_module._fetch_from_me_ag(since)

    names = {c.name for c in candidates}
    assert names == {"X-ray Insurance", "Yankee Banking"}


def test_breaches_fetch_aggregates_and_continues_on_failure() -> None:
    a, b, c = _make_candidate("HHS Co"), _make_candidate("CA Co"), _make_candidate("ME Co")

    with patch.object(breaches_module, "_fetch_from_hhs", return_value=[a]), \
         patch.object(breaches_module, "_fetch_from_ca_ag", return_value=[b]), \
         patch.object(breaches_module, "_fetch_from_me_ag", return_value=[c]):
        names = {x.name for x in breaches_module.fetch(since=datetime(2020, 1, 1))}
    assert names == {"HHS Co", "CA Co", "ME Co"}

    def boom(_since: datetime) -> list[LeadCandidate]:
        raise RuntimeError("boom")

    with patch.object(breaches_module, "_fetch_from_hhs", side_effect=boom), \
         patch.object(breaches_module, "_fetch_from_ca_ag", return_value=[b]), \
         patch.object(breaches_module, "_fetch_from_me_ag", return_value=[c]):
        names = {x.name for x in breaches_module.fetch(since=datetime(2020, 1, 1))}
    assert names == {"CA Co", "ME Co"}
