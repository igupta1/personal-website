import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import feedparser

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)
from msp_pipeline.sources import funding as funding_module

_FIXTURES = Path(__file__).parent / "fixtures" / "funding"


def _make_candidate(name: str) -> LeadCandidate:
    return LeadCandidate(
        name=name,
        initial_signal=Signal(
            type=SignalType.FUNDING_RAISED,
            source=SourceName.FUNDING,
            captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
            payload={},
        ),
    )


def test_funding_sec_edgar_parses_fixture() -> None:
    fixture = json.loads((_FIXTURES / "sec_edgar.json").read_text())
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = fixture

    since = datetime(2020, 1, 1)
    with patch.object(funding_module.requests, "get", return_value=response):
        candidates = funding_module._fetch_from_sec_edgar(since)

    names = {c.name for c in candidates}
    assert names == {"Juliet Cybersecurity LLC", "Kilo Cloud Inc"}
    assert all(c.initial_signal.type == SignalType.FUNDING_RAISED for c in candidates)


def test_funding_techcrunch_parses_fixture() -> None:
    xml = (_FIXTURES / "techcrunch.xml").read_text()
    parsed = feedparser.parse(xml)

    since = datetime(2020, 1, 1)
    with patch.object(funding_module.feedparser, "parse", return_value=parsed):
        candidates = funding_module._fetch_from_techcrunch(since)

    titles = {c.name for c in candidates}
    assert titles == {
        "Lima Robotics raises $25M Series B",
        "Mike's AI secures $10M from a16z",
    }
    assert all(c.initial_signal.type == SignalType.FUNDING_RAISED for c in candidates)


def test_funding_prnewswire_parses_fixture() -> None:
    xml = (_FIXTURES / "prnewswire.xml").read_text()
    parsed = feedparser.parse(xml)

    since = datetime(2020, 1, 1)
    with patch.object(funding_module.feedparser, "parse", return_value=parsed):
        candidates = funding_module._fetch_from_prnewswire(since)

    titles = {c.name for c in candidates}
    assert titles == {
        "November Bank closes $50M Series C",
        "Oscar Insurance announces $30M raise",
    }


def test_funding_fetch_aggregates_and_continues_on_failure() -> None:
    a, b, c = _make_candidate("SEC Co"), _make_candidate("TC Co"), _make_candidate("PR Co")

    with patch.object(funding_module, "_fetch_from_sec_edgar", return_value=[a]), \
         patch.object(funding_module, "_fetch_from_techcrunch", return_value=[b]), \
         patch.object(funding_module, "_fetch_from_prnewswire", return_value=[c]):
        names = {x.name for x in funding_module.fetch(since=datetime(2020, 1, 1))}
    assert names == {"SEC Co", "TC Co", "PR Co"}

    def boom(_since: datetime) -> list[LeadCandidate]:
        raise RuntimeError("boom")

    with patch.object(funding_module, "_fetch_from_sec_edgar", side_effect=boom), \
         patch.object(funding_module, "_fetch_from_techcrunch", return_value=[b]), \
         patch.object(funding_module, "_fetch_from_prnewswire", return_value=[c]):
        names = {x.name for x in funding_module.fetch(since=datetime(2020, 1, 1))}
    assert names == {"TC Co", "PR Co"}
