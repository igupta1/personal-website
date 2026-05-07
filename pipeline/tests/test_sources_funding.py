from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

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


def test_funding_sec_edgar_fetcher_is_removed() -> None:
    """SEC EDGAR Form D was almost exclusively private fund formations, not
    operating businesses. Fetcher dropped; no replacement."""
    assert not hasattr(funding_module, "_fetch_from_sec_edgar")
    assert not hasattr(funding_module, "_SEC_EDGAR_API")


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
    b, c = _make_candidate("TC Co"), _make_candidate("PR Co")

    with patch.object(funding_module, "_fetch_from_techcrunch", return_value=[b]), \
         patch.object(funding_module, "_fetch_from_prnewswire", return_value=[c]):
        names = {x.name for x in funding_module.fetch(since=datetime(2020, 1, 1))}
    assert names == {"TC Co", "PR Co"}

    def boom(_since: datetime) -> list[LeadCandidate]:
        raise RuntimeError("boom")

    with patch.object(funding_module, "_fetch_from_techcrunch", side_effect=boom), \
         patch.object(funding_module, "_fetch_from_prnewswire", return_value=[c]):
        names = {x.name for x in funding_module.fetch(since=datetime(2020, 1, 1))}
    assert names == {"PR Co"}


def test_is_funding_title_filters_non_funding_pr_newswire_noise() -> None:
    # Funding announcements pass.
    assert funding_module._is_funding_title("Acme Corp raises $25M Series B")
    assert funding_module._is_funding_title("Beta Inc secures $10M seed funding")
    assert funding_module._is_funding_title("Gamma closes $5M round")

    # PR Newswire class-action / regulatory noise is filtered.
    assert not funding_module._is_funding_title(
        "Rosen Law Firm Encourages Zillow Group, Inc. Investors to Inquire "
        "About Securities Class Action Investigation - Z, ZG"
    )
    assert not funding_module._is_funding_title(
        "ACME Pharma Recall Notice - Dietary Supplement"
    )
    assert not funding_module._is_funding_title(
        "Reminder: Shareholder Investigation in XYZ Corp"
    )
    # Empty / blank.
    assert not funding_module._is_funding_title("")
    assert not funding_module._is_funding_title("   ")


def test_clean_company_name_strips_cik_suffix() -> None:
    assert funding_module._clean_company_name(
        "Foo Corp  (CIK 0001234567)"
    ) == "Foo Corp"
    assert funding_module._clean_company_name(
        "Bar LLC (CIK 12345)"
    ) == "Bar LLC"
    # Names without the suffix are unchanged.
    assert funding_module._clean_company_name("Bare Co") == "Bare Co"
