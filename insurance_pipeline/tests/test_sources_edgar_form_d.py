"""Smoke tests for the SEC EDGAR Form D source.

feedparser is mocked. Tests verify the company-name extraction, the
financial-entity filter, and defensive behavior on malformed entries.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from insurance_pipeline.models import SignalType, SourceName
from insurance_pipeline.sources import edgar_form_d


class _Entry:
    def __init__(
        self,
        title: str,
        *,
        term: str = "D",
        updated: str = "Fri, 15 May 2026 17:25:23 -0400",
        link: str = "https://example.com",
    ) -> None:
        self._d = {
            "title": title,
            "updated": updated,
            "published": updated,
            "link": link,
            "tags": [{"term": term, "scheme": "x"}],
        }

    def get(self, key: str, default: Any = None) -> Any:
        return self._d.get(key, default)


class _Feed:
    def __init__(self, entries: list[_Entry]):
        self.entries = entries


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def test_extract_company_name() -> None:
    assert edgar_form_d._extract_company_name(
        "D - Pioneer Robotics Inc. (0001234567) (Filer)"
    ) == "Pioneer Robotics Inc."
    assert edgar_form_d._extract_company_name(
        "D - Acme Health Co (0009999999) (Filer)"
    ) == "Acme Health Co"
    assert edgar_form_d._extract_company_name("malformed title") is None


def test_is_operating_company_filters_vc_funds() -> None:
    # Financial vehicles: drop
    for name in (
        "Acme Ventures III LP",
        "Pioneer Capital Partners",
        "Apex Holdings LLC",
        "Sequoia Fund 2026 LP",
        "Stellar Asset Management",
        "Hudson Family Office",
        "Smith Family Trust",
    ):
        assert not edgar_form_d._is_operating_company(name), (
            f"expected FILTERED: {name!r}"
        )

    # Operating companies: keep
    for name in (
        "Pioneer Robotics Inc.",
        "Acme Health Co",
        "Bright Solutions LLC",
        "Tier One Manufacturing Inc",
    ):
        assert edgar_form_d._is_operating_company(name), f"expected KEPT: {name!r}"


def test_fetch_extracts_form_d_operating_companies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entries = [
        _Entry("D - Pioneer Robotics Inc. (0001234567) (Filer)"),
        _Entry("D - Acme Ventures III LP (0009999998) (Filer)"),  # filtered out
        _Entry("DEFA14A - Some Issuer (000111) (Filer)", term="DEFA14A"),  # filtered out
        _Entry("D - Tier One Manufacturing Inc (0008888888) (Filer)"),
    ]

    def fake_parse(url: str, **kwargs: object) -> _Feed:
        return _Feed(entries)

    monkeypatch.setattr(edgar_form_d.feedparser, "parse", fake_parse)

    cands = edgar_form_d.fetch(since=_now() - timedelta(days=14))
    names = {c.name for c in cands}
    assert names == {"Pioneer Robotics Inc.", "Tier One Manufacturing Inc"}

    by_name = {c.name: c.initial_signal for c in cands}
    sig = by_name["Pioneer Robotics Inc."]
    assert sig.type == SignalType.FUNDING_RAISED
    assert sig.source == SourceName.FUNDING
    assert sig.payload["filing_type"] == "Form D"


def test_fetch_skips_old_filings(monkeypatch: pytest.MonkeyPatch) -> None:
    entries = [
        _Entry(
            "D - Pioneer Robotics Inc. (0001234567) (Filer)",
            updated="Mon, 01 Jan 2020 00:00:00 -0400",
        ),
    ]

    def fake_parse(url: str, **kwargs: object) -> _Feed:
        return _Feed(entries)

    monkeypatch.setattr(edgar_form_d.feedparser, "parse", fake_parse)
    cands = edgar_form_d.fetch(since=_now() - timedelta(days=14))
    assert cands == []


def test_fetch_handles_feedparser_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_parse(url: str, **kwargs: object) -> _Feed:
        raise RuntimeError("network down")

    monkeypatch.setattr(edgar_form_d.feedparser, "parse", fake_parse)
    assert edgar_form_d.fetch(since=_now() - timedelta(days=14)) == []
