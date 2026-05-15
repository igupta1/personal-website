"""Tests for `sources/business_filings.py`.

The fetcher hits OpenCorporates' Search API once per target
jurisdiction (us_fl, us_co, us_wa). We verify:
- The since-parameter clamp to _MAX_FILING_AGE_DAYS.
- Each jurisdiction's response is parsed into NEW_BUSINESS_FILED
  candidates with the right payload shape.
- Per-jurisdiction failures stay isolated.
- Network errors return an empty list, not an exception.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from msp_pipeline.models import SignalType, SourceName
from msp_pipeline.sources import business_filings


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _opencorp_response(jurisdiction: str, *, names: list[str]) -> dict[str, Any]:
    """Synthetic OpenCorporates v0.4 search response. Real responses
    nest companies under `results.companies[].company`."""
    state = jurisdiction.split("_")[-1].upper()
    today = _now().date().isoformat()
    return {
        "results": {
            "companies": [
                {
                    "company": {
                        "name": name,
                        "company_type": "Limited Liability Company",
                        "incorporation_date": today,
                        "registered_agent_name": f"Agent for {name}",
                        "opencorporates_url": (
                            f"https://opencorporates.com/companies/"
                            f"{jurisdiction}/{i:06d}"
                        ),
                        "jurisdiction_code": jurisdiction,
                        "_state_for_test": state,
                    }
                }
                for i, name in enumerate(names, start=1)
            ]
        }
    }


def test_state_from_jurisdiction() -> None:
    assert business_filings._state_from_jurisdiction("us_fl") == "FL"
    assert business_filings._state_from_jurisdiction("us_co") == "CO"
    assert business_filings._state_from_jurisdiction("us_wa") == "WA"
    # Defensive fallback for unexpected shapes.
    assert business_filings._state_from_jurisdiction("solo") == "SOLO"


def test_fetch_parses_opencorporates_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        params = kwargs.get("params") or {}
        jurisdiction = params.get("jurisdiction_code", "")
        names_per_state = {
            "us_fl": ["Pioneer Logistics LLC", "Sunshine Diner LLC"],
            "us_co": ["Rockies HVAC Inc"],
            "us_wa": ["Cascade Auto Repair LLC"],
        }
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(
            return_value=_opencorp_response(
                jurisdiction,
                names=names_per_state.get(jurisdiction, []),
            )
        )
        return resp

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        candidates = business_filings.fetch(since=_now() - timedelta(days=14))

    assert len(candidates) == 4
    by_name = {c.name: c.initial_signal for c in candidates}

    for name in (
        "Pioneer Logistics LLC",
        "Sunshine Diner LLC",
        "Rockies HVAC Inc",
        "Cascade Auto Repair LLC",
    ):
        assert name in by_name
        sig = by_name[name]
        assert sig.type == SignalType.NEW_BUSINESS_FILED
        assert sig.source == SourceName.FILINGS
        assert sig.payload["filing_type"] == "Limited Liability Company"
        assert sig.payload["filed_on"]
        assert sig.payload["registered_agent"].startswith("Agent for")

    # State extraction from jurisdiction round-trips into the payload.
    assert by_name["Pioneer Logistics LLC"].payload["state"] == "FL"
    assert by_name["Rockies HVAC Inc"].payload["state"] == "CO"
    assert by_name["Cascade Auto Repair LLC"].payload["state"] == "WA"


def test_since_clamped_to_max_filing_age(monkeypatch: pytest.MonkeyPatch) -> None:
    """A caller passing a since of 1 year ago should get clamped to
    no further back than _MAX_FILING_AGE_DAYS. We verify by inspecting
    the `incorporation_date` range param sent to OpenCorporates."""
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)
    captured_params: list[dict[str, Any]] = []

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        captured_params.append(dict(kwargs.get("params") or {}))
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value={"results": {"companies": []}})
        return resp

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        # 365 days back — should clamp to _MAX_FILING_AGE_DAYS (60).
        business_filings.fetch(since=_now() - timedelta(days=365))

    assert captured_params, "expected at least one HTTP call"
    first_range = str(captured_params[0]["incorporation_date"])
    start_iso = first_range.split(":", 1)[0]
    start_dt = datetime.fromisoformat(start_iso)
    age_days = (_now().date() - start_dt.date()).days
    # Within a day's tolerance of the cap.
    assert business_filings._MAX_FILING_AGE_DAYS - 1 <= age_days <= business_filings._MAX_FILING_AGE_DAYS + 1


def test_api_key_passed_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCORPORATES_API_KEY", "secret-key-123")
    captured_params: list[dict[str, Any]] = []

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        captured_params.append(dict(kwargs.get("params") or {}))
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value={"results": {"companies": []}})
        return resp

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        business_filings.fetch(since=_now() - timedelta(days=14))

    for params in captured_params:
        assert params.get("api_token") == "secret-key-123"


def test_network_failure_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP errors from one jurisdiction must not blow up the run."""
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        raise business_filings.requests.ConnectionError("network down")

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        candidates = business_filings.fetch(since=_now() - timedelta(days=14))

    assert candidates == []


def test_per_jurisdiction_failure_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If FL errors but CO and WA succeed, we still get CO+WA candidates."""
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        jurisdiction = (kwargs.get("params") or {}).get("jurisdiction_code", "")
        if jurisdiction == "us_fl":
            raise business_filings.requests.ConnectionError("FL down")
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(
            return_value=_opencorp_response(
                jurisdiction, names=[f"{jurisdiction} Co"]
            )
        )
        return resp

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        candidates = business_filings.fetch(since=_now() - timedelta(days=14))

    names = {c.name for c in candidates}
    assert names == {"us_co Co", "us_wa Co"}


def test_limit_caps_total(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        jurisdiction = (kwargs.get("params") or {}).get("jurisdiction_code", "")
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(
            return_value=_opencorp_response(
                jurisdiction, names=[f"{jurisdiction}-{i}" for i in range(10)]
            )
        )
        return resp

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        candidates = business_filings.fetch(
            since=_now() - timedelta(days=14), limit=5
        )

    assert len(candidates) == 5


def test_filing_older_than_since_is_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: if OpenCorporates returns a result outside the
    requested window, drop it locally."""
    monkeypatch.delenv("OPENCORPORATES_API_KEY", raising=False)
    very_old = (_now() - timedelta(days=400)).date().isoformat()

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(
            return_value={
                "results": {
                    "companies": [
                        {
                            "company": {
                                "name": "Old Co",
                                "incorporation_date": very_old,
                                "company_type": "LLC",
                            }
                        }
                    ]
                }
            }
        )
        return resp

    with patch.object(business_filings.requests, "get", side_effect=fake_get):
        candidates = business_filings.fetch(since=_now() - timedelta(days=14))

    assert candidates == []
