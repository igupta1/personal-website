import csv
import io
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from msp_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)

_log = logging.getLogger(__name__)

_HHS_URL = "https://ocrportal.hhs.gov/ocr/breach/breach_report.csv"
_CA_AG_URL = "https://oag.ca.gov/privacy/databreach/list"
_ME_AG_URL = "https://www.maine.gov/AG/consumer/data_breach/index.shtml"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_us_date(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
    return None


def _fetch_from_hhs(since: datetime) -> list[LeadCandidate]:
    captured_at = _utcnow()
    candidates: list[LeadCandidate] = []
    try:
        response = requests.get(_HHS_URL, timeout=30)
        response.raise_for_status()
        text = response.text
    except Exception:
        _log.exception("hhs fetch failed")
        return []

    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        entity = (row.get("Name of Covered Entity") or "").strip()
        if not entity:
            continue
        date_str = row.get("Breach Submission Date") or ""
        disclosure = _parse_us_date(date_str)
        if disclosure and disclosure < since:
            continue
        candidates.append(
            LeadCandidate(
                name=entity,
                domain=None,
                initial_signal=Signal(
                    type=SignalType.BREACH_DISCLOSED,
                    source=SourceName.BREACHES,
                    captured_at=captured_at,
                    payload={
                        "agency": "hhs",
                        "submission_date": date_str,
                        "individuals_affected": row.get("Individuals Affected") or "",
                        "type_of_breach": row.get("Type of Breach") or "",
                        "state": row.get("State") or "",
                    },
                ),
            )
        )
    return candidates


def _fetch_html_table(url: str, agency: str, since: datetime) -> list[LeadCandidate]:
    captured_at = _utcnow()
    candidates: list[LeadCandidate] = []
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        html = response.text
    except Exception:
        _log.exception("%s fetch failed", agency)
        return []

    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            entity = tds[0].get_text(strip=True)
            if not entity:
                continue
            date_str = tds[-1].get_text(strip=True)
            disclosure = _parse_us_date(date_str)
            if disclosure and disclosure < since:
                continue
            candidates.append(
                LeadCandidate(
                    name=entity,
                    domain=None,
                    initial_signal=Signal(
                        type=SignalType.BREACH_DISCLOSED,
                        source=SourceName.BREACHES,
                        captured_at=captured_at,
                        payload={
                            "agency": agency,
                            "reported_date": date_str,
                        },
                    ),
                )
            )
    return candidates


def _fetch_from_ca_ag(since: datetime) -> list[LeadCandidate]:
    return _fetch_html_table(_CA_AG_URL, "ca_ag", since)


def _fetch_from_me_ag(since: datetime) -> list[LeadCandidate]:
    return _fetch_html_table(_ME_AG_URL, "me_ag", since)


def fetch(*, since: datetime, limit: int | None = None) -> list[LeadCandidate]:
    candidates: list[LeadCandidate] = []
    fetchers = (
        ("hhs", _fetch_from_hhs),
        ("ca_ag", _fetch_from_ca_ag),
        ("me_ag", _fetch_from_me_ag),
    )
    for name, fetcher in fetchers:
        try:
            candidates.extend(fetcher(since))
        except Exception:
            _log.exception("fetcher %s failed entirely", name)
    if limit is not None:
        candidates = candidates[:limit]
    return candidates
