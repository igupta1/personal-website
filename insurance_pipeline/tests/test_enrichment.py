"""Enrichment-disqualification tests for the insurance pipeline.

These tests exercise the pure-code disqualification path. The Gemini
lookup and Apollo enrichment paths have integration-level tests that
require live keys (run manually).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from insurance_pipeline import db, enrichment
from insurance_pipeline.models import (
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)


def _candidate(name: str) -> LeadCandidate:
    return LeadCandidate(
        name=name,
        initial_signal=Signal(
            type=SignalType.NEW_BUSINESS_FILED,
            source=SourceName.SOS_FL,
            captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
            payload={"state": "FL", "filing_type": "LLC"},
        ),
    )


def test_is_insurance_vendor_recognizes_carriers_and_brokers() -> None:
    matches = [
        "Acme Insurance Agency",
        "Acme Insurance Brokers, LLC",
        "Acme Insurance Services Inc.",
        "Acme Insurance Company",
        "Acme Mutual Insurance Co",
        "Acme Casualty Group",
        "Acme Indemnity Corp",
        "Acme Reinsurance Holdings",
        "Acme MGA",
        "Acme Managing General Agent",
        "Acme Wholesale Insurance",
        "Lloyd's Underwriters",
        "Acme TPA",
        "Acme Third Party Administrator",
        "Acme Claims Adjusters",
        "Acme Adjusting Services",
        "Acme Independent Adjusters",
    ]
    for name in matches:
        assert enrichment._is_insurance_vendor(name), f"expected match: {name!r}"


def test_is_insurance_vendor_ignores_non_insurance() -> None:
    for name in (
        "Pioneer Logistics LLC",
        "Acme Manufacturing",
        "Berger & Williams, LLP",
        "Acme IT Services",
        "Cloud Solutions Group",
        "Acme Property Management",
    ):
        assert not enrichment._is_insurance_vendor(name)


def test_disqualification_reason_for_gov_domain() -> None:
    from insurance_pipeline.models import Lead

    lead = Lead(name="State DMV", name_key="state_dmv", domain="state.ca.gov")
    assert enrichment._disqualification_reason(lead) is not None


def test_disqualification_reason_oversized() -> None:
    from insurance_pipeline.models import Lead

    lead = Lead(name="Mega Corp", name_key="mega", headcount=5000)
    reason = enrichment._disqualification_reason(lead)
    assert reason is not None and reason.startswith("oversized")


def test_disqualification_reason_insurance_vendor() -> None:
    from insurance_pipeline.models import Lead

    lead = Lead(name="Acme Insurance Agency", name_key="acme")
    assert enrichment._disqualification_reason(lead) == "insurance_vendor_name"


def test_disqualification_reason_megacorp_subsidiary() -> None:
    from insurance_pipeline.models import Lead

    for name in (
        "ATT MOBILITY LLC",
        "AT&T Wireless Inc.",
        "VERIZON CONNECT NWF INC",
        "Johnson Controls Fire Protection LP",
        "AECOM USA Inc",
        "CDM Federal Programs Corporation",
        "Tetra Tech NUS",
        "Compass PTS JV LLC",
        "Honeywell International Inc",
        "Microsoft Federal Sales LLC",
    ):
        lead = Lead(name=name, name_key=name.lower())
        assert enrichment._disqualification_reason(lead) == "megacorp_subsidiary", (
            f"expected megacorp_subsidiary for {name!r}"
        )


def test_megacorp_filter_only_matches_at_prefix() -> None:
    """Prefix-anchored — megacorp names buried mid-string don't trigger.
    The prefix match accepts the tradeoff that a real SMB named with a
    megacorp prefix (e.g. 'Boeing Avenue Diner LLC') would get caught;
    in practice companies don't name themselves after streets/locations
    that match major brands, so the false-positive risk is small and
    well below the noise that megacorp subsidiaries create."""
    for name in (
        "Smith & Verizon Plumbing Inc",  # 'Verizon' is in the middle, not prefix
        "Acme Honeywell Repair Service",  # mid-string
        "Pioneer Comcast Cabinetry LLC",  # mid-string
    ):
        assert not enrichment._is_megacorp_subsidiary(name), (
            f"expected NOT megacorp: {name!r}"
        )


def test_purge_disqualified_deletes_vendor_rows(tmp_path: Path) -> None:
    conn = db.init_db(tmp_path / "leads.db")
    keeper = db.upsert_lead(conn, _candidate("Pioneer Logistics LLC"))
    vendor = db.upsert_lead(conn, _candidate("Acme Insurance Brokers"))
    assert keeper.id and vendor.id

    purged = enrichment.purge_disqualified(conn)
    assert purged == 1
    assert db.get_lead(conn, lead_id=keeper.id) is not None
    assert db.get_lead(conn, lead_id=vendor.id) is None
