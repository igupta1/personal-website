"""Classifier-focused tests for `sources/jobs_insurance.py`.

The three fetcher shells (jobspy / adzuna / hn) are line-for-line
identical to `sources/jobs.py` and are exercised by
`tests/test_sources_jobs.py`. The fork's novelty is the classifier
and the recruiter-filter parity, so that's what we cover here.
"""

from msp_pipeline.models import SignalType
from msp_pipeline.sources import jobs_insurance


def test_classify_finance_ops_titles() -> None:
    cases = [
        "Controller",
        "Chief Financial Officer",
        "CFO",
        "Director of Finance",
        "VP of Finance",
        "HR Director",
        "Director of Human Resources",
        "HR Manager",
        "Human Resources Manager",
        "Head of People",
        "Benefits Manager",
        "Benefits Administrator",
    ]
    for title in cases:
        assert jobs_insurance._classify_job_title(title) == SignalType.JOB_FINANCE_OPS, (
            f"expected JOB_FINANCE_OPS for {title!r}"
        )


def test_classify_fleet_titles() -> None:
    cases = [
        "CDL Class A Driver",
        "Owner Operator",
        "Owner-Operator",
        "Delivery Driver",
        "Route Driver",
        "OTR Driver",
        "Truck Driver",
        "Fleet Manager",
        "Fleet Coordinator",
        "Dispatcher",
        "Transportation Manager",
    ]
    for title in cases:
        assert jobs_insurance._classify_job_title(title) == SignalType.JOB_FLEET_ROLE, (
            f"expected JOB_FLEET_ROLE for {title!r}"
        )


def test_classify_blue_collar_titles() -> None:
    cases = [
        "Warehouse Associate",
        "Warehouse Worker",
        "Production Worker",
        "Production Operator",
        "Manufacturing Technician",
        "Machine Operator",
        "General Laborer",
        "Construction Laborer",
        "Construction Worker",
        "Forklift Operator",
        "Order Picker",
        "Picker-Packer",
        "Assembler",
        "Assembly Technician",
    ]
    for title in cases:
        assert jobs_insurance._classify_job_title(title) == SignalType.JOB_BLUE_COLLAR, (
            f"expected JOB_BLUE_COLLAR for {title!r}"
        )


def test_classify_ops_role_titles() -> None:
    cases = [
        "Office Manager",
        "Office Coordinator",
        "Office Administrator",
        "Operations Coordinator",
        "Operations Specialist",
        "Administrative Assistant",
        "Executive Assistant",
        "Admin Assistant",
    ]
    for title in cases:
        assert jobs_insurance._classify_job_title(title) == SignalType.JOB_OPS_ROLE, (
            f"expected JOB_OPS_ROLE for {title!r}"
        )


def test_classifier_priority_fleet_beats_ops() -> None:
    # "Fleet Manager" contains "Manager" but should not fall into
    # JOB_OPS_ROLE — fleet pattern fires first.
    assert (
        jobs_insurance._classify_job_title("Fleet Manager")
        == SignalType.JOB_FLEET_ROLE
    )


def test_classifier_priority_finance_beats_ops() -> None:
    # An "HR Manager" could be confused with a generic "manager" role.
    # Finance/HR pattern is checked first.
    assert (
        jobs_insurance._classify_job_title("HR Manager")
        == SignalType.JOB_FINANCE_OPS
    )


def test_classifier_returns_none_for_msp_titles() -> None:
    # Titles that the MSP jobs source picks up should NOT be claimed by
    # the insurance classifier — otherwise we'd double-emit on every
    # IT/security/cloud posting.
    msp_titles = [
        "Help Desk Technician",
        "Director of IT",
        "CISO",
        "Chief Information Security Officer",
        "Senior Security Engineer",
        "DevOps Engineer",
        "Cloud Engineer (AWS)",
        "VP of IT",
        "CIO",
        "CTO",
        "Network Administrator",
        "Sysadmin",
    ]
    for title in msp_titles:
        assert jobs_insurance._classify_job_title(title) is None, (
            f"insurance classifier should ignore MSP title {title!r}"
        )


def test_classifier_returns_none_for_unrelated_titles() -> None:
    for title in ("VP of Sales", "Marketing Manager", "Plumber", "Software Engineer"):
        assert jobs_insurance._classify_job_title(title) is None


def test_recruiter_name_filter_parity() -> None:
    # Insurance fork's recruiter filter must catch the same patterns
    # the MSP one does — staffing firms repost jobs on behalf of
    # unnamed clients regardless of which keywords were searched.
    assert jobs_insurance._is_recruiter_name("Liberty Personnel Services, Inc.")
    assert jobs_insurance._is_recruiter_name("Diati Staffing")
    assert jobs_insurance._is_recruiter_name("Eleven Recruiting")
    assert jobs_insurance._is_recruiter_name("Ringside Talent")
    assert not jobs_insurance._is_recruiter_name("Acme Construction")
    assert not jobs_insurance._is_recruiter_name("Pioneer Logistics LLC")
