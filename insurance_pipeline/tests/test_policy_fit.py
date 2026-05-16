"""Tests for policy_fit — the deterministic insight + premium estimator
that replaced LLM-generated insights."""

from __future__ import annotations

from datetime import datetime, timezone

from insurance_pipeline.models import Signal, SignalType, SourceName
from insurance_pipeline.policy_fit import estimate_policy_fit, trigger_type


def _sig(*, type: SignalType, payload: dict) -> Signal:
    return Signal(
        type=type,
        source=SourceName.FMCSA,
        captured_at=datetime.now(timezone.utc).replace(tzinfo=None),
        payload=payload,
    )


# --- FMCSA ----------------------------------------------------------


def test_fmcsa_owner_operator_one_truck_one_driver() -> None:
    sig = _sig(
        type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        payload={"fleet_size_power_units": 1, "drivers": 1},
    )
    fit = estimate_policy_fit(sig)
    assert fit is not None
    assert "Commercial Auto" in fit["coverages"]
    assert "Workers Comp" in fit["coverages"]
    # 1 truck × $5K + 1 driver × $3K = $8K total
    assert fit["est_annual_premium_usd"] == 8000
    assert "$5K" in fit["tagline"] and "$3K" in fit["tagline"]


def test_fmcsa_mid_fleet() -> None:
    sig = _sig(
        type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        payload={"fleet_size_power_units": 5, "drivers": 5},
    )
    fit = estimate_policy_fit(sig)
    # 5×5K + 5×3K = $40K
    assert fit["est_annual_premium_usd"] == 40000
    assert "$25K" in fit["tagline"]
    assert "$15K" in fit["tagline"]


def test_fmcsa_large_fleet() -> None:
    sig = _sig(
        type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        payload={"fleet_size_power_units": 34, "drivers": 36},
    )
    fit = estimate_policy_fit(sig)
    # 34×5K + 36×3K = $278K
    assert fit["est_annual_premium_usd"] == 34 * 5000 + 36 * 3000


def test_fmcsa_zero_size_uses_minimum() -> None:
    sig = _sig(
        type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        payload={"fleet_size_power_units": 0, "drivers": 0},
    )
    fit = estimate_policy_fit(sig)
    # max(0,0,1) = 1 truck. No drivers, so no WC.
    assert fit["est_annual_premium_usd"] == 5000
    assert "Workers Comp" not in fit["coverages"]


# --- Federal contracts ---------------------------------------------


def test_federal_contract_construction_uses_construction_coverages() -> None:
    sig = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={
            "filing_type": "Federal contract",
            "amount_usd": 200_000,
            "naics": "COMMERCIAL AND INSTITUTIONAL BUILDING CONSTRUCTION",
        },
    )
    fit = estimate_policy_fit(sig)
    assert "GL" in fit["coverages"]
    assert "Builders Risk" in fit["coverages"]
    # 200K × 0.016 = $3200
    assert 3000 <= fit["est_annual_premium_usd"] <= 3500


def test_federal_contract_engineering_uses_e_and_o() -> None:
    sig = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={
            "filing_type": "Federal contract",
            "amount_usd": 400_000,
            "naics": "ENGINEERING SERVICES",
        },
    )
    fit = estimate_policy_fit(sig)
    assert any("E&O" in c or "Professional Liability" in c for c in fit["coverages"])
    # 400K × 0.012 = $4800
    assert 4500 <= fit["est_annual_premium_usd"] <= 5100


def test_federal_contract_hazardous_uses_pollution_liability() -> None:
    sig = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={
            "filing_type": "Federal contract",
            "amount_usd": 442_000,
            "naics": "HAZARDOUS WASTE TREATMENT AND DISPOSAL",
        },
    )
    fit = estimate_policy_fit(sig)
    assert "Pollution Liability" in fit["coverages"]


def test_federal_contract_unknown_naics_default_fallback() -> None:
    sig = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={
            "filing_type": "Federal contract",
            "amount_usd": 100_000,
            "naics": "SOMETHING ELSE ENTIRELY",
        },
    )
    fit = estimate_policy_fit(sig)
    assert fit["coverages"] == ["Cyber", "E&O", "GL"]


# --- Form D / RSS funding ------------------------------------------


def test_form_d_funding_qualitative_tagline() -> None:
    sig = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={"filing_type": "Form D"},
    )
    fit = estimate_policy_fit(sig)
    assert fit["est_annual_premium_usd"] is None
    assert "D&O" in fit["coverages"]
    assert "post-funding" in fit["tagline"]


# --- Trigger type classifier --------------------------------------


def test_trigger_type_classifier() -> None:
    fmcsa = _sig(type=SignalType.NEW_MOTOR_CARRIER_AUTHORITY, payload={})
    assert trigger_type(fmcsa) == "motor_carrier"

    fed = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={"filing_type": "Federal contract"},
    )
    assert trigger_type(fed) == "federal_contract"

    formd = _sig(
        type=SignalType.FUNDING_RAISED,
        payload={"filing_type": "Form D"},
    )
    assert trigger_type(formd) == "funding_event"

    sos = _sig(type=SignalType.NEW_BUSINESS_FILED, payload={})
    assert trigger_type(sos) == "new_entity"
