"""Deterministic policy-fit + premium estimation per signal.

Replaces the LLM-generated insight with a structured tagline an
insurance agent can triage in 2 seconds:

  "Commercial Auto $20K + WC $15K/yr"          ← FMCSA, 5 trucks 5 drivers
  "Cyber + E&O · est. $5.3K/yr"                ← Federal contract, $442K
  "D&O + EPLI · post-funding policy"           ← Form D / TechCrunch
  "GL + WC starter pack"                       ← New business filing

The premium estimates are heuristic — intended as a triage hint, not
a quote. The exact numbers don't have to be right; the *ordering* by
premium size has to be right, so an agent can sort their call list
by commission value.

Industry rate sources (rough industry-average blended rates):
- Commercial auto: $4-5K/truck/year for new authority carriers
  (higher risk surcharge, no claims history)
- Workers comp for truck drivers: $3K/driver/year (class code 7228
  typical range)
- Federal contract premium-to-revenue: 1-2% blended depending on
  vertical (Pollution Liability runs higher, Professional Liability
  lower)
"""

from __future__ import annotations

from typing import Any

from insurance_pipeline.models import Signal, SignalType


# NAICS-keyword → (coverage list, premium rate as fraction of contract).
# Order matters: first match wins. More specific verticals near the top.
_NAICS_COVERAGE_TABLE: tuple[tuple[tuple[str, ...], list[str], float], ...] = (
    (("HAZARDOUS", "REMEDIATION", "WASTE TREAT"),
     ["Pollution Liability", "GL", "Workers Comp"], 0.018),
    (("ARCHITECT", "ENGINEER"),
     ["Professional Liability (E&O)", "GL"], 0.012),
    (("CONSTRUCTION", "BUILDING CONSTRUCTION", "PREFABRICATED METAL"),
     ["GL", "Builders Risk", "Workers Comp"], 0.016),
    (("LABORATORY", "TESTING"),
     ["Professional Liability", "Cyber", "GL"], 0.012),
    (("ANALYTICAL LABORATORY INSTRUMENT",),
     ["Product Liability", "Cyber", "GL"], 0.014),
    (("SOFTWARE", "COMPUTING", "INTERNET", "DATA PROCESSING", "WEB"),
     ["Cyber", "E&O", "D&O"], 0.012),
    (("SECURITY SYSTEMS", "GUARD"),
     ["GL", "E&O", "Workers Comp"], 0.015),
    (("HEALTHCARE", "MEDICAL", "CLINIC"),
     ["Professional Liability (Malpractice)", "Cyber"], 0.018),
    (("FITNESS", "RECREATIONAL"),
     ["GL", "Professional Liability"], 0.012),
    (("CONSULTING", "MANAGEMENT", "ADMINISTRATIVE"),
     ["E&O", "D&O", "GL"], 0.010),
    (("WATER SUPPLY", "IRRIGATION"),
     ["GL", "Pollution Liability", "Property"], 0.014),
    (("MANUFACTURING", "FABRICATED METAL"),
     ["Product Liability", "GL", "Workers Comp"], 0.014),
    (("TRANSLATION", "INTERPRETATION", "PROFESSIONAL TRAINING"),
     ["E&O", "GL"], 0.008),
    (("DELIVERY", "MESSENGER", "LOCAL DELIVERY"),
     ["Commercial Auto", "GL", "Workers Comp"], 0.018),
)


def _coverages_for_naics(naics: str) -> tuple[list[str], float]:
    """Return (coverages, premium rate as fraction of contract)."""
    naics_upper = (naics or "").upper()
    for keywords, coverages, rate in _NAICS_COVERAGE_TABLE:
        if any(kw in naics_upper for kw in keywords):
            return coverages, rate
    # Default fallback for federal contractors with unknown NAICS.
    return ["Cyber", "E&O", "GL"], 0.012


def _safe_int(v: Any) -> int:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return 0


def _safe_float(v: Any) -> float:
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return 0.0


def _fmt_money_k(amount_usd: float) -> str:
    """Format dollars as compact 'K/yr' for ≤140-char taglines."""
    if amount_usd <= 0:
        return "—"
    if amount_usd < 1000:
        return f"${amount_usd:.0f}/yr"
    k = amount_usd / 1000.0
    # Drop trailing .0 for whole-thousands: "$5K/yr" not "$5.0K/yr".
    if k == int(k):
        return f"${int(k)}K/yr"
    if amount_usd < 10_000:
        return f"${k:.1f}K/yr"
    return f"${k:.0f}K/yr"


def _fmcsa_fit(sig: Signal) -> dict[str, Any]:
    trucks = _safe_int(sig.payload.get("fleet_size_power_units"))
    drivers = _safe_int(sig.payload.get("drivers"))
    # Commercial auto is per-truck; WC is per-driver. They're orthogonal.
    # When MCMIS reports trucks=0 but drivers>0 (incomplete MCS-150
    # filing), use drivers as a fallback for the truck count.
    if trucks > 0:
        auto_count = trucks
    elif drivers > 0:
        auto_count = drivers
    else:
        auto_count = 1
    auto_premium = auto_count * 5000
    wc_premium = drivers * 3000
    total = auto_premium + wc_premium

    coverages = ["Commercial Auto"]
    if wc_premium > 0:
        coverages.append("Workers Comp")

    if wc_premium > 0:
        tagline = (
            f"Commercial Auto {_fmt_money_k(auto_premium)} + "
            f"WC {_fmt_money_k(wc_premium)}"
        )
    else:
        tagline = f"Commercial Auto · est. {_fmt_money_k(auto_premium)}"

    return {
        "coverages": coverages,
        "est_annual_premium_usd": total,
        "tagline": tagline,
    }


def _federal_contract_fit(sig: Signal) -> dict[str, Any]:
    amount = _safe_float(sig.payload.get("amount_usd"))
    naics = sig.payload.get("naics") or ""
    coverages, rate = _coverages_for_naics(naics)
    estimated = amount * rate
    # Show the top two coverage products for brevity.
    cov_str = " + ".join(coverages[:2])
    tagline = f"{cov_str} · est. {_fmt_money_k(estimated)}"
    return {
        "coverages": coverages,
        "est_annual_premium_usd": estimated,
        "tagline": tagline,
    }


def _equity_funding_fit(sig: Signal) -> dict[str, Any]:
    """Form D / TechCrunch RSS funding — no amount typically available,
    so the tagline is qualitative."""
    return {
        "coverages": ["D&O", "EPLI", "Cyber"],
        "est_annual_premium_usd": None,
        "tagline": "D&O + EPLI · post-funding policy",
    }


def _new_entity_fit(sig: Signal) -> dict[str, Any]:
    """SoS new business filing — likely starter coverage need."""
    return {
        "coverages": ["GL", "Workers Comp", "Property"],
        "est_annual_premium_usd": None,
        "tagline": "GL + WC starter pack",
    }


def estimate_policy_fit(sig: Signal) -> dict[str, Any] | None:
    """Return policy-fit hint for an agent triaging this signal.

    Returns dict with:
      - coverages: list[str] of recommended P&C product lines
      - est_annual_premium_usd: float or None (rough heuristic)
      - tagline: short agent-facing string (≤80 chars)
    Or None if the signal type doesn't carry enough info.
    """
    if sig.type == SignalType.NEW_MOTOR_CARRIER_AUTHORITY:
        return _fmcsa_fit(sig)
    if sig.type == SignalType.FUNDING_RAISED:
        if sig.payload.get("filing_type") == "Federal contract":
            return _federal_contract_fit(sig)
        return _equity_funding_fit(sig)
    if sig.type == SignalType.NEW_BUSINESS_FILED:
        return _new_entity_fit(sig)
    return None


def trigger_type(sig: Signal) -> str:
    """Coarse classifier for UI filtering. Same enum used in LeadFilters."""
    if sig.type == SignalType.NEW_MOTOR_CARRIER_AUTHORITY:
        return "motor_carrier"
    if sig.type == SignalType.FUNDING_RAISED:
        ft = sig.payload.get("filing_type")
        if ft == "Federal contract":
            return "federal_contract"
        return "funding_event"
    if sig.type == SignalType.NEW_BUSINESS_FILED:
        return "new_entity"
    return "other"
