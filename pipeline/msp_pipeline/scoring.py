"""Per-niche signal-only scorer.

Score = clamp(sum(signal_weight × recency_decay), 0, 100), per niche.
No LLM. Pure deterministic. Industry and headcount live on the lead row
(written by ``enrichment.py``) but are UI filters, not score inputs.

One niche-specific override: the insurance niche zeros its score for
companies whose names match the insurance-vendor patterns below — we
don't want to pitch insurance to carriers / brokers / MGAs / TPAs.
The override lives here (not in `enrichment.purge_disqualified`) so
those companies remain valid leads for the MSP / MSSP / Cloud niches.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from msp_pipeline.models import Lead, NicheName, SignalType

HALF_LIFE_DAYS = 30.0
SCORE_MIN = 0.0
SCORE_MAX = 100.0

SIGNAL_WEIGHTS: dict[NicheName, dict[SignalType, float]] = {
    NicheName.IT_MSP: {
        # Rebalanced so every "primary" buying signal scores in the same
        # 33-39 single-fresh-signal range. Previously breach disclosures
        # at weight 40 swept the top 30 simply because state AGs report
        # them in volume; other signal types couldn't compete. The page
        # is a lead magnet — variety beats homogeneity.
        SignalType.JOB_IT_LEADERSHIP: 40,  # strongest MSP-buying signal
        SignalType.BREACH_DISCLOSED: 35,   # urgent IT pain (MSSP also high)
        SignalType.JOB_IT_SUPPORT: 35,     # growing IT team — MSP supplements
        SignalType.FUNDING_RAISED: 35,     # new cash, IT scale-up
        SignalType.EXEC_HIRED: 35,         # CISO/CIO/CTO triggers vendor eval
        SignalType.JOB_SECURITY: 15,       # primarily MSSP territory
        SignalType.JOB_CLOUD_DEVOPS: 15,   # primarily Cloud territory
    },
    NicheName.MSSP: {
        SignalType.BREACH_DISCLOSED: 45,
        SignalType.JOB_SECURITY: 40,
        SignalType.FUNDING_RAISED: 22,
        SignalType.JOB_IT_LEADERSHIP: 18,
        SignalType.EXEC_HIRED: 15,
        SignalType.JOB_CLOUD_DEVOPS: 12,
        SignalType.JOB_IT_SUPPORT: 10,
    },
    NicheName.CLOUD: {
        SignalType.JOB_CLOUD_DEVOPS: 40,
        SignalType.FUNDING_RAISED: 28,
        SignalType.JOB_IT_LEADERSHIP: 18,
        SignalType.EXEC_HIRED: 15,
        SignalType.JOB_SECURITY: 12,
        SignalType.JOB_IT_SUPPORT: 10,
        SignalType.BREACH_DISCLOSED: 8,
    },
    NicheName.INSURANCE: {
        # Strongest: a brand-new entity needs general liability, often
        # workers comp, sometimes property — the whole starter pack.
        SignalType.NEW_BUSINESS_FILED: 45,
        # Workers-comp trigger (hourly headcount growth).
        SignalType.JOB_BLUE_COLLAR: 38,
        # Commercial-auto trigger (new vehicles on the road).
        SignalType.JOB_FLEET_ROLE: 38,
        # The actual buyer — a new finance/HR lead handles renewals
        # and benefits shopping.
        SignalType.JOB_FINANCE_OPS: 35,
        # Renewal-handler turnover.
        SignalType.JOB_OPS_ROLE: 30,
        # Fresh cash → hiring → workers comp + group benefits + D&O.
        SignalType.FUNDING_RAISED: 28,
        # C-suite restructuring often re-opens vendor decisions.
        SignalType.EXEC_HIRED: 15,
        # Narrow cyber-insurance trigger; included for completeness.
        SignalType.BREACH_DISCLOSED: 8,
    },
}


# Insurance-vendor name patterns. When a lead's name matches any of
# these, its insurance-niche score is forced to 0 — we don't pitch
# insurance to other insurance companies. The lead remains in the
# pool with its non-insurance scores intact (cf. the IT-vendor purge
# in `enrichment.py`, which is global; this filter is niche-local).
_INSURANCE_VENDOR_NAME_RES: tuple[re.Pattern[str], ...] = (
    # "Acme Insurance Agency / Brokers / Services / Company / Group / ..."
    re.compile(
        r"\bInsurance\s+(?:Agency|Brokers?|Services|Company|Companies|"
        r"Corp(?:oration)?|Group|Solutions|Holdings|Partners|Advisors|"
        r"Associates|Specialists|Consultants)\b",
        re.IGNORECASE,
    ),
    # "Acme Mutual / Casualty / Indemnity / Surety [Co|Insurance|Group]"
    re.compile(
        r"\b(?:Mutual|Casualty|Indemnity|Surety)\s+(?:Insurance|Company|"
        r"Companies|Corp(?:oration)?|Group|Holdings)\b",
        re.IGNORECASE,
    ),
    # Reinsurers — always insurance-domain.
    re.compile(r"\bRe-?insurance\b", re.IGNORECASE),
    # MGAs / wholesalers / underwriters.
    re.compile(
        r"\b(?:MGA|Managing\s+General\s+Agent|Wholesale\s+Insurance|"
        r"Insurance\s+Wholesalers?|Underwriters)\b",
        re.IGNORECASE,
    ),
    # TPAs — third-party claims administrators.
    re.compile(
        r"\b(?:TPA|Third[-\s]?Party\s+Administrator)\b",
        re.IGNORECASE,
    ),
    # Independent claims adjusters / adjusting firms.
    re.compile(
        r"\b(?:Claims\s+Adjusters?|Adjusting\s+(?:Services|Company|Group)|"
        r"Independent\s+Adjusters?)\b",
        re.IGNORECASE,
    ),
)


def _is_insurance_vendor(name: str) -> bool:
    return any(p.search(name) for p in _INSURANCE_VENDOR_NAME_RES)


def score(lead: Lead, *, now: datetime | None = None) -> dict[NicheName, float]:
    if now is None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {niche: _score_niche(lead, niche, now) for niche in NicheName}


def _score_niche(lead: Lead, niche: NicheName, now: datetime) -> float:
    if niche == NicheName.INSURANCE and _is_insurance_vendor(lead.name):
        return 0.0
    weights = SIGNAL_WEIGHTS[niche]
    total = 0.0
    for sig in lead.signals:
        w = weights.get(sig.type)
        if w is None:
            continue
        total += w * _decay(sig.captured_at, now)
    return max(SCORE_MIN, min(SCORE_MAX, total))


def _decay(captured_at: datetime, now: datetime) -> float:
    days = max(0.0, (now - captured_at).total_seconds() / 86400.0)
    return 0.5 ** (days / HALF_LIFE_DAYS)
