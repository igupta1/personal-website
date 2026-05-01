"""End-to-end pipeline runner.

Fetch -> upsert -> enrich -> score -> regenerate copy -> write JSON.
Per-source / per-lead failures log and continue. Designed to be run
nightly by GitHub Actions cron (M10).
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from msp_pipeline import db, enrichment, outreach, scoring
from msp_pipeline.models import Lead, LeadCandidate, NicheName, Signal, SignalType
from msp_pipeline.sources import breaches, funding, jobs

log = logging.getLogger("daily_run")

DEFAULT_DB_PATH = Path("data/leads.db")
DEFAULT_OUTPUT_PATH = Path("data/leads.json")
LOOKBACK_DAYS = 14
COPY_THRESHOLD = 40.0
COPY_DELTA_THRESHOLD = 10.0
SIGNAL_LIMIT_IN_JSON = 6


_SCORING_SIGNAL_TYPES: frozenset[SignalType] = frozenset(
    {
        SignalType.JOB_IT_SUPPORT,
        SignalType.JOB_IT_LEADERSHIP,
        SignalType.JOB_SECURITY,
        SignalType.JOB_CLOUD_DEVOPS,
        SignalType.EXEC_HIRED,
        SignalType.FUNDING_RAISED,
        SignalType.BREACH_DISCLOSED,
    }
)

_NICHE_SCORE_COL: dict[NicheName, str] = {
    NicheName.IT_MSP: "it_msp_score",
    NicheName.MSSP: "mssp_score",
    NicheName.CLOUD: "cloud_score",
}
_NICHE_INSIGHT_COL: dict[NicheName, str] = {
    NicheName.IT_MSP: "it_msp_insight",
    NicheName.MSSP: "mssp_insight",
    NicheName.CLOUD: "cloud_insight",
}
_NICHE_OUTREACH_COL: dict[NicheName, str] = {
    NicheName.IT_MSP: "it_msp_outreach",
    NicheName.MSSP: "mssp_outreach",
    NicheName.CLOUD: "cloud_outreach",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.dry_run:
        log.info("dry-run: skipping all writes (DB, LLM calls, JSON)")
        candidates = _fetch_all(
            since=_utcnow() - timedelta(days=LOOKBACK_DAYS),
            per_source_limit=args.limit,
        )
        log.info("dry-run fetched %d candidates total", len(candidates))
        return 0

    candidates = _fetch_all(
        since=_utcnow() - timedelta(days=LOOKBACK_DAYS),
        per_source_limit=args.limit,
    )
    log.info("fetched %d candidates across all sources", len(candidates))

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.init_db(args.db_path)

    upserted_ids = _upsert_all(conn, candidates)
    log.info("upserted %d unique leads", len(upserted_ids))

    enriched_ids = _enrich_all(conn, upserted_ids, force=args.reenrich)
    log.info("kept %d enriched leads", len(enriched_ids))

    rescored, copy_calls = _rescore_and_regen_copy(
        conn, enriched_ids, model=args.copy_model
    )
    log.info(
        "re-scored %d leads, generated copy %d times",
        len(rescored),
        copy_calls,
    )

    output = _build_output(conn)
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(output, indent=2, default=str))
    log.info("wrote %s", args.output_path)
    return 0


# --- Stages ----------------------------------------------------------------


def _fetch_all(
    *, since: datetime, per_source_limit: int | None
) -> list[LeadCandidate]:
    out: list[LeadCandidate] = []
    for name, fn in (
        ("jobs", jobs.fetch),
        ("funding", funding.fetch),
        ("breaches", breaches.fetch),
    ):
        try:
            cs = fn(since=since, limit=per_source_limit)
            log.info("source %s returned %d candidates", name, len(cs))
            out.extend(cs)
        except Exception as exc:
            log.exception("source %s failed: %s", name, exc)
    return out


def _upsert_all(
    conn: sqlite3.Connection, candidates: list[LeadCandidate]
) -> list[int]:
    ids: list[int] = []
    for c in candidates:
        try:
            lead = db.upsert_lead(conn, c)
            assert lead.id is not None
            ids.append(lead.id)
        except Exception:
            log.exception("upsert failed for %r", c.name)
    return list(dict.fromkeys(ids))


def _enrich_all(
    conn: sqlite3.Connection, lead_ids: list[int], *, force: bool
) -> list[int]:
    kept: list[int] = []
    for lead_id in lead_ids:
        lead = db.get_lead(conn, lead_id=lead_id)
        if lead is None:
            continue
        try:
            if enrichment.enrich(conn, lead, force=force):
                kept.append(lead_id)
        except Exception:
            log.exception(
                "enrichment failed for lead %s (id=%s)", lead.name, lead.id
            )
            kept.append(lead_id)
    return kept


def _rescore_and_regen_copy(
    conn: sqlite3.Connection, lead_ids: list[int], *, model: str
) -> tuple[list[int], int]:
    rescored: list[int] = []
    copy_calls = 0
    for lead_id in lead_ids:
        lead = db.get_lead(conn, lead_id=lead_id)
        if lead is None:
            continue
        new_scores = scoring.score(lead)
        updates: dict[str, Any] = {}
        for niche, new in new_scores.items():
            score_col = _NICHE_SCORE_COL[niche]
            old = getattr(lead, score_col)
            updates[score_col] = new
            should_regen = new >= COPY_THRESHOLD and (
                old is None or abs(new - old) > COPY_DELTA_THRESHOLD
            )
            if should_regen:
                try:
                    copy = outreach.generate(lead, niche, new, model=model)
                    updates[_NICHE_INSIGHT_COL[niche]] = copy.insight
                    updates[_NICHE_OUTREACH_COL[niche]] = copy.outreach
                    copy_calls += 1
                except Exception:
                    log.exception(
                        "outreach.generate failed for %s / %s",
                        lead.name,
                        niche.value,
                    )
        try:
            db.update_lead(conn, lead_id, **updates)
            rescored.append(lead_id)
        except Exception:
            log.exception("update_lead failed for id=%s", lead_id)
    return rescored, copy_calls


# --- JSON output -----------------------------------------------------------


def _build_output(conn: sqlite3.Connection) -> dict[str, Any]:
    now = _utcnow()
    return {
        "generated_at": now.isoformat(),
        "niches": {
            niche.value: [
                _lead_to_json(lead, niche, now=now)
                for lead in db.iter_leads(conn, niche=niche)
            ]
            for niche in NicheName
        },
    }


def _city_state(lead: Lead) -> tuple[str | None, str | None]:
    locs = sorted(
        (s for s in lead.signals if s.type == SignalType.LOCATION_CAPTURED),
        key=lambda s: s.captured_at,
        reverse=True,
    )
    if not locs:
        return None, None
    p = locs[0].payload
    return p.get("city"), p.get("state")


def _lead_to_json(
    lead: Lead, niche: NicheName, *, now: datetime
) -> dict[str, Any]:
    city, state = _city_state(lead)
    relevant = sorted(
        (s for s in lead.signals if s.type in _SCORING_SIGNAL_TYPES),
        key=lambda s: s.captured_at,
        reverse=True,
    )[:SIGNAL_LIMIT_IN_JSON]
    return {
        "name": lead.name,
        "domain": lead.domain,
        "industry": lead.industry,
        "headcount": lead.headcount,
        "country": lead.country,
        "city": city,
        "state": state,
        "score": getattr(lead, _NICHE_SCORE_COL[niche]),
        "insight": getattr(lead, _NICHE_INSIGHT_COL[niche]),
        "outreach": getattr(lead, _NICHE_OUTREACH_COL[niche]),
        "signals": [_signal_to_json(s, now) for s in relevant],
    }


def _signal_to_json(s: Signal, now: datetime) -> dict[str, Any]:
    return {
        "type": s.type.value,
        "captured_at": s.captured_at.isoformat(),
        "days_ago": max(0, (now - s.captured_at).days),
        "payload": s.payload,
    }


# --- CLI -------------------------------------------------------------------


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="msp_pipeline.daily_run")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--limit", type=int, default=None, help="Per-source candidate cap"
    )
    parser.add_argument(
        "--reenrich",
        action="store_true",
        help="Force re-enrichment on every lead",
    )
    parser.add_argument("--copy-model", default="gpt-4o-mini")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run sources only; skip DB writes, LLM calls, JSON write",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
