"""End-to-end pipeline runner for the insurance niche.

fetch -> upsert -> enrich -> purge -> score -> apollo -> regen -> write -> upload.
Single-niche, so no NicheName loops. Output JSON shape is
``{generated_at, leads: [...]}`` — flat, matches what
``api/generate-insurance-leads.js`` expects.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from insurance_pipeline import apollo, db, enrichment, outreach, scoring
from insurance_pipeline.models import (
    Lead,
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)
from insurance_pipeline.sources import edgar_form_d, fmcsa, funding, usaspending

# sos_fl module exists but is NOT wired here. The SunBiz search UI is
# fronted by Cloudflare's bot challenge ("Just a moment..." page),
# which blocks plain `requests` scraping from GitHub Actions runners.
# Real shipping requires either a headless browser (Playwright) or a
# paid Cloudflare-bypass service. Keeping the source code on disk so
# the parser logic is preserved for when we wire one of those in.

log = logging.getLogger("insurance.daily_run")

DEFAULT_DB_PATH = Path("data/leads.db")
DEFAULT_OUTPUT_PATH = Path("data/leads.json")
LOOKBACK_DAYS = 60  # matches business_filings._MAX_FILING_AGE_DAYS conceptually
INSIGHT_THRESHOLD = 20.0
INSIGHT_DELTA_THRESHOLD = 10.0
SIGNAL_LIMIT_IN_JSON = 6
APOLLO_TOP_N_DEFAULT = 30


_SCORING_SIGNAL_TYPES: frozenset[SignalType] = frozenset(
    {
        SignalType.NEW_MOTOR_CARRIER_AUTHORITY,
        SignalType.NEW_BUSINESS_FILED,
        SignalType.OSHA_INSPECTION_RECORDED,
        SignalType.BUILDING_PERMIT_ISSUED,
        SignalType.FUNDING_RAISED,
    }
)


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

    if args.rescore_only:
        log.info("rescore-only: skipping fetch / upsert")
        args.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = db.init_db(args.db_path)

        if args.reenrich:
            existing_ids = [
                lead.id for lead in db.iter_leads(conn) if lead.id is not None
            ]
            log.info("re-enriching %d existing leads (force=True)", len(existing_ids))
            kept_ids = _enrich_all(conn, existing_ids, force=True)
            log.info("kept %d leads after re-enrichment", len(kept_ids))

        modified = db.dedup_signals_pass(conn)
        log.info("deduped signals on %d leads", modified)

        purged = enrichment.purge_disqualified(conn)
        log.info("purged %d disqualified leads", purged)

        all_ids = [lead.id for lead in db.iter_leads(conn) if lead.id is not None]
        log.info("re-scoring %d leads", len(all_ids))

        score_changes = _rescore_all(conn, all_ids)
        apollo_ids = _apollo_enrich_top_n(conn, n=args.apollo_top_n)
        fallback_dms = _apply_source_dm_fallbacks(conn)
        copy_calls = _regen_copy(
            conn, score_changes,
            force_regen_ids=apollo_ids,
            model=args.copy_model,
        )
        log.info(
            "re-scored %d, apollo-enriched %d, fallback-DMs %d, regen %d",
            len(score_changes), len(apollo_ids), fallback_dms, copy_calls,
        )

        output = _build_output(conn)
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(json.dumps(output, indent=2, default=str))
        log.info("wrote %s", args.output_path)

        if args.upload:
            try:
                _upload_blob(args.output_path)
            except Exception:
                log.exception("upload failed")
                return 1
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

    purged = enrichment.purge_disqualified(conn)
    log.info("purged %d disqualified leads", purged)
    enriched_ids = [lid for lid in enriched_ids if db.get_lead(conn, lead_id=lid)]

    score_changes = _rescore_all(conn, enriched_ids)
    apollo_ids = _apollo_enrich_top_n(conn, n=args.apollo_top_n)
    fallback_dms = _apply_source_dm_fallbacks(conn)
    copy_calls = _regen_copy(
        conn, score_changes,
        force_regen_ids=apollo_ids,
        model=args.copy_model,
    )
    log.info(
        "re-scored %d, apollo-enriched %d, fallback-DMs %d, regen %d",
        len(score_changes), len(apollo_ids), fallback_dms, copy_calls,
    )

    output = _build_output(conn)
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(output, indent=2, default=str))
    log.info("wrote %s", args.output_path)

    if args.upload:
        try:
            _upload_blob(args.output_path)
        except Exception:
            log.exception("upload failed")
            return 1
    return 0


# --- Stages ----------------------------------------------------------------


# Source registry. Each entry is (name, fetch-callable). FMCSA is
# deferred to a follow-up — MCMIS bulk-data integration needs its own
# iteration. v1 ships with funding + SunBiz.
_SOURCES: tuple[tuple[str, Any], ...] = (
    ("fmcsa", fmcsa.fetch),
    ("funding", funding.fetch),
    ("edgar_form_d", edgar_form_d.fetch),
    ("usaspending", usaspending.fetch),
)


def _fetch_all(
    *, since: datetime, per_source_limit: int | None
) -> list[LeadCandidate]:
    out: list[LeadCandidate] = []
    for name, fn in _SOURCES:
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


def _rescore_all(
    conn: sqlite3.Connection, lead_ids: list[int]
) -> dict[int, tuple[float | None, float]]:
    """Compute fresh score for each lead and persist it. Returns per-lead
    (old, new) so the regen stage can apply threshold-crossing logic."""
    changes: dict[int, tuple[float | None, float]] = {}
    for lead_id in lead_ids:
        lead = db.get_lead(conn, lead_id=lead_id)
        if lead is None:
            continue
        new_score = scoring.score(lead)
        old = lead.score
        try:
            db.update_lead(conn, lead_id, score=new_score)
            changes[lead_id] = (old, new_score)
        except Exception:
            log.exception("rescore: update_lead failed for id=%s", lead_id)
    return changes


def _apply_source_dm_fallbacks(conn: sqlite3.Connection) -> int:
    """For leads where Apollo + Gemini both failed to populate dm_name,
    fall back to the source signal's officer/contact field. Today only
    FMCSA carries a usable officer name in its payload; structured the
    same way other US-only sources could add fallbacks later.

    Returns the number of leads where a fallback was applied."""
    filled = 0
    for lead in db.iter_leads(conn):
        if lead.id is None or lead.dm_name:
            continue
        for sig in lead.signals:
            if sig.type != SignalType.NEW_MOTOR_CARRIER_AUTHORITY:
                continue
            officer = (sig.payload.get("officer_name") or "").strip()
            if not officer:
                continue
            try:
                db.update_lead(
                    conn, lead.id,
                    dm_name=officer,
                    dm_title=lead.dm_title or "Owner / Officer",
                )
                filled += 1
            except Exception:
                log.exception("dm-fallback: update_lead failed for id=%s", lead.id)
            break
    return filled


def _apollo_enrich_top_n(conn: sqlite3.Connection, *, n: int) -> set[int]:
    """Top-N by score, run Apollo on each lead not yet marked. Returns
    the set of IDs that received fresh DM data — the regen stage uses
    this to force outreach regeneration so the new DM can be referenced.
    Skips silently if APOLLO_API_KEY is unset."""
    if not apollo.is_configured():
        log.info("apollo: APOLLO_API_KEY not set, skipping DM enrichment")
        return set()

    target_ids: set[int] = set()
    for top_lead in db.iter_leads(conn, limit=n):
        if top_lead.id is None or top_lead.score is None:
            continue
        target_ids.add(top_lead.id)

    enriched: set[int] = set()
    for lead_id in target_ids:
        lead = db.get_lead(conn, lead_id=lead_id)
        if lead is None:
            continue
        if any(s.type == SignalType.APOLLO_ENRICHED for s in lead.signals):
            continue
        try:
            result = apollo.find_decision_maker(lead.name, lead.domain)
        except Exception:
            log.exception("apollo: lookup raised for %r", lead.name)
            continue

        if not result.org_found:
            continue

        if result.headcount is not None and result.headcount > enrichment._SMB_HEADCOUNT_CAP:
            log.info(
                "apollo: deleting lead %d (%s) — apollo headcount=%d exceeds SMB cap",
                lead_id, lead.name, result.headcount,
            )
            try:
                db.delete_lead(conn, lead_id)
            except Exception:
                log.exception("apollo: delete_lead failed for id=%s", lead_id)
            continue

        updates: dict[str, Any] = {}
        if result.dm_name:
            updates["dm_name"] = result.dm_name
        if result.dm_title:
            updates["dm_title"] = result.dm_title
        if result.dm_email:
            updates["dm_email"] = result.dm_email
        if result.dm_linkedin_url:
            updates["dm_linkedin_url"] = result.dm_linkedin_url
        if result.headcount is not None:
            updates["headcount"] = result.headcount
        if updates:
            try:
                db.update_lead(conn, lead_id, **updates)
                if result.dm_found:
                    enriched.add(lead_id)
            except Exception:
                log.exception("apollo: update_lead failed for id=%s", lead_id)

        try:
            db.append_signal(
                conn, lead_id,
                Signal(
                    type=SignalType.APOLLO_ENRICHED,
                    source=SourceName.APOLLO,
                    captured_at=_utcnow(),
                    payload={
                        "dm_found": result.dm_found,
                        "apollo_person_id": result.apollo_person_id,
                    },
                ),
            )
        except Exception:
            log.exception("apollo: append marker failed for id=%s", lead_id)

    return enriched


def _regen_copy(
    conn: sqlite3.Connection,
    score_changes: dict[int, tuple[float | None, float]],
    *,
    force_regen_ids: set[int],
    model: str,
) -> int:
    del force_regen_ids  # insight prompt has no DM-aware greeting; kept for parity
    copy_calls = 0
    for lead_id, (old, new) in score_changes.items():
        lead = db.get_lead(conn, lead_id=lead_id)
        if lead is None:
            continue
        insight_missing = lead.insight is None
        should_regen = new >= INSIGHT_THRESHOLD and (
            insight_missing
            or old is None
            or old < INSIGHT_THRESHOLD
            or abs(new - old) > INSIGHT_DELTA_THRESHOLD
        )
        updates: dict[str, Any] = {}
        if should_regen:
            try:
                copy = outreach.generate(lead, new, model=model)
                updates["insight"] = copy.insight
                copy_calls += 1
            except Exception:
                log.exception("outreach.generate failed for %s", lead.name)
        elif new < INSIGHT_THRESHOLD and lead.insight is not None:
            updates["insight"] = None
        if updates:
            try:
                db.update_lead(conn, lead_id, **updates)
            except Exception:
                log.exception("regen: update_lead failed for id=%s", lead_id)
    return copy_calls


# --- JSON output -----------------------------------------------------------


def _build_output(conn: sqlite3.Connection) -> dict[str, Any]:
    now = _utcnow()
    return {
        "generated_at": now.isoformat(),
        "leads": [
            _lead_to_json(lead, now=now)
            for lead in db.iter_leads(conn)
        ],
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


def _lead_to_json(lead: Lead, *, now: datetime) -> dict[str, Any]:
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
        "dm_name": lead.dm_name,
        "dm_title": lead.dm_title,
        "dm_email": lead.dm_email,
        "dm_linkedin_url": lead.dm_linkedin_url,
        "score": lead.score,
        "insight": lead.insight,
        "signals": [_signal_to_json(s, now) for s in relevant],
    }


def _signal_to_json(s: Signal, now: datetime) -> dict[str, Any]:
    return {
        "type": s.type.value,
        "captured_at": s.captured_at.isoformat(),
        "days_ago": max(0, (now - s.captured_at).days),
        "payload": s.payload,
    }


# --- Upload ----------------------------------------------------------------


def _upload_blob(json_path: Path) -> None:
    url = os.environ.get("INSURANCE_UPLOAD_URL")
    api_key = os.environ.get("INSURANCE_UPLOAD_API_KEY")
    if not url or not api_key:
        raise RuntimeError(
            "--upload requires INSURANCE_UPLOAD_URL and INSURANCE_UPLOAD_API_KEY env vars"
        )
    payload = json.loads(json_path.read_text())
    resp = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
    )
    resp.raise_for_status()
    log.info("uploaded blob: %s", resp.json())


# --- CLI -------------------------------------------------------------------


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="insurance_pipeline.daily_run")
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
    parser.add_argument(
        "--upload",
        action="store_true",
        help="POST the local JSON to INSURANCE_UPLOAD_URL after writing it",
    )
    parser.add_argument(
        "--rescore-only",
        action="store_true",
        help="Skip fetch / upsert / enrichment. Dedup, rescore, regen, write, upload.",
    )
    parser.add_argument(
        "--apollo-top-n",
        type=int,
        default=APOLLO_TOP_N_DEFAULT,
        help="Apollo runs only on the top-N leads by score. Default: %(default)d.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
