"""End-to-end pipeline runner for the fractional-CFO niche.

fetch -> record-disqualifiers -> upsert -> enrich -> purge -> score ->
apollo -> regen -> write -> upload. Output JSON shape
``{generated_at, leads: [...]}`` — flat, matches what
``api/generate-cfo-leads.js`` expects.

The new stage relative to insurance_pipeline is
``_record_disqualifiers``: the jobs source returns CFO postings as
``Disqualifier`` objects (separate from the regular lead candidates).
We persist those to the ``disqualified`` table and sweep matching
leads out of the leads table before anything else runs.
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

from cfo_pipeline import apollo, db, enrichment, outreach, scoring
from cfo_pipeline.models import (
    Disqualifier,
    Lead,
    LeadCandidate,
    Signal,
    SignalType,
    SourceName,
)
from cfo_pipeline.sources import edgar_form_d, funding, jobs

log = logging.getLogger("cfo.daily_run")

DEFAULT_DB_PATH = Path("data/leads.db")
DEFAULT_OUTPUT_PATH = Path("data/leads.json")
# Form D window is 90 days per spec; jobs source uses its own
# hours_old window internally (capped by since for safety).
LOOKBACK_DAYS = 90
INSIGHT_THRESHOLD = 20.0
SIGNAL_LIMIT_IN_JSON = 6
APOLLO_TOP_N_DEFAULT = 30


_SCORING_SIGNAL_TYPES: frozenset[SignalType] = frozenset(
    {
        SignalType.JOB_POSTED_FINANCE_LEAD,
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
        candidates, disqualifiers = _fetch_all(
            since=_utcnow() - timedelta(days=LOOKBACK_DAYS),
            per_source_limit=args.limit,
        )
        log.info(
            "dry-run fetched %d candidates, %d disqualifiers",
            len(candidates), len(disqualifiers),
        )
        return 0

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.init_db(args.db_path)

    if args.rescore_only:
        log.info("rescore-only: skipping fetch / upsert")

        if args.clear_insights:
            cleared = 0
            for lead in db.iter_leads(conn):
                if lead.id is None or lead.insight is None:
                    continue
                try:
                    db.update_lead(conn, lead.id, insight=None)
                    cleared += 1
                except Exception:
                    log.exception("clear-insights failed for id=%s", lead.id)
            log.info("clear-insights: nulled %d insight rows", cleared)

        if args.reenrich:
            existing_ids = [
                lead.id for lead in db.iter_leads(conn) if lead.id is not None
            ]
            log.info("re-enriching %d existing leads (force=True)", len(existing_ids))
            _enrich_all(conn, existing_ids, force=True)

        modified = db.dedup_signals_pass(conn)
        log.info("deduped signals on %d leads", modified)

        purged = enrichment.purge_disqualified(conn)
        log.info("purged %d disqualified leads", purged)

        all_ids = [lead.id for lead in db.iter_leads(conn) if lead.id is not None]
        log.info("re-scoring %d leads", len(all_ids))

        score_changes = _rescore_all(conn, all_ids)
        apollo_ids = _apollo_enrich_top_n(conn, n=args.apollo_top_n)
        copy_calls = _regen_copy(
            conn, score_changes,
            force_regen_ids=apollo_ids,
            model=args.copy_model,
        )
        log.info(
            "re-scored %d, apollo-enriched %d, regen %d",
            len(score_changes), len(apollo_ids), copy_calls,
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

    candidates, disqualifiers = _fetch_all(
        since=_utcnow() - timedelta(days=LOOKBACK_DAYS),
        per_source_limit=args.limit,
    )
    log.info(
        "fetched %d candidates and %d disqualifiers across all sources",
        len(candidates), len(disqualifiers),
    )

    # Record disqualifiers BEFORE upserting candidates: a CFO posting
    # for "Acme" should block the same-day Form D filing for "Acme" in
    # the same run, not just future runs.
    dq_marked, dq_swept = _record_disqualifiers(conn, disqualifiers)
    log.info(
        "recorded %d disqualifiers, swept %d existing leads",
        dq_marked, dq_swept,
    )

    upserted_ids = _upsert_all(conn, candidates)
    log.info("upserted %d unique leads", len(upserted_ids))

    enriched_ids = _enrich_all(conn, upserted_ids, force=args.reenrich)
    log.info("kept %d enriched leads", len(enriched_ids))

    purged = enrichment.purge_disqualified(conn)
    log.info("purged %d disqualified leads", purged)
    enriched_ids = [lid for lid in enriched_ids if db.get_lead(conn, lead_id=lid)]

    score_changes = _rescore_all(conn, enriched_ids)
    apollo_ids = _apollo_enrich_top_n(conn, n=args.apollo_top_n)
    copy_calls = _regen_copy(
        conn, score_changes,
        force_regen_ids=apollo_ids,
        model=args.copy_model,
    )
    log.info(
        "re-scored %d, apollo-enriched %d, regen %d",
        len(score_changes), len(apollo_ids), copy_calls,
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


def _fetch_all(
    *, since: datetime, per_source_limit: int | None
) -> tuple[list[LeadCandidate], list[Disqualifier]]:
    candidates: list[LeadCandidate] = []
    disqualifiers: list[Disqualifier] = []

    # Jobs source has a two-return signature.
    try:
        c, d = jobs.fetch(since=since, limit=per_source_limit)
        log.info("source jobs returned %d candidates, %d disqualifiers", len(c), len(d))
        candidates.extend(c)
        disqualifiers.extend(d)
    except Exception:
        log.exception("source jobs failed")

    for name, fn in (("funding", funding.fetch), ("edgar_form_d", edgar_form_d.fetch)):
        try:
            cs = fn(since=since, limit=per_source_limit)
            log.info("source %s returned %d candidates", name, len(cs))
            candidates.extend(cs)
        except Exception:
            log.exception("source %s failed", name)

    return candidates, disqualifiers


def _record_disqualifiers(
    conn: sqlite3.Connection, disqualifiers: list[Disqualifier]
) -> tuple[int, int]:
    """Persist disqualifiers and sweep any existing matching lead rows.
    Returns (marked, swept) counts."""
    marked = 0
    swept = 0
    for dq in disqualifiers:
        try:
            key = db.mark_disqualified(conn, dq)
            marked += 1
        except Exception:
            log.exception("disqualifier: mark failed for %r", dq.name)
            continue
        try:
            removed = db.delete_lead_by_name_key(conn, key)
            if removed:
                log.info(
                    "disqualifier: swept %d existing lead row(s) for %s (%s)",
                    removed, dq.name, dq.reason,
                )
                swept += removed
        except Exception:
            log.exception("disqualifier: sweep failed for %r", dq.name)
    return marked, swept


def _upsert_all(
    conn: sqlite3.Connection, candidates: list[LeadCandidate]
) -> list[int]:
    ids: list[int] = []
    refused = 0
    for c in candidates:
        try:
            lead = db.upsert_lead(conn, c)
        except Exception:
            log.exception("upsert failed for %r", c.name)
            continue
        if lead is None:
            refused += 1
            continue
        assert lead.id is not None
        ids.append(lead.id)
    if refused:
        log.info("upsert: refused %d disqualified candidates", refused)
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


def _apollo_enrich_top_n(conn: sqlite3.Connection, *, n: int) -> set[int]:
    """Top-N by score, run Apollo on each lead not yet marked. If
    Apollo surfaces a full-time CFO at the org, the lead is deleted
    and the name is written to the disqualified table — same hard-
    exclude semantics as a CFO job posting."""
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

        # Late disqualifier: if Apollo says the company has a full-time
        # CFO, drop the lead and remember it.
        if result.has_full_time_cfo:
            log.info(
                "apollo: deleting lead %d (%s) — Apollo surfaced a CFO at the org",
                lead_id, lead.name,
            )
            try:
                db.delete_lead(conn, lead_id)
                db.mark_disqualified(
                    conn,
                    Disqualifier(
                        name=lead.name,
                        reason="has_full_time_cfo_per_apollo",
                        source=SourceName.APOLLO,
                        payload={},
                    ),
                )
            except Exception:
                log.exception("apollo: cfo-disqualifier write failed for id=%s", lead_id)
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
    """Refresh insight for every scored lead that crossed the
    threshold or had Apollo data land. LLM-generated single-sentence
    insight per lead."""
    refreshed = 0
    for lead_id, (_old, new) in score_changes.items():
        lead = db.get_lead(conn, lead_id=lead_id)
        if lead is None:
            continue
        updates: dict[str, Any] = {}
        if new < INSIGHT_THRESHOLD:
            if lead.insight is not None:
                updates["insight"] = None
        else:
            should_regen = (
                lead.insight is None
                or lead_id in force_regen_ids
                or _crossed_threshold(_old, new)
            )
            if should_regen:
                try:
                    copy = outreach.generate(lead, new, model=model)
                    updates["insight"] = copy.insight
                    refreshed += 1
                except Exception:
                    log.exception("regen: outreach failed for id=%s", lead_id)
        if updates:
            try:
                db.update_lead(conn, lead_id, **updates)
            except Exception:
                log.exception("regen: update_lead failed for id=%s", lead_id)
    return refreshed


def _crossed_threshold(old: float | None, new: float) -> bool:
    """Returns True when the score moved across INSIGHT_THRESHOLD by
    >= 10 points either direction — used as a regen trigger so we
    refresh the insight when a lead heats up or cools down materially.
    """
    if old is None:
        return new >= INSIGHT_THRESHOLD
    return abs(new - old) >= 10 and (
        (old < INSIGHT_THRESHOLD <= new) or (new < INSIGHT_THRESHOLD <= old)
    )


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
    top_sig = relevant[0] if relevant else None
    cleaned_dm = _clean_dm_name(lead.dm_name)
    return {
        "name": lead.name,
        "domain": lead.domain,
        "industry": lead.industry,
        "headcount": lead.headcount if (lead.headcount or 0) > 1 else None,
        "country": lead.country,
        "city": city,
        "state": state,
        "dm_name": cleaned_dm,
        "dm_title": lead.dm_title,
        "dm_email": lead.dm_email,
        "dm_linkedin_url": lead.dm_linkedin_url,
        "contact_strength": _contact_strength(
            lead.name, cleaned_dm, lead.dm_email, lead.dm_linkedin_url
        ),
        "score": lead.score,
        "insight": lead.insight,
        "trigger_type": outreach.trigger_type(top_sig) if top_sig else "other",
        "signals": [_signal_to_json(s, now) for s in relevant],
    }


def _clean_dm_name(name: str | None) -> str | None:
    if not name:
        return name
    import re as _re
    cleaned = _re.sub(r"\s+", " ", name).strip()
    return cleaned or None


def _contact_strength(
    lead_name: str,
    dm_name: str | None,
    dm_email: str | None,
    dm_linkedin: str | None,
) -> str:
    if not dm_name:
        return "cold"
    if _names_match(lead_name, dm_name):
        return "cold"
    if dm_email or dm_linkedin:
        return "verified"
    return "partial"


def _names_match(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    import re as _re

    def _norm(s: str) -> str:
        return _re.sub(r"\s+", " ", _re.sub(r"[^a-z\s]", "", s.lower())).strip()

    return _norm(a) == _norm(b)


def _signal_to_json(s: Signal, now: datetime) -> dict[str, Any]:
    return {
        "type": s.type.value,
        "captured_at": s.captured_at.isoformat(),
        "days_ago": max(0, (now - s.captured_at).days),
        "payload": s.payload,
    }


# --- Upload ----------------------------------------------------------------


def _upload_blob(json_path: Path) -> None:
    url = os.environ.get("CFO_UPLOAD_URL")
    api_key = os.environ.get("CFO_UPLOAD_API_KEY")
    if not url or not api_key:
        raise RuntimeError(
            "--upload requires CFO_UPLOAD_URL and CFO_UPLOAD_API_KEY env vars"
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
    parser = argparse.ArgumentParser(prog="cfo_pipeline.daily_run")
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
        help="POST the local JSON to CFO_UPLOAD_URL after writing it",
    )
    parser.add_argument(
        "--rescore-only",
        action="store_true",
        help="Skip fetch / upsert / disqualifier recording. Dedup, rescore, regen, write, upload.",
    )
    parser.add_argument(
        "--clear-insights",
        action="store_true",
        help="One-shot: null out every lead's insight before rescoring. "
        "Use after an outreach-prompt change so existing leads regenerate "
        "their insight against the new prompt. Only honored in --rescore-only.",
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
