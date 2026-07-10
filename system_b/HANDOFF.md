# System B — Handoff / Orientation (read this first)

You're continuing work on a fractional-CFO cold-outreach system. This session
starts with **no prior chat context** — everything you need is in the repo and
this file. Nothing here has ever sent an email; every prospect card is
`review_status=pending`.

## The two systems

- **System A** — `cfo_pipeline/` (Python scraper). Finds small companies showing
  public "needs finance help right now" signals (just raised, hiring a
  Controller, advertising for a fractional CFO). Runs nightly via a GitHub
  Actions cron (`.github/workflows/cfo-leads.yml`, `0 8 * * *`), uploads to
  Vercel Blob, and serves the **live API at `https://www.ishaangpta.com/api/leads`**.
  Already deployed and live. **Don't confuse the two companies:** a **LEAD** =
  a company needing finance help (the gift). A **PROSPECT** = a fractional-CFO
  firm we email. We never email leads.

- **System B** — `system_b/` (Python outreach engine, **not deployed yet**).
  For each prospect firm: research their niche → build a "gift" of 1–3 leads →
  draft a cold email → assemble a human review card in Airtable. Reads leads
  from System A's live API. **This is what we're building.**

## Current status

**System B is built through M4 (77 tests passing). M5–M8 are not started.**

| Milestone | What | State |
|---|---|---|
| M0 | foundations (config, scraper/airtable clients) | ✅ |
| M1 | gift engine (`gift/`) | ✅ |
| M2 | copy engine (`copy/`) | ✅ |
| M3 | prospect research (`research/`) | ✅ |
| M4 | review card + CRM state (`review/`) — GO-LIVE point | ✅ |
| M5 | scheduler (daily queue) | ⬜ |
| M6 | sender + reply watcher — **nothing sends until this exists** | ⬜ |
| M7 | LinkedIn paste-list | ⬜ |
| M8 | follow-up lead pulls (email #2/#3) | ⬜ |

**System A** has three shipped fixes (live): A2 (large-NGO denylist), A3
(classifier tightening — services ≠ manufacturers). A1 (mis-resolved domains,
e.g. "Poaster Technologies" → warp.co) was intentionally made a **non-destructive
System-B review flag** (`review/flags.py: domain_matches_company`) rather than an
upstream data-drop — string matching a name to a domain has too many false
positives (acronyms, branded domains) to safely null data.

## Layout

```
system_b/
  config.py            env + constants
  models.py            Lead / Signal (pydantic) — the live API's lead object
  gift/                M1: engine.py (build_gift), taxonomy.py (map_prospect), models.py
  copy/                M2: subject.py, email.py (build_email_1), lex.py, honesty.py, llm.py
  research/            M3: classifier.py, fetcher.py, service.py, llm.py, models.py
  review/              M4: card.py (build_card), flags.py (review_flags), state.py, service.py
  clients/             scraper_client.py (ScraperClient + SnapshotScraper), airtable_client.py
  scripts/             m4_walkthrough.py (end-to-end card generator), dump_cards.py, m0_acceptance.py
  tests/               pytest suite (the spec's 16 worked examples live in test_gift.py)
cfo_pipeline/          System A (the scraper) — separate track
```

## Setup (secrets, venv, and the CSV do NOT come with the repo)

1. **`.env`** — copy `system_b/.env.example` → `system_b/.env` and fill:
   `AIRTABLE_TOKEN`, `AIRTABLE_BASE_ID=apptmXyx9jTct0vpu`,
   `AIRTABLE_TABLE_NAME=Prospects`, `OPENAI_API_KEY`,
   `SCRAPER_BASE_URL=https://www.ishaangpta.com`. **Gitignored — never commit.**
2. **venv** — `python3.11 -m venv system_b/.venv && system_b/.venv/bin/pip install -r system_b/requirements.txt`
3. **Apollo CSV** — `system_b/apollo-contacts-export.csv` (the prospect list).
   **Gitignored — it's real contact PII and the repo is PUBLIC. Do not commit it.**
   Upload it into the session manually.

## How to run

```bash
# tests (expect 77 passing)
system_b/.venv/bin/python -m pytest system_b/tests/ -q

# generate review cards end-to-end for the first N prospects (SENDS NOTHING;
# writes cards to Airtable at review_status=pending + prints a tuning report)
system_b/.venv/bin/python -m system_b.scripts.m4_walkthrough \
    --csv system_b/apollo-contacts-export.csv --summary
#   add --limit N to cap;  add --leads-file inventory.json to use a local lead set

# dump every Airtable card to one text file for review
system_b/.venv/bin/python -m system_b.scripts.dump_cards --out system_b/review_cards.txt
```

## Load-bearing facts (don't relearn these the hard way)

- **Live API contract:** `GET /api/leads?industry=&niche=&city=&state=&signal_type=&freshness=&exclude_ids=&limit=`
  and `GET /api/niches`. A lead is `{id, company, domain, city, state, industry,
  niche, value_prop, signal_type, freshness, signals:[{type, date,
  date_confidence, plain_words_description}]}`. **There is NO top-level `score`
  or `finance_grade`; `date_confidence` is per-signal.** Code already adapts to
  this (see `models.py`).
- **System A's API collapses under burst load** (~60–80 rapid calls → sustained
  502s). That's why batch runs use `SnapshotScraper` — **one** inventory pull,
  then filter in memory (identical filtering to the live API). Never fan out
  hundreds of per-prospect API calls.
- **Copy honesty is enforced in CODE, not the LLM** (`copy/honesty.py`,
  `review/flags.py`): recompute relative dates from `signals[].date` for
  high-confidence signals only, suppress them for low-confidence, **never** a
  dollar amount for a raise, and a niche is claimed only when `all_niche` AND a
  curated label exists (`copy/lex.py: NICHE_DISPLAY`). The **LLM (OpenAI) only
  fills freeform per-lead descriptions and the research classification** —
  everything structural (subject, framing, CTA, template, dates, flags) is
  deterministic code.
- **The spec is embedded in the code + tests**, not a separate doc. The 16
  worked examples are `tests/test_gift.py`; the subject/framing/CTA tables are
  `tests/test_copy.py` and `copy/subject.py` / `copy/email.py`. If you need the
  original build-plan / spec prose, ask the user to paste it.
- Run System A (`cfo_pipeline/`) tools via `cfo_pipeline/.venv/bin/python -m ...`.

## What's left / open threads

- **M5–M8** (scheduler → sender+reply-watcher → LinkedIn → follow-ups). Build in
  that order; the sender is last on purpose — the human review gate (M4) must
  exist before anything can auto-send.
- **#8 multi-industry** mapping under-triggers on plural/compound phrases
  ("SaaS and Professional Services" still maps to one) → "partial-truth" niche
  claims, backstopped by the LLM-classified-niche review flag. A fuller fix
  moves the single-vs-multi call into the research LLM.
- **Cross-prospect dedup** — the same lead can be gifted to multiple prospects
  in a metro; belongs with the sender (M6), where `sent_lead_ids` is tracked.
- **System A A4/A5** (API load hardening, more high-confidence `cfo_wanted`
  supply) — deferred.

## Ground rules

- **Never commit** `.env`, `apollo-contacts-export.csv`, `review_cards.txt`, or
  `system_b/logs/` (all gitignored). The repo is **public**.
- **Nothing sends.** There is no sender yet, and the rule is: only an
  `approved` card is ever eligible to send. Keep it that way.
