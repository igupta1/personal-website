# Insurance lead-magnet build plan

Target: a 4th niche `insurance` alongside `it_msp` / `mssp` / `cloud`,
surfacing independent insurance agencies' SMB prospects from public
buying signals. Same look, same plumbing, different sources + scoring +
DM titles.

## Status (2026-05-15)

Steps 1-11 complete. Step 12 (weight tuning on real data) blocked on
first real pipeline run.

One deviation from the original plan worth flagging: step 9-10
originally specified per-state bulk fetchers for FL SunBiz, CO, and
WA. The implementation ships as a single OpenCorporates Search API
fetcher targeting `us_fl`, `us_co`, `us_wa` — one parser, one set of
fixtures, easier to maintain. Per-state bulk pulls remain a v2 option
if OpenCorporates rate limits become painful. Free public tier works
without a key for demos; `OPENCORPORATES_API_KEY` env var is wired
through `.env.example` + the GitHub Actions workflow for higher
limits.

To populate the insurance dashboard:
- Trigger the daily-leads workflow with `workflow_dispatch` (use
  `--limit 5` for a fast smoke), OR
- Run locally: `python -m msp_pipeline.daily_run --upload`

---

## 1. Inventory of the three existing systems

One pipeline, three niches over a shared SQLite row. The "three
systems" share ~90% of the code; only the per-niche weights, framings,
and the score/insight column names differ.

**Pipeline (`pipeline/msp_pipeline/`):**
- `models.py` — `NicheName` (3 values), `SignalType` (10 values),
  `SourceName` (5 values), `Signal`, `Lead`, `LeadCandidate`.
- `db.py` — Single `leads` table; signals stored as JSON list on the
  row. Per-niche `<niche>_score` / `<niche>_insight` / `<niche>_outreach`
  columns. Idempotent ALTER-based migrations. Fuzzy name dedup
  (rapidfuzz 90% on a normalized `name_key`). Signal-level dedup with
  per-type keying rules.
- `sources/jobs.py` — JobSpy (Indeed/LinkedIn) + Adzuna + HN Algolia,
  classified via regex into 5 job-type SignalTypes. Recruiter-name
  filter. Per-fetcher try/except, full-source isolation.
- `sources/funding.py` — TechCrunch + PR Newswire RSS. LLM-driven
  headline extraction to pull the operating-company name and filter
  out IR noise / VC fund raises / lawsuits.
- `sources/breaches.py` — California / Maine / Washington AG HTML
  tables, parsed via per-state row parsers behind a shared
  `_fetch_html_table` helper.
- `enrichment.py` — Two-stage: (1) pure-code disqualification
  (`_BLOCKED_TLDS`, name regex, IT-vendor name regex, headcount>250,
  headline-shaped names); (2) Gemini grounded-search lookup
  (headcount, city/state/country, domain, is_it_vendor, dm_name,
  dm_title, value_prop) + OpenAI industry classifier (14-tag enum).
  US-only filter. Re-enrichment is signal-aware: skip if no new
  signals since last `ENRICHMENT_RUN` marker unless `--reenrich`.
- `scoring.py` — Pure code, per-niche `SIGNAL_WEIGHTS` × 30-day
  half-life decay, clamped 0-100. Industry/headcount are UI filters,
  not score inputs.
- `outreach.py` — One OpenAI structured call per (lead, niche) →
  `Copy.insight` (single third-person sentence). Per-niche framing
  string. Outreach emails were deleted in a prior milestone.
- `apollo.py` — Three-step org-enrich → people-search → people-match
  for the union of top-30 leads per niche. Hard-coded IT/security DM
  title list. Per-candidate scoring on seniority bucket + IT-focus
  keywords minus disqualifying titles. Headcount > 250 from Apollo
  deletes the lead.
- `daily_run.py` — fetch → upsert → enrich → purge_disqualified →
  rescore → apollo_top_n → regen_insight_copy → build_output → upload.
  `--rescore-only` and `--reenrich` modes for cheap re-runs.
- `llm.py` — OpenAI + Gemini wrappers, exponential-backoff retries,
  optional Pydantic structured response.

**UI (`src/`):**
- `LeadsPage.js` — Generic, niche-parameterized: fetches
  `/api/generate-leads?niche=<n>`, applies industry/size/state filters,
  renders `LeadCard` grid.
- `LeadCard.js` — Card layout, decision-maker panel, insight sentence,
  freshness-dot signal list. Owns `SIGNAL_KIND_META` (the
  signal-type → label/pill-color map) and `INDUSTRY_LABELS`.
- `LeadFilters.js` — Industry / size band / state dropdowns. Owns
  `INDUSTRY_LABELS` and `SIZE_BANDS`.
- `ITMsps.js`, `Mssps.js`, `Cloud.js` — 8-line wrappers over
  `LeadsPage` with niche prop + copy.

**API + storage (`api/`):**
- `upload-leads.js` — POST, Bearer-token auth, schema-validates
  `body.niches.{it_msp,mssp,cloud}` arrays, writes single Vercel Blob
  `leads-current.json`.
- `generate-leads.js` — GET, reads + caches blob URL, returns the
  requested niche slice or the full payload.

**Cron:** `.github/workflows/daily-leads.yml` runs `daily_run --upload`
nightly at 06:00 UTC, commits the updated `data/leads.db` +
`data/leads.json`.

### What's already abstracted vs. duplicated

| Surface | Abstraction quality |
|---|---|
| `llm.py` | Fully niche-agnostic. **Reuse.** |
| `db.py` | Single `leads` table is the right move. Niche-specific bits are isolated to two maps (`_NICHE_SCORE_COLUMN`, `_UPDATABLE_FIELDS`) and the column list. **Trivially extensible.** |
| Signal dedup, fuzzy upsert | Generic. **Reuse.** |
| Source modules | Each is a self-contained `fetch(since, limit) -> [LeadCandidate]`. **Pluggable.** |
| `scoring.py` `SIGNAL_WEIGHTS[NicheName]` | Per-niche dict — adding a niche = adding a dict key. **Clean.** |
| `outreach.py` `_NICHE_FRAMING` | Same pattern. **Clean.** |
| `enrichment.py` | Less clean: the Gemini prompt hard-codes IT DM titles, the disqualification filter hard-codes IT-vendor keywords. **These are MSP-shaped and must be adapted.** |
| `apollo.py` `_DM_TITLES`, `_IT_PHRASES`, `_DISQUALIFYING_TITLE_KEYWORDS` | Also MSP-shaped. The selection algorithm is generic; only the lists are niche-specific. **Lists need a per-niche flavor.** |
| `LeadsPage.js` | Generic, niche-prop'd. **Reuse.** |
| `LeadCard.js` `SIGNAL_KIND_META` | Hard-coded MSP signal kinds. **Needs new entries.** |
| `LeadFilters.js` `INDUSTRY_LABELS` | Shared with `LeadCard`; duplication. Acceptable. **Reuse.** |
| `api/*.js` `VALID_NICHES` / `REQUIRED_NICHES` | Hard-coded 3-niche array. **Trivial 1-line edit.** |
| Cron workflow | Niche-agnostic. **Reuse.** |

### Reuse surface — bottom line

- **Plug-and-play**: `llm.py`, the source-module contract, db dedup +
  upsert, scoring engine shape, outreach engine shape, LeadsPage,
  LeadFilters, the whole serverless + blob layer, the cron workflow.
- **Niche dicts that need a new key**: `SIGNAL_WEIGHTS`,
  `_NICHE_FRAMING`, `_NICHE_SCORE_COLUMN`, `_NICHE_INSIGHT_COL`,
  `_BANDS`, `VALID_NICHES`, `REQUIRED_NICHES`.
- **MSP-shaped that need a small refactor**: the enrichment Gemini
  prompt (DM-title block), the Apollo title list + scoring keywords,
  the IT-vendor disqualification.
- **Genuinely new**: source modules for insurance-specific signals,
  one new React niche page, one new nav link, additive schema columns
  + signal type enum values.

---

## 2. Signal feasibility table

Dimensions: **Where** = where the data physically lives. **Access** =
how you pull it. **Legal** = compliance posture. **Frag** = coverage
fragmentation. **Contact** = how hard to recover a decision-maker
from the raw record. **Analog** = closest existing source. **Verdict**.

| # | Signal | Where | Access | Legal | Frag | Contact | Analog | Verdict |
|---|---|---|---|---|---|---|---|---|
| 1 | New business reg (SoS) | 50 state SoS offices, all separate | Mixed: FL SunBiz daily bulk dump (free), CO/WA/NY structured downloads (free), CA/DE/TX paid or scrape-only. OpenCorporates aggregates but free tier rate-limited. | Public records — clean | **High** (50 states, all different schemas) | Filing gives registered agent (often a service co.) + sometimes officer names; needs LLM/Apollo enrichment for actual DM | None — most analogous to `funding.py` (federal, single source) but far more fragmented | **Ship v1** with **2-3 free-bulk states only** (FL + CO + WA). National coverage is post-v1. |
| 2 | Job postings (hiring velocity, first hires, blue-collar) | Adzuna / Indeed / LinkedIn / HN | **Already wired in `sources/jobs.py`.** Different keyword set. | Public job boards — clean | None (national) | Company name → existing enrichment pipeline | `sources/jobs.py` — direct fork/parameterize | **Ship v1.** Highest reuse. Swap `_JOB_QUERIES` + `_classify_job_title` for insurance-relevant patterns (office manager, warehouse worker, commercial driver, controller, HR generalist, foreman). |
| 3 | Commercial property tx | 3,000+ county recorders | No free national source. ATTOM / DataTree / CoreLogic = $$$$ paid APIs. A handful of large counties (LA, Cook, Maricopa) have free portals; bulk is paid. | Public records OK | **Extreme** (3,000+ counties) | Buyer often a shell LLC → near-impossible without paid resolution | None | **Drop v1.** Effort >> payoff. Revisit only if a paid aggregator becomes part of the budget. |
| 4 | Business license filings (city/county) | Mostly city/county; sometimes state | Major-city open-data portals are free + structured (NYC, SF, LA, Chicago, Seattle Socrata APIs). Small towns: zero. | Public records OK | **High** (5-10 free cities; rest fragmented) | Business name + sometimes owner; needs enrichment | Sort of like `breaches.py` (per-agency parsers, shared shell) | **Defer v1.** Heavy overlap with signal #1 (SoS filings) — a new license usually implies a new business already caught upstream. Ship after v1 only if a target geo isn't covered by SoS bulk. |
| 5 | Commercial fleet / vehicle registrations | State DMVs | **DPPA (18 USC 2721)** restricts DMV data to "permissible purposes". Insurance underwriting is a permitted purpose; **marketing / lead-gen is explicitly excluded**. Aggregated commercial-fleet data via LexisNexis / Polk is licensable but contract-restricted to underwriting. | **Legal blocker for a marketing-flavored lead magnet** | High (state-specific) | Owner address locked behind DPPA | None | **Drop.** Legal exposure for a marketing use case. The DPPA carve-out you flagged is real and disqualifying. |

### Bonus reuse you didn't ask about

| Signal | Verdict |
|---|---|
| **Funding raised** — reuse `sources/funding.py` as-is | **Ship v1 as a bonus.** Zero code cost: the source already returns `FUNDING_RAISED` candidates that are mostly US SMBs. Fresh funding → hiring → workers comp + group benefits + D&O. Just add an `insurance` weight to `scoring.SIGNAL_WEIGHTS`. |
| **Breaches** — reuse `sources/breaches.py` | **Skip.** Tangential to insurance buying intent (cyber-insurance trigger exists but is narrow). |

### v1 ship list — recommendation

Three signal sources, two new, one reused. Mirrors the 3-source
shape of the existing pipeline and is enough to fill a credible
dashboard on day one.

1. **`sources/jobs_insurance.py`** (fork of `jobs.py` with new
   keyword regex set) → 4 new SignalTypes:
   `job_posted_first_hire`, `job_posted_blue_collar`,
   `job_posted_fleet_role`, `job_posted_finance_ops`
2. **`sources/business_filings.py`** (new) → 1 new SignalType:
   `new_business_filed`. v1 covers FL SunBiz + CO + WA bulk.
3. **`sources/funding.py`** (reuse, weight-only change) → existing
   `funding_raised` reused as a v1 signal.

Deferred: business licenses (#4) → v2 if needed.
Dropped: property (#3), fleet (#5).

---

## 3. Reuse map

Component-by-component. Marked **Reuse** / **Adapt** / **New**.

### Pipeline (Python)

| Path | Verdict | One-line justification |
|---|---|---|
| `msp_pipeline/llm.py` | **Reuse** | Niche-agnostic. |
| `msp_pipeline/models.py` | **Adapt** | Add `NicheName.INSURANCE`, 5 new SignalTypes. |
| `msp_pipeline/db.py` | **Adapt** | Add `insurance_score`/`insurance_insight` columns via `_MIGRATIONS`; add to `_NICHE_SCORE_COLUMN` and `_UPDATABLE_FIELDS`. |
| `msp_pipeline/scoring.py` | **Adapt** | Add `SIGNAL_WEIGHTS[NicheName.INSURANCE]`, reuse 30-day half-life. Also add an `_INSURANCE_VENDOR_NAME_RES` regex set (carriers, brokers, MGAs, wholesalers, reinsurers, TPAs, adjusters) that returns 0 from `_score_niche(..., INSURANCE)` for matching leads — keeps them in the pool for the other three niches instead of purging globally. |
| `msp_pipeline/outreach.py` | **Adapt** | Add `_NICHE_FRAMING[NicheName.INSURANCE]`. |
| `msp_pipeline/enrichment.py` | **Adapt** | Generalize the DM_NAME/DM_TITLE Gemini prompt block to cover ops/finance buyers (Owner, COO, CFO, controller, office manager) — easiest is "smart" prompt that picks the most likely insurance-buying role. **No change to `_IT_VENDOR_NAME_RES` or `purge_disqualified`** — the insurance-vendor filter lives in `scoring.py` so we don't kill insurance brokers from the MSSP/IT/Cloud lead sets. |
| `msp_pipeline/apollo.py` | **Adapt** | Add insurance-buyer titles to `_DM_TITLES` (CFO, Controller, COO, Office Manager, HR Director, Owner — most already present as small-biz fallbacks). Extend `_IT_PHRASES` / introduce an `_OPS_PHRASES` for the scoring function so a CFO at an insurance prospect beats an HR director. |
| `msp_pipeline/sources/jobs.py` | **Adapt or fork** | Recommend fork → `sources/jobs_insurance.py`. The classifier regex and `_JOB_QUERIES` are MSP-specific; refactoring `jobs.py` to be parameterized adds abstraction churn we don't need yet. |
| `msp_pipeline/sources/funding.py` | **Reuse** | Same candidates, just re-weight in scoring. |
| `msp_pipeline/sources/breaches.py` | **Reuse but skip** for insurance | Not relevant — the existing MSSP/IT_MSP niches keep using it; the insurance niche scoring assigns weight 0. |
| `msp_pipeline/sources/business_filings.py` | **New** | FL SunBiz bulk + CO bulk + WA bulk. |
| `msp_pipeline/daily_run.py` | **Adapt** | Add the new source to `_fetch_all`. `NicheName` enum iteration already handles new niche transparently. |

### UI (React)

| Path | Verdict | One-line justification |
|---|---|---|
| `src/pages/LeadsPage.js` | **Reuse** | Already niche-prop'd. |
| `src/components/LeadCard.js` | **Adapt** | Add `SIGNAL_KIND_META` entries for new SignalTypes (label + pill color). |
| `src/components/LeadFilters.js` | **Reuse** | Industry vocab is shared across niches. |
| `src/pages/Insurance.js` | **New** | 8-line wrapper over `LeadsPage`, matching `ITMsps.js`. |
| `src/App.js` | **Adapt** | Add `<Route path="/insurance" element={<Insurance />} />`. (`CLAUDE.md` flags `App.js` as do-not-modify-without-instruction — this is the explicit instruction.) |
| `src/components/Header.js` | **Adapt** | Add an "Insurance" NavLink. Same flag in `CLAUDE.md` — explicit instruction here. |

### Serverless

| Path | Verdict | One-line justification |
|---|---|---|
| `api/upload-leads.js` | **Adapt** | Add `insurance` to `REQUIRED_NICHES`. |
| `api/generate-leads.js` | **Adapt** | Add `insurance` to `VALID_NICHES`. |

### Ops

| Path | Verdict | One-line justification |
|---|---|---|
| `.github/workflows/daily-leads.yml` | **Reuse** | Niche-agnostic. |
| Tests | **Adapt** | One new test file per new source. Existing `test_db.py` / `test_scoring.py` / `test_enrichment.py` need additive cases (insurance niche, new SignalTypes). |

---

## 4. New components to create

Matching existing file-naming + per-niche conventions.

### Python

- **`pipeline/msp_pipeline/sources/business_filings.py`**
  - Public `fetch(since, limit) -> list[LeadCandidate]` per the source
    contract.
  - **Recency clamp**: module constant
    `_MAX_FILING_AGE_DAYS = 60`. Inside `fetch`, the effective start
    date is `max(since, _utcnow() - timedelta(days=_MAX_FILING_AGE_DAYS))`
    so a `--reenrich` backfill or a stale `since` parameter can't
    sweep in ancient filings. Keeps the `since`-parameter contract
    that every other source obeys; just clamps internally.
  - One sub-fetcher per state, behind a shared
    `_fetch_state_filings(state, effective_since)` helper (same
    shape as `breaches.py`'s `_fetch_html_table`).
  - States v1: `_fetch_fl_sunbiz`, `_fetch_co`, `_fetch_wa`.
    - **FL SunBiz** has a daily bulk file at
      `ftp://ftp.dos.state.fl.us/public/doc/` (cordata format) —
      simplest source.
    - **CO** has a per-day CSV at the CO SoS Bizweb portal
      (`https://www.coloradosos.gov/biz/...`).
    - **WA** has a structured corporation-search export
      (`https://ccfs.sos.wa.gov`).
  - Emits `SignalType.NEW_BUSINESS_FILED` with payload
    `{ "state": "FL", "filing_type": "LLC", "filed_on": "...",
      "registered_agent": "...", "officers": [...] }`.

- **`pipeline/msp_pipeline/sources/jobs_insurance.py`**
  - Forks `jobs.py`; same three fetchers (JobSpy, Adzuna, HN). Swaps
    `_JOB_QUERIES` and `_classify_job_title` for insurance keywords:
    - `JOB_OPS_ROLE` (renamed from `JOB_FIRST_HIRE`) — "office
      manager", "administrative assistant", "operations
      coordinator". **Reasoning for rename**: true first-hire
      detection needs prior-state history the pipeline doesn't
      keep. Two alternatives existed — (a) rename to honest
      naming, (b) keep the name + add a post-enrichment
      `headcount < 25` gate. (b) requires either a new
      signal-pruning pass after enrichment or a headcount-aware
      special case inside the deliberately-pure `_score_niche`
      loop. Rename costs zero and the signal is still useful at
      any size (renewals get reassigned when ops staff turns
      over). Chose (a).
    - `JOB_BLUE_COLLAR` — "warehouse associate", "construction
      laborer", "machine operator", "production worker"
      (workers-comp trigger).
    - `JOB_FLEET_ROLE` — "CDL driver", "delivery driver",
      "fleet manager", "dispatcher" (commercial auto trigger).
    - `JOB_FINANCE_OPS` — "controller", "CFO", "VP of finance",
      "HR director" (the buyer of group benefits / D&O).

### React

- **`src/pages/Insurance.js`** — wrapper, copy-paste of `ITMsps.js`
  with insurance copy.

### Schema additions (handled in `db.py` `_MIGRATIONS`)

- Columns: `insurance_score REAL`, `insurance_insight TEXT`.
- Index: `idx_leads_insurance_score`.
- Enum: `NicheName.INSURANCE = "insurance"`,
  `SignalType.NEW_BUSINESS_FILED = "new_business_filed"`,
  `SignalType.JOB_OPS_ROLE = "job_posted_ops_role"`,
  `SignalType.JOB_BLUE_COLLAR = "job_posted_blue_collar"`,
  `SignalType.JOB_FLEET_ROLE = "job_posted_fleet_role"`,
  `SignalType.JOB_FINANCE_OPS = "job_posted_finance_ops"`,
  `SourceName.FILINGS = "filings"`.

### Tests

- `tests/test_sources_business_filings.py` — fixture-driven, one fixture
  per state, mirrors `test_sources_breaches.py` shape.
- `tests/test_sources_jobs_insurance.py` — classifier coverage for
  insurance-relevant titles.
- `tests/test_scoring.py` — add insurance-weight cases.
- `tests/test_db.py` — add insurance-column upsert/query cases.
- `tests/test_enrichment.py` — extend disqualification cases to
  include insurance-broker name patterns.

---

## 5. Schema changes

**All additive** — no destructive migrations, no column renames, no
table drops. The existing `_MIGRATIONS` tuple in `db.py` is built for
this exact shape.

```python
# msp_pipeline/db.py — append to _MIGRATIONS
_MIGRATIONS = (
    ("dm_name", "TEXT"),
    ("dm_title", "TEXT"),
    ("dm_email", "TEXT"),
    ("dm_linkedin_url", "TEXT"),
    ("value_prop", "TEXT"),
    ("insurance_score", "REAL"),
    ("insurance_insight", "TEXT"),
)

# append to _INDEXES
"CREATE INDEX IF NOT EXISTS idx_leads_insurance_score ON leads(insurance_score DESC)",

# append to _NICHE_SCORE_COLUMN
NicheName.INSURANCE: "insurance_score",

# append to _UPDATABLE_FIELDS
"insurance_score", "insurance_insight",
```

The `_DDL` constant for fresh DBs gets the same two new columns
inline. `signals` JSON shape doesn't change.

Existing rows pick up the new columns as NULL on first open — same
pattern the codebase already uses for the M8 DM columns.

---

## 6. Build order — demo-fastest (revised)

Revised after first-pass review:
- Enrichment generalization (was old step 11) moved before the
  dashboard goes live, so the first render lands with sensible DM
  names instead of MSP-shaped ones.
- Insurance-vendor scoring filter promoted from "open question" to
  an explicit build step inside step 3.
- SoS recency clamp called out in step 9.

Each step ends in a runnable artifact.

1. **DB + models, additive.** `NicheName.INSURANCE`, 5 new
   `SignalType` values (one renamed to `JOB_OPS_ROLE` — see §4),
   `SourceName.FILINGS`, `insurance_score` + `insurance_insight`
   columns via `_MIGRATIONS`, index, niche maps. Run pytest,
   confirm green. **Artifact: pipeline still passes tests; nothing
   visible yet.**

2. **Fork `sources/jobs.py` → `jobs_insurance.py`.** New regex set,
   wire into `daily_run._fetch_all`. **Artifact: a `--dry-run --limit
   5` produces insurance-flavored candidates.**

3. **Wire scoring + outreach for insurance, including the
   insurance-vendor zero-score filter.** Add
   `SIGNAL_WEIGHTS[NicheName.INSURANCE]` and
   `_NICHE_FRAMING[NicheName.INSURANCE]`. Add
   `_INSURANCE_VENDOR_NAME_RES` in `scoring.py` covering carriers,
   brokers, MGAs, wholesalers, reinsurers, TPAs, adjusters; when
   matched, `_score_niche(lead, INSURANCE, ...)` returns 0 instead
   of summing weights. Other niches' scores are untouched.
   **Artifact: `--rescore-only` produces real `insurance_score`
   numbers; an "Acme Insurance Brokers" sample lead scores 0 for
   insurance but retains its other-niche scores.**

4. **Enrichment generalization (moved up from old step 11).**
   - Generalize `enrichment.lookup_company`'s Gemini prompt block
     so DM_NAME / DM_TITLE picks from a broader title pool (Owner,
     President, CEO, COO, CFO, Controller, Office Manager, HR
     Director) and instructs Gemini to choose the role most likely
     to handle vendor decisions at the company's size and
     industry — without naming "IT" first.
   - Extend Apollo's `_DM_TITLES` with finance/ops buyers (CFO,
     Controller, VP of Finance, Director of Finance, Office
     Manager, HR Director). Add an `_OPS_PHRASES` group to
     `_score_person` so a CFO beats an HR director at the same
     org. Existing IT keyword scoring stays — it correctly
     surfaces IT DMs for MSP/MSSP/Cloud niches.
   - Run `--rescore-only --reenrich` on whatever's accumulated.
   **Artifact: top-30 insurance leads land with the right DM
   names on first dashboard render.**

5. **Adapt API endpoints.** Add `insurance` to `VALID_NICHES` and
   `REQUIRED_NICHES`. **Artifact:
   `/api/generate-leads?niche=insurance` returns the new slice.**

6. **`Insurance.js` page + `/insurance` route + Header NavLink.**
   **Artifact: <https://www.ishaangpta.com/insurance> renders.
   Demo-ready.**

7. **Add `LeadCard.js` `SIGNAL_KIND_META` entries** for the new
   signal types so they display with pill colors instead of being
   dropped by the existing filter. **Artifact: cards show
   insurance-specific signal pills.**

8. **End-to-end run on a small sample.** Push to GitHub Actions
   `workflow_dispatch` with `--limit 5`, verify the blob updates
   and the page reflects new leads.

9. **`sources/business_filings.py` — FL SunBiz first**, the
   highest-volume single-source state. Standard `fetch(since,
   limit)` contract with internal recency clamp
   (`_MAX_FILING_AGE_DAYS = 60`) so a stale `since` can't pull in
   ancient filings. Wire into `_fetch_all`. **Artifact: new-LLC
   candidates appear in the dashboard.**

10. **Add CO + WA fetchers.** Sub-fetcher per state, behind the
    same shared helper. Same recency clamp inherited from the
    module.

11. **Reuse `sources/funding.py`** by giving `FUNDING_RAISED` a
    non-zero weight under `SIGNAL_WEIGHTS[NicheName.INSURANCE]`.
    No code change beyond the weight dict.

12. **Tune weights on real data.** Look at the top 30. Adjust
    `SIGNAL_WEIGHTS`. Extend `_INSURANCE_VENDOR_NAME_RES` if
    obvious carrier / broker names slipped through.

Steps 1-8 = demo-ready (now including correct DMs and zero-scored
insurance vendors). Steps 9-12 = production polish.

---

## 7. Open questions

Trimmed: items resolved by the first-pass review are now build steps.

1. **Target geographies beyond FL.** FL SunBiz is the obvious first
   pick (free, bulk, high volume). Pick #2 / #3 based on where you
   want leads to surface. Default proposal: CO + WA (both free
   bulk).
2. **DM ranking at small businesses.** At a freshly-filed LLC there
   is no CFO. The Apollo title list already includes Owner /
   Founder / COO / President / CEO as small-biz fallbacks — confirm
   we're happy ranking those above CFO / Controller when the
   company is < 25 headcount. (Step 4 will need a tiebreak rule.)
3. **Newly-formed LLCs with no web presence.** The Gemini lookup
   will return `unknown` for most fields. Two options: (a) show
   them anyway with a "newly filed" badge and no DM panel; (b)
   hide them until they're enriched. Recommend (a) — that's the
   lead-magnet's differentiator.
4. **Apollo budget.** Top-30 per niche × 4 niches = ~120
   enrichments per night, vs. ~90 today. Confirm the plan
   supports the extra credit burn.
5. **Compliance review.** None of v1's chosen signals (jobs, SoS
   filings, funding) carry DPPA / GLBA / FCRA issues, but a
   one-line sign-off in the repo (`COMPLIANCE.md` or a section in
   the new page's footer) is worth its weight. Defer the actual
   document.

### Resolved in this revision

- **Vendor-name disqualification** → score-zero in `scoring.py`
  for `_INSURANCE_VENDOR_NAME_RES` matches, preserving the lead
  for other niches. Build step #3.
- **Recency decay for SoS filings** → keep the 30-day scoring
  half-life; clamp `since` to ≤60 days inside `business_filings.py`
  via `_MAX_FILING_AGE_DAYS`. Build step #9.
- **Bulk-file storage** → fetch + parse + discard each run; do
  not commit FL SunBiz dumps to the repo. Build step #9.
- **Enrichment ordering** → DM-title generalization moved to
  build step #4, before the dashboard goes live.
- **`JOB_FIRST_HIRE` naming** → renamed to `JOB_OPS_ROLE`.
  Reasoning in §4 (rename costs zero; a headcount-conditional
  signal would force the first lead-context special case into the
  deliberately-pure scoring engine, not worth it for v1).

---

## 8. Things considered, recommended to skip for v1

- **Commercial property transactions.** 3,000+ counties, no free
  national source, shell-LLC ownership obfuscation. Drop unless a
  paid aggregator gets budgeted.
- **Commercial fleet / vehicle registrations.** DPPA carve-out for
  marketing makes this legally risky for a lead-magnet use case.
  Drop.
- **Business license filings.** Heavy overlap with SoS filings;
  the marginal lead isn't worth the per-city parser work. Defer to
  v2.
- **A second contact-enrichment provider.** Apollo coverage will be
  thin for freshly-filed LLCs. Adding Clearbit / Hunter / People
  Data Labs is the obvious next move, but it's a multi-day effort
  for low v1 marginal value — the "Owner" name from the SoS filing
  itself is usable for the demo.
- **Email outreach drafts.** Already removed from the existing
  pipeline; don't reintroduce.
- **State-by-state full coverage.** 50-state SoS coverage is a
  multi-quarter project. 3-state coverage gets you a credible v1.
- **Per-niche industry classification.** Current 14-tag industry
  enum was tuned for MSP buyers. Insurance buyers span the same
  industries, plus arguably "newly_formed". For v1, reuse the
  existing enum and let `other` absorb the mismatch.
- **A "first hire" velocity signal that requires history.** True
  first-hire detection requires comparing today's signal set to a
  prior snapshot. The pipeline's lookback-only architecture doesn't
  support this without new state. For v1, classify operations-role
  job postings as the proxy and call it "ops hire".
- **`pages/LeadsPage` redesign.** It works. Resist the urge to
  refactor while adding the 4th niche.

---

## Next step

This is a plan, not code. Awaiting review before any file edits.
The fastest concrete first step on approval is steps 1-5 above —
roughly half a day to a live `/insurance` route with the
jobs-insurance source feeding it.
