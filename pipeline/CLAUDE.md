# pipeline/CLAUDE.md

Python pipeline for the MSP lead magnet. Read this BEFORE working in `pipeline/`.

## What this project does

Scrapes signals about SMBs from three sources, attaches them to companies, scores
each company against three MSP niches, generates per-niche outreach copy, and
writes a JSON blob consumed by the React pages at `/it-msps`, `/mssps`, `/cloud`.

## Architecture (deliberately simple)

- Single SQLite table `leads` (one row per company).
- Signals stored inline as a JSON list on each row.
- Per-niche scores and copy stored as columns.
- Daily job: fetch sources → attach signals → enrich → score → regenerate copy → upload.

## Niches

- `it_msp` — IT MSPs (help desk, sysadmin, general IT support)
- `mssp` — managed security service providers
- `cloud` — cloud consultancies / AWS/GCP/Azure partners

The same company gets three scores, three insights, three outreach drafts.

## Sources

Each source module wraps multiple free APIs:

- `msp_pipeline/sources/jobs.py` — JobSpy + Adzuna + HN Algolia
- `msp_pipeline/sources/funding.py` — SEC EDGAR Form D + TechCrunch RSS + PR Newswire RSS
- `msp_pipeline/sources/breaches.py` — California AG + Maine AG + Washington State AG

## Signal types

From jobs: `job_posted_it_support`, `job_posted_it_leadership`,
`job_posted_security`, `job_posted_cloud_devops`, `exec_hired`
From funding: `funding_raised`
From breaches: `breach_disclosed`
Computed: `headcount_threshold_crossed`, `headcount_growth_rapid`

## Conventions

- Python 3.11+, type hints required everywhere.
- Pydantic models for structured data.
- All LLM calls go through `msp_pipeline/llm.py` wrappers (never call the
  Anthropic or Gemini SDKs directly elsewhere).
- Tests use captured fixtures, never live HTTP.
- One source module per file in `msp_pipeline/sources/`.

## Setup

```bash
cd pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env  # fill in real keys
```

## Running the pipeline

```bash
source .venv/bin/activate
python -m msp_pipeline.daily_run
```

## Testing / linting

```bash
source .venv/bin/activate
pytest
ruff check msp_pipeline tests
mypy msp_pipeline
```

## Storage

`data/leads.db` is committed to git so the lead set persists across runs.
`.env` is never committed; copy `.env.example` to start.

## Forbidden without explicit user instruction

Deleting individual files (e.g. orphan fixtures, generated artifacts) is fine.
What needs explicit permission is anything that destroys real work or shared
state:

- Wholesale removal: `rm -rf` on directories, removing source modules, wiping
  `pipeline/` or `msp_pipeline/`.
- Dropping or recreating `data/leads.db`, `DROP TABLE`, `DELETE FROM` on
  populated tables, `TRUNCATE`.
- Force push, `git reset --hard`, history rewrites on shared branches.
- Modifying anything outside `pipeline/` (the React app and root configs are
  out of scope here — see root `CLAUDE.md`).
