# CLAUDE.md

Ishaan's personal website + its lead-magnet API. Deployed on Vercel.

## Stack
- Create React App, React 19, React Router v7, Tailwind CSS.
- Pages in `src/pages/`, components in `src/components/`.
- Routes (`src/App.js`): `/`→About, `/gtm`→AITools. (The lead pages
  `/it-msps`, `/mssps`, `/cloud`, `/insurance`, `/cfo` and their shared
  `LeadsPage`/`LeadCard`/`LeadFilters` components were removed; unknown paths
  redirect to `/`.)

## API (`api/`, Vercel serverless)
- `upload-*` — the lead pipelines POST inventories here (→ Vercel Blob).
- `generate-*` / `leads` / `niches` — the React pages GET leads (← Vercel Blob).
- Storage: Vercel Blob, via `BLOB_READ_WRITE_TOKEN`.

The lead pipelines now live in the separate **lead-platform** repo and the
cold-outreach engine in **outreach-platform**. They talk to this deployment
only over HTTP (pipelines POST to `api/upload-*`), so they are not part of this
repo.

## Forbidden without explicit instruction
- Modifying `vercel.json`.
- Committing `.env` / `.env.local`.
- Wholesale-destructive ops (rm -rf of dirs, git reset --hard, force push, mass deletes).
