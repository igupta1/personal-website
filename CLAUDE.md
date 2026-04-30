# CLAUDE.md

This repo contains **two projects** that share a Vercel deployment.

## 1. Personal website (existing)

- **Stack:** Create React App, React 19, React Router v7, Tailwind CSS
- **Routes** (`src/App.js`):
  - `/` → `About`
  - `/gtm` → `AITools`
- **New routes coming:** `/it-msps`, `/mssps`, `/cloud`
- **Layout:** pages in `src/pages/`, components in `src/components/`

**Do not modify existing routes or pages without explicit instruction.** This includes:
- `src/App.js` (routing)
- `src/pages/About.js`
- `src/pages/AITools.js`
- `src/components/Header.js`
- `src/components/Footer.js`

## 2. MSP lead magnet pipeline (new, building now)

- **Location:** `pipeline/` — Python, isolated from the React app
- **Output:** JSON written to Vercel Blob storage
- **Conventions:** see `pipeline/CLAUDE.md` for full Python conventions
- **Scope:** scraping pipeline + 3 new React pages + 2 Vercel serverless functions to bridge them

## API layer (to be created)

Vercel serverless functions live in `api/` at the repo root:

- `api/upload-leads.js` — pipeline POSTs leads JSON here
- `api/generate-leads.js` — React pages GET leads JSON from here

**Storage:** Vercel Blob, via the `BLOB_READ_WRITE_TOKEN` env var.

## Forbidden without explicit instruction

- Modifying `vercel.json`
- Committing `.env` or `.env.local` files
- Modifying existing site code: `App.js` routes, `About.js`, `AITools.js`, `Header.js`, `Footer.js`
- **Wholesale destruction:** `rm -rf` of directories, wiping the pipeline or its data, `git reset --hard`, force push, dropping database tables, truncating data, mass-deleting files. Single-file deletes (orphan fixtures, generated artifacts, etc.) are fine without asking.

The user will explicitly say when wholesale-destructive operations are okay. Until then, ask before doing anything irreversible at scale.

## Conventions

- **React:** follow existing patterns — Tailwind classes, component style of the existing pages
- **Python:** see `pipeline/CLAUDE.md`
