# Stock Scanner — Claude Code Handover

> ⚠️ **This document is from 2026-04-25 and is no longer the authoritative handover.**
> The current handover lives at **`CLAUDE_CODE_HANDOVER.md`** (last updated 2026-05-03).
> Read that one first. The architecture sections below are still accurate but the
> "Recent Commits", "Schema versions", "Key settings" and "Common gotchas" sections
> have been superseded.

**Purpose:** Everything an incoming Claude Code session needs to understand this project, ship changes, and deploy them.
**Audience:** A new Claude Code agent with no prior context on this codebase.
**Last updated:** 2026-04-25 — after the 5-stage Scanner UI modernisation.

> Feed this file to a fresh Claude session with:
> *"Read /Users/dharmender/Desktop/Stock Scanner c/CLAUDE_HANDOVER.md before doing anything."*

---

## 1. Project Overview

Indian stocks scanner SaaS, India-only (NSE).

| Layer | Stack | Hosting |
|-------|-------|---------|
| Frontend | React 18 + Vite + TypeScript | **Vercel** |
| Backend | FastAPI + Pandas + (numerical libs) | **Hugging Face Spaces** (Docker, cpu-basic, 16 GB RAM) |
| Daily data refresh | GitHub Actions (`.github/workflows/daily-bhavcopy.yml`) | Pushes EOD data to HF Space |
| Local dev path | `/Users/dharmender/Desktop/Stock Scanner c` | macOS / zsh |

The repo has **NO** `AGENTS.md` file currently — the coding rules used during handovers are tracked in Claude memory, summarised in §6 below.

---

## 2. Repositories, URLs, and Live Endpoints

| Resource | Value |
|---|---|
| GitHub repo | `https://github.com/dharmmalik002-ops/mys-screener.git` |
| HF Space | `https://huggingface.co/spaces/dharmmalik/stock-scanner-backend` |
| Backend API base | `https://dharmmalik-stock-scanner-backend.hf.space` |
| Liveness check | `GET https://dharmmalik-stock-scanner-backend.hf.space/api/dashboard?market=india` (200 OK in ~2-3 s) — note `/health` returns 404 |
| Production frontend | `https://my-screener-theta.vercel.app/` |
| Vercel project name | `dharmmalik002-ops-projects/frontend` |

`origin` and `hf` are both configured as git remotes:

```bash
$ git remote -v
hf       https://hf_<TOKEN>@huggingface.co/spaces/dharmmalik/stock-scanner-backend
hf-push  https://dharmmalik:hf_<TOKEN>@huggingface.co/spaces/dharmmalik/stock-scanner-backend
origin   https://github.com/dharmmalik002-ops/mys-screener.git
```

⚠️ The `hf` and `hf-push` URLs in `.git/config` already contain tokens. **Do not commit `.git/config` anywhere.**

---

## 3. Repository Layout

```
Stock Scanner c/
├── backend/
│   ├── app/
│   │   ├── api/routes.py            # FastAPI router
│   │   ├── services/
│   │   │   ├── dashboard_service.py # Orchestration layer (chart, scans)
│   │   │   ├── industry_classifier.py
│   │   │   ├── industry_groups.py   # 96-group taxonomy + ranking engine
│   │   │   └── scanners/            # Custom, Minervini, GapUp, etc.
│   │   ├── providers/
│   │   │   └── free.py              # ~3000-line free data provider (yfinance + others)
│   │   ├── models/market.py         # Pydantic response models
│   │   └── data/
│   │       ├── groups/              # taxonomy.py, keyword_rules.json, peer aliases CSV
│   │       └── rank_history/        # On-disk daily group-rank snapshots
│   ├── scripts/                     # Diagnostic scripts (untracked test_*.py allowed)
│   ├── requirements.txt
│   └── run_local.py
│
├── frontend/
│   ├── package.json                 # vite 7, recharts 3, lucide-react 1
│   ├── src/
│   │   ├── App.tsx                  # ~5000-line orchestrator; lazy-loads every panel
│   │   ├── lib/
│   │   │   ├── api.ts               # All API types + fetch helpers
│   │   │   └── virtualRows.ts       # Custom row virtualization
│   │   ├── styles/
│   │   │   ├── app.css              # Legacy global CSS (~6000 lines)
│   │   │   └── premium-overrides.css
│   │   └── components/              # One CSS file per panel where possible
│   │       ├── HomePanel.{tsx,css}
│   │       ├── GroupsPanel.{tsx,css}
│   │       ├── ScreenerSidebar.{tsx,css}
│   │       ├── ScannerHeader.{tsx,css}      # Stage 3
│   │       ├── QueryBuilder.{tsx,css}       # Stage 3
│   │       ├── ScanDashboard.{tsx,css}      # Stage 2
│   │       ├── ScanTable.{tsx,css}          # Stage 4
│   │       ├── ScanFooter.{tsx,css}         # Stage 5
│   │       ├── CustomScannerPanel.tsx       # 30+ filter form
│   │       ├── ChartPanel.tsx               # Right-rail chart + fundamentals
│   │       └── ChartGridModal.tsx           # Grid view modal
│   └── vercel.json                  # Vercel config (if present)
│
├── CLAUDE_HANDOVER.md               # ← THIS FILE
├── CLAUDE_CODE_HANDOVER.md          # Older handover (April 2026)
├── HANDOVER_FULL.md                 # Original full handover (verbose)
├── FREE_DEPLOYMENT_GUIDE.md
└── README.md
```

---

## 4. Credentials

> 🚫 **NEVER commit live tokens to the repo.** Hugging Face's secret scanner will block any push containing `hf_…` strings. The user re-pastes tokens per session.

| Secret | Stored where | Purpose | Status (2026-04-25) |
|---|---|---|---|
| HF Token (`hf_imuaN…` aka "stock new", role `write`) | User pastes per session; embedded in `.git/config` `hf-push` remote | Push backend & upload via `huggingface_hub.HfApi` | ✅ verified working via `/api/whoami-v2` |
| GitHub PAT (`ghp_eLXdS…`) | User pastes per session | `git push origin main` | ⚠️ Fine-grained PAT — API auth works, but `git push` may 403 if `Contents:write` scope missing. User likely needs a **classic** PAT with `repo` scope |
| Vercel Token | `~/.vercel/auth.json` (set up via `npx vercel login`) | `npx vercel deploy --prod` | ✅ working |
| Gemini API Key | Set as HF Space secret (`GEMINI_API_KEY`) | AI summaries | not tested recently |
| Maintenance Token | Set as HF Space secret (`MAINTENANCE_TOKEN`) | Trigger cache refresh `/api/maintenance/*` | not tested recently |

**Where to find the actual token strings on this machine:** ask the user. They paste at session start. Do not echo them back to chat or commit them anywhere.

---

## 5. Build & Deploy Workflow (the commands that actually work)

### 5.1 Frontend (Vercel)

```bash
# From repo root
cd frontend
npm install                               # only after package.json changes
npm run build                             # produces dist/

# Commit & push
cd ..
git add frontend/src/...                  # specific files only — never `git add .`
git commit -m "feat(scanner): …"
git push origin main

# Deploy preview to production
cd frontend
npx vercel deploy --prod --yes
# Capture the returned preview URL, e.g.
# https://frontend-<hash>-dharmmalik002-ops-projects.vercel.app

# CRITICAL: alias the preview URL to the production domain — Vercel's
# `--prod` flag does not automatically promote it for this project.
npx vercel alias set frontend-<hash>-dharmmalik002-ops-projects.vercel.app \
                     my-screener-theta.vercel.app
```

**Verify the deploy actually went live:** the bundle hash in the production page changes (`assets/index-<hash>.js`). Use:

```bash
curl -s https://my-screener-theta.vercel.app/ | grep -oE 'index-[A-Za-z0-9_-]+\.js' | head -1
curl -s https://my-screener-theta.vercel.app/assets/<NewBundle>.js | grep -oE 'YourMarkerString' | head -3
```

### 5.2 Backend (Hugging Face Spaces)

⚠️ `git push hf main` is **blocked** because data files >10 MB live in `backend/app/data/`. Use the Python API instead:

```python
from huggingface_hub import HfApi
api = HfApi(token=HF_TOKEN)  # paste the hf_ token at session start

api.upload_file(
    repo_id="dharmmalik/stock-scanner-backend",
    repo_type="space",
    path_or_fileobj="backend/app/services/dashboard_service.py",
    path_in_repo="backend/app/services/dashboard_service.py",
    commit_message="fix: …",
)
```

For multi-file commits use `api.create_commit()` with `CommitOperationAdd` operations.

**Verify the HF deploy:**
```bash
# Wait ~30-90 s for the Space to rebuild, then:
curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" \
  "https://dharmmalik-stock-scanner-backend.hf.space/api/dashboard?market=india"
# Expect: 200 ~2-3 s
```

If it returns 503 or "Space is sleeping", the cold-start can take 60-90 s. Frontend shows *"Failed to fetch. Showing cached market snapshot."* during this window — that's normal, **not** a deployment bug.

### 5.3 Don't push to HF every change
Only push backend changes to HF. Pure frontend work goes through Vercel only — no HF deploy needed.

---

## 6. Coding Rules (from prior owner)

These are tracked in Claude memory under `feedback_coding_rules.md`. The originating `AGENTS.md` is no longer in the repo:

1. **Fix-first, minimal explanation.** No long narration. Final summaries should follow `DONE / Changed files / Run commands / Status` format unless the user asks for more.
2. **Reproduce bugs before fixing** — find root cause, not symptom.
3. **Keep changes minimal and surgical.** Don't refactor adjacent code unless asked.
4. **Frontend resilience.** Every panel must guard for empty / partial / null API data — loading and error states are required.
5. **Never `git add .` or `git add -A`.** Always specify files. The repo contains untracked diagnostic scripts and a live `.git/config` that must not leak.
6. **Don't commit live tokens.** HF secret scanner will block.
7. **No `git push --force` to `main`** unless the user explicitly authorises it.

---

## 7. Recent Work — 5-Stage Scanner UI Modernisation (April 2026)

The Scanner tab was rebuilt to a premium fintech aesthetic (primary `#6366f1`, light surfaces `#f9fafb`, dark-mode tokens, Inter/Outfit font, Lucide icons, Recharts for analytics). All 5 stages are live on `my-screener-theta.vercel.app` as of commit `ba68449`.

| Stage | Components shipped | Bundle | Notes |
|-------|--------------------|--------|-------|
| 1 | `ScreenerSidebar.{tsx,css}` | 4.6 kB | "+ New Screener" CTA, scanner list with per-mode lucide icons, saved presets nested under parent. (Quick Filters and Pro Features were added then removed at user's request — keep them gone.) |
| 2 | `ScanDashboard.{tsx,css}` | 6.8 kB | Recharts dashboard above the table: Total Results sparkline, Groups mini-donut, Market Cap donut + legend, Group Distribution bar (sourced from `IndustryGroupsResponse`) |
| 3 | `ScannerHeader.{tsx,css}` + `QueryBuilder.{tsx,css}` | 2.6 + 5.5 kB | Bold title with edit-icon rename, "311 stocks found" pill, purple **Run Scanner** CTA, removable filter chips with AND/OR group toggle |
| 4 | `ScanTable.{tsx,css}` rewrite | 15.5 kB | View tabs (Table/Grid/Chart), Sort By + Columns popovers, stock logos, inline 52-px SVG sparklines, RS Rating gradient circles (green/amber/red), Watch button. Preserves virtualization, sector grouping, and ChartGridModal. |
| 5 | `ScanFooter.{tsx,css}` | 6.1 kB | Scan Performance card (Execution Time / Last Refreshed / Results / Accuracy), Top Gainers, Top Losers with mini-trend bars |

**Deployment artifacts after Stage 5 + tightening:**
- HEAD: `ba68449 fix(scanner): tighten Scan Performance + Market Cap legend text`
- Vercel deployment: `frontend-prwin0i3d…` aliased to `my-screener-theta.vercel.app`
- No backend changes were needed for any of the 5 stages.

**Things deliberately NOT touched** (don't refactor unless asked):
- The 30+ filter form in `CustomScannerPanel.tsx` — `QueryBuilder` is a visual chip rail above it; the form remains the source of truth for `customFilters` (see `DEFAULT_CUSTOM_FILTERS` in `App.tsx:352`)
- `ScanTableProps` API — App.tsx integration uses the same prop signature
- Existing virtualization in `ScanTable` (`useVirtualRows`) and sector grouping logic

---

## 8. Architecture Cheat-Sheet

### Frontend — App.tsx orchestration

`App.tsx` is the giant root component. Key state:

| State | Type | Drives |
|---|---|---|
| `activePage` | `"home" | "screener" | "watchlists" | "groups" | …` | Which top-level tab is shown |
| `activeMarket` | `"india"` | NSE-only (US has been removed) |
| `activeScanner` | `ScreenerMode` | Which scanner panel is shown inside Screener page |
| `customFilters` | `CustomScanRequest` | Live form state for Custom Scanner |
| `appliedCustomFilters` | `CustomScanRequest` | Last-submitted filters; bumping `scannerRunNonce` triggers refetch |
| `scanResults` | `ScanResultsResponse \| null` | Backend response |
| `groupsData` | `IndustryGroupsResponse \| null` | Industry group rankings (used by Groups tab AND ScanDashboard) |
| `savedScanners` | `SavedScannerPreset[]` | localStorage-persisted presets |
| `activeSavedScannerId` | `string \| null` | Currently loaded preset (gates the rename icon) |

All panel components are **lazy-loaded** via `React.lazy` and wrapped in a single `<Suspense fallback={…}>` near the page-render point. When you add a new panel, follow the same pattern.

### Frontend — key API calls (`src/lib/api.ts`)

```ts
fetchDashboard(market)              // Top gainers/losers, market summary
fetchScanResults(scanId, request)   // Scanner results (custom or built-in)
fetchIndustryGroups(market)         // /api/groups — used by Groups tab + ScanDashboard
fetchChart(symbol, …)               // Right-rail chart bars + RS line
```

Important type definitions live at the top of `src/lib/api.ts`:
- `ScanMatch` (line 9) — every row in scan results
- `ScanResultsResponse` (line 71)
- `IndustryGroupsResponse` (search for it)
- `CustomScanRequest` (search) — backend-bound filter shape

### Backend — key endpoints (`backend/app/api/routes.py`)

| Endpoint | Purpose |
|---|---|
| `GET /api/dashboard?market=india` | Home page data + liveness check |
| `POST /api/scan/{scan_id}` | Run a scanner (custom, gap-up-openers, ema-expansion, …) |
| `GET /api/chart?symbol=…` | Daily bars + RS line for ChartPanel |
| `GET /api/groups?market=india` | 96-group rankings used by Groups tab + ScanDashboard |
| `GET /api/fundamentals?symbol=…` | Used by ChartPanel fundamentals tab |

### Backend — services

- `dashboard_service.py` — Orchestration. Builds dashboard, scan results, industry groups payload. `_top_industry_groups_response()` was simplified to a passthrough so all 74 groups are returned.
- `industry_groups.py` — 96-group taxonomy with weighted-median scoring (`0.50 × med(126d) + 0.30 × med(63d) + 0.20 × med(21d)`), 5/95 winsorization, parent-bucket merge for under-5 groups (`unstable_flag`), on-disk daily rank-history snapshots in `backend/app/data/rank_history/`.
- `industry_classifier.py` — 4-layer deterministic classifier: override CSV → keyword JSON → peer aliases → needs_review queue. Confidence threshold `0.35`.
- `providers/free.py` — Free data provider, ~3000 lines. Chart logic at ~line 1945. **Use `logger.debug()`, not `print()`.**

### Backend memory budget (HF cpu-basic = 16 GB)

| Setting | Value | Why |
|---|---|---|
| `MARKET_CAP_MIN_CRORE` | 1500.0 | Cuts universe by ~40% |
| `STARTUP_CACHE_WARM_ENABLED` | `False` | Avoids boot-time spike |
| Thread pools | `max_workers=1` on HF | Memory headroom |
| `gc.collect()` | After dashboard / industry-groups builds | Release pandas frames |

If you bump any of these, expect OOM crashes within minutes on HF.

---

## 9. Common Gotchas

1. **`/health` is 404.** Use `/api/dashboard?market=india` for liveness.
2. **HF cold-start (~60-90 s).** "Failed to fetch" on the frontend is normal during this window. Don't chase it as a bug unless the Space stays cold past ~2 min.
3. **`git push hf main` is blocked.** Always use `huggingface_hub.HfApi` for backend deploys.
4. **Vercel `--prod` flag doesn't promote.** You must run `npx vercel alias set <preview> my-screener-theta.vercel.app` after every prod deploy.
5. **HEAD requests return 405.** The backend only accepts GET. Do not use `curl -sI` to test API liveness — it always 405s. Use `curl -s -o /dev/null -w "%{http_code}\n"`.
6. **The bundle hash doesn't change after backend-only edits.** That's expected. To verify a frontend change shipped, search the new JS bundle for a marker string (a class name or literal you added).
7. **Don't restart the Custom Scanner form.** `QueryBuilder` is a visual chip rail above the existing `CustomScannerPanel`. Both must stay in sync — chip removals call `onFiltersChange({...filters, [key]: defaultValue})`.
8. **Saved scanners are localStorage-only.** Key prefix: `marketScopedKey(SAVED_SCANNERS_KEY, activeMarket)`.
9. **Untracked test scripts are allowed.** `backend/scripts/test_get_chart.py` and similar diagnostic files are intentionally untracked. Don't `git add` them.
10. **`AGENTS.md` no longer exists in the repo.** The rules are encoded in §6 above and in the user's Claude memory.

---

## 10. The "I'm Done" Format

When you finish a task, end with:

```
DONE

Changed files
- frontend/src/components/X.tsx
- frontend/src/components/X.css

Run commands
- npm run build && git push origin main
- npx vercel deploy --prod --yes
- npx vercel alias set <hash>.vercel.app my-screener-theta.vercel.app

Status
- Live at https://my-screener-theta.vercel.app/ (verified bundle hash X)
```

No paragraphs of explanation unless the user asks. The owner prefers brevity and action.

---

## 11. First-Session Checklist for Incoming Claude

1. Read this file end-to-end.
2. Confirm `git status` is clean and `git remote -v` shows `origin` + `hf`.
3. Ask the user to paste the **HF Token** and (if pushing to GitHub) a **classic GitHub PAT with `repo` scope**.
4. Run a quick smoke test: `curl -s -o /dev/null -w "%{http_code}\n" https://dharmmalik-stock-scanner-backend.hf.space/api/dashboard?market=india` should return `200`.
5. Then: ask the user what they want done.
