# Stock Scanner — Handoff Brief for a New Claude Code Operator

Written 2026-10-08. Read this first, then `CLAUDE.md` (the full system reference; Claude Code loads it automatically from the repo root), then `DESIGN.md` before any UI work.

---

## 1. What this project is

An Indian-stocks (NSE/BSE) scanner SaaS plus research tooling.

| Layer | Stack | Where it runs |
|---|---|---|
| Frontend | React 19 + Vite 7 + TypeScript (`frontend/`) | Vercel, https://my-screener-theta.vercel.app/ — auto-deploys on push to `main` |
| Backend | FastAPI + Pandas (`backend/`) | Hugging Face Space (Docker, 16 GB RAM) — deployed by `.github/workflows/deploy.yml` when `backend/**` changes |
| Data | Yahoo, BSE, NSE bhavcopy | GitHub Actions: `daily-bhavcopy.yml` (~4:20 PM IST), `bot-refresh.yml`, `lookalike-daily.yml`, `mutual-funds-refresh.yml`, `breakout-stats.yml` |
| AI | Gemini | Prose only (reviews, journal analysis). Numbers are always computed in Python |

Main areas: technical scanners, sector/industry groups, Markets regime page, trade journal, mutual-fund screener, Chart Gym (study drills), chart look-alikes, a research course page, and a **trading-bot research subsystem** (`backend/app/services/bot/`).

---

## 2. Day-one setup

```bash
cd "/Users/dharmender/Desktop/Stock Scanner c"
cd backend && pip install -r requirements.txt && python run_local.py      # http://localhost:8000
cd frontend && npm install && npm run dev                                  # http://localhost:5173
cd backend && pytest                                                       # backend tests
cd frontend && npx --no-install tsc --noEmit                               # type check
```

Notes:
- Local backend never serves `/api/dashboard` or `/api/groups` quickly; verify data-driven UI another way.
- `backend/tests` has **4 known pre-existing failures** (2 in dashboard_service, 2 date-dependent in free_provider). Don't chase them.
- A stale `frontend/vite.config.js` can shadow `vite.config.ts` and silently ignore `VITE_PROXY_TARGET`.
- Large local-only data (gitignored): `backend/app/services/bot/` deep history (~100 MB, rebuild with `scripts/build_deep_history.py`, ~10 min), chart cache, `data/x_archive/`, `data/quarterly_results/`. Without them the bot backtests and course export cannot be re-run; the committed JSON artifacts still serve the site.

## 3. Credentials and access (you must obtain these yourself)

- **Nothing secret is in this document, and none should ever be committed.** Git credentials live in `.git/config` and the macOS Keychain on the owner's machine.
- You will need: push access to the GitHub repo (`dharmmalik002-ops`), a GitHub PAT with `repo` + workflow scope, a Hugging Face token (the owner's was expired as of April 2026), the Space's `GEMINI_API_KEY` secret, and Vercel access. Ask the owner to grant these through proper channels, or work on a fork/branch and have the owner merge.
- Without these you can still develop and test locally; you just cannot deploy.

## 4. Non-negotiable rules (from CLAUDE.md — the short list)

1. **Never `git add .` or `git add -A`.** Add files explicitly. Never commit `.git/config` or tokens.
2. Pushing to `main` deploys to production (Vercel immediately; the Space if `backend/**` changed). Prefer a branch + PR unless the owner says otherwise.
3. **Response/commit style:** fix-first, minimal narration; finish with `DONE / Changed files / Run commands / Status`.
4. Make surgical changes. Reproduce the bug first. No drive-by refactors.
5. Frontend must tolerate null/empty/missing API data (optional chaining, loading and error states).
6. **New `history_source` labels must be added to `RELIABLE_HISTORY_SOURCES`** in `backend/app/providers/free.py`, or all 20d/50d volume baselines go to zero.
7. Keep `MARKET_CAP_MIN_CRORE >= 500`; never set `STARTUP_CACHE_WARM_ENABLED=True` (16 GB limit).
8. Mutual-fund route handlers stay sync `def`, never `async def`.
9. **New binary assets** (images, `.npy`, `.db`) must be added to `.github/hf-deploy-excludes.txt` or the Space's pre-receive hook rejects the push and the daily data silently stops shipping.
10. Full-screen overlays must `createPortal(..., document.body)`.
11. Scanner code that counts sessions must read true daily closes, not the chart grid (~2.17 sessions per point).
12. Fund/AI features describe measured evidence; they never give personalised investment advice.
13. Anything "as of" uses the data session date, not build time.
14. Journal and watchlist writes against the live backend hit the user's only copy. Snapshot first; never use them as test fixtures.

CLAUDE.md gotchas 1–150 each record a failure that already happened. Before touching an area, search CLAUDE.md for its keywords.

## 5. Current state (as of 2026-10-08)

- Branch `main`; latest commits: course charts stored on site, Course page, fundamentals/peers tab, MF universe refresh, research-layout screener.
- Uncommitted at handoff: `.claude/launch.json`, `backend/data/bhavcopy_status.json` (modified) and four untracked `backend/app/data/rank_history/ranks_2026092x/1001/1007.json` files. Inspect with `git status`/`git diff`; the rank files are generated data, the owner decides whether to commit them.
- A parked chart WIP exists on branch `wip/chart-journal-markers` (journal markers/compare on charts); needs reapplying on current `main`.
- Honest status of the **trading bot**: it is a research instrument, not a trading system. Realistic walk-forward is about +29%/yr (executable, pre-tax, survivorship-flattered, capacity ~Rs 1–2 cr). Earlier higher numbers came from look-ahead bugs that were found and fixed. Do not quote any bot figure without its caveats (gotchas 59, 115, 124).
- Look-alikes: recognises a trader's style (~85%) but does not predict winners; the page must keep both numbers visible.

## 6. Suggested first-week plan

1. Read `CLAUDE.md` sections 1–5 and `DESIGN.md`. Run backend + frontend locally; confirm `pytest` shows only the 4 known failures.
2. Check production health:
   ```bash
   curl -s https://dharmmalik-stock-scanner-backend.hf.space/api/health
   curl -s https://dharmmalik-stock-scanner-backend.hf.space/api/bhavcopy/status
   ```
   Compare bhavcopy status with the newest `data:` commit; a mismatch means the Space push failed (gotcha 21). Recovery: run the **Deploy to HuggingFace** workflow via `workflow_dispatch`.
3. Look at the last runs of each GitHub workflow listed in section 1; fix or report red ones.
4. Resolve the uncommitted files and decide on the parked chart WIP with the owner.
5. Only then take new feature work, from the owner's priorities.

## 7. How to work

- Branch per task; small commits; run `pytest` (backend) and `tsc --noEmit` (frontend) before pushing.
- UI changes: verify in a browser at desktop and phone widths, in both designs (Studio default, Classic) and both themes.
- Data/bot changes: re-run the relevant script and the matching tests (`test_bot_*.py`, `test_study_*.py`, etc.). A new setup that fires zero signals is a bug until proven otherwise.
- Any statistic you add needs its sample size and a matched control. Several earlier "findings" were artefacts.
- If a number looks too good, look for look-ahead or accounting bugs before believing it.

---

## 8. REPORT-BACK TEMPLATE (fill this in when you finish and send it to the owner)

Copy this into `HANDOFF_LOG.md` (append, don't overwrite) and paste it to the owner's Claude session.

```
Date / operator:
Branch(es) and commit hashes:

1. What I was asked to do:
2. What I changed (files + one line each):
3. What I ran to verify (commands + results; test counts; failures that remain):
4. Deployed? (Vercel / HF Space / workflows touched) and how I confirmed it:
5. Data or workflow state changed (artifacts regenerated, schema versions bumped, files added to hf-deploy-excludes.txt):
6. New gotchas learned (candidates for CLAUDE.md):
7. Things I did NOT finish or am unsure about:
8. Anything that needs the owner's decision or credentials:
9. Uncommitted / local-only state the owner should know about:
```

When the owner receives this, they can paste it to their Claude Code session, which will update `CLAUDE.md` and memory accordingly.
