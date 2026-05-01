# Stock Scanner — Claude Code Handover

**Last updated:** 2026-05-01 — after bhavcopy fix, group widening, and keep-alive patch.
**Audience:** Incoming Claude Code session. Read this before doing anything.

---

## 1. Project Overview

Indian stocks scanner SaaS.

| Layer | Stack | Hosting |
|-------|-------|---------|
| Frontend | React 19 + Vite 7 + TypeScript | **Vercel** — `https://my-screener-theta.vercel.app/` |
| Backend | FastAPI + Pandas (cpu-basic, 16 GB RAM) | **Hugging Face Spaces** — `https://dharmmalik-stock-scanner-backend.hf.space` |
| Daily data | GitHub Actions (`.github/workflows/daily-bhavcopy.yml`) | Pushes EOD OHLC patch to repo; HF startup applies it |
| Local dev | `/Users/dharmender/Desktop/Stock Scanner c` | macOS / zsh |

---

## 2. Repositories & Endpoints

| Resource | Value |
|---|---|
| GitHub repo | `https://github.com/dharmmalik002-ops/mys-screener.git` |
| HF Space | `https://huggingface.co/spaces/dharmmalik/stock-scanner-backend` |
| Backend API | `https://dharmmalik-stock-scanner-backend.hf.space` |
| Health check | `GET /api/health` → `{"ok":true}` (fast, ~2 s) |
| Liveness check | `GET /api/dashboard?market=india` → 200 OK (~3–10 s warm, up to 3 min cold) |
| Production frontend | `https://my-screener-theta.vercel.app/` |

Git remotes (tokens already embedded in `.git/config` — **never commit this file**):

```
hf        https://hf_<TOKEN>@huggingface.co/spaces/dharmmalik/…   ← broken (interactive auth)
hf-push   https://dharmmalik:hf_<TOKEN>@huggingface.co/spaces/…   ← works for manual push
origin    https://github_pat_<TOKEN>@github.com/dharmmalik002-ops/mys-screener.git
```

---

## 3. Deploy Workflow

### Frontend (Vercel — auto)
Push to GitHub `main`. Vercel auto-deploys. No manual step needed.

### Backend (HF Spaces — via GitHub Actions)
Push to GitHub `main` with any change under `backend/**`, `Dockerfile`, or `render.yaml`.
The `deploy.yml` workflow builds a clean snapshot (excluding heavy data files) and
force-pushes it to the HF Space. **Do not push to `hf-push` manually unless the CI is broken.**

Manual fallback (if CI is broken):
```bash
git push hf-push HEAD:main --force
```

**Deploy verification:**
```bash
curl -s -w "HTTP %{http_code} t=%{time_total}\n" "https://dharmmalik-stock-scanner-backend.hf.space/api/health"
# Expect: HTTP 200 in ~2 s
curl -s "https://dharmmalik-stock-scanner-backend.hf.space/api/bhavcopy/status"
# Should show today's date and source=BSE or source=YFINANCE
```

The first `/api/dashboard` after a Space restart can take up to 3 minutes (cold build).
This is normal — snapshot is reconstructed from seed files and bhavcopy patch is applied.

---

## 4. Daily Bhavcopy Workflow

- **BSE primary:** `.github/workflows/daily-bhavcopy.yml`, runs at ~4:15 PM IST on market days.
- **YFINANCE fallback:** same workflow, runs at ~4:30 PM IST if BSE download fails.
- Writes `backend/data/bhavcopy_patch.json` (date, OHLC, volume, PREVCLOSE field "p").
- On HF Space restart, `apply_bhavcopy_patch_on_startup()` in `app/main.py` reads the patch and updates `free_snapshots.json` before serving requests.
- Idempotent — skips re-apply if `bhavcopy_status.json` already shows same date + `schema_version=2`.
- Force-reapply: bump `APPLY_SCHEMA_VERSION` in `providers/free.py` and commit a new bhavcopy_status.

---

## 5. Directory Layout

```
backend/
├── app/
│   ├── api/routes.py            # FastAPI router
│   ├── core/config.py           # Settings (MARKET_CAP_MIN_CRORE, etc.)
│   ├── services/
│   │   ├── dashboard_service.py # ~4000-line orchestration layer
│   │   ├── industry_groups.py   # 96-group scoring + ranking engine
│   │   ├── industry_classifier.py
│   │   └── scanners/
│   ├── providers/free.py        # ~3000-line free data provider (yfinance)
│   ├── models/market.py         # Pydantic response models
│   └── data/
│       ├── bhavcopy_patch.json      # EOD price patch (committed daily)
│       ├── bhavcopy_status.json     # Last-applied date + schema_version
│       ├── free_snapshots_seed_*.json  # Snapshot bootstrap (included in HF deploy)
│       ├── groups/                  # taxonomy.py, keyword_rules.json, peer aliases CSV
│       └── rank_history/            # On-disk daily group-rank snapshots

frontend/
├── src/
│   ├── App.tsx                 # ~5100-line root orchestrator (lazy-loads all panels)
│   ├── lib/api.ts              # All API types + fetch helpers + pingBackendHealth()
│   └── components/             # One CSS file per panel
```

---

## 6. Key Settings

| Setting | Default | Set in | Effect |
|---------|---------|--------|--------|
| `MARKET_CAP_MIN_CRORE` | 1500 | `core/config.py` env override | Scanner universe filter — stocks below this are excluded from scans |
| `STARTUP_CACHE_WARM_ENABLED` | `False` | `core/config.py` | Disabled on HF to avoid boot-time RAM spike |
| `GROUP_MIN_MARKET_CAP_CR` | **250** | `services/industry_groups.py` line 46 | Industry-group eligibility — widened from 1000 so IPOs show a group widget |
| `GROUP_MIN_AVG_DAILY_VALUE_CR` | **0.25** | `services/industry_groups.py` line 47 | Widened from 1.0 |
| `india_eod_only` | `True` | config | No intraday data on HF (eod_only_mode) |

⚠️ **Memory budget (HF 16 GB):** Do not raise `MARKET_CAP_MIN_CRORE` below ~500 (more stocks = more RAM). Do not enable `STARTUP_CACHE_WARM_ENABLED`. Do not use `max_workers > 1` for thread pools. Always call `gc.collect()` after building the dashboard or industry groups.

---

## 7. Industry Groups Engine

File: `backend/app/services/industry_groups.py`

**Ranking formula:**
```
group_score = 0.50 × winsorized_median(126d returns)   ← ~6 months
            + 0.30 × winsorized_median(63d returns)    ← ~3 months
            + 0.20 × winsorized_median(21d returns)    ← ~1 month
```
- Winsorized at 5th/95th percentile before median.
- Dense rank descending by score.
- Groups with < 5 eligible stocks merge into a parent bucket (`unstable_flag=true`).
- Rank-change history stored in `backend/app/data/rank_history/ranks_YYYYMMDD.json`.
- Eligible: NSE or BSE, market_cap > 250 Cr, avg_daily_traded_value_50d > 0.25 Cr.

**RS line on charts:**
- Requires ≥ 252 daily bars (1 year of trading history).
- IPO stocks listed within ~1 year will NOT show the RS line — this is intentional (same as IBD convention). No fix needed; it's mathematically impossible to compute 12-month relative strength with < 12 months of data.

---

## 8. Frontend Architecture

`App.tsx` is the root (~5100 lines). Key patterns:

- All heavy panels are **lazy-loaded** via `React.lazy`.
- `groupsData: IndustryGroupsResponse | null` drives both the Groups tab and the chart's group widget.
- `resolveChartGroupContext(symbol, groupsData)` computes which group a charted stock belongs to — returns `null` if the stock isn't in the groups payload (IPOs below the eligibility filter).
- **Keep-alive:** a `useEffect` in `App.tsx` pings `/api/health` every 4 minutes while the document is visible, preventing HF Space from sleeping mid-session.
- Chart data: `getChart()` → `/api/chart/{symbol}` (cached, 30 s timeout). `getChartHistory()` → `/api/chart/{symbol}/history` (uncached live fetch, 35 s timeout).
- Chart prefetch: hovering a scan row calls `getChart()` in the background (commit `66ac3b1`).

---

## 9. Key API Endpoints

| Endpoint | Notes |
|---|---|
| `GET /api/health` | Always fast, `{"ok":true}`. Use for liveness. |
| `GET /api/dashboard?market=india` | Top gainers/losers/volume, scanner list. Slow on first cold-start. |
| `GET /api/scan-counts?market=india` | Scan hit counts only (lightweight). |
| `GET /api/scans/{scan_id}?market=india` | Full scan results. |
| `GET /api/groups?market=india` | 96-group rankings (slow on first build, then cached). |
| `GET /api/chart/{symbol}?timeframe=1D&market=india` | Daily bars + RS line (cached). |
| `GET /api/chart/{symbol}/history?timeframe=1D&market=india` | Full history, live yfinance fetch. |
| `GET /api/bhavcopy/status` | Last applied patch date + source. |
| `GET /api/fundamentals/{symbol}?market=india` | Company fundamentals. |

---

## 10. Coding Rules

1. **Fix-first, minimal explanation.** Final output: `DONE / Changed files / Run commands / Status`.
2. **Reproduce before fixing** — find root cause, not symptom.
3. **Surgical changes only.** Don't refactor adjacent code unless asked.
4. **Frontend resilience.** Every panel must guard for empty / null API data.
5. **Never `git add .`** — specify files. `.git/config` contains live tokens.
6. **Never commit tokens.** HF secret scanner blocks pushes with `hf_…` strings.
7. **No `git push --force` to `main`** without explicit user authorisation.
8. **Use `logger.debug()` / `logger.info()`**, not `print()`, in backend.
9. **Verify deploys** by checking `/api/bhavcopy/status` date and `/api/health`.

---

## 11. Common Gotchas

1. **`/api/health` returns 200 fast.** Use it for liveness. `/api/dashboard` is slow on cold-start (up to 3 min) — not a bug.
2. **HF Space cold-start after deploy:** the Space rebuilds from seed snapshots + applies bhavcopy patch on first request. Budget 2–3 minutes.
3. **GitHub Actions keep-alive (`keep-alive.yml`) is unreliable** on free-tier — GH delays/skips cron jobs. Client-side 4-min ping in `App.tsx` provides defence-in-depth.
4. **`git push hf main` requires interactive auth** (broken in automated flow). Use `hf-push` remote or GitHub Actions.
5. **IPO charts:** first load is slow (~5–8 s) because there's no chart cache yet — yfinance live fetch. Subsequent loads are instant (cache populated). RS line won't appear until the stock has 252 trading days of history.
6. **`GET /api/dashboard` slow after deploy** — this is snapshot cold-start, not a regression.
7. **`AGENTS.md` no longer exists** in the repo. Coding rules live in §10 above and in Claude memory (`feedback_coding_rules.md`).

---

## 12. Recent Commits (as of 2026-05-01)

| Commit | Summary |
|--------|---------|
| `1e418a5` | fix(perf): keep HF Space warm + widen group eligibility (GROUP_MIN_MARKET_CAP_CR 1000→250) |
| `b00b09b` | data: YFINANCE EOD bhavcopy patch for 2026-05-01 |
| `6bcbd93` | fix(bhavcopy): use bhavcopy PREVCLOSE (p) for change_pct (fixes stale breadth) |
| `3ac8961` | fix(bhavcopy): apply committed patch on startup so prices stay fresh |
| `194e17c` | feat(chart): make group widget floating, draggable, resizable, toggleable |
| `873e0e8` | feat(chart): persistent group widget + side-by-side compare mode |
| `66ac3b1` | perf(watchlists): prefetch chart on row hover |

---

## 13. First-Session Checklist

1. Read this file end-to-end.
2. `git status` — should be clean. `git remote -v` — should show `origin`, `hf`, `hf-push`.
3. Smoke test: `curl -s "https://dharmmalik-stock-scanner-backend.hf.space/api/health"` → `{"ok":true}`.
4. Check today's bhavcopy: `curl -s "https://dharmmalik-stock-scanner-backend.hf.space/api/bhavcopy/status"` — date should be today or yesterday.
5. Ask the user what they want done.
