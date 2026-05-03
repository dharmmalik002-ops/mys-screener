# Stock Scanner — Claude Code Handover

**Last updated:** 2026-05-03 — after a multi-day stabilisation pass: snapshot freshness, scanner correctness, earnings widget, watchlist UX.
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

- **BSE primary:** `.github/workflows/daily-bhavcopy.yml`, runs at ~4:20 PM IST on market days (Mon–Fri).
- **YFINANCE fallback:** same workflow, retries through 6:30 PM IST.
- Writes `backend/data/bhavcopy_patch.json` (date, OHLC, volume, PREVCLOSE field "p").
- On HF Space restart, `apply_bhavcopy_patch_on_startup()` in `app/main.py` reads the patch and updates `free_snapshots.json` before serving requests.
- Idempotent — skips re-apply if `bhavcopy_status.json` already shows same date + matching schema version.
- Force-reapply: bump `APPLY_SCHEMA_VERSION` in `providers/free.py`. Each version bump re-runs the apply on every cold start until the status file catches up.

**APPLY_SCHEMA_VERSION history (current = 9):**
| Version | What the apply path now does |
|--------|-------------------------------|
| v3 | yfinance fallback populates `p` (prev close); fall back to existing `previous_close` when `p=0` on re-apply |
| v5 | Recompute window returns (`stock_return_5d/20d/40d/60d/126d/189d/12m/504d`) from stored baselines so RS / groups / scanners stay fresh |
| v6 | Lift stale `high_52w / ath / multi_year_high / high_6m / month_high / week_high / range_high_20d` (and low counterparts) when today's bar pushes past them; snapshot the OLD value into `*_prev` so breakout scanners keep a valid baseline |
| v7 | Recompute RS score / rating + roll `recent_closes` / `chart_grid_points` from the EOD bar so RS / contraction don't depend on a warm chart cache |
| v8 | Verify the snapshot rows themselves before trusting the status file's already-applied marker |
| v9 | Rescale `market_cap_crore` by the price-move ratio so the earnings widget / dashboard show the cap at the current close instead of the seed-snapshot price |

**Snapshot freshness guard (added 2026-05-03):** `_scan_eligible_snapshots` now drops rows whose `history_session_date` is older than the latest applied `bhavcopy_status.date`. This prevents NSE-only stocks (BSE Ltd., CDSL, MARINE — they're not in BSE's bhavcopy CSV) from surfacing in scanners with their stale seed-day `change_pct` / volume. The filter is a no-op when no patch has ever been applied (fresh deploy / fresh seed), so it can't regress safe behavior.

**Trusted-source whitelist:** `RELIABLE_HISTORY_SOURCES` in `providers/free.py` controls which `history_source` values are allowed to keep their `avg_volume_20d/30d/50d` baselines on snapshot load. As of v8 it includes `{"history", "chart_cache", "legacy_chart_cache", "bhavcopy_patch"}`. **Adding a new history-source label without touching this set will silently zero every volume baseline** and collapse RVOL / liquidity-floored scanners to ~0 hits. The set is exhaustively commented in source.

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
| `Uvicorn --workers` | **2** | `Dockerfile` line 20 | cpu-basic has 2 vCPUs. The handlers are `async def` but call sync pandas/sorting work; with 1 worker, ~6 parallel page-load calls queued behind the event loop and Vercel's edge proxy 500'd the slowest. Two workers process in parallel. |
| `EARNINGS_CACHE_TTL_SECONDS` | **6 × 3600** | `services/dashboard_service.py` | Per-symbol disk cache at `backend/data/earnings_cache/<SYMBOL>.json`. Fresh-validated against snapshot date (rejects payloads where the latest quarter is > 18 months stale). |
| `SAME_BASE_RETRY_ATTEMPTS` | **2** (1.5 s backoff) | `frontend/src/lib/api.ts` | Retries on transient TypeError + 5xx without hopping to a different base. |

**Multi-worker safety:** `lifespan` in `backend/app/main.py` takes a `/tmp/scanner_scheduler.lock` file lock; only the worker that wins the lock runs the APScheduler. Without this, both workers would fire the daily refresh + bhavcopy cron jobs.

⚠️ **Memory budget (HF 16 GB):** Do not raise `MARKET_CAP_MIN_CRORE` below ~500 (more stocks = more RAM). Do not enable `STARTUP_CACHE_WARM_ENABLED`. Do not use `max_workers > 1` for thread pools (the Uvicorn `--workers 2` is multi-process; in-process thread pools are still single-threaded). Always call `gc.collect()` after building the dashboard or industry groups.

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
| `GET /api/scans/{scan_id}?market=india` | Full scan results. Optional query params for the **Expansion** scanner: `expansion_min_change_pct` (default 6.5) and `expansion_min_relative_volume` (default 3.0) let users dial the gates without a Custom Scanner round-trip. |
| `POST /api/custom-scan?market=india` | Custom scan with body of filters. |
| `POST /api/near-pivot \| /api/pull-backs \| /api/returns \| /api/consolidating` | Parametrised scans (60 s frontend timeout each). |
| `GET /api/groups?market=india` | 96-group rankings (slow on first build, then cached). |
| `GET /api/chart/{symbol}?timeframe=1D&market=india` | Daily bars + RS line (cached). |
| `GET /api/chart/{symbol}/history?timeframe=1D&market=india` | Full history, live yfinance fetch. |
| `GET /api/bhavcopy/status` | Last applied patch date + source. |
| `GET /api/fundamentals/{symbol}?market=india` | Company fundamentals. |
| `GET /api/earnings/{symbol}?market=india` | Earnings widget payload. **Backed by per-symbol disk cache** (`backend/data/earnings_cache/<SYMBOL>.json`, 6 h TTL). On cache miss, scrapes Screener (consolidated → standalone fallback). Stale-cache fallback on Screener failure. |

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
8. **BSE bhavcopy doesn't cover NSE-only stocks.** BSE Ltd. (the exchange itself, NSE ticker `BSE`), CDSL, MARINE, etc. trade only on NSE — they're not in BSE's EOD CSV at all (verified by ISIN grep). With BSE-source patches, those rows keep stale seed-day values. The staleness guard in `_scan_eligible_snapshots` (§4) hides them from scanners. **Proper long-term fix:** UNION BSE patch + yfinance for missing universe symbols inside `generate_bhavcopy_patch.py`. Not done yet.
9. **`history_source` whitelist trap (§4).** Any new code that writes a fresh `history_source` value MUST also be added to `RELIABLE_HISTORY_SOURCES`. Otherwise `_normalize_snapshot_volume_baselines` zeros every `avg_volume_*` field and collapses RVOL / liquidity-floored scanners to ~0 hits. Wide-scope failure mode — be paranoid here.
10. **Charts wickless after a regression?** Check `_fetch_chart_bars` for any new fallback that synthesises bars from `chart_grid_points` (those are close-only, so OHLC collapses to flat candles). Bump `CHART_CACHE_VERSION` to invalidate any wickless caches that got persisted before the fix.
11. **Earnings widget showing 2016 quarters?** The screener.in parser's table picker now boosts tables containing a recent year (`_earnings_recent_year_floor`) and the row-level filter drops columns where every headline field is null/zero. Both must stay in sync — see `_select_table` and `_quarterly_row_has_data` in `providers/free.py`.
12. **Watchlists: a symbol can live in multiple lists.** `handleAddToWatchlist` no longer removes from other lists when adding to one. Don't reintroduce the move-on-add behaviour.

---

## 12. Recent Commits (latest first)

### 2026-05-03 — scanner correctness + earnings + watchlists pass
| Commit | Summary |
|--------|---------|
| `db8fbc0` | fix(scanners): exclude stocks whose snapshot session date predates the latest bhavcopy. Hides stale BSE/CDSL/MARINE etc. (NSE-only, not in BSE CSV) from every scanner / top-gainers / sector view |
| `18e9a84` | feat(watchlists): bulk **Import** button + multi-list membership. `parseImportedSymbols` accepts `NSE:RELIANCE` / `BSE:RELIANCE` / `RELIANCE-EQ` / bare `RELIANCE` mixed with commas/spaces/newlines. `handleAddToWatchlist` no longer removes from other lists |
| `c6b480b` | fix(snapshots): rescale `market_cap_crore` by price-move ratio on bhavcopy apply (`APPLY_SCHEMA_VERSION 8 → 9`) |
| `397459e` | fix: surface expansion threshold panel (was unreachable behind a duplicate `activeScanner === "ema-expansion"` branch) + drop empty quarterly columns from screener parser |
| `61bbbe8` | feat: tunable expansion thresholds (query params `expansion_min_change_pct` / `expansion_min_relative_volume`) + per-symbol earnings disk cache + YoY/QoQ toggle in widget |
| `48f4e02` | fix(snapshots): trust `bhavcopy_patch` history_source so volume baselines stop zeroing |
| `6be4c0f` | fix: stop wickless candles (remove `_chart_bars_from_snapshot_points` fallback, bump `CHART_CACHE_VERSION`) + restore deterministic improving-rs scanner |
| `296d744` / `fad4c84` | feat: 52-week-RS screener + earnings widget (introduced regressions fixed by the commits above) |

### 2026-05-02 — performance + scanner extreme fields
| Commit | Summary |
|--------|---------|
| `dd5a4d7` | fix(scanners): snapshot old extreme into `*_prev` when lifting a new high/low |
| `a77e2e1` | fix(scanners): lift stale `52W / ATH / month / week / range` extreme fields when today's bar pushes past them |
| `54e2c85` | fix: scheduler-coordination lock + 60 s scan timeouts |
| `9ecdd01` | fix(backend): Uvicorn `--workers 2` so parallel page-loads stop 500-ing |
| `c5f0778` | fix(frontend): retry transient 500s instead of failing the scanner panel |
| `b1c2116` | fix(frontend): drop dead Render / localhost fallback URLs from production base list |

### 2026-05-01 and earlier
| Commit | Summary |
|--------|---------|
| `5819002` | fix(bootstrap): seed universe catalog from dashboard so fresh tabs render data immediately |
| `ebc6bd4` | fix(bhavcopy): recompute window returns on apply so groups/RS stay fresh |
| `1e418a5` | fix(perf): keep HF Space warm + widen group eligibility (GROUP_MIN_MARKET_CAP_CR 1000→250) |
| `6bcbd93` | fix(bhavcopy): use bhavcopy PREVCLOSE (p) for change_pct |
| `3ac8961` | fix(bhavcopy): apply committed patch on startup so prices stay fresh |
| `194e17c` | feat(chart): make group widget floating, draggable, resizable, toggleable |
| `873e0e8` | feat(chart): persistent group widget + side-by-side compare mode |

---

## 13. Earnings widget architecture

Inspired by the user's "scrape on a slow cadence, expose own API, frontend never scrapes external sites" framework, scoped to HF's ephemeral-disk constraints.

```
                       (cold-fetch, 25 s timeout, dual-URL retry)
       Screener.in  ─────────────────────────────────────────────┐
                                                                 │
                        ┌─────────────────────────┐              │
   /api/earnings/{sym} ─┤  get_earnings_summary   │ ◄────────────┘
                        │                         │
                        │  1. read_earnings_cache │  ← per-symbol JSON
                        │     (6 h TTL)           │     in backend/data/earnings_cache/
                        │  2. live snapshot       │
                        │     metrics merge       │
                        │  3. on miss: scrape +   │
                        │     write_earnings_     │
                        │     cache               │
                        │  4. on scrape failure:  │
                        │     stale-cache fallback│
                        └─────────────────────────┘
```

- Cache is per-symbol so different stocks don't invalidate each other.
- Freshness validator (`_earnings_payload_is_fresh`) rejects payloads where the latest quarter is > 18 months stale → JAYNECOIND-style 2016 results can't survive.
- Snapshot metrics (RVOL, turnover, ADR, % from 52W) are computed per-request from the live snapshot, NOT from cache, so they always track today's bar.
- Adding Tijori as a primary source later is a one-function swap behind an env flag — `_fetch_screener_company_page` is the only place that knows about Screener URLs.

---

## 14. Known data / coverage gaps

| Gap | Impact | Mitigation in place | Long-term fix |
|---|---|---|---|
| BSE bhavcopy doesn't include NSE-only stocks (BSE Ltd., CDSL, MARINE, etc.) | Their snapshots stay at seed-day values | Staleness filter in `_scan_eligible_snapshots` hides them from scanners | UNION BSE + yfinance for missing universe symbols inside `generate_bhavcopy_patch.py` |
| Some BSE-only small-caps lack a Screener page (BELDING, CIANAGRO, KRISHANA, JAYNECOIND in some quarters) | Earnings widget shows "No quarterly earnings data available" | Stale-cache fallback if available; otherwise empty payload (honest) | Add Tijori paid-API fallback behind env flag |
| HF Spaces ephemeral disk wipes chart cache + earnings cache on every restart | First chart load per symbol = 5–8 s | yfinance fetch is fast enough; cache rebuilds quickly | None needed — HF cpu-basic doesn't offer persistent disk |
| BSE bhavcopy isn't always available before 4:20 PM IST | Workflow has 5 retry slots through 6:30 PM IST | yfinance fallback in the same workflow | None needed |

---

## 15. First-Session Checklist

1. Read this file end-to-end.
2. `git status` — should be clean. `git remote -v` — should show `origin`, `hf`, `hf-push`.
3. Smoke test: `curl -s "https://dharmmalik-stock-scanner-backend.hf.space/api/health"` → `{"ok":true}`.
4. Check today's bhavcopy: `curl -s "https://dharmmalik-stock-scanner-backend.hf.space/api/bhavcopy/status"` — date should be today or yesterday.
5. Verify staleness guard is doing its job:
   ```bash
   curl -s "https://dharmmalik-stock-scanner-backend.hf.space/api/dashboard?market=india" \
     | python3 -c "import json,sys; d=json.load(sys.stdin); [print(g['symbol'], g['change_pct']) for g in d['top_gainers'][:5]]"
   ```
   Top gainers should be stocks confirmed in the latest BSE bhavcopy. If you see "BSE", "CDSL", "MARINE" with weird change%, the filter regressed.
6. Ask the user what they want done.
