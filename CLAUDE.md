# Stock Scanner — Developer Guide & System Reference (CLAUDE.md)

> **Automatic Context Load:** Claude Code automatically reads this `CLAUDE.md` file on every startup when launched in the root directory.

---

## 1. Project Overview & Tech Stack

Indian stocks scanner SaaS web app for NSE/BSE stocks with technical scanners (Minervini, VCP, Expansion, RS, Gap-Up), sector/industry group analysis, money flow tracking, and Gemini AI analysis.

| Layer | Stack | Hosting & Deployment |
|---|---|---|
| **Frontend** | React 19 + Vite 7 + TypeScript | **Vercel** (`https://my-screener-theta.vercel.app/`) — Auto-deploys on push to `main` |
| **Backend** | FastAPI + Pandas + PyJWT | **Hugging Face Spaces** Docker (`cpu-basic`, 16 GB RAM) — Deploys via GitHub Actions (`deploy.yml`) |
| **Data Engine** | Yahoo Finance, BSE, NSE, EOD Bhavcopy | Daily GitHub Actions workflow (`daily-bhavcopy.yml`) pushes `backend/data/bhavcopy_patch.json` at ~4:20 PM IST |
| **AI Layer** | Google Gemini API | Money flow reports, natural-language stock search & AI screener |

---

## 2. Codebase Map & Key Files

### Backend (`backend/app/`)
- `main.py`: Entrypoint, startup bhavcopy patch application (`APPLY_SCHEMA_VERSION = 10`), CORS, worker scheduler lock (`/tmp/scanner_scheduler.lock`).
- `api/routes.py`: Main API router — health, scanners, dashboard, markets, sector groups, news, watchlists, trade journal, AI endpoints.
- `providers/free.py`: Main data provider (~370KB). Handles stock history, price cache, chart grid, and volume whitelist (`RELIABLE_HISTORY_SOURCES`).
- `scanners/definitions.py`: Core technical scanner implementations (Minervini Trend Template, VCP Contraction, Momentum Burst, Expansion, Gap Up, Demand Zone, RS Rating).
- `services/dashboard_service.py`: Market breadth calculation, industry group rankings, sector summary, and scanner execution engine.
- `services/ai_analysis_service.py`: Integration with Gemini API for automated market intelligence and money-flow analysis.
- `services/earnings_metrics.py`: Quarterly EPS/Sales growth calculations for earnings widget.
- `services/industry_groups.py` & `industry_classifier.py`: Classification and group strength scoring for Indian stocks.
- `services/news_service.py` & `rss_news_service.py`: News fetching, deduplication, and RSS scraping.
- `services/watchdog_agent.py`: System health watchdog and self-healing task runner.
- `services/watchlists_store.py` & `journal_store.py`: Persistence handlers for user watchlists and trade journal entries.
- `services/mutual_funds/`: Mutual fund screener subsystem (India-only, mounted at `/api/mf` via `api/mutual_funds_routes.py`).
  - `nav_source.py`: **Authoritative** leg — daily NAV history per AMFI scheme code, via the mfapi.in mirror. Every return/rank/risk number derives from this.
  - `groww_source.py`: Best-effort reference data (holdings, benchmark name, TER, AUM). Reads the **public HTML** pages and parses `__NEXT_DATA__` — Groww's `robots.txt` disallows `/v1/api/*`, so the JSON API is never touched. Degrades to "no holdings", never takes the page down.
  - `metrics.py`: Pure-Python returns, CAGR, rolling returns, drawdown, Sharpe/Sortino, beta/alpha/capture, category ranks, XIRR. No pandas (16 GB budget).
  - `benchmarks.py`: SEBI sub-category → index. Prefers an index **fund's NAV** (total-return, same trading calendar) over a Yahoo price index. Sectoral/thematic funds resolve to their own Nifty sector index (16 of them) by fund name, falling back to the dominant holdings sector; strategy themes (momentum, quant, ESG) keep Nifty 500 rather than guess.
  - `fund_review.py`: Measured standing of a fund against its own sub-category — percentile scorecard, rank trajectory, peers better on return+cost+downside together. Deterministic arithmetic; recommends nothing (see gotcha 12).
  - `holdings_enrich.py`: Matches disclosed holdings to `free_universe.json` for symbol links + SEBI large/mid/small classification; buckets overseas/derivative/debt/cash lines.
  - `service.py`, `portfolio.py`, `index_source.py`, `paths.py`, `harvest.py`.

### Frontend (`frontend/src/`)
- `App.tsx`: Core UI container, top navigation bar, main state management, and tab switcher.
- `components/HomePanel.tsx`: Primary dashboard showing market indices, top movers, market health, and quick scanners.
- `components/ScanTable.tsx`: Reusable data table for displaying stock scan results with sorting, filtering, and chart popups.
- `components/ChartPanel.tsx` & `ChartGridModal.tsx`: Lightweight Charts integration, candlestick rendering, technical overlay markers, and chart grid multi-view.
- `components/ScreenerSidebar.tsx` & `CustomScannerPanel.tsx`: Filter controls for custom technical & fundamental parameter scans.
- `components/MarketsPanel.tsx` & `GroupsPanel.tsx`: Market breadth metrics, sector heatmap, and industry group leadership tables.
- `components/TradeJournalPanel.tsx`: Comprehensive trade logging, analytics, and journal management.
- `components/LivePanel.tsx`: Streaming intraday watch — quotes flow browser-side, the backend is not involved.
- AI surfaces are journal-scoped only (`/api/ai/swing-analysis`, `/ai/journal-review`, `/ai/learnings-review`,
  reached from `TradeJournalPanel`). The standalone Gemini screener and chat window were removed in `0d84f790`;
  their `/api/ai/scan` and knowledge-base endpoints no longer exist.
- `components/MutualFundsPanel.tsx`: Funds page — screener table (sortable, with category rank as a first-class column), category leaderboard, and manual portfolio with XIRR + stock-level look-through.
- `components/FundDetailModal.tsx` & `FundNavChart.tsx`: Per-fund deep dive — growth-of-100 NAV chart vs benchmark, rolling returns, drawdown episodes, holdings with links into the equity chart.
- `lib/api.ts`: Centralized API client wrapper with request error handling and base URL configuration.

---

## 3. Data Pipeline & Bhavcopy Engine

1. **Daily EOD Bhavcopy Workflow:** `.github/workflows/daily-bhavcopy.yml` runs Mon–Fri at ~4:20 PM IST (retries through 6:30 PM IST).
2. **Patch Application:** On HF Space startup, `apply_bhavcopy_patch_on_startup()` in `app/main.py` reads `backend/data/bhavcopy_patch.json` and patches `free_snapshots.json`.
3. **Current Schema Version:** `APPLY_SCHEMA_VERSION = 10` (includes NSE volume overlay from yfinance for combined BSE+NSE accuracy).
4. **Staleness Guard:** `_scan_eligible_snapshots` filters out stocks with obsolete `history_session_date` relative to the patch date.

---

### Mutual Fund Universe
1. **Refresh workflow:** `.github/workflows/mutual-funds-refresh.yml` runs ~1:07 AM IST on weekdays. It refreshes NAV history and recomputes every rank nightly, and re-crawls all ~1,600 fund pages weekly (Saturdays) to pick up monthly holdings disclosures.
2. **Tracked artifact:** only `backend/data/mf_universe.json` (~2.5 MB, ~1,070 Direct/Growth equity + hybrid schemes) is committed — it must ship so a cold Space serves the page immediately.
3. **Runtime caches:** `mf_nav/`, `mf_details/`, `mf_reference.json` are gitignored and rebuilt on demand. The user's portfolio lives in `APP_STATE_DIR`, like the trade journal.
4. **Commit guard:** the workflow refuses to commit a universe with < 800 funds or < 400 three-year records, so a blocked crawl cannot take the page down.

## 4. Key Commands Cheatsheet

### Local Development
```bash
# Run Backend (http://localhost:8000)
cd backend && python run_local.py

# Run Frontend (http://localhost:5173)
cd frontend && npm run dev
```

### Verification & Testing
```bash
# Run Backend Unit Tests
cd backend && pytest

# Rebuild the mutual fund universe (full crawl ~5 min; --compute-only re-derives
# metrics from cached data in seconds)
cd backend && python scripts/build_mf_universe.py
cd backend && python scripts/build_mf_universe.py --compute-only --refresh-navs

# Frontend Type Check
cd frontend && npx --no-install tsc --noEmit

# API Health & Status Verification
curl -s https://dharmmalik-stock-scanner-backend.hf.space/api/health
curl -s https://dharmmalik-stock-scanner-backend.hf.space/api/bhavcopy/status
```

### Deployment Procedures
- **Frontend (Vercel):** Automatically deployed on push to `main` branch on GitHub.
  - *Manual Vercel CLI (fallback):* `cd frontend && npm run build && npx vercel deploy --prod --yes`
- **Backend (Hugging Face Spaces):** Automatically deployed on push to `main` (if files in `backend/**` changed) via `.github/workflows/deploy.yml`.
  - *Manual Git Push (if CI breaks):* `git push hf-push HEAD:main --force`

---

## 5. Coding Guidelines & Critical Gotchas

1. **Response Style:** Fix-first, minimal narration. Wrap up responses with: `DONE / Changed files / Run commands / Status`.
2. **Surgical Modifications:** Reproduce issues first. Make targeted code modifications without refactoring surrounding modules unnecessarily.
3. **Volume Whitelist Gotcha:** Any new `history_source` label MUST be registered in `RELIABLE_HISTORY_SOURCES` set in `backend/app/providers/free.py`. Omitting a label will zero all 20d/50d average volume baselines across all stocks!
4. **Memory Management (16GB Limit):** Keep `MARKET_CAP_MIN_CRORE >= 500` to prevent memory exhaustion on HF Spaces. Do **NOT** set `STARTUP_CACHE_WARM_ENABLED=True`.
5. **Dual Worker Scheduler Lock:** Dual workers run in production Docker. Only the worker acquiring `/tmp/scanner_scheduler.lock` runs background cron tasks.
6. **Git & Credential Security:** 
   - **NEVER** run `git add .` or `git add -A`. Add files explicitly (`git add <file>`).
   - Active Git credentials live in `.git/config` and macOS Keychain. **NEVER commit `.git/config` or plain tokens.**
7. **Frontend Resilience:** All React components must handle null, empty, or missing API responses gracefully using optional chaining and loading/error states.
8. **Mutual Fund Route Handlers Must Stay Sync:** every handler in `api/mutual_funds_routes.py` is a plain `def`, **not** `async def`. `MutualFundService` is fully synchronous and does blocking I/O (NAV files, holdings fetches, yfinance index history). Declaring them `async` puts that work on the event loop, where one slow benchmark fetch stalls every request in the process — measured at 43 s for six concurrent opens, versus 160 ms once they were sync.
9. **Fund Performance Numbers Never Come From the Scrape:** `groww_source.py` fields are reference data only (holdings, TER, AUM, benchmark name). Returns, ranks, drawdown and alpha are always computed in `metrics.py` from AMFI NAV, so the page stays correct if the third-party source drifts. The `source_*` columns exist purely as cross-checks.
11. **Never Filter the Fund Universe by Slug Shape:** `groww_source.list_scheme_slugs()` is deliberately permissive and the Direct/Growth test is `is_direct_growth()` on the *payload's* `plan_type`/`scheme_type`. An earlier version kept only `*-direct-growth` slugs and silently dropped 49 real funds whose slug reads `-direct-plan-growth` — Quant Mutual Fund's entire range, missing with nothing in the logs.
12. **The Fund Review Reports, It Does Not Advise:** `fund_review.py` and the `generate_fund_review_note` prompt are both constrained to describing measured evidence. No switch recommendations, no SIP sizing, no lump-sum timing — that is personalised investment advice and this app is not a licensed adviser. `test_signals_never_instruct_the_reader` enforces it on the deterministic side; the prompt forbids it on the prose side. Keep both if you touch this.
13. **Benchmark Mapping Is Re-Resolved Every Build:** `phase_compute` re-runs `benchmarks.resolve()` from the cached reference rows rather than trusting the `benchmark_key` frozen in at crawl time, so a mapping fix needs `--compute-only` (seconds) and not a 1,650-page re-crawl.
10. **Alpha Against a Price Index Is Flattered:** most equity categories benchmark to a Yahoo price index (no dividends), which overstates alpha by roughly 1.2%/yr. Rows carry `alpha_vs_price_index: true` and the UI flags it with a dagger — keep that flag if you touch the benchmark plumbing. Small and mid caps route through index-fund NAV instead precisely to avoid this (and because Yahoo's `^CNXSC` has no usable history).
