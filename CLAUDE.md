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
- `services/study_coach.py`: Chart Gym coach — turns the drill log into measured facts (avg R sliced by wait, stop width, RS, base depth, volume dry-up, industry group; selection edge against the deck's 50/50 base rate; earlier-vs-recent trajectory). Every slice carries its sample size and anything under `MIN_SAMPLE` is dropped rather than reported.
- `services/study_deck.py`: Chart Gym deck server — deals a balanced daily hand of historical setups and splits each symbol's bars at the trigger session so the answer never ships with the question. The deck itself is mined offline by `scripts/generate_study_deck.py`.
- `services/bot/`: Regime-aware trading bot — the answer to "which strategy works in which market condition", measured rather than asserted. Mounted at `/api/bot` via `api/bot_routes.py`.
  - `history.py`: Deep daily-bar store (~1,575 symbols back to 2007-1996, 5.2M bars, gzipped columnar JSON). **Gitignored** — it is raw material, rebuilt by `scripts/build_deep_history.py` in ~10 min.
  - `indicators.py`: Causal indicators. Element `i` uses only `0..i`; warm-up is nan, never zero. `test_bot_engine.py` recomputes each on truncated input to pin the contract.
  - `regime.py`: Six regimes (`bull_strong`, `bull_narrow`, `choppy`, `correction`, `bear`, `recovery`) from index trend, breadth and volatility. Thresholds declared before measurement, not fitted.
  - `breadth.py`: Market breadth counted across the universe's own bars, carrying `constituents` so thin early years are visible.
  - `strategies.py`: Ten setups, each declaring `expects` (the regimes it is *predicted* to work in) before anything is measured.
  - `engine.py` / `costs.py`: Trade simulation. Fills at the next open, gaps through stops fill at the open, same-bar stop+target resolves as a loss, and full Indian delivery costs (~0.53% round trip) are charged on every trade.
  - `attribution.py`: Strategy x regime cells with bootstrap intervals, Benjamini-Hochberg across all 60 cells, and a chronological walk-forward split.
  - `policy.py`: Playbook per regime, built only from cells that survived the held-out period. "Stand down" is a first-class output.
  - `macro.py` / `context_series.py`: 10 external series, each with its headwind direction declared in advance, split within a fixed regime so the test can come back "no".
  - `survivorship.py`: Measures the bias rather than hiding it — era coverage against era performance.
  - `live.py`: Today's regime, playbook, macro gate and ranked candidates with position sizing.
  - `ledger.py`: The trade database (SQLite, in `APP_STATE_DIR` beside the journal). Every trade with the state of the world **at entry** — regime, breadth, volatility band, macro count — so decision quality can be separated from outcome later. `backtest` / `paper` / `live` share one schema; re-seeding is idempotent on (source, strategy, symbol, entry_day).
  - `review.py`: Deterministic post-trade judgement from MAE/MFE/exit. Seven verdicts (`clean_win`, `lucky_win`, `round_trip_loss`, …). **Entry tags vs outcome tags are strictly separated** — only entry tags may become lessons.
  - `conditions.py`: Entry conditions measured across their full range in buckets, not at one cutoff. A finding must be monotone **and** survive the held-out period.
  - `evolution.py`: Cell lifecycle (`confirmed` / `watch` / `retired` / `candidate` / `rejected`) and `replay_evolution`, which walks history quarterly scoring each cell on trades closed by that date only.
  - `quality.py`: Candidate ranking = cell expectancy + the one validated entry-time adjustment, with every part reported. Degrades to expectancy alone if the study is not certified.
  - `portfolio.py`: The bot as an **account** — finite capital, 8 slots, risk-based sizing, an equity curve. Trade-level R is a statement about a population; this is what an account that had to choose between signals actually ended up with.
  - `selection.py`: Chooses which entry conditions may influence ranking. Selection **and** sizing happen on pre-split data, with the pre-split window split again for inner validation — because in-sample significance alone admitted two conditions that took the held-out account from +10.95% to -1.94%.
  - `calibration.py`: **The live feedback loop.** Closed live/paper trades audit the study rather than retrain it — a cell short of its expectation on enough trades is flagged and halved, then stood down; a cell *beating* expectation is never promoted. Recorded via `POST /api/bot/ledger/trade`.
  - **`timing.py`: the one result here that beats a professional**, and the only component the learning loop is usefully pointed at (`assess_health`). Not by picking stocks — by deciding *when to be exposed*. Hold the Nifty 500 in `bull_strong`/`bull_narrow`/`recovery`, park cash otherwise: **+12.62% CAGR at -13.6% drawdown held-out, against buy-and-hold's +10.46% at -18.8% and the median fund's +11.36% at -27.5%** — better on return *and* drawdown, with 20% tax and 5 bps per switch charged.
  - `earnings.py`: **The one non-price signal source.** Historical announcement dates with EPS surprise, fetched by `scripts/build_earnings_history.py` (gitignored; only **484 of 1,581** symbols have analyst estimates). Entry is strictly the session *after* an announcement — Indian results are commonly released after the close, and same-day entry would capture the very gap the drift is meant to predict.
  - **`scripts/rolling_walkforward.py`: the evaluation that matters.** Rebuilds the playbook each January from prior data only, then trades that year — eleven independent out-of-sample years instead of one window. Result: **-2.60%/yr against the index's +10.35%, beating it in 2 of 11 years.** Served at `/api/bot/walkforward` and shown at the top of the Scorecard.
  - `sensitivity.py`: Runs the account across **30 defensible book structures** and reports the distribution, because choosing one on pre-split data correlates **-0.70** with held-out return — selection points the wrong way, so no single configuration's CAGR is quoted as "the" result.
  - `benchmark.py`: Ranks that account inside the real distribution of ~700-1,100 Indian equity funds (AMFI NAV) and against Nifty buy-and-hold, dimension by dimension, with the caveats attached to the payload rather than to a footnote.
  - `learning.py`: Bridge — turns the trade population into the committed learning payload (aggregates only; the SQLite ledger cannot ship, see gotcha 21).
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
- `components/StudyCoach.tsx`: the Coach panel — measured numbers and slice tables on top, the written review below, shown separately so it is always clear which is which. The numbers still render when Gemini is unavailable.
- `components/StudyPanel.tsx` & `StudyChart.tsx`: Chart Gym — the chart-reading drill. Shows a historical VCP / flag setup truncated at its trigger bar, lets you step forward up to 15 sessions before choosing your own entry day, then holds for up to 15 more and grades the trade against your own entry and stop. Carries its own trendline / measure / region-snip tools (click an active tool or press Esc to drop it), a draggable stop line, a candles / bars / HLC toggle, PNG export, and a symbol search that opens any stock as a free study. Scores persist in `localStorage`.
- `components/BotFeedbackLoop.tsx`: the Bot tab's **Live loop** view — whether the live book still behaves as modelled, which setups are flagged or stood down by their own record, and which are still waiting on evidence.
- `components/BotScorecard.tsx`: the Bot tab's **Scorecard** view — the account's equity curve and its dimension-by-dimension comparison against real fund managers, plus the four-way run table showing what each design choice was worth.
- `components/BotLearning.tsx`: the Bot tab's **Learning** and **Evolution** views — trade-shape breakdown, entry-condition bucket studies (with rejected ones shown as rejected), the quarterly evolution timeline, and every point the bot changed its mind about a strategy.
- `components/BotPanel.tsx`: the Bot tab — four views in the order a decision gets made: **Today** (regime, stance, macro gate, sized candidates), **Playbook** (regime -> cleared setups), **Evidence** (the strategy x regime matrix, what survived validation, what decayed), and **What this can't tell you** (survivorship, macro, caveats). The last view is not an appendix and gets equal billing on purpose.
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
3b. **Sector index history is also committed:** `backend/data/sector_indices.json` (~1.9 MB, 16 Nifty sector indices, weekly full history + 5 years daily with a precomputed 30-week average). Yahoo serves these symbols to residential IPs but refuses most of them from datacenter ranges — the Space gets `^CNXIT`, `^NSEBANK`, `^CNXPHARMA` and is turned away from the other thirteen, so a request-time fetch renders 3 of 16 sectors in production and 16 in development. Runtime prefers a live fetch and falls back to this file. Rebuild with `python3 scripts/build_sector_indices.py`; it merges rather than overwrites and refuses to shrink.
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

# Rebuild the Chart Gym deck (~1.8 hrs for 52 weeks over the full universe —
# workstation only, never on the Space)
cd backend && python3 scripts/generate_study_deck.py --weeks 52
cd backend && python3 scripts/generate_study_deck.py --weeks 3 --limit-symbols 300   # quick check

# Trading bot: build history (~10 min), backtest (~1.5 min), then today's signals
cd backend && python3 scripts/build_deep_history.py
cd backend && python3 scripts/run_bot_backtest.py
cd backend && python3 scripts/generate_bot_signals.py
cd backend && python3 scripts/sweep_exit_models.py --limit-symbols 350   # exit-rule protocol
# Exit horizon judged by what the ACCOUNT earns, not per-trade R
cd backend && python3 scripts/sweep_account_exits.py
# Earnings announcement history (~4 min; 484 of 1,581 symbols have estimates)
cd backend && python3 scripts/build_earnings_history.py
# The evaluation that matters: playbook rebuilt yearly, traded the next year
cd backend && python3 scripts/rolling_walkforward.py
cd backend && python3 scripts/rolling_walkforward.py --only-family fundamental
# Seed the SQLite trade ledger (APP_STATE_DIR; ~68 MB, never committed)
cd backend && python3 scripts/build_bot_ledger.py

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
14. **Sector Data Must Never Be Fetched Live-Only:** `service._sector_index_series` tries Yahoo first and falls back to the committed `sector_indices.json`. Removing that fallback does not fail any test and looks fine locally — it silently drops 13 of 16 sectors on the Space, because Yahoo blocks those symbols from datacenter IPs. `SectorArtifactTests` asserts every `SECTOR_BENCHMARKS` entry ships with history; keep it.
13. **Benchmark Mapping Is Re-Resolved Every Build:** `phase_compute` re-runs `benchmarks.resolve()` from the cached reference rows rather than trusting the `benchmark_key` frozen in at crawl time, so a mapping fix needs `--compute-only` (seconds) and not a 1,650-page re-crawl.
15. **Every Stock Must Land in a Group — Keep the Sector Fallback:** `industry_classifier` runs override → keyword → peer alias → **sector fallback**, and the last layer is what makes the output total: `sector_fallback_groups.csv` names a destination group for every macro sector, so a vendor industry label nobody has seen still lands with its own sector's peers. Delete it and 19+ live stocks reappear in the "Unclassified (Parent bucket)" row on the Groups page — they were never unclassifiable, they just had labels (`Metal Fabrication`, `Packaging`, `Drug Manufacturers - Specialty & Generic`) no layer covered. `test_group_classification_coverage.py` asserts the shipped `free_universe.json` classifies completely and that any industry label carried by 3+ stocks resolves on its own rather than by luck of a company-name keyword. Also: the keyword layer matches substrings of the company name, so it mis-fires (KRN Heat *Exchanger* → exchanges, ION *Exchange* → exchanges, Gujarat Ambuja Ex*port*s → ports); those are fixed with rows in `manual_group_overrides.csv`, which runs first.
16. **`needs_review.csv` Is Tracked, and `write_needs_review` Refuses to Truncate It:** a run that classified nothing leaves the file alone. Without that guard `pytest` — whose fixtures classify two or three stocks — left the curation queue truncated to its header, ready to be committed.

17. **A New Full-Screen Overlay Must Portal to `<body>`:** `main.workspace` carries a transform, which makes it the containing block for `position: fixed`. A backdrop rendered inside the tree with `inset: 0` therefore sizes itself to the whole scrollable page — measured at 1445px tall in a 1200px window — and the dialog hangs off the bottom of the screen. `GroupStocksModal` uses `createPortal(..., document.body)` for exactly this reason.
18. **The Groups Page Chart Column Is Per-Tab:** `GroupsPanel` reports its tab up through `onViewChange`, and `App` renders the right-hand `ChartPanel` for Rankings and Market Map but not for Rotation, which runs full width (`workspace-grid-solo`). The rotation graph's own chart surface is the group dialog, which renders the same panel from the shared `pageChartPanelProps` object — keep that object as the single source, or the two copies drift.
19. **`handlePickSymbolWithContext` Opens the Floating Chart Modal:** it sets `chartOpen`, which renders a z-index 9999 overlay. Anything with its own chart pane wants `selectSymbolInContext` instead — the group dialog used the former first and buried itself under the popup.

20. **A Due Close Refresh Must Never Block a Request:** every screener endpoint begins with `provider.get_snapshots(...)`. That call used to rebuild the whole universe inline whenever `_market_close_refresh_due()` was true, and `_get_or_create_snapshot_request_task` then started a *second*, unregistered rebuild for each request that had piggy-backed on the first. The refresh being due is a condition a rebuild cannot clear on its own — the session's bhavcopy has to arrive first — so with the bhavcopy pipeline stuck, every request launched its own crawl of ~1,900 symbols and none finished: all screeners returned 500 for hours while `/api/groups` (a committed artifact) stayed fast. It is stale-while-revalidate now: serve the cached close, schedule one background rebuild, and back off `CLOSE_REFRESH_RETRY_SECONDS` between attempts. Only a completely empty cache blocks. `test_snapshot_refresh_stampede.py` holds it shut.
21. **The Space Only Gets EOD Data From `daily-bhavcopy.yml`, and Its Push Rejects Binaries:** `deploy.yml` is skipped for the bhavcopy commits (they carry `[skip ci]`, and a `GITHUB_TOKEN` push cannot trigger a workflow anyway), so the bhavcopy job pushes to the Space itself. When that push fails the repo still has the day's patch and the Space silently does not — check `/api/bhavcopy/status` against the newest `data:` commit. This happened for three days from 2026-09-07: the Space's pre-receive hook rejects binary files outright ("Please use Xet to store binary files"), `frontend/public/liquid-glass-bg.jpg` was in the payload, and only `daily-bhavcopy.yml` was missing the exclusion that `deploy.yml` had. **Both workflows now rsync with `--exclude-from '.github/hf-deploy-excludes.txt'`** — add any new binary asset there, or the daily data stops shipping. The manual recovery is the `workflow_dispatch` on **Deploy to HuggingFace**.

21. **Chart Gym Must Never Re-Fit the Viewport Mid-Replay:** the auto-fit runs once per chart (keyed on the first bar + bar count + style) and never again. An effect that re-framed on every revealed bar silently undid the user's zoom every time they stepped — the chart looked fine, it just refused to stay where it was put.
22. **Drag State Lives in a Ref, Not in React State:** the stop drag and the region snip read `snipRef` / `draggingStopRef` inside window-level listeners bound once. Mirroring the rect from state at render time dropped every mousemove that arrived before React re-rendered, which made fast drags do nothing at all.
23. **Saved Studies Are an Opaque Blob:** `study_library.json` lives in `APP_STATE_DIR` beside the trade journal and is owned by the frontend — a study is a symbol, a date window, some drawings and a note, and the backend deliberately does not model any of it.
18. **Chart Gym Serves Forward Bars in Slices, Never in One Lump:** `/api/study/forward` hands out `FORWARD_CHUNK` sessions at a time as the user steps. The user picks their own entry partway along that window, so shipping all 30 bars up front would put the outcome in the browser before any of it had been earned. `/api/study/reveal` carries no bars at all for the same reason.
19. **A Rebuilt Chart Series Needs Its Data Effect Re-Run:** switching candles/bars/HLC tears the lightweight-charts series down and builds a new one — there is no in-place conversion. `style` is therefore a dependency of `StudyChart`'s data effect; dropping it leaves the new series holding no data and the chart renders blank with no error.
20. **Stepping Must Not Advance the Card:** ArrowRight steps sessions only. It deliberately does not move to the next card when the trade finishes — holding the key down through the last session used to blow straight past the result screen, and the grade was lost with it. The grade is also written the moment the outcome is known rather than when `/study/reveal` returns, so a fast Next cannot drop a card from the record.
24. **The Coach Computes in Python and Writes with Gemini — Never the Other Way Round:** every number in `study_review`'s prompt comes from `study_coach.build()`, and the prompt tells the model not to derive new statistics. A fabricated figure here is indistinguishable from a real one to the reader, which is the same reason `fund_review` and `market_regime` are built this way. `test_study_coach.py` pins the arithmetic.
25. **Every Coach Slice Ships Its Sample Size, and Thin Ones Are Dropped:** `MIN_SAMPLE` gates each bucket. Twenty graded cards will otherwise produce a confident "you are -0.9R on wide stops" built on three trades, and the model will faithfully repeat it.
26. **The Drill Log Lives on the Server:** `study_log.json` in `APP_STATE_DIR`, keyed by card id so re-grading a card corrects the record instead of double-counting it. The browser copy is a cache for first paint only — and the push is gated on the server's copy having been merged in first, or a fresh browser would overwrite the whole history with its one new card.
15. **The Chart Gym Deck Must Stay Balanced, and Its Answer Must Stay Server-Side:** the deck deals equal numbers of winners and losers (`StudyDeck.deal`) because a deck of winners trains the eye to see a breakout in every base — the opposite of the skill. And the forward bars live behind `/api/study/reveal`, never in `/api/study/deck` or `/api/study/bars`; shipping them with the question puts the answer in the browser before the user has called it. `test_study_deck.py` pins both.
16. **Chart Gym Bars Need the Provider Fallback:** `_study_bars` in `routes.py` reads `chart_cache` first and falls back to `service.get_chart`. `chart_cache/` is gitignored, so a freshly deployed Space has nothing in it and every card would render as an empty chart — fine locally, silently blank in production, exactly like gotcha 14. Keep the fallback.
17. **`power-base` Is Excluded From the Deck On Purpose:** it fires ~1,500 times a week (20,323 signals in a 13-week replay, versus 167 for `vcp` and 865 for `high-tight-flag`). It describes a state, not an entry, and adding it to `DEFAULT_SETUPS` would swamp the deck with marginal examples and dull the eye rather than sharpen it. Gate it by score first if you ever want it in.
27. **The Bot's Exit Rule Was Selected By Protocol — Don't Quietly Retune It:** `ExitModel`'s defaults (no target, trail 1.5R at 4xATR, 90-session ceiling) are what `scripts/sweep_exit_models.py` chose from five rules declared in advance, scored on the in-sample period alone, with the held-out period reported unchanged. It returned +0.28R in-sample and +0.25R held-out, and the full ranking of all five rules was *identical* in both periods. The rule it replaced (2.5R target, 15-session stop) scored -0.005R — only 11% of trades ever reached 2.5R while 46% were closed alive by the time stop. Two traps here: (a) `run_bot_backtest.py` must not duplicate the numbers in its CLI defaults — it did once, and silently ran the rejected rule; it now defaults to `None` and defers to `ExitModel`. (b) A per-strategy exit fitted on the same data used to measure the strategy is overfitting with extra steps. Keep exits global.

28. **"Stand Down" Is a Feature, and the Bot Must Be Allowed to Say It:** `recovery` and `bear` currently clear *no* strategies — every recovery cell was strongly positive in-sample and negative out-of-sample, and no bear cell had enough held-out trades to judge. `policy.build_playbooks` returns an empty playbook for those regimes and `live.scan_today` returns zero candidates. That is the correct output, not a bug to be fixed by loosening `TRADEABLE_VERDICTS`. A bot that always finds something to buy is the one that empties the account in a bear market.

29. **The Backtest Is Committed, the History Behind It Is Not:** `bot_backtest.json` (~900 KB) and `bot_signals.json` (~6 KB) ship in git; `deep_history/` (~100 MB) is gitignored. The Space therefore cannot recompute anything, which is why `/api/bot/signals` prefers a live scan and falls back to the committed copy, and why `bot-refresh.yml` caches the store and commits only the artifacts. Removing the fallback does not fail a single test and renders the Bot tab empty in production — exactly the shape of gotchas 14 and 15.

30. **Every Bot Number Is After Costs, and That Is Load-Bearing:** `costs.py` charges STT, stamp duty, exchange, SEBI, GST, DP and 15 bps slippage each way — ~0.53% a round trip. Against the original 5% target that is roughly a tenth of the gross edge, and it is the difference between several strategies reading positive and reading flat. `test_bot_engine.py::test_round_trip_is_material` fails if the schedule is ever gutted; `test_costs_reduce_the_result` fails if costs stop being applied at all.

31. **Survivorship Is Measured, Not Corrected — and It Currently Points the Other Way:** the universe is today's list, so companies that delisted are absent and pre-2018 results are filtered by survival (only 35% of today's symbols existed in 2007). `survivorship.py` reports coverage against performance per era; the correlation is **+0.65**, meaning the best-covered recent era carries the result rather than the heavily-filtered old one. Do not delete this measurement because it currently reads reassuring — it is the thing that would catch the opposite.

32. **Macro Is Tested Within a Fixed Regime, or It Only Rediscovers the Regime:** `measure_macro_edge` holds `regime_filter` constant (default `bull_strong`) before splitting on each external series, and each series' headwind direction is declared in `context_series.py` *before* measurement. Drop the regime filter and every series will look material, because they all correlate with bull markets. Choose the direction after seeing returns and every series "works" by construction. 7 of 10 currently carry real information; global VIX is worth ~0.58R per trade between tailwind and headwind.

33. **A Lesson May Only Come From What Was Knowable At Entry:** `review.py` splits tags into `ENTRY_TAGS` (volatility band, breadth, the name's ATR, macro headwind count) and outcome tags (`stop_hit`, `survived_near_stop`, `profit_handed_back`). **Only entry tags feed `lessons`.** Averaging R over an outcome tag is circular — `survived_near_stop` cannot contain a stopped-out trade, so it "discovers" +1.88R and means nothing. Before the split, seven of eight headline lessons were this artefact; afterwards one real one remained (quieter names pay more). `test_bot_learning.py::test_outcome_tags_never_become_lessons` pins it. Outcome tags stay visible — the verdict breakdown is built from them — but are barred from lessons.

34. **The Round-Trip Leak Is the Price of the Winners — Don't "Fix" It:** the review found 20.9% of trades (19,885) went over 1R in profit and finished negative, averaging -0.68R. The obvious fix — lock in a floor once the move is real — was tested through the full protocol as three candidates (`lock_0.5R_after_2R`, `lock_1R_after_2.5R`, `lock_1.5R_after_3R`). All three **raised win rate** (32.6% → 38.7%) and **lowered expectancy** (+0.284R → +0.249R), in-sample and out-of-sample alike. Clean wins average +5.27R and any floor that prevents giving back 1R also truncates those runs. `ExitModel.lock_trigger_r` exists and defaults off, with this result recorded. Re-deriving this by hand costs an afternoon.

35. **Evolution Means Re-Validating, Never Re-Fitting:** `evolution.py` keeps the rules fixed and moves a cell's *standing* — a cell positive lifetime and negative over the trailing two years is `retired`, not repaired. Nothing is refitted on recent results, because a system that tunes itself to the last few months destroys the out-of-sample evidence that made the edge believable in the first place. `replay_evolution` proves the adaptation is real rather than claimed: 76 quarterly checkpoints, each scoring cells on trades closed by that date only, producing 317 recorded status changes. `test_bot_learning.py::test_scoring_ignores_trades_that_had_not_closed_yet` is the causality guard.

36. **One Effect Measured Twice Is Not Two Findings:** the engine sets every stop at `stop_atr_mult x ATR`, so the "stop width" study and the "name volatility" study are the same quantity rescaled and *must* agree. `conditions.MECHANICALLY_LINKED` flags the dependent one, the UI marks it "Same effect, restated", and `quality.py` applies the ATR adjustment **only** — adding both would count one effect twice in the ranking. Of five entry conditions studied, exactly one real effect survives (quieter names pay ~0.24R more); breadth, index drawdown and volatility band are non-monotone and are shown as rejected rather than dropped.

37. **The Ledger Is SQLite and Therefore Can Never Be Committed:** `bot_ledger.db` (~68 MB at 95k trades) lives in `APP_STATE_DIR` beside the trade journal. The Space's pre-receive hook rejects binaries outright (gotcha 21), so it cannot ship even if it were small. Everything the UI needs travels as JSON inside `bot_backtest.json` via `learning.py`; the `/api/bot/ledger/*` endpoints return a clear 503 where no database exists rather than 500ing, and the Learning and Evolution tabs work without one.

38. **Trade-Level R Is Not a Result — Only the Account Is:** the population averages +0.178R (after honest costs), and an account with 8 slots taking ~220 of ~28,000 held-out signals returned **+1.61%** a year with the same trades; the same trades in a 60-position book returned +10.15%. The gap is capacity: which signals you can take is decided by when a slot frees, and that is not a random sample. Any claim about this system that quotes R per trade without the account number is quoting the easy half. `portfolio.py` exists to close that gap and `PortfolioResult.signals_declined` is reported for exactly this reason.

39. **The Ranking Adjustment Must Be Leak-Free — and Is Now a Minor Effect:** the adjustment must come from pre-split trades, never the published bucket study. Note the magnitude has since collapsed: under honest liquidity-scaled slippage (gotcha 43) and a wide book (gotcha 44), ranking with the volatility adjustment gives +10.15%/yr against +10.79% without it — the effect is now within noise, and the earlier "+10 points of CAGR" belonged to the narrow book it was measured in. The adjustment must come from `portfolio.derive_atr_adjustment(rows, before=split)` — trades entered *before* the split — and never from the published `condition_studies` buckets, which are computed over the whole population and would feed held-out results back into the ranking that selects inside it. Same numbers, wrong provenance. `test_bot_portfolio.py::test_adjustment_uses_only_trades_before_the_cutoff` pins it.

40. **Reactive Eligibility Loses Money — `evolution.py` Is Monitoring, Not a Trading Gate:** re-deciding every quarter from recent performance returned **-3.51%/yr**. Cells it *blocked* returned +0.403R while cells it *cleared* returned +0.106R, and within the cleared set the ranking picked trades worse than the ones it declined. Promoting a strategy after two good years buys it at its peak; strategy performance mean-reverts. The effect is period-dependent (it reverses on the held-out window, +0.295R), so it is **not** inverted into a contrarian rule either — it is simply not used to trade. Live eligibility comes from `policy.py`'s single walk-forward split. The reactive run is kept in `portfolio_runs` as the cautionary control.

41. **Never Annualise a Short Window:** `growth ** (1/span_years)` with a span near zero raises `OverflowError` and took the entire backtest down rather than degrading. `MIN_ANNUALISE_DAYS` (180) now reports total return instead of a compound rate built from three weeks.

42. **The Benchmark Answers Four Questions, Not One:** "better than a professional" is not a single claim. Against 691 real funds over 3.8 held-out years the account is **better on 3 of 4** — it beats Nifty (9.45% vs 6.06%), has a shallower worst drawdown (-19.9% vs -27.5%) and earns more per unit of drawdown (0.47 vs 0.41) — and **worse on raw return** (9.45% vs an 11.36% fund median, the 33rd percentile). Keep all four. Collapsing them into a yes is marketing; collapsing them into a no throws away a real result. `benchmark.CAVEATS` ships inside the payload because a verdict that travels without them will be quoted without them.

43. **Slippage Must Scale With Liquidity, or the Backtest Invents an Edge in Thin Names:** with a flat 15 bps, a bucket study found less-liquid names returned materially more R at p<0.0001 — and it was entirely a costing artifact. `SLIPPAGE_BY_TURNOVER` charges 10 bps at ₹100 cr/day up to 65 bps at the ₹2 cr floor; under it the turnover effect collapses (spread 0.178R → 0.050R, p 0.000 → 0.871) while the volatility effect strengthens. Fixing this cut the book from +0.242R to **+0.178R** and the headline held-out CAGR from 9.45% to 4.85%, so a chunk of the earlier result was cost mis-modelling. Do not revert to a flat rate to make the numbers look better.

44. **The Slot Cap Was the Single Biggest Drag — Wide Books, Small Positions:** a hard cap of 8 concurrent positions makes the account *queue* for entries, and positions close fastest when stopped out, so entries cluster into deteriorating conditions. Spreading the **same 6% total risk** across 60 positions instead of 8 took the identical trade record from +1.61%/yr at -24.6% to **+10.15%/yr at -12.8%**, Sharpe 0.18 → 0.99. That was worth more than every ranking refinement combined. The grid (8/15/25/40/60 positions × 6/9/12% total risk) was scored on pre-split data and 60 × 6% won on Sharpe; the held-out window was then run once. Practical caveat that no backtest charges for: sixty concurrent positions is a bot's book, not a person's.

45. **In-Sample Significance Is Not Enough — `selection.py` Validates Inside the Training Half:** three entry conditions passed a rank-correlation gate and a bootstrap at p<0.005 on pre-split data; applying all three took the held-out account from +10.95% to -1.94%. Two were significant and useless. `_survives_inner_validation` now fits on the earlier part of the pre-split window and requires the same direction on the later part before a condition may size an adjustment. Related trap: a permutation test over *five bucket means* is hopelessly underpowered (only 120 orderings, so rho=-0.90 returns p=0.18) — bootstrap the thousands of trades in the extreme buckets instead.

46. **An Ignoring Insert Silently Defeats a Schema Migration:** `record_trades` upserts. When the ledger gained six stock-level columns, every row still matched on (source, strategy, symbol, entry_day), so `INSERT OR IGNORE` skipped all 95,286 of them and the new columns stayed NULL — the condition studies then reported "0 usable buckets" rather than failing. Identity columns are excluded from the update so a re-seed can only enrich, never move a trade.

47. **The Book Must Be Capital-Constrained, Not Only Risk-Constrained:** `max_portfolio_risk_pct` caps what a full set of stop-outs would cost, which is a completely different quantity from capital deployed — sixty positions risking 0.10% each behind 2% stops is 6% of risk and 300% of capital. Without `max_deployed_pct` the book quietly ran to **128% invested**, i.e. on margin, for a cash-delivery strategy that has none. It is capped at 100% now. The leverage was worth only ~0.2pp, but a backtest that silently borrows is not a backtest.

48. **"Better Than a Professional" Is Settled by Dominance, Not by One Column:** return and risk trade off, so any single measure can be won by losing another. The count that cannot be gamed is how many funds beat the account on **both** return and drawdown at once: **9 of 691 (1.3%)**. Held-out, the account runs +11.37% CAGR at -12.6% drawdown, Sharpe 1.08 — the 99th percentile of 691 real funds on Sharpe, the 90th on drawdown, and the 50th on raw return. `benchmark.py` reports all six measures; do not collapse them.

49. **A 0.01-Point Win Is a Tie — `CAGR_TIE_BAND` Exists So the Scorecard Cannot Oversell:** the account beat the fund median by 0.01 percentage points and the scorecard duly printed "BETTER", which is how a measurement becomes marketing. Differences inside ±0.5pp of CAGR (±0.05 on Sharpe and return/drawdown) now read "level". Keep the bands; a scorecard that always finds a winner is not measuring anything.

50. **Live Trades Audit the Model, They Never Retrain It:** `calibration.py` is the closed loop and its asymmetry is deliberate. Twenty R-multiples have a standard error near 0.45R — wider than any edge in the book — so refitting on them is fitting noise, and this system has already *measured* that demoting after a bad run sells the bottom (gotcha 40). A cell is flagged and halved only at `MIN_LIVE_TRADES` (25) with a significant shortfall, stood down at `MIN_TRADES_TO_SUSPEND` (40) with p<0.01, and a cell **beating** its expectation is never promoted — upside surprise on a small sample is the most seductive noise there is. `test_bot_calibration.py` pins the restraint as hard as the action.

51. **The Scan Cache Must Include the Live-Trade Count:** `/api/bot/signals` caches on (session, equity, live trade count). Keyed on session alone — as it first was — recording a trade that suspends a cell left the stale candidate list served from cache, and the loop looked broken from outside while working perfectly inside.

52. **Shorter Holds Do Not Help a Capital-Constrained Book:** the obvious hypothesis — that a 60-position book limited by capital should prefer faster turnover — was tested through the full protocol by `scripts/sweep_account_exits.py`, scoring the *account's* Sharpe rather than per-trade R. It is false: 90 sessions scored 0.40 against 0.28 (60), 0.07 (40) and -0.08 (25) on pre-split data. Shorter holds destroy per-trade R faster than the extra turnover repays. The incumbent stands; don't re-derive this.

53. **Book Configuration Cannot Be Selected, So Report the Distribution:** across 30 defensible structures (40-100 positions, 5-9% total risk), the rank correlation between a configuration's pre-split Sharpe and its held-out CAGR is **-0.70** — choosing on the data available at the time points the *wrong way*, the same mean-reversion as gotcha 40. Held-out CAGR spans 7.9% to 14.9% across structures that are all reasonable. So `sensitivity.py` reports median (11.39%), IQR (9.8-13.1%) and the share beating each benchmark (100% beat Nifty, 50% beat the median fund), and no single configuration's figure is presented as the result. Anything quoted to a tenth of a point is one draw described as a measurement. Do not "improve" this by picking the configuration that won out-of-sample — that is selecting on the held-out window, the one thing nothing here is allowed to do.

54. **A Bigger Strategy Library Made It Worse — the Second Cohort Is Unregistered On Purpose:** five structurally different setups were added (episodic pivot, long-base breakout, RS-leader pullback, coiled spring, failed breakdown) on the reasonable hypothesis that the account's return is capped by what the library can recognise. Each was individually sound — **+0.15R to +0.36R, payoffs 2.2-3.9, all significant** over 133,036 trades — and enabling them still cost **4.3 points of median CAGR** (11.39% → 7.07% across 30 book structures; share beating the median fund 50% → **0%**). The cause is capacity, not quality: the book is capital-constrained and already declines ~95% of what it sees, so a high-volume setup at a middling edge (`rs_leader_pullback` fires thousands of times) crowds better candidates out of a fixed sixty slots. A larger library only helps a book short of ideas. They live in `strategies.SECOND_COHORT`, defined, tested and **not registered**; re-add one only alongside a ranking that can reliably keep it out of the book when something better exists.

55. **The Account Is Lumpy, So a Single CAGR Is a Statement About Which Years You Picked:** across four twelve-month periods the account ran from **-9.4 to +29.2 points against the index** (+29.2 / -9.4 / -5.0 / +12.2). That makes any single-window figure a choice of window, and the first version of this benchmark made exactly that mistake — quoting a **3.8-year** bot CAGR (11.39%) against a **3-year** fund CAGR (11.36%). Run over the funds' own 3-year window (2023-09 to 2026-09), the account's median is **5.60%** and **zero** of 30 book structures beat the median fund. Both figures are now reported, with `sensitivity.by_period` as the table that matters more than either. Never compare windows of different lengths here; the extra ten months contained the best run in the sample and inflated the headline by ~6 points.

56. **`sweep_account_exits.py` Holds the Playbook Fixed While Varying the Exit — Its Ranking Is a Screen, Not a Result:** the playbook is produced by walk-forward validation *on the trades*, and the exit rule changes every trade, so the sweep scores each candidate against cells validated under a different rule. The error is large: `trail_only_wide` (no time stop, 6 ATR trail) scored **+15.09%** in the sweep and returned **5.20%** on a full rebuild that re-derived the playbook under its own trades. Any rule that wins there must be confirmed by a full `run_bot_backtest.py` before touching `ExitModel`.

57. **The 90-Session Ceiling Is Load-Bearing, and Removing It Is the Obvious Wrong Move:** the account lags in strong rising markets (16.6% in a year the index made 26.0%) because a time stop sells out of live trends a fully-invested fund rides — so removing it looks like the fix. It is not. Trail-only with a wide leash gave **+56 points against the index in 2022-23 and -19.2 / -15.1 in the two most recent years**: it rides a trend magnificently and hands everything back when the market stops trending. Median CAGR across book structures fell from 11.39% to 5.20%, and the share beating the median fund from 50% to 0%. The ceiling caps the give-back.

58. **Benchmark a Broad-Universe Book Against the Nifty 500, Not the Nifty 50 — and Know What the Fund Median Really Is:** the bot trades 1,575 names across every size band, so measuring it against a large-cap index credits it with a size premium it never earned. Over 2023-09 to 2026-09 the Nifty 50 returned **5.47%**, the Nifty 500 **9.41%** and the Midcap index **15.57%**. `benchmark.INDEX_KEY` is the Nifty 500. The same correction reframes the funds: the median fund's **11.36%** against the Nifty 50 reads as five points of annual alpha and is mostly mid-cap beta — 11.36% sits between the Nifty 500 and the Midcap index. "Beats the median fund" and "beats the market" are different claims and neither substitutes for the other. Switching to the correct index cut the share of book structures beating it from 100% to **73%**, and the 2023-24 period from -9.4 to **-19.0** points.

59. **THE HEADLINE FINDING — THE EDGE DOES NOT SURVIVE ROLLING WALK-FORWARD.** Every positive figure in this system rests on one train/test split whose 3.8-year test window contains 2023, the single year the system worked. Rebuild the playbook each January from prior data only and trade the year that follows — the way it would actually run — and it compounds at **-2.60% a year against the index's +10.35%, beating the index in 2 of 11 years** (`scripts/rolling_walkforward.py`).

    The clinching column is `signal_avg_r`: the average R of every trade the playbook *allowed* that year, ignoring slots, sizing and capital entirely. It cannot be blamed on how the book is run — and it is **negative in 9 of 11 years, averaging -0.165R**. Deployment is 100% from 2022 onward, so this is not an under-invested book; the cells the playbook selects simply go on to lose money. Only 2021 (+0.27R) and 2023 (+1.20R) were positive, and 2023 is the outlier carrying the entire single-split result.

    Cells that validate on data through year Y-1 lose money in year Y — the same mean-reversion measured in gotchas 40 and 53, now a third instance. Do not quote the single-split numbers (11.37% CAGR, Sharpe 1.08, "better on 5 of 6") without this beside them; the Scorecard leads with the rolling table for that reason. **The system is a research instrument, not something to trade.**

60. **Earnings Drift Is the Best Signal Here and Still Does Not Beat Buy-and-Hold:** with every price-pattern family exhausted (gotcha 59), the remaining question was whether a *different kind* of information carries an edge. Post-earnings-announcement drift is the best candidate in the literature, and on full-sample numbers it is comfortably the strongest thing in this library — `earnings_gap_hold` **+0.543R** and `earnings_drift` **+0.489R**, against +0.20R for the best price pattern, and `earnings_drift/bull_strong` went **+0.33R in-sample to +0.81R out-of-sample on 528 trades**. Under rolling walk-forward a fundamental-only book returns **+3.21%/yr with a mean signal edge of +0.062R and a -2.8% median drawdown** — the first positive signal edge measured anywhere in this project, against -0.165R for the full system. It still loses to the index's +8.49% and beats it in only 4 of 11 years, and 2023 carries it exactly as it carries everything else (+2.12R signal edge that year, flat or negative in most others). Keep the earnings setups registered: they are low-frequency, so they cannot crowd the book (gotcha 54), and they are the only thing that has ever shown a positive out-of-sample signal edge. Do not mistake that for a tradeable system.

61. **Measure an Event Effect Against a MATCHED CONTROL, Never Against Zero:** raw forward returns after a >=10% positive earnings surprise look like textbook drift — +0.45% at 5 sessions rising monotonically to **+5.76% at 60**, holding out-of-sample. Against a matched random-date control *in the same stocks*, nearly all of it vanishes: 60-day drift is +3.94% against a control of +3.30%, a **+0.64pp difference with a 95% CI of [-0.54, +1.64] — not distinguishable from zero**. At 20 days the gap is +0.03pp. The long-horizon effect is market beta plus the plain fact that companies with analyst coverage and positive surprises are good companies that outperform anyway. Only the first week carries anything the announcement explains (~0.3pp), which is why `DRIFT_WINDOW_SESSIONS` stays at 5 — widening it captures beta and calls it edge. **Any event study here must carry this control.** Without it the effect looked nine times larger than it is.

62. **The Regime Engine Works as a TIMING Tool Even Though It Fails as a SELECTION Tool:** the same causal regime labels that cannot pick profitable stocks (gotcha 59) do usefully time index exposure. Holding the Nifty 500 only in `bull_strong`/`bull_narrow`/`recovery` and sitting in cash otherwise, 2007-2026, with 5 bps charged per switch: **+9.77% CAGR at a -16.9% drawdown against buy-and-hold's +11.69% at -64.3%** — 83% of the return for 26% of the damage, at 58% exposure and 68 switches over nineteen years. Return per unit of drawdown is 0.58 against 0.18 for buy-and-hold and 0.41 for the median fund.

    This is the same shape every result in this project takes: **the system consistently trades absolute return for risk reduction, which is what stops and cash positions do.** It is worth knowing that the regime classifier is the component carrying real information — it is just information about *when*, not about *which*.

63. **THE POSITIVE RESULT — REGIME TIMING BEATS THE MEDIAN FUND ON BOTH RETURN AND DRAWDOWN.** The regime classifier cannot pick stocks (gotcha 59) and can time exposure. Holding the Nifty 500 only in `bull_strong`/`bull_narrow`/`recovery`, cash otherwise: **+12.62% CAGR at -13.6% drawdown** on the held-out window, against buy-and-hold's +10.46% at -18.8% and the median professional fund's +11.36% at -27.5%. It wins on both axes, and survives every assumption tested — 3% to 7% cash with 20% STCG charged, it beats both references in all ten combinations.

    Three frictions are charged and must stay charged: cash earns a real rate (sitting out 26% of the time at 0% is as unrealistic as ignoring slippage), 5 bps per switch, and **tax on every realised gain at the short-term rate** where the hold was under a year — which most are, at a 147-day average. `test_bot_timing.py` pins the look-ahead guard: the rule acts on the **prior** session's label, and the test proves it takes the full hit from a crash that flips the regime the same day.

    The caveat travels with it: the drawdown advantage comes from being out of the market ~26% of the time including stretches when it is rising, which is the part a person finds hardest to do. And it is index timing — the stock-selection side of this system has no edge at all.

64. **THE LEARNED STOCK SELECTION SUBTRACTS FROM THE TIMING RESULT — DO NOT COMBINE THEM.** The obvious synthesis is to run both: deploy into validated stock signals when they fire, hold the index with the remainder while the regime is healthy, cash otherwise. It is worse than timing alone, measured on the held-out window with identical frictions:

        index only when regime healthy   +13.52%   (-9.8% drawdown)
        stock signals first, index fills  +8.37%  (-15.9% drawdown)
        stock signals only, cash else     +6.21%  (-14.6% drawdown)

    Every rupee the stock book takes from the index leg earns less and carries more risk. This is the cleanest available statement of what this project found: **the regime classifier carries real information about *when* to be exposed; the trade-level learning machinery — the ledger, the reviews, the attribution, the calibration loop — is mechanically sound and produces negative value.** Both facts are true and neither cancels the other. The Bot tab reports both, in that order.

65. **THE LEARNING LOOP CANNOT DRIVE EXPOSURE EITHER — ALL THREE USES TESTED AND ALL THREE FAIL.** The last idea worth trying was to point the learning machinery at the decision that *does* work: instead of picking stocks, let the rolling average R of the bot's own recently-closed trades decide index exposure. It is a worse signal than the regime classifier, and mixing the two degrades the regime signal:

        always invested (buy & hold)          +10.18%   -18.8%
        regime classifier only                +12.38%   -13.6%
        learned edge only, no regime input     +4.34%   -16.7%
        regime AND learned edge                +8.48%    -9.1%
        regime OR learned edge                 +8.50%   -22.2%

    That completes the search. The learning machinery has now been tested as a **stock selector** (-2.6%/yr, gotcha 59), as an **additive overlay on timing** (costs 5 points, gotcha 64), and as the **primary exposure signal** (+4.34% against the regime rule's +12.38%). Every configuration underperforms a causal regime classifier whose thresholds were written down before anything was measured.

    The conclusion is not that the machinery is broken — it is mechanically correct and 69 tests say so. It is that **recent trade outcomes carry no information about future trade outcomes in this data.** That is the same mean-reversion measured in gotchas 40, 53 and 59, stated in its most general form. Do not point the loop at a fourth target expecting a different answer.

66. **HELD WITHOUT STOPS, THE SIGNALS ARE WORSE THAN RANDOM DATES IN THE SAME STOCKS.** Every exit tested here uses a stop, including `trail_only`. The untested horizon was simple buy-and-hold, which is how most professionals actually earn their returns. Excess over the Nifty 500, against a matched random-date control in the same names:

        hold        picks      random control    difference
        6 months    +5.53%         +11.09%          -5.6pp
        1 year      +8.97%         +26.94%         -18.0pp   CI [-20.4, -15.8]
        2 years    +19.02%         +62.64%         -43.6pp   CI [-47.2, -40.3]

    Not merely absent — **negative**, and significantly so. The mechanism is plain: these are breakout and momentum signals, so they fire *after* a move, and holding from there means buying near local peaks while random dates also sample the run-up. One caveat on the size of the gap: random dates are spread uniformly over each symbol's history while signal dates cluster later, so part of the difference is that the control samples earlier, faster-growing periods. The direction is not in doubt at this magnitude.

    This also explains why the swing system works at all: short holds and stops are what keep the negative entry selection from expressing itself. Remove them and it does. **The search across holding periods is now complete — days through three years, no positive selection edge anywhere.**

67. **LONG-HORIZON CONCLUSIONS FROM THIS UNIVERSE ARE UNRELIABLE — EVERY BAND BEATS THE INDEX.** Testing the inverse of gotcha 66 (if breakouts mark peaks, buying weakness should pay) produced a tell rather than an answer. Sorted by prior 12-month return and held a year, **every single band** shows positive excess over the Nifty 500, from +4.5% to +27%. Stocks cannot all beat the index they compose. The universe is today's listed companies, so every constituent survived, and over one- and two-year horizons that survivorship swamps whatever signal is being measured (see gotcha 31 — the bias is mild at swing horizons and dominant here).

    Within that contaminated baseline the held-out ordering favours prior *winners* (+19.5% for the +60-200% band) over prior losers (+1.5% for -90 to -60%), which is the opposite of the reversal hypothesis. So the inverse fails too — but the honest reading is that **no long-horizon claim from this universe should be trusted at all** unless it is measured within-stock against a matched control, as gotcha 66 was.

68. **THE LEARNING LOOP BELONGS ON THE TIMING RULE, NOT THE STOCK CELLS:** `calibration.py` audits strategy x regime cells, which have no edge — so it was monitoring noise with great rigour. `timing.assess_health` points the same discipline at the component that actually earns. It deliberately does **not** watch recent P&L, which over a handful of switches says nothing; it watches whether the classifier still *discriminates*: the average daily return on days the rule was invested against days it sat out. That is the rule's entire claim, and if the gap closes the rule has broken whatever its recent return looks like. Currently **+0.021%/day invested against -0.039% out — a +0.060pp gap over 11 switches**, i.e. still separating good tape from bad.

    The asymmetry from `calibration.py` carries over and matters: health can stand the rule **down** and can never promote it. There is no state above `tracking`, because a good stretch across a few decisions is the most seductive noise in the system. `test_bot_timing.py::HealthMonitorTests` pins both directions — a rule whose invested days stop beating its out days is flagged `diverging`, and fewer than `MIN_SWITCHES_TO_JUDGE` reads `insufficient` rather than guessing.

69. **PER-SYMBOL LEARNING IS EXACTLY ZERO — THE LAST LEVEL, AND THE CLEANEST NULL IN THE PROJECT.** All learning here operates at the strategy x regime level; the untested granularity was the individual stock. Across five independent cut dates, ranking symbols by their prior average R under these strategies and measuring the next period's average R:

        cut      symbols   rank correlation
        2016         373        +0.053
        2018         471        +0.038
        2020         543        +0.016
        2022         718        -0.122
        2024         797        +0.021
        mean                    +0.001

    Quintiles by prior record, pooled: **+0.104 / +0.071 / +0.106 / +0.082 / +0.090** — flat. Best minus worst quintile **-0.014R, CI [-0.093, +0.065]**.

    That completes the search across every level of granularity this system supports: strategy x regime cells mean-revert (gotcha 59), book configuration is anti-predictive (gotcha 53), exposure driven by trade outcomes loses to the a-priori regime rule (gotcha 65), and individual symbols carry nothing at all. **The learning machinery is correct and there is nothing in this data for it to learn.** A stock that traded well under these setups is no more likely than any other to trade well next period.

70. **THE ADAPTIVE EXIT LOSES — AND SO DOES PERFECT FORESIGHT, WHICH IS WHAT CLOSES THE QUESTION.** Gotchas 53/59/65/69 all test the same thing in different places: does past performance of X predict future performance of X — and all four ask it about *stock selection*, the axis already known to be empty. `scripts/adaptive_exit_walkforward.py` asks it about the **exit** instead, which is the better-motivated question on both counts: the exit is the highest-leverage parameter in the system (choosing it moved the result from -0.005R to +0.28R, larger than any selection effect here), and the axis it keys off — market state — is the one axis proven to carry information, since regime timing beats the professional benchmark.

    Each year boundary, the chooser sees only trades that had already **closed**, ranks the five exit rules declared in `sweep_exit_models.py` by realised avg R, and the pick is scored unchanged on the year ahead. Full universe, 106,110 trades, 19 years:

        adaptive (global)      avgR +0.1638   CI [+0.1489, +0.1785]
        adaptive (per regime)  avgR +0.1416   CI [+0.1275, +0.1559]
        frozen incumbent       avgR +0.2084   CI [+0.1919, +0.2241]
        hindsight best/year    avgR +0.1741   CI [+0.1613, +0.1867]

    Adaptive loses by **-0.045R** globally and **-0.067R** per regime, and beat the frozen rule in **2 years of 19**. **The fourth line is the one that matters:** a chooser granted perfect foresight — told in advance which rule would win each coming year — *still finishes below the single rule left alone*. That is an upper bound on what any exit-learning scheme can achieve here, and it is negative. No smarter chooser, no better features, no longer training window recovers a prize that does not exist.

    The mechanism is visible in the yearly table: the years another rule "wins" are losing years, where it wins by losing less while taking far more trades at a much worse payoff (swing 1.39, quick 1.43, against the incumbent's **2.86**). The money is in 2020, 2021 and 2023 and the frozen rule already takes those in full; switching away to cushion a bad year forfeits the good one. This is also why gotcha 27's "keep exits global" holds along the *time* axis and not just the per-strategy one.

    `exit_learning.py` keeps the chooser rather than deleting it, for gotcha 31's reason — it is the component that would detect the opposite. `test_bot_exit_learning.py` pins the information set from four directions (a trade closing *on* the boundary is still the future; open trades are excluded rather than scored at zero) and asserts the recorded verdict, including `test_perfect_foresight_also_lost` — if that test ever fails there is a prize after all and a cleverer chooser becomes worth building.

71. **THE DEFENSIVE BREAKER IS THE ONE LEARNING RESULT THAT MEASURED POSITIVE — AND IT CUTS DRAWDOWN ONLY, NOT RETURN.** Gotchas 53/59/65/69/70 all test the *offensive* half of learning: can the bot learn to pick something better? All five say no. They are also all half the job — most of a professional's trade database exists to say what to **stop doing**, and `calibration.py` implements that half (it can demote, never promote) without it ever having been measured. `circuit_breaker.py` is the measurement: a strategy x regime cell stands down while its own last 25 **closed** trades average below zero, and returns the moment they recover.

    The control is the whole experiment. A breaker cuts the trade count, and a smaller book has a smaller drawdown for reasons that are arithmetic rather than intelligent — so every setting is scored against **the same number of signals suspended at random**, 40 seeded draws. Full universe, 100,871 signals, held-out window, primary rule:

        maxDD   -7.63%  vs random -11.65% (95th -10.12)   BEATS random
        CAGR   +10.38%  vs random +10.97% (95th +13.61)   below control mean
        Sharpe   +1.01  vs random   +1.05 (95th  +1.27)   below control mean

    **Report both halves or the result is a lie.** maxDD and Sharpe were declared co-primary before running because the hypothesis was about risk. Drawdown passes — the improvement survives the matched-random control, so it is not simply the effect of trading less. Sharpe fails, and CAGR sits below the control mean too. The breaker makes the worst hole shallower without making the account earn more: a **tail-risk brake, not an edge**. Baseline -13.04% -> -7.63% on a flat return is worth having, since drawdown is what ends accounts, but calling it an edge would be the ninth false positive in a project that has caught eight.

    What makes it credible is *where* it splits. Beating the random control on drawdown, by family: **window 25 -> 5 of 6; window 50 -> 1 of 6.** A 50-trade window in a cell firing a few dozen times a year reacts too slowly to stand anything down, so the split falls along reaction speed rather than at random. That is the main reason to believe the effect exists, and why `PRIMARY_WINDOW = 25`.

    **One load-bearing assumption:** the trailing window counts every signal the strategy generated, not only trades the account had a slot for. A live bot must therefore paper-track setups it could not fill — what a professional does with missed trades, computable in real time with no look-ahead. The stricter version is unworkable: the account takes ~56 trades a year, so per-cell 25-trade windows would never fill. `test_bot_circuit_breaker.py` pins the boundary (a trade settling on the decision day is not yet known), pins that a suspended cell is **reinstated** on recovery — a brake that never releases is a shutdown — and asserts `improves_risk_adjusted_return()` is still **False**, so no future summary can quietly promote "cuts drawdown" into "makes more money".

72. **REGIME TIMING BEATS THE MEDIAN FUND ON BOTH AXES; 6 OF 691 BEAT IT. SELECTION IS BEATEN BY 46.** Only two components survived this project — regime timing (when to be exposed) and the defensive breaker (what to stand down). `combined.py` holds them as sleeves and `scripts/combined_product.py` measures **every configuration separately** against the same funds, over a window cut to match the funds' own exactly. Measuring only one weighting would let the weighting be chosen after seeing which came out best.

                            CAGR      maxDD    percentile   funds beating it on BOTH
        timing alone      +12.63%    -12.12%      59.3        6 of 691  (0.9%)
        blend 50/50        +9.05%     -7.54%      29.5        5 of 691  (0.7%)
        stock book alone   +5.26%     -9.18%       3.2       46 of 691  (6.7%)
        median fund       +11.36%    -27.53%      50.0

    **The timing sleeve beats the median fund on return AND drawdown at once** — by +1.27pp (clear of `benchmark.CAGR_TIE_BAND`) and by 15.4 points — and only six funds of 691 beat it on both. That is the claim this project supports, and it belongs to deciding *when* to be exposed. **Stock selection sits in the 3.2nd percentile and is beaten on both axes by 46 funds**; it is reported in the same table at the same size, because a reader who leaves thinking the stock picking works has been misled by the layout rather than the number.

    The 50/50 blend is **not** the return-maximising choice and must never be presented as one: it gives up 3.58pp of return to buy 4.6 points of drawdown and a marginally better Sharpe. Which is the better product depends on the reader's tolerance, so both ship rather than one being declared the winner.

    **`INDEX_KEY` IS LOAD-BEARING AND WAS ONCE WRONG HERE.** The timing rule holds the **Nifty 500**, which is what `benchmark.INDEX_KEY` and the shipped timing study both use. An early version of `combined_product.py` read `"NIFTY"` — the Nifty 50 — and that single wrong string cost the blend ~1.4pp of CAGR, pushed it from the 29.5th percentile to the 18.5th, and produced a confident written conclusion that the bot "earns less than the median fund and cannot be made to earn more". It was caught only because the timing sleeve disagreed with the Scorecard's own figure for the same rule over the same window (+9.88% against +12.62%). **When two paths compute the same quantity, make them disagree loudly rather than quietly** — that discrepancy was the only thing standing between a measurement bug and a shipped falsehood.

    **What the learning is worth inside the product**, measured by running the identical blend with the breaker off: held-out `CAGR +0.22pp, maxDD +2.31pp, Sharpe +0.04`; exact 3y `CAGR -0.11pp, maxDD +1.25pp, Sharpe -0.02`. Drawdown in both windows and nothing else — the same answer the breaker gave on its own bench (gotcha 71). A learned component that grows a return contribution on its way into the product has not been measured, it has been marketed.

    Two caveats from `benchmark.CAVEATS` ride with every number above: the bot may sit in cash and a fund may not (a large structural advantage in a falling market, and not skill), and the bot's returns are simulated while the funds' are realised money net of fees actually charged.

74. **LETTING THE *EARNING* RULE EVOLVE MAKES IT WORSE — AND THE FIRST MEASUREMENT OF IT UNDERSTATED DRAWDOWN THREEFOLD.** Every learning test before this aimed at stock selection, the exit, or which cells to stand down. The component that actually beats a professional is regime timing, and its one parameter — `INVESTED_REGIMES` — was chosen **once** from five sets declared in advance. `scripts/adaptive_timing.py` asks the sharpest remaining version of "the strategy should evolve with market conditions": let the rule re-derive its own invested set each January from index sessions and regime labels strictly before that date.

    Run continuously, with the set changing at year boundaries and positions carried across them:

                          CAGR      maxDD    ret/DD   exposure
        adaptive        +11.37%    -19.28%    0.59      79.6%
        frozen (ships)  +10.51%    -16.03%    0.66      69.4%
        hindsight       +12.91%    -18.91%    0.68      66.2%
        buy and hold    +10.41%    -38.30%    0.27     100.0%

    The learner lifts raw return by **+0.86pp** and pays **+3.25pp of drawdown** for it at ten points more exposure — a worse trade than the frozen rule already offers (0.59 against 0.66). It is mostly buying more market in a market that rose. **Risk-adjusted, learning makes the one component that earns slightly worse**, which is the same verdict as every other offensive learning test here.

    **The measurement trap is the more useful half of this entry.** The first version chained the years — simulate each year, multiply the growth factors — and reported `maxDD -6.23%`, beating the frozen rule on *both* axes. That number was garbage: chaining samples equity **once a year**, so it cannot see an intra-year drawdown at all, and it understated the real figure (-19.28%) roughly **threefold**. A yearly-chained equity curve is fine for return and worthless for risk. Restarting the simulation each January also forces a flat-and-re-enter at every boundary and charges a switch the rule never asked for. `continuous()` exists to avoid both; the chained table is still printed, under a header telling the reader not to use it.

    `test_letting_the_timing_rule_evolve_does_not_improve_it` pins the verdict, including that the adaptive set is the *deeper* drawdown and the *higher* exposure — if it ever fails, learning has started improving the earning rule on risk-adjusted terms and the playbook should change.

75. **THE MINED RULES — SELECTIVITY PLUS A VERY WIDE TRAIL IS THE ONLY THING THAT BEAT THE SMALLCAP INDEX.** Earlier work asked whether the bot could *learn* which cells or symbols do better (gotchas 59/65/69/70/74) and all of it came back empty. `rules.py` asks the simpler question instead: across 100,871 trades, **what kind of trade paid** — measured on pre-2018 data, thresholds then frozen and applied to 2018-2026 untouched. Five conditions survived: **tight stop** (bottom-40% risk: +1.12R vs +0.40R in training, +1.59R vs +1.19R in test), **low turnover** (still true under the liquidity-scaled slippage that killed it the first time, gotcha 43), **positive 3-month momentum**, **six of twelve setups**, and a **healthy regime** (gotcha 63).

    **The exit carries most of the result and is deliberately extreme:** no target, trail 8 ATR, 500-session ceiling. The identical filtered trades return **+5.25%/yr** under the shipped 90-session/4-ATR rule and **+18.84%/yr** under this one. Widening the trail improved return, drawdown, Sharpe *and* payoff simultaneously — almost nothing else in this project does that. The trade record shows why: the biggest winner runs to **142R** and 4,003 trades exceed 10R, every one of which a 3R or 4R target would have cut off.

        full period, after full costs:  CAGR +18.84%  maxDD -18.41%  Sharpe 1.04  payoff 11.12  win 24.8%
        Nifty Smallcap 250, same span:  CAGR +16.26%  worst year -69%

    Two things make it work and both must survive any edit. **It declines 99% of what it sees** — 964 trades from 100,871 signals; a book that must be this picky cannot afford the six weaker setups. And **the 24.8% win rate is the price of the payoff, not a defect** — three trades in four lose, and the brief asked for 3-4:1 while the trail delivers 11:1.

    The returns are also **counter-cyclical**, which is where the alpha actually is: +18.1% in 2011 (smallcap -36.0%), +48.6% in 2018 (-26.8%), +53.7% in 2022 (-3.6%), +18.4% in 2025 (-6.0%). Gotcha 57 said removing the time ceiling was the obvious wrong move, and that remains true *for the unfiltered book* — a wide trail hands everything back when applied to marginal signals. It only pays once the entry filter has already thrown almost everything away. **Do not lift the exit out of `rules.py` and apply it to the general book.**

    `test_bot_rules.py` pins the filter (every rule rejects on its own; a **missing field rejects rather than waving through** — turnover defaulting to 0.0 sailed straight through the low-turnover test and let unknown names in), pins that the thresholds are not round numbers, and asserts the win rate stays reported beside the payoff.

76. **BOOKING P&L AT EXIT PUT THE GAINS IN THE WRONG YEAR — `mtm_account.py` EXISTS BECAUSE THAT INVERTED A WHOLE REPORT.** `portfolio.simulate` credits a trade's entire profit on its **exit** day. At the 90-session ceiling that is a minor distortion; at the 500-session trail `rules.py` needs it is fatal. A position opened in 2023 and closed in late 2024 puts every rupee into 2024.

    The first yearly table read *"the bot loses money in 2009, 2019 and 2023"* and the obvious next move was to go fix those years. **All three were artefacts** — their gains had not been booked yet. Marked to market, 2009 is +4.1%, 2017 +73.0% (reported as +11.0%) and 2023 +39.7% (reported as -2.2%), and the genuinely weak years turn out to be **2018, 2019 and 2025** — several of which the realised curve had shown as the *best* years, because that was when the previous years' positions were being closed out. Any question of the form "when does this struggle" must be answered from a marked book.

    **Drawdown gets worse under this accounting and that is correct:** -18.4% becomes -29.7%. A realised-only curve cannot see a position hand back 40% of its gain, because it never sees the gain until the trade is over. The old figure was not a better result, it was a blind one.

    With the book marked, two changes followed. **Diversification, not de-risking, was what cut drawdown**: the filter passes so few signals that a 10% position cap left the book concentrated, and 4% at 0.35% risk gave `CAGR +23.4%, maxDD -29.7%, Sharpe 1.36` against 10%'s +18.9%/-31.9%/1.16. And **selling everything when the regime turns is the wrong reflex** — it cuts drawdown from -31.9% to -26.0% but takes payoff from 11.1 to 4.0, because the result lives in a few positions held a year or more. `derisk_losers_only=True` sells only the under-water positions and keeps the tail.

    Result: **+22.5%/yr against the Nifty Smallcap 250's +16.3%, beating it in 14 of 18 years**, with positive alpha in the index's down years (2011 +37.7pp, 2013 +25.7pp, 2018 +17.1pp) *and* its up years (2014 +23.3pp, 2017 +15.7pp, 2020 +19.2pp, 2024 +20.6pp). The book is **80.7% small cap, 14.6% mid, 4.5% large**. 2009 is the one real miss (-109.8pp): a V-shaped recovery where `ret_63 > 0` excludes everything until the move is over.

    `test_bot_mtm_account.py` pins that open profit reaches equity before the exit, that a round trip shows a drawdown the realised curve would miss, and that a flat year does not borrow the next year's gain. Two of those tests failed first on **my own fixtures** rather than the code — the store's schema is `d`/`o`/`h`/`l`/`c`/`v` with date ordinals, not `dates`/`close`.

77. **THE LAGGING YEARS WERE UNDER-DEPLOYED, NOT BADLY PICKED — AND PARTIAL PROFITS COST MORE THAN THEY BUY.** Six rounds went into "fix the bad years" by hand-reading tables and guessing. `diagnose.py` does it mechanically and separates the three causes, which need opposite fixes and are trivially confused: **starved** (the filter passed almost nothing, so the book sat in cash), **bad_shots** (plenty of trades, negative avg R) and **under_deployed** (good R, too little capital in it). Every lagging year came back **under_deployed** — 2009 at +1.58R, 2012 at +1.63R, 2023 at +3.01R, all still behind the index. Six rounds had been aimed at the entry rules; the entries were fine.

    The fix was a bigger risk budget per trade, not a wider book. Raising risk from 0.35% to 0.50% took the account from `+23.4% / -29.7% / Sharpe 1.36` to `+28.5% / -35.3% / Sharpe 1.47`. Going the other way — 150 slots at 1.5% — returns +34.0% at **-60.4%** and Sharpe 1.16, so thinner-and-wider buys return at a price not worth paying. **13 of 18 years now beat the Nifty Smallcap 250, +28.5%/yr against +16.3%**, and the book amplifies rather than tracks in the index's good years (2021: index +61.9%, bot +91.1%; 2024: +26.4% against +62.8%).

    **Partial profit-taking works exactly as advertised and still loses money.** `ExitModel.scale_out_at_r` / `scale_out_fraction` sell a slice at a chosen R and pull the stop to entry. Taking 30% off at 2R moves the win rate from 25.1% to **39.1%** — squarely in the 35-40% band anyone would ask for — and the account goes `+23.4% -> +14.2%` with drawdown **deepening** from -29.7% to -40.9%. The deepening is the surprise and the mechanism is the breakeven stop: it closes positions that would have recovered, so the book churns and re-enters instead of holding through noise. Dropping the breakeven move recovers part of it (30% at 3R without it: +17.9%, -32.7%) and never reaches the unscaled book. Every variant is charged a second set of STT and brokerage, because a partial exit is a real sale.

    **A 25% win rate at a 10.7 payoff is the correct shape for this book**, whose result lives in positions running past 50R; the high win rate is available if wanted for its own sake and is not a free upgrade. `SCALE_OUT_AT_R` is `None` and `test_scaling_out_is_recorded_as_a_cost_not_an_upgrade` keeps it that way.

    Two tests failed on my own premises rather than on the code while building this: `diagnose` crashed formatting `alpha` for a year with no index return (now `no_benchmark` rather than a guessed verdict), and `test_drawdown_is_shallower_than_the_return` encoded the old 0.35% sizing — it now asserts the return/drawdown **ratio**, which is the quantity that actually has to stay defensible.

78. **BET SIZE FOLLOWS THE MARKET — THE ONE ADAPTATION THAT MEASURED POSITIVE, AND ONLY BECAUSE IT NEVER LOOKS AT ITS OWN P&L.** `diagnose.py` said every lagging year was **under-deployed**, which a fixed risk budget guarantees: it puts the same money to work in a year the market doubles as in one that falls apart. `adaptive_sizing.py` scales risk **2.0x / 1.0x / 0.5x** from three inputs known that morning — regime, breadth above the 200 DMA, and whether the index is above its own 200 DMA (the one condition that held direction in both halves: +1.16R vs +0.23R training, +1.85R vs +1.47R out of sample).

        fixed 0.50%    CAGR +29.69%  maxDD -35.31%  Sharpe 1.47
        adaptive       CAGR +34.49%  maxDD -36.27%  Sharpe 1.54

    **Behind the index in 2 years of 18, down from 5**, at **+33.1%/yr against the Nifty Smallcap 250's +16.3%** and beating it in **16 of 18**. It fixes the years pressing was supposed to fix: 2023 +37.6 -> +51.4 (past the index), 2025 -11.4 -> +0.7, 2018 +6.7 -> +38.9, 2011 +11.4 -> +57.2.

    **Why this one works when gotchas 40/65/69/74 all failed:** it reads the *market*, which carries information (gotcha 63), and never the bot's own recent trade outcomes, which do not. The multipliers were declared before measurement and there are three of them rather than a continuous function of breadth — a curve would be fitted, and there is not enough independent market history to fit one honestly. **A later edit that feeds recent P&L into this turns it into the loop that failed everywhere else**; `test_bot_adaptive_sizing.py` pins the boundary, including that a missing input sizes **down** rather than up.

    `memory.py` keeps the per-run diagnosis in `APP_STATE_DIR` so the recurring complaint survives between sessions — it was rediscovered by hand six times. It stores **diagnoses, not parameters**, deliberately: a store remembering "0.50% risk worked well" is a fitted parameter wearing a memory's clothes. "Every lagging year was under-deployed" is what survived and pointed somewhere useful. A year behind in one run is noise and is not called chronic; **2009 and 2012 are, and both are `starved` — 68 and 107 signals, so no exit rule or sizing change reaches them.**

79. **"TIGHT STOP" MUST BE RELATIVE TO CONDITIONS — THE ABSOLUTE CAP WAS WHAT STARVED THE WORST YEARS.** `diagnose.py` labelled 2009 and 2012 `starved` and I wrote that off as unreachable. That was wrong, and the diagnosis itself said so: *starved* means no rule fires, which is a rule problem. In 2009 the book saw **1,727 signals and only 298 cleared the 7.60% stop cap** — after a crash every stop is wide, so a fixed cap locks the book out of the market exactly while it rallies hardest. It took 68 trades all year.

    The rule that was actually mined is "tighter than typical", and typical moves. `accepted_with_rolling_risk` re-derives the 40th percentile from the **trailing year of signals**, strictly before the trade being judged, so the cap breathes with volatility while the selection stays identical:

        absolute      CAGR +34.49%  maxDD -36.27%  Sharpe 1.54   2009 -12.5%  (68 trades)
        rolling 365d  CAGR +36.26%  maxDD -33.09%  Sharpe 1.63   2009 +15.4%  (243 trades)
        rolling 730d  CAGR +32.76%  maxDD -37.38%  Sharpe 1.49   2009  +8.7%

    Better return, **shallower** drawdown and a higher win rate together — rare enough here to be worth stating plainly. A year is the right window; two averages across regime changes and hands most of the gain back.

    Final: **+34.8%/yr against the Nifty Smallcap 250's +16.3%, beating it in 15 of 18 years**, 80.5% small cap, 21-582 trades a year. 2009 still trails (-98.5pp) because the index did **+113.9%** off a crash bottom, and no long-only momentum book catches that from a standing start — but it is now +15.4% rather than -12.5%.

    `test_bot_rules.py::RollingRiskTests` pins that a calm history keeps the cap tight, a volatile one widens it, and **only earlier signals set it** — a later, calmer period must not change a decision already made. Two of those tests first failed on my own fixtures, dated 366 days apart and so outside the very window under test.

80. **A CAPPED POSITION MUST RE-DERIVE ITS RISK — THIS BUG INFLATED EVERY MTM RESULT BY ~2x.** `mtm_account.simulate` sized a position as `risk_amount / stop_pct`, clipped the *capital* at `max_position_pct`, and then booked P&L as `risk_amount * r` on the **unclipped** risk. `portfolio.py` had always recomputed `risk_amount = position_value * stop_distance_pct / 100` after clipping; the module I wrote to replace it did not.

    It was invisible because it never fails — it just pays too much. And it bit on **100% of trades**: a 0.25% budget behind a typical 5% stop asks for 7.9% of equity against a 4% cap, so every single position clipped and every single one over-booked. Four reported figures were wrong before it was caught (+23.4%, +28.5%, +34.5%, +36.3%); the true number is **+18.0%**.

    It surfaced from a different question — checking whether the adaptive multiplier was doing anything, since doubling risk cannot change a position that is already clipped. The answer was that it was not, and the cap check is what exposed the P&L path. **When a parameter appears to have an effect it structurally cannot have, the accounting is the first suspect.**

    Corrected and re-tuned (8% cap, 0.25% risk): **CAGR +17.97%, maxDD -34.58%, Sharpe 1.10, payoff 10.36, win 25.8%, 691 trades — beating the Nifty Smallcap 250 in 13 of 18 years, +17.3%/yr against +16.3%.** The alpha is **+1.0pp/yr**, not the +18.5pp reported before the fix. Selecting the book on return-per-drawdown gave a config that beat the index in only 8 of 18 years, so the config is chosen on years-beaten and CAGR jointly.

    `test_a_capped_position_books_pnl_on_what_it_was_allowed_to_take` pins it. Two assertions in `test_bot_rules.py` had to be relaxed afterwards because they encoded the inflated numbers — a test written against a wrong measurement will defend that measurement.

81. **RE-MEASURE EVERY CONCLUSION THAT WAS REACHED ON BROKEN ACCOUNTING — ONE OF THEM REVERSED.** Gotcha 80 fixed a ~2x P&L inflation in `mtm_account`. Two design decisions had been taken while it was live, and both had to be run again:

    * **Adaptive sizing reverses.** It ships **off**. `fixed +18.69% / -33.30% / Sharpe 1.21 / win 27.8%` against `adaptive +17.30% / -34.58% / Sharpe 1.10 / win 25.8%` — worse on all four. The +4.8pp it first appeared to deliver *was* the bug: pressing size in strong markets looked free precisely because the extra size booked P&L it was never allowed to take. A bug that inflates in proportion to position size will make any "bet bigger" rule look good, which is the general trap.
    * **Partial exits do not reverse.** The verdict holds and the gap is wider: `30% at 2R` gives the asked-for **39.9% win rate** and takes CAGR from +18.7% to **+10.3%** with drawdown deepening to -42.3%, and it halves the years that beat the index (13 of 18 down to 8). `30% at 3R` without the breakeven move is the least-bad version at +14.3% and 35.9% wins, still well behind.

    **Final shipped book:** rolling stop-width cap, no scale-out, fixed sizing, 8% position cap at 0.25% risk — `CAGR +18.69%, maxDD -33.30%, Sharpe 1.21, payoff 10.04, win 27.8%, 691 trades`, **beating the Nifty Smallcap 250 in 13 of 18 years at +18.7%/yr against +16.3%** (alpha +2.4pp). 80.5% small cap, 21-582 trades a year.

    The book is chosen on **years-beaten and CAGR jointly**, not on return-per-drawdown — that criterion picked a config beating the index in 8 years of 18. Choosing the metric is itself a modelling decision and gets stated rather than defaulted.

    **Loosening the rules in broad rallies was re-tested too, and also fails.** Allowing all twelve setups when the regime is `bull_strong` with breadth above 70 gives `+17.40% / -31.58% / 12 of 18`; adding a wide turnover cap on top gives `+16.80% / -53.76% / 11 of 18`. Base stays best on return, Sharpe and years-beaten. The informative part is what it does **not** move: with every setup and every liquidity band available, 2012 is still +18.0% against the index's +38.2% and 2009 still +24.8% against +113.9%. **Those two gaps are not the filter — the setups simply do not fire in a V-shaped recovery off a crash bottom**, and no relaxation of the entry rules reaches them. That is a property of a breakout/momentum library, not a bug to be tuned out.

    `test_it_is_not_wired_into_the_shipped_backtest` fails if adaptive sizing is re-enabled. It first failed on its own string check after I removed the call it was looking for — a test asserting on source text has to be updated with the source.

82. **A RECOVERY SETUP WAS BUILT FOR THE ONE GAP RULES COULD NOT REACH — IT WORKS AND THE BOOK IS WORSE WITH IT.** 2009 and 2012 survived every entry-rule change because the library is entirely breakout/momentum/pullback: after a crash nothing has a base to break out of, so nothing fires. `_recovery_reversal` is the missing shape — deeply broken stock (>35% below its 52-week high, near its own low) reclaiming the 50-day average on volume while its **three-month return is still negative**, the opposite of every other setup's requirement. Wide 3 ATR stop, because post-crash a 2 ATR stop is noise rather than invalidation.

    It does exactly what it was built to do: **2,218 signals at +0.647R**, firing 51 times in 2009 and 54 in 2012 when the rest of the library was silent. The book is still worse with it — `CAGR +17.23% -> +16.71%`, years beating the index `12 -> 11` — because +0.65R is below the book's own average and a capital-constrained account spends slots on it that better candidates wanted. Same capacity effect as the second cohort (gotcha 54). **And it did not close its own gap: 2009 stayed at +26.5%, 2012 at +17.0%.** It is defined, tested and unregistered, in `SECOND_COHORT`.

    **Two bugs surfaced building it, both worth keeping in mind.** It used `f.close`, which does not exist — the field is `f.bars.close` — and `run_strategies` catches per-strategy exceptions and continues, so a completely broken strategy reports **zero signals rather than failing**. A new setup that fires zero times is a bug until proven otherwise.

    And registering it moved the book from **+18.69% to +17.23% with no rule changed**, because `ROLLING_RISK_QUANTILE` is a percentile over *every* signal the library produces. Computing it over the tradeable setups alone is the tempting fix and it is wrong: the same 7.58% threshold sits at their **52nd** percentile, so passing 0.40 there silently tightens the rule to 6.82% (measured: +17.75%, 11 of 18). The pool stays as mined and `test_the_cap_is_coupled_to_the_registered_library` states the dependency out loud — **anyone registering a strategy must re-run the book rather than assume the rules are unaffected.**

83. **PARKING IDLE CAPITAL IN THE INDEX BUYS RETURN AND DOUBLES THE DRAWDOWN — AND TWO ATTEMPTS AT IT PRINTED MONEY.** Every diagnosis run returns `under_deployed`: the filter is selective, so in a year like 2009 most of the book sits in cash while the index compounds. The direct answer is to park the remainder in the Smallcap 250 (`mtm_account.park_idle_in`).

        cash idle     CAGR +18.69%  maxDD -33.30%  Sharpe 1.21  beat 13/18
        index idle    CAGR +25.45%  maxDD -78.04%  Sharpe 1.16  beat 13/18
        index gated   CAGR +22.71%  maxDD -64.32%  Sharpe 1.27  beat 12/18

    Gated means parking only while the regime is healthy, which is gotcha 63's rule applied to the idle sleeve. It is the first change in this whole sequence to lift CAGR materially — and it costs **31 points of drawdown for 4 points of return**, which is the wrong direction for a brief asking for more return at *less* risk. It also changes what the account is: a selective book plus an index fund, not a stock picker. Off by default.

    **The two bugs are the durable lesson.** First attempt: cash was restored from parked units at the *end* of the day, after trades had already spent it — a flat index printed money, and 2009 read **+1345%**. Money only conserves if the sleeve is liquidated at the **start** of the session, before anything touches cash, and re-established at the end. Second attempt: gating was implemented by dropping days out of the price map, so on an ungated day the lookup returned `None` and units already held were marked at **zero** — a -95.7% drawdown made entirely of arithmetic. Eligibility and valuation are now separate arguments (`park_only_on` vs `park_idle_in`).

    **There was a third bug, and fixing it reversed the verdict.** The index keeps its own calendar, so on a session it does not print, `park_idle_in.get(day)` returned `None` and the held units were marked at zero *again* — one layer below the gating bug and with the same signature. That single missing price was the entire -64% drawdown, in a book whose worst year is -12%. Carrying the last known price forward:

        cash idle     CAGR +18.69%  maxDD -33.30%  Sharpe 1.21  beat 13/18
        index gated   CAGR +23.21%  maxDD -29.89%  Sharpe 1.28  beat 13/18
        index always  CAGR +24.93%  maxDD -37.17%  Sharpe 1.14  beat 14/18

    Regime-gated parking is **better on return, drawdown and Sharpe at once** — +4.5pp of CAGR at 3.4pp *less* drawdown — which is the first thing in this entire sequence to improve both sides. It is now **on by default**. 2009 goes from +20.7% to +55.9%, and the alpha over the Smallcap 250 from +2.4pp/yr to **+7.0pp/yr**.

    The lesson is the one that repeated three times in a single feature: **a missing price is not a zero price.** Every lookup against a series with its own calendar needs a carry-forward, and the symptom is always an impossible drawdown rather than an error. Four tests pin this now, and the first is the one that matters: **parking in a perfectly flat index must change the result by nothing at all.**

84. **EXITING WHEN THE THESIS BREAKS RAISES THE WIN RATE AND DESTROYS THE PAYOFF.** Every exit in `ExitModel` is a price rule — a stop, a trail, a clock — and none asks whether the reason for owning the stock still holds. `exit_on_break` closes at the next open once the close has sat below its moving average for two consecutive sessions (read on the close, acted on the next open, like every other decision here).

        none             CAGR +23.21%  maxDD -29.89%  Sharpe 1.28  win 26.9%  payoff 9.76  13/18
        break sma50 x2   CAGR +16.43%  maxDD -28.43%  Sharpe 0.95  win 27.2%  payoff 4.25   7/18
        break sma50 x5   CAGR +17.95%  maxDD -28.94%  Sharpe 0.98  win 29.0%  payoff 4.35   9/18
        break ema21 x3   CAGR +14.10%  maxDD -29.74%  Sharpe 0.84  win 31.8%  payoff 2.91   7/18

    The same shape as every sell-earlier idea tested in this project: win rate up, drawdown marginally better, **payoff destroyed** (9.76 -> 2.91) and years-beaten halved. The mechanism is always the same — a position that runs to 50R spends weeks below its 50-day average on the way, and an exit that cannot tolerate that cannot hold the trades this book is made of. Off by default.

    **All of them were re-measured with parking switched on**, because the first runs predated it and the interaction was a fair objection: with idle capital riding the index rather than sitting in cash, money freed by an early exit is no longer a drag. It changes nothing. Under parking, `none/fixed` still wins at `+23.21% / -29.89% / Sharpe 1.28 / 13 of 18`, against `30% at 2R` at `+16.33% / -41.74% / 8 of 18` and adaptive sizing at `+21.27% / -32.87% / 11 of 18`. The best partial variant (`20% at 5R`, no breakeven) reaches +21.12% and still trails.

    That completes the list: **partial profit-taking, sizing up in strong markets, loosening the rules in rallies, a dedicated recovery setup, and a thesis-break exit — five ideas, all built, all measured on their own and again in combination, all net-negative.** The only one that improved both return and drawdown was parking idle capital in the index (gotcha 83), and it only did so after three accounting bugs in the same feature were fixed. **In a book whose result lives in the tail, every rule that sells earlier costs more than it saves**, and the win rate it buys is the clearest symptom rather than a benefit.

85. **WIN RATE IS A PROPERTY OF THE EXIT, NOT THE ENTRY — NO ENTRY CONDITION PREDICTS IT.** Every attempt to reach a 35-40% win rate in this book has gone through the exit (partial profits, thesis-break, tighter trails) and every one costs return, because selling earlier truncates the tail the result is made of. The untested alternative was to buy the win rate at the **entry**, which would be free — all the mining in `rules.py` targeted average R and none of it targeted hit rate.

    Measured across every recorded entry feature, quintiles on the training half against the same cuts on the held-out half (base rate 23.6%):

        risk_pct        train  24.0 29.6 27.1 23.6 24.7  |  test  19.3 18.7 25.1 26.8 20.1
        ret_63          train  23.6 19.7 28.8 28.8 28.0  |  test  22.4 20.4 19.9 23.1 26.2
        rel_volume      train  31.7 26.5 26.4 21.8 22.6  |  test  21.8 21.9 21.6 18.8 24.7
        turnover        train  24.6 28.5 25.2 25.9 24.8  |  test  26.4 19.4 22.0 22.7 21.5

    **Nothing replicates.** Stop width peaks in the first training band and the fourth test band; relative volume points one way in training and the other way out of sample; every spread is inside the noise of a 23.6% base rate. The same held per setup.

    So the win rate cannot be bought at the entry in this data, and buying it at the exit costs 7-8 points of CAGR every way it has been tried (gotchas 77, 84). **A 27% win rate at a 9.8 payoff is not a defect to be engineered away — it is what this book is**, and the two numbers are the same fact stated twice. Any future change that raises the win rate should be assumed to have sold a winner early until its payoff ratio proves otherwise.

86. **PARK IN THE BROAD INDEX, NOT THE BENCHMARK — THE BOOK IS ALREADY 80% SMALL CAP.** The idle sleeve (gotcha 83) first parked in the Smallcap 250 because that is what the book is benchmarked against. That is the wrong reason: the book is **80.5% small cap already**, so parking the remainder there doubles down on exactly the exposure it is meant to diversify.

        park in         CAGR      maxDD    Sharpe   ret/DD   beats benchmark
        smallcap250   +23.21%   -29.89%     1.28     0.78        13 of 18
        nifty500      +22.89%   -27.98%     1.32     0.82        15 of 18
        nifty50       +21.80%   -26.91%     1.30     0.81        14 of 18
        midcap150     +19.95%   -27.39%     1.27     0.73        14 of 18

    The Nifty 500 wins on drawdown, Sharpe, return-per-drawdown and — the measure that matters most here — **years beating the benchmark, 15 against 13**, for 0.3pp of CAGR. It also ships in the local deep-history store, so the account no longer depends on an external fetch to run.

    **Rotating the sleeve between indices is much worse and was the obvious next idea.** Holding whichever of Nifty 500 / Midcap 150 / Smallcap 250 led on trailing 63-day return (causal, chosen from data strictly before each session) gives `+13.88% / -52.87% / Sharpe 0.84 / 11 of 18` against the fixed Nifty 500's `+22.89% / -27.98% / 1.32 / 15 of 18` — and **2009 turns negative (-19.2%)**, the very year the rotation was supposed to capture. Momentum between indices buys each one at its top; by the time smallcap's trailing return leads, the move it is measuring has happened. The sleeve stays fixed.

    **Final shipped book:** `CAGR +23.81%, maxDD -27.98%, Sharpe 1.32, payoff 10.63, win 26.8%, 717 trades`, **+22.9%/yr against the Nifty Smallcap 250's +16.3% (alpha +6.6pp), behind it in 3 years of 18.** 80.5% small cap, 21-582 trades a year. The three remaining misses are 2009 (-73.4pp, the index did +113.9% off a crash bottom), 2012 (-13.3pp) and 2025 (-5.9pp), and all three are `starved` or `under_deployed` rather than bad selection — the entry rules were never the binding constraint in any of them.

87. **WITH 500-SESSION HOLDS THERE IS NO SPARE CAPACITY — A WEAK TRADE TAKEN TODAY BLOCKS A STRONG ONE FOR TWO YEARS.** Every earlier attempt at loosening the rules triggered on *market state* (a broad rally, a strong regime) and lost. The obvious objection was that the trigger was wrong: the diagnosis says the bad years are `starved`, which is a statement about capacity, not about the market. So `mtm_account.reserve` takes a looser pool **only after every core signal for the day is placed and capacity still remains** — it can never displace a core trade.

        reserve          n      CAGR      maxDD   Sharpe   beat    2009     2012     2025
        none             0   +22.89%   -27.98%     1.32   15/18   +40.5%   +24.9%   -11.9%
        wider turnover 3988   +15.96%   -31.29%     1.01    8/18   +53.3%   +21.7%    +1.2%
        more setups    2909   +19.58%   -30.46%     1.14   11/18   +53.9%   +21.5%   -10.2%
        both          10891   +15.96%   -33.31%     0.96    8/18   +59.5%   +14.3%    -8.5%

    It does exactly what it was built for — **2009 goes from +40.5% to +59.5%** — and costs 3 to 7 points of CAGR and up to seven years of outperformance. The mechanism is the one thing this experiment was designed to rule out and could not: a position held up to 500 sessions **borrows capacity from the future**. Filling a free slot today with a marginal trade blocks a better one for the next two years, so "only when there is room" is not the safe condition it sounds like. This is gotcha 54's capacity effect again, and the reason it survives a trigger built specifically to avoid it.

    That is nine ideas built and measured across this work — partial profits, adaptive sizing, rally relaxation, a recovery setup, a thesis-break exit, win-rate mining at the entry, sleeve choice, sleeve rotation, and capacity-triggered relaxation. **Two improved the book** (a volatility-relative stop cap, gotcha 79; parking idle capital in the broad index, gotchas 83/86). The rest are defined, tested and off, each with its number recorded so none of them needs re-deriving.

88. **THE 35-40% WIN RATE, BOUGHT WITHOUT TRUNCATING A SINGLE WINNER.** Every attempt at the brief's win-rate target went through the *trade* — a profit target, a scale-out, a thesis-break exit — and every one capped the positions that carry the result and **deepened** the drawdown doing it (gotchas 77, 84). The version that works goes through the *market* instead: when the regime turns, sell the stock book and hold the index sleeve, which `mtm_account` already supported and the shipped runner had never switched on.

        hold through   CAGR +22.89%  maxDD -27.98%  win 26.8%  payoff 10.63  ret/DD 0.82  15 of 18
        sell the turn  CAGR +19.05%  maxDD -17.58%  win 36.8%  payoff  3.75  ret/DD 1.08  13 of 18

    **36.8% win rate at a 3.75 payoff — both inside the brief's stated bands — with the drawdown nearly halved** and return-per-drawdown improving from 0.82 to 1.08. It costs 3.8pp of CAGR and two years of outperformance. No profit target is involved; `EXIT_TARGET_R` is still `None` and a winner still runs as far as the trail allows. `test_the_win_rate_now_clears_the_brief` asserts that, because a future change that hits the same win rate with a target would be the thing this whole sequence measured as harmful.

    **It is a flag, not the default, and the reason is the brief's own priority order.** Its first and most repeated complaint is years that trail the index — and holding through wins **15 of 18 against de-risking's 13**, while also returning 3.8pp more. The win rate is the second ask and buying it costs the first one, so `DERISK=1` selects it and the shipped default holds through.

    **The two profiles cannot be combined, which was the last idea worth trying.** When de-risking fires the stock book sells into cash, and parking is gated to healthy regimes, so that cash then earns nothing — an obvious-looking waste. Letting it park anyway puts the money straight back into the market the book just sold out of: `+19.85% / -35.64% / Sharpe 1.01 / 13 of 18`, twice the drawdown of the gated version for the same 36.8% win rate. Parking it in the Nifty 50 instead is the same result (`-35.31%`, 12 of 18). The idle cash is not waste, it *is* the protection.

    **There is no middle setting.** Cutting only in a genuine `bear` and leaving `choppy` and `correction` alone gives `+19.77% / -31.30% / Sharpe 1.13 / 11 of 18` — worse than *both* options on drawdown and years beaten. By the time the label reads `bear` the fall has already happened, so it gives up the upside without buying the protection. `cut losers` is not a middle option either: `+21.24% / -33.48% / 21.9% win / 13 of 18`, because the losers it cuts are the ones that would have recovered.

89. **DE-RISKED CAPITAL BELONGS IN GOLD, NOT CASH — THAT ONE CHANGE MADE THE WIN-RATE TARGET FREE.** Gotcha 88 hit the brief's 35-40% win rate by selling the stock book into a regime turn, and it cost 3.8pp of CAGR and two years of outperformance because the proceeds then sat in cash earning nothing. Holding them in **gold** instead — the standard crisis hedge, named before it was measured rather than picked from a list afterwards:

        default (cash sleeve)   CAGR +22.89%  maxDD -27.98%  Sharpe 1.32  win 26.8%  14/18*
        de-risk into cash       CAGR +19.05%  maxDD -17.58%  Sharpe 1.23  win 36.8%  13/18
        **de-risk into gold**   **CAGR +26.05%  maxDD -29.80%  Sharpe 1.45  win 36.8%  14/18**

    **+3.2pp of CAGR over the old default, the best Sharpe measured anywhere in this work, and the win rate arrives free.** Several chronic losing years turn: 2011 `-8.6% -> +9.3%` against an index that fell 36%, 2022 `-3.5% -> +9.5%`, 2025 `-11.9% -> +33.5%`, and 2012 `+24.9% -> +45.6%`, past the index for the first time. It costs one year (15 of 18 -> 14) and 1.8pp of drawdown. Alpha over the Smallcap 250 goes from +6.6pp to **+9.8pp a year**.

    **The sleeve must be built as a compounded level, never by switching between two price maps.** It holds *units*, so swapping a ~1,000-level index series for a ~10-level gold series would reprice those units 100x overnight; `mtm_account.composite_sleeve` chains daily returns instead and carries the last price through either calendar's gaps — the fourth appearance of "a missing price is not a zero price" in this one feature. `CompositeSleeveTests` pins both, and the first test is a flat pair producing no jump at the handover.

10. **Alpha Against a Price Index Is Flattered:** most equity categories benchmark to a Yahoo price index (no dividends), which overstates alpha by roughly 1.2%/yr. Rows carry `alpha_vs_price_index: true` and the UI flags it with a dagger — keep that flag if you touch the benchmark plumbing. Small and mid caps route through index-fund NAV instead precisely to avoid this (and because Yahoo's `^CNXSC` has no usable history).
