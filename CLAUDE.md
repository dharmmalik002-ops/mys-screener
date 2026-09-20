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
  - **`timing.py`: the one result here that beats a professional.** Not by picking stocks — by deciding *when to be exposed*. Hold the Nifty 500 in `bull_strong`/`bull_narrow`/`recovery`, park cash otherwise: **+12.62% CAGR at -13.6% drawdown held-out, against buy-and-hold's +10.46% at -18.8% and the median fund's +11.36% at -27.5%** — better on return *and* drawdown, with 20% tax and 5 bps per switch charged.
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

10. **Alpha Against a Price Index Is Flattered:** most equity categories benchmark to a Yahoo price index (no dividends), which overstates alpha by roughly 1.2%/yr. Rows carry `alpha_vs_price_index: true` and the UI flags it with a dagger — keep that flag if you touch the benchmark plumbing. Small and mid caps route through index-fund NAV instead precisely to avoid this (and because Yahoo's `^CNXSC` has no usable history).
