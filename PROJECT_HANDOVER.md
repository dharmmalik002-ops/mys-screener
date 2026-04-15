# Project Handover Bible: Mr. Malik Stock Scanner

This document is a comprehensive "brain dump" designed for another LLM to pick up where we left off. It covers architecture, business logic, data integrity rules, and the current deployment state.

---

## 1. Project Essence & Identity
**Name**: Mr. Malik Scanner (Stock Scanner)
**Mission**: A high-performance, professional-grade stock screener for **Indian (NSE/BSE)** and **US** markets. It focuses on "Smart Data Integrity"—solving common data errors (like volume scaling) that plague free APIs, and enriching them with AI-driven growth analysis.

---

## 2. Technical Stack
### Backend (Python/FastAPI)
- **Framework**: FastAPI (fully asynchronous).
- **Database**: 
    - **Neon Postgres**: Used for user-facing persistent state (Watchlists). Uses `asyncpg` with custom DSN parsing for Hugging Face compatibility.
    - **Filesystem (JSON)**: High-speed snapshot caching (`data/free_snapshots.json`) and fundamental enrichment data.
- **Data APIs**: 
    - `yfinance` (Quotes/Charts)
    - Direct NSE/BSE API calls (Bhavcopy ground-truth)
    - `BeautifulSoup4` (Web scraping research)
- **AI**: `google-genai` (Gemini Pro) for "Money Flow" reports and management guidance parsing.
- **Scheduling**: `APScheduler` for background maintenance, watchdog cycles, and data warming.

### Frontend (React/Vite)
- **Core**: React 19 + TypeScript.
- **Charts**: `lightweight-charts` (TradingView) with custom "patching" logic to overlay live quotes on historical candles.
- **UI/UX**: Custom Vanilla CSS (Modern Dark Mode). High-density dashboard design.
- **Architecture**: In transition from a monolithic `App.tsx` (225KB) to modular components (`PremiumResearchPanel.tsx`, `EandCScannerPanel.tsx`).

---

## 3. Core Business Logic (The "Secret Sauce")

### A. The 100x Volume Normalizer (`backend/app/providers/free.py`)
Free APIs (Yahoo Finance) often report Indian stock volumes with a 100x discrepancy. This project solves this by:
1.  Downloading official **NSE Bhavcopy** files daily.
2.  Comparing Yahoo data against Bhavcopy ground-truth.
3.  Applying a patch layer to ensure screener results and charts reflect real trading volume.

### B. Autonomous Watchdog (`backend/app/services/watchdog_agent.py`)
A self-healing agent that runs every 30-90 seconds. 
- **Staleness Detection**: If a data snapshot is >180s old during market hours, it forces a live refresh.
- **Cache Bashing**: Automatically clears memory and runtime caches if data corruption is detected or if an official update is available.
- **Proactive Warmup**: Rebuilds the dashboard and sector heatmaps in the background so users never hit a "cold" cache.

### C. Money Flow & AI Research
Instead of just showing "Net Profit," the AI parses results into:
- **Management Guidance**: Future outlook vs. historical data.
- **Growth Triggers**: Material business changes (new orders, capacity expansion).
- **Editorial Filtering**: Separates generic news from market-moving announcements.

---

## 4. Architecture & Directory Map
```text
/
├── backend/
│   ├── app/
│   │   ├── api/            # FastAPI routes (India/US/Watchlist)
│   │   ├── core/           # Configuration & Pydantic settings
│   │   ├── db/             # Neon Postgres (asyncpg) logic
│   │   ├── providers/      # Market data logic (free.py is the main engine)
│   │   ├── scanners/       # Technical logic (Minervini, VCP, Momentum)
│   │   ├── services/       # Watchdog, Dashboard, AI Analysis
│   │   └── main.py         # App Entry, Lifecycle, & Scheduler
│   ├── data/               # Persistent JSON snapshots & bhavcopy storage
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── components/     # UI components (Research, Heatmaps, Scanners)
│   │   ├── styles/         # CSS Design System
│   │   └── App.tsx         # Dashboard Logic
│   └── package.json
└── Dockerfile              # Deployment for Hugging Face
```

---

## 5. Deployment & Reliability (Hugging Face Nuances)

- **Keep-Alive**: The backend includes a `_keep_alive_self_ping` loop (every 10 min) to prevent the HF Space from sleeping.
- **503 Graceful Handling**: A global exception handler converts all unhandled errors to `503 Service Unavailable`. This tells the frontend to "Retry in X seconds" instead of showing a crash screen.
- **Database URL Resilience**: `backend/app/db/neon.py` includes logic to strip quotes and fix `postgresql://` vs `postgres://` protocols commonly messed up by secret management UI.

---

## 6. Current Implementation State (As of April 2026)

### ✅ Done / Stable
- **Watchlist Persistence**: Fully functional via Neon Postgres.
- **Indian Market Accuracy**: Volume normalization pipeline is solid.
- **US Market Support**: Fundamental data and charting for 5800+ tickers.
- **Multi-Level Caching**: Prevents CPU exhaustion on HF free tier.

### ⚠️ In Progress / Known Issues
- **Frontend Refactor**: `App.tsx` is still too large. Moving logic to child components is urgent.
- **Data Snapshot Freshness**: Occasionally Yahoo Finance "tarpits" (hangs indefinitely). The 15s global timeout in `main.py` is the current fix.
- **Memory Pressure**: Handling 6000+ US stocks in memory on 2GB RAM (HF tier) requires careful GC (Garbage Collection).

### 🚀 Future Roadmap
1.  **VCP Pattern Scanner**: Advanced Stage-2 analysis logic.
2.  **User Authentication**: Separating watchlists by user account.
3.  **Real-Time WebSocket Feed**: Moving from polling to a push architecture.

---

## 7. Configuration Checklist (Env Vars)
- `GEMINI_API_KEY`: Required for Research Panel.
- `DATABASE_URL`: Neon Postgres connection string.
- `MARKET_CAP_MIN_CRORE`: Default `800`.
- `DATA_MODE`: Always set to `free` for current architecture.
- `APP_ENV`: `production` (on HF) or `development` (local).

---
**Handover Note**: To verify backend health, hit `/api/health`. To verify data integrity, check the "Watchdog Heartbeat" logs in the terminal.
