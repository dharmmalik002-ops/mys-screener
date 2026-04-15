# Project Handover Document: Stock Scanner

This document provides a comprehensive overview of the **Stock Scanner** project (referred to in the code as "Mr. Malik Scanner"). It is designed to be shared with another LLM to ensure a seamless transition of development.

---

## 1. Project Overview
A professional-grade stock screener and research tool covering **Indian (NSE/BSE)** and **US** markets. It features a high-performance data pipeline, automated technical scanners, and AI-driven fundamental analysis.

### Core Value Proposition
- **Data Integrity**: Autonomous "Watchdog" agent handles self-healing and data staleness detection.
- **Accuracy**: Solves the common "100x volume error" in Indian stock data by patching Yahoo Finance data with official NSE Bhavcopy ground truth.
- **AI Research**: Enrichment of stock fundamentals using Google Gemini to identify growth triggers and project future earnings.

---

## 2. Tech Stack

### Backend (Python/FastAPI)
- **Framework**: FastAPI (Asynchronous)
- **Data Fetching**: `yfinance`, `httpx` (direct NSE/BSE API calls), `beautifulsoup4`
- **Logic/Processing**: `pandas`, `pydantic` (settings & schemas), `apscheduler` (background jobs)
- **AI**: `google-genai` (Gemini Pro) for report generation and fundamental analysis.

### Frontend (React/Vite)
- **Framework**: React 19 + TypeScript + Vite
- **Visuals**: 
  - `lightweight-charts` (TradingView) for professional OHLCV charts.
  - `recharts` for fundamental data transitions.
  - Custom CSS (Modern Aesthetics: Dark mode, HSL-based palettes).
- **Structure**: Currently uses a high-density monolithic `App.tsx` (225KB) for the dashboard.

---

## 3. Architecture & Key Modules

### Data Engine (`backend/app/providers/free.py`)
This is the heart of the project. It implements:
- **Multi-Source Fetching**: Combines Yahoo Finance, NSE/BSE direct APIs, and Screener.in.
- **Volume Normalization**: Sophisticated logic to detect and correct 100x volume discrepancies using `bhavcopy` patching.
- **Caching Layer**: Multi-level JSON-based cache (`free_snapshots.json`, `free_fundamentals.json`) with versioning to prevent data corruption.

### Autonomous Watchdog (`backend/app/services/watchdog_agent.py`)
- Monitors the "health" of data across both markets.
- **Self-Healing**: If it detects a corrupt snapshot or stale data (>180s during market hours), it automatically clears caches and forces a live refresh.
- **Post-Close Maintenance**: Automatically triggers EOD data normalization when the market closes.

### AI Service (`backend/app/services/ai_analysis_service.py`)
- Generates "Money Flow" reports.
- Parses company updates into structured "Growth Triggers" and "Management Guidance".
- Filters noise from news to focus on material business changes.

---

## 4. Key Directories
```text
/
├── backend/
│   ├── app/
│   │   ├── api/          # API Route Definitions
│   │   ├── core/         # Config/Settings (Pydantic)
│   │   ├── providers/    # Data Fetching Logic (FREE, UPSTOX, DEMO)
│   │   ├── scanners/     # Technical Scan Logic (Minervini, etc.)
│   │   ├── services/     # Watchdog, AI Analysis, Dashboard Logic
│   │   └── main.py       # FastAPI Entry Point & Job Scheduler
│   ├── data/             # Persistent JSON Caches & Data storage
│   └── requirements.txt  # Dependencies
├── frontend/
│   ├── src/
│   │   ├── components/   # UI Fragments
│   │   ├── styles/       # Design System
│   │   └── App.tsx       # Main Dashbord Logic
│   └── package.json      # Dependencies
└── Dockerfile            # Deployment Config
```

---

## 5. Setup & Environment Variables

### Backend `.env` Keys:
- `GEMINI_API_KEY`: Required for AI fundamental enrichment.
- `APP_ENV`: `development` or `production`.
- `MARKET_CAP_MIN_CRORE`: (Default: `800`) Filters the universe.
- `DATA_MODE`: `free` (standard) or `upstox` (if using broker API).

### How to Run:
**Backend**:
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 10000 --reload
```

**Frontend**:
```bash
cd frontend
npm install
npm run dev
```

---

## 6. Current State & Known Implementation Details
- **503 Resilience**: The backend uses detailed exception handlers to return 503 errors during data rebuilds, allowing the frontend to retry gracefully rather than failing hard.
- **Hugging Face Compatibility**: Includes a "Keep-Alive" self-pinging loop to prevent environment sleeping on free tiers.
- **Chart Patching**: Chart data is dynamically "patched" with the latest quotes to ensure candles reflect the absolute latest price even before the provider updates the historical feed.

---

## 7. Next Steps for Development
1. **Frontend Refactoring**: Break down the monolithic `App.tsx` into specialized feature components (Screener, Charting, AI Insights).
2. **Database Migration**: Move from JSON-based filesystem storage to a proper database (e.g., SQLite or PostgreSQL) as the universe grows.
3. **Advanced Scanners**: Implement more complex technical scans (VCP patterns, Stage analysis).
