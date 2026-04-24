# Project Handover: Stock Scanner (Claude Code Edition)

This document provides the essential context, credentials, and constraints for managing the **Stock Scanner** project. 

## 1. Project Overview & Current State
The project has been optimized for **Hugging Face Free Tier (16GB RAM)**. It is now a **Lean India-Only** scanner.

- **Frontend**: React/Vite (Vercel) -> `https://my-screener-theta.vercel.app/`
- **Backend**: FastAPI (Hugging Face Spaces) -> `https://dharmmalik-stock-scanner-backend.hf.space`
- **Market Focus**: NSE India only. **US Market support has been removed** to save memory.
- **UI State**: Heavy modules (Sectors heatmap, Money Flow AI, Market Health/Breadth, and Newsdesk) have been **removed** to prevent OOM crashes.
- **Home Page**: Displays a simple list of **Top 10 Industry Groups** (no charts/aggregations).

## 2. Infrastructure & Credentials

| Service | Purpose | URL / Key |
|---------|---------|-----------|
| **GitHub** | Source Code | `https://github.com/dharmmalik002-ops/mys-screener.git` |
| **Hugging Face** | Backend Hosting | `https://huggingface.co/spaces/dharmmalik/stock-scanner-backend` |
| **Gemini API** | AI Analysis | `[REDACTED - Set in environment]` |
| **GitHub PAT** | Git Access | `[REDACTED - Set in environment]` |
| **HF Token** | Deployments | `[REDACTED - Set in environment]` |
| **Vercel Token**| Web Deployment | `[REDACTED - Set in environment]` |

## 3. Critical Memory Constraints (Hugging Face)
> [!CAUTION]
> Hugging Face Space has a strict **16GB RAM limit**. To prevent OOM crashes:
> 1. **Market Cap Filter**: Keep `MARKET_CAP_MIN_CRORE = 1500.0` (reduces universe by ~40%).
> 2. **No Startup Warmup**: Disable `STARTUP_CACHE_WARM_ENABLED` to prevent boot-time spikes.
> 3. **Single Threading**: All thread pools must use `max_workers=1` on HF.
> 4. **Garbage Collection**: Always call `gc.collect()` after building the dashboard or industry groups.

## 4. Common Workflows

### **Deploying Changes**
1.  **Commit to GitHub**: `git push origin main`
2.  **Deploy to HF**: `git push hf main --force` (if remote data commits conflict).
3.  **Vercel**: Pushing to GitHub `main` automatically triggers a production build.

### **Daily Maintenance**
- **Bhavcopy Update**: Automated via GitHub Actions (`.github/workflows/daily-bhavcopy.yml`). It runs every market day at 4:15 PM IST.
- **Cache Refresh**: If data is stale, trigger:
  `curl -X POST "https://dharmmalik-stock-scanner-backend.hf.space/api/maintenance/refresh" -H "Authorization: Bearer <MAINT_TOKEN>"`

## 5. Directory Structure
- `backend/`: FastAPI app, Pydantic models, and Data Providers.
- `frontend/`: React components and UI logic.
- `backend/data/`: JSON snapshot caches and chart data.
- `scripts/`: Maintenance and deployment scripts.

## 6. Coding Philosophy
- **Fix first, minimal explanation**.
- **Resilience**: The frontend must never crash if a backend field is missing (use optional chaining).
- **No Hallucinations**: If a library or API changed, verify with a test script before committing.
