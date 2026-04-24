# Full Project Handover — Indian Stock Scanner
# Feed this file to Claude Code at session start with:
# "Read this file for full project context: HANDOVER_FULL.md"

---

## What is this project
Indian stocks scanner SaaS web app — React/Vite frontend + FastAPI backend.
Users scan NSE/BSE stocks using custom filters, Minervini, VCP, Gap-up, RS scans.
AI-powered money flow reports and natural-language stock search via Gemini.

---

## Repositories & Live URLs

| | Value |
|---|---|
| GitHub repo | https://github.com/dharmmalik002-ops/mys-screener.git |
| HF Space (backend) | https://huggingface.co/spaces/dharmmalik/stock-scanner-backend |
| Live backend API | https://dharmmalik-stock-scanner-backend.hf.space |
| Frontend | Deployed on Vercel (check Vercel dashboard) |

---

## Credentials

| Secret | Value | Purpose |
|--------|-------|---------|
| Gemini API Key | [REDACTED] | AI analysis & weekly reports |
| Maintenance Token | [REDACTED] | Trigger backend cache refresh |
| GitHub PAT | [REDACTED] | Push to GitHub |
| HF Token | [REDACTED] | Deploy to HF Space |
| Vercel Token | [REDACTED] | Manage Vercel deployments |
| HF Space ID | dharmmalik/stock-scanner-backend | HF identifier |
| GitHub username | dharmmalik002-ops | Git user |

---

## Local setup

```bash
# Backend (FastAPI)
cd backend && python run_local.py
# Runs on http://localhost:8000

# Frontend (React/Vite)
cd frontend && npm run dev
# Runs on http://localhost:5173
```

---

## Stack

- **Frontend:** React + TypeScript + Vite, deployed on Vercel
- **Backend:** FastAPI + Python, deployed on Hugging Face Spaces (Docker)
- **Data:** Yahoo Finance, NSE, BSE (free tier). Bhavcopy for EOD prices.
- **AI:** Google Gemini API for money flow reports and natural-language search
- **Automation:** GitHub Actions — daily-bhavcopy.yml runs every market day

---

## Key source files

| File | Purpose |
|------|---------|
| backend/app/api/routes.py | All FastAPI endpoints |
| backend/app/providers/free.py | Core data provider (Yahoo/NSE/BSE), chart logic |
| backend/app/services/dashboard_service.py | Dashboard, scanners, chart orchestration |
| backend/app/models/market.py | Pydantic models |
| backend/app/scanners/definitions.py | Scanner definitions (Minervini, VCP, Gap-up etc.) |
| frontend/src/components/HomePanel.tsx | Home dashboard |
| frontend/src/components/ScreenerSidebar.tsx | Screener UI |
| frontend/src/components/SectorExplorerPanel.tsx | Sector explorer |
| frontend/src/components/MarketHealthPanel.tsx | Market breadth/health |
| frontend/src/components/MoneyFlowPanel.tsx | Money flow + AI reports |

---

## Deploy backend to HF Spaces

`git push hf main` is blocked — large data files (free_snapshots.json > 10MB).
Use huggingface_hub Python API instead:

```python
from huggingface_hub import HfApi
api = HfApi(token="[REDACTED]")
files_to_upload = [
    "backend/app/api/routes.py",
    "backend/app/providers/free.py",
    "backend/app/services/dashboard_service.py",
    # add any changed backend file
]
for f in files_to_upload:
    api.upload_file(
        path_or_fileobj=f,
        path_in_repo=f,
        repo_id="dharmmalik/stock-scanner-backend",
        repo_type="space",
        commit_message="fix: <description>",
    )
```

---

## Push to GitHub

```bash
git add <files>
git commit -m "fix: description"
git push origin main
# Credential is stored in macOS Keychain (osxkeychain)
# If push fails 403, regenerate a CLASSIC GitHub PAT with repo scope
# and store: printf 'protocol=https\nhost=github.com\nusername=dharmmalik002-ops\npassword=<PAT>\n' | git credential-osxkeychain store
```

---

## Trigger cache refresh without deploy

```bash
curl -X POST "https://dharmmalik-stock-scanner-backend.hf.space/api/watchdog/fix" \
  -H "Content-Type: application/json" -d '{}'
```

Or use the Maintenance Token:
```bash
curl -X POST "https://dharmmalik-stock-scanner-backend.hf.space/api/maintenance/refresh" \
  -H "Authorization: Bearer [REDACTED]"
```

---

## Coding rules (from AGENTS.md)

- Fix first, minimal explanation, no unnecessary narration
- Reproduce bugs before fixing — find root cause, not symptom
- Keep changes minimal and safe
- Make frontend resilient to empty/partial API data
- Add null/undefined guards, loading/error states for charts and async requests
- Done format: DONE / Changed files / Run commands / Status

---

## Known issues / gotchas

- `git push hf main` always fails — use HfApi.upload_file() instead
- GitHub PAT may need regeneration as classic token (current is fine-grained)
- CLAUDE_HANDOVER.md must NOT contain live tokens — GitHub and HF secret scanners will block the push
- Tokens should be rotated — they have been shared in plaintext during this handover session
- backend/.env is gitignored — contains GEMINI_API_KEY, MAINTENANCE_TRIGGER_TOKEN, HF_SPACE
- Daily bhavcopy runs via .github/workflows/daily-bhavcopy.yml automatically
