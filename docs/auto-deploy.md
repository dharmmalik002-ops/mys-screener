Automation for live site EOD patching
====================================

What this adds
- A GitHub Actions workflow: `.github/workflows/bhavcopy-updater.yml` — generates the official NSE bhavcopy patch, commits it to `main`, prepares a deploy snapshot and pushes it to the Hugging Face Space, polls the Space until healthy and calls the maintenance endpoint to apply the patch and prewarm caches.
- Helper scripts in `scripts/`: `deploy_to_hf.sh` (local deploy) and `poll_and_apply_maintenance.py` (poll + maintenance call).

Required secrets / environment
- `HUGGINGFACE_TOKEN` — a Hugging Face token with repo write access for the Space (set in GitHub repository secrets as `HUGGINGFACE_TOKEN`).
- `HF_SPACE` — the Hugging Face space id in the form `owner/space-name` (set as secret `HF_SPACE`).
- `MAINTENANCE_TOKEN` — the maintenance token to send to the deployed service (set as secret `MAINTENANCE_TOKEN` in GitHub; also set the same value in the Hugging Face Space environment variable `MAINTENANCE_TRIGGER_TOKEN`).

How it works
- The scheduled Action runs after market close (Mon–Fri), generates the patch, and commits it to `main` if changed.
- The workflow then prepares a slim snapshot and pushes it to the Hugging Face Space repository using the HF token.
- After the Space finishes building, the workflow polls `/api/health`. When healthy, it POSTs to `/api/maintenance/eod-refresh` with header `x-maintenance-token: <MAINTENANCE_TOKEN>`.

Local/manual steps
- To deploy from your machine (useful for debugging):

```bash
HF_SPACE="owner/space-name" HUGGINGFACE_TOKEN="$HF_TOKEN" ./scripts/deploy_to_hf.sh
```

- To poll + apply maintenance manually:

```bash
python3 scripts/poll_and_apply_maintenance.py --space owner/space-name --token "$MAINT_TOKEN"
```

Notes & security
- Do not commit tokens to the repo. Use GitHub repository secrets and set the same `MAINTENANCE_TRIGGER_TOKEN` value in the Hugging Face Space environment settings.
- Adjust the cron schedule in the workflow to match your desired timing.
- If your generator requires additional credentials (exchange APIs), add them as secrets and modify the workflow to expose them as env vars.
