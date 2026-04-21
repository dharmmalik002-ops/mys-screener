#!/usr/bin/env bash
set -euo pipefail
# Usage: deploy_to_hf.sh <hf-space> <hf-token>
# or set HF_SPACE and HUGGINGFACE_TOKEN env vars

HF_SPACE=${1:-${HF_SPACE:-}}
HUGGINGFACE_TOKEN=${2:-${HUGGINGFACE_TOKEN:-}}

if [ -z "$HF_SPACE" ] || [ -z "$HUGGINGFACE_TOKEN" ]; then
  echo "Usage: $0 <hf-space> <hf-token>  or set HF_SPACE and HUGGINGFACE_TOKEN env vars"
  exit 2
fi

TMPDIR="/tmp/hf-deploy-$(date +%s)"
rm -rf "$TMPDIR" || true
mkdir -p "$TMPDIR"

echo "Preparing snapshot in $TMPDIR"
rsync -a --delete \
  --exclude '.git' \
  --exclude '.github' \
  --exclude '.venv' \
  --exclude 'backend/.venv' \
  --exclude 'frontend/node_modules' \
  --exclude 'test-results' \
  --exclude 'tmp' \
  --exclude 'backend/data/chart_cache' \
  --exclude 'backend/data/chart_cache_us' \
  --exclude 'backend/data/news_cache' \
  --exclude 'backend/data/free_snapshots.json' \
  --exclude 'backend/data/free_snapshots_us.json' \
  --exclude 'backend/data/free_snapshots.json.gz' \
  --exclude 'backend/data/free_snapshots_us.json.gz' \
  ./ "$TMPDIR/"

cd "$TMPDIR"
git init
git checkout -b main
git config user.email "script@local"
git config user.name "local-deploy-script"
git add .
if git diff --cached --quiet; then
  echo "No deploy changes to push"
  exit 0
fi
git commit -m "Deploy bhavcopy snapshot from local"
REMOTE_URL="https://x-access-token:${HUGGINGFACE_TOKEN}@huggingface.co/spaces/${HF_SPACE}"
git remote add hf "$REMOTE_URL"
git push hf main:main --force

echo "Pushed snapshot to Hugging Face Space ${HF_SPACE}"
