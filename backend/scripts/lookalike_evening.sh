#!/bin/bash
# Weekday-evening look-alike run.
#
# Scheduled by ~/Library/LaunchAgents/com.mrmalik.lookalike-evening.plist, which
# runs a COPY of this file at ~/.lookalike/lookalike_evening.sh — the run below
# resets the repository to origin/main, and bash reads a script as it goes, so
# the scheduled copy must not live inside the tree being reset.
#
# Everything happens in ~/lookalike-runner, a dedicated clone OUTSIDE the
# Desktop: macOS refuses background jobs access to ~/Desktop ("Operation not
# permitted") unless bash is given Full Disk Access, which this setup avoids.
# The private library (data/lookalike/) and the price store (data/deep_history/)
# live in the runner; the Desktop working copy reaches them through symlinks.
#
#   1. reset the runner to origin/main (latest code; ignored data is untouched)
#   2. top up data/deep_history/ with the sessions since the last run
#   3. scripts/run_lookalike_day.py: review open picks, pick today, re-export
#   4. commit the two public files with [skip ci] and push — the Space pulls
#      them itself within the hour (app/api/lookalike_routes.py)
#
# The pick ledger (data/lookalike/picks.json) exists only on this Mac. Back it up.
#
# Install / update the scheduled copy:
#   mkdir -p ~/.lookalike && cp backend/scripts/lookalike_evening.sh ~/.lookalike/

set -uo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin"

RUN="$HOME/lookalike-runner"
PY="$(command -v python3)"

echo "=== look-alike evening run $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
cd "$RUN" || { echo "ERROR: $RUN missing"; exit 1; }
git fetch -q origin main && git reset -q --hard origin/main || echo "WARNING: could not update the runner; using the code it has"

cd "$RUN/backend" || exit 1
"$PY" scripts/build_deep_history.py || echo "WARNING: price top-up failed; picking from the bars already on disk"
"$PY" scripts/run_lookalike_day.py || { echo "ERROR: look-alike run failed"; exit 1; }

cd "$RUN" || exit 1
git add backend/data/lookalikes.json backend/data/lookalike_picks.json
if git diff --cached --quiet; then
  echo "nothing new to publish"
  exit 0
fi
git commit -q -m "data: look-alike picks for $(date +%F) [skip ci]"
for attempt in 1 2 3; do
  if git push -q origin HEAD:main; then
    echo "published $(git rev-parse --short HEAD)"
    exit 0
  fi
  echo "push rejected (attempt $attempt); rebasing on the newest main"
  sleep 20
  git pull -q --rebase origin main || { git rebase --abort 2>/dev/null; echo "ERROR: rebase failed"; exit 1; }
done
echo "ERROR: publish failed after 3 attempts"
exit 1
