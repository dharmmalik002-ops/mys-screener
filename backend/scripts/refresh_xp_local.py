#!/usr/bin/env python3
"""One-command XP-breadth refresh — run from a machine that can reach BSE.

Why this exists
---------------
The XP Market Breadth Score needs the FULL BSE bhavcopy (~2,000 NSE equities).
BSE is geo-blocked from GitHub Actions' US/EU runners, so on days the CI job
can only get the YFINANCE universe (~1,575 symbols), the partial-day guard
correctly refuses to advance XP — and it freezes on the last full-coverage day.

BSE *is* reachable from an Indian connection (your Mac). This script fetches
the full BSE feed for the latest trading session, merges the authoritative
YFINANCE NSE closes, recomputes the day's XP score, and commits + pushes ONLY
``backend/data/xp_breadth_history.json``. The live backend's self-heal then
pulls that committed file within minutes — no HF login, no manual deploy.

Usage
-----
    cd backend && python scripts/refresh_xp_local.py

Add ``--no-push`` to compute + commit locally without pushing (review first),
or ``--dry-run`` to only print what the new score would be and write nothing.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
XP_REL_PATH = "backend/data/xp_breadth_history.json"


def _run_git(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh XP breadth from a BSE-reachable machine.")
    parser.add_argument("--no-push", action="store_true", help="commit locally but do not push")
    parser.add_argument("--dry-run", action="store_true", help="compute + print only; write nothing")
    args = parser.parse_args()

    # Import the generator's building blocks (same code the daily CI job uses).
    sys.path.insert(0, str(BACKEND_ROOT))
    from scripts.generate_bhavcopy_patch import (  # noqa: E402
        _fetch_from_bse,
        _last_trading_day,
        _merge_bse_with_yfinance,
        _update_xp_breadth,
    )
    import json

    target = _last_trading_day()
    print(f"→ Target session: {target.isoformat()}")

    bse = _fetch_from_bse(target)
    if not bse:
        print("✗ BSE unreachable from this machine. Are you on an Indian connection / VPN?")
        return 1
    print(f"  BSE feed: {len(bse)} symbols")

    merged = _merge_bse_with_yfinance(bse, target)
    breadth_src = dict(bse)
    if merged:
        breadth_src.update(merged)
    print(f"  Merged breadth universe: {len(breadth_src)} symbols")

    xp_path = BACKEND_ROOT / "data" / "xp_breadth_history.json"
    before = ""
    try:
        doc = json.loads(xp_path.read_text(encoding="utf-8"))
        before = str((doc.get("latest") or {}).get("date") or "")
    except Exception:
        pass

    if args.dry_run:
        # Compute against a throwaway copy so the real file is untouched.
        import tempfile
        import shutil

        tmp_hist = Path(tempfile.mkdtemp()) / "xp.json"
        tmp_roll = tmp_hist.with_name("roll.json")
        shutil.copy(xp_path, tmp_hist)
        roll_path = BACKEND_ROOT / "data" / "xp_breadth_rolling.json"
        if roll_path.exists():
            shutil.copy(roll_path, tmp_roll)
        import scripts.generate_bhavcopy_patch as g

        g.XP_HISTORY_PATH, g.XP_ROLLING_PATH = tmp_hist, tmp_roll
        _update_xp_breadth(target, breadth_src, "YF+BSE")
        latest = (json.loads(tmp_hist.read_text(encoding="utf-8")).get("latest") or {})
        print(f"  [dry-run] would set: date={latest.get('date')} xp_score={latest.get('xp_score')} regime={latest.get('regime')}")
        return 0

    _update_xp_breadth(target, breadth_src, "YF+BSE")
    doc = json.loads(xp_path.read_text(encoding="utf-8"))
    latest = doc.get("latest") or {}
    after = str(latest.get("date") or "")
    print(f"  XP updated: {before or '<none>'} → {after}  (score={latest.get('xp_score')}, regime={latest.get('regime')})")

    if after == before:
        print("• No new session to publish (already current). Nothing to commit.")
        return 0

    # Stage ONLY the XP history file — never `git add .`.
    add = _run_git(["add", XP_REL_PATH])
    if add.returncode != 0:
        print(f"✗ git add failed: {add.stderr.strip()}")
        return 1
    msg = f"data: XP breadth for {after} (full BSE feed, score={latest.get('xp_score')}) [skip ci]"
    commit = _run_git(["commit", "-m", msg])
    if commit.returncode != 0:
        print(f"✗ git commit failed: {commit.stderr.strip() or commit.stdout.strip()}")
        return 1
    print(f"✓ Committed: {msg}")

    if args.no_push:
        print("• --no-push set. Run `git push origin main` when ready.")
        return 0

    push = _run_git(["push", "origin", "HEAD:main"])
    if push.returncode != 0:
        print(f"✗ git push failed (pull --rebase and retry?): {push.stderr.strip()}")
        return 1
    print("✓ Pushed. The live backend self-heal will pull it within ~10 min "
          "(open the app to trigger it immediately).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
