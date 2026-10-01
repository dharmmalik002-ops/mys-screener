"""`/api/lookalikes` — today's Indian charts that look like the reference library.

Serves the committed `data/lookalikes.json` written by
`scripts/scan_lookalikes.py`. The scan needs torch and the deep bar store,
neither of which exists on the Space, so the Space only ever reads the file —
the same live/offline split as the bot (gotcha 29). Sync `def` because it is
blocking file I/O (gotcha 8).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

RESULT_FILE = "lookalikes.json"
PICKS_FILE = "lookalike_picks.json"

# The evening job on the workstation commits both files with [skip ci], so no
# deploy carries them to the Space — the same situation as breakout_stats.json
# (main.py::pull_latest_breakout_stats). The route therefore pulls the
# committed copies itself, at most hourly, in a background thread: a request
# never waits on GitHub, it serves what is on disk and the next one gets the
# newer file.
RAW_BASE = os.environ.get(
    "LOOKALIKE_RAW_BASE",
    "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data/",
)
PULL_INTERVAL_S = 3600
_pull_lock = threading.Lock()
_last_pull = {"t": 0.0}


def _stamp(path: Path) -> str:
    try:
        return str(json.loads(path.read_text()).get("generated_at") or "")
    except (OSError, ValueError):
        return ""


def pull_latest(data_dir: Path) -> list[str]:
    """Replace each file with the committed copy when that copy is newer.
    Returns the names that changed. Never replaces a file with one that is
    unreadable or older."""
    import requests

    changed = []
    for name in (RESULT_FILE, PICKS_FILE):
        path = data_dir / name
        try:
            resp = requests.get(RAW_BASE + name, timeout=25)
            if resp.status_code != 200:
                continue
            remote = resp.json()
        except Exception as exc:  # network trouble: keep what we have
            logger.info("look-alike self-update %s skipped: %s", name, exc)
            continue
        remote_stamp = str(remote.get("generated_at") or "") if isinstance(remote, dict) else ""
        if remote_stamp and remote_stamp > _stamp(path):
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(remote, separators=(",", ":")))
            tmp.replace(path)
            changed.append(name)
    if changed:
        logger.info("look-alike self-update pulled %s", ", ".join(changed))
    return changed


def _maybe_pull_in_background(data_dir: Path) -> None:
    if os.environ.get("LOOKALIKE_SELF_UPDATE", "1") == "0":
        return
    now = time.monotonic()
    if now - _last_pull["t"] < PULL_INTERVAL_S or not _pull_lock.acquire(blocking=False):
        return
    _last_pull["t"] = now

    def run():
        try:
            pull_latest(data_dir)
        finally:
            _pull_lock.release()

    threading.Thread(target=run, name="lookalike-pull", daemon=True).start()


def build_lookalike_router(data_dir: Path) -> APIRouter:
    router = APIRouter(prefix="/api/lookalikes", tags=["lookalikes"])
    cache: dict[str, Any] = {"mtime": None, "payload": None}

    @router.get("")
    def lookalikes() -> dict[str, Any]:
        _maybe_pull_in_background(data_dir)
        path = data_dir / RESULT_FILE
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return {"available": False, "reason": "No look-alike scan has been run yet."}
        if cache["mtime"] != mtime:
            try:
                cache["payload"] = json.loads(path.read_text())
                cache["mtime"] = mtime
            except (OSError, ValueError) as exc:
                logger.warning("unreadable %s: %s", path, exc)
                return {"available": False, "reason": "The look-alike scan file could not be read."}
        return {"available": True, **cache["payload"]}

    picks_cache: dict[str, Any] = {"mtime": None, "payload": None}

    def _picks() -> dict[str, Any] | None:
        path = data_dir / PICKS_FILE
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return None
        if picks_cache["mtime"] != mtime:
            try:
                picks_cache["payload"] = json.loads(path.read_text())
                picks_cache["mtime"] = mtime
            except (OSError, ValueError) as exc:
                logger.warning("unreadable %s: %s", path, exc)
                return None
        return picks_cache["payload"]

    @router.get("/picks")
    def picks_summary() -> dict[str, Any]:
        """Everything the calendar needs except the picks themselves: which days
        have picks and how each day turned out, plus the reviews and lessons."""
        _maybe_pull_in_background(data_dir)
        payload = _picks()
        if payload is None:
            return {"available": False, "reason": "No picks have been made yet."}
        calendar = {}
        for day, rows in payload.get("days", {}).items():
            labels = [r.get("outcome", {}).get("label") for r in rows]
            calendar[day] = {
                "picks": len(rows),
                "worked": labels.count("worked"),
                "failed": labels.count("failed"),
                "pending": labels.count("pending"),
                "source": rows[0].get("source") if rows else None,
            }
        return {
            "available": True,
            **{k: v for k, v in payload.items() if k != "days"},
            "calendar": calendar,
        }

    @router.get("/picks/{day}")
    def picks_for_day(day: str) -> dict[str, Any]:
        payload = _picks()
        if payload is None:
            raise HTTPException(status_code=404, detail="No picks have been made yet.")
        rows = payload.get("days", {}).get(day)
        if rows is None:
            return {"date": day, "picks": [], "note": "No picks were made on this date."}
        return {"date": day, "picks": rows, "rule_labels": payload.get("rule_labels", {})}

    return router
