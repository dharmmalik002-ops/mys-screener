"""Pull the nightly fund artifacts from the repo onto a running Space.

`mutual-funds-refresh.yml` commits `mf_universe.json` and `sector_indices.json`
around 1 AM IST with `GITHUB_TOKEN`, and a push made with that token never
triggers `deploy.yml`. The Space therefore only received them with the next
afternoon's bhavcopy push (~4:20 PM IST) — and on 2026-10-14 a code deploy
landed six minutes before the refreshed sector file, so the Sectors tab kept
showing 13 indices "behind" all afternoon with the fixed file sitting on main.

Same remedy as `main.py::pull_latest_breakout_stats`: fetch from raw GitHub,
at most once per `MIN_INTERVAL_S` per file, in a background thread so no
request waits on it, with a conditional request so an unchanged file costs a
304. A pulled file must be at least as new as the local one and pass its
validator, or it is ignored — a bad commit can never replace a good file.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Callable

from . import paths

logger = logging.getLogger(__name__)

RAW_BASE = (
    os.environ.get("MF_ARTIFACT_RAW_BASE")
    or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data"
).rstrip("/")
MIN_INTERVAL_S = 3600

_lock = threading.Lock()
_last_try: dict[str, float] = {}
_etags: dict[str, str] = {}
_running: set[str] = set()


def _sector_stamp(doc: dict[str, Any]) -> str | None:
    """The stalest sector's last session — what decides whether the page is current."""
    sectors = doc.get("sectors")
    if not isinstance(sectors, dict) or not sectors:
        return None
    try:
        return min(entry["daily"]["dates"][-1] for entry in sectors.values())
    except (KeyError, IndexError, TypeError):
        return None


def _universe_stamp(doc: dict[str, Any]) -> str | None:
    funds = doc.get("funds")
    if not isinstance(funds, list) or len(funds) < 800:
        return None
    return str(doc.get("as_of") or "") or None


STAMPS: dict[str, Callable[[dict[str, Any]], str | None]] = {
    "sector_indices.json": _sector_stamp,
    "mf_universe.json": _universe_stamp,
}


def pull(name: str, *, http: Any = None) -> bool:
    """Fetch one artifact now. True when a newer, valid copy was written."""
    import requests

    stamp_of = STAMPS[name]
    path = paths.DATA_DIR / name
    local_stamp: str | None = None
    local_sectors = 0
    try:
        local = json.loads(path.read_text())
        local_stamp = stamp_of(local)
        local_sectors = len(local.get("sectors") or {})
    except (OSError, ValueError):
        pass

    headers = {}
    if name in _etags and local_stamp:
        headers["If-None-Match"] = _etags[name]
    try:
        response = (http or requests).get(f"{RAW_BASE}/{name}", headers=headers, timeout=30)
    except Exception as exc:  # noqa: BLE001 - keep the local copy
        logger.info("%s pull failed: %s", name, exc)
        return False
    if response.status_code != 200:
        return False
    try:
        remote = response.json()
    except ValueError:
        return False
    remote_stamp = stamp_of(remote) if isinstance(remote, dict) else None
    if not remote_stamp or (local_stamp and remote_stamp < local_stamp):
        return False
    if name == "sector_indices.json" and len(remote.get("sectors") or {}) < local_sectors:
        return False
    etag = response.headers.get("ETag")
    if etag:
        _etags[name] = etag
    if local_stamp and remote_stamp == local_stamp and path.stat().st_size == len(response.content):
        return False

    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes(response.content)
    temp.replace(path)
    logger.info("%s pulled: %s (was %s)", name, remote_stamp, local_stamp or "<none>")
    return True


def pull_in_background(name: str) -> None:
    """Throttled, non-blocking `pull`; the reader picks the file up by mtime."""
    # Only on the Space (HF sets SPACE_ID): a workstation, a test run or the
    # nightly builder holds a file it just built and must never trade it for
    # the copy on main.
    if not (os.environ.get("SPACE_ID") or os.environ.get("MF_ARTIFACT_PULL") == "1"):
        return
    now = time.monotonic()
    with _lock:
        if name in _running or now - _last_try.get(name, -MIN_INTERVAL_S) < MIN_INTERVAL_S:
            return
        _last_try[name] = now
        _running.add(name)

    def _run() -> None:
        try:
            pull(name)
        finally:
            with _lock:
                _running.discard(name)

    threading.Thread(target=_run, name=f"pull-{name}", daemon=True).start()
