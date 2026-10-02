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

from fastapi import APIRouter, Body, HTTPException, Query

logger = logging.getLogger(__name__)

RESULT_FILE = "lookalikes.json"
PICKS_FILE = "lookalike_picks.json"
REFS_FILE = "lookalike_refs.json"
INDEX_FILE = "lookalike_index.json"
DAYS_DIR = "lookalike_days"
VOTES_BACKUP_FILE = "lookalike_feedback_votes.json"
PUBLIC_FILES = (RESULT_FILE, PICKS_FILE, REFS_FILE, INDEX_FILE, VOTES_BACKUP_FILE)

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
    for name in PUBLIC_FILES:
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


SHOW_NEAR, SHOW_PEERS = 5, 6


def build_lookalike_router(data_dir: Path, database_url: str | None = None, state_dir: Path | None = None) -> APIRouter:
    from app.services.lookalike import feedback as fb

    router = APIRouter(prefix="/api/lookalikes", tags=["lookalikes"])
    votes_store = fb.FeedbackStore(database_url, state_dir, backup=data_dir / fb.BACKUP_FILE)
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

    file_cache: dict[str, dict[str, Any]] = {}

    def _load(name: str) -> dict[str, Any] | None:
        path = data_dir / name
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return None
        hit = file_cache.get(name)
        if hit is None or hit["mtime"] != mtime:
            try:
                file_cache[name] = {"mtime": mtime, "payload": json.loads(path.read_text())}
            except (OSError, ValueError) as exc:
                logger.warning("unreadable %s: %s", path, exc)
                return None
        return file_cache[name]["payload"]

    def _refs(keys) -> dict[str, Any]:
        refs = (_load(REFS_FILE) or {}).get("refs", {})
        return {k: refs[k] for k in keys if k in refs}

    def _day_file(day: str, stamp: str | None) -> dict[str, Any] | None:
        """A day's picks. Thousands of day files cannot all be pulled hourly,
        so a stale one is fetched from the repository the first time it is
        asked for — the calendar carries each file's checksum, which is how
        a stale copy is recognised."""
        import zlib

        path = data_dir / DAYS_DIR / f"{day}.json"
        local = path.read_bytes() if path.exists() else None
        if stamp and (local is None or f"{zlib.crc32(local):08x}" != stamp) and os.environ.get("LOOKALIKE_SELF_UPDATE", "1") != "0":
            try:
                import requests

                resp = requests.get(f"{RAW_BASE}{DAYS_DIR}/{day}.json", timeout=10)
                if resp.status_code == 200 and f"{zlib.crc32(resp.content):08x}" == stamp:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(resp.content)
                    local = resp.content
            except Exception as exc:  # serve what we have
                logger.info("look-alike day %s fetch skipped: %s", day, exc)
        if local is None:
            return None
        try:
            return json.loads(local)
        except ValueError:
            return None

    @router.get("/picks")
    def picks_summary() -> dict[str, Any]:
        """The calendar (which days have picks and how each turned out), plus
        the reviews, lessons, baselines and learner status."""
        _maybe_pull_in_background(data_dir)
        payload = _load(PICKS_FILE)
        if payload is None:
            return {"available": False, "reason": "No picks have been made yet."}
        return {"available": True, **{k: v for k, v in payload.items() if k != "days"}}

    @router.get("/picks/{day}")
    def picks_for_day(day: str) -> dict[str, Any]:
        """A day's picks, each with its own chart at the pick date and the
        reference charts it resembles, so the page can lay both out side by side."""
        if len(day) != 10 or not day.replace("-", "").isdigit():
            raise HTTPException(status_code=400, detail="Use YYYY-MM-DD.")
        summary = _load(PICKS_FILE) or {}
        stamp = (summary.get("calendar") or {}).get(day, {}).get("stamp")
        data = _day_file(day, stamp)
        if data is None:
            return {"date": day, "picks": [], "refs": {}, "note": "No picks were made on this date."}
        keys = {n["key"] for p in data.get("picks", []) for n in p.get("nearest", []) if n.get("key")}
        return {**data, "refs": _refs(keys), "rule_labels": summary.get("rule_labels", {})}

    @router.get("/similar/{symbol}")
    def similar(symbol: str) -> dict[str, Any]:
        """For the big chart's Similar button: the reference setups this stock's
        latest chart most resembles, per style, and the Indian stocks whose
        charts look most like it — all computed by the evening run."""
        _maybe_pull_in_background(data_dir)
        index = _load(INDEX_FILE)
        if index is None:
            return {"available": False, "reason": "The similar-charts index has not been built yet."}
        sym = symbol.strip().upper()
        row = (index.get("symbols") or {}).get(sym)
        try:
            my_votes = votes_store.for_symbol(sym)
        except Exception as exc:  # the votes store being down must not take the charts with it
            logger.warning("look-alike votes unavailable: %s", exc)
            my_votes = []
        hidden = fb.hidden_for(my_votes, sym)
        vote_of = {(v["kind"], v["target"]): v["vote"] for v in my_votes if v.get("session") == (row or {}).get("session")}
        if row is None:
            return {
                "available": False,
                "reason": f"{sym} was not in the last scan (it needs a year of history and at least ₹2 cr a day of turnover).",
                "session": index.get("session"),
            }
        styles = {}
        for style, st in (row.get("styles") or {}).items():
            near = [[k, sim] for k, sim in st.get("near", []) if ("ref", k) not in hidden][:SHOW_NEAR]
            styles[style] = {**st, "near": near, "votes": {k: vote_of.get(("ref", k), 0) for k, _ in near}}
        keys = {k for st in styles.values() for k, _ in st["near"]}
        peers = []
        for peer, sim in row.get("peers", []):
            if ("peer", peer) in hidden:
                continue
            other = index["symbols"].get(peer, {})
            peers.append({
                "symbol": peer, "similarity": sim, "closes": other.get("closes"),
                "session": other.get("session"), "vote": vote_of.get(("peer", peer), 0),
            })
            if len(peers) == SHOW_PEERS:
                break
        return {
            "available": True,
            "symbol": sym,
            "session": row.get("session"),
            "index_session": index.get("session"),
            "template": row.get("template"),
            "closes": row.get("closes"),
            "styles": styles,
            "peers": peers,
            "refs": _refs(keys),
            "feedback": index.get("feedback"),
            "hidden": len(hidden),
        }

    @router.post("/feedback")
    def record_feedback(payload: dict = Body(...)) -> dict[str, Any]:
        """👍 (1) / 👎 (-1) / undo (0) on one match for one stock and session."""
        try:
            saved = votes_store.record(
                str(payload.get("query", "")), str(payload.get("session", "")), str(payload.get("kind", "")),
                str(payload.get("target", "")), int(payload.get("vote", 0)), payload.get("style"),
            )
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=f"Bad vote: {exc}") from exc
        except Exception as exc:
            logger.warning("look-alike vote not saved: %s", exc)
            raise HTTPException(status_code=503, detail="The vote could not be saved right now.") from exc
        return {"ok": True, **saved, "counts": votes_store.counts()}

    @router.get("/feedback")
    def list_feedback(symbol: str | None = Query(default=None)) -> dict[str, Any]:
        """Votes for one stock, or every vote (what the evening run learns from)."""
        votes = votes_store.for_symbol(symbol) if symbol else votes_store.all()
        return {"votes": votes, "counts": votes_store.counts()}

    return router
