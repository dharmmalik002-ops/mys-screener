"""Daily closes for the scan universe, read from the committed artifact.

A Space-built snapshot's `chart_grid_points` is a stub — the daily bhavcopy
patch only ever appends to an existing grid, so a snapshot built without history
never gains one. Power Base and VCP need months of daily closes and cannot
afford a per-symbol fetch across ~1,900 names, so the closes ship in the repo
(`scripts/build_close_history.py`), the same reason `sector_indices.json` ships.

The artifact goes stale between rebuilds, so `closes_for` splices the snapshot's
20 trailing closes onto the end using the session dates. That heals up to 20
sessions of drift; past that a gap opens and the artifact wants rebuilding.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Lock

from app.models.market import StockSnapshot

logger = logging.getLogger(__name__)

ARTIFACT_PATH = Path(__file__).resolve().parents[2] / "data" / "close_history.json"

_cache: dict[str, dict] | None = None
_lock = Lock()


def _load() -> dict[str, dict]:
    global _cache
    if _cache is not None:
        return _cache
    with _lock:
        if _cache is None:
            try:
                payload = json.loads(ARTIFACT_PATH.read_text())
                _cache = payload.get("symbols") or {}
                logger.info("close_history: loaded %d symbols", len(_cache))
            except (OSError, ValueError) as error:
                # Missing artifact is survivable: the scanners fall back to the
                # grid and then to the 20 trailing closes.
                logger.warning("close_history: unavailable (%s)", error)
                _cache = {}
    return _cache


def _sessions_between(start: date, end: date) -> int:
    days = (end - start).days
    if days <= 0:
        return 0
    return max(1, round(days * 5 / 7))


def closes_for(snapshot: StockSnapshot) -> list[float]:
    """Daily closes oldest→newest, ending at the snapshot's own session.

    Empty when the symbol is not in the artifact, which is the signal to fall
    back to the grid.
    """
    entry = _load().get(snapshot.symbol)
    if not entry:
        return []
    closes = [float(value) for value in (entry.get("closes") or []) if value]
    if len(closes) < 40:
        return []

    session = snapshot.history_session_date
    last_time = int(entry.get("last_time") or 0)
    if not session or last_time <= 0:
        return closes

    artifact_date = datetime.fromtimestamp(last_time, tz=timezone.utc).date()
    missing = _sessions_between(artifact_date, session)
    if missing <= 0:
        return closes
    recent = [float(value) for value in (snapshot.recent_closes or []) if value]
    if not recent:
        return closes
    # More drift than the trailing window can cover: splicing anyway would weld
    # two non-adjacent stretches together and misplace every base measured
    # across the seam.
    if missing > len(recent):
        return []
    return closes + recent[-missing:]


def reset_cache() -> None:
    """Test hook — the artifact is read once per process."""
    global _cache
    _cache = None
