"""Results announcement dates per symbol, and which session each one moves.

Source: BSE `Result` category filings (the regulator of record; every listed
company files there). Loaded by the runner into `RESULTS_CALENDAR`; an empty
calendar means every results-aware rule and setup has nothing to act on —
a symbol with no dates is never guessed at.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np

# symbol -> [(announcement date, minutes after midnight IST)]
RESULTS_CALENDAR: dict[str, list[tuple[date, int]]] = {}
MARKET_OPEN_MINUTES = 9 * 60 + 15
MARKET_CLOSE_MINUTES = 15 * 60 + 30
FILE_NAME = "results_calendar.json"


def load(data_dir: Path) -> int:
    """Fill RESULTS_CALENDAR from `data_dir/results_calendar.json`; returns symbols loaded."""
    path = Path(data_dir) / FILE_NAME
    if not path.exists():
        return 0
    raw = json.loads(path.read_text(encoding="utf-8"))
    RESULTS_CALENDAR.clear()
    RESULTS_CALENDAR.update({
        str(s).upper(): [(date.fromisoformat(d), int(m)) for d, m in v] for s, v in raw.items()
    })
    return len(RESULTS_CALENDAR)


def _ordinals(dates) -> np.ndarray:
    return np.array([d.toordinal() for d in dates])


def impact_sessions(symbol: str, dates) -> np.ndarray:
    """Boolean per bar: True on the first session whose prices reflect a result.

    A filing before the close of day D is priced on D; one after the close is
    priced on the next session.
    """
    out = np.zeros(len(dates), dtype=bool)
    events = RESULTS_CALENDAR.get(symbol)
    if not events:
        return out
    ords = _ordinals(dates)
    for day, minutes in events:
        k = int(np.searchsorted(ords, day.toordinal()))
        if k < len(ords) and ords[k] == day.toordinal() and minutes >= MARKET_CLOSE_MINUTES:
            k += 1
        if 0 <= k < len(ords):
            out[k] = True
    return out


def exit_sessions(symbol: str, dates) -> np.ndarray:
    """Boolean per bar: True where a position must be out by the OPEN."""
    out = np.zeros(len(dates), dtype=bool)
    events = RESULTS_CALENDAR.get(symbol)
    if not events:
        return out
    ords = _ordinals(dates)
    for day, minutes in events:
        k = int(np.searchsorted(ords, day.toordinal()))
        if minutes < MARKET_OPEN_MINUTES or k >= len(ords) or ords[k] != day.toordinal():
            k -= 1
        if 0 <= k < len(ords):
            out[k] = True
    return out
