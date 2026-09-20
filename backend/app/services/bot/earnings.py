"""Earnings announcements as a point-in-time signal source.

Every other strategy in this bot reads price and volume. That entire family has
been measured and does not hold up out of sample (see gotcha 59), so the open
question is whether a *different kind* of information does. This is that test.

The hypothesis is post-earnings-announcement drift: prices under-react to large
surprises and keep moving in the direction of the surprise for weeks afterwards.
It is among the most replicated findings in the literature — which makes it
worth testing and equally worth suspecting, because a well-known effect is a
crowded one and Indian mid-caps in 2026 are not US large-caps in 1985.

**The announcement date is the whole discipline here.** A surprise is only
tradeable from the session the market first knew it, and the store keeps the
date the result was announced rather than the quarter it covers. Entry is the
session *after* the announcement at the earliest, because a result released
during or after market hours cannot be acted on that day. Getting this wrong
does not produce a subtle bias; it produces a strategy that buys the gap it is
supposed to be predicting.
"""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

STORE = "earnings_history"
# A surprise smaller than this is inside the noise of analyst estimates and
# rounding; treating it as news would flood the signal with non-events.
STRONG_SURPRISE_PCT = 10.0
# Drift is measured in weeks, not days. The signal is live for this many
# sessions after the announcement.
DRIFT_WINDOW_SESSIONS = 5


@dataclass(frozen=True)
class Announcement:
    day: date
    surprise_pct: float | None
    reported_eps: float | None
    eps_estimate: float | None


def store_dir(data_dir: Path) -> Path:
    return data_dir / STORE


def read_announcements(data_dir: Path, symbol: str) -> list[Announcement]:
    """One symbol's announcement history, oldest first."""
    path = store_dir(data_dir) / f"{symbol}.json.gz"
    if not path.exists():
        return []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as exc:
        logger.warning("earnings: unreadable %s: %s", path.name, exc)
        return []
    out: list[Announcement] = []
    for row in payload.get("announcements") or []:
        try:
            out.append(
                Announcement(
                    day=date.fromisoformat(str(row["date"])),
                    surprise_pct=row.get("surprise_pct"),
                    reported_eps=row.get("reported_eps"),
                    eps_estimate=row.get("eps_estimate"),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return sorted(out, key=lambda a: a.day)


def surprise_flags(
    bars_dates: np.ndarray,
    announcements: list[Announcement],
    *,
    min_surprise: float = STRONG_SURPRISE_PCT,
    window: int = DRIFT_WINDOW_SESSIONS,
) -> tuple[np.ndarray, np.ndarray]:
    """(positive-surprise window, negative-surprise window) per bar.

    A bar is flagged when a qualifying announcement landed within the previous
    `window` sessions — *strictly* previous. An announcement dated the same
    session is not tradeable that session: Indian results are commonly released
    after the close, and treating them as actionable on the day would let the
    strategy capture the announcement gap itself, which is precisely the move it
    is supposed to be predicting.
    """
    n = len(bars_dates)
    positive = np.zeros(n, dtype=bool)
    negative = np.zeros(n, dtype=bool)
    if not announcements or n == 0:
        return positive, negative

    position = {day: i for i, day in enumerate(bars_dates)}
    for item in announcements:
        if item.surprise_pct is None or abs(item.surprise_pct) < min_surprise:
            continue
        index = position.get(item.day)
        if index is None:
            # The announcement fell on a non-trading day (or a session this
            # symbol did not trade). Use the next session that exists.
            later = [i for d, i in position.items() if d > item.day]
            if not later:
                continue
            index = min(later)
        start = index + 1                    # strictly after — see the docstring
        end = min(n, start + window)
        if start >= n:
            continue
        if item.surprise_pct > 0:
            positive[start:end] = True
        else:
            negative[start:end] = True
    return positive, negative


def coverage(data_dir: Path) -> dict:
    directory = store_dir(data_dir)
    if not directory.exists():
        return {"present": False, "symbols": 0}
    paths = [p for p in directory.glob("*.json.gz")]
    return {
        "present": bool(paths),
        "symbols": len(paths),
        "path": str(directory),
    }
