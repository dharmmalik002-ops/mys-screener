"""Pin an approximate buy date read off a chart to the exact breakout session.

A buy point read by eye against a chart's x-axis is good to a week or a month,
and that matters: a window that ends three weeks late already contains the
breakout and the run after it, so the model would learn "the move" instead of
"the setup before the move". This finds the breakout the reader was pointing
at — the first close above the prior `PIVOT_LOOKBACK` sessions' high on
above-average volume — inside a window sized by how precise the reading was,
and ends the reference the session BEFORE it.

The rule is declared here and is the same for every chart. When no breakout
qualifies inside the window the reading is kept as it is, and flagged.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

PIVOT_LOOKBACK = 30
VOLUME_MULTIPLE = 1.2
VOLUME_AVERAGE = 50
WINDOW_SESSIONS = {"day": 3, "week": 7, "month": 15}


@dataclass(frozen=True)
class Refined:
    end: int          # index of the last session the reference chart shows
    breakout: int | None
    refined: bool


def breakout_sessions(h, c, v, lo: int, hi: int) -> list[int]:
    h = np.asarray(h, dtype=float)
    c = np.asarray(c, dtype=float)
    v = np.nan_to_num(np.asarray(v, dtype=float))
    found = []
    for b in range(max(lo, PIVOT_LOOKBACK, VOLUME_AVERAGE), min(hi, len(c) - 1) + 1):
        pivot = h[b - PIVOT_LOOKBACK : b].max()
        avg_vol = v[b - VOLUME_AVERAGE : b].mean()
        if c[b] > pivot and (avg_vol <= 0 or v[b] >= VOLUME_MULTIPLE * avg_vol):
            found.append(b)
    return found


def refine(h, c, v, approx: int, precision: str) -> Refined:
    """`approx` is the index of the session on or before the date read off the
    chart. Returns the index the reference window should end on."""
    width = WINDOW_SESSIONS.get(precision, WINDOW_SESSIONS["month"])
    candidates = breakout_sessions(h, c, v, approx - width, approx + width)
    if not candidates:
        return Refined(approx, None, False)
    # Nearest to the reading; on a tie, the earlier one — the first breakout is
    # the buy point, a later one is the follow-through.
    b = min(candidates, key=lambda i: (abs(i - approx), i))
    return Refined(b - 1, b, True)
