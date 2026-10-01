"""What a reference setup went on to do — the automatic winner/loser label.

A library of hand-picked charts is a library of ideas, and not every idea
worked. Because the bars after each chart's date exist, every reference is
graded the same way: from the NEXT session's open, did it reach `TARGET_PCT`
before `STOP_PCT`, inside `HORIZON` sessions? That turns "charts that gave good
moves" into "setups that worked" and "setups that looked the same and failed",
and the second group is what stops the model recommending every look-alike.

The thresholds are declared here, before anything is measured, and are not
fitted. A session that touches both levels counts as the stop — the same
conservative rule the bot engine uses.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

TARGET_PCT = 20.0
STOP_PCT = 8.0
HORIZON = 40

WORKED = "worked"
FAILED = "failed"
PENDING = "pending"


@dataclass(frozen=True)
class Outcome:
    label: str
    sessions: int            # sessions observed after the chart date
    max_gain_pct: float      # best high reached, vs the entry
    max_loss_pct: float      # worst low reached, vs the entry
    days_to_result: int | None


def grade(o, h, l, end: int) -> Outcome:
    """Grade the setup whose chart ends at index `end`. Entry is the open of
    `end + 1`; nothing at or before `end` is used except to locate it."""
    first = end + 1
    last = min(len(o) - 1, end + HORIZON)
    if first > last or not np.isfinite(o[first]) or o[first] <= 0:
        return Outcome(PENDING, 0, 0.0, 0.0, None)
    entry = float(o[first])
    target = entry * (1 + TARGET_PCT / 100)
    stop = entry * (1 - STOP_PCT / 100)
    best, worst = 0.0, 0.0
    for k, i in enumerate(range(first, last + 1), start=1):
        hi, lo = float(h[i]), float(l[i])
        best = max(best, (hi / entry - 1) * 100)
        worst = min(worst, (lo / entry - 1) * 100)
        if lo <= stop:
            return Outcome(FAILED, k, round(best, 2), round(worst, 2), k)
        if hi >= target:
            return Outcome(WORKED, k, round(best, 2), round(worst, 2), k)
    observed = last - first + 1
    # Neither level hit: only a completed horizon is a verdict. A setup still
    # inside its window has not failed, it has not finished.
    label = FAILED if observed >= HORIZON else PENDING
    return Outcome(label, observed, round(best, 2), round(worst, 2), observed if label == FAILED else None)
