"""Learning that tells the bot what to STOP — the one form that measured positive.

Five learning tests in this project asked the *offensive* question: can the bot
learn to pick something better? Cells, books, exposure, symbols, exits — every
answer was no, and the exit test proved the ceiling itself is negative.

Those are all half the job. Most of a professional's trade database exists to
tell them what to **stop doing**, and that half had never been measured here.
This module is the defensive rule: a strategy x regime cell is suspended for as
long as its own recently closed trades average below a threshold, and comes
back the moment they recover. Only trades that had CLOSED before the day in
question are visible, so it is the information a live bot actually holds.

**The result, stated at its true strength and no higher.**

Against the matched random control — the same number of signals suspended at
random, 40 seeded draws, which is the control that matters because a smaller
book has a smaller drawdown for reasons that are arithmetic, not intelligence:

    held-out, w25_below_0 (primary)
        maxDD   -7.63%  vs random -11.65% (95th -10.12)   BEATS random
        CAGR   +10.38%  vs random +10.97% (95th +13.61)   below control mean
        Sharpe   +1.01  vs random   +1.05 (95th  +1.27)   below control mean

So: **the breaker cuts tail drawdown beyond what trading less would do, and it
does not improve risk-adjusted return.** Drawdown was a declared primary metric
and it passes. Sharpe was the other declared primary metric and it fails. Both
belong in any honest summary of this, and the CAGR line is not an improvement
either — it sits below the random control's mean.

What survives scrutiny is narrow and specific: the **short** window works and
the long one does not. Beating the random control on drawdown, by family:

    window 25 trades:  held-out 3 of 3,  full history 2 of 3   -> 5 of 6
    window 50 trades:  held-out 1 of 3,  full history 0 of 3   -> 1 of 6

A 50-trade window in a cell that fires a few dozen times a year reacts too
slowly to be standing anything down. That the split falls along reaction speed
rather than at random is the main reason to believe there is an effect here at
all, and it is also why the primary rule is a 25-trade window.

The honest summary for a reader: this reduces how deep the worst hole gets
without making the account earn more. Max drawdown is what ends accounts, so
that is worth having — but it is a tail-risk brake, not an edge, and calling it
an edge would be the ninth false positive in a project that has already caught
eight.

**One assumption is load-bearing.** The trailing window counts every signal the
strategy generated, not only the trades the account had a slot for. A live bot
must therefore paper-track the setups it could not fill — which is exactly what
a professional does with missed trades, and is computable in real time with no
look-ahead. The stricter version is not workable: the account takes ~56 trades
a year, so per-cell 25-trade windows would essentially never fill.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from typing import Mapping, Sequence

# The declared primary rule. Shorter reacts fast enough to matter; see above.
PRIMARY_WINDOW = 25
PRIMARY_THRESHOLD = 0.0

# Measured by scripts/circuit_breaker_walkforward.py, full universe,
# 100,871 signals, against a 40-draw matched random control.
MEASURED_HELD_OUT_BASELINE_MAXDD = -13.04
MEASURED_HELD_OUT_BREAKER_MAXDD = -7.63
MEASURED_HELD_OUT_RANDOM_MAXDD_P95 = -10.12
MEASURED_HELD_OUT_BASELINE_SHARPE = 1.04
MEASURED_HELD_OUT_BREAKER_SHARPE = 1.01
MEASURED_HELD_OUT_RANDOM_SHARPE_MEAN = 1.05
MEASURED_W25_BEATS_RANDOM = 5      # of 6 window x setting combinations
MEASURED_W50_BEATS_RANDOM = 1      # of 6


def suspended_mask(
    rows: Sequence[Mapping],
    window: int = PRIMARY_WINDOW,
    threshold: float = PRIMARY_THRESHOLD,
) -> list[bool]:
    """True where the trade's cell was under suspension on its own entry day.

    Per cell the already-closed trades are held in exit-day order with a running
    sum, so the trailing average as at any entry day costs two lookups.

    A trade closing ON the entry day is not yet known and does not count: the
    bot decides in the morning and that trade settles in the evening. This is
    the same boundary `exit_learning.prior_returns` enforces, and for the same
    reason — it is the cheapest possible place to leak a day of hindsight.

    Below `window` closed trades the cell is never suspended. Refusing to act
    on thin evidence is the point, not a degenerate case: a cell stood down on
    six trades is noise given the power to veto.
    """
    by_cell: dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    for row in rows:
        if row.get("exit_day") and row.get("r_multiple") is not None:
            by_cell[(row["strategy"], row["regime"])].append(
                (str(row["exit_day"]), float(row["r_multiple"]))
            )

    closed: dict[tuple[str, str], tuple[list[str], list[float]]] = {}
    for cell, items in by_cell.items():
        items.sort()
        days = [d for d, _ in items]
        cumulative = [0.0]
        for _, r in items:
            cumulative.append(cumulative[-1] + r)
        closed[cell] = (days, cumulative)

    out: list[bool] = []
    for row in rows:
        days, cumulative = closed.get((row["strategy"], row["regime"]), ([], [0.0]))
        n = bisect.bisect_left(days, str(row["entry_day"]))
        if n < window:
            out.append(False)
            continue
        out.append(((cumulative[n] - cumulative[n - window]) / window) < threshold)
    return out


def cuts_tail_risk() -> bool:
    """Drawdown beat the matched random control. The half that passed."""
    return MEASURED_HELD_OUT_BREAKER_MAXDD > MEASURED_HELD_OUT_RANDOM_MAXDD_P95


def improves_risk_adjusted_return() -> bool:
    """Sharpe did not beat the control. The half that failed, kept visible."""
    return MEASURED_HELD_OUT_BREAKER_SHARPE > MEASURED_HELD_OUT_RANDOM_SHARPE_MEAN
