"""An exit rule that re-picks itself from closed trades — measured, and rejected.

Every other learning test in this project asks whether past performance of
something predicts its future performance, and asks it about **stock
selection** — the axis already shown to be empty. This module asks about the
**exit**, which is a different and much better-motivated question:

* the exit is the highest-leverage parameter in the system — choosing it moved
  the result from -0.005R to +0.28R per trade, larger than any selection effect
  ever measured here;
* and the axis it would key off (market state) is the one axis that
  demonstrably carries information, since the regime timing rule beats the
  professional benchmark.

So the chooser below is the real thing: at each year boundary it sees only
trades that had already **closed**, ranks the candidate exit rules by realised
average R, and hands back a pick for the year ahead. It can also be asked per
regime, which is "the strategy evolves with market conditions" stated precisely
enough to come back false.

It came back false. `scripts/adaptive_exit_walkforward.py`, full universe,
106,110 trades across 19 years:

    adaptive (global)      avgR +0.1638   CI [+0.1489, +0.1785]
    adaptive (per regime)  avgR +0.1416   CI [+0.1275, +0.1559]
    frozen incumbent       avgR +0.2084   CI [+0.1919, +0.2241]
    hindsight best/year    avgR +0.1741   CI [+0.1613, +0.1867]

Adaptive loses by -0.045R per trade globally and -0.067R per regime, and beat
the frozen rule in 2 years of 19.

**The line that settles it is the fourth one.** A chooser granted perfect
foresight — told in advance which rule would score best in each coming year —
still finishes below the single rule left alone. That is an upper bound on what
any exit-learning scheme can achieve here, and the bound is negative. The
adaptation target is not merely hard to hit; there is nothing at the far end of
it. No smarter chooser, no better features, no longer training window recovers
a prize that does not exist.

The reason is visible in the yearly table. The years another rule "wins" are
losing years, where it wins by losing less while taking many more trades at a
far worse payoff (swing 1.39, quick 1.43, against the incumbent's 2.86). The
money is made in 2020, 2021 and 2023, and the frozen rule already captures
those in full. Switching away to cushion a bad year forfeits the good one.

Kept, rather than deleted, for the reason gotcha 31 keeps the survivorship
measurement: this is the component that would detect the opposite. If a future
candidate rule genuinely does carry a timing edge, re-running the script is how
it shows up — and the tests below keep the chooser honest about its information
set in the meantime.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable, Mapping, Sequence

# Measured by scripts/adaptive_exit_walkforward.py over the full universe.
MEASURED_ADAPTIVE_AVG_R = 0.1638
MEASURED_PER_REGIME_AVG_R = 0.1416
MEASURED_FROZEN_AVG_R = 0.2084
MEASURED_ORACLE_AVG_R = 0.1741
MEASURED_YEARS_ADAPTIVE_WON = 2
MEASURED_YEARS_SCORED = 19

MIN_PRIOR_TRADES = 200   # per candidate, globally, before a switch is allowed
MIN_PRIOR_REGIME = 60    # per candidate within a single regime


def prior_returns(
    closed_trades: Sequence,
    boundary: date,
    *,
    regime: str | None = None,
    trailing_years: int = 0,
) -> list[float]:
    """R-multiples the chooser is allowed to see at `boundary`.

    A trade counts only once it has **closed**, and only if it closed strictly
    before the boundary. Both halves matter. Including trades still open on the
    boundary leaks their eventual outcome; including trades that closed on the
    boundary itself leaks a day. A live bot standing at 1 January knows the
    result of every trade it has exited and nothing whatever about the rest.
    """
    low = date(boundary.year - trailing_years, 1, 1) if trailing_years else None
    out: list[float] = []
    for trade in closed_trades:
        exit_day = getattr(trade, "exit_day", None)
        if exit_day is None or exit_day >= boundary:
            continue
        if low is not None and exit_day < low:
            continue
        if regime is not None and getattr(trade, "regime", "") != regime:
            continue
        out.append(float(trade.r_multiple))
    return out


def choose_exit(
    closed_by_rule: Mapping[str, Sequence],
    boundary: date,
    *,
    fallback: str,
    regime: str | None = None,
    trailing_years: int = 0,
    min_trades: int | None = None,
) -> str:
    """Pick the exit rule for the period beginning at `boundary`.

    Returns `fallback` — the frozen incumbent — when no candidate has cleared
    the minimum sample. Refusing to choose is the correct behaviour, not a
    degenerate case: a switch made on eighty trades is noise given a licence.
    """
    floor = min_trades if min_trades is not None else (
        MIN_PRIOR_REGIME if regime is not None else MIN_PRIOR_TRADES
    )
    best_name, best_score = fallback, None
    for name, trades in closed_by_rule.items():
        values = prior_returns(trades, boundary, regime=regime, trailing_years=trailing_years)
        if len(values) < floor:
            continue
        score = sum(values) / len(values)
        if best_score is None or score > best_score:
            best_name, best_score = name, score
    return best_name


def adaptive_is_worse_than_frozen() -> bool:
    """The recorded verdict, as a value the rest of the system can assert on."""
    return MEASURED_ADAPTIVE_AVG_R < MEASURED_FROZEN_AVG_R


def oracle_is_worse_than_frozen() -> bool:
    """Perfect foresight still loses — the finding that closes the question."""
    return MEASURED_ORACLE_AVG_R < MEASURED_FROZEN_AVG_R
