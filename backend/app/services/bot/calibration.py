"""Does the live book behave the way the study said it would?

This is the closed loop, and its design is a deliberate refusal of the obvious
one. The tempting version retrains on live results: a cell does badly for a
month, so its expectancy is revised down and it is sized smaller. That is the
reactive gating already measured in `evolution.py`, and it lost money —
strategy performance mean-reverts, so demoting on a bad run sells the bottom.
Twenty live trades cannot retrain anything; the standard error on twenty
R-multiples is about half an R, which is wider than every edge in the book.

So live trades are not used to refit the model. They are used to **audit** it.
The backtest supplies the prior — what a cell is expected to return — and each
closed live trade tests whether reality still matches. Three outcomes:

  `tracking`     live results are consistent with the study. Nothing to do.
  `diverging`    live is materially below expectation, on enough trades to
                 say so. Flagged, sized down, still traded.
  `suspended`    the divergence has persisted across a larger sample. The cell
                 stops trading until the nightly rebuild either confirms the
                 edge is gone or shows it recovered.

The asymmetry is deliberate: a cell doing *better* than expected is never
promoted. Upside surprise on a small sample is the most seductive noise there
is, and acting on it is how a book ends up concentrated in whatever last got
lucky.

Nothing here fires on a handful of trades. `MIN_LIVE_TRADES` gates every
verdict, and below it the honest answer is "not yet known", which is printed
rather than hidden.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from typing import Mapping, Sequence

import numpy as np

logger = logging.getLogger(__name__)

# Below this, a cell's live record says nothing. Chosen from the arithmetic
# rather than taste: R-multiples here have a standard deviation near 2, so
# twenty trades give a standard error of ~0.45R — already wider than the
# largest cell edge in the book. Twenty-five is the least that can distinguish
# "this broke" from "this is Tuesday".
MIN_LIVE_TRADES = 25
# Suspension needs more than a flag does; it stops the cell trading.
MIN_TRADES_TO_SUSPEND = 40
# One-sided: only underperformance is actionable.
DIVERGENCE_P = 0.05
SUSPEND_P = 0.01
BOOTSTRAP = 2000
RNG_SEED = 20260920

STATUSES = ("tracking", "diverging", "suspended", "insufficient")

STATUS_NOTES = {
    "tracking": "Live results are consistent with the study. Trading normally.",
    "diverging": "Live results are materially below what the study predicted, on enough trades to be worth acting on. Size reduced, still trading.",
    "suspended": "The shortfall has persisted across a larger sample. Stood down until a rebuild says otherwise.",
    "insufficient": "Too few closed live trades to tell whether this still works.",
}

# Size multiplier applied to a cell's risk while it is flagged.
DIVERGING_SIZE_MULTIPLIER = 0.5


@dataclass
class CellCalibration:
    strategy: str
    regime: str
    expected_r: float
    live_trades: int
    live_avg_r: float
    shortfall_r: float          # expected minus live; positive means underperforming
    p_value: float              # P(live is at or above expectation)
    status: str
    size_multiplier: float
    note: str

    def to_dict(self) -> dict:
        out = asdict(self)
        out["status_note"] = STATUS_NOTES.get(self.status, "")
        return out


def _shortfall_p(live: np.ndarray, expected: float, iterations: int = BOOTSTRAP) -> float:
    """One-sided P(the true mean is at or above `expected`), by bootstrap.

    Bootstrap rather than a t-test for the reason given throughout this
    package: R-multiples are capped at -1R below and open-ended above, so the
    normal assumption is wrong in exactly the direction that manufactures
    confidence.
    """
    if len(live) < 2:
        return 1.0
    rng = np.random.default_rng(RNG_SEED)
    idx = rng.integers(0, len(live), size=(iterations, len(live)))
    means = live[idx].mean(axis=1)
    return float((means >= expected).mean())


def calibrate_cell(
    strategy: str,
    regime: str,
    expected_r: float,
    live_r: Sequence[float],
) -> CellCalibration:
    """Compare one cell's live record against what the study predicted."""
    values = np.asarray([float(r) for r in live_r], dtype=np.float64)
    count = int(len(values))
    average = float(values.mean()) if count else 0.0
    shortfall = expected_r - average

    if count < MIN_LIVE_TRADES:
        return CellCalibration(
            strategy=strategy, regime=regime, expected_r=round(expected_r, 3),
            live_trades=count, live_avg_r=round(average, 3),
            shortfall_r=round(shortfall, 3), p_value=1.0,
            status="insufficient", size_multiplier=1.0,
            note=(
                f"{count} closed live trades against a floor of {MIN_LIVE_TRADES}. "
                "Not enough to judge; trading continues at full size."
            ),
        )

    p_value = _shortfall_p(values, expected_r)

    # Only underperformance acts. Beating the study is never a promotion —
    # see the module docstring.
    if average >= expected_r:
        status, multiplier = "tracking", 1.0
        note = (
            f"{count} live trades at {average:+.2f}R against an expected {expected_r:+.2f}R. "
            "Running at or above the study."
        )
    elif p_value <= SUSPEND_P and count >= MIN_TRADES_TO_SUSPEND:
        status, multiplier = "suspended", 0.0
        note = (
            f"{count} live trades at {average:+.2f}R against an expected {expected_r:+.2f}R "
            f"(p={p_value:.3f}). The shortfall is large and has persisted; stood down."
        )
    elif p_value <= DIVERGENCE_P:
        status, multiplier = "diverging", DIVERGING_SIZE_MULTIPLIER
        note = (
            f"{count} live trades at {average:+.2f}R against an expected {expected_r:+.2f}R "
            f"(p={p_value:.3f}). Materially short; size halved while this is watched."
        )
    else:
        status, multiplier = "tracking", 1.0
        note = (
            f"{count} live trades at {average:+.2f}R against an expected {expected_r:+.2f}R "
            f"(p={p_value:.2f}). Below the study but within what {count} trades can show."
        )

    return CellCalibration(
        strategy=strategy, regime=regime, expected_r=round(expected_r, 3),
        live_trades=count, live_avg_r=round(average, 3),
        shortfall_r=round(shortfall, 3), p_value=round(p_value, 4),
        status=status, size_multiplier=multiplier, note=note,
    )


def calibrate(
    live_trades: Sequence[Mapping],
    playbooks: Sequence[Mapping],
) -> dict:
    """Audit every playbook cell against the live record."""
    expectations: dict[tuple[str, str], float] = {}
    for book in playbooks or []:
        for entry in book.get("entries") or []:
            expectations[(str(entry["strategy"]), str(book["regime"]))] = float(
                entry.get("out_sample_r") or 0.0
            )

    grouped: dict[tuple[str, str], list[float]] = {}
    for trade in live_trades:
        if trade.get("r_multiple") is None or not trade.get("exit_day"):
            continue
        key = (str(trade["strategy"]), str(trade["regime"]))
        grouped.setdefault(key, []).append(float(trade["r_multiple"]))

    cells = [
        calibrate_cell(strategy, regime, expected, grouped.get((strategy, regime), []))
        for (strategy, regime), expected in sorted(expectations.items())
    ]

    # Book-level read: does the whole thing behave as modelled? Carries more
    # weight than any single cell because it pools every live trade.
    all_live = np.asarray(
        [r for values in grouped.values() for r in values], dtype=np.float64
    )
    book_expected = (
        float(np.mean(list(expectations.values()))) if expectations else 0.0
    )
    if len(all_live) >= MIN_LIVE_TRADES:
        book_p = _shortfall_p(all_live, book_expected)
        book_status = "tracking" if float(all_live.mean()) >= book_expected or book_p > DIVERGENCE_P else "diverging"
        book_note = (
            f"{len(all_live)} closed live trades averaging {all_live.mean():+.2f}R against a "
            f"modelled {book_expected:+.2f}R (p={book_p:.2f})."
        )
    else:
        book_status, book_note = "insufficient", (
            f"{len(all_live)} closed live trades. The book needs {MIN_LIVE_TRADES} before its "
            "overall calibration means anything."
        )

    suspended = [c for c in cells if c.status == "suspended"]
    diverging = [c for c in cells if c.status == "diverging"]

    return {
        "cells": [c.to_dict() for c in cells],
        "suspended": [f"{c.strategy}/{c.regime}" for c in suspended],
        "diverging": [f"{c.strategy}/{c.regime}" for c in diverging],
        "book": {
            "status": book_status,
            "live_trades": int(len(all_live)),
            "live_avg_r": round(float(all_live.mean()), 3) if len(all_live) else 0.0,
            "expected_r": round(book_expected, 3),
            "note": book_note,
        },
        "method": (
            "Live trades audit the study rather than retrain it. Twenty R-multiples have a "
            "standard error near half an R, which is wider than any edge in the book, so "
            "refitting on them would be fitting noise — and demoting a strategy after a bad "
            "run sells the bottom, which is measurable in this system's own history. A cell is "
            "flagged only when it falls materially short on enough trades to say so, and a cell "
            "beating its expectation is never promoted."
        ),
    }


def size_multiplier_for(calibration: Mapping | None, strategy: str, regime: str) -> float:
    """What the live record says about sizing this cell right now."""
    if not calibration:
        return 1.0
    for cell in calibration.get("cells") or []:
        if cell.get("strategy") == strategy and cell.get("regime") == regime:
            return float(cell.get("size_multiplier", 1.0))
    return 1.0
