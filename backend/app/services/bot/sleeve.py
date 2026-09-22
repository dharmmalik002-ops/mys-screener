"""The idle-capital sleeve, built once and shared by the backtest and the book.

The account is two things: a selective stock book, and a sleeve holding
whatever capital the stock side is not using. The sleeve is not a detail — on
the yearly-rebuild test it returns +31.63% a year on its own against the full
book's +38.85%, so a paper book that leaves idle cash in cash is paper-trading
a different strategy and would understate the study by most of its return.

That is exactly what happened: the first paper replay returned +5.87% over
2.7 years because it had no sleeve. This module exists so the two paths cannot
drift again — `run_robust_backtest.py` and `run_paper_session.py` both build
the series here rather than each rolling their own.

What it holds, in order of precedence:

  * **gold** when the market is risk-off — a healthy regime has failed AND the
    index is below its 200 DMA AND there has been no follow-through thrust;
  * **small caps** while recovering from a crash (the index has been 20% below
    its 52-week high within the last year and has climbed back above that);
  * **the broad index** otherwise.

It is built as a **compounded level from chained daily returns**, never by
switching between two price maps: the sleeve holds units, and swapping a
~25,000-level index series for a ~70-level gold series would reprice those
units overnight by a factor of 350 (gotcha 89).
"""

from __future__ import annotations

from datetime import date
from typing import Mapping

import numpy as np

from . import indicators as ind
from . import rules as R

HEALTHY_REGIMES = frozenset({"bull_strong", "bull_narrow", "recovery"})


def risk_on_days(
    index_close: Mapping[date, float],
    regime_by_day: Mapping[date, str],
) -> tuple[set, set]:
    """(sleeve risk-on, book risk-on) — deliberately different conditions.

    The sleeve re-enters on a healthy regime OR an intact 200-DMA trend OR a
    thrust: it should be in the market whenever the market is worth being in.
    The book re-enters on regime OR thrust only, without the trend leg —
    individual stocks need more than an index that has not broken yet
    (gotcha 97). Coupling them has cost real money twice.
    """
    days = sorted(index_close)
    closes = np.asarray([index_close[d] for d in days], dtype=float)
    s200 = ind.sma(closes, 200)
    above = {d: (bool(closes[i] > s200[i]) if not np.isnan(s200[i]) else True)
             for i, d in enumerate(days)}
    thrust = R.thrust_days(days, [float(c) for c in closes])
    healthy = {d for d in days if regime_by_day.get(d) in HEALTHY_REGIMES}
    sleeve_on = {d for d in days if d in healthy or above.get(d, True) or d in thrust}
    book_on = {d for d in days if d in healthy or d in thrust}
    return sleeve_on, book_on


def recovery_days(index_close: Mapping[date, float]) -> set:
    days = sorted(index_close)
    return R.recovery_days(days, [float(index_close[d]) for d in days])


def build_level(
    index_close: Mapping[date, float],
    gold_close: Mapping[date, float],
    smallcap_close: Mapping[date, float] | None,
    sleeve_on: set,
) -> dict:
    """A single compounded level series the book can hold units of."""
    small = smallcap_close or {}
    recovering = recovery_days(index_close) if small else set()
    days = sorted(set(index_close) | set(gold_close) | set(small))
    level, out = 100.0, {}
    prev_i = prev_g = prev_s = None
    for d in days:
        i = index_close.get(d, prev_i)
        g = gold_close.get(d, prev_g)
        s = small.get(d, prev_s)
        if d in sleeve_on:
            # Equity leg: small caps while a crash recovery is live, broad
            # index otherwise.
            if d in recovering and prev_s and s:
                level *= s / prev_s
            elif prev_i and i:
                level *= i / prev_i
        elif prev_g and g:
            level *= g / prev_g
        # A missing price is not a zero price — carry the last one forward.
        prev_i, prev_g, prev_s = i or prev_i, g or prev_g, s or prev_s
        out[d] = level
    return out
