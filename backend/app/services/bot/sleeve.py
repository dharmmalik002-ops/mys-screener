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


SLEEVE_MODE = __import__("os").environ.get("BOT_SLEEVE_MODE", "default")


def risk_on_days(
    index_close: Mapping[date, float],
    regime_by_day: Mapping[date, str],
    mode: str | None = None,
) -> tuple[set, set]:
    """(sleeve risk-on, book risk-on) — deliberately different conditions.

    The sleeve holds equities while the regime is healthy OR the index is
    above its 200-DMA; the book stays invested only while the regime is
    healthy. Each is read at a close and acts on the NEXT session.

    The thrust leg (gotchas 96/97) is gone: it was chosen while the sleeve
    read same-day state, so it was credited with the very rebound that
    triggered it. With the one-session lag it added nothing and deepened the
    drawdown (gotcha 115).
    """
    days = sorted(index_close)
    closes = np.asarray([index_close[d] for d in days], dtype=float)
    s200 = ind.sma(closes, 200)
    above = {d: (bool(closes[i] > s200[i]) if not np.isnan(s200[i]) else True)
             for i, d in enumerate(days)}
    if (mode or SLEEVE_MODE) == "bear_only":
        # Defend only in a genuine bear: gold and a flat stock book while the
        # regime reads `bear`, fully invested through choppy and correction
        # tape. Measured as the higher-return profile (gotcha 116) at a lower
        # win rate; opt in with BOT_SLEEVE_MODE=bear_only.
        on = {d for d in days if regime_by_day.get(d) != "bear"}
        return on, set(on)
    healthy = {d for d in days if regime_by_day.get(d) in HEALTHY_REGIMES}
    sleeve_on = {d for d in days if d in healthy or above.get(d, True)}
    book_on = set(healthy)
    return sleeve_on, book_on


def clean_series(prices: Mapping[date, float], max_jump: float = 0.5) -> dict:
    """Drop prints that move more than `max_jump` from the last kept price.

    Yahoo's GOLDBEES history reads 0.3355 on 2019-12-19 and 33.65 on
    2019-12-23 — a -99% / +9900% pair around a real ~33 price. With the
    sleeve's state lagged a session, a switch landing between the two would
    book one leg of it. A dropped day carries the previous price instead.
    """
    out: dict = {}
    last = None
    for d in sorted(prices):
        px = prices[d]
        if not px or px <= 0:
            continue
        if last is not None and abs(px / last - 1.0) > max_jump:
            continue
        out[d] = px
        last = px
    return out


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
    # State is read on index sessions only and carried across days the index
    # does not print (a gold-only print must not flip the sleeve into gold).
    # Day d's return belongs to the state held going INTO d — what the close
    # before it chose (gotcha 115).
    held_on, held_rec = True, False
    cur_on, cur_rec = True, False
    for d in days:
        i = index_close.get(d, prev_i)
        g = gold_close.get(d, prev_g)
        s = small.get(d, prev_s)
        held_on, held_rec = cur_on, cur_rec
        if held_on:
            # Equity leg: small caps while a crash recovery is live, broad
            # index otherwise.
            if held_rec and prev_s and s:
                level *= s / prev_s
            elif prev_i and i:
                level *= i / prev_i
        elif prev_g and g:
            level *= g / prev_g
        if d in index_close:
            cur_on, cur_rec = d in sleeve_on, d in recovering
        # A missing price is not a zero price — carry the last one forward.
        prev_i, prev_g, prev_s = i or prev_i, g or prev_g, s or prev_s
        out[d] = level
    return out


def book_regime(days, index_days, book_on: set) -> dict:
    """Per-day label the account's de-risk reads, carried across non-index days."""
    out, cur = {}, True
    for d in sorted(days):
        if d in index_days:
            cur = d in book_on
        out[d] = "bull_strong" if cur else "bear"
    return out
