"""Market breadth computed from the universe's own bars, session by session.

Breadth is the part of a regime read that an index cannot give you. The Nifty
can make a new high on five stocks; whether the other 1,500 are participating
is a different fact, and it is the one that decides whether a breakout strategy
gets paid. So every number here is counted across the whole stored universe
rather than read off a cap-weighted index.

Two honesty constraints are wired in rather than left to the reader:

*Constituent count ships with every row.* The store holds today's universe, so
a session in 2009 is measured over the few hundred names that both existed then
and still exist now, while 2025 is measured over ~1,500. A breadth percentage
means something different at those two sample sizes, and `constituents` is what
lets the regime engine refuse rows that are too thin (see `MIN_CONSTITUENTS`).

*Survivorship is acknowledged, not corrected.* Companies that delisted are
absent, which flatters historical breadth — the losers were removed from the
sample. It cannot be fixed without point-in-time constituent lists that the
free sources do not publish, so it is measured (`survivorship.py`) and stated
in the UI instead of being quietly ignored.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path

import numpy as np

from . import indicators as ind
from .history import Bars, iter_bars

logger = logging.getLogger(__name__)

# A breadth percentage over fewer names than this is a statement about a handful
# of stocks, not about the market. Rows below it are computed but flagged.
MIN_CONSTITUENTS = 120

LOOKBACK_52W = 252
MA_FAST = 50
MA_SLOW = 200


@dataclass
class BreadthRow:
    """Breadth for one session. Percentages are of `constituents`."""

    day: date
    constituents: int
    pct_above_50dma: float
    pct_above_200dma: float
    pct_at_52w_high: float
    pct_at_52w_low: float
    advancers_pct: float
    median_20d_return: float
    pct_up_25pct_3m: float

    @property
    def thin(self) -> bool:
        return self.constituents < MIN_CONSTITUENTS

    @property
    def net_new_highs_pct(self) -> float:
        """New highs minus new lows — the classic single-number breadth read."""
        return round(self.pct_at_52w_high - self.pct_at_52w_low, 2)

    def to_dict(self) -> dict:
        out = asdict(self)
        out["day"] = self.day.isoformat()
        out["net_new_highs_pct"] = self.net_new_highs_pct
        out["thin"] = self.thin
        return out


@dataclass
class _Accumulator:
    """Per-session running totals across symbols."""

    constituents: np.ndarray
    above_50: np.ndarray
    above_200: np.ndarray
    at_high: np.ndarray
    at_low: np.ndarray
    advancers: np.ndarray
    advance_decline_base: np.ndarray
    up_25_3m: np.ndarray
    # Returns are collected per session to take a median, which a running sum
    # cannot give. Lists of floats keep this to a few hundred MB at 4,600
    # sessions x ~1,500 symbols.
    returns_20d: list[list[float]]


def _new_accumulator(n_sessions: int) -> _Accumulator:
    zeros = lambda: np.zeros(n_sessions, dtype=np.int64)  # noqa: E731
    return _Accumulator(
        constituents=zeros(),
        above_50=zeros(),
        above_200=zeros(),
        at_high=zeros(),
        at_low=zeros(),
        advancers=zeros(),
        advance_decline_base=zeros(),
        up_25_3m=zeros(),
        returns_20d=[[] for _ in range(n_sessions)],
    )


def _fold_symbol(acc: _Accumulator, bars: Bars, position_of: dict[date, int]) -> None:
    """Add one symbol's contribution to every session it traded."""
    close = bars.close
    n = len(close)
    if n < MA_FAST + 2:
        return

    ma50 = ind.sma(close, MA_FAST)
    ma200 = ind.sma(close, MA_SLOW)
    high_52 = ind.rolling_max(bars.high, LOOKBACK_52W)
    low_52 = ind.rolling_min(bars.low, LOOKBACK_52W)
    ret_20 = ind.pct_change(close, 20)
    ret_63 = ind.pct_change(close, 63)
    prev_close = np.empty(n, dtype=np.float64)
    prev_close[0] = np.nan
    prev_close[1:] = close[:-1]

    for i in range(n):
        slot = position_of.get(bars.dates[i])
        if slot is None:
            # The symbol traded on a session the benchmark did not. Rare, and
            # counting it would put a stock into a session that has no index
            # bar to anchor it, so it is dropped.
            continue
        acc.constituents[slot] += 1

        price = close[i]
        if np.isfinite(ma50[i]):
            acc.above_50[slot] += int(price > ma50[i])
        if np.isfinite(ma200[i]):
            acc.above_200[slot] += int(price > ma200[i])
        # "At a 52-week high" means the session's own high set it — using the
        # close understates new highs by roughly a third.
        if np.isfinite(high_52[i]) and bars.high[i] >= high_52[i]:
            acc.at_high[slot] += 1
        if np.isfinite(low_52[i]) and bars.low[i] <= low_52[i]:
            acc.at_low[slot] += 1
        if np.isfinite(prev_close[i]) and prev_close[i] > 0:
            acc.advance_decline_base[slot] += 1
            acc.advancers[slot] += int(price > prev_close[i])
        if np.isfinite(ret_20[i]):
            acc.returns_20d[slot].append(float(ret_20[i]))
        if np.isfinite(ret_63[i]):
            acc.up_25_3m[slot] += int(ret_63[i] >= 25.0)


def _pct(part: np.ndarray, whole: np.ndarray, slot: int) -> float:
    denom = int(whole[slot])
    return round(100.0 * int(part[slot]) / denom, 2) if denom else 0.0


def build_breadth(
    data_dir: Path,
    sessions: list[date],
    symbols: list[str] | None = None,
) -> list[BreadthRow]:
    """Breadth for each session in `sessions` (the benchmark's calendar).

    `sessions` must be the benchmark trading calendar rather than the union of
    every symbol's dates: the union includes vendor-glitch dates on which a
    single illiquid name printed a bar, and a breadth row built from one stock
    is worse than no row at all.
    """
    position_of = {day: i for i, day in enumerate(sessions)}
    acc = _new_accumulator(len(sessions))

    folded = 0
    for bars in iter_bars(data_dir, symbols):
        _fold_symbol(acc, bars, position_of)
        folded += 1
        if folded % 250 == 0:
            logger.info("breadth: folded %d symbols", folded)
    logger.info("breadth: folded %d symbols over %d sessions", folded, len(sessions))

    rows: list[BreadthRow] = []
    for slot, day in enumerate(sessions):
        count = int(acc.constituents[slot])
        if count == 0:
            continue
        window = acc.returns_20d[slot]
        rows.append(
            BreadthRow(
                day=day,
                constituents=count,
                pct_above_50dma=_pct(acc.above_50, acc.constituents, slot),
                pct_above_200dma=_pct(acc.above_200, acc.constituents, slot),
                pct_at_52w_high=_pct(acc.at_high, acc.constituents, slot),
                pct_at_52w_low=_pct(acc.at_low, acc.constituents, slot),
                advancers_pct=_pct(acc.advancers, acc.advance_decline_base, slot),
                median_20d_return=round(float(np.median(window)), 2) if window else 0.0,
                pct_up_25pct_3m=_pct(acc.up_25_3m, acc.constituents, slot),
            )
        )
    return rows
