"""Per-symbol indicator bundle, computed once and shared by every strategy.

Ten strategies each recomputing a 200 DMA over 1,500 symbols is ten times the
work for the same numbers, and the run is long enough already. Everything here
is causal (see `indicators.py`) and every array is the same length as `bars`.

`liquid` deserves a note. A backtest that fills orders in stocks trading ₹5 lakh
a day is describing trades nobody could have made — the fill alone would move
the price several percent. So every strategy is gated on a rolling turnover
floor, and the floor is applied *as of the signal bar* rather than from today's
liquidity, because a stock that is liquid now was often not liquid in 2012.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from . import indicators as ind
from .history import Bars

# ₹2 crore of median daily turnover. Below this a swing position of any useful
# size is a meaningful share of the day's volume.
MIN_TURNOVER_CRORE = 2.0
TURNOVER_WINDOW = 20
MIN_PRICE = 20.0          # penny stocks have their own dynamics; out of scope


@dataclass
class Features:
    """Everything the strategies read, precomputed for one symbol."""

    bars: Bars
    ema10: np.ndarray
    ema21: np.ndarray
    sma50: np.ndarray
    sma150: np.ndarray
    sma200: np.ndarray
    atr14: np.ndarray
    atr_pct: np.ndarray            # ATR as % of price — volatility, comparable across names
    rsi14: np.ndarray
    high_10: np.ndarray
    high_20: np.ndarray
    high_52w: np.ndarray
    low_52w: np.ndarray
    low_20: np.ndarray
    vol_sma20: np.ndarray
    vol_sma50: np.ndarray
    rel_volume: np.ndarray         # today's volume / 50-day average
    turnover_crore: np.ndarray     # rolling median daily turnover
    ret_21: np.ndarray
    ret_63: np.ndarray
    ret_126: np.ndarray
    ret_252: np.ndarray
    range_pct_20: np.ndarray       # 20-bar high-low range as % — tightness
    range_pct_10: np.ndarray
    dist_52w_high: np.ndarray      # % below the 52-week high
    liquid: np.ndarray             # bool: tradeable at this bar
    ma200_slope: np.ndarray
    # Relative strength against the benchmark, as a ratio line. The *slope* of
    # this line is what matters: a stock can be far above the index and rolling
    # over, or below it and taking the lead, and only the slope distinguishes
    # them. Absolute RS level is a backward-looking fact; its direction is the
    # tradeable one.
    rs_line: np.ndarray
    rs_slope_63: np.ndarray
    rs_at_high_63: np.ndarray      # bool: RS line at a 63-bar high
    range_pct_5: np.ndarray        # 5-bar range — compression at the entry bar
    low_10: np.ndarray

    @property
    def n(self) -> int:
        return len(self.bars)


def build_features(bars: Bars, benchmark_close: np.ndarray | None = None) -> Features | None:
    """Compute the bundle, or None when the symbol is too short to be useful.

    `benchmark_close` must already be aligned to `bars.dates` by the caller —
    aligning here would mean re-doing the same date join for every symbol, and
    a mis-aligned benchmark silently turns relative strength into noise rather
    than failing.
    """
    n = len(bars)
    if n < 260:
        return None

    close, high, low, volume = bars.close, bars.high, bars.low, bars.volume

    atr14 = ind.atr(high, low, close, 14)
    with np.errstate(divide="ignore", invalid="ignore"):
        atr_pct = np.where(close > 0, atr14 / close * 100.0, np.nan)

    vol_sma50 = ind.sma(volume, 50)
    with np.errstate(divide="ignore", invalid="ignore"):
        rel_volume = np.where(vol_sma50 > 0, volume / vol_sma50, np.nan)

    # Turnover in ₹ crore: price x shares / 1e7. Median rather than mean so one
    # block deal does not mark an illiquid stock tradeable for a month.
    turnover = close * volume / 1e7
    turnover_med = np.full(n, np.nan)
    if n >= TURNOVER_WINDOW:
        strided = np.lib.stride_tricks.sliding_window_view(turnover, TURNOVER_WINDOW)
        turnover_med[TURNOVER_WINDOW - 1:] = np.median(strided, axis=1)

    high_20 = ind.rolling_max(high, 20)
    low_20 = ind.rolling_min(low, 20)
    high_52w = ind.rolling_max(high, 252)
    low_52w = ind.rolling_min(low, 252)

    high_10 = ind.rolling_max(high, 10)
    low_10 = ind.rolling_min(low, 10)
    with np.errstate(divide="ignore", invalid="ignore"):
        dist_52w_high = np.where(high_52w > 0, (close - high_52w) / high_52w * 100.0, np.nan)
        range_pct_20 = np.where(low_20 > 0, (high_20 - low_20) / low_20 * 100.0, np.nan)
        range_pct_10 = np.where(low_10 > 0, (high_10 - low_10) / low_10 * 100.0, np.nan)

    sma200 = ind.sma(close, 200)

    if benchmark_close is not None and len(benchmark_close) == n:
        with np.errstate(divide="ignore", invalid="ignore"):
            rs_line = np.where(benchmark_close > 0, close / benchmark_close, np.nan)
        rs_slope_63 = ind.slope_pct_per_bar(np.nan_to_num(rs_line, nan=0.0), 63)
        rs_high = ind.rolling_max(np.nan_to_num(rs_line, nan=0.0), 63)
        rs_at_high = np.isfinite(rs_line) & np.isfinite(rs_high) & (rs_line >= rs_high * 0.999)
    else:
        rs_line = np.full(n, np.nan)
        rs_slope_63 = np.full(n, np.nan)
        rs_at_high = np.zeros(n, dtype=bool)

    high_5 = ind.rolling_max(high, 5)
    low_5 = ind.rolling_min(low, 5)
    with np.errstate(divide="ignore", invalid="ignore"):
        range_pct_5 = np.where(low_5 > 0, (high_5 - low_5) / low_5 * 100.0, np.nan)

    liquid = (
        np.nan_to_num(turnover_med, nan=0.0) >= MIN_TURNOVER_CRORE
    ) & (close >= MIN_PRICE)

    return Features(
        bars=bars,
        ema10=ind.ema(close, 10),
        ema21=ind.ema(close, 21),
        sma50=ind.sma(close, 50),
        sma150=ind.sma(close, 150),
        sma200=sma200,
        atr14=atr14,
        atr_pct=atr_pct,
        rsi14=ind.rsi(close, 14),
        high_10=high_10,
        high_20=high_20,
        high_52w=high_52w,
        low_52w=low_52w,
        low_20=low_20,
        vol_sma20=ind.sma(volume, 20),
        vol_sma50=vol_sma50,
        rel_volume=rel_volume,
        turnover_crore=turnover_med,
        ret_21=ind.pct_change(close, 21),
        ret_63=ind.pct_change(close, 63),
        ret_126=ind.pct_change(close, 126),
        ret_252=ind.pct_change(close, 252),
        range_pct_20=range_pct_20,
        range_pct_10=range_pct_10,
        dist_52w_high=dist_52w_high,
        liquid=liquid,
        ma200_slope=ind.slope_pct_per_bar(sma200, 40),
        rs_line=rs_line,
        rs_slope_63=rs_slope_63,
        rs_at_high_63=rs_at_high,
        range_pct_5=range_pct_5,
        low_10=low_10,
    )
