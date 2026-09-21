"""Causal technical indicators over numpy arrays.

Every function here returns an array the same length as its input where element
`i` is computed from elements `0..i` and **never** from `i+1` onwards. That is
the whole contract, and it is the one that decides whether a backtest is
research or fiction: a single centred rolling window or a forward-filled gap
turns tomorrow's price into today's signal, and the equity curve that comes out
is beautiful and completely unreachable.

Warm-up positions — where there is not yet enough history — are `np.nan`, not
zero and not the first valid value. Downstream code treats nan as "no opinion"
and refuses the signal, which is the honest reading. `test_no_lookahead` in
`tests/test_bot_indicators.py` checks the contract by recomputing each
indicator on truncated inputs and asserting the values do not move.
"""

from __future__ import annotations

import numpy as np


def _empty_like(values: np.ndarray) -> np.ndarray:
    return np.full(len(values), np.nan, dtype=np.float64)


def sma(values: np.ndarray, window: int) -> np.ndarray:
    """Simple moving average; positions before `window-1` are nan."""
    n = len(values)
    out = _empty_like(values)
    if window <= 0 or n < window:
        return out
    # Cumulative-sum differencing is O(n) rather than O(n*window). float64
    # throughout because the cumulative sum of 30 years of prices loses
    # meaningful precision in float32.
    cumsum = np.cumsum(np.insert(values.astype(np.float64), 0, 0.0))
    out[window - 1:] = (cumsum[window:] - cumsum[:-window]) / window
    return out


def ema(values: np.ndarray, window: int) -> np.ndarray:
    """Exponential moving average seeded with the first `window` SMA.

    Seeding with the SMA rather than the first price keeps the series from
    carrying a large startup bias forward for hundreds of bars.
    """
    n = len(values)
    out = _empty_like(values)
    if window <= 0 or n < window:
        return out
    alpha = 2.0 / (window + 1.0)
    acc = float(np.mean(values[:window]))
    out[window - 1] = acc
    for i in range(window, n):
        acc = alpha * float(values[i]) + (1.0 - alpha) * acc
        out[i] = acc
    return out


def rolling_max(values: np.ndarray, window: int) -> np.ndarray:
    """Highest value over the trailing `window` bars, inclusive of the current."""
    n = len(values)
    out = _empty_like(values)
    if window <= 0 or n < window:
        return out
    strided = np.lib.stride_tricks.sliding_window_view(values, window)
    out[window - 1:] = strided.max(axis=1)
    return out


def rolling_min(values: np.ndarray, window: int) -> np.ndarray:
    n = len(values)
    out = _empty_like(values)
    if window <= 0 or n < window:
        return out
    strided = np.lib.stride_tricks.sliding_window_view(values, window)
    out[window - 1:] = strided.min(axis=1)
    return out


def rolling_std(values: np.ndarray, window: int) -> np.ndarray:
    n = len(values)
    out = _empty_like(values)
    if window <= 0 or n < window:
        return out
    strided = np.lib.stride_tricks.sliding_window_view(values, window)
    out[window - 1:] = strided.std(axis=1, ddof=0)
    return out


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    """Wilder's true range. The first bar has no prior close, so it is nan."""
    n = len(close)
    out = _empty_like(close)
    if n < 2:
        return out
    prev_close = close[:-1]
    out[1:] = np.maximum.reduce(
        [
            high[1:] - low[1:],
            np.abs(high[1:] - prev_close),
            np.abs(low[1:] - prev_close),
        ]
    )
    return out


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, window: int = 14) -> np.ndarray:
    """Average true range, Wilder-smoothed."""
    tr = true_range(high, low, close)
    n = len(close)
    out = _empty_like(close)
    if n < window + 1:
        return out
    seed = float(np.mean(tr[1: window + 1]))
    out[window] = seed
    acc = seed
    for i in range(window + 1, n):
        acc = (acc * (window - 1) + float(tr[i])) / window
        out[i] = acc
    return out


def rsi(close: np.ndarray, window: int = 14) -> np.ndarray:
    """Wilder's RSI."""
    n = len(close)
    out = _empty_like(close)
    if n < window + 1:
        return out
    delta = np.diff(close)
    gains = np.clip(delta, 0.0, None)
    losses = np.clip(-delta, 0.0, None)
    avg_gain = float(np.mean(gains[:window]))
    avg_loss = float(np.mean(losses[:window]))

    def value(g: float, l: float) -> float:
        if l <= 0:
            return 100.0 if g > 0 else 50.0
        rs = g / l
        return 100.0 - (100.0 / (1.0 + rs))

    out[window] = value(avg_gain, avg_loss)
    for i in range(window + 1, n):
        avg_gain = (avg_gain * (window - 1) + float(gains[i - 1])) / window
        avg_loss = (avg_loss * (window - 1) + float(losses[i - 1])) / window
        out[i] = value(avg_gain, avg_loss)
    return out


def pct_change(values: np.ndarray, periods: int) -> np.ndarray:
    """Percent change over `periods` bars back, as a percentage."""
    n = len(values)
    out = _empty_like(values)
    if periods <= 0 or n <= periods:
        return out
    prior = values[:-periods]
    with np.errstate(divide="ignore", invalid="ignore"):
        out[periods:] = np.where(prior > 0, (values[periods:] - prior) / prior * 100.0, np.nan)
    return out


def slope_pct_per_bar(values: np.ndarray, window: int) -> np.ndarray:
    """Least-squares slope over the trailing window, as % of the window mean.

    Normalising by the mean makes the number comparable across a ₹50 stock and
    a ₹5,000 one, which a raw slope is not.
    """
    n = len(values)
    out = _empty_like(values)
    if window < 2 or n < window:
        return out
    x = np.arange(window, dtype=np.float64)
    x_centered = x - x.mean()
    denom = float(np.sum(x_centered ** 2))
    strided = np.lib.stride_tricks.sliding_window_view(values, window)
    means = strided.mean(axis=1)
    slopes = (strided - means[:, None]) @ x_centered / denom
    with np.errstate(divide="ignore", invalid="ignore"):
        out[window - 1:] = np.where(means > 0, slopes / means * 100.0, np.nan)
    return out


def drawdown_pct(values: np.ndarray) -> np.ndarray:
    """Percent below the running peak so far — causal by construction."""
    n = len(values)
    if n == 0:
        return _empty_like(values)
    peak = np.maximum.accumulate(values)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(peak > 0, (values - peak) / peak * 100.0, np.nan)


def distance_pct(values: np.ndarray, reference: np.ndarray) -> np.ndarray:
    """How far `values` sits above `reference`, in percent."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(
            np.isfinite(reference) & (reference > 0),
            (values - reference) / reference * 100.0,
            np.nan,
        )


def rolling_percentile_rank(values: np.ndarray, window: int) -> np.ndarray:
    """Where the current value sits inside its own trailing window, 0-100.

    Used for regime work: "India VIX at 18" means nothing on its own, but "VIX
    in the 92nd percentile of the last two years" is a statement about the tape.
    """
    n = len(values)
    out = _empty_like(values)
    if window < 2 or n < window:
        return out
    strided = np.lib.stride_tricks.sliding_window_view(values, window)
    current = values[window - 1:]
    # Fraction of the window at or below the current value, current included.
    out[window - 1:] = (strided <= current[:, None]).sum(axis=1) / window * 100.0
    return out


def consecutive_true(flags: np.ndarray) -> np.ndarray:
    """Run length of consecutive True values ending at each position."""
    out = np.zeros(len(flags), dtype=np.int64)
    run = 0
    for i, flag in enumerate(flags):
        run = run + 1 if flag else 0
        out[i] = run
    return out
