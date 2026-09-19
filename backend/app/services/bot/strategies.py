"""The strategy library: ten setups, each a causal signal generator over bars.

Every strategy declares `expects` — the regimes it is *supposed* to work in —
before anything is measured. That declaration is the point. Without it, a
backtest that finds mean reversion paying in corrections is indistinguishable
from a backtest that found it paying in corrections by chance and then had a
story written around it. With it, the attribution table either confirms a prior
or refutes it, and a refuted prior is information rather than embarrassment.

The library is deliberately mixed. Breakout and momentum setups that need a
trending tape sit beside mean-reversion and low-volatility setups that do not,
because a study where every strategy wants the same conditions can only ever
discover "trade more in bull markets". The interesting question is whether
anything gets paid when the Nifty is not going up, and there have to be
candidates in the running for that question to have an answer.

Signals are emitted on the bar the condition completes; the engine fills at the
*next* bar's open (see `engine.py`). Nothing here reads a future bar.

These are independent reimplementations of the app's scanners rather than calls
into `scanners/definitions.py`: the live scanners take a rebuilt `StockSnapshot`
per symbol-date at ~16 ms, which is fine for a 12-week window and is 35 hours
for 19 years. `scripts/crosscheck_strategies.py` replays both over the
overlapping window and reports the agreement rate, so the shortcut stays honest.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np

from .features import Features

# Families group strategies that share a failure mode — when a whole family
# stops working at once that is a market fact, not ten independent findings.
FAMILIES = ("breakout", "momentum", "pullback", "mean_reversion", "volatility")


@dataclass(frozen=True)
class StrategySpec:
    id: str
    label: str
    family: str
    thesis: str
    expects: tuple[str, ...]      # regimes where this is predicted to work
    generate: Callable[[Features], np.ndarray]
    # Initial stop, as a multiple of ATR(14) below the entry. Per-strategy
    # because a tight-flag entry and a pullback entry have different natural
    # invalidation points; a single global stop would flatter one and strangle
    # the other.
    stop_atr_mult: float = 2.0


def _safe(arr: np.ndarray) -> np.ndarray:
    """nan -> False for boolean composition."""
    return np.nan_to_num(arr, nan=0.0).astype(bool)


def _prior(arr: np.ndarray, k: int = 1) -> np.ndarray:
    """Shift an array forward by k bars — `_prior(x)[i]` is `x[i-k]`.

    Every 'yesterday' in this file goes through here rather than through manual
    slicing, because an off-by-one in the wrong direction is exactly how a
    backtest reads tomorrow's bar and nobody notices.
    """
    out = np.full(len(arr), np.nan, dtype=np.float64)
    if k < len(arr):
        out[k:] = arr[:-k]
    return out


def _prior_flag(flags: np.ndarray, k: int = 1) -> np.ndarray:
    """`_prior` for boolean gates, keeping the dtype boolean.

    The float version fills the shifted head with nan, and `bool_array &
    float_array` is a TypeError rather than a silent wrong answer — which is
    how four strategies were caught doing it. Warm-up bars fill False: "we did
    not see the condition yesterday", which is the correct reading of having no
    yesterday to look at.
    """
    out = np.zeros(len(flags), dtype=bool)
    if k < len(flags):
        out[k:] = flags[:-k].astype(bool)
    return out


def _trend_template(f: Features) -> np.ndarray:
    """Minervini's trend template — the stage-2 filter several setups share."""
    c = f.bars.close
    return (
        (c > f.sma50)
        & (f.sma50 > f.sma150)
        & (f.sma150 > f.sma200)
        & (f.ma200_slope > 0)
        & (c > f.low_52w * 1.30)
        & (f.dist_52w_high > -25.0)
    )


# --- The strategies --------------------------------------------------------


def _minervini_breakout(f: Features) -> np.ndarray:
    """Stage-2 leader clearing a 20-day high on expanding volume."""
    c = f.bars.close
    breakout = (c > _prior(f.high_20)) & (f.rel_volume > 1.4)
    return _safe(_trend_template(f) & breakout & f.liquid)


def _vcp_breakout(f: Features) -> np.ndarray:
    """Volatility contraction, then a break out of it.

    The contraction is the signal: the 10-bar range must be meaningfully
    tighter than the 20-bar range (price coiling), volume must have dried up,
    and then price clears the prior 20-day high.
    """
    c = f.bars.close
    contraction = (
        (f.range_pct_10 < f.range_pct_20 * 0.6)
        & (f.range_pct_10 < 12.0)
        & (_prior(f.rel_volume) < 0.9)
    )
    trigger = (c > _prior(f.high_20)) & (f.rel_volume > 1.5)
    return _safe(_trend_template(f) & _prior_flag(contraction) & trigger & f.liquid)


def _fifty_two_week_breakout(f: Features) -> np.ndarray:
    """New 52-week high with volume confirmation."""
    c = f.bars.close
    return _safe(
        (c > _prior(f.high_52w))
        & (f.rel_volume > 1.5)
        & (c > f.sma200)
        & (f.ma200_slope > 0)
        & f.liquid
    )


def _momentum_burst(f: Features) -> np.ndarray:
    """A sharp multi-day thrust out of quiet — short-horizon continuation."""
    c = f.bars.close
    thrust = (f.ret_21 > 12.0) & (c > f.ema10) & (f.rel_volume > 1.8)
    quiet_before = _prior(f.range_pct_10, 21) < 18.0
    return _safe(thrust & quiet_before & (c > f.sma50) & f.liquid)


def _high_tight_flag(f: Features) -> np.ndarray:
    """Doubled in a quarter, then went quiet without giving much back."""
    c = f.bars.close
    ran_hard = f.ret_63 > 70.0
    tight = (f.range_pct_10 < 15.0) & (f.dist_52w_high > -18.0)
    trigger = c > _prior(f.high_10)
    return _safe(ran_hard & tight & trigger & (f.rel_volume > 1.3) & f.liquid)


def _pullback_to_ema21(f: Features) -> np.ndarray:
    """Uptrend pulls back to the 21 EMA and turns up — buying strength cheaply."""
    c, low = f.bars.close, f.bars.low
    touched = (low <= f.ema21 * 1.02) & (_prior(low) <= _prior(f.ema21) * 1.03)
    turning = (c > _prior(c)) & (c > f.ema21)
    return _safe(_trend_template(f) & _prior_flag(touched) & turning & f.liquid)


def _pullback_to_sma50(f: Features) -> np.ndarray:
    """The deeper, slower cousin — a full reset to the 50 DMA in a live uptrend."""
    c, low = f.bars.close, f.bars.low
    reached = low <= f.sma50 * 1.03
    reclaim = (c > f.sma50) & (c > _prior(c))
    return _safe(
        (f.sma50 > f.sma200) & (f.ma200_slope > 0) & _prior_flag(reached) & reclaim & f.liquid
    )


def _oversold_bounce(f: Features) -> np.ndarray:
    """RSI washout inside a long-term uptrend — the mean-reversion candidate.

    Deliberately does NOT require a short-term uptrend: the whole premise is
    buying when the short term looks bad and the long term does not. Predicted
    to pay in corrections and chop, and to be a trap in a real bear.
    """
    c = f.bars.close
    washout = (_prior(f.rsi14) < 30.0) & (f.rsi14 > _prior(f.rsi14))
    long_term_ok = (c > f.sma200) & (f.ma200_slope > -0.02)
    return _safe(washout & long_term_ok & (c > _prior(c)) & f.liquid)


def _squeeze_release(f: Features) -> np.ndarray:
    """Volatility compresses to a multi-month low, then price picks a side.

    Regime-agnostic by design: a squeeze resolves in trends and in ranges, and
    it is the one setup with no directional prior about the index.
    """
    c = f.bars.close
    squeeze = f.atr_pct < 0.6 * np.nan_to_num(_prior(f.atr_pct, 60), nan=np.inf)
    release = (c > _prior(f.high_20)) & (f.rel_volume > 1.6)
    return _safe(_prior_flag(squeeze) & release & (c > f.sma200) & f.liquid)


def _gap_continuation(f: Features) -> np.ndarray:
    """Gaps up out of a base on heavy volume and holds the gap."""
    o, c = f.bars.open, f.bars.close
    gap = o > _prior(c) * 1.03
    held = (c >= o) & (f.rel_volume > 2.0)
    return _safe(gap & held & (c > f.sma50) & (c > f.sma200) & f.liquid)


STRATEGIES: tuple[StrategySpec, ...] = (
    StrategySpec(
        "minervini_breakout", "Minervini Breakout", "breakout",
        "A stage-2 leader clearing a 20-day high on expanding volume.",
        ("bull_strong", "bull_narrow"), _minervini_breakout, stop_atr_mult=2.0,
    ),
    StrategySpec(
        "vcp_breakout", "VCP Breakout", "breakout",
        "Volatility contracts to a coil inside an uptrend, then price breaks out of it.",
        ("bull_strong",), _vcp_breakout, stop_atr_mult=1.8,
    ),
    StrategySpec(
        "week52_breakout", "52-Week High Breakout", "breakout",
        "New yearly high on volume — the crudest and most-studied momentum edge.",
        ("bull_strong", "bull_narrow", "recovery"), _fifty_two_week_breakout, stop_atr_mult=2.2,
    ),
    StrategySpec(
        "momentum_burst", "Momentum Burst", "momentum",
        "A sharp thrust out of a quiet stretch; rides short-horizon continuation.",
        ("bull_strong", "recovery"), _momentum_burst, stop_atr_mult=2.5,
    ),
    StrategySpec(
        "high_tight_flag", "High Tight Flag", "momentum",
        "Doubled in a quarter, then consolidated without giving much back.",
        ("bull_strong", "recovery"), _high_tight_flag, stop_atr_mult=2.5,
    ),
    StrategySpec(
        "pullback_ema21", "Pullback to 21 EMA", "pullback",
        "Buys the first orderly pullback in an established uptrend.",
        ("bull_strong", "bull_narrow"), _pullback_to_ema21, stop_atr_mult=1.8,
    ),
    StrategySpec(
        "pullback_sma50", "Pullback to 50 DMA", "pullback",
        "The deeper reset — a full pullback to the 50 DMA that holds.",
        ("bull_strong", "bull_narrow", "correction"), _pullback_to_sma50, stop_atr_mult=2.0,
    ),
    StrategySpec(
        "oversold_bounce", "Oversold Bounce", "mean_reversion",
        "RSI washout while the long-term trend is intact. Predicted to pay in "
        "pullbacks and chop, and to be a trap in a genuine bear.",
        ("correction", "choppy"), _oversold_bounce, stop_atr_mult=2.0,
    ),
    StrategySpec(
        "squeeze_release", "Volatility Squeeze", "volatility",
        "Range compresses to a multi-month low, then resolves. No directional "
        "prior about the index — the control case for regime dependence.",
        ("bull_strong", "choppy", "recovery"), _squeeze_release, stop_atr_mult=2.0,
    ),
    StrategySpec(
        "gap_continuation", "Gap Continuation", "momentum",
        "Gaps out of a base on heavy volume and holds the gap into the close.",
        ("bull_strong", "recovery"), _gap_continuation, stop_atr_mult=2.5,
    ),
)

BY_ID = {s.id: s for s in STRATEGIES}
