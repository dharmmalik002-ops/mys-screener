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

from . import indicators as ind
from .features import Features

# Families group strategies that share a failure mode — when a whole family
# stops working at once that is a market fact, not ten independent findings.
FAMILIES = ("breakout", "momentum", "pullback", "mean_reversion", "volatility", "fundamental")


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


# --- Second cohort: measured, and NOT enabled ------------------------------
# These five were added on a reasonable hypothesis — that the account's return
# is capped by the edge per trade, which is capped by what the library can
# recognise, so five structurally different setups (a news gap, a multi-quarter
# base, a relative-strength leader, a tight coil, a failed breakdown) should
# widen the opportunity set.
#
# The hypothesis was wrong, and expensively so. Every one of them looked good
# in isolation: +0.15R to +0.36R, payoffs of 2.2 to 3.9, all statistically
# significant over 133,036 trades. But at the *account* level, with the same
# capital and the same sixty positions, they made things markedly worse:
#
#     10 strategies   median CAGR 11.39% across 30 book structures, 50% beat
#                     the median fund
#     15 strategies   median CAGR  7.07% across the same 30, 0% beat it
#
# The reason is capacity, not quality. The book is capital-constrained, so it
# is always choosing a small slice of available signals; adding a high-volume
# setup at a middling edge (rs_leader_pullback alone fires thousands of times)
# crowds the slice with mediocre candidates and displaces better ones. A larger
# library only helps a book that is short of ideas, and this one is not — it
# already declines ~95% of what it sees.
#
# They are kept here, unregistered, so the experiment is not repeated. Re-add
# any of them to STRATEGIES only alongside a ranking that can actually keep
# them out of the book when something better is available.


def _episodic_pivot(f: Features) -> np.ndarray:
    """A violent gap out of a quiet stretch — the signature of new information.

    Qullamaggie's "episodic pivot": something changed (earnings, an order book,
    a regulatory decision) and the gap is the market repricing it. The quiet
    period before matters as much as the gap; a jump in an already-volatile
    name is noise, the same jump out of a dormant one is news.
    """
    o, c = f.bars.open, f.bars.close
    gap = o > _prior(c) * 1.06
    dormant = _prior(f.range_pct_20, 2) < 20.0
    held = (c > o * 0.98) & (f.rel_volume > 3.0)
    return _safe(gap & dormant & held & (c > f.sma50) & f.liquid)


def _long_base_breakout(f: Features) -> np.ndarray:
    """A breakout from a multi-quarter base, not a three-week one.

    Every other breakout in this library works off a 20-day high. This one
    needs the stock to have gone nowhere for roughly half a year first, which
    is a different animal: a long base means a long accumulation, and the
    supply overhead that has to clear is correspondingly larger.
    """
    c = f.bars.close
    high_126 = ind.rolling_max(f.bars.high, 126)
    low_126 = ind.rolling_min(f.bars.low, 126)
    with np.errstate(divide="ignore", invalid="ignore"):
        base_depth = np.where(low_126 > 0, (high_126 - low_126) / low_126 * 100.0, np.nan)
    flat_base = base_depth < 35.0
    trigger = (c > _prior(high_126)) & (f.rel_volume > 1.5)
    return _safe(_prior_flag(flat_base) & trigger & (c > f.sma200) & f.liquid)


def _rs_leader_pullback(f: Features) -> np.ndarray:
    """A stock outperforming the index, pulling back to its own rising 10 EMA.

    The distinction from `pullback_ema21` is the relative-strength condition:
    this asks that the stock be *beating the market* on the way in, not merely
    rising. In a market where everything rises, those are the same trade; in a
    choppy one they are not, which is the whole reason to separate them.
    """
    c, low = f.bars.close, f.bars.low
    leading = _safe(f.rs_at_high_63) | (np.nan_to_num(f.rs_slope_63, nan=-1.0) > 0.02)
    touched = low <= f.ema10 * 1.015
    turning = (c > _prior(c)) & (c > f.ema10)
    return _safe(leading & _prior_flag(touched) & turning & (c > f.sma50) & (f.sma50 > f.sma200) & f.liquid)


def _coiled_spring(f: Features) -> np.ndarray:
    """Range compresses to a five-bar knot inside an uptrend, then expands.

    `squeeze_release` measures compression with ATR over sixty bars; this
    measures it as a five-bar range inside a twenty-bar one, which catches a
    much tighter and shorter coil. Different clock, different setup.
    """
    c = f.bars.close
    knot = (f.range_pct_5 < 4.0) & (f.range_pct_5 < f.range_pct_20 * 0.35)
    expansion = (c > _prior(f.high_10)) & (f.rel_volume > 1.5)
    return _safe(_prior_flag(knot) & expansion & _trend_template(f) & f.liquid)


def _failed_breakdown(f: Features) -> np.ndarray:
    """Undercuts a prior low, then reclaims it — a spring, in Wyckoff's sense.

    The only setup here that buys weakness rather than strength. It is included
    precisely because everything else in the library is long-strength: if the
    whole library shares one failure mode, a study across it cannot find that
    out. Predicted to work in corrections and chop, and to be a falling-knife
    trade in a real bear.
    """
    c, low = f.bars.close, f.bars.low
    undercut = _prior(low) < _prior(f.low_20, 2)
    reclaim = (c > _prior(f.low_20, 2)) & (c > _prior(c))
    still_in_trend = (c > f.sma200) & (f.ma200_slope > -0.01)
    return _safe(_prior_flag(undercut) & reclaim & still_in_trend & (f.rel_volume > 1.2) & f.liquid)


# --- Earnings: a different kind of information -----------------------------
# Every setup above reads price and volume. That whole family has been measured
# and does not hold up out of sample, so these two test whether a genuinely
# different input does. The hypothesis is post-earnings-announcement drift:
# prices under-react to large surprises and keep moving for weeks.
#
# `features.earnings_positive` is already lagged by one session — a result
# released after the close cannot be traded that day — so nothing here needs to
# shift it again. See `earnings.surprise_flags`.


def _earnings_drift(f: Features) -> np.ndarray:
    """Inside the drift window after a big positive surprise, with price agreeing.

    The confirmation matters. A surprise the market shrugs off is not news it
    under-reacted to; it is news it disagreed with. Requiring price above the
    50 DMA and above the prior close keeps the signal to surprises the tape has
    at least acknowledged.
    """
    c = f.bars.close
    return _safe(
        f.earnings_positive
        & (c > f.sma50)
        & (c > _prior(c))
        & f.liquid
    )


def _earnings_gap_continuation(f: Features) -> np.ndarray:
    """A positive surprise that gapped, held the gap, and is still in its window.

    The stricter cousin: it wants the market to have repriced visibly on the
    news rather than merely drifted. Distinct from `gap_continuation`, which
    knows nothing about why a stock gapped and fires on any of them.
    """
    o, c = f.bars.open, f.bars.close
    gapped = o > _prior(c) * 1.02
    held = c >= o
    return _safe(
        f.earnings_positive
        & _prior_flag(gapped & held)
        & (c > f.sma200)
        & (f.rel_volume > 1.5)
        & f.liquid
    )


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

    # The second cohort is deliberately absent — see the note above its
    # definitions. Enabling it cost 4.3 points of median CAGR.

    # --- earnings cohort: a different information source -----------------
    # Low frequency by nature (four announcements a year per name), so unlike
    # the second cohort these cannot flood a capital-constrained book.
    StrategySpec(
        "earnings_drift", "Earnings Drift", "fundamental",
        "Inside the drift window after a large positive earnings surprise, with price "
        "confirming. Tests whether a different kind of information carries an edge the "
        "price patterns do not.",
        ("bull_strong", "bull_narrow", "choppy", "correction"), _earnings_drift, stop_atr_mult=2.5,
    ),
    StrategySpec(
        "earnings_gap_hold", "Earnings Gap Hold", "fundamental",
        "A positive surprise that gapped, held the gap, and is still inside its drift window. "
        "Unlike the plain gap setup, this one knows why the stock gapped.",
        ("bull_strong", "bull_narrow", "recovery"), _earnings_gap_continuation, stop_atr_mult=2.5,
    ),
)

# Defined but not registered. Kept so the measurement is reproducible and the
# experiment is not repeated; `test_bot_engine.py` still exercises them.
SECOND_COHORT = (
    ("episodic_pivot", _episodic_pivot),
    ("long_base_breakout", _long_base_breakout),
    ("rs_leader_pullback", _rs_leader_pullback),
    ("coiled_spring", _coiled_spring),
    ("failed_breakdown", _failed_breakdown),
)

BY_ID = {s.id: s for s in STRATEGIES}
