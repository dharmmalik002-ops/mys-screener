"""Classify every session into a market regime, using only that session's past.

This is the axis the whole bot turns on. "Which strategy works in which market
condition" is only answerable if "market condition" is a label that could have
been known on the day — so every input here is causal, and the classifier is a
pure function of arrays that end at the session being labelled. A regime that
needs next month's data to assign is a regime you cannot trade.

The taxonomy is six states, chosen to be few enough that each carries a real
sample across ~19 years and distinct enough that they ask different things of a
strategy:

    bull_strong    index above a rising 200 DMA, broad participation
    bull_narrow    index still above the 200 DMA, but breadth has rolled over
    choppy         directionless: repeatedly crossing the 50 DMA, no trend
    correction     a defined pullback inside a longer uptrend
    bear           below a falling 200 DMA, deep drawdown
    recovery       reclaiming after a bear, before the 200 DMA has turned up

`bull_narrow` earns its place: it looks like a bull market on the index and
behaves like a trap for breakout strategies, and lumping it into `bull_strong`
is exactly the averaging that makes a backtest say "breakouts work" when what
it means is "breakouts worked for 14 of those 19 months".

Volatility is a *separate* tag rather than a seventh state. It cuts across all
six — there are calm bulls and violent bulls — and crossing it into the primary
label would produce 18 cells, most of them too thin to report.

Thresholds are constants here, declared before any outcome was measured. They
are not fitted: a threshold tuned until the regimes separate returns nicely is
just curve-fitting wearing a different hat.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date

import numpy as np

from . import indicators as ind

# --- Thresholds (declared, not fitted) -------------------------------------
MA_FAST = 50
MA_SLOW = 200
SLOPE_WINDOW = 40          # bars used to call the 200 DMA rising or falling
DRAWDOWN_WINDOW = 252      # peak-to-here measured over a rolling year

BEAR_DRAWDOWN = -15.0      # index this far off its 1-year peak
CORRECTION_DRAWDOWN = -7.0
BREADTH_BROAD = 50.0       # % of the universe above its own 200 DMA
BREADTH_NARROW = 35.0
CHOP_CROSSES = 4           # 50 DMA crossings in CHOP_WINDOW that define chop
CHOP_WINDOW = 40

VIX_CALM_PCTILE = 30.0
VIX_STRESSED_PCTILE = 75.0
VOL_PCTILE_WINDOW = 504    # ~2 years, so "high VIX" is relative to recent norms

# A regime flag that survives fewer sessions than this is noise from a single
# volatile week. Raw labels are smoothed by persistence before being published.
MIN_REGIME_RUN = 5

REGIMES = ("bull_strong", "bull_narrow", "choppy", "correction", "bear", "recovery")
VOLATILITY_BANDS = ("calm", "normal", "stressed")

REGIME_LABELS = {
    "bull_strong": "Strong Bull",
    "bull_narrow": "Narrow Bull",
    "choppy": "Choppy / Rangebound",
    "correction": "Correction",
    "bear": "Bear",
    "recovery": "Recovery",
}

REGIME_NOTES = {
    "bull_strong": "Index above a rising 200 DMA with the majority of stocks participating.",
    "bull_narrow": "Index still holding its 200 DMA, but breadth has rolled over — fewer names carrying it.",
    "choppy": "No trend. Price keeps crossing back and forth through the 50 DMA.",
    "correction": "A defined pullback from the highs, inside what is still a longer uptrend.",
    "bear": "Below a falling 200 DMA and well off the highs.",
    "recovery": "Reclaiming after a decline, before the long-term trend has turned back up.",
}


@dataclass
class RegimeRow:
    """One session's classification plus the evidence behind it."""

    day: date
    regime: str
    volatility_band: str
    regime_age: int           # sessions this regime has been in force
    index_close: float
    pct_from_200dma: float
    pct_from_52w_high: float
    ma200_slope: float
    breadth_above_200dma: float
    breadth_net_new_highs: float
    vix_percentile: float
    constituents: int

    def to_dict(self) -> dict:
        out = asdict(self)
        out["day"] = self.day.isoformat()
        out["regime_label"] = REGIME_LABELS.get(self.regime, self.regime)
        return out


def _classify_one(
    *,
    above_200: bool,
    above_50: bool,
    slope_200: float,
    drawdown: float,
    breadth_200: float,
    crosses_50: int,
    breadth_known: bool,
) -> str:
    """The ladder, in priority order. First match wins.

    Order matters and is deliberate: a deep drawdown is a bear market whatever
    breadth says, and chop is only chop once trend and drawdown have both been
    ruled out.
    """
    # 1. Deep decline below a falling long-term trend.
    if drawdown <= BEAR_DRAWDOWN and not above_200:
        return "bear"

    # 2. Reclaiming: back above the 50 DMA while the 200 DMA has not yet turned
    #    up. This is the highest-payoff and highest-risk state, and folding it
    #    into "bear" (price is often still below the 200 DMA) would hide it.
    if not above_200 and above_50 and drawdown > BEAR_DRAWDOWN:
        return "recovery"
    if above_200 and slope_200 <= 0 and drawdown <= CORRECTION_DRAWDOWN and above_50:
        return "recovery"

    # 3. A pullback that has not broken the long-term trend.
    if drawdown <= CORRECTION_DRAWDOWN and not above_50:
        return "correction"

    # 4. Above the long-term trend: strong or narrow, decided by breadth.
    if above_200 and slope_200 > 0:
        if not breadth_known:
            # Early history, before enough symbols existed to count breadth.
            # Calling it strong would invent participation that was never
            # measured, so it reads as the weaker label.
            return "bull_narrow"
        if breadth_200 >= BREADTH_BROAD:
            return "bull_strong"
        if breadth_200 < BREADTH_NARROW:
            return "bull_narrow"
        return "bull_narrow" if not above_50 else "bull_strong"

    # 5. Whipsawing through the 50 DMA with no drawdown to speak of.
    if crosses_50 >= CHOP_CROSSES:
        return "choppy"

    # 6. Everything left: below a flat/falling 200 DMA without a deep drawdown.
    return "choppy" if above_200 else "bear"


def _smooth_runs(labels: list[str], min_run: int) -> list[str]:
    """Absorb runs shorter than `min_run` into the regime that preceded them.

    Without this the label flickers — one violent week inside a bull market
    prints two sessions of `correction`, and a per-regime statistic built on
    two-session runs is measuring noise. Absorbing *backwards* (into the prior
    regime, never the following one) keeps the operation causal: the label at
    session i still depends only on sessions <= i.
    """
    if not labels:
        return labels
    out = list(labels)
    run_start = 0
    for i in range(1, len(out) + 1):
        if i < len(out) and out[i] == out[run_start]:
            continue
        run_len = i - run_start
        if run_len < min_run and run_start > 0:
            out[run_start:i] = [out[run_start - 1]] * run_len
        run_start = i
    return out


def classify(
    sessions: list[date],
    index_close: np.ndarray,
    index_high: np.ndarray,
    breadth_above_200: np.ndarray,
    breadth_net_new_highs: np.ndarray,
    constituents: np.ndarray,
    vix_close: np.ndarray | None,
    min_constituents: int,
) -> list[RegimeRow]:
    """Label every session. All arrays are aligned to `sessions`.

    `vix_close` may be None or full of nan for the stretch before India VIX
    existed (pre-2008); the volatility band then falls back to realised
    volatility of the index itself, which is available for the whole history.
    """
    n = len(sessions)
    if n == 0:
        return []

    ma50 = ind.sma(index_close, MA_FAST)
    ma200 = ind.sma(index_close, MA_SLOW)
    slope200 = ind.slope_pct_per_bar(ma200, SLOPE_WINDOW)
    peak = ind.rolling_max(index_high, DRAWDOWN_WINDOW)
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdown = np.where(peak > 0, (index_close - peak) / peak * 100.0, np.nan)
    pct_from_200 = ind.distance_pct(index_close, ma200)

    above_50 = index_close > ma50
    above_200 = index_close > ma200
    # Count 50 DMA crossings in the trailing window — the operational
    # definition of "this market keeps changing its mind".
    crossed = np.zeros(n, dtype=np.int64)
    crossed[1:] = (above_50[1:] != above_50[:-1]).astype(np.int64)
    crosses_50 = np.zeros(n, dtype=np.int64)
    for i in range(n):
        start = max(0, i - CHOP_WINDOW + 1)
        crosses_50[i] = int(crossed[start : i + 1].sum())

    # Volatility band: India VIX when it exists, realised volatility before that.
    if vix_close is not None and np.isfinite(vix_close).any():
        vol_source = np.where(np.isfinite(vix_close), vix_close, np.nan)
    else:
        vol_source = np.full(n, np.nan)
    returns = np.full(n, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        returns[1:] = np.diff(index_close) / index_close[:-1]
    realised = ind.rolling_std(np.nan_to_num(returns), 20) * np.sqrt(252) * 100.0
    vol_series = np.where(np.isfinite(vol_source), vol_source, realised)
    vol_pctile = ind.rolling_percentile_rank(vol_series, VOL_PCTILE_WINDOW)

    raw: list[str] = []
    for i in range(n):
        breadth_known = bool(constituents[i] >= min_constituents and np.isfinite(breadth_above_200[i]))
        # Before the 200 DMA exists there is no trend to speak of. Labelling
        # these sessions would put ~200 unclassifiable days into whichever
        # bucket the defaults happened to fall in.
        if not np.isfinite(ma200[i]) or not np.isfinite(drawdown[i]):
            raw.append("choppy")
            continue
        raw.append(
            _classify_one(
                above_200=bool(above_200[i]),
                above_50=bool(above_50[i]),
                slope_200=float(slope200[i]) if np.isfinite(slope200[i]) else 0.0,
                drawdown=float(drawdown[i]),
                breadth_200=float(breadth_above_200[i]) if breadth_known else 0.0,
                crosses_50=int(crosses_50[i]),
                breadth_known=breadth_known,
            )
        )

    labels = _smooth_runs(raw, MIN_REGIME_RUN)

    rows: list[RegimeRow] = []
    age = 0
    for i, day in enumerate(sessions):
        age = age + 1 if i > 0 and labels[i] == labels[i - 1] else 1
        pctile = float(vol_pctile[i]) if np.isfinite(vol_pctile[i]) else 50.0
        band = (
            "calm" if pctile < VIX_CALM_PCTILE
            else "stressed" if pctile >= VIX_STRESSED_PCTILE
            else "normal"
        )
        rows.append(
            RegimeRow(
                day=day,
                regime=labels[i],
                volatility_band=band,
                regime_age=age,
                index_close=round(float(index_close[i]), 2),
                pct_from_200dma=round(float(pct_from_200[i]), 2) if np.isfinite(pct_from_200[i]) else 0.0,
                pct_from_52w_high=round(float(drawdown[i]), 2) if np.isfinite(drawdown[i]) else 0.0,
                ma200_slope=round(float(slope200[i]), 4) if np.isfinite(slope200[i]) else 0.0,
                breadth_above_200dma=round(float(breadth_above_200[i]), 2) if np.isfinite(breadth_above_200[i]) else 0.0,
                breadth_net_new_highs=round(float(breadth_net_new_highs[i]), 2) if np.isfinite(breadth_net_new_highs[i]) else 0.0,
                vix_percentile=round(pctile, 1),
                constituents=int(constituents[i]),
            )
        )
    return rows
