"""Score every signal 1-10 before taking it, and size by the score.

The book has always been binary: a signal clears the rules or it does not,
and every survivor gets the same money. That throws away the obvious fact
that some setups are better than others — and the trade record can say which,
because the features that separate them were already measured.

The score is built from five components, each one an effect established on
the **training half** and each carrying a weight proportional to how much R
it was worth there. Nothing here is fitted to the held-out window.

  * **stop width** — the strongest single effect in the record (+1.12R in the
    tightest band against +0.40R in the widest, and the ordering held out of
    sample). A tight stop is also what makes a large R multiple arithmetically
    possible.
  * **turnover** — smaller names pay more, and still do under the
    liquidity-scaled slippage that killed this effect the first time.
  * **momentum** — the stock's own 3-month return.
  * **setup** — the six survivors are not equal; each carries its measured
    training-half average R.
  * **market state** — regime and the index's own trend. A good setup in a
    falling market is a worse trade than the same setup in a rising one, and
    the binary gate could not express that.

Two deliberate choices. The score is a **sum of independent components**
rather than a fitted model: with five inputs and a 20-year sample a fitted
one would find interactions that are noise, and this way each part can be
read and argued with. And it is **capped at 10 and floored at 1**, so a single
extreme input cannot manufacture a maximum-conviction trade on its own.
"""

from __future__ import annotations

from typing import Mapping

# Weights sum to 10. Each is proportional to the R-spread its feature showed
# on the training half — stop width was worth roughly twice what momentum was,
# and the weights say so.
W_STOP = 3.0
W_TURNOVER = 2.0
W_SETUP = 2.0
W_MOMENTUM = 1.5
W_MARKET = 1.5

# Measured on the training half. Order matters, magnitudes do not need to be
# exact — they set the ranking, and the ranking is what the score uses.
SETUP_QUALITY: dict[str, float] = {
    "squeeze_release": 1.00,
    "earnings_gap_hold": 1.00,
    "high_tight_flag": 0.90,
    "earnings_drift": 0.85,
    "pullback_ema21": 0.70,
    "minervini_breakout": 0.55,
}

STRONG_REGIMES = frozenset({"bull_strong"})
OK_REGIMES = frozenset({"bull_narrow", "recovery"})

HIGH_CONVICTION = 8.0      # the bar for taking a trade at all


def _band(value: float, best: float, worst: float) -> float:
    """1.0 at `best`, 0.0 at `worst`, linear between, clamped outside."""
    if best == worst:
        return 0.5
    raw = (worst - value) / (worst - best)
    return max(0.0, min(1.0, raw))


def score(trade: Mapping, index_above_200: bool | None = None) -> float:
    """Conviction from 1 to 10. Missing inputs score as neutral, never high."""
    stop = float(trade.get("risk_pct") or 99.0)
    turnover = float(trade.get("turnover_crore_at_entry") or 1e9)
    momentum = float(trade.get("ret_63_at_entry") or 0.0)
    setup = str(trade.get("strategy") or "")
    regime = str(trade.get("regime") or "")

    total = 0.0
    total += W_STOP * _band(stop, best=2.0, worst=8.0)
    total += W_TURNOVER * _band(turnover, best=1.0, worst=12.0)
    total += W_SETUP * SETUP_QUALITY.get(setup, 0.3)
    total += W_MOMENTUM * _band(momentum, best=40.0, worst=0.0)

    market = 0.0
    if regime in STRONG_REGIMES:
        market += 0.7
    elif regime in OK_REGIMES:
        market += 0.35
    if index_above_200:
        market += 0.3
    total += W_MARKET * market

    return max(1.0, min(10.0, round(total, 2)))


def size_multiplier(conviction: float) -> float:
    """How much more to bet on a 10 than on an 8.

    Bounded at 2x deliberately. Conviction here is a ranking, not a
    probability — it has no calibration behind it that would justify betting
    five times as much on a 10, and an unbounded multiplier turns one
    mis-scored trade into the whole year.
    """
    if conviction >= 9.5:
        return 2.0
    if conviction >= 9.0:
        return 1.6
    if conviction >= 8.5:
        return 1.3
    return 1.0
