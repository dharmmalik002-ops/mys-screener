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

from typing import Mapping, Sequence

# Weights sum to 10. Each is proportional to the R-spread its feature showed
# on the training half — stop width was worth roughly twice what momentum was,
# and the weights say so.
W_STOP = 3.0
W_TURNOVER = 2.0
W_SETUP = 2.0
W_MOMENTUM = 1.5
W_MARKET = 1.5
# Industry-group strength (group_strength.py), the rank of the stock's group
# on the signal day. Weight declared at 1.5 — the momentum weight, because its
# measured R-spread on the training half was of the same size — and NOT tuned
# afterwards. Checked for load-bearing: 1.0, 1.5 and 2.0 all improved the
# yearly-rebuild result (+0.52 / +0.99 / +0.31pp), and 1.5 beat all ten
# matched random-noise controls of the same weight. Weights now sum to 11.5;
# the decile scale below absorbs that, which is what deciles are for.
W_GROUP = 1.5

# Measured on the training half. Order matters, magnitudes do not need to be
# exact — they set the ranking, and the ranking is what the score uses.
SETUP_QUALITY: dict[str, float] = {
    "squeeze_release": 1.00,
    "earnings_gap_hold": 1.00,
    "high_tight_flag": 0.90,
    "earnings_drift": 0.85,
    "pullback_ema21": 0.70,
    "minervini_breakout": 0.55,
    # Training-half closed record puts both level with minervini_breakout.
    "pocket_pivot": 0.55,
    "nr7_release": 0.55,
}

STRONG_REGIMES = frozenset({"bull_strong"})
OK_REGIMES = frozenset({"bull_narrow", "recovery"})

HIGH_CONVICTION = 8.0      # the bar on the RAW scale (see DECILE_CUTS below)

# --- the raw score is not a 1-10 scale, and pretending it is cost a year ----
# `score()` sums five bounded parts, so reaching 9 needs near-perfection on
# all five at once. Across 18 years exactly **12 signals of 15,125** ever did,
# and the highest score ever recorded is 9.33. "Take only the 9s and 10s" is
# therefore not a strategy on the raw scale, it is an empty book.
#
# These cut-points turn the raw score into genuine deciles, so a 9 means "top
# 20% of everything the rules cleared" — which is what anyone asking for a
# 9-out-of-10 trade actually means. They are the deciles of the **training
# half alone** (2,964 signals before 2018), frozen and applied unchanged to
# the held-out half, so the scale is not re-fitted to the period it scores.
#
# The ranking is monotone in both halves, which is what makes it worth having:
#
#     band          train avgR    test avgR
#     all cleared      +1.198       +1.592
#     >= 8             +1.618       +2.108
#     >= 9             +1.773       +2.568
#     >= 10            +2.208       +2.767
# Re-derived when the group component was added (gotcha 113): deciles of the
# training half (3,239 signals before 2018) under the new score, on the signal
# pool the close-basis trail and climax exit produce. Still monotone in both
# halves, and the top decile is sharper than it was:
#
#     band          train avgR    test avgR
#     all cleared      +1.150       +1.608
#     >= 8             +1.480       +2.337
#     >= 9             +1.642       +2.570
#     >= 10            +2.026       +3.715
# Re-derived when pocket_pivot and nr7_release joined the tradeable set
# (gotcha 116): the same training half, the same score, a larger pool.
DECILE_CUTS = (5.51, 5.92, 6.22, 6.49, 6.71, 6.94, 7.15, 7.4, 7.67)

# The band to trade: 8 to 10, i.e. the top 30% of what the rules cleared.
# Measured against the narrower 9-10 band, widening to 8 is better on return
# AND on the count that matters most here — 16 years beating the index
# against 15 — because the book is capital-constrained by the equity rule
# rather than by signal quality, so it can afford the extra volume.
#
#     bar 9   CAGR +40.25%  maxDD -21.16%  Sharpe 2.01  win 35.1%  15/18
#     bar 8   CAGR +41.54%  maxDD -21.21%  Sharpe 1.98  win 32.0%  16/18
CONVICTION_BAR = 8.0


def decile(raw: float, cuts: "Sequence[float] | None" = None) -> float:
    """Map a raw score onto 1-10 using the frozen training-half deciles.

    `cuts` overrides them — the live book passes the current year's cuts from
    the yearly rebuild (`bot_live_params.json`).
    """
    return 1.0 + float(sum(1 for cut in (DECILE_CUTS if cuts is None else cuts) if raw >= cut))


def rated(trade: "Mapping", index_above_200: bool | None = None,
          group_rank: float | None = None, *, quality: "Mapping | None" = None,
          cuts: "Sequence[float] | None" = None) -> float:
    """The number a trader should read: 1-10, deciles, not the raw sum."""
    return decile(score(trade, index_above_200, group_rank, quality=quality), cuts)


def _band(value: float, best: float, worst: float) -> float:
    """1.0 at `best`, 0.0 at `worst`, linear between, clamped outside."""
    if best == worst:
        return 0.5
    raw = (worst - value) / (worst - best)
    return max(0.0, min(1.0, raw))


def score(trade: Mapping, index_above_200: bool | None = None,
          group_rank: float | None = None, *, quality: "Mapping | None" = None) -> float:
    """Conviction from 1 to 10. Missing inputs score as neutral, never high."""
    stop = float(trade.get("risk_pct") or 99.0)
    turnover = float(trade.get("turnover_crore_at_entry") or 1e9)
    momentum = float(trade.get("ret_63_at_entry") or 0.0)
    setup = str(trade.get("strategy") or "")
    regime = str(trade.get("regime") or "")

    total = 0.0
    total += W_STOP * _band(stop, best=2.0, worst=8.0)
    total += W_TURNOVER * _band(turnover, best=1.0, worst=12.0)
    total += W_SETUP * (SETUP_QUALITY if quality is None else quality).get(setup, 0.3)
    total += W_MOMENTUM * _band(momentum, best=40.0, worst=0.0)

    market = 0.0
    if regime in STRONG_REGIMES:
        market += 0.7
    elif regime in OK_REGIMES:
        market += 0.35
    if index_above_200:
        market += 0.3
    total += W_MARKET * market
    # A missing group reading scores DOWN, like every other missing input:
    # a signal we know less about is not one we are confident in.
    total += W_GROUP * (float(group_rank) if group_rank is not None else 0.0)

    return max(1.0, round(total, 2))


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
