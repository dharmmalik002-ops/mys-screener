"""Bet more when the market is paying, less when it is not.

`diagnose.py` returned the same verdict for every lagging year: **under
deployed**. The trades were good — 2009 at +1.58R, 2012 at +1.63R, 2023 at
+3.01R — and the account still finished behind the index, because a fixed risk
budget puts the same money to work in a year the market doubles as in a year
it falls apart.

So the size of the bet moves with the market. The multiplier is built only
from what was knowable that morning:

  * **index above its own 200-day average** — the single condition that held
    its direction in both halves of the split (+1.16R against +0.23R in
    training, +1.85R against +1.47R out of sample);
  * **breadth**, the share of the universe above its own 200-day average,
    which says whether a rally is broad enough to be worth pressing;
  * **the regime label**, which already gates entry.

This is deliberately NOT the learning loop that failed everywhere else in this
project. It does not look at the bot's own recent P&L — gotchas 40, 65 and 69
all measured that recent trade outcomes carry no information about future
ones, and gotcha 74 measured that letting the earning rule re-fit itself makes
it worse. This reads the *market*, which is the one axis shown to carry
information (gotcha 63), and it applies a multiplier declared in advance
rather than one fitted to results.

The ceiling matters as much as the multiplier. Risk is capped at `MAX_SCALE`
times base, and the account still cannot exceed 100% deployed — there is no
margin here, so pressing hard in a strong market means filling the book
faster, not borrowing.
"""

from __future__ import annotations

from datetime import date
from typing import Mapping

# Declared before measurement. Three states, not a continuous curve: a
# continuous function of breadth would be fitted, and there is not enough
# independent market history to fit one honestly.
MAX_SCALE = 2.0
STRONG_SCALE = 2.0
NORMAL_SCALE = 1.0
WEAK_SCALE = 0.5

STRONG_REGIMES = frozenset({"bull_strong"})
HEALTHY_REGIMES = frozenset({"bull_strong", "bull_narrow", "recovery"})
BROAD_BREADTH_PCT = 65.0     # share of the universe above its own 200 DMA


def scale_for(
    regime: str | None,
    breadth_above_200: float | None,
    index_above_200: bool | None,
) -> float:
    """The risk multiplier for one session, from that session's own tape.

    A missing input reads as "not confirmed" and pulls the multiplier down
    rather than up. Sizing up on an unknown is the expensive direction to be
    wrong in.
    """
    if regime not in HEALTHY_REGIMES:
        return WEAK_SCALE
    if index_above_200 is not True:
        return WEAK_SCALE
    if (
        regime in STRONG_REGIMES
        and breadth_above_200 is not None
        and breadth_above_200 >= BROAD_BREADTH_PCT
    ):
        return STRONG_SCALE
    return NORMAL_SCALE


def build_schedule(
    regime_by_day: Mapping[date, str],
    breadth_by_day: Mapping[date, float],
    index_above_200_by_day: Mapping[date, bool],
) -> dict[date, float]:
    """A multiplier for every session the regime tape covers."""
    return {
        day: scale_for(
            regime,
            breadth_by_day.get(day),
            index_above_200_by_day.get(day),
        )
        for day, regime in regime_by_day.items()
    }
