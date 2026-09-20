"""Tests for the adaptive exit — the learning test that ran on the right axis.

The exit is the highest-leverage parameter in this system, and market state is
the one axis proven to carry information, so an exit rule that re-picks itself
from closed trades is the strongest form of "self-improvement" the data
supports. It was measured over 106,110 trades and it loses.

These tests pin two separate things:

  1. **The chooser's information set.** An adaptive rule that quietly sees the
     outcome of trades still open at the decision boundary would post a
     handsome number and mean nothing. Most of this file is that one concern,
     approached from four directions.
  2. **The recorded verdict**, including the result that closes the question —
     perfect foresight also loses, so no better chooser can win.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date

from app.services.bot import exit_learning as el


@dataclass
class FakeTrade:
    exit_day: date | None
    r_multiple: float
    regime: str = "bull_strong"


BOUNDARY = date(2020, 1, 1)


class InformationSetTests(unittest.TestCase):
    """What the chooser may see on the morning it decides."""

    def test_trades_closing_after_the_boundary_are_invisible(self):
        trades = [
            FakeTrade(date(2019, 6, 1), +1.0),
            FakeTrade(date(2020, 6, 1), +9.0),   # the future
        ]
        self.assertEqual(el.prior_returns(trades, BOUNDARY), [1.0])

    def test_a_trade_closing_on_the_boundary_is_still_the_future(self):
        # Standing at 1 January the bot does not know how 1 January ends.
        trades = [FakeTrade(BOUNDARY, +9.0)]
        self.assertEqual(el.prior_returns(trades, BOUNDARY), [])

    def test_open_trades_are_excluded_rather_than_scored_at_zero(self):
        # Scoring a live position as flat is its own quiet bias: it drags
        # every candidate toward zero by an amount that depends on how many
        # positions it happens to be holding.
        trades = [FakeTrade(None, +9.0), FakeTrade(date(2019, 1, 1), -1.0)]
        self.assertEqual(el.prior_returns(trades, BOUNDARY), [-1.0])

    def test_trailing_window_drops_older_history(self):
        trades = [
            FakeTrade(date(2015, 5, 1), +5.0),
            FakeTrade(date(2018, 5, 1), +1.0),
            FakeTrade(date(2019, 5, 1), +2.0),
        ]
        got = el.prior_returns(trades, BOUNDARY, trailing_years=2)
        self.assertEqual(sorted(got), [1.0, 2.0])

    def test_regime_filter_selects_only_that_regime(self):
        trades = [
            FakeTrade(date(2019, 1, 1), +1.0, regime="bull_strong"),
            FakeTrade(date(2019, 2, 1), +7.0, regime="bear"),
        ]
        self.assertEqual(el.prior_returns(trades, BOUNDARY, regime="bull_strong"), [1.0])


class ChooserTests(unittest.TestCase):

    def _many(self, n: int, r: float, day=date(2019, 1, 1)):
        return [FakeTrade(day, r) for _ in range(n)]

    def test_picks_the_best_candidate_once_all_clear_the_floor(self):
        closed = {
            "frozen": self._many(300, 0.10),
            "rival": self._many(300, 0.40),
        }
        self.assertEqual(el.choose_exit(closed, BOUNDARY, fallback="frozen"), "rival")

    def test_a_thin_rival_cannot_win_a_licence_to_switch(self):
        # The rival looks twice as good on eighty trades. Eighty trades is
        # noise, and the floor is what stops noise becoming a decision.
        closed = {
            "frozen": self._many(300, 0.10),
            "rival": self._many(80, 0.90),
        }
        self.assertEqual(el.choose_exit(closed, BOUNDARY, fallback="frozen"), "frozen")

    def test_no_eligible_candidate_falls_back_rather_than_guessing(self):
        closed = {"frozen": self._many(5, 0.10), "rival": self._many(5, 5.0)}
        self.assertEqual(el.choose_exit(closed, BOUNDARY, fallback="frozen"), "frozen")

    def test_a_rival_that_only_wins_after_the_boundary_does_not_win(self):
        """The whole experiment in one assertion."""
        closed = {
            "frozen": self._many(300, 0.10),
            "rival": self._many(300, -0.50) + self._many(300, +9.0, day=date(2020, 7, 1)),
        }
        self.assertEqual(el.choose_exit(closed, BOUNDARY, fallback="frozen"), "frozen")

    def test_regime_choice_uses_the_lower_floor(self):
        # 60 trades is enough within one regime, where 200 would never be met.
        closed = {
            "frozen": self._many(100, 0.10),
            "rival": self._many(100, 0.40),
        }
        self.assertEqual(
            el.choose_exit(closed, BOUNDARY, fallback="frozen", regime="bull_strong"),
            "rival",
        )
        self.assertEqual(el.choose_exit(closed, BOUNDARY, fallback="frozen"), "frozen")


class RecordedVerdictTests(unittest.TestCase):
    """The measurement, kept where it cannot be quietly forgotten."""

    def test_adaptive_lost_to_the_frozen_rule(self):
        self.assertTrue(el.adaptive_is_worse_than_frozen())
        self.assertLess(el.MEASURED_PER_REGIME_AVG_R, el.MEASURED_FROZEN_AVG_R)

    def test_perfect_foresight_also_lost(self):
        """The upper bound on every exit-learning scheme, and it is negative.

        If this ever fails it means the oracle beat the frozen rule, i.e. there
        is a prize after all and a cleverer chooser could be worth building.
        """
        self.assertTrue(el.oracle_is_worse_than_frozen())

    def test_adaptive_won_almost_no_years(self):
        self.assertEqual(el.MEASURED_YEARS_ADAPTIVE_WON, 2)
        self.assertEqual(el.MEASURED_YEARS_SCORED, 19)


if __name__ == "__main__":
    unittest.main()
