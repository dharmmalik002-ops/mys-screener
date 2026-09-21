"""Tests for the 1-10 conviction score.

The score decides which trades are taken AND how much each one gets, so a
bug here is doubly expensive. These pin the direction of every component, the
bounds, and — most importantly — that a missing input can never produce a
high-conviction trade.
"""

from __future__ import annotations

import unittest

from app.services.bot import confidence as cf


def signal(**kw):
    base = {"risk_pct": 3.0, "turnover_crore_at_entry": 3.0, "ret_63_at_entry": 25.0,
            "strategy": "squeeze_release", "regime": "bull_strong"}
    base.update(kw)
    return base


class DirectionTests(unittest.TestCase):
    """Each component must push the way the trade record said it does."""

    def test_a_tighter_stop_scores_higher(self):
        self.assertGreater(cf.score(signal(risk_pct=2.5)),
                           cf.score(signal(risk_pct=7.5)))

    def test_a_smaller_name_scores_higher(self):
        self.assertGreater(cf.score(signal(turnover_crore_at_entry=2.0)),
                           cf.score(signal(turnover_crore_at_entry=11.0)))

    def test_stronger_momentum_scores_higher(self):
        self.assertGreater(cf.score(signal(ret_63_at_entry=35.0)),
                           cf.score(signal(ret_63_at_entry=2.0)))

    def test_a_better_setup_scores_higher(self):
        self.assertGreater(cf.score(signal(strategy="squeeze_release")),
                           cf.score(signal(strategy="minervini_breakout")))

    def test_a_stronger_market_scores_higher(self):
        self.assertGreater(cf.score(signal(regime="bull_strong"), True),
                           cf.score(signal(regime="recovery"), False))

    def test_the_index_trend_moves_the_score_on_its_own(self):
        self.assertGreater(cf.score(signal(), True), cf.score(signal(), False))


class SafetyTests(unittest.TestCase):

    def test_an_empty_signal_scores_the_floor(self):
        """Unknown must never read as conviction."""
        self.assertEqual(cf.score({}), 1.0)

    def test_each_missing_field_pulls_the_score_down(self):
        full = cf.score(signal(), True)
        for field in ("risk_pct", "turnover_crore_at_entry", "ret_63_at_entry",
                      "strategy", "regime"):
            with self.subTest(field=field):
                row = signal()
                row.pop(field)
                self.assertLess(cf.score(row, True), full)

    def test_an_unknown_setup_does_not_score_as_a_good_one(self):
        self.assertLess(cf.score(signal(strategy="something_new")),
                        cf.score(signal(strategy="squeeze_release")))

    def test_the_score_stays_inside_one_to_ten(self):
        extremes = [
            signal(risk_pct=0.1, turnover_crore_at_entry=0.1, ret_63_at_entry=500.0),
            signal(risk_pct=99.0, turnover_crore_at_entry=1e6, ret_63_at_entry=-99.0),
        ]
        for row in extremes:
            for trend in (True, False):
                self.assertGreaterEqual(cf.score(row, trend), 1.0)
                self.assertLessEqual(cf.score(row, trend), 10.0)

    def test_no_single_input_can_manufacture_a_maximum_score(self):
        """A perfect stop on an otherwise poor setup must not clear the bar."""
        row = signal(risk_pct=0.5, turnover_crore_at_entry=1e6,
                     ret_63_at_entry=-50.0, strategy="unknown", regime="bear")
        self.assertLess(cf.score(row, False), cf.HIGH_CONVICTION)


class SizingTests(unittest.TestCase):

    def test_more_conviction_gets_more_money(self):
        self.assertGreater(cf.size_multiplier(9.8), cf.size_multiplier(8.2))

    def test_the_multiplier_is_bounded(self):
        """Conviction is a ranking, not a probability. One mis-scored trade
        must not be able to become the whole year."""
        for c in (8.0, 9.0, 9.5, 10.0, 99.0):
            self.assertLessEqual(cf.size_multiplier(c), 2.0)

    def test_a_bar_clearing_trade_is_never_shrunk(self):
        self.assertGreaterEqual(cf.size_multiplier(cf.HIGH_CONVICTION), 1.0)


if __name__ == "__main__":
    unittest.main()
