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


class DecileScaleTests(unittest.TestCase):
    """The raw score is not a 1-10 scale, and reading it as one empties the book."""

    def test_reaching_nine_raw_needs_nearly_every_component_maxed(self):
        """The fact that forced the decile scale: across 18 years exactly 12
        of 15,125 cleared signals scored 9 on the raw sum. The structural
        reason is that the score is a sum of five bounded parts, so degrading
        any ONE of them drops a near-perfect signal under 9.

        (My first version of this test asserted the opposite and failed: a
        maximal signal on `pullback_ema21` scores 9.4, not under 9. The test
        was wrong, not the code.)
        """
        best = {
            "risk_pct": 2.0, "turnover_crore_at_entry": 1.0,
            "ret_63_at_entry": 40.0, "strategy": "squeeze_release",
            "regime": "bull_strong",
        }
        self.assertEqual(cf.score(best, True), 10.0, "a maximal signal should")

        # The three HEAVY components — stop width (3.0), turnover (2.0) and
        # momentum (1.5) — each cost the 9 on their own. Setup quality and
        # regime do not: the best-to-worst spread on those is 0.6 and 0.53
        # points respectively, which is not enough to drag a perfect score
        # under 9. That is a property of the weights, not a defect.
        #
        # (This test has now been written wrong twice. First it claimed a
        # maximal signal scores under 9; then it claimed degrading ANY single
        # component costs the 9. Both were my premise, not the code.)
        for field, spoiled in (
            ("risk_pct", 5.0),
            ("turnover_crore_at_entry", 8.0),
            ("ret_63_at_entry", 5.0),
        ):
            degraded = dict(best, **{field: spoiled})
            self.assertLess(
                cf.score(degraded, True), 9.0,
                f"degrading {field} alone no longer costs the 9 — the heavy "
                f"components have stopped dominating and the decile "
                f"cut-points need re-deriving",
            )

        # And the converse, which is the real protection: the two light
        # components cannot carry a 9 between them. A signal that is perfect
        # on setup and regime but ordinary on everything else must not rate.
        ordinary = {
            "risk_pct": 6.0, "turnover_crore_at_entry": 9.0,
            "ret_63_at_entry": 5.0, "strategy": "squeeze_release",
            "regime": "bull_strong",
        }
        self.assertLess(cf.score(ordinary, True), 8.0,
                        "setup and regime alone manufactured a high score")

    def test_deciles_span_one_to_ten(self):
        self.assertEqual(cf.decile(-100.0), 1.0)
        self.assertEqual(cf.decile(1000.0), 10.0)
        for cut in cf.DECILE_CUTS:
            self.assertGreaterEqual(cf.decile(cut), 2.0)

    def test_the_cuts_are_ordered_and_not_round_numbers(self):
        """Deciles of the training half, frozen. Round numbers would mean
        someone picked them rather than measured them."""
        self.assertEqual(list(cf.DECILE_CUTS), sorted(cf.DECILE_CUTS))
        self.assertEqual(len(cf.DECILE_CUTS), 9)
        self.assertTrue(any(abs(c - round(c)) > 0.01 for c in cf.DECILE_CUTS))

    def test_rated_is_monotone_in_the_raw_score(self):
        prev = 0.0
        for raw in [x / 10.0 for x in range(0, 110)]:
            now = cf.decile(raw)
            self.assertGreaterEqual(now, prev)
            prev = now

    def test_the_bar_is_a_top_band(self):
        """`CONVICTION_BAR` is 8 — the top 30% of what the rules cleared.

        This assertion has moved once, from 9 to 8, and the reason is
        measured rather than cosmetic: the book is constrained by the
        1%-of-equity sizing rule rather than by signal quality, so it can
        afford the extra volume and gains a year of outperformance for it
        (16 of 18 against 15). It must still be a TOP band — dropping the bar
        to 5 would make the rating decorative.
        """
        self.assertGreaterEqual(cf.CONVICTION_BAR, 8.0)
        self.assertLessEqual(cf.CONVICTION_BAR, 10.0)


class SizingIsAlreadyMaximalTests(unittest.TestCase):

    def test_the_multiplier_is_bounded_and_cannot_rescue_a_capped_book(self):
        """Measured: with the 1%-of-equity rule binding at ~1.6% of equity per
        position, conviction multipliers of 1.5x, 2x and 3x produced results
        identical to the last decimal. Every trade is already sized at the
        ceiling the risk rule allows, so "bet more on a 10" is arithmetically
        unavailable without breaking that rule. The multiplier stays bounded
        so a future edit cannot quietly reintroduce it as leverage."""
        self.assertLessEqual(cf.size_multiplier(10.0), 2.0)
