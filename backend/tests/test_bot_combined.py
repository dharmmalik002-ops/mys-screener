"""Tests for the combined product — the strongest claim this project makes.

It is therefore the one most worth attacking. Eight favourable numbers here
turned out to be measurement errors, so these pin the specific ways a two-sleeve
blend flatters itself:

  - a rebalancing bonus smuggled in by the alignment
  - dropping the flat stretches of one sleeve, which is where the other earns
  - a Sharpe or CAGR taken on a calendar that is not the one being claimed

and they pin, as assertions, the two facts any honest summary has to carry:
the blend earns LESS than the median fund, and the learned component's
contribution is drawdown and nothing else.
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

import numpy as np

from app.services.bot import combined as cb


def curve(start: date, values):
    return [{"day": (start + timedelta(days=i)).isoformat(), "equity": v}
            for i, v in enumerate(values)]


class BlendArithmeticTests(unittest.TestCase):

    def test_each_sleeve_is_normalised_before_weighting(self):
        """A sleeve starting at 100 must not outvote one starting at 1."""
        big = curve(date(2020, 1, 1), [100.0, 110.0, 120.0])
        small = curve(date(2020, 1, 1), [1.0, 1.0, 1.0])
        days, values = cb.blend(big, small, 0.5)
        self.assertAlmostEqual(values[0], 1.0, places=9)
        # +20% on half the book, flat on the other half.
        self.assertAlmostEqual(values[-1], 1.10, places=9)

    def test_no_rebalancing_between_the_sleeves(self):
        """Each sleeve compounds alone; the total is their sum, nothing more.

        A rebalanced blend of a riser and a faller beats the un-rebalanced one,
        and the size of the win is set by the interval. That number would be an
        artefact of the alignment code, so it must not appear.
        """
        up = curve(date(2020, 1, 1), [1.0, 2.0, 4.0])
        down = curve(date(2020, 1, 1), [1.0, 0.5, 0.25])
        _, values = cb.blend(up, down, 0.5)
        self.assertAlmostEqual(values[-1], 0.5 * 4.0 + 0.5 * 0.25, places=9)

    def test_a_sleeve_that_stops_printing_is_held_flat_not_dropped(self):
        """The book only prints when active. Its idle days are the point."""
        short = curve(date(2020, 1, 1), [1.0, 1.5])              # stops early
        long = curve(date(2020, 1, 1), [1.0, 1.0, 1.0, 2.0])
        days, values = cb.blend(short, long, 0.5)
        self.assertEqual(len(days), 4, "the union calendar lost days")
        self.assertAlmostEqual(values[-1], 0.5 * 1.5 + 0.5 * 2.0, places=9)

    def test_weight_shifts_the_result_in_the_declared_direction(self):
        up = curve(date(2020, 1, 1), [1.0, 2.0])
        flat = curve(date(2020, 1, 1), [1.0, 1.0])
        _, heavy = cb.blend(up, flat, 0.9)
        _, light = cb.blend(up, flat, 0.1)
        self.assertGreater(heavy[-1], light[-1])

    def test_too_short_a_sleeve_returns_nothing_rather_than_a_number(self):
        self.assertIsNone(cb.blend(curve(date(2020, 1, 1), [1.0]),
                                   curve(date(2020, 1, 1), [1.0, 1.1]), 0.5))


class StatsTests(unittest.TestCase):

    def test_drawdown_is_peak_to_trough_not_start_to_end(self):
        days = [date(2020, 1, 1) + timedelta(days=i) for i in range(4)]
        values = np.asarray([1.0, 2.0, 1.0, 2.0])   # ends flat, halved on the way
        self.assertAlmostEqual(cb.stats(days, values)["max_drawdown_pct"], -50.0, places=6)

    def test_cagr_is_annualised_on_the_actual_span(self):
        days = [date(2020, 1, 1), date(2022, 1, 1)]
        values = np.asarray([1.0, 4.0])             # 4x over ~2 years -> ~100%/yr
        self.assertAlmostEqual(cb.stats(days, values)["cagr_pct"], 100.0, delta=0.3)

    def test_a_flat_curve_has_no_drawdown_and_no_return(self):
        days = [date(2020, 1, 1) + timedelta(days=i) for i in range(5)]
        s = cb.stats(days, np.ones(5))
        self.assertEqual(s["max_drawdown_pct"], 0.0)
        self.assertAlmostEqual(s["cagr_pct"], 0.0, places=6)


class RecordedVerdictTests(unittest.TestCase):
    """Both halves of the claim, kept where they cannot drift apart."""

    def test_only_a_handful_of_funds_dominate_it(self):
        self.assertEqual(cb.MEASURED_FUNDS_DOMINATING, 5)
        self.assertEqual(cb.MEASURED_FUNDS_COUNTED, 691)
        self.assertLess(cb.funds_dominating_pct(), 1.0)

    def test_the_blend_earns_less_than_the_median_fund(self):
        """The half a headline would drop. It does not get to be dropped."""
        self.assertTrue(cb.blend_earns_less_than_median_fund())
        self.assertLess(cb.MEASURED_RETURN_PERCENTILE, 50.0)

    def test_timing_beats_the_median_fund_on_both_axes(self):
        """The claim this project supports, pinned so it cannot drift.

        Return must clear benchmark.CAGR_TIE_BAND, not merely exceed by a
        rounding error — a 0.01-point win over three years is a tie.
        """
        self.assertTrue(cb.timing_beats_median_fund_on_both())
        self.assertLess(cb.MEASURED_TIMING_FUNDS_DOMINATING, 10)
        self.assertGreater(cb.MEASURED_TIMING_RETURN_PERCENTILE, 50.0)

    def test_stock_selection_is_reported_beside_the_win_not_beneath_it(self):
        """46 funds beat selection on both axes against timing's 6."""
        self.assertTrue(cb.selection_is_beaten_by_many_funds())
        self.assertLess(cb.MEASURED_BOOK_CAGR, cb.MEASURED_FUND_MEDIAN_CAGR)

    def test_it_wins_on_drawdown_and_on_sharpe(self):
        self.assertGreater(cb.MEASURED_BLEND_DRAWDOWN, cb.MEASURED_FUND_MEDIAN_DRAWDOWN)
        self.assertGreater(cb.MEASURED_BLEND_SHARPE, cb.MEASURED_FUND_MEDIAN_SHARPE)

    def test_more_risk_does_not_buy_more_return(self):
        """The answer to "you are only winning because you take less risk".

        Turning the risk up is the obvious remedy and it makes things worse on
        both axes, because the book is already ~97% deployed — a bigger risk
        budget concentrates the same capital instead of adding any. If this
        ever fails, the frontier has changed shape and the return ceiling is
        no longer structural.
        """
        self.assertFalse(cb.return_can_be_bought_with_risk())
        self.assertGreater(cb.MEASURED_DEPLOYED_PCT_AT_SHIPPED_RISK, 90.0)
        self.assertLess(
            cb.MEASURED_BOOK_DRAWDOWN_AT_RISK_060,
            cb.MEASURED_BOOK_DRAWDOWN_AT_RISK_010,
            "more risk should deepen the drawdown",
        )

    def test_learning_contributes_drawdown_and_not_return(self):
        """Its contribution must not grow on the way into the product."""
        self.assertGreater(cb.MEASURED_LEARNING_DRAWDOWN_GAIN_HELD_OUT, 1.0)
        self.assertLess(cb.MEASURED_LEARNING_CAGR_GAIN_HELD_OUT, 0.5)
        self.assertLess(cb.MEASURED_LEARNING_CAGR_GAIN_3Y, 0.0)


if __name__ == "__main__":
    unittest.main()
