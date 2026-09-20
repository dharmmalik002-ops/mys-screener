"""Tests for the mined entry/exit rules.

The rules were derived from a training half and frozen. Two things can quietly
break that: a threshold drifting to a rounder, more flattering number, and the
filter silently accepting everything. Both are pinned here, along with the
measured verdict — including the 24.8% win rate, which is the cost of the
payoff and must stay visible next to it.
"""

from __future__ import annotations

import unittest

from app.services.bot import rules as R


def signal(**kw):
    base = {"risk_pct": 5.0, "turnover_crore_at_entry": 4.0, "ret_63_at_entry": 10.0,
            "strategy": "squeeze_release", "regime": "bull_strong"}
    base.update(kw)
    return base


class FilterTests(unittest.TestCase):

    def test_a_clean_signal_is_accepted(self):
        self.assertTrue(R.accepts(signal()))

    def test_each_rule_can_reject_on_its_own(self):
        for field, bad in (("risk_pct", 12.0), ("turnover_crore_at_entry", 500.0),
                           ("ret_63_at_entry", -5.0), ("strategy", "oversold_bounce"),
                           ("regime", "bear")):
            with self.subTest(field=field):
                self.assertFalse(R.accepts(signal(**{field: bad})))

    def test_a_missing_field_rejects_rather_than_waving_through(self):
        """An absent field must not be read as a pass."""
        for missing in ("risk_pct", "turnover_crore_at_entry", "ret_63_at_entry",
                        "strategy", "regime"):
            with self.subTest(missing=missing):
                row = signal()
                row.pop(missing)
                self.assertFalse(R.accepts(row))

    def test_the_boundary_is_inclusive_where_it_was_measured(self):
        self.assertTrue(R.accepts(signal(risk_pct=R.MAX_RISK_PCT)))
        self.assertFalse(R.accepts(signal(risk_pct=R.MAX_RISK_PCT + 0.01)))


class FrozenThresholdTests(unittest.TestCase):
    """Thresholds came from training-half quantiles, not from judgement."""

    def test_thresholds_are_not_round_numbers(self):
        # A round number here is the tell that someone re-picked it by hand.
        self.assertNotEqual(R.MAX_RISK_PCT, round(R.MAX_RISK_PCT))
        self.assertNotEqual(R.MAX_TURNOVER_CRORE, round(R.MAX_TURNOVER_CRORE))

    def test_the_exit_carries_no_target(self):
        """A target is what caps the 142R winners the result depends on."""
        self.assertIsNone(R.EXIT_TARGET_R)
        self.assertGreaterEqual(R.EXIT_TRAIL_ATR_MULT, 6.0)


class VerdictTests(unittest.TestCase):

    def test_it_beats_the_smallcap_index(self):
        self.assertTrue(R.beats_smallcap())

    def test_the_payoff_clears_the_brief_and_the_win_rate_is_low(self):
        """Both halves. A 10:1 payoff is bought with a 1-in-4 win rate."""
        self.assertTrue(R.payoff_clears_the_brief())
        self.assertGreater(R.MEASURED_PAYOFF, 10.0)
        self.assertLess(R.MEASURED_WIN_RATE, 30.0)

    def test_the_book_is_highly_selective(self):
        self.assertLess(R.MEASURED_TRADES, 2000)

    def test_drawdown_is_shallower_than_the_return(self):
        self.assertGreater(R.MEASURED_CAGR, abs(R.MEASURED_MAX_DRAWDOWN))


if __name__ == "__main__":
    unittest.main()
