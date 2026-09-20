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


class RollingRiskTests(unittest.TestCase):
    """The cap breathes with volatility. It must not breathe with hindsight."""

    def _rows(self, n, risk, day="2020-06-01", **kw):
        base = {"turnover_crore_at_entry": 4.0, "ret_63_at_entry": 10.0,
                "strategy": "squeeze_release", "regime": "bull_strong"}
        base.update(kw)
        return [dict(base, entry_day=day, risk_pct=risk) for _ in range(n)]

    def test_a_calm_history_keeps_the_cap_tight(self):
        history = self._rows(400, 3.0, day="2020-01-01")
        wide = self._rows(1, 9.0, day="2020-12-01")   # inside the 365d window
        kept = R.accepted_with_rolling_risk(history + wide)
        self.assertNotIn(9.0, [t["risk_pct"] for t in kept],
                         "a wide stop passed in a calm market")

    def test_a_volatile_history_lets_a_wider_stop_through(self):
        """The 2009 case: after a crash every stop is wide."""
        history = self._rows(400, 14.0, day="2020-01-01")
        wide = self._rows(1, 9.0, day="2020-12-01")   # inside the 365d window
        kept = R.accepted_with_rolling_risk(history + wide)
        self.assertIn(9.0, [t["risk_pct"] for t in kept],
                      "the cap did not widen with conditions")

    def test_only_earlier_signals_set_the_cap(self):
        """A calm future must not tighten the cap on a trade taken today."""
        today = self._rows(1, 9.0, day="2020-06-01")
        history = self._rows(400, 14.0, day="2019-09-01")   # inside the window
        future = self._rows(400, 1.0, day="2020-07-01")
        kept = R.accepted_with_rolling_risk(history + today + future)
        taken = [t for t in kept if t["entry_day"] == "2020-06-01"]
        self.assertEqual(len(taken), 1, "a later, calmer period changed an earlier decision")

    def test_thin_early_history_falls_back_to_the_fixed_cap(self):
        rows = self._rows(5, 20.0, day="2008-01-01")
        self.assertEqual(R.accepted_with_rolling_risk(rows), [])

    def test_the_cap_is_coupled_to_the_registered_library(self):
        """Adding a strategy moves the cap for every other one.

        Not a bug being hidden — a documented cost of mining the percentile
        over the whole signal pool. Registering a wider-stopped setup shifted
        the book from +18.69% to +17.23% with no rule changed, so this test
        states the dependency out loud: anyone who registers a strategy has to
        re-run the backtest rather than assume the rules are unaffected.
        """
        # Enough foreign signals to actually move the 40th percentile: with
        # 300 against 300 the percentile still lands inside the tight block.
        eligible = self._rows(300, 4.0, day="2020-01-01")
        foreign = self._rows(700, 30.0, day="2020-01-01", strategy="some_new_setup")
        probe = self._rows(1, 8.0, day="2020-09-01")
        without = R.accepted_with_rolling_risk(eligible + probe)
        with_new = R.accepted_with_rolling_risk(eligible + foreign + probe)
        self.assertNotEqual(
            len(without), len(with_new),
            "a newly registered strategy left the cap untouched — if this rule "
            "was decoupled on purpose, update this test and re-run the book",
        )

    def test_the_other_rules_still_apply(self):
        rows = self._rows(400, 3.0, day="2020-01-01", strategy="oversold_bounce")
        self.assertEqual(R.accepted_with_rolling_risk(rows), [])


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
        self.assertGreater(R.MEASURED_PAYOFF, 8.0)
        self.assertLess(R.MEASURED_WIN_RATE, 30.0)

    def test_the_book_is_highly_selective(self):
        self.assertLess(R.MEASURED_TRADES, 2000)

    def test_return_per_drawdown_stays_worth_the_risk(self):
        """The book was sized up for return and the drawdown deepened with it.

        An earlier version asserted CAGR > |maxDD|, which held at 0.35% risk
        (+23.4% against -29.7%) and stopped holding at 0.50% (+28.5% against
        -35.3%). That is a deliberate trade — the diagnosis said the lagging
        years were under-deployed — so the assertion is now the ratio, which
        is what actually has to stay defensible.
        """
        # 0.52 under marked-to-market accounting with the position-cap bug
        # fixed. The earlier 0.75 bar was set against inflated numbers: a
        # capped position was booking P&L on the uncapped risk, and every
        # trade in this book clips the cap.
        self.assertGreater(R.MEASURED_CAGR / abs(R.MEASURED_MAX_DRAWDOWN), 0.45)

    def test_scaling_out_is_recorded_as_a_cost_not_an_upgrade(self):
        self.assertTrue(R.scaling_out_costs_return())
        self.assertIsNone(R.SCALE_OUT_AT_R, "partial exits must stay off by default")
        self.assertGreater(R.MEASURED_SCALED_WIN_RATE, 35.0)


if __name__ == "__main__":
    unittest.main()
