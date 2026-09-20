"""Tests for regime timing — the one result here that beats a professional.

Precisely because it is the positive result, it gets the harshest tests. Every
other favourable number this project produced turned out to be a measurement
error, so these pin the specific ways this one could be:

  - acting on a regime label the day it is known (look-ahead)
  - crediting idle cash a return it would not earn
  - omitting tax on a rule that realises gains every few months
  - reporting an advantage that only exists at one cash-rate assumption
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from app.services.bot import timing as tm


def series(n: int, daily: float, start: date = date(2015, 1, 1)):
    """A straight-line index and its calendar."""
    days = [start + timedelta(days=i) for i in range(n)]
    closes, level = {}, 100.0
    for d in days:
        closes[d] = level
        level *= 1.0 + daily
    return days, closes


class LookaheadTests(unittest.TestCase):
    def test_the_rule_uses_the_prior_session_label(self) -> None:
        """Today's regime is not knowable until today closes.

        A crash on day k, with the regime flipping to bear on that same day.
        Acting on the same day's label would dodge the crash entirely; using
        the prior session's label must not.
        """
        days, closes = series(200, 0.0)
        crash = 100
        for i, d in enumerate(days):
            if i >= crash:
                closes[d] = 50.0            # halves and stays there
        regime = {d: ("bull_strong" if i < crash else "bear") for i, d in enumerate(days)}

        result = tm.simulate(days, closes, regime, cash_rate_pct=0.0, tax_pct=0.0)
        self.assertIsNotNone(result)
        # It must take the hit: the label said bear only once the crash had
        # happened, so the position was still on when it did.
        self.assertLess(result.max_drawdown_pct, -40.0)

    def test_it_does_exit_once_the_regime_is_known(self) -> None:
        """Having taken the first hit, it must not keep taking them."""
        days, closes = series(300, 0.0)
        level = 100.0
        for i, d in enumerate(days):
            if i >= 100:
                level *= 0.99               # a long grinding decline
            closes[d] = level
        regime = {d: ("bull_strong" if i < 100 else "bear") for i, d in enumerate(days)}

        timed = tm.simulate(days, closes, regime, cash_rate_pct=0.0, tax_pct=0.0)
        passive = tm.buy_and_hold(days, closes)
        self.assertIsNotNone(timed)
        self.assertIsNotNone(passive)
        self.assertGreater(timed.cagr_pct, passive.cagr_pct)
        self.assertGreater(timed.max_drawdown_pct, passive.max_drawdown_pct)


class FrictionTests(unittest.TestCase):
    def _flipping(self, n: int = 600):
        days, closes = series(n, 0.0003)
        # Alternate regime every 50 sessions, so the rule switches often.
        regime = {
            d: ("bull_strong" if (i // 50) % 2 == 0 else "bear")
            for i, d in enumerate(days)
        }
        return days, closes, regime

    def test_tax_reduces_the_result(self) -> None:
        days, closes, regime = self._flipping()
        untaxed = tm.simulate(days, closes, regime, tax_pct=0.0)
        taxed = tm.simulate(days, closes, regime, tax_pct=20.0)
        self.assertIsNotNone(untaxed)
        self.assertIsNotNone(taxed)
        self.assertLess(taxed.cagr_pct, untaxed.cagr_pct)

    def test_switch_costs_reduce_the_result(self) -> None:
        days, closes, regime = self._flipping()
        free = tm.simulate(days, closes, regime, switch_cost_bps=0.0, tax_pct=0.0)
        costly = tm.simulate(days, closes, regime, switch_cost_bps=50.0, tax_pct=0.0)
        self.assertIsNotNone(free)
        self.assertIsNotNone(costly)
        self.assertLess(costly.cagr_pct, free.cagr_pct)
        self.assertGreater(costly.switches, 0)

    def test_a_higher_cash_rate_helps_and_zero_is_the_floor(self) -> None:
        """The cash assumption is the one most open to argument, so it is bounded."""
        days, closes, regime = self._flipping()
        results = [
            tm.simulate(days, closes, regime, cash_rate_pct=rate, tax_pct=0.0)
            for rate in (0.0, 3.0, 6.0)
        ]
        self.assertTrue(all(r is not None for r in results))
        cagrs = [r.cagr_pct for r in results]
        self.assertEqual(cagrs, sorted(cagrs), "more cash yield must not reduce the result")

    def test_always_invested_matches_buy_and_hold(self) -> None:
        """With every regime tradeable and no frictions, the two must agree."""
        days, closes = series(400, 0.0005)
        regime = {d: "bull_strong" for d in days}
        timed = tm.simulate(days, closes, regime, cash_rate_pct=0.0, tax_pct=0.0,
                            switch_cost_bps=0.0)
        passive = tm.buy_and_hold(days, closes)
        self.assertIsNotNone(timed)
        self.assertIsNotNone(passive)
        self.assertAlmostEqual(timed.cagr_pct, passive.cagr_pct, delta=0.05)
        self.assertEqual(timed.exposure_pct, 100.0)


class StudyTests(unittest.TestCase):
    def test_the_study_reports_sensitivity_not_a_single_number(self) -> None:
        """An advantage that exists at only one assumption is not an advantage."""
        days, closes = series(800, 0.0004)
        regime = {
            d: ("bull_strong" if (i // 100) % 2 == 0 else "bear")
            for i, d in enumerate(days)
        }
        study = tm.build_timing_study(
            days, closes, regime, days[200], fund_median_cagr=11.36,
            fund_median_drawdown=-27.53,
        )
        self.assertTrue(study["available"])
        self.assertGreaterEqual(len(study["sensitivity"]), 6)
        # Both assumptions must be varied, not just one.
        self.assertGreater(len({s["cash_rate_pct"] for s in study["sensitivity"]}), 1)
        self.assertGreater(len({s["tax_pct"] for s in study["sensitivity"]}), 1)
        self.assertIn("caveat", study)
        self.assertIn("index timing, not stock selection", study["caveat"])

    def test_too_little_data_returns_nothing(self) -> None:
        days, closes = series(20, 0.001)
        regime = {d: "bull_strong" for d in days}
        self.assertIsNone(tm.simulate(days, closes, regime))


if __name__ == "__main__":
    unittest.main()


class HealthMonitorTests(unittest.TestCase):
    """The learning discipline pointed at the component that actually earns.

    The rest of the project audits stock cells, which have no edge — so it was
    monitoring noise. These pin the same asymmetry `calibration.py` uses: a
    rule can be flagged for breaking, and is never promoted for a good run.
    """

    def _labelled(self, n: int, good_up: bool, switches: int = 12):
        """A series where the regime alternates, optionally predicting direction."""
        days = [date(2015, 1, 1) + timedelta(days=i) for i in range(n)]
        block = max(1, n // switches)
        closes, level = {}, 100.0
        regime = {}
        for i, d in enumerate(days):
            invested = (i // block) % 2 == 0
            regime[d] = "bull_strong" if invested else "bear"
            closes[d] = level
            # When `good_up`, invested stretches rise and out stretches fall —
            # the classifier is doing its job.
            drift = 0.002 if (invested == good_up) else -0.002
            level *= 1.0 + drift
        return days, closes, regime

    def test_a_working_rule_reads_as_tracking(self) -> None:
        days, closes, regime = self._labelled(600, good_up=True)
        health = tm.assess_health(days, closes, regime, expected_exposure_pct=50.0)
        self.assertEqual(health.status, "tracking")
        self.assertGreater(health.discrimination_pp, 0.0)

    def test_a_rule_that_stops_discriminating_is_flagged(self) -> None:
        """Invested days no better than days out means the claim has failed."""
        days, closes, regime = self._labelled(600, good_up=False)
        health = tm.assess_health(days, closes, regime, expected_exposure_pct=50.0)
        self.assertEqual(health.status, "diverging")
        self.assertLess(health.discrimination_pp, 0.0)
        self.assertIn("stopped separating", health.note)

    def test_too_few_switches_is_reported_as_unknown(self) -> None:
        """A timing rule makes a handful of decisions a year; two say nothing."""
        days, closes, regime = self._labelled(200, good_up=False, switches=2)
        health = tm.assess_health(days, closes, regime, expected_exposure_pct=50.0)
        self.assertEqual(health.status, "insufficient")
        self.assertIn("judging noise", health.note)

    def test_a_good_run_never_promotes(self) -> None:
        """Health has no state above 'tracking' — upside is not actionable."""
        days, closes, regime = self._labelled(600, good_up=True)
        health = tm.assess_health(days, closes, regime, expected_exposure_pct=50.0)
        self.assertIn(health.status, {"tracking", "insufficient", "diverging"})
        self.assertNotIn("promot", health.note.lower())

    def test_the_study_carries_its_own_health(self) -> None:
        days, closes, regime = self._labelled(900, good_up=True)
        study = tm.build_timing_study(days, closes, regime, days[300],
                                      fund_median_cagr=11.36, fund_median_drawdown=-27.53)
        self.assertTrue(study["available"])
        self.assertIn("health", study)
        self.assertIn(study["health"]["status"], {"tracking", "diverging", "insufficient"})
