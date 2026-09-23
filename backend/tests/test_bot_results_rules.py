"""Results-day rules: built, measured on the yearly rebuild, not adopted (gotcha 114).

Selling ahead of results cost 0.5-6.5pp a year on the walk-forward in every
form tried, so both rules ship OFF. These tests keep them honest: each must
bite when switched on, and a symbol with no calendar is never guessed at.
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

import numpy as np

from app.services.bot import engine, rules as R
from app.services.bot.engine import ExitModel, results_exit_sessions, simulate_symbol
from app.services.bot.features import build_features
from app.services.bot.history import Bars
from app.services.bot.strategies import StrategySpec

SPEC = StrategySpec(id="t", label="t", family="breakout", thesis="t",
                    expects=("bull_strong",), generate=lambda f: None, stop_atr_mult=2.0)
D0 = date(2015, 1, 1)


def bars(n=400):
    c = np.linspace(100, 160, n)
    return Bars(symbol="TST", dates=np.array([D0 + timedelta(days=i) for i in range(n)], dtype=object),
                open=c.copy(), high=c * 1.01, low=c * 0.99, close=c, volume=np.full(n, 1e7))


def run(b, **kw):
    sig = np.zeros(len(b.close), bool); sig[300] = True
    base = dict(target_r=None, max_hold_sessions=500, trail_after_r=None)
    base.update(kw)
    return simulate_symbol(SPEC, build_features(b), sig, ExitModel(**base))


class ResultsRulesTests(unittest.TestCase):

    def tearDown(self):
        engine.RESULTS_CALENDAR.clear()

    def test_defaults_off_and_not_adopted(self):
        m = ExitModel()
        self.assertIsNone(m.results_exit_below_r)
        self.assertIsNone(m.results_entry_blackout)
        self.assertIsNone(R.exit_model().results_exit_below_r)
        self.assertIsNone(R.exit_model().results_entry_blackout)

    def test_filing_after_the_open_exits_that_morning_before_it_the_prior_morning(self):
        b = bars()
        engine.RESULTS_CALENDAR["TST"] = [(D0 + timedelta(days=320), 16 * 60), (D0 + timedelta(days=340), 8 * 60)]
        out = results_exit_sessions(b)
        self.assertTrue(out[320]); self.assertTrue(out[339]); self.assertEqual(int(out.sum()), 2)

    def test_exit_rule_sells_at_the_open_before_results(self):
        b = bars()
        engine.RESULTS_CALENDAR["TST"] = [(D0 + timedelta(days=320), 16 * 60)]
        t = run(b, results_exit_below_r=1e9)[0]
        self.assertEqual(t.exit_reason, "results")
        self.assertEqual(t.exit_day, D0 + timedelta(days=320))

    def test_a_position_with_enough_cushion_holds_through(self):
        b = bars()
        engine.RESULTS_CALENDAR["TST"] = [(D0 + timedelta(days=320), 16 * 60)]
        self.assertNotEqual(run(b, results_exit_below_r=0.0)[0].exit_reason, "results")

    def test_blackout_skips_an_entry_just_before_results(self):
        b = bars()
        engine.RESULTS_CALENDAR["TST"] = [(D0 + timedelta(days=303), 16 * 60)]
        self.assertEqual(run(b, results_entry_blackout=5), [])
        self.assertEqual(len(run(b)), 1)

    def test_no_calendar_means_no_change(self):
        b = bars()
        self.assertEqual(run(b, results_exit_below_r=1e9, results_entry_blackout=5)[0].exit_reason,
                         run(b)[0].exit_reason)


if __name__ == "__main__":
    unittest.main()
