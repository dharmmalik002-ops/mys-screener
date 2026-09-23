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


class ChampionSetupsFireTests(unittest.TestCase):
    """Gotcha 82's failure mode: a broken setup reports zero signals, not an error."""

    def tearDown(self):
        engine.RESULTS_CALENDAR.clear()

    def _market(self):
        from app.services.bot import strategies as S  # noqa: F401
        n = 320
        c = list(np.linspace(100, 140, 270))
        # results session 270: gap 6%, strong close, triple volume
        o = c[:]
        c += [149.0] + [148.5 + 0.1 * k for k in range(8)] + [151.0 + 0.8 * k for k in range(n - 279)]
        o += [148.4] + c[271:]
        c = np.asarray(c[:n]); o = np.asarray(o[:n]); o[271:] = c[270:-1]
        v = np.full(n, 1e7); v[270] = 4e7
        return Bars(symbol="TST", dates=np.array([D0 + timedelta(days=i) for i in range(n)], dtype=object),
                    open=o, high=np.maximum(o, c) * 1.002, low=np.minimum(o, c) * 0.998, close=c, volume=v)

    def test_results_setups_fire_only_with_a_calendar(self):
        from app.services.bot import strategies as S
        b = self._market()
        f = build_features(b)
        self.assertFalse(S._results_breakaway(f).any(), "fired with no results calendar")
        engine.RESULTS_CALENDAR["TST"] = [(D0 + timedelta(days=270), 10 * 60)]
        f = build_features(b)
        self.assertTrue(S._results_breakaway(f)[270])
        self.assertTrue(S._results_follow_through(f).any())

    def test_wedge_pop_fires_on_a_reclaimed_pullback(self):
        from app.services.bot import strategies as S
        n = 320
        c = np.concatenate([np.linspace(100, 160, 290), np.linspace(159, 150, 20), [156.0] * 10])
        v = np.full(n, 1e7); v[310] = 2e7
        b = Bars(symbol="TST", dates=np.array([D0 + timedelta(days=i) for i in range(n)], dtype=object),
                 open=np.concatenate([[c[0]], c[:-1]]), high=c * 1.005, low=c * 0.995, close=c, volume=v)
        self.assertTrue(S._wedge_pop(build_features(b)).any())


class SwingDisciplineTests(unittest.TestCase):
    """Gotcha 118: cutting a trade that has not worked after N sessions — built, measured, off."""

    def test_default_off(self):
        self.assertIsNone(ExitModel().cut_loser_after_sessions)
        self.assertIsNone(R.exit_model().cut_loser_after_sessions)

    def test_it_cuts_a_trade_still_under_water(self):
        n = 400
        c = np.concatenate([np.linspace(100, 160, 301), np.full(n - 301, 159.7)])
        b = Bars(symbol="TST", dates=np.array([D0 + timedelta(days=i) for i in range(n)], dtype=object),
                 open=np.concatenate([[c[0]], c[:-1]]), high=c * 1.001, low=c * 0.999, close=c, volume=np.full(n, 1e7))
        t = run(b, cut_loser_after_sessions=5)[0]
        self.assertEqual(t.exit_reason, "time_cut")


class MinerviniSignatureTests(unittest.TestCase):
    """Gotcha 119: the measured Minervini entry — defined, fires, not registered."""

    def test_it_fires_on_a_tight_quiet_base_that_breaks_out(self):
        from app.services.bot import strategies as S
        n = 330
        c = np.concatenate([np.linspace(50, 100, 300), np.full(29, 99.0), [104.0]])
        v = np.concatenate([np.full(300, 1e7), np.full(29, 6e6), [2e7]])
        h = np.concatenate([c[:300] * 1.01, np.full(29, 100.0), [104.5]])
        l = np.concatenate([c[:300] * 0.99, np.full(29, 97.5), [99.5]])
        b = Bars(symbol="TST", dates=np.array([D0 + timedelta(days=i) for i in range(n)], dtype=object),
                 open=np.concatenate([[c[0]], c[:-1]]), high=h, low=l, close=c, volume=v)
        self.assertTrue(S._minervini_signature(build_features(b))[-1])

    def test_not_registered(self):
        from app.services.bot import strategies as S
        self.assertNotIn("minervini_signature", {s.id for s in S.STRATEGIES})
