"""The seasoned-trader rules: built, measured on the yearly rebuild, not adopted.

Five rules a discretionary trader applies by reflex, declared together before
any was measured (CLAUDE.md gotcha 112). None cleared the bar, so each ships
OFF. These tests keep them honest in both directions: each rule must actually
change behaviour when switched on (a rule that silently does nothing is the
zero-signal bug of gotcha 82), and none may be switched on by default.
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

import numpy as np

from app.services.bot import rules as R
from app.services.bot.engine import ExitModel, simulate_symbol
from app.services.bot.features import build_features
from app.services.bot.history import Bars
from app.services.bot.strategies import StrategySpec

SPEC = StrategySpec(id="t", label="t", family="breakout", thesis="t",
                    expects=("bull_strong",), generate=lambda f: None, stop_atr_mult=2.0)


def make(closes, opens=None, highs=None, lows=None):
    n = len(closes)
    c = np.asarray(closes, float)
    return Bars(
        symbol="TST",
        dates=np.array([date(2015, 1, 1) + timedelta(days=i) for i in range(n)], dtype=object),
        open=np.asarray(opens if opens is not None else c, float),
        high=np.asarray(highs if highs is not None else c * 1.01, float),
        low=np.asarray(lows if lows is not None else c * 0.99, float),
        close=c, volume=np.full(n, 1e7),
    )


def run(bars, sig_at, **kw):
    f = build_features(bars)
    sig = np.zeros(len(bars.close), bool)
    sig[sig_at] = True
    base = dict(target_r=None, max_hold_sessions=500, trail_after_r=1.0, trail_atr_mult=3.0)
    base.update(kw)
    return simulate_symbol(SPEC, f, sig, ExitModel(**base))


class DefaultsAreOffTests(unittest.TestCase):

    def test_every_seasoned_rule_defaults_off(self):
        m = ExitModel()
        self.assertIsNone(m.max_entry_gap_pct)
        self.assertFalse(m.confirm_above_signal_high)
        self.assertFalse(m.stop_at_signal_low)
        self.assertFalse(m.trail_on_close)
        self.assertIsNone(m.climax_sma50_mult)

    def test_none_has_been_adopted(self):
        """Measured on the yearly rebuild (walk-forward +38.85% baseline):
        trail-on-close +38.99, climax +38.99, no-chase +38.54, buy-stop
        confirmation +37.51, stop-under-signal-low +34.07; the two best
        combined +39.22 — inside the 0.5pp tie band and a year worse on the
        single split. If this test fails, one was adopted: re-measure it."""
        self.assertEqual(R.SEASONED_RULES, {})

    def test_the_shared_builder_matches_the_shipped_constants(self):
        m = R.exit_model()
        self.assertEqual(m.max_stop_pct, R.EXIT_MAX_STOP_PCT)
        self.assertEqual(m.trail_atr_mult, R.EXIT_TRAIL_ATR_MULT)
        self.assertEqual(m.max_hold_sessions, R.EXIT_MAX_HOLD_SESSIONS)


class EachRuleActuallyBitesTests(unittest.TestCase):
    """A rule that silently changes nothing looks exactly like a rule that
    was tested and found neutral. Each must visibly alter a trade."""

    def _uptrend(self, n=320):
        return list(100.0 * np.exp(np.linspace(0, 0.6, n)))

    def test_no_chase_skips_a_gap_up_entry(self):
        closes = self._uptrend()
        opens = list(closes)
        opens[251] = closes[250] * 1.08               # 8% gap on the entry bar
        bars = make(closes, opens=opens)
        self.assertTrue(run(bars, 250))
        self.assertFalse(run(bars, 250, max_entry_gap_pct=3.0), "gap entry was chased")

    def test_confirmation_skips_a_bar_that_never_clears_the_trigger(self):
        closes = self._uptrend()
        highs = [c * 1.01 for c in closes]
        highs[251] = highs[250] * 0.99                # never trades above signal high
        bars = make(closes, highs=highs)
        self.assertTrue(run(bars, 250))
        self.assertFalse(run(bars, 250, confirm_above_signal_high=True))

    def test_structural_stop_is_tighter_than_the_atr_stop(self):
        closes = self._uptrend()
        lows = [c * 0.999 for c in closes]            # a very shallow signal-day low
        bars = make(closes, lows=lows)
        wide = run(bars, 250)[0]
        tight = run(bars, 250, stop_at_signal_low=True)[0]
        self.assertGreater(tight.stop, wide.stop)

    def test_trail_on_close_survives_an_intraday_wick(self):
        closes = self._uptrend(330)
        lows = [c * 0.99 for c in closes]
        # A wick through the TRAIL (~3 ATR under the close) but not through the
        # hard initial stop, which stays intraday by design. The first version
        # of this test used a 20% wick, which also broke the hard stop — the
        # test was wrong, not the rule.
        lows[300] = closes[300] * 0.92
        bars = make(closes, lows=lows)
        intraday = run(bars, 250)[0]
        on_close = run(bars, 250, trail_on_close=True)[0]
        self.assertLessEqual(intraday.exit_day, bars.dates[300])
        self.assertGreater(on_close.exit_day, bars.dates[300], "the wick still stopped it out")

    def test_climax_exits_a_parabolic_run(self):
        base = self._uptrend(260)
        blow = [base[-1] * (1.06 ** k) for k in range(1, 40)]   # vertical run
        bars = make(base + blow)
        t = run(bars, 250, climax_sma50_mult=1.7)[0]
        self.assertEqual(t.exit_reason, "climax")


if __name__ == "__main__":
    unittest.main()
