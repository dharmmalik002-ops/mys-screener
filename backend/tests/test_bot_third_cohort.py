"""The three setups taken from the published literature.

They are implemented faithfully, measured, and NOT registered. These tests
exist for two reasons: to keep the verdict attached to the code, and to guard
the failure mode that has now bitten twice in this project — a strategy that
fires zero times reports as "no signals" rather than as an error, because
`run_strategies` catches per-strategy exceptions and continues.
"""

from __future__ import annotations

import unittest

import numpy as np

from app.services.bot import strategies as S
from app.services.bot.features import build_features
from app.services.bot.history import Bars
from datetime import date, timedelta

THIRD_COHORT = ("pocket_pivot", "nr7_release", "rsi2_reversion")


def _bars(closes, highs=None, lows=None, volumes=None, opens=None) -> Bars:
    n = len(closes)
    c = np.asarray(closes, dtype=float)
    return Bars(
        symbol="TEST",
        dates=np.array([date(2005, 1, 3) + timedelta(days=i) for i in range(n)], dtype=object),
        open=np.asarray(opens if opens is not None else closes, dtype=float),
        high=np.asarray(highs if highs is not None else c * 1.01, dtype=float),
        low=np.asarray(lows if lows is not None else c * 0.99, dtype=float),
        close=c,
        volume=np.asarray(volumes if volumes is not None else [1e6] * n, dtype=float),
    )


class ThirdCohortFiresTests(unittest.TestCase):
    """Each setup must fire on a market built to trigger it.

    A new strategy that never fires looks identical to a new strategy that
    finds nothing, and the first version of `pocket_pivot` fired zero times
    for a whole run because `dist_52w_high` is signed negative and the
    "not extended" test was written the wrong way round.
    """

    def _uptrend(self, n=900):
        rng = np.random.default_rng(7)
        # A long grinding uptrend with ordinary noise: every one of these
        # setups requires price above its 200-day average.
        steps = rng.normal(0.0008, 0.018, n)
        return 100.0 * np.exp(np.cumsum(steps))

    def test_each_setup_fires_at_least_once(self):
        closes = self._uptrend()
        rng = np.random.default_rng(11)
        vols = rng.lognormal(14.0, 0.5, len(closes))
        f = build_features(_bars(closes, volumes=vols))
        for name in THIRD_COHORT:
            fn = getattr(S, f"_{name}")
            fired = int(np.asarray(fn(f)).sum())
            self.assertGreater(
                fired, 0,
                f"{name} fired zero times — a strategy that never fires is a "
                f"bug, not a finding (run_strategies swallows its exceptions)",
            )

    def test_registration_follows_the_honest_retest(self):
        """Gotcha 101 rejected all three against a sleeve that read same-day
        state. Re-measured against the corrected sleeve (gotcha 116), pocket
        pivot and NR7 each add return in both halves and are registered;
        RSI(2) does not and stays in the cohort."""
        registered = {s.id for s in S.STRATEGIES}
        self.assertIn("pocket_pivot", registered)
        self.assertIn("nr7_release", registered)
        self.assertNotIn("rsi2_reversion", registered)
        self.assertIn("rsi2_reversion", {n for n, _ in S.SECOND_COHORT})


class RSI2IsTheOnlyMeanReversionEntryTests(unittest.TestCase):

    def test_it_buys_weakness_not_strength(self):
        """The point of adding it: every registered setup buys after an up
        move. This one must be able to fire on a down day, or it is not
        testing the hypothesis it was added for."""
        n = 600
        closes = list(100.0 + np.arange(n) * 0.10)
        closes[-3:] = [closes[-4] * 0.94, closes[-4] * 0.89, closes[-4] * 0.86]
        f = build_features(_bars(closes))
        sig = np.asarray(S._rsi2_reversion(f))
        self.assertTrue(sig[-3:].any(), "did not fire into a sharp washout")


if __name__ == "__main__":
    unittest.main()
