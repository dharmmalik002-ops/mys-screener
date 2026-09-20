"""Tests for the live feedback loop.

The loop's danger is the opposite of the rest of the system's. Everywhere else
the risk is claiming an edge that is not there; here it is *reacting* to a
handful of live trades, which this project has already measured as a way to
lose money (`evolution.py`: demoting on a bad run sells the bottom). So these
tests pin restraint as hard as they pin action:

  - a small sample must not move anything, however bad it looks
  - beating expectation must never promote
  - a large, significant shortfall must actually stop the cell trading
"""

from __future__ import annotations

import unittest

import numpy as np

from app.services.bot import calibration as cal


def playbooks(expected: float = 0.30) -> list[dict]:
    return [
        {
            "regime": "bull_strong",
            "entries": [{"strategy": "pullback_ema21", "out_sample_r": expected}],
        }
    ]


def live(n: int, mean_r: float, strategy: str = "pullback_ema21", regime: str = "bull_strong") -> list[dict]:
    """`n` live trades averaging `mean_r`, with a realistic R distribution.

    Identical values would give the bootstrap zero variance, so it would be
    certain about every mean and every mild shortfall would read as decisive.
    Real R-multiples are capped near -1R and open-ended above, so the fixture
    is built that way: a majority of full stop-outs and a minority of runners,
    scaled to hit the requested mean.
    """
    rng = np.random.default_rng(7)
    # ~68% losers near -1R, the rest right-skewed winners.
    losers = int(round(n * 0.68))
    values = np.concatenate([
        rng.normal(-0.95, 0.12, losers),
        rng.gamma(shape=2.0, scale=1.2, size=n - losers),
    ])
    values = values - values.mean() + mean_r
    return [
        {
            "strategy": strategy, "regime": regime, "r_multiple": float(v),
            "exit_day": "2026-01-01", "entry_day": "2026-01-01",
        }
        for v in values
    ]


class RestraintTests(unittest.TestCase):
    def test_a_small_sample_changes_nothing(self) -> None:
        """Ten disastrous trades is a fortnight, not evidence."""
        result = cal.calibrate(live(10, -1.0), playbooks())
        cell = result["cells"][0]
        self.assertEqual(cell["status"], "insufficient")
        self.assertEqual(cell["size_multiplier"], 1.0)
        self.assertEqual(result["suspended"], [])

    def test_beating_expectation_never_promotes(self) -> None:
        """Upside surprise on a small sample is the most seductive noise there is."""
        result = cal.calibrate(live(60, 2.5), playbooks())
        cell = result["cells"][0]
        self.assertEqual(cell["status"], "tracking")
        # Never above 1.0 — the loop can only ever reduce.
        self.assertEqual(cell["size_multiplier"], 1.0)

    def test_a_mild_shortfall_is_not_actioned(self) -> None:
        """Below expectation but inside what the sample can show."""
        result = cal.calibrate(live(30, 0.22), playbooks(expected=0.30))
        cell = result["cells"][0]
        self.assertEqual(cell["status"], "tracking")
        self.assertEqual(cell["size_multiplier"], 1.0)

    def test_no_live_record_leaves_every_cell_alone(self) -> None:
        result = cal.calibrate([], playbooks())
        self.assertTrue(all(c["status"] == "insufficient" for c in result["cells"]))
        self.assertEqual(result["book"]["status"], "insufficient")


class ActionTests(unittest.TestCase):
    def test_a_large_persistent_shortfall_suspends_the_cell(self) -> None:
        result = cal.calibrate(live(50, -0.95), playbooks())
        cell = result["cells"][0]
        self.assertEqual(cell["status"], "suspended")
        self.assertEqual(cell["size_multiplier"], 0.0)
        self.assertIn("pullback_ema21/bull_strong", result["suspended"])

    def test_suspension_needs_more_trades_than_a_flag(self) -> None:
        """Between the two floors the cell is flagged and halved, not stopped."""
        enough_to_flag = cal.MIN_LIVE_TRADES + 2
        self.assertLess(enough_to_flag, cal.MIN_TRADES_TO_SUSPEND)
        result = cal.calibrate(live(enough_to_flag, -0.95), playbooks())
        cell = result["cells"][0]
        self.assertEqual(cell["status"], "diverging")
        self.assertEqual(cell["size_multiplier"], cal.DIVERGING_SIZE_MULTIPLIER)

    def test_only_the_failing_cell_is_touched(self) -> None:
        books = [
            {"regime": "bull_strong", "entries": [
                {"strategy": "pullback_ema21", "out_sample_r": 0.30},
                {"strategy": "vcp_breakout", "out_sample_r": 0.30},
            ]},
        ]
        result = cal.calibrate(live(50, -0.95, strategy="pullback_ema21"), books)
        by_strategy = {c["strategy"]: c for c in result["cells"]}
        self.assertEqual(by_strategy["pullback_ema21"]["status"], "suspended")
        self.assertEqual(by_strategy["vcp_breakout"]["status"], "insufficient")
        self.assertEqual(by_strategy["vcp_breakout"]["size_multiplier"], 1.0)

    def test_size_multiplier_lookup_defaults_to_full(self) -> None:
        """An unknown cell, or no calibration at all, must not shrink anything."""
        self.assertEqual(cal.size_multiplier_for(None, "x", "y"), 1.0)
        result = cal.calibrate(live(50, -0.95), playbooks())
        self.assertEqual(cal.size_multiplier_for(result, "unknown", "bull_strong"), 1.0)
        self.assertEqual(cal.size_multiplier_for(result, "pullback_ema21", "bull_strong"), 0.0)


if __name__ == "__main__":
    unittest.main()
