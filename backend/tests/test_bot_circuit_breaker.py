"""Tests for the defensive breaker — the one learning result that measured positive.

Precisely because it is the positive result it gets the harshest tests, the
same treatment `test_bot_timing.py` gives regime timing. Eight favourable
numbers in this project turned out to be measurement errors, so these pin the
specific ways this one could be:

  - seeing a trade's outcome on the day it is still being decided
  - standing a cell down on a handful of trades and calling it discipline
  - refusing to reinstate, which would turn a brake into a slow shutdown

and, just as importantly, they pin the half that FAILED, so no future summary
can quietly upgrade "cuts drawdown" into "makes more money".
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from app.services.bot import circuit_breaker as cb


def row(entry: str, exit_: str | None, r: float, strategy="vcp", regime="bull_strong"):
    return {"strategy": strategy, "regime": regime, "entry_day": entry,
            "exit_day": exit_, "r_multiple": r}


def losing_run(n: int, start=date(2020, 1, 1), r=-1.0, **kw):
    """`n` closed losers, one per day."""
    return [row(str(start + timedelta(days=i)), str(start + timedelta(days=i + 1)), r, **kw)
            for i in range(n)]


class InformationSetTests(unittest.TestCase):

    def test_a_trade_cannot_be_suspended_by_its_own_outcome(self):
        rows = losing_run(40)
        mask = cb.suspended_mask(rows, window=5, threshold=0.0)
        # The first five have no history behind them and must be allowed.
        self.assertEqual(mask[:5], [False] * 5)

    def test_a_trade_closing_on_the_entry_day_is_not_yet_known(self):
        same = "2020-06-01"
        rows = losing_run(30) + [row(same, same, -1.0), row(same, "2020-06-09", -1.0)]
        # Build a case where the trailing window is exactly at the boundary by
        # asking for a window longer than the settled history.
        mask = cb.suspended_mask(rows, window=31, threshold=0.0)
        self.assertFalse(mask[-1], "a trade settling on the decision day leaked in")

    def test_open_trades_never_enter_the_window(self):
        rows = [row(f"2020-01-{i+1:02d}", None, -5.0) for i in range(30)]
        rows.append(row("2020-03-01", "2020-03-05", -1.0))
        mask = cb.suspended_mask(rows, window=5, threshold=0.0)
        self.assertFalse(any(mask), "an unresolved position was scored")


class SuspendAndReinstateTests(unittest.TestCase):

    def test_a_cell_is_stood_down_once_the_evidence_is_there(self):
        rows = losing_run(40)
        mask = cb.suspended_mask(rows, window=10, threshold=0.0)
        self.assertTrue(mask[-1])

    def test_thin_evidence_never_stands_a_cell_down(self):
        rows = losing_run(6)
        mask = cb.suspended_mask(rows, window=25, threshold=0.0)
        self.assertEqual(mask, [False] * 6)

    def test_the_cell_comes_back_when_the_trades_recover(self):
        """A brake that never releases is a shutdown, not a brake."""
        bad = losing_run(12, start=date(2020, 1, 1), r=-1.0)
        good = losing_run(12, start=date(2021, 1, 1), r=+3.0)
        probe = [row("2022-01-01", "2022-01-05", 0.0)]
        mask = cb.suspended_mask(bad + good + probe, window=10, threshold=0.0)
        self.assertFalse(mask[-1], "cell stayed suspended after recovering")

    def test_suspension_is_per_cell_not_global(self):
        bad = losing_run(30, strategy="vcp")
        fine = losing_run(30, strategy="flag", r=+2.0)
        probe_bad = [row("2022-01-01", "2022-01-05", 0.0, strategy="vcp")]
        probe_fine = [row("2022-01-01", "2022-01-05", 0.0, strategy="flag")]
        mask = cb.suspended_mask(bad + fine + probe_bad + probe_fine, window=10)
        self.assertTrue(mask[-2], "the failing cell was not stood down")
        self.assertFalse(mask[-1], "a healthy cell was punished for its neighbour")

    def test_regime_separates_cells_of_the_same_strategy(self):
        bad = losing_run(30, regime="choppy")
        probe = [row("2022-01-01", "2022-01-05", 0.0, regime="bull_strong")]
        mask = cb.suspended_mask(bad + probe, window=10)
        self.assertFalse(mask[-1], "a bad run in one regime leaked into another")


class RecordedVerdictTests(unittest.TestCase):
    """The measurement, including the half that did not work."""

    def test_drawdown_beat_the_matched_random_control(self):
        self.assertTrue(cb.cuts_tail_risk())
        self.assertGreater(
            cb.MEASURED_HELD_OUT_BREAKER_MAXDD, cb.MEASURED_HELD_OUT_BASELINE_MAXDD
        )

    def test_risk_adjusted_return_did_not_improve(self):
        """Kept as an assertion so no summary can quietly promote this result.

        If this ever fails, the breaker has started beating the random control
        on Sharpe and the claim may be widened — deliberately, not by drift.
        """
        self.assertFalse(cb.improves_risk_adjusted_return())

    def test_the_short_window_is_what_carries_the_effect(self):
        self.assertGreater(cb.MEASURED_W25_BEATS_RANDOM, cb.MEASURED_W50_BEATS_RANDOM)
        self.assertEqual(cb.PRIMARY_WINDOW, 25)


if __name__ == "__main__":
    unittest.main()
