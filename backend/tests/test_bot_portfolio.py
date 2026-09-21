"""Tests for the account simulation and the professional benchmark.

The account layer is where a trade-level edge either survives contact with
finite capital or does not, and it has its own ways of lying:

  - ranking trades using information from after the entry date
  - deriving the volatility adjustment from the window it is then scored on
  - letting an unlimited book pretend to be an eight-position one
  - reporting a benchmark percentile computed from a handful of funds
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta
from pathlib import Path
import json
import tempfile

from app.services.bot import benchmark as bm
from app.services.bot import portfolio as pf


def trade(
    day: date,
    r: float,
    *,
    strategy: str = "s",
    regime: str = "bull_strong",
    symbol: str = "AAA",
    hold: int = 20,
    atr: float = 3.0,
    risk_pct: float = 6.0,
) -> dict:
    return {
        "strategy": strategy, "regime": regime, "symbol": symbol,
        "entry_day": day.isoformat(),
        "exit_day": (day + timedelta(days=hold)).isoformat(),
        "r_multiple": r, "risk_pct": risk_pct, "atr_pct_at_entry": atr,
    }


def snapshot(as_of: date, cells: list[tuple[str, str, str, float]]) -> dict:
    return {
        "as_of": as_of.isoformat(),
        "cells": [
            {"strategy": s, "regime": rg, "status": st, "avg_r": r}
            for s, rg, st, r in cells
        ],
        "counts": {},
        "tradeable": 0,
    }


class EligibilityTests(unittest.TestCase):
    def test_checkpoint_on_the_same_day_is_not_used(self) -> None:
        """A checkpoint dated today was computed from today's closes."""
        lookup = pf._eligible_lookup([
            snapshot(date(2020, 1, 1), [("s", "bull_strong", "confirmed", 0.3)]),
            snapshot(date(2020, 6, 1), [("s", "bull_strong", "retired", -0.2)]),
        ])
        # Strictly-before: on the checkpoint date itself we must still be on
        # the previous one, not the one being computed from that session.
        eligible, _ = pf._eligible_at(lookup, date(2020, 6, 1))
        self.assertIn(("s", "bull_strong"), eligible)
        later, _ = pf._eligible_at(lookup, date(2020, 6, 2))
        self.assertNotIn(("s", "bull_strong"), later)

    def test_before_the_first_checkpoint_nothing_is_eligible(self) -> None:
        lookup = pf._eligible_lookup([snapshot(date(2020, 1, 1), [("s", "b", "confirmed", 0.3)])])
        eligible, _ = pf._eligible_at(lookup, date(2019, 1, 1))
        self.assertEqual(eligible, set())

    def test_playbook_mode_ignores_reactive_status(self) -> None:
        """The live system uses a fixed playbook; reactive status must not override it."""
        days = [date(2023, 1, 1) + timedelta(days=i * 30) for i in range(12)]
        trades = [trade(d, 1.0) for d in days]
        snaps = [snapshot(date(2022, 1, 1), [("s", "bull_strong", "retired", -0.5)])]
        result = pf.simulate(
            trades, snaps, playbook_cells={("s", "bull_strong")},
            cell_expectancy={("s", "bull_strong"): 0.3}, label="playbook",
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.trades_taken, len(trades))


class CapacityTests(unittest.TestCase):
    def test_capacity_actually_binds_and_declines_are_counted(self) -> None:
        day = date(2023, 1, 2)
        trades = [trade(day, 1.0, symbol=f"S{i}", hold=200) for i in range(30)]
        config = pf.PortfolioConfig(max_concurrent=5, max_portfolio_risk_pct=100.0)
        result = pf.simulate(trades, [], config, point_in_time=False, label="capped")
        self.assertIsNotNone(result)
        self.assertEqual(result.trades_taken, 5)
        self.assertEqual(result.signals_declined, 25)

    def test_position_size_is_capped_by_capital(self) -> None:
        """A 0.5% stop on a 0.75% risk budget asks for 150% of the book."""
        day = date(2023, 1, 2)
        config = pf.PortfolioConfig(max_position_pct=20.0, risk_per_trade_pct=0.75)
        result = pf.simulate(
            [trade(day, 1.0, risk_pct=0.5)], [], config, point_in_time=False, label="cap",
        )
        self.assertIsNotNone(result)
        # Risk is scaled down with the position, so one trade cannot return
        # more than the cap allows: 20% of equity moving 1R at a 0.5% stop.
        self.assertLess(result.total_return_pct, 1.0)

    def test_losses_reduce_equity(self) -> None:
        days = [date(2023, 1, 1) + timedelta(days=i * 40) for i in range(10)]
        result = pf.simulate(
            [trade(d, -1.0) for d in days], [], point_in_time=False, label="losing",
        )
        self.assertIsNotNone(result)
        self.assertLess(result.ending_equity, result.starting_equity)
        self.assertLess(result.cagr_pct, 0)
        self.assertLess(result.max_drawdown_pct, 0)


class AdjustmentLeakageTests(unittest.TestCase):
    def test_adjustment_uses_only_trades_before_the_cutoff(self) -> None:
        """The held-out window must not inform the ranking that selects in it."""
        cutoff = date(2022, 1, 1)
        before = [
            trade(date(2020, 1, 1) + timedelta(days=i), 1.0, atr=1.0, symbol=f"A{i}")
            for i in range(200)
        ]
        # After the cutoff the same bucket is catastrophic. If that leaks in,
        # the adjustment for "under 2%" turns negative.
        after = [
            trade(date(2023, 1, 1) + timedelta(days=i), -5.0, atr=1.0, symbol=f"B{i}")
            for i in range(200)
        ]
        adjustment = pf.derive_atr_adjustment(before + after, cutoff)
        self.assertIn("under 2%", adjustment)
        self.assertGreaterEqual(adjustment["under 2%"], 0.0)

    def test_thin_buckets_are_dropped(self) -> None:
        rows = [trade(date(2020, 1, 1) + timedelta(days=i), 1.0, atr=1.0) for i in range(10)]
        self.assertEqual(pf.derive_atr_adjustment(rows, date(2021, 1, 1)), {})

    def test_ranking_prefers_the_better_bucket(self) -> None:
        """Two identical cells; only ATR differs. The adjustment must order them."""
        day = date(2023, 1, 2)
        trades = [
            trade(day, 3.0, symbol="QUIET", atr=1.0, hold=300),
            trade(day, -1.0, symbol="JUMPY", atr=8.0, hold=300),
        ]
        config = pf.PortfolioConfig(max_concurrent=1, max_portfolio_risk_pct=100.0)
        result = pf.simulate(
            trades, [], config, point_in_time=False, label="ranked",
            atr_adjustment={"under 2%": 0.2, "over 6%": -0.2},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.trades_taken, 1)
        # The single slot must go to the quiet name, which is the winner here.
        self.assertGreater(result.avg_r, 0)


class BenchmarkTests(unittest.TestCase):
    def _universe(self, tmp: Path, returns: list[float]) -> Path:
        payload = {
            "funds": [
                {"return_3y": value, "max_drawdown": -30.0} for value in returns
            ]
        }
        (tmp / "mf_universe.json").write_text(json.dumps(payload), encoding="utf-8")
        return tmp

    def _result(self, cagr: float, drawdown: float = -20.0) -> pf.PortfolioResult:
        return pf.PortfolioResult(
            label="playbook_held_out", start="2022-11-28", end="2026-09-16", years=3.8,
            starting_equity=1_000_000.0, ending_equity=1_400_000.0, cagr_pct=cagr,
            max_drawdown_pct=drawdown, sharpe=0.5, total_return_pct=40.0,
            trades_taken=200, signals_declined=5000, win_rate=32.0, avg_r=0.3,
            payoff=3.0, exposure_pct=80.0,
        )

    def test_percentile_places_the_bot_in_the_real_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = self._universe(Path(tmp), [float(i) for i in range(100)])
            comparison = bm.compare(self._result(75.0), data_dir)
            self.assertIsNotNone(comparison)
            self.assertAlmostEqual(comparison.percentile, 75.0, delta=2.0)
            self.assertTrue(comparison.beats_median)

    def test_a_tiny_fund_sample_produces_no_comparison(self) -> None:
        """A percentile over ten funds is not a ranking against professionals."""
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = self._universe(Path(tmp), [10.0] * 10)
            self.assertIsNone(bm.compare(self._result(20.0), data_dir))

    def test_scorecard_reports_losing_dimensions_as_losses(self) -> None:
        """A worse number must never be rendered as a win."""
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = self._universe(Path(tmp), [float(i) for i in range(60, 160)])
            comparison = bm.compare(self._result(5.0), data_dir)
            self.assertIsNotNone(comparison)
            self.assertFalse(comparison.beats_median)
            returns = {d["dimension"]: d["verdict"] for d in comparison.scorecard}
            self.assertFalse(returns["Return vs the median professional fund"])

    def test_caveats_always_travel_with_the_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = self._universe(Path(tmp), [float(i) for i in range(100)])
            payload = bm.build_benchmark([self._result(20.0)], data_dir)
            self.assertTrue(payload["caveats"])
            self.assertTrue(any("simulated" in c for c in payload["caveats"]))
            self.assertTrue(any("cash" in c for c in payload["caveats"]))


if __name__ == "__main__":
    unittest.main()
