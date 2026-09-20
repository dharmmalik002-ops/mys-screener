"""Tests for market-driven bet sizing and the run memory.

The sizing rule is the one form of adaptation this project has not already
measured as harmful, and the reason is narrow: it reads the MARKET, which
carries information (gotcha 63), and never the bot's own recent P&L, which
does not (gotchas 40, 65, 69, 74). These tests pin that boundary, because a
later edit that "improves" it by feeding in recent returns would quietly turn
it into the loop that failed everywhere else.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from app.services.bot import adaptive_sizing as ad
from app.services.bot import memory as mem


class ScaleTests(unittest.TestCase):

    def test_a_broad_strong_market_gets_the_full_multiplier(self):
        self.assertEqual(ad.scale_for("bull_strong", 80.0, True), ad.STRONG_SCALE)

    def test_a_narrow_rally_is_not_pressed(self):
        """Strong label, thin participation — normal size, not double."""
        self.assertEqual(ad.scale_for("bull_strong", 40.0, True), ad.NORMAL_SCALE)

    def test_an_unhealthy_regime_sizes_down(self):
        for regime in ("bear", "correction", "choppy"):
            with self.subTest(regime=regime):
                self.assertEqual(ad.scale_for(regime, 90.0, True), ad.WEAK_SCALE)

    def test_below_the_200dma_sizes_down_however_broad_it_looks(self):
        self.assertEqual(ad.scale_for("bull_strong", 95.0, False), ad.WEAK_SCALE)

    def test_a_missing_input_sizes_down_rather_than_up(self):
        """Unknown must never read as confirmation."""
        self.assertEqual(ad.scale_for("bull_strong", None, None), ad.WEAK_SCALE)
        self.assertEqual(ad.scale_for(None, 90.0, True), ad.WEAK_SCALE)
        # Breadth unknown but trend confirmed: normal, never doubled.
        self.assertEqual(ad.scale_for("bull_strong", None, True), ad.NORMAL_SCALE)

    def test_the_multiplier_is_capped(self):
        for regime in ("bull_strong", "bull_narrow", "recovery", "bear"):
            for breadth in (0.0, 50.0, 100.0):
                self.assertLessEqual(ad.scale_for(regime, breadth, True), ad.MAX_SCALE)

    def test_the_schedule_covers_every_session_it_is_given(self):
        days = [date(2020, 1, i + 1) for i in range(5)]
        sched = ad.build_schedule({d: "bull_strong" for d in days},
                                  {d: 80.0 for d in days},
                                  {d: True for d in days})
        self.assertEqual(set(sched), set(days))


class MemoryTests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_an_empty_store_says_so_rather_than_guessing(self):
        r = mem.recall(self.dir)
        self.assertEqual(r.runs, 0)
        self.assertIsNone(r.dominant_verdict)

    def test_the_recurring_complaint_is_what_comes_back(self):
        for alpha in (-10.0, -8.0):
            mem.record(self.dir, [
                {"year": 2023, "verdict": "under_deployed", "alpha": alpha},
                {"year": 2024, "verdict": "ok", "alpha": 40.0},
            ], {"cagr": 34.5})
        r = mem.recall(self.dir)
        self.assertEqual(r.dominant_verdict, "under_deployed")
        self.assertEqual(r.chronic_years, [2023])

    def test_a_year_behind_only_once_is_not_called_chronic(self):
        """Chasing a one-off is how six rounds were spent."""
        mem.record(self.dir, [{"year": 2019, "verdict": "bad_shots", "alpha": -2.0}], {})
        mem.record(self.dir, [{"year": 2019, "verdict": "ok", "alpha": +5.0}], {})
        self.assertEqual(mem.recall(self.dir).chronic_years, [])

    def test_a_corrupt_store_degrades_instead_of_raising(self):
        mem.memory_path(self.dir).write_text("{not json", encoding="utf-8")
        self.assertEqual(mem.load(self.dir), [])
        self.assertEqual(mem.recall(self.dir).runs, 0)

    def test_the_store_does_not_grow_without_bound(self):
        for _ in range(mem.MAX_RUNS + 10):
            mem.record(self.dir, [{"year": 2024, "verdict": "ok", "alpha": 1.0}], {})
        self.assertEqual(len(mem.load(self.dir)), mem.MAX_RUNS)


if __name__ == "__main__":
    unittest.main()
