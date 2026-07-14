"""Tests for the Positive Earnings pipeline: A/B evaluator grading, the
best-pop reaction math, and the merge/prune/never-wipe file semantics."""
from __future__ import annotations

import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models.market import StockSnapshot
from app.scanners.definitions import current_earnings_season_start, make_positive_earnings_evaluator
from app.services.earnings_metrics import (
    _compute_one,
    load_calendar_file,
    load_metrics_file,
    save_calendar_file,
    save_metrics_file,
)


def _earnings_snapshot(**overrides) -> StockSnapshot:
    base = dict(
        symbol="TEST",
        name="Test Industries",
        exchange="NSE",
        sector="Test",
        sub_sector="Test",
        market_cap_crore=5000,
        last_price=120.0,
        change_pct=1.0,
        volume=1_000_000,
        avg_volume_20d=500_000,
    )
    base.update(overrides)
    return StockSnapshot.model_construct(**base)


class PositiveEarningsEvaluatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.evaluate = make_positive_earnings_evaluator()
        self.recent = date.today() - timedelta(days=3)

    def test_grade_a_full_pattern_passes(self) -> None:
        snapshot = _earnings_snapshot(
            latest_earnings_date=self.recent,
            earnings_close_in_range_pct=0.9,
            earnings_next_day_gap_pct=3.0,
            earnings_day_rvol_50d=4.0,
            earnings_return_5d_pct=15.0,
        )
        result = self.evaluate(snapshot)
        self.assertIsNotNone(result)
        score, reasons = result
        self.assertIn("Grade A", reasons[0])
        self.assertGreater(score, 50)

    def test_grade_b_pop_passes_without_follow_through(self) -> None:
        # JUSTDIAL-style: +20% on results day, 30x volume, no 5-day data yet.
        snapshot = _earnings_snapshot(
            latest_earnings_date=self.recent,
            earnings_best_pop_pct=20.0,
            earnings_best_pop_rvol=30.0,
            earnings_return_5d_pct=None,
            earnings_close_in_range_pct=None,
            earnings_next_day_gap_pct=None,
            earnings_day_rvol_50d=None,
        )
        result = self.evaluate(snapshot)
        self.assertIsNotNone(result)
        _score, reasons = result
        self.assertIn("Grade B", reasons[0])

    def test_grade_a_outranks_grade_b(self) -> None:
        grade_a = self.evaluate(_earnings_snapshot(
            latest_earnings_date=self.recent,
            earnings_close_in_range_pct=0.8,
            earnings_next_day_gap_pct=1.5,
            earnings_day_rvol_50d=2.5,
            earnings_return_5d_pct=11.0,
        ))
        grade_b = self.evaluate(_earnings_snapshot(
            latest_earnings_date=self.recent,
            earnings_best_pop_pct=40.0,
            earnings_best_pop_rvol=12.0,
        ))
        assert grade_a is not None and grade_b is not None
        self.assertGreater(grade_a[0], grade_b[0])

    def test_weak_pop_fails_both_tiers(self) -> None:
        snapshot = _earnings_snapshot(
            latest_earnings_date=self.recent,
            earnings_best_pop_pct=3.0,   # < 5%
            earnings_best_pop_rvol=4.0,
            earnings_return_5d_pct=4.0,  # < 10%
            earnings_close_in_range_pct=0.9,
            earnings_next_day_gap_pct=2.0,
            earnings_day_rvol_50d=3.0,
        )
        self.assertIsNone(self.evaluate(snapshot))

    def test_stale_earnings_date_fails(self) -> None:
        snapshot = _earnings_snapshot(
            latest_earnings_date=date.today() - timedelta(days=90),
            earnings_best_pop_pct=20.0,
            earnings_best_pop_rvol=30.0,
        )
        self.assertIsNone(self.evaluate(snapshot))

    def test_previous_season_excluded_even_within_lookback(self) -> None:
        # A result from just before the current season start must be rejected
        # even though it's inside the 60-day outer bound.
        season_start = current_earnings_season_start(date.today())
        prev_season = season_start - timedelta(days=2)
        if (date.today() - prev_season).days > 60:
            self.skipTest("early in a season — no prior-season date inside 60d window")
        snapshot = _earnings_snapshot(
            latest_earnings_date=prev_season,
            earnings_best_pop_pct=20.0,
            earnings_best_pop_rvol=30.0,
        )
        self.assertIsNone(self.evaluate(snapshot))


class EarningsSeasonStartTests(unittest.TestCase):
    def test_season_starts_map_to_reporting_months(self) -> None:
        self.assertEqual(current_earnings_season_start(date(2026, 7, 14)), date(2026, 7, 1))
        self.assertEqual(current_earnings_season_start(date(2026, 8, 30)), date(2026, 7, 1))
        self.assertEqual(current_earnings_season_start(date(2026, 9, 30)), date(2026, 7, 1))
        self.assertEqual(current_earnings_season_start(date(2026, 10, 3)), date(2026, 10, 1))
        self.assertEqual(current_earnings_season_start(date(2026, 2, 5)), date(2026, 1, 1))
        self.assertEqual(current_earnings_season_start(date(2026, 5, 20)), date(2026, 4, 1))


class ComputeOneReactionTests(unittest.TestCase):
    def _synthetic_bars(self) -> tuple[list[dict], date]:
        """60 flat sessions at 100 / 100k volume, then a +20% results day on
        3M volume and a follow-through day. Returns (bars, event_date)."""
        bars: list[dict] = []
        # Start far enough back that the event day lands in the PAST (the
        # calendar anchor only applies once the meeting date has passed).
        start = datetime.now(timezone.utc) - timedelta(days=95)
        cursor = start
        added = 0
        while added < 60:
            if cursor.weekday() < 5:
                bars.append({
                    "time": int(cursor.timestamp()),
                    "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
                    "volume": 100_000,
                })
                added += 1
            cursor += timedelta(days=1)
        while cursor.weekday() >= 5:
            cursor += timedelta(days=1)
        event_date = cursor.date()
        bars.append({
            "time": int(cursor.timestamp()),
            "open": 105.0, "high": 121.0, "low": 104.0, "close": 120.0,
            "volume": 3_000_000,
        })
        cursor += timedelta(days=1)
        while cursor.weekday() >= 5:
            cursor += timedelta(days=1)
        bars.append({
            "time": int(cursor.timestamp()),
            "open": 122.0, "high": 126.0, "low": 121.0, "close": 125.0,
            "volume": 1_500_000,
        })
        return bars, event_date

    def test_best_pop_computed_from_bse_anchor(self) -> None:
        bars, event_date = self._synthetic_bars()
        metrics = _compute_one(
            "TEST", "TEST.NS",
            lambda _s, _t: bars,
            60,
            bse_filing_date=event_date,
            use_yfinance_dates=False,
        )
        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertEqual(metrics.earnings_date, event_date)
        self.assertAlmostEqual(metrics.best_pop_pct or 0, 20.0, delta=0.2)
        self.assertAlmostEqual(metrics.best_pop_rvol or 0, 30.0, delta=0.5)
        self.assertEqual(metrics.source, "bse")

    def test_calendar_anchor_used_when_no_filing(self) -> None:
        bars, event_date = self._synthetic_bars()
        metrics = _compute_one(
            "TEST", "TEST.NS",
            lambda _s, _t: bars,
            60,
            bse_filing_date=None,
            calendar_date=event_date,
            use_yfinance_dates=False,
        )
        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertEqual(metrics.source, "bse-calendar")
        self.assertEqual(metrics.earnings_date, event_date)


class MetricsFileTests(unittest.TestCase):
    def test_roundtrip_preserves_new_fields(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            entries = {
                "JUSTDIAL": {
                    "symbol": "JUSTDIAL",
                    "earnings_date": "2026-07-10",
                    "close_in_range_pct": 0.95,
                    "next_day_gap_pct": 8.0,
                    "day_rvol_50d": 5.0,
                    "return_5d_pct": 20.0,
                    "source": "bse",
                    "best_pop_pct": 20.0,
                    "best_pop_rvol": 33.0,
                },
            }
            save_metrics_file(root, entries)
            loaded = load_metrics_file(root)
            self.assertEqual(loaded["JUSTDIAL"]["best_pop_pct"], 20.0)
            self.assertEqual(loaded["JUSTDIAL"]["best_pop_rvol"], 33.0)

    def test_calendar_roundtrip(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            save_calendar_file(root, {"RELIANCE": "2026-07-18", "TCS": "2026-07-15"})
            loaded = load_calendar_file(root)
            self.assertEqual(loaded["RELIANCE"], "2026-07-18")
            self.assertEqual(len(loaded), 2)


class PruneStaleTests(unittest.TestCase):
    def test_prune_drops_only_old_entries(self) -> None:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        from compute_earnings_metrics import _prune_stale

        fresh_date = (date.today() - timedelta(days=5)).isoformat()
        stale_date = (date.today() - timedelta(days=90)).isoformat()
        entries = {
            "FRESH": {"earnings_date": fresh_date},
            "STALE": {"earnings_date": stale_date},
            "BROKEN": {"earnings_date": None},
        }
        pruned = _prune_stale(entries, 60)
        self.assertIn("FRESH", pruned)
        self.assertNotIn("STALE", pruned)
        self.assertNotIn("BROKEN", pruned)


if __name__ == "__main__":
    unittest.main()
