"""Unit tests for `sparkline_closes`, the compact per-row close series.

The interesting case is the anchor: `StockSnapshot.recent_closes` may or may
not already end on the current session, depending on whether the bhavcopy patch
has been applied to that row. Get it wrong in one direction and the sparkline
misses today's move; get it wrong in the other and it draws today twice.

Run: `cd backend && pytest tests/test_sparkline_closes.py`
"""

from __future__ import annotations

import unittest

from app.models.market import StockSnapshot
from app.scanners.definitions import SPARKLINE_MAX_POINTS, sparkline_closes


def _make_snapshot(**overrides) -> StockSnapshot:
    closes = overrides.pop("recent_closes", [100.0 + i for i in range(30)])
    last_price = overrides.pop("last_price", 130.0)
    payload = {
        "symbol": "SPARK",
        "name": "Spark Test Ltd",
        "exchange": "NSE",
        "sector": "Industrials",
        "market_cap_crore": 5000.0,
        "last_price": last_price,
        "change_pct": 1.0,
        "volume": 100_000,
        "avg_volume_20d": 100_000,
        "day_high": last_price * 1.01,
        "day_low": last_price * 0.99,
        "ath": 200.0,
        "high_52w": 190.0,
        "range_high_20d": 150.0,
        "benchmark_return_20d": 1.0,
        "sector_return_20d": 1.0,
        "pivot_high": 150.0,
        "darvas_high": 150.0,
        "darvas_low": 120.0,
        "pullback_depth_pct": 2.0,
        "trend_strength": 0.8,
        "recent_closes": list(closes),
    }
    payload.update(overrides)
    return StockSnapshot(**payload)


class SparklineClosesTests(unittest.TestCase):
    def test_series_is_capped_and_oldest_first(self):
        snapshot = _make_snapshot(recent_closes=[float(i) for i in range(1, 41)])
        series = sparkline_closes(snapshot)
        self.assertLessEqual(len(series), SPARKLINE_MAX_POINTS)
        self.assertEqual(series, sorted(series), "series must stay oldest-first")

    def test_appends_last_price_when_tail_is_yesterday(self):
        # closes[-1] equals previous_close, so the stored tail is yesterday and
        # today's price has to be appended or the sparkline lags a session.
        snapshot = _make_snapshot(
            recent_closes=[100.0, 101.0, 102.0],
            previous_close=102.0,
            last_price=105.0,
            change_pct=2.94,
        )
        series = sparkline_closes(snapshot)
        self.assertEqual(series[-1], 105.0)
        self.assertEqual(series, [100.0, 101.0, 102.0, 105.0])

    def test_does_not_duplicate_today_when_tail_is_already_current(self):
        # The recorded change between the last two closes matches change_pct,
        # so closes[-1] IS today. Appending last_price would draw it twice.
        snapshot = _make_snapshot(
            recent_closes=[100.0, 102.0],
            last_price=102.0,
            change_pct=2.0,
        )
        series = sparkline_closes(snapshot)
        self.assertEqual(series, [100.0, 102.0])
        self.assertEqual(len(series), 2)

    def test_drops_missing_and_non_positive_values(self):
        snapshot = _make_snapshot(recent_closes=[100.0, 0.0, 102.0, -5.0, 103.0])
        series = sparkline_closes(snapshot)
        self.assertNotIn(0.0, series)
        self.assertTrue(all(value > 0 for value in series))

    def test_empty_history_yields_empty_series_not_a_fabricated_shape(self):
        self.assertEqual(sparkline_closes(_make_snapshot(recent_closes=[])), [])

    def test_respects_an_explicit_limit(self):
        snapshot = _make_snapshot(recent_closes=[float(i) for i in range(1, 41)])
        self.assertEqual(len(sparkline_closes(snapshot, limit=5)), 5)


if __name__ == "__main__":
    unittest.main()
