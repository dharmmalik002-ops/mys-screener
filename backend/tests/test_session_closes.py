"""The base scanners measure their windows in SESSIONS, not in list slots.

`chart_grid_points` downsamples 520 bars into 240 points, so a point is ~2.17
sessions and the step is not constant across symbols. Counting points as
sessions stretched every window and made one constant mean different things for
different stocks. These tests pin the conversion.

Run: `cd backend && pytest tests/test_session_closes.py`
"""

from __future__ import annotations

import unittest

from app.models.market import StockSnapshot
from app.scanners.definitions import _session_closes

BASE_EPOCH = 1_750_000_000


def _snapshot(grid_points: list[dict], recent_closes: list[float] | None = None) -> StockSnapshot:
    return StockSnapshot.model_validate({
        "symbol": "T", "name": "T", "exchange": "NSE", "sector": "Industrials",
        "sub_sector": "Capital Goods", "market_cap_crore": 2000.0, "last_price": 100.0,
        "change_pct": 0.0, "volume": 100000, "avg_volume_20d": 100000,
        "day_high": 101.0, "day_low": 99.0, "ath": 120.0, "high_52w": 118.0, "low_52w": 60.0,
        "range_high_20d": 110.0, "benchmark_return_20d": 1.0, "sector_return_20d": 1.0,
        "pivot_high": 110.0, "darvas_high": 110.0, "darvas_low": 95.0,
        "pullback_depth_pct": 2.0, "trend_strength": 0.8,
        "chart_grid_points": grid_points,
        "recent_closes": recent_closes or [],
    })


def _grid(count: int, calendar_days_per_point: int) -> list[dict]:
    return [
        {"time": BASE_EPOCH - (count - 1 - i) * calendar_days_per_point * 86400, "value": 100.0 + i}
        for i in range(count)
    ]


class SessionClosesTests(unittest.TestCase):
    def test_sparse_grid_is_read_in_sessions_not_points(self) -> None:
        """A production grid sits ~3 calendar days apart. Ten points back is
        ~21 sessions ago, not 10."""
        series, short = _session_closes(_snapshot(_grid(40, 3)), 60, min_sessions=20)
        self.assertFalse(short)
        ages = [ago for ago, _ in series]
        self.assertEqual(ages[-1], 0)
        self.assertEqual(ages[-11], 21)  # ten points back = 30 calendar days = 21 sessions

    def test_daily_grid_is_one_session_per_point(self) -> None:
        """A short listing's grid is not downsampled, so a point IS a session.
        The same window must not silently mean 2.17x more history for it."""
        series, _ = _session_closes(_snapshot(_grid(40, 7 / 5)), 60, min_sessions=20)
        ages = [ago for ago, _ in series]
        self.assertEqual(ages[-11], 10)

    def test_window_is_clipped_to_max_sessions(self) -> None:
        series, _ = _session_closes(_snapshot(_grid(120, 3)), 45, min_sessions=20)
        self.assertLessEqual(max(ago for ago, _ in series), 45)

    def test_thin_grid_falls_back_to_daily_closes_and_says_so(self) -> None:
        series, short = _session_closes(
            _snapshot(_grid(2, 3), recent_closes=[100.0 + i for i in range(20)]),
            45,
            min_sessions=13,
        )
        self.assertTrue(short)
        self.assertEqual(len(series), 20)
        self.assertEqual([ago for ago, _ in series][:3], [19, 18, 17])

    def test_scans_needing_months_refuse_the_short_fallback(self) -> None:
        """VCP and Power Base cannot measure a three-month leg from 20 closes,
        so they get nothing rather than a base measured over three weeks."""
        series, short = _session_closes(
            _snapshot(_grid(2, 3), recent_closes=[100.0 + i for i in range(20)]),
            150,
            min_sessions=32,
            allow_short_history=False,
        )
        self.assertEqual(series, [])
        self.assertFalse(short)


if __name__ == "__main__":
    unittest.main()
