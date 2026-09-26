"""`breadth_today.date` is the session the snapshots describe, not the calendar.

The frontend expires any cached chart whose newest bar predates this date. When
it carried today's calendar date, every weekend, holiday and pre-close weekday
threw away every prewarmed chart, and each chart open waited on the network.

Run: `cd backend && pytest tests/test_breadth_today_session.py`
"""

from __future__ import annotations

import unittest
from datetime import date

from app.models.market import StockSnapshot
from app.services.dashboard_service import DashboardService


def _snapshot(session: date | None, change: float) -> StockSnapshot:
    return StockSnapshot.model_validate({
        "symbol": "T", "name": "T", "exchange": "NSE", "sector": "Industrials",
        "sub_sector": "Capital Goods", "market_cap_crore": 2000.0, "last_price": 100.0,
        "change_pct": change, "volume": 100000, "avg_volume_20d": 100000,
        "day_high": 101.0, "day_low": 99.0, "ath": 200.0, "high_52w": 190.0, "low_52w": 60.0,
        "range_high_20d": 110.0, "benchmark_return_20d": 1.0, "sector_return_20d": 1.0,
        "pivot_high": 110.0, "darvas_high": 110.0, "darvas_low": 95.0,
        "pullback_depth_pct": 2.0, "trend_strength": 0.8,
        "history_session_date": session,
    })


class BreadthTodaySessionTests(unittest.TestCase):
    def test_date_is_the_newest_session_not_the_calendar(self) -> None:
        # Friday's session read on a Saturday must still say Friday.
        rows = [_snapshot(date(2026, 9, 25), 1.0), _snapshot(date(2026, 9, 24), -1.0), _snapshot(None, 0.0)]
        counts = DashboardService._breadth_today_from_snapshots(rows)
        assert counts is not None
        self.assertEqual(counts.date, "2026-09-25")
        self.assertEqual((counts.advances, counts.declines, counts.unchanged), (1, 1, 1))

    def test_falls_back_to_a_date_when_no_row_knows_its_session(self) -> None:
        counts = DashboardService._breadth_today_from_snapshots([_snapshot(None, 0.0)])
        assert counts is not None
        self.assertRegex(counts.date, r"^\d{4}-\d{2}-\d{2}$")


if __name__ == "__main__":
    unittest.main()
