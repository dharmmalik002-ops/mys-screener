"""The committed daily-close artifact and how it is spliced onto a snapshot.

Run: `cd backend && pytest tests/test_close_history.py`
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone

from app.models.market import StockSnapshot
from app.services import close_history


def _snapshot(session: date | None, recent: list[float]) -> StockSnapshot:
    return StockSnapshot.model_validate({
        "symbol": "T", "name": "T", "exchange": "NSE", "sector": "Industrials",
        "sub_sector": "Capital Goods", "market_cap_crore": 2000.0, "last_price": recent[-1] if recent else 100.0,
        "change_pct": 0.0, "volume": 100000, "avg_volume_20d": 100000,
        "day_high": 101.0, "day_low": 99.0, "ath": 200.0, "high_52w": 190.0, "low_52w": 60.0,
        "range_high_20d": 110.0, "benchmark_return_20d": 1.0, "sector_return_20d": 1.0,
        "pivot_high": 110.0, "darvas_high": 110.0, "darvas_low": 95.0,
        "pullback_depth_pct": 2.0, "trend_strength": 0.8,
        "recent_closes": recent,
        "history_session_date": session,
    })


class CloseHistorySpliceTests(unittest.TestCase):
    def setUp(self) -> None:
        close_history.reset_cache()
        self.artifact_date = date(2026, 9, 1)
        stamp = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())
        close_history._cache = {
            "T": {"last_time": stamp, "closes": [100.0 + i for i in range(200)]},
        }

    def tearDown(self) -> None:
        close_history.reset_cache()

    def test_same_session_needs_no_splice(self) -> None:
        closes = close_history.closes_for(_snapshot(self.artifact_date, [299.0]))
        self.assertEqual(len(closes), 200)
        self.assertEqual(closes[-1], 299.0)

    def test_drift_is_healed_from_the_trailing_closes(self) -> None:
        """A week of drift appends exactly that week, not the whole window."""
        session = self.artifact_date + timedelta(days=7)  # 5 sessions
        recent = [400.0 + i for i in range(20)]
        closes = close_history.closes_for(_snapshot(session, recent))
        self.assertEqual(len(closes), 205)
        self.assertEqual(closes[-5:], recent[-5:])

    def test_drift_beyond_the_trailing_window_is_refused(self) -> None:
        """Welding two non-adjacent stretches together would misplace every
        base measured across the seam, so the artifact is treated as unusable."""
        session = self.artifact_date + timedelta(days=90)
        closes = close_history.closes_for(_snapshot(session, [400.0 + i for i in range(20)]))
        self.assertEqual(closes, [])

    def test_unknown_symbol_returns_empty(self) -> None:
        close_history._cache = {}
        self.assertEqual(close_history.closes_for(_snapshot(self.artifact_date, [100.0])), [])

    def test_missing_artifact_is_survivable(self) -> None:
        close_history.reset_cache()
        original = close_history.ARTIFACT_PATH
        try:
            close_history.ARTIFACT_PATH = original.with_name("does_not_exist.json")
            self.assertEqual(close_history.closes_for(_snapshot(self.artifact_date, [100.0])), [])
        finally:
            close_history.ARTIFACT_PATH = original
            close_history.reset_cache()


class ShippedArtifactTests(unittest.TestCase):
    def test_artifact_ships_with_the_repo(self) -> None:
        """It is committed for the same reason sector_indices.json is: the Space
        cannot rebuild it, and without it Power Base and VCP measure nothing."""
        close_history.reset_cache()
        symbols = close_history._load()
        self.assertGreater(len(symbols), 800)
        sample = next(iter(symbols.values()))
        self.assertGreaterEqual(len(sample["closes"]), 60)


if __name__ == "__main__":
    unittest.main()
