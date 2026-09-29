"""Expansion tracker keeps sessions nobody opened the scanner on.

Run: `cd backend && pytest tests/test_expansion_backfill.py`
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from app.models.market import StockSnapshot
from app.services import expansion_backfill
from app.services.dashboard_service import DashboardService
from app.services.scan_history_store import ScanHistoryStore

# Weekday sessions from mid-August; 2026-09-14 (Ganesh Chaturthi) is a holiday.
CALENDAR = [
    d.isoformat()
    for d in (date(2026, 8, 17) + timedelta(days=i) for i in range(31))
    if d.weekday() < 5 and d != date(2026, 9, 14)
]


def _snapshot(symbol: str) -> StockSnapshot:
    return StockSnapshot.model_validate({
        "symbol": symbol, "name": symbol, "exchange": "NSE", "sector": "Industrials",
        "sub_sector": "Capital Goods", "market_cap_crore": 2000.0, "last_price": 100.0,
        "change_pct": 0.0, "volume": 100000, "avg_volume_20d": 100000,
        "day_high": 101.0, "day_low": 99.0, "ath": 200.0, "high_52w": 190.0, "low_52w": 60.0,
        "range_high_20d": 110.0, "benchmark_return_20d": 1.0, "sector_return_20d": 1.0,
        "pivot_high": 110.0, "darvas_high": 110.0, "darvas_low": 95.0,
        "pullback_depth_pct": 2.0, "trend_strength": 0.8,
        "recent_closes": [100.0] * 20,
        "recent_volumes": [100000] * 20,
        "history_session_date": date(2026, 9, 16),
    })


def _bars(spike_day: str) -> dict[str, list]:
    """Flat at 100 on 100k shares, with a +10% day on 5x volume."""
    bars = {day: [100.0, 100.0, 100.0, 100.0, 100000] for day in CALENDAR}
    bars[spike_day] = [100.0, 111.0, 100.0, 110.0, 500000]
    return bars


class HitsForSessionTests(unittest.TestCase):
    def test_spike_day_is_a_hit_with_that_days_numbers(self) -> None:
        hits = expansion_backfill.hits_for_session(
            "2026-09-10", [_snapshot("A")], {"A": _bars("2026-09-10")}, CALENDAR
        )
        self.assertEqual([h.symbol for h in hits], ["A"])
        self.assertEqual(hits[0].last_price, 110.0)
        self.assertAlmostEqual(hits[0].change_pct, 10.0)
        self.assertEqual(hits[0].session_date, "2026-09-10")

    def test_quiet_day_is_not(self) -> None:
        hits = expansion_backfill.hits_for_session(
            "2026-09-09", [_snapshot("A")], {"A": _bars("2026-09-10")}, CALENDAR
        )
        self.assertEqual(hits, [])

    def test_prior_close_is_the_previous_session_not_a_holiday(self) -> None:
        # 2026-09-14 is a holiday; a stray EOD file for it must not become the
        # prior close for the 15th.
        bars = _bars("2026-09-15")
        bars["2026-09-14"] = [110.0, 110.0, 110.0, 110.0, 100000]
        hits = expansion_backfill.hits_for_session("2026-09-15", [_snapshot("A")], {"A": bars}, CALENDAR)
        self.assertEqual([h.symbol for h in hits], ["A"])


class _Provider:
    def __init__(self, bars: dict) -> None:
        self._bars = bars

    def _latest_completed_market_session_date(self) -> date:
        return date(2026, 9, 16)

    def _load_recent_eod_bars(self) -> dict:
        return self._bars


class TrackerBackfillTests(unittest.TestCase):
    def test_unopened_session_appears_in_the_tracker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            (data_dir / "nse_breadth_history.json").write_text(
                json.dumps({"days": [{"date": d} for d in CALENDAR]})
            )
            service = DashboardService.__new__(DashboardService)
            service.provider = _Provider({"A": _bars("2026-09-10")})
            service._scan_history_store = ScanHistoryStore(None, data_dir / "scan_history.json")
            service._legacy_data_dir = lambda: data_dir  # type: ignore[method-assign]

            # The scanner is opened only today; nothing fires today.
            merged = service._merge_expansion_history([], [_snapshot("A")])

            self.assertEqual([(m.symbol, m.session_date) for m in merged], [("A", "2026-09-10")])
            history = service._scan_history_store.load("ema-expansion", keep_dates=30)
            self.assertNotIn("2026-09-14", history)
            self.assertIn("2026-09-15", history)

    def test_recorded_session_is_not_rewritten(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            (data_dir / "nse_breadth_history.json").write_text(
                json.dumps({"days": [{"date": d} for d in CALENDAR]})
            )
            service = DashboardService.__new__(DashboardService)
            service.provider = _Provider({"A": _bars("2026-09-10")})
            service._scan_history_store = ScanHistoryStore(None, data_dir / "scan_history.json")
            service._legacy_data_dir = lambda: data_dir  # type: ignore[method-assign]
            service._scan_history_store.record_once("ema-expansion", "2026-09-10", [], keep_dates=30)

            merged = service._merge_expansion_history([], [_snapshot("A")])

            self.assertEqual(merged, [])


if __name__ == "__main__":
    unittest.main()
