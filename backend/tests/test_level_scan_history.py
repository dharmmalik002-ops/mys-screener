"""A 52-week or all-time high is only one when the app holds that much of the
stock's history. 192 of the companies NSE admitted in bulk arrived with only
their NSE bars, so every short rally read as a fresh 52-week / all-time high
for companies that have traded on BSE for decades (ELANTAS, TIMEX, KENNAMET).

The evidence is the long moving average, not `history_bars` — the daily patch
rolls prices forward without bumping that count.

Run: `cd backend && pytest tests/test_level_scan_history.py`
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta
from types import SimpleNamespace

from app.scanners import definitions as d

TODAY = date.today()
BATCH = TODAY - timedelta(days=42)


def _snap(symbol: str, listed: date | None, sma200: float | None = None, sma150: float | None = None):
    return SimpleNamespace(symbol=symbol, listing_date=listed, sma200=sma200, sma150=sma150, history_bars=2)


class HistoryCoversWindowTests(unittest.TestCase):
    batch = {BATCH}

    def covers(self, snap, need):
        return d._history_covers_window(snap, need, self.batch, TODAY)

    def test_an_established_stock_qualifies(self):
        reliance = _snap("RELIANCE", date(1995, 11, 29), sma200=1400.0, sma150=1420.0)
        self.assertTrue(self.covers(reliance, None))
        self.assertTrue(self.covers(reliance, 240))

    def test_a_bulk_admission_without_long_history_does_not(self):
        timex = _snap("TIMEX", BATCH)
        self.assertFalse(self.covers(timex, 240))
        self.assertFalse(self.covers(timex, None))

    def test_a_bulk_admission_that_carries_real_history_does(self):
        # KMCSHIL: history_bars read 2, but it has a real 200-day average.
        self.assertTrue(self.covers(_snap("KMCSHIL", BATCH, sma200=103.9, sma150=112.1), 240))

    def test_a_genuine_ipo_qualifies_without_long_averages(self):
        self.assertTrue(self.covers(_snap("MILKYMIST", TODAY - timedelta(days=41)), 240))
        self.assertTrue(self.covers(_snap("MILKYMIST", TODAY - timedelta(days=41)), None))

    def test_an_old_stock_with_a_hole_in_its_history_does_not(self):
        self.assertFalse(self.covers(_snap("HMT", date(2003, 8, 29)), 240))
        self.assertFalse(self.covers(_snap("E2E", None), 240))

    def test_six_month_windows_need_only_the_150_day_average(self):
        self.assertTrue(self.covers(_snap("X", date(2010, 1, 1), sma150=50.0), 120))
        self.assertFalse(self.covers(_snap("X", date(2010, 1, 1), sma150=50.0), 240))


class RunScanGateTests(unittest.TestCase):
    def _universe(self):
        batch = [_snap(f"OLD{i}", BATCH) for i in range(d.IPO_BATCH_LISTING_MIN + 2)]
        established = [_snap(f"EST{i}", date(2000, 1, 1), sma200=100.0, sma150=100.0) for i in range(20)]
        return batch + established + [_snap("IPO", TODAY - timedelta(days=20))]

    def _seen(self, scan_id, universe):
        seen: list[str] = []
        run = d.ScanDefinition(scan_id, scan_id, "Core", "", lambda s: seen.append(s.symbol))
        d.run_scan(run, universe)
        return seen

    def test_long_window_scans_skip_bulk_admissions_without_history(self):
        for scan_id in ("high-52w", "all-time-high", "breakout-ath", "six-month-high"):
            seen = self._seen(scan_id, self._universe())
            self.assertFalse(any(s.startswith("OLD") for s in seen), scan_id)
            self.assertIn("EST0", seen)
            self.assertIn("IPO", seen)

    def test_short_window_scans_are_untouched(self):
        self.assertEqual(len(self._seen("week-high", self._universe())), len(self._universe()))

    def test_a_cache_without_moving_averages_disables_the_gate(self):
        blank = [_snap(f"S{i}", None) for i in range(10)]
        self.assertEqual(len(self._seen("high-52w", blank)), 10)


if __name__ == "__main__":
    unittest.main()
