"""NSE's DATE OF LISTING dates a company's arrival ON NSE. When NSE admits
BSE-only companies in bulk, dozens share one date -- 110 on 2026-08-17 -- and
every one of those decades-old companies read as an "IPO" (148 of 319 rows).

Run: `cd backend && pytest tests/test_ipo_batch_listings.py`
"""

from __future__ import annotations

import unittest
from datetime import date, timedelta
from types import SimpleNamespace

from app.scanners.definitions import (
    IPO_BATCH_LISTING_MIN,
    ScanDefinition,
    ipo_batch_listing_dates,
    run_scan,
)

# Relative to the real today: run_scan dates the one-year window itself.
TODAY = date.today()
BATCH = TODAY - timedelta(days=42)


def _snap(symbol: str, listed: date | None):
    return SimpleNamespace(symbol=symbol, listing_date=listed)


def _universe():
    batch = [_snap(f"OLD{i}", BATCH) for i in range(IPO_BATCH_LISTING_MIN + 5)]
    real_day = TODAY - timedelta(days=11)
    real = [_snap(f"IPO{i}", real_day) for i in range(6)]  # busiest real IPO day
    return batch + real + [_snap("NEW", TODAY - timedelta(days=3)), _snap("NODATE", None)]


class IpoBatchListingTests(unittest.TestCase):
    def test_a_bulk_listing_day_is_detected(self):
        self.assertEqual(ipo_batch_listing_dates(_universe(), TODAY), {BATCH})

    def test_a_busy_genuine_ipo_day_is_not(self):
        self.assertNotIn(TODAY - timedelta(days=11), ipo_batch_listing_dates(_universe(), TODAY))

    def test_listings_older_than_a_year_are_ignored(self):
        old = [_snap(f"X{i}", TODAY - timedelta(days=400)) for i in range(30)]
        self.assertEqual(ipo_batch_listing_dates(old, TODAY), set())

    def test_the_ipo_scan_never_evaluates_a_batch_listing(self):
        seen: list[str] = []
        probe = ScanDefinition("ipo", "IPO", "Core", "", lambda s: seen.append(s.symbol))
        run_scan(probe, _universe())
        self.assertFalse(any(sym.startswith("OLD") for sym in seen))
        self.assertIn("IPO0", seen)
        self.assertIn("NEW", seen)

    def test_other_scans_are_untouched(self):
        seen: list[str] = []
        probe = ScanDefinition("day-high", "Day High", "Core", "", lambda s: seen.append(s.symbol))
        run_scan(probe, _universe())
        self.assertEqual(len(seen), len(_universe()))


if __name__ == "__main__":
    unittest.main()
