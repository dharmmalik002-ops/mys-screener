"""The IPO screener must not be emptied by its own liquidity filter.

`avg_rupee_volume_30d_crore` is a 30-session average and does not exist for a
stock listed days ago; the pipeline also leaves it at 0 for recent listings
generally. Measured on the live API: unfiltered the scan returns 322 rows with
the newest a day old, but at >= 1 crore it returned 77 rows whose newest was
five months stale -- the filter was deleting the subject of the screener.

Run: `cd backend && pytest tests/test_ipo_liquidity_filter.py`
"""

from __future__ import annotations

import unittest
from datetime import date

from app.models.market import ScanMatch
from app.services.dashboard_service import DashboardService


def _match(symbol: str, turnover: float | None, listed: str) -> ScanMatch:
    return ScanMatch(
        scan_id="ipo", symbol=symbol, name=symbol, exchange="NSE",
        listing_date=date.fromisoformat(listed), sector="Capital Goods",
        market_cap_crore=0.0, last_price=100.0, change_pct=1.0,
        relative_volume=1.0, avg_rupee_volume_30d_crore=turnover, score=100.0,
    )


class IpoLiquidityFilterTests(unittest.TestCase):
    f = staticmethod(DashboardService._filter_ipo_items_by_liquidity)

    def test_no_threshold_keeps_everything(self):
        rows = [_match("A", 0.0, "2026-09-08"), _match("B", 12.0, "2025-11-01")]
        self.assertEqual(len(self.f(rows, None)), 2)

    def test_unknown_turnover_is_kept_not_dropped(self):
        # The regression: a brand-new listing reports 0 and was deleted by any
        # threshold, so the newest IPO in the list became months old.
        rows = [_match("FRESH", 0.0, "2026-09-08"), _match("ALSOFRESH", None, "2026-09-07")]
        self.assertEqual([m.symbol for m in self.f(rows, 5.0)], ["FRESH", "ALSOFRESH"])

    def test_reported_turnover_below_threshold_is_still_removed(self):
        # The filter must keep working for listings that DO report turnover.
        rows = [_match("THIN", 0.4, "2026-05-01"), _match("LIQUID", 9.0, "2026-05-01")]
        self.assertEqual([m.symbol for m in self.f(rows, 5.0)], ["LIQUID"])

    def test_threshold_boundary_is_inclusive(self):
        self.assertEqual(len(self.f([_match("X", 5.0, "2026-05-01")], 5.0)), 1)

    def test_shared_filter_is_unchanged_for_other_scans(self):
        # Only the IPO path loosens; every other scanner keeps strict semantics.
        rows = [_match("A", 0.0, "2026-09-08"), _match("B", 9.0, "2026-05-01")]
        strict = DashboardService._filter_scan_items_by_liquidity(rows, 5.0)
        self.assertEqual([m.symbol for m in strict], ["B"])


if __name__ == "__main__":
    unittest.main()
