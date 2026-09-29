"""The IPO screener's Min Liquidity setting must filter by what it says.

It once exempted rows reporting 0 turnover, because the pipeline left recent
listings at 0 and any threshold emptied the list. That was fixed at the source
(seed rows now get the patch's indicator block — see
test_ipo_listing_completeness.py), after which the exemption only let "5 Cr"
show listings nobody could show trade 5 Cr.

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

    def test_unknown_turnover_fails_a_floor(self):
        # An unknown number cannot be shown to clear a floor — the same rule
        # the universe gate applies.
        rows = [_match("FRESH", 0.0, "2026-09-08"), _match("ALSOFRESH", None, "2026-09-07")]
        self.assertEqual(self.f(rows, 5.0), [])

    def test_reported_turnover_below_threshold_is_still_removed(self):
        # The filter must keep working for listings that DO report turnover.
        rows = [_match("THIN", 0.4, "2026-05-01"), _match("LIQUID", 9.0, "2026-05-01")]
        self.assertEqual([m.symbol for m in self.f(rows, 5.0)], ["LIQUID"])

    def test_threshold_boundary_is_inclusive(self):
        self.assertEqual(len(self.f([_match("X", 5.0, "2026-05-01")], 5.0)), 1)

    def test_ipo_filter_matches_every_other_scanner(self):
        rows = [_match("A", 0.0, "2026-09-08"), _match("B", 9.0, "2026-05-01"), _match("C", 4.9, "2026-05-01")]
        strict = DashboardService._filter_scan_items_by_liquidity(rows, 5.0)
        self.assertEqual(self.f(rows, 5.0), strict)
        self.assertEqual([m.symbol for m in strict], ["B"])


if __name__ == "__main__":
    unittest.main()
