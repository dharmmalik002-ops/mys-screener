"""Bread & Butter runs outside the static scan catalog, so /api/scan-counts had
no row for it and the Screener sidebar badge read 0 while the scan held 110.

Run: `cd backend && pytest tests/test_scan_counts_bread_butter.py`
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from app.models.market import ScanDescriptor
from app.services import dashboard_service as ds


class ScanCountsBreadButterTests(unittest.TestCase):
    def _service(self):
        service = ds.DashboardService.__new__(ds.DashboardService)

        async def snapshots():
            return ["a", "b", "c"]

        service._snapshots = snapshots  # type: ignore[method-assign]
        service._scan_eligible_snapshots = lambda snaps: snaps  # type: ignore[method-assign]
        service._scan_catalog = lambda snaps: (  # type: ignore[method-assign]
            [ScanDescriptor(id="vcp", name="VCP", category="Setups", description="", hit_count=6)],
            {},
        )
        return service

    def test_the_badge_counts_what_the_scan_returns(self):
        with mock.patch.object(ds, "run_bread_butter_scan", return_value=[object()] * 110):
            rows = asyncio.run(self._service().get_scan_counts())
        counts = {row.id: row.hit_count for row in rows}
        self.assertEqual(counts["bread-butter"], 110)
        self.assertEqual(counts["vcp"], 6)

    def test_a_failing_scan_does_not_take_the_counts_down(self):
        with mock.patch.object(ds, "run_bread_butter_scan", side_effect=RuntimeError("boom")):
            rows = asyncio.run(self._service().get_scan_counts())
        self.assertEqual([row.id for row in rows], ["vcp"])


if __name__ == "__main__":
    unittest.main()
