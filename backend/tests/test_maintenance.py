from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import WatchlistItem, WatchlistsStateResponse
from app.services.maintenance import _daily_chart_warm_symbols, warm_daily_chart_cache


class StubWarmService:
    def __init__(self, symbols: list[str], *, fail_symbol: str | None = None) -> None:
        self._symbols = symbols
        self._fail_symbol = fail_symbol
        self.provider = self
        self.active_requests = 0
        self.max_active_requests = 0
        self.calls: list[tuple[str, str, int]] = []

    def get_watchlists_state(self) -> WatchlistsStateResponse:
        return WatchlistsStateResponse(
            market="india",
            active_watchlist_id="wl-1",
            watchlists=[
                WatchlistItem(
                    id="wl-1",
                    name="Core",
                    color="#4f8cff",
                    symbols=self._symbols,
                )
            ],
        )

    @staticmethod
    def _chart_bar_limit(timeframe: str) -> int:
        return 1300 if timeframe == "1D" else 500

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
        self.calls.append((symbol, timeframe, bars))
        self.active_requests += 1
        self.max_active_requests = max(self.max_active_requests, self.active_requests)
        try:
            await asyncio.sleep(0.01)
            if symbol == self._fail_symbol:
                raise RuntimeError("boom")
            return {"symbol": symbol, "timeframe": timeframe}
        finally:
            self.active_requests -= 1


class ChartWarmMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    def test_daily_warm_symbols_include_watchlists_then_rs_above_80(self) -> None:
        service = StubWarmService([" zzz ", "AAA", ""])
        snapshots = [
            SimpleNamespace(symbol="AAA", rs_eligible=True, rs_rating=81, market_cap_crore=100),
            SimpleNamespace(symbol="BBB", rs_eligible=True, rs_rating=80, market_cap_crore=500),
            SimpleNamespace(symbol="CCC", rs_eligible=True, rs_rating=90, market_cap_crore=200),
            SimpleNamespace(symbol="DDD", rs_eligible=False, rs_rating=99, market_cap_crore=900),
        ]

        symbols = _daily_chart_warm_symbols(service, snapshots)

        self.assertEqual(symbols, ["ZZZ", "AAA", "CCC"])

    async def test_daily_chart_warm_runs_in_batches_for_1d_only(self) -> None:
        service = StubWarmService(["A", "B", "C", "D", "E"], fail_symbol="D")

        result = await warm_daily_chart_cache(
            "india",
            service,
            snapshots=[],
            batch_size=2,
            batch_pause_seconds=0,
        )

        self.assertEqual(result["attempted"], 5)
        self.assertEqual(result["succeeded"], 4)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["failed_symbols"], ["D"])
        self.assertLessEqual(service.max_active_requests, 2)
        self.assertTrue(all(timeframe == "1D" and bars == 1300 for _, timeframe, bars in service.calls))
