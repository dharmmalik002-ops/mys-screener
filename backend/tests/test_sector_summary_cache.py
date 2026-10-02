"""The By Sector view's prior-week / prior-month hit counts.

Building them means rebuilding every stock in the scan's sectors as it stood
5 and 20 sessions ago. That used to read each stock's history once per offset,
on the event loop, for every request (~30 s on the Space). These tests pin the
three properties that made it fast without changing the answer:

  * one history read per stock covers both offsets,
  * a second scan over the same sectors rebuilds nothing,
  * a new snapshot version throws the cached rebuilds away.
"""

from __future__ import annotations

import asyncio
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import ScanMatch, StockSnapshot  # noqa: E402
from app.services.dashboard_service import DashboardService  # noqa: E402


def _snapshot(symbol: str, sector: str, price: float = 100.0) -> StockSnapshot:
    return StockSnapshot(
        symbol=symbol,
        name=f"{symbol} Ltd",
        exchange="NSE",
        sector=sector,
        market_cap_crore=5_000.0,
        last_price=price,
        change_pct=0.0,
        volume=10_000,
        avg_volume_20d=10_000,
        day_high=price,
        day_low=price,
        ath=price,
        high_52w=price,
        range_high_20d=price,
        benchmark_return_20d=0.0,
        sector_return_20d=0.0,
        pivot_high=price,
        darvas_high=price,
        darvas_low=price,
        pullback_depth_pct=0.0,
        trend_strength=0.0,
    )


class _Provider:
    def __init__(self) -> None:
        self.updated_at = datetime(2026, 9, 30, 10, 0, tzinfo=timezone.utc)
        self.reads: list[str] = []
        self.builds: list[tuple[str, int]] = []
        # Symbols whose chart is not cached yet (as on a freshly deployed Space).
        self.uncached: set[str] = set()

    def get_snapshot_updated_at(self) -> datetime:
        return self.updated_at

    async def get_chart(self, symbol, timeframe, bars=0):  # benchmark series
        del symbol, timeframe, bars
        return []

    def _history_frame_from_cached_bars(self, symbol: str, bars: int, allow_legacy: bool = True):
        del allow_legacy
        self.reads.append(symbol)
        if symbol in self.uncached:
            return pd.DataFrame()
        index = pd.bdate_range(end="2026-09-30", periods=bars)
        return pd.DataFrame({"Close": [100.0 + i for i in range(bars)]}, index=index)

    def _history_to_snapshot(self, instrument, history, benchmark):
        del benchmark
        self.builds.append((instrument["symbol"], len(history)))
        return _snapshot(instrument["symbol"], instrument["sector"], float(history["Close"].iloc[-1])).model_dump()


def _service(provider: _Provider) -> DashboardService:
    service = DashboardService.__new__(DashboardService)
    service.provider = provider
    service._historical_snapshot_cache = {}
    service._historical_snapshot_cache_version = None
    service._historical_build_tasks = {}
    service._historical_missing_at = {}
    service._scan_sector_summary_cache = {}
    return service


class HistoricalSectorSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = _Provider()
        self.service = _service(self.provider)
        self.universe = [
            _snapshot("AAA", "Healthcare"),
            _snapshot("BBB", "Healthcare"),
            _snapshot("CCC", "Energy"),
        ]

        # Sector labels are normalised on construction, so read them back.
        self.health = self.universe[0].sector
        self.energy = self.universe[2].sector

    def _run(self, sectors: set[str]):
        return asyncio.run(
            self.service._historical_sector_snapshots_multi(self.universe, sectors, (5, 20))
        )

    def test_one_history_read_serves_both_offsets(self) -> None:
        result = self._run({self.health})
        self.assertEqual(sorted(self.provider.reads), ["AAA", "BBB"])
        self.assertEqual([s.symbol for s in result[5]], ["AAA", "BBB"])
        self.assertEqual([s.symbol for s in result[20]], ["AAA", "BBB"])
        # Each offset sees the stock as it stood that many sessions earlier.
        self.assertEqual(result[5][0].last_price - result[20][0].last_price, 15.0)

    def test_a_second_scan_over_the_same_sectors_rebuilds_nothing(self) -> None:
        self._run({self.health})
        builds = len(self.provider.builds)
        self._run({self.health, self.energy})
        self.assertEqual(self.provider.reads.count("AAA"), 1)
        self.assertEqual(self.provider.reads.count("CCC"), 1)
        self.assertEqual(len(self.provider.builds), builds + 2)

    def test_a_new_snapshot_version_drops_the_cache(self) -> None:
        self._run({self.health})
        self.provider.updated_at = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)
        self._run({self.health})
        self.assertEqual(self.provider.reads.count("AAA"), 2)



class SectorSummaryResponseTests(unittest.TestCase):
    """The first request answers at once; the prior-hit counts follow."""

    def test_cold_request_answers_now_and_counts_on_the_next_one(self) -> None:
        provider = _Provider()
        service = _service(provider)
        universe = [_snapshot("AAA", "Healthcare"), _snapshot("BBB", "Healthcare")]
        sector = universe[0].sector
        item = ScanMatch.model_construct(symbol="AAA", sector=sector)

        def runner(snapshots):
            # Pretend the scan fires on AAA in every historical session.
            return [ScanMatch.model_construct(symbol=s.symbol, sector=s.sector) for s in snapshots if s.symbol == "AAA"]

        async def scenario():
            kwargs = dict(
                scan_key="demo",
                request_signature="{}",
                snapshots=universe,
                items=[item],
                historical_runner=runner,
            )
            first = await service._build_scan_sector_summaries(**kwargs)
            # Let the background rebuild finish.
            await asyncio.gather(*service._historical_build_tasks.values())
            second = await service._build_scan_sector_summaries(**kwargs)
            return first, second

        first, second = asyncio.run(scenario())
        self.assertEqual(first[0].current_hits, 1)
        self.assertIsNone(first[0].prior_week_hits)
        self.assertIsNone(first[0].prior_month_hits)
        self.assertEqual(second[0].prior_week_hits, 1)
        self.assertEqual(second[0].prior_month_hits, 1)
        # Two stocks, read once each, by the single background rebuild.
        self.assertEqual(sorted(provider.reads), ["AAA", "BBB"])


    def test_an_empty_chart_cache_reads_unknown_not_zero_then_recovers(self) -> None:
        """Right after a deploy no chart is cached; the counts must not say 0."""
        provider = _Provider()
        provider.uncached = {"AAA", "BBB"}
        service = _service(provider)
        universe = [_snapshot("AAA", "Healthcare"), _snapshot("BBB", "Healthcare")]
        sector = universe[0].sector
        item = ScanMatch.model_construct(symbol="AAA", sector=sector)

        def runner(snapshots):
            return [ScanMatch.model_construct(symbol=s.symbol, sector=s.sector) for s in snapshots if s.symbol == "AAA"]

        kwargs = dict(scan_key="demo", request_signature="{}", snapshots=universe, items=[item], historical_runner=runner)

        async def poll():
            result = await service._build_scan_sector_summaries(**kwargs)
            await asyncio.gather(*service._historical_build_tasks.values())
            return await service._build_scan_sector_summaries(**kwargs)

        still_cold = asyncio.run(poll())
        self.assertIsNone(still_cold[0].prior_week_hits)

        # The warm-up job fills the cache; once the retry window has passed
        # the stocks are read again and the counts appear.
        provider.uncached.clear()
        for symbol in list(service._historical_missing_at):
            service._historical_missing_at[symbol] -= 10_000
        warmed = asyncio.run(poll())
        self.assertEqual(warmed[0].prior_week_hits, 1)
        self.assertEqual(provider.reads.count("AAA"), 2)


if __name__ == "__main__":
    unittest.main()
