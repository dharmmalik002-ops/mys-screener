from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import Settings
from app.models.market import (
    ChartBar,
    ChartLineMarker,
    ChartLinePoint,
    ConsolidatingScanRequest,
    CustomScanRequest,
    HistoricalBreadthDataPoint,
    HistoricalBreadthResponse,
    HistoricalUniverseBreadth,
    IndustryGroupFilters,
    IndustryGroupMasterItem,
    IndustryGroupRankItem,
    IndustryGroupsResponse,
    IndustryGroupStockItem,
    IndexQuoteItem,
    MarketHealthResponse,
    MoneyFlowStockIdea,
    NearPivotScanRequest,
    PullBackScanRequest,
    MoneyFlowReport,
    ReturnsScanRequest,
    SectorTabResponse,
    SectorCard,
    StockSnapshot,
    UniverseBreadth,
)
from app.providers.free import FreeMarketDataProvider
from app.scanners.definitions import SCAN_BY_ID, run_consolidating_scan, run_custom_scan, run_returns_scan, run_scan
from app.services.dashboard_service import DashboardService
from app.services.us_dashboard_service import USDashboardService


class DashboardServiceIndexHeatmapTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.builder = FreeMarketDataProvider()
        self.snapshot_updated_at = datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc)

    def _build_snapshot_with_history(
        self,
        *,
        symbol: str,
        name: str,
        sector: str,
        sub_sector: str,
        market_cap_crore: float,
        start_close: float,
        step: float,
    ) -> tuple[StockSnapshot, pd.DataFrame, pd.Series]:
        index = pd.bdate_range(end=self.snapshot_updated_at, periods=520)
        history = pd.DataFrame(
            [
                {
                    "Open": start_close + (idx * step) - 1,
                    "High": start_close + (idx * step) + 2,
                    "Low": start_close + (idx * step) - 2,
                    "Close": start_close + (idx * step),
                    "Adj Close": start_close + (idx * step),
                    "Volume": 100_000 + (idx * 500),
                    "Stock Splits": 0.0,
                }
                for idx in range(len(index))
            ],
            index=index,
        )
        benchmark = pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)
        row = self.builder._history_to_snapshot(
            {
                "symbol": symbol,
                "name": name,
                "exchange": "NSE",
                "listing_date": "2020-01-02",
                "sector": sector,
                "sub_sector": sub_sector,
                "market_cap_crore": market_cap_crore,
                "ticker": f"{symbol}.NS",
            },
            history,
            benchmark,
        )
        assert row is not None
        row["market_cap_crore"] = market_cap_crore
        row["sector"] = sector
        row["sub_sector"] = sub_sector
        return StockSnapshot.model_validate(row), history, benchmark

    def _build_snapshot(
        self,
        *,
        symbol: str,
        name: str,
        sector: str,
        sub_sector: str,
        market_cap_crore: float,
        start_close: float,
        step: float,
    ) -> StockSnapshot:
        snapshot, _, _ = self._build_snapshot_with_history(
            symbol=symbol,
            name=name,
            sector=sector,
            sub_sector=sub_sector,
            market_cap_crore=market_cap_crore,
            start_close=start_close,
            step=step,
        )
        return snapshot

    async def test_sector_tab_includes_requested_index_cards(self) -> None:
        snapshots = [
            self._build_snapshot(
                symbol="AAA",
                name="AAA Industries",
                sector="Information Technology",
                sub_sector="Software",
                market_cap_crore=18_000.0,
                start_close=100.0,
                step=1.4,
            ),
            self._build_snapshot(
                symbol="BBB",
                name="BBB Manufacturing",
                sector="Industrials",
                sub_sector="Capital Goods",
                market_cap_crore=9_000.0,
                start_close=80.0,
                step=0.9,
            ),
            self._build_snapshot(
                symbol="CCC",
                name="CCC Retail",
                sector="Consumer Services",
                sub_sector="Retail",
                market_cap_crore=2_500.0,
                start_close=50.0,
                step=0.5,
            ),
        ]

        class StubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                price_map = {
                    "^NSEI": 22700.25,
                    "^NSEMDCP50": 18654.40,
                    "^CNXSC": 14231.15,
                    "^CNX500": 20891.60,
                }
                return [
                    IndexQuoteItem(symbol=symbol, price=price_map[symbol], change_pct=0.0, updated_at=self.updated_at)
                    for symbol in symbols
                    if symbol in price_map
                ]

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=StubProvider(snapshots, self.snapshot_updated_at), settings=Settings())
        service._index_constituents_cache = (
            self.snapshot_updated_at,
            {
                "Nifty 50": {"AAA"},
                "Nifty Midcap 50": {"BBB"},
                "Nifty SmallCap 250": {"CCC"},
                "Nifty 500": {"AAA", "BBB", "CCC"},
            },
        )

        response = await service.get_sector_tab("1D", "desc")

        sector_names = {card.sector for card in response.sectors}
        self.assertTrue({"Nifty 50", "Nifty Midcap 50", "Nifty SmallCap 250", "Nifty 500"}.issubset(sector_names))

        nifty_500 = next(card for card in response.sectors if card.sector == "Nifty 500")
        self.assertEqual(nifty_500.group_kind, "index")
        self.assertEqual(nifty_500.last_price, 20891.6)
        nifty_500_symbols = {
            company.symbol
            for group in nifty_500.sub_sectors
            for company in group.companies
        }
        self.assertEqual(nifty_500_symbols, {"AAA", "BBB", "CCC"})

        information_technology = next(card for card in response.sectors if card.sector == "Information Technology")
        self.assertEqual(information_technology.group_kind, "sector")
        self.assertGreater(len(information_technology.sparkline), 1)
        self.assertGreater(information_technology.return_1y, information_technology.return_3m)

    async def test_refresh_market_data_uses_cached_snapshots_when_session_is_already_current(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )

        class RefreshProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at
                self.get_snapshot_calls = 0
                self.full_refresh_calls = 0

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.get_snapshot_calls += 1
                return [self.row]

            def preferred_refresh_strategy(self) -> str:
                return "cache"

            async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.full_refresh_calls += 1
                return [self.row]

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {"applied_quote_count": 0, "historical_rebuild": False, "quote_source": None}

        provider = RefreshProvider(snapshot, self.snapshot_updated_at)
        service = DashboardService(provider=provider, settings=Settings())
        cached_dashboard = object()
        service._dashboard_cache = cached_dashboard

        response = await service.refresh_market_data()

        self.assertEqual(provider.get_snapshot_calls, 1)
        self.assertEqual(provider.full_refresh_calls, 0)
        self.assertIs(service._dashboard_cache, cached_dashboard)
        self.assertEqual(response["refresh_mode"], "cached-current")

    async def test_refresh_market_data_uses_full_refresh_for_closed_session_rebuild(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )

        class RefreshProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at
                self.full_refresh_calls = 0

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            def preferred_refresh_strategy(self) -> str:
                return "historical"

            async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.full_refresh_calls += 1
                self.updated_at = self.updated_at + timedelta(minutes=10)
                return [self.row]

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {"applied_quote_count": 0, "historical_rebuild": True, "quote_source": None}

        provider = RefreshProvider(snapshot, self.snapshot_updated_at)
        service = DashboardService(provider=provider, settings=Settings())
        service._dashboard_cache = object()

        response = await service.refresh_market_data()

        self.assertEqual(provider.full_refresh_calls, 1)
        self.assertIsNone(service._dashboard_cache)
        self.assertEqual(response["refresh_mode"], "historical-refresh")

    async def test_refresh_market_data_returns_cache_fallback_for_unknown_strategy(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )

        class RefreshProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at
                self.get_snapshot_calls = 0
                self.full_refresh_calls = 0

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.get_snapshot_calls += 1
                return [self.row]

            def preferred_refresh_strategy(self) -> str:
                return "unknown"

            async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.full_refresh_calls += 1
                return [self.row]

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {"applied_quote_count": 5, "historical_rebuild": True, "quote_source": "stale"}

        provider = RefreshProvider(snapshot, self.snapshot_updated_at)
        service = DashboardService(provider=provider, settings=Settings())
        cached_dashboard = object()
        service._dashboard_cache = cached_dashboard

        response = await service.refresh_market_data()

        self.assertEqual(provider.get_snapshot_calls, 1)
        self.assertEqual(provider.full_refresh_calls, 0)
        self.assertIs(service._dashboard_cache, cached_dashboard)
        self.assertEqual(response["refresh_mode"], "cache-fallback")
        self.assertEqual(response["applied_quote_count"], 0)
        self.assertFalse(response["historical_rebuild"])
        self.assertIsNone(response["quote_source"])

    async def test_refresh_market_data_returns_cached_current_when_session_is_already_fresh(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )

        class RefreshProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at
                self.get_snapshot_calls = 0
                self.full_refresh_calls = 0

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.get_snapshot_calls += 1
                return [self.row]

            def preferred_refresh_strategy(self) -> str:
                return "cache"

            async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.full_refresh_calls += 1
                return [self.row]

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {"applied_quote_count": 99, "historical_rebuild": True, "quote_source": "stale"}

        provider = RefreshProvider(snapshot, self.snapshot_updated_at)
        service = DashboardService(provider=provider, settings=Settings())
        cached_dashboard = object()
        service._dashboard_cache = cached_dashboard

        response = await service.refresh_market_data()

        self.assertEqual(provider.get_snapshot_calls, 1)
        self.assertEqual(provider.full_refresh_calls, 0)
        self.assertIs(service._dashboard_cache, cached_dashboard)
        self.assertEqual(response["refresh_mode"], "cached-current")
        self.assertEqual(response["applied_quote_count"], 0)
        self.assertFalse(response["historical_rebuild"])
        self.assertIsNone(response["quote_source"])

    async def test_chart_summary_uses_latest_chart_rs_rating(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )

        class ChartStubProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                return [
                    ChartBar(
                        time=index + 1,
                        open=100.0 + index,
                        high=103.0 + index,
                        low=99.0 + index,
                        close=101.0 + index,
                        volume=10_000 + (index * 100),
                    )
                    for index in range(20)
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ChartStubProvider(snapshot, self.snapshot_updated_at), settings=Settings())

        with patch.object(
            service,
            "_build_rs_line",
            return_value=(
                [
                    ChartLinePoint(time=1, value=61.0),
                    ChartLinePoint(time=2, value=78.0),
                ],
                [],
            ),
        ):
            response = await service.get_chart("AAA", "1D")

        assert response.summary is not None
        self.assertEqual(response.summary.rs_rating, 78)
        expected_adr_pct = round((4.0 / sum(101.0 + index for index in range(20)) * 20) * 100, 2)
        self.assertEqual(response.summary.adr_pct_20, expected_adr_pct)

    async def test_chart_summary_includes_circuit_limits(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        snapshot.circuit_band_label = "5%"
        snapshot.lower_circuit_limit = 120.5
        snapshot.upper_circuit_limit = 133.15

        class ChartStubProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                return [
                    ChartBar(time=1, open=100.0, high=101.0, low=99.5, close=100.5, volume=10_000),
                    ChartBar(time=2, open=101.0, high=106.0, low=100.0, close=105.0, volume=11_000),
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ChartStubProvider(snapshot, self.snapshot_updated_at), settings=Settings())

        with patch.object(service, "_build_rs_line", return_value=([], [])):
            response = await service.get_chart("AAA", "1D")

        assert response.summary is not None
        self.assertEqual(response.summary.circuit_band_label, "5%")
        self.assertEqual(response.summary.lower_circuit_limit, 120.5)
        self.assertEqual(response.summary.upper_circuit_limit, 133.15)

    async def test_chart_summary_uses_fresh_chart_close_for_price_and_change(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        snapshot.last_price = 121.2
        snapshot.previous_close = 120.0
        snapshot.change_pct = 1.0

        class ChartStubProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                return [
                    ChartBar(time=1, open=119.5, high=120.5, low=119.0, close=120.0, volume=10_000),
                    ChartBar(time=2, open=120.1, high=126.5, low=119.8, close=126.0, volume=11_000),
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ChartStubProvider(snapshot, self.snapshot_updated_at), settings=Settings())

        with patch.object(service, "_build_rs_line", return_value=([], [])):
            response = await service.get_chart("AAA", "1D")

        assert response.summary is not None
        self.assertEqual(response.summary.last_price, 126.0)
        self.assertEqual(response.summary.change_pct, 5.0)

    async def test_chart_history_returns_full_history_rs_overlay(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        bars = [
            ChartBar(time=1, open=100.0, high=101.0, low=99.0, close=100.5, volume=10_000),
            ChartBar(time=2, open=101.0, high=103.0, low=100.0, close=102.5, volume=11_000),
        ]
        rs_line = [
            ChartLinePoint(time=1, value=72.0),
            ChartLinePoint(time=2, value=79.0),
        ]
        rs_markers = [
            ChartLineMarker(time=2, value=79.0, color="#f59e0b", label="52W high"),
        ]

        class ChartStubProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ChartStubProvider(snapshot, self.snapshot_updated_at), settings=Settings())

        with patch.object(service, "get_chart_full_history", return_value=bars), patch.object(
            service,
            "_build_rs_line",
            return_value=(rs_line, rs_markers),
        ):
            response = await service.get_chart_history("AAA", "1D")

        self.assertEqual(response.symbol, "AAA")
        self.assertEqual(response.timeframe, "1D")
        self.assertEqual(response.bars, bars)
        self.assertEqual(response.rs_line, rs_line)
        self.assertEqual(response.rs_line_markers, rs_markers)
        assert response.summary is not None
        self.assertEqual(response.summary.last_price, 102.5)
        self.assertEqual(response.summary.rs_rating, 79)

    async def test_rs_markers_only_flag_52_week_rs_highs(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=0.8,
        )
        peer_snapshots = [
            snapshot.model_copy(update={"symbol": f"P{score}", "rs_eligible": True, "rs_weighted_score": float(score)})
            for score in range(1, 120)
        ]
        bars = [
            ChartBar(time=index + 1, open=100.0, high=101.0, low=99.0, close=100.0, volume=10_000)
            for index in range(530)
        ]
        ratings_by_index = {index: 70.0 for index in range(len(bars))}
        ratings_by_index[252] = 90.0
        ratings_by_index[505] = 85.0

        class ChartStubProvider:
            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

        service = DashboardService(provider=ChartStubProvider(), settings=Settings())

        def weighted_score_for_index(closes: list[float], index: int) -> float | None:
            del closes
            return float(index) if index >= 252 else None

        def score_to_rating(score: float, ordered_scores: list[float]) -> float:
            del ordered_scores
            return ratings_by_index.get(int(score), 70.0)

        with patch.object(DashboardService, "_weighted_rs_score_for_index", side_effect=weighted_score_for_index), patch.object(
            DashboardService,
            "_score_to_rs_rating",
            side_effect=score_to_rating,
        ):
            _, markers = await service._build_rs_line("AAA", "1D", bars, peer_snapshots)

        self.assertEqual([marker.time for marker in markers], [506])
        self.assertEqual(markers[0].label, "RS 85 52W high")

    async def test_chart_grid_returns_sector_member_sparklines(self) -> None:
        snapshots = [
            self._build_snapshot(
                symbol="AAA",
                name="AAA Industries",
                sector="Information Technology",
                sub_sector="Software",
                market_cap_crore=18_000.0,
                start_close=100.0,
                step=1.4,
            ),
            self._build_snapshot(
                symbol="BBB",
                name="BBB Platforms",
                sector="Information Technology",
                sub_sector="Internet",
                market_cap_crore=8_500.0,
                start_close=60.0,
                step=0.9,
            ),
        ]

        class GridStubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                return [
                    ChartBar(time=1, open=100.0, high=101.0, low=99.5, close=100.5, volume=10_000),
                    ChartBar(time=2, open=101.0, high=103.0, low=100.0, close=102.0, volume=12_000),
                    ChartBar(time=3, open=102.0, high=105.0, low=101.0, close=104.0, volume=13_000),
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=GridStubProvider(snapshots, self.snapshot_updated_at), settings=Settings())

        response = await service.get_chart_grid(
            name="Information Technology",
            group_kind="sector",
            timeframe="6M",
        )

        self.assertEqual(response.group_kind, "sector")
        self.assertEqual(response.name, "Information Technology")
        self.assertEqual(response.total_items, 2)
        self.assertEqual(response.cards[0].symbol, "AAA")
        self.assertGreater(len(response.cards[0].sparkline), 1)
        self.assertIsNotNone(response.cards[0].weight_pct)
        self.assertGreater(response.cards[0].return_1y, response.cards[0].return_3m)

    async def test_market_health_uses_index_constituent_memberships(self) -> None:
        snapshots = [
            self._build_snapshot(
                symbol="AAA",
                name="AAA Industries",
                sector="Information Technology",
                sub_sector="Software",
                market_cap_crore=18_000.0,
                start_close=100.0,
                step=1.4,
            ),
            self._build_snapshot(
                symbol="BBB",
                name="BBB Manufacturing",
                sector="Industrials",
                sub_sector="Capital Goods",
                market_cap_crore=9_000.0,
                start_close=80.0,
                step=0.9,
            ),
            self._build_snapshot(
                symbol="CCC",
                name="CCC Retail",
                sector="Consumer Services",
                sub_sector="Retail",
                market_cap_crore=2_500.0,
                start_close=50.0,
                step=0.5,
            ),
        ]

        class StubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

        service = DashboardService(provider=StubProvider(snapshots, self.snapshot_updated_at), settings=Settings())

        with patch.object(
            service,
            "_load_index_constituents_map",
            return_value={
                "Nifty 50": {"CCC"},
                "Nifty 500": {"AAA", "CCC"},
            },
        ):
            response = await service.get_market_health()

        universe_totals = {item.universe: item.total for item in response.universes}
        self.assertEqual(universe_totals["Nifty 50"], 1)
        self.assertEqual(universe_totals["Nifty 500"], 2)


class USDashboardServiceMarketHealthTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.builder = FreeMarketDataProvider()
        self.snapshot_updated_at = datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc)

    def _build_snapshot_with_history(
        self,
        *,
        symbol: str,
        name: str,
        sector: str,
        sub_sector: str,
        market_cap_crore: float,
        start_close: float,
        step: float,
    ) -> tuple[StockSnapshot, pd.DataFrame, pd.Series]:
        index = pd.bdate_range(end=self.snapshot_updated_at, periods=520)
        history = pd.DataFrame(
            [
                {
                    "Open": start_close + (idx * step) - 1,
                    "High": start_close + (idx * step) + 2,
                    "Low": start_close + (idx * step) - 2,
                    "Close": start_close + (idx * step),
                    "Adj Close": start_close + (idx * step),
                    "Volume": 100_000 + (idx * 500),
                    "Stock Splits": 0.0,
                }
                for idx in range(len(index))
            ],
            index=index,
        )
        benchmark = pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)
        row = self.builder._history_to_snapshot(
            {
                "symbol": symbol,
                "name": name,
                "exchange": "NYSE",
                "listing_date": "2020-01-02",
                "sector": sector,
                "sub_sector": sub_sector,
                "market_cap_crore": market_cap_crore,
                "ticker": symbol,
            },
            history,
            benchmark,
        )
        assert row is not None
        row["market_cap_crore"] = market_cap_crore
        row["sector"] = sector
        row["sub_sector"] = sub_sector
        return StockSnapshot.model_validate(row), history, benchmark

    def _build_snapshot(
        self,
        *,
        symbol: str,
        name: str,
        sector: str,
        sub_sector: str,
        market_cap_crore: float,
        start_close: float,
        step: float,
    ) -> StockSnapshot:
        snapshot, _, _ = self._build_snapshot_with_history(
            symbol=symbol,
            name=name,
            sector=sector,
            sub_sector=sub_sector,
            market_cap_crore=market_cap_crore,
            start_close=start_close,
            step=step,
        )
        return snapshot

    async def test_us_sector_tab_cache_does_not_depend_on_index_constituent_state(self) -> None:
        snapshot_updated_at = datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc)

        class StubProvider:
            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return []

            def get_snapshot_updated_at(self) -> datetime:
                return snapshot_updated_at

        service = USDashboardService(provider=StubProvider(), settings=Settings())
        cached_response = SectorTabResponse(
            generated_at=snapshot_updated_at,
            total_sectors=0,
            sort_by="1D",
            sort_order="desc",
            sectors=[],
        )
        service._sector_tab_cache[("1D", "desc")] = cached_response

        with patch.object(service, "_index_constituents_cache_fresh", return_value=False):
            response = await service.get_sector_tab("1D", "desc")

        self.assertIs(response, cached_response)

    async def test_market_health_uses_capped_exchange_universes(self) -> None:
        builder = FreeMarketDataProvider()
        snapshot_updated_at = datetime(2026, 3, 27, 10, 0, tzinfo=timezone.utc)

        def build_snapshot(symbol: str, exchange: str, market_cap_crore: float, sector: str = "Technology", sub_sector: str = "Software") -> StockSnapshot:
            index = pd.bdate_range(end=snapshot_updated_at, periods=520)
            history = pd.DataFrame(
                [
                    {
                        "Open": 100 + idx - 1,
                        "High": 100 + idx + 2,
                        "Low": 100 + idx - 2,
                        "Close": 100 + idx,
                        "Adj Close": 100 + idx,
                        "Volume": 100_000 + (idx * 500),
                        "Stock Splits": 0.0,
                    }
                    for idx in range(len(index))
                ],
                index=index,
            )
            benchmark = pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)
            row = builder._history_to_snapshot(
                {
                    "symbol": symbol,
                    "name": f"{symbol} Inc",
                    "exchange": exchange,
                    "listing_date": "2020-01-02",
                    "sector": sector,
                    "sub_sector": sub_sector,
                    "market_cap_crore": market_cap_crore,
                    "ticker": symbol,
                },
                history,
                benchmark,
            )
            assert row is not None
            row["market_cap_crore"] = market_cap_crore
            row["sector"] = sector
            row["sub_sector"] = sub_sector
            row["exchange"] = exchange
            return StockSnapshot.model_validate(row)

        snapshots = [build_snapshot(f"NY{index:03d}", "NYSE", 10_000 - index) for index in range(260)]
        snapshots.extend(build_snapshot(f"NA{index:03d}", "NASDAQ", 10_000 - index) for index in range(255))
        snapshots.append(build_snapshot("ETF001", "NYSE", 20_000, sector="Exchange Traded Funds", sub_sector="ETF"))

        class StubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

        service = USDashboardService(provider=StubProvider(snapshots, snapshot_updated_at), settings=Settings())
        response = await service.get_market_health()

        universe_totals = {item.universe: item.total for item in response.universes}
        self.assertEqual(universe_totals["NYSE"], 250)
        self.assertEqual(universe_totals["NASDAQ"], 250)

    async def test_chart_grid_returns_index_member_sparklines(self) -> None:
        snapshots = [
            self._build_snapshot(
                symbol="AAA",
                name="AAA Industries",
                sector="Information Technology",
                sub_sector="Software",
                market_cap_crore=18_000.0,
                start_close=100.0,
                step=1.4,
            ),
            self._build_snapshot(
                symbol="BBB",
                name="BBB Manufacturing",
                sector="Industrials",
                sub_sector="Capital Goods",
                market_cap_crore=9_000.0,
                start_close=80.0,
                step=0.9,
            ),
        ]

        class IndexGridStubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                return [
                    ChartBar(time=10, open=80.0, high=82.0, low=79.0, close=81.0, volume=9_000),
                    ChartBar(time=11, open=81.0, high=84.0, low=80.0, close=83.0, volume=9_500),
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=IndexGridStubProvider(snapshots, self.snapshot_updated_at), settings=Settings())

        with patch.object(
            service,
            "_load_index_constituents_map",
            return_value={"Nifty 50": {"AAA", "BBB"}},
        ):
            response = await service.get_chart_grid(
                name="Nifty 50",
                group_kind="index",
                timeframe="1Y",
            )

        self.assertEqual(response.group_kind, "index")
        self.assertEqual(response.total_items, 2)
        self.assertEqual({card.symbol for card in response.cards}, {"AAA", "BBB"})

    async def test_chart_grid_prefers_snapshot_sparklines_over_chart_fetches(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        snapshot.chart_grid_points = [
            ChartLinePoint(time=1, value=100.0),
            ChartLinePoint(time=2, value=104.0),
            ChartLinePoint(time=3, value=108.0),
        ]

        class SnapshotSparklineProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise AssertionError("chart fetch should not be needed when snapshot sparkline exists")

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=SnapshotSparklineProvider(snapshot, self.snapshot_updated_at), settings=Settings())
        response = await service.get_chart_grid(name="Information Technology", group_kind="sector", timeframe="3M")

        self.assertEqual(response.total_items, 1)
        self.assertEqual([point.value for point in response.cards[0].sparkline], [100.0, 104.0, 108.0])

    async def test_chart_grid_series_returns_requested_daily_bars(self) -> None:
        class SeriesStubProvider:
            def __init__(self, updated_at: datetime) -> None:
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return []

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                base_time = 100 if symbol == "AAA" else 200
                return [
                    ChartBar(time=base_time + 1, open=10.0, high=11.0, low=9.5, close=10.5, volume=1000),
                    ChartBar(time=base_time + 2, open=10.5, high=12.0, low=10.0, close=11.5, volume=1200),
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=SeriesStubProvider(self.snapshot_updated_at), settings=Settings())
        response = await service.get_chart_grid_series(symbols=["AAA", "AAA", "BBB"], timeframe="1Y")

        self.assertEqual(response.total_items, 2)
        self.assertEqual([item.symbol for item in response.items], ["AAA", "BBB"])
        self.assertEqual(response.items[0].bars[-1].close, 11.5)

    async def test_near_pivot_excludes_rs_ineligible_names(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        snapshot.rs_eligible = False
        snapshot.rs_rating = 98

        class ScanStubProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ScanStubProvider(snapshot, self.snapshot_updated_at), settings=Settings())
        response = await service.get_near_pivot_scan_results(NearPivotScanRequest())
        self.assertEqual(response.total_hits, 0)

    async def test_pull_back_excludes_rs_ineligible_names(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        snapshot.rs_eligible = False
        snapshot.rs_rating = 98

        class PullbackStubProvider:
            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=PullbackStubProvider(snapshot, self.snapshot_updated_at), settings=Settings())
        response = await service.get_pull_back_scan_results(PullBackScanRequest())
        self.assertEqual(response.total_hits, 0)

    async def test_gap_up_liquidity_filter_excludes_illiquid_names(self) -> None:
        liquid = self._build_snapshot(
            symbol="LIQ",
            name="Liquid Co",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=8_000.0,
            start_close=1000.0,
            step=1.2,
        )
        liquid.gap_pct = 3.2
        liquid.avg_volume_20d = 250_000
        liquid.avg_volume_30d = 250_000

        illiquid = self._build_snapshot(
            symbol="ILLQ",
            name="Illiquid Co",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=1_200.0,
            start_close=90.0,
            step=0.3,
        )
        illiquid.gap_pct = 3.8
        illiquid.avg_volume_20d = 8_000
        illiquid.avg_volume_30d = 8_000

        class GapUpProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=GapUpProvider([illiquid, liquid], self.snapshot_updated_at), settings=Settings())
        response = await service.get_gap_up_openers(min_gap_pct=2.0, min_liquidity_crore=5.0)

        self.assertEqual(response.total_hits, 1)
        self.assertEqual(response.items[0].symbol, "LIQ")

    async def test_near_pivot_liquidity_filter_excludes_illiquid_names(self) -> None:
        liquid = self._build_snapshot(
            symbol="LIQ",
            name="Liquid Co",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=8_000.0,
            start_close=1000.0,
            step=1.1,
        )
        liquid.rs_eligible = True
        liquid.rs_rating = 90
        liquid.last_price = 2000.0
        liquid.high_52w = round(liquid.last_price / 0.97, 2)
        liquid.avg_volume_20d = 250_000
        liquid.avg_volume_30d = 250_000
        liquid.recent_highs = [2000.0, 2002.0, 2003.0, 2004.0, 2004.5]
        liquid.recent_lows = [1988.0, 1990.0, 1992.0, 1993.0, 1994.0]

        illiquid = self._build_snapshot(
            symbol="ILLQ",
            name="Illiquid Co",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=1_200.0,
            start_close=90.0,
            step=0.25,
        )
        illiquid.rs_eligible = True
        illiquid.rs_rating = 91
        illiquid.last_price = 120.0
        illiquid.high_52w = round(illiquid.last_price / 0.98, 2)
        illiquid.avg_volume_20d = 8_000
        illiquid.avg_volume_30d = 8_000
        illiquid.recent_highs = [120.0, 120.5, 121.0, 121.2, 121.3]
        illiquid.recent_lows = [118.0, 118.5, 118.8, 119.0, 119.2]

        class NearPivotProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=NearPivotProvider([illiquid, liquid], self.snapshot_updated_at), settings=Settings())
        response = await service.get_near_pivot_scan_results(NearPivotScanRequest(min_liquidity_crore=5.0))

        self.assertEqual(response.total_hits, 1)
        self.assertEqual(response.items[0].symbol, "LIQ")

    async def test_returns_liquidity_filter_excludes_illiquid_names(self) -> None:
        liquid = self._build_snapshot(
            symbol="LIQ",
            name="Liquid Co",
            sector="Consumer Services",
            sub_sector="Retail",
            market_cap_crore=8_000.0,
            start_close=1000.0,
            step=1.2,
        )
        liquid.stock_return_20d = 14.0
        liquid.avg_volume_20d = 250_000
        liquid.avg_volume_30d = 250_000

        illiquid = self._build_snapshot(
            symbol="ILLQ",
            name="Illiquid Co",
            sector="Consumer Services",
            sub_sector="Retail",
            market_cap_crore=1_200.0,
            start_close=90.0,
            step=0.25,
        )
        illiquid.stock_return_20d = 18.0
        illiquid.avg_volume_20d = 8_000
        illiquid.avg_volume_30d = 8_000

        class ReturnsProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ReturnsProvider([illiquid, liquid], self.snapshot_updated_at), settings=Settings())
        response = await service.get_returns_scan_results(ReturnsScanRequest(timeframe="1M", min_liquidity_crore=5.0))

        self.assertEqual(response.total_hits, 1)
        self.assertEqual(response.items[0].symbol, "LIQ")

    async def test_returns_scan_includes_sector_summaries_with_historical_counts(self) -> None:
        alpha, alpha_history, benchmark = self._build_snapshot_with_history(
            symbol="ALPHA",
            name="Alpha Tech",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        beta, beta_history, _ = self._build_snapshot_with_history(
            symbol="BETA",
            name="Beta Services",
            sector="Information Technology",
            sub_sector="IT Services",
            market_cap_crore=9_000.0,
            start_close=80.0,
            step=0.9,
        )
        gamma, gamma_history, _ = self._build_snapshot_with_history(
            symbol="GAMMA",
            name="Gamma Pharma",
            sector="Pharma",
            sub_sector="Formulations",
            market_cap_crore=7_500.0,
            start_close=60.0,
            step=0.7,
        )

        histories = {
            "ALPHA": alpha_history,
            "BETA": beta_history,
            "GAMMA": gamma_history,
        }

        class ReturnsSummaryProvider:
            def __init__(
                self,
                rows: list[StockSnapshot],
                histories_by_symbol: dict[str, pd.DataFrame],
                benchmark_history: pd.Series,
                updated_at: datetime,
                builder: FreeMarketDataProvider,
            ) -> None:
                self.rows = rows
                self.histories_by_symbol = histories_by_symbol
                self.benchmark_history = benchmark_history
                self.updated_at = updated_at
                self.builder = builder

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                if symbol != "^NSEI":
                    raise NotImplementedError
                return [
                    ChartBar(
                        time=int(timestamp.timestamp()),
                        open=float(close),
                        high=float(close),
                        low=float(close),
                        close=float(close),
                        volume=0,
                    )
                    for timestamp, close in self.benchmark_history.tail(bars).items()
                ]

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

            def _history_frame_from_cached_bars(
                self,
                symbol: str,
                bars: int,
                allow_legacy: bool = True,
            ) -> pd.DataFrame:
                return self.histories_by_symbol[symbol].tail(bars)

            def _history_to_snapshot(
                self,
                instrument: dict[str, object],
                history: pd.DataFrame,
                benchmark_history: pd.Series,
            ) -> dict[str, object] | None:
                return self.builder._history_to_snapshot(instrument, history, benchmark_history)

        service = DashboardService(
            provider=ReturnsSummaryProvider(
                [alpha, beta, gamma],
                histories,
                benchmark,
                self.snapshot_updated_at,
                self.builder,
            ),
            settings=Settings(),
        )
        response = await service.get_returns_scan_results(
            ReturnsScanRequest(timeframe="1M", min_return_pct=1.0, limit=100),
        )

        self.assertEqual(response.total_hits, 3)
        self.assertEqual(len(response.sector_summaries), 2)
        information_technology = next(summary for summary in response.sector_summaries if summary.sector == "Information Technology")
        pharma = next(summary for summary in response.sector_summaries if summary.sector == "Pharma")
        self.assertEqual(information_technology.current_hits, 2)
        self.assertEqual(pharma.current_hits, 1)
        self.assertGreaterEqual(information_technology.prior_week_hits, 1)
        self.assertGreaterEqual(information_technology.prior_month_hits, 1)
        self.assertNotEqual(information_technology.sector_return_1w, 0.0)
        self.assertNotEqual(pharma.sector_return_1m, 0.0)

    async def test_consolidating_liquidity_filter_excludes_illiquid_names(self) -> None:
        liquid = self._build_snapshot(
            symbol="LIQ",
            name="Liquid Co",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=8_000.0,
            start_close=1000.0,
            step=1.0,
        )
        illiquid = self._build_snapshot(
            symbol="ILLQ",
            name="Illiquid Co",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=1_200.0,
            start_close=100.0,
            step=0.2,
        )

        for snapshot in (liquid, illiquid):
            snapshot.sma200 = 880.0 if snapshot.symbol == "LIQ" else 88.0
            snapshot.sma50 = 960.0 if snapshot.symbol == "LIQ" else 96.0
            snapshot.avg_volume_50d = 250_000 if snapshot.symbol == "LIQ" else 8_000
            snapshot.avg_volume_30d = 250_000 if snapshot.symbol == "LIQ" else 8_000
            snapshot.avg_volume_20d = 250_000 if snapshot.symbol == "LIQ" else 8_000
            snapshot.volume = 500_000 if snapshot.symbol == "LIQ" else 16_000
            snapshot.high_52w = 1105.0 if snapshot.symbol == "LIQ" else 110.5
            snapshot.low_52w = 700.0 if snapshot.symbol == "LIQ" else 70.0
            snapshot.recent_highs = [1_005.0] * 5 + [1_010.0] * 5 + [1_015.0] * 5 if snapshot.symbol == "LIQ" else [100.5] * 15
            snapshot.recent_lows = [960.0] * 5 + [965.0] * 5 + [970.0] * 5 if snapshot.symbol == "LIQ" else [96.0] * 15
            snapshot.recent_volumes = [120_000] * 10 + [150_000] * 10 if snapshot.symbol == "LIQ" else [4_000] * 20

        class ConsolidatingProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ConsolidatingProvider([illiquid, liquid], self.snapshot_updated_at), settings=Settings())
        response = await service.get_consolidating_scan_results(ConsolidatingScanRequest(min_liquidity_crore=5.0))

        self.assertEqual(response.total_hits, 1)
        self.assertEqual(response.items[0].symbol, "LIQ")

    def test_consolidating_run_up_mode_returns_only_run_up_matches(self) -> None:
        run_up = self._build_snapshot(
            symbol="RUNA",
            name="Run Up A",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=9_000.0,
            start_close=100.0,
            step=1.2,
        )
        breakout_only = self._build_snapshot(
            symbol="BRKA",
            name="Breakout A",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=9_500.0,
            start_close=100.0,
            step=0.9,
        )

        run_up.sma50 = 150.0
        run_up.sma200 = 130.0
        run_up.high_52w = 180.0
        run_up.low_52w = 120.0
        run_up.last_price = 171.0
        run_up.avg_volume_50d = 200_000
        run_up.avg_volume_30d = 200_000
        run_up.avg_volume_20d = 200_000
        run_up.volume = 140_000
        run_up.recent_highs = [170.0, 170.5, 171.0, 171.5, 172.0, 171.8, 171.7, 171.4, 171.1, 170.9, 170.8, 170.7, 170.9, 171.0, 171.2]
        run_up.recent_lows = [158.0, 158.2, 158.5, 158.8, 159.0, 159.2, 159.4, 159.6, 159.8, 160.0, 160.2, 160.3, 160.5, 160.6, 160.8]
        run_up.recent_volumes = [90_000] * 10 + [130_000] * 10

        breakout_only.sma50 = 150.0
        breakout_only.sma200 = 120.0
        breakout_only.high_3y = 205.0
        breakout_only.multi_year_high = 205.0
        breakout_only.last_price = 194.0
        breakout_only.volume = 160_000
        breakout_only.avg_volume_50d = 110_000
        breakout_only.avg_volume_30d = 110_000
        breakout_only.avg_volume_20d = 110_000
        breakout_only.high_52w = 205.0
        breakout_only.low_52w = 110.0
        breakout_only.recent_highs = [205.0] * 15
        breakout_only.recent_lows = [180.0] * 15
        breakout_only.recent_volumes = [120_000] * 20

        results = run_consolidating_scan(
            ConsolidatingScanRequest(enable_run_up_consolidation=True, enable_near_multi_year_breakout=False, limit=20),
            [run_up, breakout_only],
        )

        self.assertEqual([item.symbol for item in results], ["RUNA"])
        self.assertEqual(results[0].pattern, "Long Consolidation After a Run-Up")

    def test_consolidating_near_multi_year_breakout_mode_returns_only_breakout_matches(self) -> None:
        run_up_only = self._build_snapshot(
            symbol="RUNB",
            name="Run Up B",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=9_000.0,
            start_close=100.0,
            step=1.1,
        )
        breakout = self._build_snapshot(
            symbol="BRKB",
            name="Breakout B",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=10_000.0,
            start_close=100.0,
            step=1.0,
        )

        run_up_only.sma50 = 150.0
        run_up_only.sma200 = 130.0
        run_up_only.high_52w = 180.0
        run_up_only.low_52w = 120.0
        run_up_only.last_price = 171.0
        run_up_only.avg_volume_50d = 200_000
        run_up_only.avg_volume_30d = 200_000
        run_up_only.avg_volume_20d = 200_000
        run_up_only.volume = 140_000
        run_up_only.recent_highs = [170.0, 170.5, 171.0, 171.5, 172.0, 171.8, 171.7, 171.4, 171.1, 170.9, 170.8, 170.7, 170.9, 171.0, 171.2]
        run_up_only.recent_lows = [158.0, 158.2, 158.5, 158.8, 159.0, 159.2, 159.4, 159.6, 159.8, 160.0, 160.2, 160.3, 160.5, 160.6, 160.8]
        run_up_only.recent_volumes = [90_000] * 10 + [130_000] * 10

        breakout.sma50 = 150.0
        breakout.sma200 = 120.0
        breakout.high_3y = 210.0
        breakout.multi_year_high = 210.0
        breakout.last_price = 195.0
        breakout.volume = 180_000
        breakout.avg_volume_50d = 120_000
        breakout.avg_volume_30d = 120_000
        breakout.avg_volume_20d = 120_000

        results = run_consolidating_scan(
            ConsolidatingScanRequest(enable_run_up_consolidation=False, enable_near_multi_year_breakout=True, limit=20),
            [run_up_only, breakout],
        )

        self.assertEqual([item.symbol for item in results], ["BRKB"])
        self.assertEqual(results[0].pattern, "Near Multi-Year Breakout")

    def test_consolidating_combines_union_when_both_modes_are_enabled(self) -> None:
        both = self._build_snapshot(
            symbol="BOTH",
            name="Both Match",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=11_000.0,
            start_close=100.0,
            step=1.2,
        )
        run_up_only = self._build_snapshot(
            symbol="RUNO",
            name="Run Only",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=9_500.0,
            start_close=100.0,
            step=1.1,
        )
        breakout_only = self._build_snapshot(
            symbol="BRKO",
            name="Break Only",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=10_500.0,
            start_close=100.0,
            step=1.0,
        )

        for snapshot in (both, run_up_only):
            snapshot.sma50 = 150.0
            snapshot.sma200 = 130.0
            snapshot.high_52w = 180.0
            snapshot.low_52w = 120.0
            snapshot.last_price = 171.0 if snapshot.symbol == "RUNO" else 174.0
            snapshot.avg_volume_50d = 200_000
            snapshot.avg_volume_30d = 200_000
            snapshot.avg_volume_20d = 200_000
            snapshot.recent_highs = [170.0, 170.5, 171.0, 171.5, 172.0, 171.8, 171.7, 171.4, 171.1, 170.9, 170.8, 170.7, 170.9, 171.0, 171.2]
            snapshot.recent_lows = [158.0, 158.2, 158.5, 158.8, 159.0, 159.2, 159.4, 159.6, 159.8, 160.0, 160.2, 160.3, 160.5, 160.6, 160.8]
            snapshot.recent_volumes = [90_000] * 10 + [130_000] * 10
            snapshot.volume = 140_000 if snapshot.symbol == "RUNO" else 160_000

        both.high_3y = 180.0
        both.multi_year_high = 180.0
        both.last_price = 174.0

        breakout_only.sma50 = 150.0
        breakout_only.sma200 = 120.0
        breakout_only.high_3y = 210.0
        breakout_only.multi_year_high = 210.0
        breakout_only.last_price = 195.0
        breakout_only.volume = 180_000
        breakout_only.avg_volume_50d = 120_000
        breakout_only.avg_volume_30d = 120_000
        breakout_only.avg_volume_20d = 120_000

        results = run_consolidating_scan(ConsolidatingScanRequest(limit=20), [both, run_up_only, breakout_only])

        self.assertEqual({item.symbol for item in results}, {"BOTH", "RUNO", "BRKO"})
        both_match = next(item for item in results if item.symbol == "BOTH")
        self.assertIn("Long Consolidation After a Run-Up", both_match.pattern or "")
        self.assertIn("Near Multi-Year Breakout", both_match.pattern or "")

    def test_custom_scan_rs_filter_excludes_non_eligible_names(self) -> None:
        snapshot = self._build_snapshot(
            symbol="AAA",
            name="AAA Industries",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.4,
        )
        snapshot.rs_eligible = False
        snapshot.rs_rating = 98

        response = run_custom_scan(CustomScanRequest(min_rs_rating=70, limit=20), [snapshot])
        self.assertEqual(response, [])

    def test_returns_scan_uses_normalized_sector_labels(self) -> None:
        private_bank = self._build_snapshot(
            symbol="PVTBANK",
            name="Private Bank",
            sector="Financial Services",
            sub_sector="Private Sector Bank",
            market_cap_crore=15_000.0,
            start_close=100.0,
            step=1.0,
        )
        psu_bank = self._build_snapshot(
            symbol="PSUBANK",
            name="PSU Bank",
            sector="Financial Services",
            sub_sector="Public Sector Bank",
            market_cap_crore=11_000.0,
            start_close=80.0,
            step=0.8,
        )
        defence = self._build_snapshot(
            symbol="DEFENCE",
            name="Defence Co",
            sector="Capital Goods",
            sub_sector="Aerospace & Defense",
            market_cap_crore=22_000.0,
            start_close=90.0,
            step=1.1,
        )

        response = run_returns_scan(ReturnsScanRequest(timeframe="1M", limit=50), [private_bank, psu_bank, defence])
        sectors_by_symbol = {item.symbol: item.sector for item in response}

        self.assertEqual(sectors_by_symbol["PVTBANK"], "Private Sector Banks")
        self.assertEqual(sectors_by_symbol["PSUBANK"], "PSU Banks")
        self.assertEqual(sectors_by_symbol["DEFENCE"], "Capital Goods - Aerospace & Defense")

    def test_returns_scan_excludes_short_history_name_when_200sma_is_required(self) -> None:
        index = pd.bdate_range(end=self.snapshot_updated_at, periods=60)
        history = pd.DataFrame(
            [
                {
                    "Open": 100 + idx - 1,
                    "High": 100 + idx + 2,
                    "Low": 100 + idx - 2,
                    "Close": 100 + idx,
                    "Adj Close": 100 + idx,
                    "Volume": 100_000 + (idx * 500),
                    "Stock Splits": 0.0,
                }
                for idx in range(len(index))
            ],
            index=index,
        )
        benchmark = pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float)
        row = self.builder._history_to_snapshot(
            {
                "symbol": "SHORT",
                "name": "Short History",
                "exchange": "NSE",
                "listing_date": "2025-10-01",
                "sector": "Financial Services",
                "sub_sector": "Private Sector Bank",
                "market_cap_crore": 2_000.0,
                "ticker": "SHORT.NS",
            },
            history,
            benchmark,
        )
        assert row is not None
        snapshot = StockSnapshot.model_validate(row)

        response = run_returns_scan(ReturnsScanRequest(timeframe="1M", above_200_sma=True, limit=50), [snapshot])
        self.assertEqual(response, [])

    def test_minervini_scan_returns_qualifying_trend_template_name(self) -> None:
        snapshot = self._build_snapshot(
            symbol="MINPASS",
            name="Minervini Pass",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        snapshot.rs_eligible = True
        snapshot.rs_rating = 92

        response = run_scan(SCAN_BY_ID["minervini-1m"], [snapshot])

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0].symbol, "MINPASS")
        self.assertEqual(response[0].scan_id, "minervini-1m")
        self.assertIn("Price above 50/150/200 SMA", response[0].reasons[0])

    def test_minervini_scan_excludes_name_when_200sma_is_not_rising(self) -> None:
        snapshot = self._build_snapshot(
            symbol="MINFAIL",
            name="Minervini Fail",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        snapshot.sma200_1m_ago = snapshot.sma200

        response = run_scan(SCAN_BY_ID["minervini-1m"], [snapshot])

        self.assertEqual(response, [])

    def test_ipo_scan_includes_only_recently_listed_names(self) -> None:
        recent_listing = (date.today() - timedelta(days=120)).isoformat()
        stale_listing = (date.today() - timedelta(days=366)).isoformat()

        recent_snapshot = self._build_snapshot(
            symbol="RECENTIPO",
            name="Recent IPO",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=8_000.0,
            start_close=100.0,
            step=0.9,
        )
        recent_snapshot.listing_date = date.fromisoformat(recent_listing)

        stale_snapshot = self._build_snapshot(
            symbol="OLDIPO",
            name="Old IPO",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=8_000.0,
            start_close=90.0,
            step=0.7,
        )
        stale_snapshot.listing_date = date.fromisoformat(stale_listing)

        response = run_scan(SCAN_BY_ID["ipo"], [stale_snapshot, recent_snapshot])

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0].symbol, "RECENTIPO")
        self.assertEqual(response[0].scan_id, "ipo")
        self.assertIn("Listed on", response[0].reasons[0])

    def test_minervini_5m_scan_returns_qualifying_trend_template_name(self) -> None:
        snapshot = self._build_snapshot(
            symbol="MIN5PASS",
            name="Minervini 5M Pass",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        snapshot.rs_eligible = True
        snapshot.rs_rating = 94

        response = run_scan(SCAN_BY_ID["minervini-5m"], [snapshot])

        self.assertEqual(len(response), 1)
        self.assertEqual(response[0].symbol, "MIN5PASS")
        self.assertEqual(response[0].scan_id, "minervini-5m")
        self.assertIn("Price above 50/150/200 SMA", response[0].reasons[0])

    def test_minervini_5m_scan_excludes_name_when_5m_trend_is_not_rising(self) -> None:
        snapshot = self._build_snapshot(
            symbol="MIN5FAIL",
            name="Minervini 5M Fail",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        snapshot.sma200_5m_ago = snapshot.sma200

        response = run_scan(SCAN_BY_ID["minervini-5m"], [snapshot])

        self.assertEqual(response, [])

    async def test_minervini_scan_results_apply_min_liquidity_filter(self) -> None:
        liquid = self._build_snapshot(
            symbol="MINLIQ",
            name="Minervini Liquid",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        illiquid = self._build_snapshot(
            symbol="MINILLQ",
            name="Minervini Illiquid",
            sector="Information Technology",
            sub_sector="Software",
            market_cap_crore=18_000.0,
            start_close=100.0,
            step=1.2,
        )
        liquid.rs_eligible = True
        liquid.rs_rating = 92
        illiquid.rs_eligible = True
        illiquid.rs_rating = 91
        liquid.avg_volume_30d = 800_000
        illiquid.avg_volume_30d = 10_000

        class ScanStubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=ScanStubProvider([liquid, illiquid], self.snapshot_updated_at), settings=Settings())

        response = await service.get_scan_results("minervini-1m", min_liquidity_crore=5.0)

        self.assertEqual(response.total_hits, 1)
        self.assertEqual(response.items[0].symbol, "MINLIQ")


class USDashboardServiceMoneyFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = USDashboardService(provider=object(), settings=Settings())

    def _theme_snapshot(self, *, name: str, sector: str, sub_sector: str) -> StockSnapshot:
        return StockSnapshot.model_construct(
            symbol="TEST",
            name=name,
            exchange="NASDAQ",
            sector=sector,
            sub_sector=sub_sector,
            market_cap_crore=2_000.0,
            last_price=100.0,
            change_pct=1.0,
            volume=1_000_000,
            avg_volume_20d=900_000,
            avg_volume_30d=900_000,
            day_high=101.0,
            day_low=99.0,
            ath=120.0,
            high_52w=120.0,
            range_high_20d=110.0,
            benchmark_return_20d=5.0,
            sector_return_20d=6.0,
            avg_rupee_volume_30d_crore=25.0,
            stock_return_20d=8.0,
            stock_return_60d=22.0,
            stock_return_12m=35.0,
            pct_from_52w_high=9.0,
            pct_from_ath=9.0,
            pullback_depth_pct=8.0,
            relative_volume=1.4,
            rs_eligible=True,
            rs_rating=82,
            ema20=98.0,
            ema50=95.0,
        )

    def test_us_money_flow_theme_priority_prefers_new_age_and_niche_groups(self) -> None:
        themed = self._theme_snapshot(
            name="CyberCloud Systems",
            sector="Technology",
            sub_sector="Computer Software: Programming Data Processing",
        )
        generic = self._theme_snapshot(
            name="Regional Bank Holdings",
            sector="Finance",
            sub_sector="Major Banks",
        )

        self.assertGreater(
            self.service._money_flow_stock_theme_priority(themed, "consolidation"),
            self.service._money_flow_stock_theme_priority(generic, "consolidation"),
        )

    def test_us_money_flow_schedule_uses_post_close_et_cutover(self) -> None:
        et = ZoneInfo("America/New_York")
        before_close = datetime(2026, 3, 31, 16, 20, tzinfo=et)
        after_close = datetime(2026, 3, 31, 16, 35, tzinfo=et)

        self.assertEqual(self.service._money_flow_stock_recommendation_date(before_close), "2026-03-30")
        self.assertEqual(self.service._money_flow_stock_recommendation_date(after_close), "2026-03-31")
        self.assertEqual(self.service._money_flow_stock_recommendation_date(after_close, force_today=True), "2026-03-31")

        next_update = self.service._money_flow_stock_next_update(after_close)
        self.assertEqual(next_update.astimezone(et).strftime("%Y-%m-%d %H:%M"), "2026-04-01 16:30")

    def test_us_weekly_money_flow_target_tracks_last_due_saturday(self) -> None:
        et = ZoneInfo("America/New_York")
        monday_morning = datetime(2026, 4, 6, 10, 0, tzinfo=et)
        saturday_before_release = datetime(2026, 4, 11, 8, 30, tzinfo=et)

        week_key, week_start, scheduled_run = self.service._money_flow_target_week(monday_morning)
        self.assertEqual(week_key, "2026-W14")
        self.assertEqual(week_start, "2026-03-30")
        self.assertEqual(scheduled_run.strftime("%Y-%m-%d %H:%M"), "2026-04-04 09:00")

        previous_week_key, previous_week_start, previous_scheduled_run = self.service._money_flow_target_week(saturday_before_release)
        self.assertEqual(previous_week_key, "2026-W14")
        self.assertEqual(previous_week_start, "2026-03-30")
        self.assertEqual(previous_scheduled_run.strftime("%Y-%m-%d %H:%M"), "2026-04-04 09:00")


class DashboardServiceMoneyFlowFundamentalsCacheTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.service = DashboardService(provider=object(), settings=Settings())

    def _snapshot(self, symbol: str) -> StockSnapshot:
        return StockSnapshot.model_construct(
            symbol=symbol,
            name=f"{symbol} Corp",
            exchange="NSE",
            sector="Technology",
            sub_sector="Software",
            market_cap_crore=2_500.0,
            last_price=100.0,
            change_pct=1.0,
            volume=1_000_000,
            avg_volume_20d=900_000,
            avg_volume_30d=900_000,
            day_high=102.0,
            day_low=98.0,
            ath=120.0,
            high_52w=120.0,
            range_high_20d=110.0,
            benchmark_return_20d=5.0,
            sector_return_20d=6.0,
            avg_rupee_volume_30d_crore=25.0,
            stock_return_20d=8.0,
            stock_return_60d=22.0,
            stock_return_12m=35.0,
            pct_from_52w_high=9.0,
            pct_from_ath=9.0,
            pullback_depth_pct=8.0,
            relative_volume=1.4,
            rs_eligible=True,
            rs_rating=82,
            ema20=98.0,
            ema50=95.0,
        )

    async def test_fetch_fundamentals_prefers_recent_cached_payloads(self) -> None:
        snapshots = [self._snapshot("AAA"), self._snapshot("BBB")]
        cached_fundamentals = object()
        refreshed_fundamentals = object()

        class StubProvider:
            def __init__(self) -> None:
                self.cache_calls: list[tuple[str, float | None]] = []
                self.refresh_calls: list[str] = []

            async def get_fundamentals_cached(
                self,
                symbol: str,
                snapshot: StockSnapshot | None = None,
                max_age_hours: float | None = None,
            ):
                self.cache_calls.append((symbol, max_age_hours))
                if symbol == "AAA":
                    return cached_fundamentals
                return None

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                self.refresh_calls.append(symbol)
                return refreshed_fundamentals

        provider = StubProvider()
        self.service.provider = provider

        result = await self.service._fetch_fundamentals_for_symbols(snapshots, limit=2)

        self.assertEqual(provider.cache_calls, [("AAA", 72), ("BBB", 72)])
        self.assertEqual(provider.refresh_calls, ["BBB"])
        self.assertIs(result["AAA"], cached_fundamentals)
        self.assertIs(result["BBB"], refreshed_fundamentals)

    async def test_generate_money_flow_stock_ideas_rotates_away_from_yesterday_symbols(self) -> None:
        symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]

        def snapshot(symbol: str, index: int) -> StockSnapshot:
            return StockSnapshot.model_construct(
                symbol=symbol,
                name=f"{symbol} Corp",
                exchange="NSE",
                sector=f"Sector {index}",
                sub_sector=f"Sub Sector {index}",
                market_cap_crore=1_000.0 + index,
                last_price=100.0 + index,
                change_pct=1.0,
                volume=1_000_000,
                avg_volume_20d=900_000,
                avg_volume_30d=900_000,
                day_high=101.0,
                day_low=99.0,
                ath=120.0,
                high_52w=120.0,
                range_high_20d=110.0,
                benchmark_return_20d=5.0,
                sector_return_20d=6.0,
                avg_rupee_volume_30d_crore=25.0,
                stock_return_20d=8.0,
                stock_return_60d=22.0,
                stock_return_12m=35.0,
                pct_from_52w_high=9.0,
                pct_from_ath=9.0,
                pullback_depth_pct=8.0,
                relative_volume=1.4,
                rs_eligible=True,
                rs_rating=82,
                ema20=98.0,
                ema50=95.0,
            )

        snapshots = [snapshot(symbol, index) for index, symbol in enumerate(symbols, start=1)]
        base_scores = {
            "AAA": 110.0,
            "BBB": 109.0,
            "CCC": 108.0,
            "DDD": 107.0,
            "EEE": 95.0,
            "FFF": 94.0,
            "GGG": 93.0,
            "HHH": 92.0,
        }

        class StubProvider:
            def __init__(self) -> None:
                self.ai_service = type("AIService", (), {"available": False})()

        service = DashboardService(provider=StubProvider(), settings=Settings())
        async def load_snapshots():
            return snapshots

        service._snapshots = load_snapshots
        service._load_money_flow_stock_payloads = lambda: {
            "2026-04-02": {
                "recommendation_date": "2026-04-02",
                "generated_at": "2026-04-02T12:30:00Z",
                "next_update_at": "2026-04-03T12:30:00Z",
                "consolidating_ideas": [
                    {"symbol": "AAA"},
                    {"symbol": "BBB"},
                    {"symbol": "CCC"},
                    {"symbol": "DDD"},
                ],
                "value_ideas": [],
                "ai_model": None,
            }
        }
        stored_payloads: dict[str, dict] = {}
        service._save_money_flow_stock_payloads = lambda payloads: stored_payloads.update(payloads)
        service._money_flow_stock_recommendation_date = lambda now_local, force_today=False: "2026-04-03"
        service._score_consolidation_snapshot = lambda stock: (
            base_scores[stock.symbol],
            f"{stock.symbol} setup",
            f"{stock.symbol} thesis",
        )
        service._score_value_candidate = lambda stock, fundamentals: None

        async def fetch_fundamentals(selected_snapshots: list[StockSnapshot], limit: int):
            return {stock.symbol: object() for stock in selected_snapshots[:limit]}

        service._fetch_fundamentals_for_symbols = fetch_fundamentals
        service._build_money_flow_stock_idea = lambda snapshot, fundamentals, recommendation_type, setup_score, setup_summary, thesis, sector_context=None: MoneyFlowStockIdea(
            symbol=snapshot.symbol,
            name=snapshot.name,
            exchange=snapshot.exchange,
            sector=snapshot.sector,
            sub_sector=snapshot.sub_sector,
            recommendation_type=recommendation_type,
            last_price=snapshot.last_price,
            change_pct=snapshot.change_pct,
            market_cap_crore=snapshot.market_cap_crore,
            rs_rating=snapshot.rs_rating,
            relative_volume=snapshot.relative_volume,
            stock_return_20d=snapshot.stock_return_20d,
            stock_return_60d=snapshot.stock_return_60d,
            stock_return_12m=snapshot.stock_return_12m,
            pct_from_52w_high=snapshot.pct_from_52w_high,
            pct_from_ath=snapshot.pct_from_ath,
            pullback_depth_pct=snapshot.pullback_depth_pct,
            setup_score=setup_score,
            setup_summary=setup_summary,
            thesis=thesis,
            future_growth_summary="summary",
            recent_quarter_summary="quarter",
            valuation_summary=None,
            recent_developments=[],
            growth_drivers=[],
            risk_flags=[],
            key_metrics={},
        )

        response = await service.generate_and_store_money_flow_stock_ideas(
            force=True,
            reference_time=datetime(2026, 4, 3, 19, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        )

        self.assertEqual([idea.symbol for idea in response.consolidating_ideas], ["EEE", "FFF", "GGG", "HHH"])
        self.assertEqual(stored_payloads["2026-04-03"]["recommendation_date"], "2026-04-03")

    async def test_ensure_money_flow_stock_ideas_current_generates_even_when_ai_is_unavailable(self) -> None:
        expected_payload = object()

        class StubProvider:
            def __init__(self) -> None:
                self.ai_service = type("AIService", (), {"available": False})()

        service = DashboardService(provider=StubProvider(), settings=Settings())

        async def load_payloads() -> dict[str, dict]:
            return {}

        async def generate_payload(reference_time=None):
            return expected_payload

        service._money_flow_now = lambda: datetime(2026, 4, 3, 19, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        service._money_flow_stock_recommendation_date = lambda now_local, force_today=False: "2026-04-02"
        service._load_money_flow_stock_payloads = lambda: {}
        service.generate_and_store_money_flow_stock_ideas = generate_payload

        result = await service.ensure_money_flow_stock_ideas_current()

        self.assertIs(result, expected_payload)

    async def test_generate_and_store_money_flow_falls_back_without_ai(self) -> None:
        class StubProvider:
            def __init__(self) -> None:
                self.ai_service = type("AIService", (), {"available": False})()

        service = DashboardService(provider=StubProvider(), settings=Settings())
        stored_reports: dict[str, dict] = {}

        async def get_sector_tab(timeframe: str, sort_order: str):
            return SectorTabResponse(
                total_sectors=3,
                sort_by="1W",
                sort_order="desc",
                sectors=[
                    SectorCard(
                        sector="Technology",
                        company_count=12,
                        sub_sector_count=3,
                        return_1d=1.2,
                        return_1w=4.5,
                        return_1m=8.1,
                        return_3m=14.2,
                        return_6m=20.0,
                        return_1y=31.0,
                        return_2y=45.0,
                        sparkline=[],
                        sub_sectors=[],
                    ),
                    SectorCard(
                        sector="Industrials",
                        company_count=10,
                        sub_sector_count=2,
                        return_1d=0.7,
                        return_1w=2.1,
                        return_1m=3.5,
                        return_3m=9.4,
                        return_6m=12.3,
                        return_1y=24.0,
                        return_2y=36.0,
                        sparkline=[],
                        sub_sectors=[],
                    ),
                    SectorCard(
                        sector="Energy",
                        company_count=8,
                        sub_sector_count=2,
                        return_1d=-1.1,
                        return_1w=-3.8,
                        return_1m=-6.4,
                        return_3m=-2.5,
                        return_6m=4.1,
                        return_1y=7.0,
                        return_2y=10.0,
                        sparkline=[],
                        sub_sectors=[],
                    ),
                ],
            )

        service.get_sector_tab = get_sector_tab
        service._load_money_flow_reports = lambda: {}
        service._save_money_flow_reports = lambda reports: stored_reports.update(reports)

        report = await service.generate_and_store_money_flow(
            reference_time=datetime(2026, 4, 4, 9, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        )

        self.assertIsInstance(report, MoneyFlowReport)
        self.assertEqual(report.week_key, "2026-W14")
        self.assertEqual(report.ai_model, "fallback-rules")
        self.assertEqual(report.inflows[0].name, "Technology")
        self.assertEqual(report.outflows[0].name, "Energy")
        self.assertIn("2026-W14", stored_reports)


class DashboardServiceMarketHealthHistoryTests(unittest.TestCase):
    def test_merge_market_health_into_historical_breadth_replaces_latest_date(self) -> None:
        service = DashboardService(provider=object(), settings=Settings())
        historical = HistoricalBreadthResponse(
            generated_at=datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc),
            universes=[
                HistoricalUniverseBreadth(
                    universe="Nifty 500",
                    history=[
                        HistoricalBreadthDataPoint(
                            date="2026-04-02",
                            above_ma20_pct=45.0,
                            above_ma50_pct=41.0,
                            above_sma200_pct=38.0,
                            new_high_52w_pct=4.0,
                            new_low_52w_pct=1.0,
                        )
                    ],
                )
            ],
        )
        market_health = MarketHealthResponse(
            generated_at=datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
            universes=[
                UniverseBreadth(
                    universe="Nifty 500",
                    total=500,
                    advances=300,
                    declines=150,
                    unchanged=50,
                    above_ma20_pct=55.0,
                    above_ma50_pct=52.0,
                    above_sma200_pct=47.0,
                    ma20_above_ma50_pct=49.0,
                    ma50_above_ma200_pct=44.0,
                    new_high_52w_pct=6.0,
                    new_low_52w_pct=0.5,
                    rsi_14_overbought_pct=8.0,
                    rsi_14_oversold_pct=2.0,
                )
            ],
        )

        merged = service._merge_market_health_into_historical_breadth(historical, market_health)

        self.assertEqual(merged.generated_at, market_health.generated_at)
        self.assertEqual(len(merged.universes), 1)
        self.assertEqual(len(merged.universes[0].history), 1)
        self.assertEqual(merged.universes[0].history[0].date, "2026-04-02")
        self.assertEqual(merged.universes[0].history[0].above_ma20_pct, 55.0)


class DashboardServiceIndustryGroupCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_industry_groups_uses_persisted_cache_when_snapshot_is_current(self) -> None:
        snapshot_updated_at = datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc)
        response = IndustryGroupsResponse(
            generated_at=snapshot_updated_at,
            as_of_date="2026-04-03",
            benchmark="NIFTY 500",
            filters=IndustryGroupFilters(min_market_cap_cr=800.0, min_avg_daily_value_cr=5.0),
            total_groups=1,
            groups=[
                IndustryGroupRankItem(
                    rank=1,
                    rank_label="#1",
                    strength_bucket="Top 10",
                    trend_label="Improving",
                    group_id="software",
                    group_name="Software",
                    parent_sector="Information Technology",
                    description="Software companies",
                    stock_count=1,
                    score=92.5,
                    return_1m=12.0,
                    return_3m=24.0,
                    return_6m=36.0,
                    relative_return_1m=4.0,
                    relative_return_3m=8.0,
                    relative_return_6m=12.0,
                    median_return_1m=11.0,
                    median_return_3m=22.0,
                    median_return_6m=33.0,
                    pct_above_50dma=100.0,
                    pct_above_200dma=100.0,
                    pct_outperform_benchmark_3m=100.0,
                    pct_outperform_benchmark_6m=100.0,
                    breadth_score=100.0,
                    trend_health_score=95.0,
                    leaders=["AAA"],
                    laggards=[],
                    top_constituents=[],
                    symbols=["AAA"],
                )
            ],
            master=[
                IndustryGroupMasterItem(
                    group_id="software",
                    group_name="Software",
                    parent_sector="Information Technology",
                    description="Software companies",
                    stock_count=1,
                    symbols=["AAA"],
                )
            ],
            stocks=[
                IndustryGroupStockItem(
                    symbol="AAA",
                    company_name="AAA Software",
                    exchange="NSE",
                    market_cap_cr=1200.0,
                    avg_traded_value_50d_cr=12.0,
                    sector="Information Technology",
                    raw_industry="Software",
                    final_group_id="software",
                    final_group_name="Software",
                    last_price=125.0,
                    change_pct=2.5,
                    return_1m=12.0,
                    return_3m=24.0,
                    return_6m=36.0,
                    return_1y=48.0,
                    rs_rating=97,
                )
            ],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            backend_root = Path(temp_dir)
            data_dir = backend_root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "industry_groups_cache.json").write_text(
                json.dumps(response.model_dump(mode="json"), indent=2),
                encoding="utf-8",
            )

            class StubProvider:
                def __init__(self, root: Path, updated_at: datetime) -> None:
                    self.backend_root = root
                    self.updated_at = updated_at

                async def get_snapshots(self, market_cap_min_crore: float):
                    raise AssertionError("persisted industry-group cache should avoid snapshot rebuilds")

                def get_snapshot_updated_at(self) -> datetime:
                    return self.updated_at

                def _default_exchange(self) -> str:
                    return "NSE"

            service = DashboardService(provider=StubProvider(backend_root, snapshot_updated_at), settings=Settings())

            cached = await service.get_industry_groups()

            self.assertEqual(cached.total_groups, 1)
            self.assertEqual(cached.groups[0].group_name, "Software")
            self.assertEqual(cached.stocks[0].symbol, "AAA")


class DashboardServiceVolumeLeaderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.builder = FreeMarketDataProvider()
        self.snapshot_updated_at = datetime(2026, 3, 31, 10, 0, tzinfo=timezone.utc)

    def _snapshot(self, *, symbol: str, start_close: float, step: float, volume_boost: int, keep_recent_volumes: bool) -> StockSnapshot:
        index = pd.bdate_range(end=self.snapshot_updated_at, periods=60)
        history = pd.DataFrame(
            [
                {
                    "Open": start_close + (idx * step) - 1,
                    "High": start_close + (idx * step) + 2,
                    "Low": start_close + (idx * step) - 2,
                    "Close": start_close + (idx * step),
                    "Adj Close": start_close + (idx * step),
                    "Volume": 100_000 + (idx * 500),
                    "Stock Splits": 0.0,
                }
                for idx in range(len(index))
            ],
            index=index,
        )
        row = self.builder._history_to_snapshot(
            {
                "symbol": symbol,
                "name": f"{symbol} Industries",
                "exchange": "NSE",
                "listing_date": "2020-01-02",
                "sector": "Industrials",
                "sub_sector": "Capital Goods",
                "market_cap_crore": 5000.0,
                "ticker": f"{symbol}.NS",
            },
            history,
            pd.Series([1000 + idx for idx in range(len(index))], index=index, dtype=float),
        )
        assert row is not None
        row["volume"] = int(row["avg_volume_20d"] * volume_boost)
        if not keep_recent_volumes:
            row["recent_volumes"] = []
        return StockSnapshot.model_validate(row)

    async def test_build_dashboard_excludes_unreliable_relative_volume_rows(self) -> None:
        unreliable = self._snapshot(symbol="NOHIST", start_close=100.0, step=0.8, volume_boost=40, keep_recent_volumes=False)
        reliable = self._snapshot(symbol="HIST", start_close=120.0, step=0.9, volume_boost=12, keep_recent_volumes=True)

        class StubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                raise NotImplementedError

            async def get_index_quotes(self, symbols: list[str]):
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=StubProvider([unreliable, reliable], self.snapshot_updated_at), settings=Settings())

        response = await service.build_dashboard()

        self.assertIn("HIST", [item.symbol for item in response.top_volume_spikes])
        self.assertNotIn("NOHIST", [item.symbol for item in response.top_volume_spikes])


if __name__ == "__main__":
    unittest.main()
