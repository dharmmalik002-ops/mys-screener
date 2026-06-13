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
    WatchlistItem,
    WatchlistsStateResponse,
)
from app.providers.free import FreeMarketDataProvider
from app.scanners.definitions import SCAN_BY_ID, run_consolidating_scan, run_custom_scan, run_returns_scan, run_scan
from app.services.dashboard_service import DashboardService


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

    def test_watchlists_migrate_from_legacy_repo_data_to_app_state_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            legacy_backend_root = temp_root / "legacy-backend"
            legacy_data_dir = legacy_backend_root / "data"
            legacy_data_dir.mkdir(parents=True, exist_ok=True)
            legacy_watchlists_path = legacy_data_dir / "watchlists_state.json"
            legacy_payload = {
                "market": "india",
                "updated_at": "2026-04-18T03:00:00Z",
                "active_watchlist_id": "wl-1",
                "watchlists": [
                    {
                        "id": "wl-1",
                        "name": "Core",
                        "color": "#4f8cff",
                        "symbols": ["INFY", "TCS"],
                    }
                ],
            }
            legacy_watchlists_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

            class StubProvider:
                def __init__(self, backend_root: Path) -> None:
                    self.backend_root = backend_root

                @staticmethod
                def _default_exchange() -> str:
                    return "NSE"

            settings = Settings(app_state_dir=temp_root / "state")
            service = DashboardService(provider=StubProvider(legacy_backend_root), settings=settings)

            state = service.get_watchlists_state()

            self.assertEqual(state.active_watchlist_id, "wl-1")
            self.assertEqual(state.watchlists[0].symbols, ["INFY", "TCS"])
            self.assertTrue((settings.app_state_dir / "data" / "watchlists_state.json").exists())

    def test_save_watchlists_state_writes_outside_repo_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            legacy_backend_root = temp_root / "legacy-backend"
            (legacy_backend_root / "data").mkdir(parents=True, exist_ok=True)

            class StubProvider:
                def __init__(self, backend_root: Path) -> None:
                    self.backend_root = backend_root

                @staticmethod
                def _default_exchange() -> str:
                    return "NSE"

            settings = Settings(app_state_dir=temp_root / "state")
            service = DashboardService(provider=StubProvider(legacy_backend_root), settings=settings)
            payload = WatchlistsStateResponse(
                market="india",
                active_watchlist_id="wl-1",
                watchlists=[
                    WatchlistItem(
                        id="wl-1",
                        name="Swing",
                        color="#22c55e",
                        symbols=["DIXON", "BSE"],
                    )
                ],
            )

            service.save_watchlists_state(payload)

            self.assertTrue((settings.app_state_dir / "data" / "watchlists_state.json").exists())
            self.assertFalse((legacy_backend_root / "data" / "watchlists_state.json").exists())

    def test_contraction_scan_tolerates_legacy_snapshot_without_recent_closes(self) -> None:
        snapshot = self._build_snapshot(
            symbol="LEGACY",
            name="Legacy Industries",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=12_000.0,
            start_close=100.0,
            step=0.55,
        )
        legacy_payload = snapshot.model_dump(mode="python")
        legacy_payload.pop("recent_closes", None)

        legacy_snapshot = StockSnapshot.model_validate(legacy_payload)
        results = run_scan(SCAN_BY_ID["contraction"], [legacy_snapshot])

        self.assertIsInstance(results, list)

    async def test_contraction_scan_results_enrich_sparse_snapshots_from_chart_history(self) -> None:
        snapshot = self._build_snapshot(
            symbol="CNTRH",
            name="Contraction History Match",
            sector="Industrials",
            sub_sector="Capital Goods",
            market_cap_crore=8_900.0,
            start_close=100.0,
            step=0.45,
        )
        snapshot.last_price = 150.0
        snapshot.previous_close = 148.0
        snapshot.change_pct = round(((150.0 / 148.0) - 1) * 100, 2)
        snapshot.ema50 = 135.0
        snapshot.sma50 = 125.0
        snapshot.avg_volume_50d = 60_000
        snapshot.volume = 38_000
        snapshot.recent_closes = []
        snapshot.baseline_close_5d = None
        snapshot.baseline_close_20d = None
        snapshot.baseline_close_60d = None
        snapshot.baseline_close_63d = None
        snapshot.stock_return_5d = 5.0
        snapshot.stock_return_20d = 24.0
        snapshot.stock_return_60d = 18.0

        self.assertEqual(run_scan(SCAN_BY_ID["contraction"], [snapshot]), [])

        closes = [120.0, 124.0, 128.0, 132.0, 136.0, 140.0, 142.0, 143.0, 144.0, 144.8, 148.0, 150.0]

        class StubProvider:
            def __init__(self, rows: list[StockSnapshot], updated_at: datetime) -> None:
                self.rows = rows
                self.updated_at = updated_at

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return self.rows

            async def get_chart(self, symbol: str, timeframe: str, bars: int = 240):
                del bars
                if symbol != "CNTRH" or timeframe != "1D":
                    return []
                base_time = int(self.updated_at.timestamp()) - (len(closes) * 86400)
                return [
                    ChartBar(
                        time=base_time + (index * 86400),
                        open=close - 1,
                        high=close + 1,
                        low=close - 2,
                        close=close,
                        volume=50_000,
                    )
                    for index, close in enumerate(closes)
                ]

            async def get_index_quotes(self, symbols: list[str]):
                del symbols
                return []

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                del symbol, snapshot
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                del market_cap_min_crore
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=StubProvider([snapshot], self.snapshot_updated_at), settings=Settings())

        response = await service.get_scan_results("contraction", include_sector_summaries=False)

        self.assertEqual([item.symbol for item in response.items], ["CNTRH"])

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

    async def test_refresh_market_data_uses_live_refresh_when_provider_supports_intraday_updates(self) -> None:
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
                self.live_refresh_calls = 0

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                return [self.row]

            def preferred_refresh_strategy(self) -> str:
                return "cache"

            async def refresh_live_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.live_refresh_calls += 1
                self.updated_at = self.updated_at + timedelta(minutes=5)
                return [self.row]

            async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                raise AssertionError("historical refresh should not be used for live updates")

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {"applied_quote_count": 42, "historical_rebuild": False, "quote_source": "nse"}

        provider = RefreshProvider(snapshot, self.snapshot_updated_at)
        service = DashboardService(provider=provider, settings=Settings())
        service._dashboard_cache = object()

        response = await service.refresh_market_data()

        self.assertEqual(provider.live_refresh_calls, 1)
        self.assertIsNone(service._dashboard_cache)
        self.assertEqual(response["refresh_mode"], "live-refresh")
        self.assertEqual(response["applied_quote_count"], 42)
        self.assertEqual(response["quote_source"], "nse")

    async def test_refresh_market_data_skips_live_refresh_for_eod_only_provider(self) -> None:
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
            eod_only_mode = True

            def __init__(self, row: StockSnapshot, updated_at: datetime) -> None:
                self.row = row
                self.updated_at = updated_at
                self.get_snapshot_calls = 0

            async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                self.get_snapshot_calls += 1
                return [self.row]

            def preferred_refresh_strategy(self) -> str:
                return "cache"

            async def refresh_live_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                raise AssertionError("EOD-only providers should not use live refresh")

            async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
                raise AssertionError("Historical refresh should not run for current EOD cache")

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {"applied_quote_count": 0, "historical_rebuild": False, "quote_source": None}

        provider = RefreshProvider(snapshot, self.snapshot_updated_at)
        service = DashboardService(provider=provider, settings=Settings())

        response = await service.refresh_market_data()

        self.assertEqual(provider.get_snapshot_calls, 1)
        self.assertEqual(response["refresh_mode"], "cached-current")
        self.assertEqual(response["applied_quote_count"], 0)

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
            cache_payload = response.model_dump(mode="json")
            cache_payload["cache_version"] = 2
            (data_dir / "industry_groups_cache.json").write_text(
                json.dumps(cache_payload, indent=2),
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

    async def test_build_dashboard_returns_twenty_market_leaders(self) -> None:
        snapshots = [
            self._snapshot(
                symbol=f"STK{index:02d}",
                start_close=80.0 + index,
                step=0.4 + (index * 0.02),
                volume_boost=2 + (index / 10),
                keep_recent_volumes=True,
            ).model_copy(update={"change_pct": float(index - 12)})
            for index in range(25)
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
                raise NotImplementedError

            async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None):
                raise NotImplementedError

            async def refresh_snapshots(self, market_cap_min_crore: float):
                raise NotImplementedError

            def get_snapshot_updated_at(self) -> datetime:
                return self.updated_at

            def get_last_refresh_metadata(self) -> dict[str, object]:
                return {}

        service = DashboardService(provider=StubProvider(snapshots, self.snapshot_updated_at), settings=Settings())

        response = await service.build_dashboard()

        self.assertEqual(len(response.top_gainers), 20)
        self.assertEqual(len(response.top_losers), 20)
        self.assertEqual(len(response.top_volume_spikes), 20)
        self.assertEqual(response.top_gainers[0].symbol, "STK24")
        self.assertEqual(response.top_losers[0].symbol, "STK00")


if __name__ == "__main__":
    unittest.main()
