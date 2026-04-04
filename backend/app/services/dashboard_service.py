import asyncio
import csv
import io
import json
import re
import time
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

from app.core.config import Settings
from app.models.market import (
    AlertItem,
    ChartGridCard,
    ChartGridResponse,
    ChartGridSeriesItem,
    ChartGridSeriesResponse,
    ChartGridTimeframe,
    ChartResponse,
    ChartLineMarker,
    ChartLinePoint,
    CompanyQuestionResponse,
    CompanyFundamentals,
    ConsolidatingScanRequest,
    CustomScanRequest,
    DashboardResponse,
    DetailedNews,
    HistoricalBreadthDataPoint,
    IndexPeHistoryResponse,
    IndexPePoint,
    IndexQuotesResponse,
    HistoricalUniverseBreadth,
    ImprovingRsItem,
    ImprovingRsResponse,
    ImprovingRsWindow,
    IndustryGroupsResponse,
    HistoricalBreadthResponse,
    MarketHealthResponse,
    MoneyFlowReport,
    MoneyFlowHistoryResponse,
    MoneyFlowSector,
    MoneyFlowStockIdea,
    MoneyFlowStockIdeasHistoryResponse,
    MoneyFlowStockIdeasResponse,
    NearPivotScanRequest,
    PullBackScanRequest,
    ReturnsScanRequest,
    ScanDescriptor,
    ScanMatch,
    ScanSectorSummary,
    ScanResultsResponse,
    SectorCard,
    SectorCompanyItem,
    SectorGroupKind,
    SectorGroup,
    SectorSortBy,
    SectorTabResponse,
    StockOverview,
    StockSnapshot,
    UniverseBreadth,
    WatchlistItem,
    WatchlistsStateResponse,
)
from app.providers.base import MarketDataProvider
from app.scanners.definitions import (
    run_consolidating_scan,
    run_custom_scan,
    run_returns_scan,
    scan_catalog_with_counts,
    scanner_sector_label,
)
from app.services.industry_groups import build_industry_groups_response, write_industry_group_files

INDEX_HEATMAP_SOURCES: tuple[tuple[str, str], ...] = (
    ("Nifty 50", "https://niftyindices.com/IndexConstituent/ind_nifty50list.csv"),
    ("Nifty Midcap 50", "https://niftyindices.com/IndexConstituent/ind_niftymidcap50list.csv"),
    ("Nifty SmallCap 250", "https://niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"),
    ("Nifty 500", "https://niftyindices.com/IndexConstituent/ind_nifty500list.csv"),
)

INDEX_HEATMAP_SYMBOLS: dict[str, str] = {
    "Nifty 50": "^NSEI",
    "Nifty Midcap 50": "^NSEMDCP50",
    "Nifty SmallCap 250": "^CNXSC",
    "Nifty 500": "^CNX500",
}
INDEX_CONSTITUENT_CACHE_MAX_AGE = timedelta(hours=6)
INDEX_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
IST = timezone(timedelta(hours=5, minutes=30))
GENERIC_COMPLIANCE_KEYWORDS = (
    "regulation 30",
    "sebi (lodr)",
    "lodr regulations",
    "analyst / investor meet",
    "analyst/institutional investor meet",
    "investor meet",
    "investor meeting",
    "analyst meeting",
    "one-on-one meeting",
    "plant visit",
    "institutional investor",
    "investment community",
    "engaging with investors",
    "newspaper publication",
    "share certificate",
    "loss of share certificate",
    "trading window",
    "voting results",
    "scrutinizer",
    "certificate under regulation",
    "compliance certificate",
    "intimation",
    "audio recording",
    "transcript",
)
MATERIAL_IMPACT_KEYWORDS = (
    "revenue",
    "sales",
    "profit",
    "margin",
    "ebitda",
    "earnings",
    "guidance",
    "outlook",
    "demand",
    "order",
    "contract",
    "deal",
    "approval",
    "capacity",
    "plant",
    "capex",
    "expansion",
    "acquisition",
    "merger",
    "demerger",
    "commissioning",
    "launch",
    "price increase",
    "pricing",
    "volume growth",
    "market share",
    "bookings",
    "pipeline",
    "cost",
    "operating leverage",
    "roce",
    "roe",
    "usfda",
    "warning letter",
    "shutdown",
    "fire",
    "default",
    "downgrade",
    "upgrade",
    "buyback",
)
MANAGEMENT_COMMENTARY_KEYWORDS = (
    "revenue",
    "growth",
    "margin",
    "profit",
    "sales",
    "demand",
    "capacity",
    "order",
    "book",
    "pricing",
    "ebitda",
    "capex",
    "guidance",
    "outlook",
)
RESULTS_UPDATE_KEYWORDS = (
    "financial results",
    "quarterly results",
    "results for the quarter",
    "outcome of board meeting",
    "statement of standalone",
    "statement of consolidated",
    "audited financial results",
    "unaudited financial results",
    "earnings release",
)


def build_leader_match(scan_id: str, snapshot: StockSnapshot, score: float, reason: str) -> ScanMatch:
    display_sector = scanner_sector_label(snapshot.sector, snapshot.sub_sector)
    return ScanMatch(
        scan_id=scan_id,
        symbol=snapshot.symbol,
        name=snapshot.name,
        exchange=snapshot.exchange,
        sector=display_sector,
        sub_sector=snapshot.sub_sector,
        market_cap_crore=snapshot.market_cap_crore,
        last_price=snapshot.last_price,
        change_pct=snapshot.change_pct,
        relative_volume=snapshot.relative_volume,
        avg_rupee_volume_30d_crore=snapshot.avg_rupee_volume_30d_crore,
        score=score,
        pattern="Leaders",
        rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
        rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
        rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
        rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
        nifty_outperformance=snapshot.nifty_outperformance,
        sector_outperformance=snapshot.sector_outperformance,
        three_month_rs=snapshot.three_month_rs,
        stock_return_20d=snapshot.stock_return_20d,
        stock_return_60d=snapshot.stock_return_60d,
        stock_return_12m=snapshot.stock_return_12m,
        gap_pct=snapshot.gap_pct,
        reasons=[reason],
    )


def build_stock_overview(snapshot: StockSnapshot) -> StockOverview:
    return StockOverview(
        symbol=snapshot.symbol,
        name=snapshot.name,
        exchange=snapshot.exchange,
        sector=snapshot.sector,
        sub_sector=snapshot.sub_sector,
        circuit_band_label=snapshot.circuit_band_label,
        upper_circuit_limit=snapshot.upper_circuit_limit,
        lower_circuit_limit=snapshot.lower_circuit_limit,
        market_cap_crore=snapshot.market_cap_crore,
        last_price=snapshot.last_price,
        change_pct=snapshot.change_pct,
        relative_volume=snapshot.relative_volume,
        avg_rupee_volume_30d_crore=snapshot.avg_rupee_volume_30d_crore,
        rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
        rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
        rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
        rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
        nifty_outperformance=snapshot.nifty_outperformance,
        sector_outperformance=snapshot.sector_outperformance,
        three_month_rs=snapshot.three_month_rs,
        stock_return_5d=snapshot.stock_return_5d,
        stock_return_20d=snapshot.stock_return_20d,
        stock_return_60d=snapshot.stock_return_60d,
        stock_return_126d=snapshot.stock_return_126d,
        stock_return_12m=snapshot.stock_return_12m,
        adr_pct_20=snapshot.adr_pct_20,
        pct_from_52w_high=snapshot.pct_from_52w_high,
        pct_from_ath=snapshot.pct_from_ath,
        pct_from_52w_low=snapshot.pct_from_52w_low,
        gap_pct=snapshot.gap_pct,
    )


class DashboardService:
    def __init__(self, provider: MarketDataProvider, settings: Settings) -> None:
        self.provider = provider
        self.settings = settings
        self._dashboard_cache: DashboardResponse | None = None
        self._scan_catalog_cache: tuple[datetime, list[ScanDescriptor], dict[str, list[ScanMatch]]] | None = None
        self._sector_tab_cache: dict[tuple[str, str], SectorTabResponse] = {}
        self._improving_rs_cache: dict[ImprovingRsWindow, ImprovingRsResponse] = {}
        self._chart_grid_cache: dict[tuple[str, str, str, datetime], ChartGridResponse] = {}
        self._chart_response_cache: dict[tuple[str, str], tuple[datetime, datetime, ChartResponse]] = {}
        self._index_constituents_cache: tuple[datetime, dict[str, set[str]]] | None = None
        self._index_constituent_task: asyncio.Task[dict[str, set[str]]] | None = None
        self._scan_sector_summary_cache: dict[tuple[str, str, datetime], list[ScanSectorSummary]] = {}
        self._market_overview_cache: tuple[float, object] | None = None
        self._industry_groups_cache: IndustryGroupsResponse | None = None
        self._market_health_cache: MarketHealthResponse | None = None
        self._sector_rotation_cache = None
        # Resolve money-flow storage path next to data/
        from pathlib import Path
        _backend_root = Path(__file__).resolve().parents[2]
        self._money_flow_cache_path = _backend_root / "data" / "money_flow_reports.json"
        self._money_flow_stock_cache_path = _backend_root / "data" / "money_flow_stock_ideas.json"

    @staticmethod
    def _chart_bar_limit(timeframe: str) -> int:
        if timeframe == "1D":
            return 1300
        if timeframe == "1W":
            return 520
        if timeframe == "1h":
            return 1400
        return 500

    @staticmethod
    def _chart_grid_bar_limit(timeframe: ChartGridTimeframe) -> int:
        return {
            "3M": 78,
            "6M": 132,
            "1Y": 260,
            "2Y": 520,
        }[timeframe]

    @classmethod
    def _chart_grid_series_bar_limit(cls) -> int:
        return cls._chart_grid_bar_limit("2Y")

    @staticmethod
    def _chart_grid_point_limit(timeframe: ChartGridTimeframe) -> int:
        return {
            "3M": 60,
            "6M": 72,
            "1Y": 96,
            "2Y": 120,
        }[timeframe]

    @staticmethod
    def _chart_response_cache_ttl(timeframe: str) -> timedelta:
        if timeframe in {"15m", "30m"}:
            return timedelta(seconds=60)
        if timeframe == "1h":
            return timedelta(seconds=120)
        if timeframe == "1D":
            return timedelta(seconds=300)
        return timedelta(minutes=10)

    async def _snapshots(self):
        return await self.provider.get_snapshots(self.settings.market_cap_min_crore)

    def _snapshot_updated_at(self) -> datetime:
        return self.provider.get_snapshot_updated_at() or datetime.now(timezone.utc)

    @staticmethod
    def _copy_scan_descriptors(scanners: list[ScanDescriptor]) -> list[ScanDescriptor]:
        return [scanner.model_copy() for scanner in scanners]

    def _scan_catalog(self, snapshots: list[StockSnapshot]) -> tuple[list[ScanDescriptor], dict[str, list[ScanMatch]]]:
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._scan_catalog_cache
        if cached is not None:
            cached_updated_at, cached_scanners, cached_results = cached
            if cached_updated_at >= snapshot_updated_at:
                return self._copy_scan_descriptors(cached_scanners), cached_results

        scanners, results = scan_catalog_with_counts(snapshots)
        copied = self._copy_scan_descriptors(scanners)
        self._scan_catalog_cache = (snapshot_updated_at, copied, results)
        return self._copy_scan_descriptors(copied), results

    @staticmethod
    def _has_reliable_classification(snapshot: StockSnapshot) -> bool:
        if str(snapshot.exchange or "").strip().upper() != "BSE":
            return True
        sector = str(snapshot.sector or "").strip()
        sub_sector = str(snapshot.sub_sector or "").strip()
        return sector not in {"", "Unclassified"} and sub_sector not in {"", "Unclassified"}

    def _scan_eligible_snapshots(self, snapshots: list[StockSnapshot]) -> list[StockSnapshot]:
        return [snapshot for snapshot in snapshots if self._has_reliable_classification(snapshot)]

    @staticmethod
    def _has_reliable_relative_volume(snapshot: StockSnapshot) -> bool:
        return snapshot.avg_volume_20d > 0 and len(snapshot.recent_volumes) >= 5

    def _index_constituents_cache_fresh(self) -> bool:
        if self._index_constituents_cache is None:
            return False
        fetched_at, _ = self._index_constituents_cache
        return datetime.now(timezone.utc) - fetched_at <= INDEX_CONSTITUENT_CACHE_MAX_AGE

    def _load_index_constituents_map(self) -> dict[str, set[str]]:
        if self._index_constituents_cache_fresh():
            assert self._index_constituents_cache is not None
            return self._index_constituents_cache[1]

        cached_map = self._index_constituents_cache[1] if self._index_constituents_cache is not None else {}
        headers = {
            "User-Agent": INDEX_USER_AGENT,
            "Accept": "text/csv,text/plain,*/*",
            "Referer": "https://niftyindices.com/",
        }

        downloaded: dict[str, set[str]] = {}
        try:
            with httpx.Client(timeout=20, headers=headers, follow_redirects=True) as client:
                for index_name, url in INDEX_HEATMAP_SOURCES:
                    response = client.get(url)
                    response.raise_for_status()
                    reader = csv.DictReader(io.StringIO(response.text))
                    members = {
                        str(row.get("Symbol") or row.get("SYMBOL") or "").strip().upper()
                        for row in reader
                        if str(row.get("Symbol") or row.get("SYMBOL") or "").strip()
                    }
                    if members:
                        downloaded[index_name] = members
        except Exception:
            if cached_map:
                return cached_map
            return {}

        if downloaded:
            self._index_constituents_cache = (datetime.now(timezone.utc), downloaded)
            return downloaded

        return cached_map

    def _cached_index_constituents_or_schedule_refresh(self) -> dict[str, set[str]]:
        if self._index_constituents_cache_fresh():
            assert self._index_constituents_cache is not None
            return self._index_constituents_cache[1]

        cached_map = self._index_constituents_cache[1] if self._index_constituents_cache is not None else {}
        current_task = self._index_constituent_task
        if current_task is None or current_task.done():
            task = asyncio.create_task(asyncio.to_thread(self._load_index_constituents_map))
            self._index_constituent_task = task

            def clear_task(completed_task: asyncio.Task[dict[str, set[str]]]) -> None:
                if self._index_constituent_task is completed_task:
                    self._index_constituent_task = None

            task.add_done_callback(clear_task)

        return cached_map

    def _build_sector_card(
        self,
        sector_name: str,
        members: list[StockSnapshot],
        grouped_members: dict[str, list[StockSnapshot]],
        group_kind: SectorGroupKind,
        sort_by: SectorSortBy,
        sort_field: str,
        reverse: bool,
        metric_field_map: dict[str, str],
        last_price: float | None = None,
    ) -> SectorCard:
        return SectorCard(
            group_kind=group_kind,
            sector=sector_name,
            company_count=len(members),
            sub_sector_count=len(grouped_members),
            last_price=last_price,
            return_1d=self._weighted_average_return(members, "change_pct"),
            return_1w=self._weighted_average_return(members, "stock_return_5d"),
            return_1m=self._weighted_average_return(members, "stock_return_20d"),
            return_3m=self._weighted_average_return(members, "stock_return_60d"),
            return_6m=self._weighted_average_return(members, "stock_return_126d"),
            return_1y=self._weighted_average_return(members, "stock_return_12m"),
            return_2y=self._weighted_average_return(members, "stock_return_504d"),
            sparkline=self._aggregate_group_sparkline(members),
            sub_sectors=[
                SectorGroup(
                    sub_sector=sub_sector,
                    company_count=len(companies),
                    companies=sorted(
                        [
                            SectorCompanyItem(
                                symbol=item.symbol,
                                name=item.name,
                                exchange=item.exchange,
                                sector=item.sector,
                                sub_sector=item.sub_sector,
                                market_cap_crore=item.market_cap_crore,
                                last_price=item.last_price,
                                return_1d=item.change_pct,
                                return_1w=item.stock_return_5d,
                                return_1m=item.stock_return_20d,
                                return_3m=item.stock_return_60d,
                                return_6m=item.stock_return_126d,
                                return_1y=item.stock_return_12m,
                                return_2y=item.stock_return_504d,
                                rs_rating=item.rs_rating if item.rs_eligible else None,
                            )
                            for item in companies
                        ],
                        key=lambda company: (
                            getattr(company, sort_field),
                            company.rs_rating or 0,
                            company.market_cap_crore,
                        ),
                        reverse=reverse,
                    ),
                )
                for sub_sector, companies in sorted(
                    grouped_members.items(),
                    key=lambda entry: (
                        self._weighted_average_return(entry[1], metric_field_map[sort_by]),
                        len(entry[1]),
                    ),
                    reverse=reverse,
                )
            ],
        )

    def _aggregate_group_sparkline(self, members: list[StockSnapshot]) -> list[ChartLinePoint]:
        weighted_series: list[pd.Series] = []
        weights: list[float] = []

        for member in members:
            points = [
                point
                for point in (member.chart_grid_points or [])
                if point.value not in (None, 0)
            ]
            if len(points) < 2:
                continue

            index = pd.to_datetime([point.time for point in points], unit="s", utc=True)
            series = pd.Series([float(point.value) for point in points], index=index, dtype=float)
            series = series[~series.index.duplicated(keep="last")].sort_index()
            if len(series) < 2:
                continue

            baseline = float(series.iloc[0])
            if baseline <= 0:
                continue

            weight = max(float(member.market_cap_crore or 0.0), 1.0)
            weighted_series.append((series / baseline) * 100.0)
            weights.append(weight)

        if not weighted_series:
            return []

        combined = pd.concat(weighted_series, axis=1).sort_index()
        combined = combined.ffill().dropna(how="all")
        if combined.empty:
            return []

        active_weights = combined.notna().mul(weights, axis=1)
        weighted_values = combined.ffill().mul(weights, axis=1)
        total_weights = active_weights.sum(axis=1).replace(0, pd.NA)
        aggregate = (weighted_values.sum(axis=1) / total_weights).dropna()
        if aggregate.empty:
            return []

        points = [
            ChartLinePoint(
                time=int(index_value.to_pydatetime().replace(tzinfo=timezone.utc).timestamp()),
                value=round(float(value), 4),
            )
            for index_value, value in aggregate.items()
        ]
        return self._downsample_chart_points(points, self._chart_grid_point_limit("2Y"))

    def _snapshot_age_minutes(self, snapshot_updated_at: datetime) -> int:
        age = datetime.now(timezone.utc) - snapshot_updated_at
        return max(0, int(age.total_seconds() // 60))

    def _market_status(self, snapshot_updated_at: datetime) -> str:
        if self.settings.data_mode == "demo":
            return "Demo feed ready"
        age_minutes = self._snapshot_age_minutes(snapshot_updated_at)
        if age_minutes <= 20:
            return "Market snapshot refreshed"
        if age_minutes < 60:
            return f"Snapshot {age_minutes}m old"
        age_hours = round(age_minutes / 60, 1)
        return f"Snapshot {age_hours}h old"

    def _calculate_market_breadth(self, universe_name: str, universe_stocks: list[StockSnapshot]) -> UniverseBreadth:
        total = len(universe_stocks)
        if total == 0:
            return UniverseBreadth(
                universe=universe_name,
                total=0,
                advances=0,
                declines=0,
                unchanged=0,
                above_ma20_pct=0.0,
                above_ma50_pct=0.0,
                above_sma200_pct=0.0,
                ma20_above_ma50_pct=0.0,
                ma50_above_ma200_pct=0.0,
                new_high_52w_pct=0.0,
                new_low_52w_pct=0.0,
                rsi_14_overbought_pct=0.0,
                rsi_14_oversold_pct=0.0,
            )

        advances = sum(1 for snapshot in universe_stocks if snapshot.change_pct > 0)
        declines = sum(1 for snapshot in universe_stocks if snapshot.change_pct < 0)
        unchanged = total - advances - declines

        above_ma20_eligible = [snapshot for snapshot in universe_stocks if snapshot.ema20 is not None]
        above_ma50_eligible = [snapshot for snapshot in universe_stocks if snapshot.ema50 is not None]
        above_ma200_eligible = [snapshot for snapshot in universe_stocks if snapshot.sma200 is not None]
        ma20_above_50_eligible = [snapshot for snapshot in universe_stocks if snapshot.ema20 is not None and snapshot.ema50 is not None]
        ma50_above_200_eligible = [snapshot for snapshot in universe_stocks if snapshot.ema50 is not None and snapshot.sma200 is not None]

        above_ma20 = sum(1 for snapshot in above_ma20_eligible if snapshot.last_price > (snapshot.ema20 or 0))
        above_ma50 = sum(1 for snapshot in above_ma50_eligible if snapshot.last_price > (snapshot.ema50 or 0))
        above_ma200 = sum(1 for snapshot in above_ma200_eligible if snapshot.last_price > (snapshot.sma200 or 0))

        ma20_above_50 = sum(1 for snapshot in ma20_above_50_eligible if (snapshot.ema20 or 0) > (snapshot.ema50 or 0))
        ma50_above_200 = sum(1 for snapshot in ma50_above_200_eligible if (snapshot.ema50 or 0) > (snapshot.sma200 or 0))

        new_highs = sum(1 for snapshot in universe_stocks if snapshot.last_price >= snapshot.high_52w * 0.98)
        new_lows = sum(1 for snapshot in universe_stocks if snapshot.low_52w and snapshot.last_price <= snapshot.low_52w * 1.02)

        rsi_overbought = sum(1 for snapshot in universe_stocks if snapshot.rsi_14 > 70)
        rsi_oversold = sum(1 for snapshot in universe_stocks if snapshot.rsi_14 < 30)

        return UniverseBreadth(
            universe=universe_name,
            total=total,
            advances=advances,
            declines=declines,
            unchanged=unchanged,
            above_ma20_pct=round((above_ma20 / len(above_ma20_eligible)) * 100, 2) if above_ma20_eligible else 0.0,
            above_ma50_pct=round((above_ma50 / len(above_ma50_eligible)) * 100, 2) if above_ma50_eligible else 0.0,
            above_sma200_pct=round((above_ma200 / len(above_ma200_eligible)) * 100, 2) if above_ma200_eligible else 0.0,
            ma20_above_ma50_pct=round((ma20_above_50 / len(ma20_above_50_eligible)) * 100, 2) if ma20_above_50_eligible else 0.0,
            ma50_above_ma200_pct=round((ma50_above_200 / len(ma50_above_200_eligible)) * 100, 2) if ma50_above_200_eligible else 0.0,
            new_high_52w_pct=round((new_highs / total) * 100, 2),
            new_low_52w_pct=round((new_lows / total) * 100, 2),
            rsi_14_overbought_pct=round((rsi_overbought / total) * 100, 2),
            rsi_14_oversold_pct=round((rsi_oversold / total) * 100, 2),
        )

    def _resolve_market_health_universes(self, snapshots: list[StockSnapshot]) -> list[tuple[str, list[StockSnapshot]]]:
        snapshot_by_symbol = {str(snapshot.symbol or "").strip().upper(): snapshot for snapshot in snapshots if str(snapshot.symbol or "").strip()}
        try:
            constituent_map = self._load_index_constituents_map()
        except Exception:
            constituent_map = {}

        universes: list[tuple[str, list[StockSnapshot]]] = []
        for universe_name, fallback_size in (("Nifty 500", 500), ("Nifty 50", 50)):
            members = constituent_map.get(universe_name, set())
            resolved = [snapshot_by_symbol[symbol] for symbol in members if symbol in snapshot_by_symbol]
            if not resolved:
                resolved = snapshots[:fallback_size]
            universes.append((universe_name, resolved))
        return universes

    async def get_market_health(self) -> MarketHealthResponse:
        snapshots = await self._snapshots()
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._market_health_cache
        if cached is not None and cached.generated_at >= snapshot_updated_at:
            return cached

        universes = self._resolve_market_health_universes(snapshots)

        response = MarketHealthResponse(
            generated_at=snapshot_updated_at,
            universes=[
                self._calculate_market_breadth(universe_name, universe_stocks)
                for universe_name, universe_stocks in universes
            ]
        )
        self._market_health_cache = response
        return response

    async def warm_startup_views(self) -> None:
        await self.build_dashboard()
        await asyncio.gather(
            self.get_sector_tab("1D", "desc"),
            self.get_sector_tab("1M", "desc"),
            self.get_industry_groups(),
            self.get_market_health(),
            self.get_sector_rotation(),
            self.get_money_flow_history(),
            self.get_money_flow_stock_ideas_history(),
            return_exceptions=True,
        )

    def get_historical_breadth(self) -> HistoricalBreadthResponse:
        from pathlib import Path
        import json
        cache_file = getattr(self.provider, "historical_breadth_cache_path", None)
        if cache_file is None:
            cache_file = Path(__file__).resolve().parents[2] / "data" / "free_historical_breadth.json"
        
        if not cache_file.exists():
            return HistoricalBreadthResponse(universes=[])
            
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            return HistoricalBreadthResponse.model_validate(data)
        except Exception:
            return HistoricalBreadthResponse(universes=[])

    def _historical_breadth_cache_path(self) -> Path:
        cache_file = getattr(self.provider, "historical_breadth_cache_path", None)
        if isinstance(cache_file, Path):
            return cache_file
        return Path(__file__).resolve().parents[2] / "data" / "free_historical_breadth.json"

    def _historical_breadth_point_from_universe(
        self,
        breadth: UniverseBreadth,
        date_value: str,
    ) -> HistoricalBreadthDataPoint:
        return HistoricalBreadthDataPoint(
            date=date_value,
            above_ma20_pct=breadth.above_ma20_pct,
            above_ma50_pct=breadth.above_ma50_pct,
            above_sma200_pct=breadth.above_sma200_pct,
            new_high_52w_pct=breadth.new_high_52w_pct,
            new_low_52w_pct=breadth.new_low_52w_pct,
        )

    def _merge_market_health_into_historical_breadth(
        self,
        historical: HistoricalBreadthResponse,
        market_health: MarketHealthResponse,
    ) -> HistoricalBreadthResponse:
        generated_at = market_health.generated_at or datetime.now(timezone.utc)
        latest_date = generated_at.astimezone(self._money_flow_timezone()).date().isoformat()
        history_by_universe: dict[str, dict[str, HistoricalBreadthDataPoint]] = {
            universe.universe: {point.date: point for point in universe.history}
            for universe in historical.universes
        }
        universe_order = [universe.universe for universe in historical.universes]

        for breadth in market_health.universes:
            if breadth.universe not in history_by_universe:
                history_by_universe[breadth.universe] = {}
                universe_order.append(breadth.universe)
            history_by_universe[breadth.universe][latest_date] = self._historical_breadth_point_from_universe(
                breadth,
                latest_date,
            )

        return HistoricalBreadthResponse(
            generated_at=generated_at,
            universes=[
                HistoricalUniverseBreadth(
                    universe=universe_name,
                    history=[
                        history_by_universe[universe_name][date_key]
                        for date_key in sorted(history_by_universe[universe_name].keys())
                    ],
                )
                for universe_name in universe_order
            ],
        )

    def _save_historical_breadth(self, payload: HistoricalBreadthResponse) -> None:
        cache_path = self._historical_breadth_cache_path()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload.model_dump(mode="json"), indent=2), encoding="utf-8")

    async def refresh_historical_breadth_latest(self) -> HistoricalBreadthResponse:
        snapshot_before = self.provider.get_snapshot_updated_at()
        snapshots = await self.provider.get_snapshots(self.settings.market_cap_min_crore)

        snapshot_after = self.provider.get_snapshot_updated_at()
        if snapshot_after != snapshot_before:
            self._clear_runtime_caches()

        live_market_health = MarketHealthResponse(
            generated_at=snapshot_after,
            universes=[
                self._calculate_market_breadth(universe_name, universe_stocks)
                for universe_name, universe_stocks in self._resolve_market_health_universes(snapshots)
            ],
        )
        merged = self._merge_market_health_into_historical_breadth(self.get_historical_breadth(), live_market_health)
        await asyncio.to_thread(self._save_historical_breadth, merged)
        return merged

    async def build_dashboard(self) -> DashboardResponse:
        snapshots = await self._snapshots()
        scan_snapshots = self._scan_eligible_snapshots(snapshots)
        snapshot_updated_at = self._snapshot_updated_at()
        cached_dashboard = self._dashboard_cache
        if cached_dashboard is not None and cached_dashboard.generated_at >= snapshot_updated_at:
            return cached_dashboard
        scanners, results = self._scan_catalog(scan_snapshots)

        top_gainers = sorted(scan_snapshots, key=lambda item: item.change_pct, reverse=True)[:5]
        top_losers = sorted(scan_snapshots, key=lambda item: item.change_pct)[:5]
        top_volume = sorted(
            [item for item in scan_snapshots if self._has_reliable_relative_volume(item)],
            key=lambda item: item.relative_volume,
            reverse=True,
        )[:5]

        alerts: list[AlertItem] = []
        for scan_id in ("breakout-ath", "volume-price", "strong-nifty", "darvas-box"):
            first_hit = next(iter(results.get(scan_id, [])), None)
            if not first_hit:
                continue
            scan_name = next(scan.name for scan in scanners if scan.id == scan_id)
            alerts.append(
                AlertItem(
                    id=f"{scan_id}:{first_hit.symbol}",
                    symbol=first_hit.symbol,
                    scan_name=scan_name,
                    message=f"{first_hit.symbol} flagged in {scan_name.lower()}",
                )
            )

        response = DashboardResponse(
            app_name=self.settings.app_name,
            generated_at=snapshot_updated_at,
            market_status=self._market_status(snapshot_updated_at),
            data_mode=self.settings.data_mode,
            market_cap_min_crore=self.settings.market_cap_min_crore,
            universe_count=len(snapshots),
            scanners=scanners,
            popular_scan_ids=["breakout-ath", "volume-price", "strong-nifty", "clean-pullback", "relative-strength"],
            top_gainers=[
                build_leader_match("gainers", item, item.change_pct, f"{item.change_pct:.2f}% on the day")
                for item in top_gainers
            ],
            top_losers=[
                build_leader_match("losers", item, abs(item.change_pct), f"{item.change_pct:.2f}% on the day")
                for item in top_losers
            ],
            top_volume_spikes=[
                build_leader_match("volume", item, item.relative_volume, f"RVOL {item.relative_volume}x")
                for item in top_volume
            ],
            recent_alerts=alerts,
        )
        self._dashboard_cache = response
        return response

    async def get_scan_counts(self) -> list[ScanDescriptor]:
        snapshots = await self._snapshots()
        scanners, _ = self._scan_catalog(self._scan_eligible_snapshots(snapshots))
        return scanners

    def _gap_up_items(
        self,
        snapshots: list[StockSnapshot],
        *,
        min_gap_pct: float,
        min_liquidity_crore: float | None = None,
    ) -> list[ScanMatch]:
        items: list[ScanMatch] = []

        for snapshot in snapshots:
            if snapshot.gap_pct < min_gap_pct:
                continue
            if min_liquidity_crore is not None and snapshot.avg_rupee_volume_30d_crore < min_liquidity_crore:
                continue

            display_sector = scanner_sector_label(snapshot.sector, snapshot.sub_sector)
            score = round(
                60
                + (snapshot.gap_pct * 8)
                + max(snapshot.change_pct, 0)
                + max(snapshot.relative_volume - 1, 0) * 3,
                2,
            )
            items.append(
                ScanMatch(
                    scan_id="gap-up-openers",
                    symbol=snapshot.symbol,
                    name=snapshot.name,
                    exchange=snapshot.exchange,
                    sector=display_sector,
                    sub_sector=snapshot.sub_sector,
                    market_cap_crore=snapshot.market_cap_crore,
                    last_price=snapshot.last_price,
                    change_pct=snapshot.change_pct,
                    relative_volume=snapshot.relative_volume,
                    avg_rupee_volume_30d_crore=snapshot.avg_rupee_volume_30d_crore,
                    score=score,
                    pattern=f"Gap {snapshot.gap_pct:.2f}%",
                    rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
                    rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
                    rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
                    rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
                    nifty_outperformance=snapshot.nifty_outperformance,
                    sector_outperformance=snapshot.sector_outperformance,
                    three_month_rs=snapshot.three_month_rs,
                    stock_return_20d=snapshot.stock_return_20d,
                    stock_return_60d=snapshot.stock_return_60d,
                    stock_return_12m=snapshot.stock_return_12m,
                    gap_pct=snapshot.gap_pct,
                    reasons=[f"Gap up {snapshot.gap_pct:.2f}%", f"Day change {snapshot.change_pct:.2f}%"],
                )
            )

        return sorted(
            items,
            key=lambda item: (
                item.gap_pct or 0,
                item.change_pct,
                item.relative_volume,
                item.rs_rating or 0,
            ),
            reverse=True,
        )

    def _near_pivot_items(self, snapshots: list[StockSnapshot], request: NearPivotScanRequest) -> list[ScanMatch]:
        items: list[ScanMatch] = []

        for snapshot in snapshots:
            if not snapshot.rs_eligible:
                continue
            if snapshot.rs_rating < request.min_rs_rating:
                continue
            if request.min_liquidity_crore is not None and snapshot.avg_rupee_volume_30d_crore < request.min_liquidity_crore:
                continue
            if snapshot.pct_from_52w_high > request.max_pct_from_52w_high:
                continue

            consolidation_days, consolidation_range_pct, pivot_level = self._near_pivot_consolidation(
                snapshot,
                request.max_consolidation_range_pct,
            )
            if consolidation_days < request.min_consolidation_days:
                continue

            distance_to_pivot_pct = 0.0 if pivot_level <= 0 else round(((pivot_level - snapshot.last_price) / pivot_level) * 100, 2)
            display_sector = scanner_sector_label(snapshot.sector, snapshot.sub_sector)
            score = round(
                60
                + (snapshot.rs_rating * 0.34)
                + max(0.0, 20 - snapshot.pct_from_52w_high) * 0.7
                + max(0, consolidation_days - request.min_consolidation_days) * 1.1
                + max(0.0, request.max_consolidation_range_pct - consolidation_range_pct) * 2.8
                - max(distance_to_pivot_pct, 0) * 0.8,
                2,
            )
            items.append(
                ScanMatch(
                    scan_id="near-pivot",
                    symbol=snapshot.symbol,
                    name=snapshot.name,
                    exchange=snapshot.exchange,
                    sector=display_sector,
                    sub_sector=snapshot.sub_sector,
                    market_cap_crore=snapshot.market_cap_crore,
                    last_price=snapshot.last_price,
                    change_pct=snapshot.change_pct,
                    relative_volume=snapshot.relative_volume,
                    avg_rupee_volume_30d_crore=snapshot.avg_rupee_volume_30d_crore,
                    score=score,
                    pattern="Near Pivot",
                    rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
                    rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
                    rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
                    rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
                    nifty_outperformance=snapshot.nifty_outperformance,
                    sector_outperformance=snapshot.sector_outperformance,
                    three_month_rs=snapshot.three_month_rs,
                    stock_return_20d=snapshot.stock_return_20d,
                    stock_return_60d=snapshot.stock_return_60d,
                    stock_return_12m=snapshot.stock_return_12m,
                    gap_pct=snapshot.gap_pct,
                    reasons=[
                        f"RS Rating {snapshot.rs_rating}",
                        f"{consolidation_days}-day range {consolidation_range_pct:.2f}%",
                        f"{snapshot.pct_from_52w_high:.2f}% below 52W high",
                    ],
                )
            )

        return sorted(
            items,
            key=lambda item: (
                item.score,
                item.rs_rating or 0,
                item.change_pct,
                item.last_price,
            ),
            reverse=True,
        )

    def _pull_back_items(self, snapshots: list[StockSnapshot], request: PullBackScanRequest) -> list[ScanMatch]:
        items: list[ScanMatch] = []

        for snapshot in snapshots:
            if snapshot.sma20 is None or snapshot.sma50 is None or snapshot.ema20 is None or snapshot.ema50 is None:
                continue
            if snapshot.last_price <= snapshot.sma20:
                continue
            if snapshot.sma20 <= snapshot.sma50:
                continue
            if request.min_liquidity_crore is not None and snapshot.avg_rupee_volume_30d_crore < request.min_liquidity_crore:
                continue

            if request.enable_rs_rating and (not snapshot.rs_eligible or snapshot.rs_rating < request.min_rs_rating):
                continue

            first_leg_up_pct = snapshot.stock_return_40d
            if request.enable_first_leg_up and first_leg_up_pct < request.min_first_leg_up_pct:
                continue

            consolidation_days = 0
            consolidation_range_pct = 0.0
            if request.enable_consolidation_range or request.enable_consolidation_days:
                consolidation_days, consolidation_range_pct, _ = self._near_pivot_consolidation(
                    snapshot,
                    request.max_consolidation_range_pct,
                )
                if request.enable_consolidation_days and consolidation_days < request.min_consolidation_days:
                    continue
                if request.enable_consolidation_range and consolidation_range_pct > request.max_consolidation_range_pct:
                    continue

            recent_volumes = snapshot.recent_volumes[-3:]
            volume_vs_avg20 = 0.0
            if request.enable_volume_contraction:
                if len(recent_volumes) < 3 or snapshot.avg_volume_20d <= 0:
                    continue
                volume_vs_avg20 = max(recent_volumes) / snapshot.avg_volume_20d
                if any(volume >= snapshot.avg_volume_20d * request.max_recent_volume_vs_avg20 for volume in recent_volumes):
                    continue

            ema10_value = snapshot.ema10 or snapshot.ema20
            ema20_value = snapshot.ema20
            ema_distances = {
                "ema10": abs(((snapshot.last_price - ema10_value) / ema10_value) * 100) if ema10_value > 0 else 999.0,
                "ema20": abs(((snapshot.last_price - ema20_value) / ema20_value) * 100) if ema20_value > 0 else 999.0,
            }
            if request.pullback_ma == "ema10":
                selected_ma_key = "ema10"
            elif request.pullback_ma == "ema20":
                selected_ma_key = "ema20"
            else:
                selected_ma_key = min(ema_distances, key=ema_distances.get)

            ma_distance_pct = round(float(ema_distances[selected_ma_key]), 2)
            if request.enable_ma_support and ma_distance_pct > request.max_ma_distance_pct:
                continue

            if snapshot.last_price < snapshot.ema50:
                continue
            if snapshot.ema20 <= snapshot.ema50:
                continue
            if snapshot.pullback_depth_pct < 0 or snapshot.pullback_depth_pct > 14:
                continue

            score = round(
                55
                + (snapshot.rs_rating * 0.36)
                + (max(0.0, first_leg_up_pct - request.min_first_leg_up_pct) * 1.4 if request.enable_first_leg_up else 0.0)
                + (max(0, consolidation_days - request.min_consolidation_days) * 1.5 if request.enable_consolidation_days else 0.0)
                + (max(0.0, request.max_consolidation_range_pct - consolidation_range_pct) * 3.2 if request.enable_consolidation_range else 0.0)
                + (max(0.0, request.max_ma_distance_pct - ma_distance_pct) * 4.4 if request.enable_ma_support else 0.0)
                + (max(0.0, request.max_recent_volume_vs_avg20 - volume_vs_avg20) * 4.0 if request.enable_volume_contraction else 0.0),
                2,
            )
            reasons = []
            if request.enable_rs_rating:
                reasons.append(f"RS Rating {snapshot.rs_rating}")
            if request.enable_first_leg_up:
                reasons.append(f"40D price up {first_leg_up_pct:.2f}%")
            if request.enable_consolidation_days or request.enable_consolidation_range:
                reasons.append(f"{consolidation_days}-day consolidation within {consolidation_range_pct:.2f}%")
            if request.enable_volume_contraction:
                reasons.append(f"3-day volume max is {volume_vs_avg20:.2f}x of 20D average")
            if request.enable_ma_support:
                reasons.append(f"Pulling into {selected_ma_key.upper()} within {ma_distance_pct:.2f}%")
            display_sector = scanner_sector_label(snapshot.sector, snapshot.sub_sector)
            items.append(
                ScanMatch(
                    scan_id="pull-backs",
                    symbol=snapshot.symbol,
                    name=snapshot.name,
                    exchange=snapshot.exchange,
                    sector=display_sector,
                    sub_sector=snapshot.sub_sector,
                    market_cap_crore=snapshot.market_cap_crore,
                    last_price=snapshot.last_price,
                    change_pct=snapshot.change_pct,
                    relative_volume=snapshot.relative_volume,
                    avg_rupee_volume_30d_crore=snapshot.avg_rupee_volume_30d_crore,
                    score=score,
                    pattern="Pull Back",
                    rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
                    rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
                    rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
                    rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
                    nifty_outperformance=snapshot.nifty_outperformance,
                    sector_outperformance=snapshot.sector_outperformance,
                    three_month_rs=snapshot.three_month_rs,
                    stock_return_20d=snapshot.stock_return_20d,
                    stock_return_60d=snapshot.stock_return_60d,
                    stock_return_12m=snapshot.stock_return_12m,
                    gap_pct=snapshot.gap_pct,
                    reasons=reasons,
                )
            )

        return sorted(
            items,
            key=lambda item: (
                item.score,
                item.rs_rating or 0,
                item.stock_return_60d or 0,
                item.change_pct,
            ),
            reverse=True,
        )

    async def _benchmark_close_series(self, bars: int = 620) -> pd.Series:
        benchmark_symbol_getter = getattr(self.provider, "_benchmark_symbol", None)
        benchmark_symbol = benchmark_symbol_getter() if callable(benchmark_symbol_getter) else "^NSEI"
        try:
            benchmark_bars = await self.provider.get_chart(benchmark_symbol, "1D", bars=bars)
        except Exception:
            return pd.Series(dtype=float)

        filtered = [bar for bar in benchmark_bars if getattr(bar, "close", None) not in (None, 0)]
        if not filtered:
            return pd.Series(dtype=float)
        return pd.Series(
            [float(bar.close) for bar in filtered],
            index=pd.DatetimeIndex([pd.Timestamp(datetime.fromtimestamp(int(bar.time), tz=timezone.utc)) for bar in filtered]),
            dtype=float,
        ).sort_index()

    async def _historical_sector_snapshots(
        self,
        snapshots: list[StockSnapshot],
        sectors: set[str],
        offset_bars: int,
    ) -> list[StockSnapshot]:
        history_reader = getattr(self.provider, "_history_frame_from_cached_bars", None)
        history_builder = getattr(self.provider, "_history_to_snapshot", None)
        if not callable(history_reader) or not callable(history_builder):
            return []

        benchmark_close = await self._benchmark_close_series(max(620, 520 + offset_bars))
        historical_snapshots: list[StockSnapshot] = []

        for snapshot in snapshots:
            if snapshot.sector not in sectors:
                continue
            history = history_reader(snapshot.symbol, max(620, 520 + offset_bars), allow_legacy=True)
            if history is None or history.empty:
                continue
            if offset_bars > 0:
                if len(history) <= offset_bars:
                    continue
                history = history.iloc[:-offset_bars]
            if history.empty or len(history) < 30:
                continue

            benchmark_history = benchmark_close[benchmark_close.index <= history.index[-1]] if not benchmark_close.empty else benchmark_close
            instrument = {
                "symbol": snapshot.symbol,
                "name": snapshot.name,
                "exchange": snapshot.exchange,
                "listing_date": snapshot.listing_date.isoformat() if snapshot.listing_date else None,
                "sector": snapshot.sector,
                "sub_sector": snapshot.sub_sector,
                "market_cap_crore": snapshot.market_cap_crore,
                "ticker": snapshot.instrument_key or f"{snapshot.symbol}.NS",
            }
            row = history_builder(instrument, history, benchmark_history)
            if row is None:
                continue
            historical_snapshots.append(StockSnapshot.model_validate(row))

        return historical_snapshots

    async def _build_scan_sector_summaries(
        self,
        *,
        scan_key: str,
        request_signature: str,
        snapshots: list[StockSnapshot],
        items: list[ScanMatch],
        historical_runner,
    ) -> list[ScanSectorSummary]:
        if not items:
            return []

        sectors = {item.sector for item in items if item.sector}
        snapshot_updated_at = self._snapshot_updated_at()
        cache_key = (
            scan_key,
            json.dumps({"signature": request_signature, "sectors": sorted(sectors)}, sort_keys=True),
            snapshot_updated_at,
        )
        cached = self._scan_sector_summary_cache.get(cache_key)
        if cached is not None:
            return cached

        current_counts: dict[str, int] = {}
        for item in items:
            current_counts[item.sector] = current_counts.get(item.sector, 0) + 1

        sector_universe: dict[str, list[StockSnapshot]] = {}
        for snapshot in snapshots:
            if snapshot.sector in sectors:
                sector_universe.setdefault(snapshot.sector, []).append(snapshot)

        historical_1w_snapshots = await self._historical_sector_snapshots(snapshots, sectors, 5)
        historical_1m_snapshots = await self._historical_sector_snapshots(snapshots, sectors, 20)
        historical_1w_items = historical_runner(historical_1w_snapshots) if historical_1w_snapshots else []
        historical_1m_items = historical_runner(historical_1m_snapshots) if historical_1m_snapshots else []

        historical_1w_counts: dict[str, int] = {}
        for item in historical_1w_items:
            historical_1w_counts[item.sector] = historical_1w_counts.get(item.sector, 0) + 1

        historical_1m_counts: dict[str, int] = {}
        for item in historical_1m_items:
            historical_1m_counts[item.sector] = historical_1m_counts.get(item.sector, 0) + 1

        summaries = [
            ScanSectorSummary(
                sector=sector,
                current_hits=current_counts.get(sector, 0),
                prior_week_hits=historical_1w_counts.get(sector, 0),
                prior_month_hits=historical_1m_counts.get(sector, 0),
                sector_return_1w=self._weighted_average_return(sector_universe.get(sector, []), "stock_return_5d"),
                sector_return_1m=self._weighted_average_return(sector_universe.get(sector, []), "stock_return_20d"),
            )
            for sector in current_counts
        ]
        self._scan_sector_summary_cache[cache_key] = summaries
        return summaries

    async def _scan_results_response(
        self,
        *,
        scan: dict[str, object] | ScanDescriptor,
        scan_key: str,
        request_signature: str,
        snapshots: list[StockSnapshot],
        items: list[ScanMatch],
        historical_runner,
        include_sector_summaries: bool,
    ) -> ScanResultsResponse:
        sector_summaries = (
            await self._build_scan_sector_summaries(
                scan_key=scan_key,
                request_signature=request_signature,
                snapshots=snapshots,
                items=items,
                historical_runner=historical_runner,
            )
            if include_sector_summaries
            else []
        )
        return ScanResultsResponse(
            scan=scan,
            generated_at=self._snapshot_updated_at(),
            market_cap_min_crore=self.settings.market_cap_min_crore,
            total_hits=len(items),
            items=items,
            sector_summaries=sector_summaries,
        )

    @staticmethod
    def _filter_scan_items_by_liquidity(items: list[ScanMatch], min_liquidity_crore: float | None) -> list[ScanMatch]:
        if min_liquidity_crore is None:
            return items
        return [item for item in items if float(item.avg_rupee_volume_30d_crore or 0.0) >= min_liquidity_crore]

    async def get_scan_results(
        self,
        scan_id: str,
        include_sector_summaries: bool = True,
        min_liquidity_crore: float | None = None,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        scanners, results = self._scan_catalog(snapshots)
        descriptor = next((scan for scan in scanners if scan.id == scan_id), None)
        if descriptor is None:
            raise KeyError(scan_id)

        items = self._filter_scan_items_by_liquidity(results[scan_id], min_liquidity_crore)
        descriptor = descriptor.model_copy(update={"hit_count": len(items)})
        request_signature = scan_id if min_liquidity_crore is None else f"{scan_id}:{min_liquidity_crore:.2f}"
        return await self._scan_results_response(
            scan=descriptor,
            scan_key=scan_id,
            request_signature=request_signature,
            snapshots=snapshots,
            items=items,
            historical_runner=lambda historical_snapshots: self._filter_scan_items_by_liquidity(
                scan_catalog_with_counts(historical_snapshots)[1].get(scan_id, []),
                min_liquidity_crore,
            ),
            include_sector_summaries=include_sector_summaries,
        )

    async def get_custom_scan_results(
        self,
        request: CustomScanRequest,
        include_sector_summaries: bool = True,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        items = run_custom_scan(request, snapshots)
        return await self._scan_results_response(
            scan={
                "id": "custom-scan",
                "name": "Custom Scanner",
                "category": "Custom",
                "description": "Build a scan with price, volume, relative strength, and pattern filters.",
                "hit_count": len(items),
            },
            scan_key="custom-scan",
            request_signature=json.dumps(request.model_dump(mode="python"), sort_keys=True, default=str),
            snapshots=snapshots,
            items=items,
            historical_runner=lambda historical_snapshots: run_custom_scan(request, historical_snapshots),
            include_sector_summaries=include_sector_summaries,
        )

    async def get_chart(self, symbol: str, timeframe: str):
        snapshot_updated_at = self._snapshot_updated_at()
        cache_key = (symbol, timeframe)
        cached = self._chart_response_cache.get(cache_key)
        now = datetime.now(timezone.utc)
        if cached is not None:
            cached_at, cached_snapshot_at, cached_response = cached
            if cached_snapshot_at >= snapshot_updated_at and now - cached_at <= self._chart_response_cache_ttl(timeframe):
                return cached_response

        snapshots, bars = await asyncio.gather(
            self._snapshots(),
            self.provider.get_chart(symbol, timeframe, bars=self._chart_bar_limit(timeframe)),
        )
        snapshot = next((item for item in snapshots if item.symbol == symbol), None)
        rs_line, rs_line_markers = await self._build_rs_line(symbol=symbol, timeframe=timeframe, bars=bars, snapshots=snapshots)
        summary = build_stock_overview(snapshot) if snapshot else None
        if summary is not None:
            summary = self._with_chart_summary(summary, bars, rs_line, snapshot.previous_close if snapshot else None)
        response = ChartResponse(
            symbol=symbol,
            timeframe=timeframe,
            bars=bars,
            summary=summary,
            rs_line=rs_line,
            rs_line_markers=rs_line_markers,
        )
        self._chart_response_cache[cache_key] = (now, snapshot_updated_at, response)
        return response

    async def get_chart_history(self, symbol: str, timeframe: str):
        snapshots = await self._snapshots()
        snapshot = next((item for item in snapshots if item.symbol == symbol), None)

        bars = await self.get_chart_full_history(symbol=symbol, timeframe=timeframe)
        rs_line, rs_line_markers = await self._build_rs_line(symbol=symbol, timeframe=timeframe, bars=bars, snapshots=snapshots)

        summary = build_stock_overview(snapshot) if snapshot else None
        if summary is not None:
            summary = self._with_chart_summary(summary, bars, rs_line, snapshot.previous_close if snapshot else None)

        return ChartResponse(
            symbol=symbol,
            timeframe=timeframe,
            bars=bars,
            summary=summary,
            rs_line=rs_line,
            rs_line_markers=rs_line_markers,
        )

    async def get_chart_full_history(self, symbol: str, timeframe: str):
        bar_limit = {"1D": 5000, "1W": 1040, "1h": 2500, "30m": 1200, "15m": 800}.get(timeframe, 5000)
        try:
            # Prefer uncached fetch for explicit full-history requests.
            return await asyncio.to_thread(self.provider._fetch_chart_bars, symbol, timeframe, bar_limit)
        except Exception:
            try:
                # If live fetch fails (e.g., transient provider/rate-limit issue), fall back to provider cache path.
                return await self.provider.get_chart(symbol, timeframe, bar_limit)
            except Exception:
                return []

    async def get_market_overview(self):
        now = time.time()
        cached = getattr(self, "_market_overview_cache", None)
        if cached and now - cached[0] < 300:
            return cached[1]
        items_raw = await asyncio.to_thread(self.provider.get_market_overview)
        from app.models.market import MarketMacroItem, MarketOverviewResponse
        items = [MarketMacroItem(**item) for item in items_raw]
        result = MarketOverviewResponse(items=items)
        self._market_overview_cache = (now, result)
        return result

    async def get_index_pe_history(self, symbol: str) -> IndexPeHistoryResponse:
        """Fetch historical P/E for an index and compute 5-year average."""
        _PE_LABELS = {
            "^NSEI": "Nifty 50",
            "^CNXSC": "Nifty SmallCap 250",
            "^NSEMDCP50": "Nifty Midcap 50",
        }
        label = _PE_LABELS.get(symbol.upper(), symbol)
        source = "nse"
        raw = await asyncio.to_thread(self.provider.get_index_pe_history, symbol)
        points = [IndexPePoint(date=r["date"], pe=r["pe"]) for r in raw]

        avg_5y: float | None = None
        if points:
            pe_values = [p.pe for p in points]
            avg_5y = round(sum(pe_values) / len(pe_values), 2)

        # current P/E from market overview cache if available
        current_pe: float | None = None
        try:
            overview = await self.get_market_overview()
            for item in overview.items:
                if item.symbol.upper() == symbol.upper():
                    current_pe = item.trailing_pe
                    break
        except Exception:
            pass

        # Fallback: if NSE PE history is unavailable, create a proxy PE curve from index price history.
        # Proxy assumption: PE scales proportionally with index price around current PE.
        if not points and current_pe and current_pe > 0:
            try:
                chart_bars = await self.provider.get_chart(symbol=symbol.upper(), timeframe="1D", bars=1300)
                bars = chart_bars[-1260:] if chart_bars else []
                if bars and bars[-1].close > 0:
                    source = "proxy"
                    last_close = bars[-1].close
                    step = max(len(bars) // 320, 1)
                    proxy_points: list[IndexPePoint] = []
                    for index, bar in enumerate(bars):
                        if index % step != 0 and index != len(bars) - 1:
                            continue
                        proxy_pe = round(float(current_pe) * (float(bar.close) / float(last_close)), 2)
                        if proxy_pe <= 0:
                            continue
                        proxy_points.append(
                            IndexPePoint(
                                date=datetime.fromtimestamp(bar.time, tz=timezone.utc).date().isoformat(),
                                pe=proxy_pe,
                            )
                        )
                    points = proxy_points
                    if points:
                        pe_values = [p.pe for p in points]
                        avg_5y = round(sum(pe_values) / len(pe_values), 2)
            except Exception:
                pass

        return IndexPeHistoryResponse(
            symbol=symbol,
            label=label,
            points=points,
            avg_5y=avg_5y,
            current_pe=current_pe or (points[-1].pe if points else None),
            source=source,
        )

    # ── Money Flow ──────────────────────────────────────────────────────────

    def _load_money_flow_reports(self) -> dict[str, dict]:
        if not self._money_flow_cache_path.exists():
            return {}
        try:
            return json.loads(self._money_flow_cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_money_flow_reports(self, reports: dict[str, dict]) -> None:
        try:
            self._money_flow_cache_path.write_text(
                json.dumps(reports, indent=2, default=str), encoding="utf-8"
            )
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("Failed to save money flow reports: %s", exc)

    @staticmethod
    def _week_key(dt: datetime) -> tuple[str, str]:
        """Return (week_key like '2026-W13', week_start ISO date)."""
        iso = dt.isocalendar()
        week_start = dt - timedelta(days=dt.weekday())
        return f"{iso.year}-W{iso.week:02d}", week_start.strftime("%Y-%m-%d")

    async def generate_and_store_money_flow(self, reference_time: datetime | None = None) -> MoneyFlowReport:
        """AI-generate the weekly money flow report and persist it."""
        now_local = reference_time or self._money_flow_now()
        week_key, week_start = self._week_key(now_local)

        # Build sector summary for the prompt
        sector_lines: list[str] = []
        try:
            sector_tab = await self.get_sector_tab("1W", "desc")
            for card in sector_tab.sectors[:30]:
                sector_lines.append(
                    f"{card.sector}: 1D={card.return_1d:+.1f}%, 1W={card.return_1w:+.1f}%, "
                    f"1M={card.return_1m:+.1f}%, 3M={card.return_3m:+.1f}%, {card.company_count} stocks"
                )
        except Exception:
            sector_lines = ["Sector data unavailable"]

        sector_data = "\n".join(sector_lines)

        # Generate via AI
        ai_service = getattr(self.provider, "ai_service", None)
        raw: dict[str, object] | None = None
        if ai_service is not None and ai_service.available:
            try:
                raw = await asyncio.to_thread(ai_service.generate_money_flow_report, sector_data, week_key)
            except Exception:
                raw = None

        def _parse_sectors(lst: list) -> list[MoneyFlowSector]:
            result = []
            for item in lst or []:
                try:
                    result.append(MoneyFlowSector(**item))
                except Exception:
                    pass
            return result

        if raw is None:
            report = self._build_fallback_money_flow_report(week_key, week_start, sector_tab)
        else:
            report = MoneyFlowReport(
                week_key=week_key,
                week_start=week_start,
                generated_at=datetime.now(timezone.utc),
                inflows=_parse_sectors(raw.get("inflows", [])),
                outflows=_parse_sectors(raw.get("outflows", [])),
                sector_performance=_parse_sectors(raw.get("sector_performance", [])),
                short_term_headwinds=_parse_sectors(raw.get("short_term_headwinds", [])),
                short_term_tailwinds=_parse_sectors(raw.get("short_term_tailwinds", [])),
                long_term_tailwinds=_parse_sectors(raw.get("long_term_tailwinds", [])),
                macro_summary=raw.get("macro_summary", ""),
                ai_model="gemini",
            )

        # Persist
        reports = await asyncio.to_thread(self._load_money_flow_reports)
        reports[week_key] = report.model_dump(mode="json")
        await asyncio.to_thread(self._save_money_flow_reports, reports)
        return report

    @staticmethod
    def _money_flow_magnitude(value: float) -> str:
        absolute_value = abs(value)
        if absolute_value >= 8:
            return "strong"
        if absolute_value >= 3:
            return "moderate"
        return "mild"

    @classmethod
    def _money_flow_sector_reason(cls, card: SectorCard, focus: str) -> str:
        if focus == "momentum":
            return (
                f"{card.sector} led with {card.return_1w:+.1f}% over 1W and {card.return_1m:+.1f}% over 1M "
                f"across {card.company_count} stocks."
            )
        if focus == "pressure":
            return (
                f"{card.sector} lagged with {card.return_1w:+.1f}% over 1W and {card.return_1m:+.1f}% over 1M, "
                f"showing broad pressure across {card.company_count} stocks."
            )
        if focus == "trend":
            return (
                f"{card.sector} is sustaining {card.return_3m:+.1f}% over 3M with {card.return_1m:+.1f}% over 1M, "
                f"keeping the intermediate trend constructive."
            )
        return (
            f"{card.sector} moved {card.return_1d:+.1f}% on the day, {card.return_1w:+.1f}% over 1W, and "
            f"{card.return_1m:+.1f}% over 1M."
        )

    @classmethod
    def _money_flow_sector_entry(cls, card: SectorCard, sentiment: str, focus: str) -> MoneyFlowSector:
        metric = card.return_3m if focus == "trend" else card.return_1w if focus in {"momentum", "pressure"} else card.return_1d
        return MoneyFlowSector(
            name=card.sector,
            sentiment=sentiment,
            reason=cls._money_flow_sector_reason(card, focus),
            magnitude=cls._money_flow_magnitude(metric),
        )

    @classmethod
    def _build_fallback_money_flow_report(
        cls,
        week_key: str,
        week_start: str,
        sector_tab: SectorTabResponse,
    ) -> MoneyFlowReport:
        sectors = [card for card in sector_tab.sectors if card.group_kind == "sector"]
        if not sectors:
            return MoneyFlowReport(
                week_key=week_key,
                week_start=week_start,
                generated_at=datetime.now(timezone.utc),
                inflows=[],
                outflows=[],
                sector_performance=[],
                short_term_headwinds=[],
                short_term_tailwinds=[],
                long_term_tailwinds=[],
                macro_summary="Sector data was unavailable during the scheduled run, so the weekly report could not rank sector flows.",
                ai_model="fallback-rules",
            )

        by_week = sorted(sectors, key=lambda card: (card.return_1w, card.return_1m, card.company_count), reverse=True)
        by_day = sorted(sectors, key=lambda card: (card.return_1d, card.return_1w, card.company_count), reverse=True)
        by_trend = sorted(sectors, key=lambda card: (card.return_3m, card.return_1m, card.return_1w), reverse=True)
        positives_1w = sum(1 for card in sectors if card.return_1w > 0)
        avg_1w = sum(card.return_1w for card in sectors) / len(sectors)
        leaders = ", ".join(card.sector for card in by_week[:3]) or "none"
        laggards = ", ".join(card.sector for card in reversed(by_week[-3:])) or "none"

        return MoneyFlowReport(
            week_key=week_key,
            week_start=week_start,
            generated_at=datetime.now(timezone.utc),
            inflows=[cls._money_flow_sector_entry(card, "bullish", "momentum") for card in by_week[:5]],
            outflows=[cls._money_flow_sector_entry(card, "bearish", "pressure") for card in list(reversed(by_week[-5:]))],
            sector_performance=[cls._money_flow_sector_entry(card, "bullish" if card.return_1w >= 0 else "bearish", "momentum") for card in by_week[:5]],
            short_term_headwinds=[cls._money_flow_sector_entry(card, "bearish", "pressure") for card in list(reversed(by_day[-3:]))],
            short_term_tailwinds=[cls._money_flow_sector_entry(card, "bullish", "daily") for card in by_day[:3]],
            long_term_tailwinds=[cls._money_flow_sector_entry(card, "bullish", "trend") for card in by_trend[:3]],
            macro_summary=(
                f"Rule-based fallback summary: {positives_1w}/{len(sectors)} sectors closed positive over 1W, with an average 1W move of "
                f"{avg_1w:+.1f}%. Leaders were {leaders}; laggards were {laggards}."
            ),
            ai_model="fallback-rules",
        )

    async def get_money_flow_history(self) -> MoneyFlowHistoryResponse:
        """Return all stored money-flow reports, newest first."""
        raw = await asyncio.to_thread(self._load_money_flow_reports)
        reports: list[MoneyFlowReport] = []
        for week_key in sorted(raw.keys(), reverse=True):
            try:
                reports.append(MoneyFlowReport(**raw[week_key]))
            except Exception:
                pass
        latest = reports[0].week_key if reports else None
        return MoneyFlowHistoryResponse(reports=reports, latest_week_key=latest)

    async def get_money_flow_latest(self) -> MoneyFlowReport | None:
        """Return the most recent report, or None if none exists yet."""
        history = await self.get_money_flow_history()
        return history.reports[0] if history.reports else None

    def _load_money_flow_stock_payloads(self) -> dict[str, dict]:
        if not self._money_flow_stock_cache_path.exists():
            return {}
        try:
            return json.loads(self._money_flow_stock_cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_money_flow_stock_payloads(self, payloads: dict[str, dict]) -> None:
        try:
            self._money_flow_stock_cache_path.write_text(
                json.dumps(payloads, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("Failed to save money flow stock ideas: %s", exc)

    @staticmethod
    def _previous_trading_day(date_value):
        current = date_value
        current -= timedelta(days=1)
        while current.weekday() >= 5:
            current -= timedelta(days=1)
        return current

    @staticmethod
    def _current_or_previous_trading_day(date_value):
        current = date_value
        while current.weekday() >= 5:
            current -= timedelta(days=1)
        return current

    def _money_flow_timezone(self) -> timezone | ZoneInfo:
        return IST

    def _money_flow_now(self) -> datetime:
        return datetime.now(self._money_flow_timezone())

    def _money_flow_stock_cutoff(self) -> tuple[int, int]:
        return 18, 0

    def _money_flow_stock_theme_priority(self, snapshot: StockSnapshot, recommendation_type: str) -> int:
        return 0

    def _money_flow_value_fundamentals_limit(self) -> int:
        return 24

    def _money_flow_max_per_sector(self) -> int:
        return 1

    def _money_flow_weekly_release_time(self) -> tuple[int, int]:
        return 9, 0

    def _money_flow_target_week(self, now_local: datetime | None = None) -> tuple[str, str, datetime]:
        current_local = now_local or self._money_flow_now()
        release_hour, release_minute = self._money_flow_weekly_release_time()
        days_since_saturday = (current_local.weekday() - 5) % 7
        scheduled_date = current_local.date() - timedelta(days=days_since_saturday)
        scheduled_run = datetime.combine(scheduled_date, datetime.min.time(), tzinfo=self._money_flow_timezone()).replace(
            hour=release_hour,
            minute=release_minute,
        )
        if current_local < scheduled_run:
            scheduled_run -= timedelta(days=7)

        week_key, week_start = self._week_key(scheduled_run)
        return week_key, week_start, scheduled_run

    def _money_flow_stock_recommendation_date(self, now_local: datetime, force_today: bool = False) -> str:
        date_value = now_local.date()
        if force_today:
            return self._current_or_previous_trading_day(date_value).isoformat()
        if now_local.weekday() >= 5:
            return self._current_or_previous_trading_day(date_value).isoformat()
        cutoff_hour, cutoff_minute = self._money_flow_stock_cutoff()
        cutoff = now_local.replace(hour=cutoff_hour, minute=cutoff_minute, second=0, microsecond=0)
        if now_local < cutoff:
            date_value -= timedelta(days=1)
        return self._current_or_previous_trading_day(date_value).isoformat()

    def _money_flow_stock_next_update(self, now_local: datetime) -> datetime:
        cutoff_hour, cutoff_minute = self._money_flow_stock_cutoff()
        next_update = now_local.replace(hour=cutoff_hour, minute=cutoff_minute, second=0, microsecond=0)
        if now_local >= next_update:
            next_update += timedelta(days=1)
        while next_update.weekday() >= 5:
            next_update += timedelta(days=1)
        return next_update.astimezone(timezone.utc)

    async def ensure_money_flow_report_current(self) -> MoneyFlowReport | None:
        target_week_key, _, target_run = self._money_flow_target_week()
        reports = await asyncio.to_thread(self._load_money_flow_reports)
        payload = reports.get(target_week_key)
        if isinstance(payload, dict):
            try:
                return MoneyFlowReport(**payload)
            except Exception:
                pass

        try:
            return await self.generate_and_store_money_flow(reference_time=target_run)
        except Exception:
            return await self.get_money_flow_latest()

    async def ensure_money_flow_stock_ideas_current(self) -> MoneyFlowStockIdeasResponse | None:
        now_local = self._money_flow_now()
        recommendation_date = self._money_flow_stock_recommendation_date(now_local)
        payloads = await asyncio.to_thread(self._load_money_flow_stock_payloads)
        payload = payloads.get(recommendation_date)
        if isinstance(payload, dict):
            try:
                return MoneyFlowStockIdeasResponse(**payload)
            except Exception:
                pass

        try:
            return await self.generate_and_store_money_flow_stock_ideas(reference_time=now_local)
        except Exception:
            history = await self.get_money_flow_stock_ideas_history()
            return history.reports[0] if history.reports else None

    @staticmethod
    def _parse_any_datetime(value: str | None) -> datetime | None:
        cleaned = str(value or "").strip()
        if not cleaned:
            return None
        normalized = cleaned.replace("Z", "+00:00")
        for candidate in (normalized, cleaned):
            try:
                parsed = datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue
        for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _date_label(value: str | None) -> str:
        parsed = DashboardService._parse_any_datetime(value)
        if parsed is None:
            return "Recent"
        return parsed.astimezone(IST).strftime("%d %b %Y")

    @staticmethod
    def _normalize_text(value: str | None) -> str:
        return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()

    @classmethod
    def _dedupe_fingerprint(cls, text: str) -> str:
        normalized = cls._normalize_text(text).lower()
        normalized = re.sub(r"\b\d{1,2}\s+[a-z]{3,9}\s+\d{4}\b", " ", normalized)
        normalized = re.sub(r"\d+", " ", normalized)
        normalized = re.sub(r"[^a-z% ]+", " ", normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    @classmethod
    def _is_generic_compliance_update(cls, text: str) -> bool:
        normalized = cls._normalize_text(text).lower()
        if not normalized:
            return False
        if any(keyword in normalized for keyword in MATERIAL_IMPACT_KEYWORDS):
            return False
        return any(keyword in normalized for keyword in GENERIC_COMPLIANCE_KEYWORDS)

    @classmethod
    def _is_material_financial_update(cls, text: str) -> bool:
        normalized = cls._normalize_text(text).lower()
        if not normalized:
            return False
        return any(keyword in normalized for keyword in MATERIAL_IMPACT_KEYWORDS)

    @classmethod
    def _is_results_update(cls, text: str) -> bool:
        normalized = cls._normalize_text(text).lower()
        if not normalized:
            return False
        if any(marker in normalized for marker in ("transcript", "concall", "conference call", "earnings call")):
            return False
        return any(keyword in normalized for keyword in RESULTS_UPDATE_KEYWORDS)

    @classmethod
    def _detail_lines_from_news(cls, item: DetailedNews, max_points: int = 2) -> list[str]:
        combined = cls._normalize_text(f"{item.title}. {item.summary}")
        if cls._is_results_update(combined):
            return []

        detail_points = [
            cls._normalize_text(point)
            for point in item.detailed_points
            if cls._normalize_text(point)
        ]
        filtered_points = [
            point
            for point in detail_points
            if not cls._is_generic_compliance_update(point)
            and not cls._is_results_update(point)
            and (cls._is_material_financial_update(point) or item.relevance_score >= 0.82)
        ]
        if filtered_points:
            return filtered_points[:max_points]

        summary = cls._format_update_summary(item.title, item.summary)
        if summary and not cls._is_results_update(summary):
            return [summary]
        return []

    @classmethod
    def _append_unique_line(cls, lines: list[str], extra: str | None, limit: int) -> list[str]:
        ordered = [*lines, extra] if extra else list(lines)
        result: list[str] = []
        seen: set[str] = set()
        for line in ordered:
            normalized = cls._normalize_text(line)
            if not normalized:
                continue
            fingerprint = cls._dedupe_fingerprint(normalized)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            result.append(normalized)
            if len(result) >= limit:
                break
        return result

    @classmethod
    def _format_update_summary(cls, title: str, summary: str | None = None) -> str:
        primary = cls._normalize_text(summary or "")
        fallback = cls._normalize_text(title)
        text = primary or fallback
        text = re.sub(r"^(announcement|disclosure|intimation)\s*[:\-]\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bAnnouncement Under Regulation 30\b(?:\s*of\s*SEBI\s*\(LODR\)\s*Regulations,?\s*2015)?\s*[:\-]?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^\(?lodr\)?\s*[-:–]?\s*[a-z_ /]+\s*", "", text, flags=re.IGNORECASE)
        text = cls._normalize_text(text) or fallback
        if len(text) <= 280:
            return text
        sentence = re.split(r"(?<=[.!?])\s+", text)[0].strip()
        return sentence if sentence else text[:277].rstrip() + "..."

    @classmethod
    def _latest_management_commentary(cls, fundamentals: CompanyFundamentals, limit: int = 3) -> list[str]:
        guidance_items = sorted(
            fundamentals.management_guidance,
            key=lambda item: cls._parse_any_datetime(item.guidance_date or "") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        items: list[str] = []
        seen: set[str] = set()
        for guidance in guidance_items:
            source = cls._normalize_text(guidance.guidance_source or guidance.fiscal_year)
            date_label = cls._date_label(guidance.guidance_date)
            points = [
                cls._normalize_text(point)
                for point in guidance.key_guidance_points
                if cls._normalize_text(point)
            ]
            filtered_points = [
                point for point in points if any(keyword in point.lower() for keyword in MANAGEMENT_COMMENTARY_KEYWORDS)
            ]
            for point in filtered_points:
                entry = f"{date_label} · {source}: {point}"
                key = entry.lower()
                if key in seen:
                    continue
                seen.add(key)
                items.append(entry)
                if len(items) >= limit:
                    return items
        return items

    @classmethod
    def _extract_recent_developments(cls, fundamentals: CompanyFundamentals) -> list[str]:
        ranked: list[tuple[datetime | None, str, str]] = []
        for item in fundamentals.detailed_news:
            combined = cls._normalize_text(f"{item.title}. {item.summary}")
            if cls._is_generic_compliance_update(combined):
                continue
            if cls._is_results_update(combined):
                continue
            if item.impact_category.lower() == "regulatory" and not cls._is_material_financial_update(combined):
                continue
            if not cls._is_material_financial_update(combined) and item.relevance_score < 0.7:
                continue
            for detail in cls._detail_lines_from_news(item, max_points=2):
                ranked.append(
                    (
                        cls._parse_any_datetime(item.published_date),
                        f"{combined.lower()}::{detail.lower()}",
                        f"{cls._date_label(item.published_date)} · {item.source}: {detail}",
                    )
                )

        for update in fundamentals.recent_updates:
            combined = cls._normalize_text(f"{update.title}. {update.summary or ''}")
            if cls._is_generic_compliance_update(combined):
                continue
            if update.kind == "results" or cls._is_results_update(combined):
                continue
            if not cls._normalize_text(update.summary) and cls._is_generic_compliance_update(update.title):
                continue
            if not cls._normalize_text(update.summary) and any(token in update.title.lower() for token in ("lodr", "regulation 30")):
                continue
            if update.kind == "filing" and not cls._normalize_text(update.summary):
                continue
            if update.kind in {"filing", "concall"} and not cls._is_material_financial_update(combined):
                continue
            ranked.append(
                (
                    cls._parse_any_datetime(update.published_at),
                    combined.lower(),
                    f"{cls._date_label(update.published_at)} · {update.source}: {cls._format_update_summary(update.title, update.summary)}",
                )
            )

        ranked.sort(key=lambda item: item[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        deduped: list[str] = []
        seen: set[str] = set()
        for _, key, text in ranked:
            fingerprint = cls._dedupe_fingerprint(text or key)
            if key in seen or fingerprint in seen:
                continue
            seen.add(key)
            seen.add(fingerprint)
            deduped.append(text)
            if len(deduped) >= 5:
                break
        return deduped

    @classmethod
    def _extract_growth_driver_lines(cls, fundamentals: CompanyFundamentals) -> list[str]:
        drivers: list[str] = []
        seen: set[str] = set()

        for item in cls._latest_management_commentary(fundamentals, limit=3):
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            drivers.append(item)

        detailed_news = sorted(
            fundamentals.detailed_news,
            key=lambda item: cls._parse_any_datetime(item.published_date) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        for item in detailed_news:
            combined = cls._normalize_text(f"{item.title}. {item.summary}")
            if cls._is_results_update(combined):
                continue
            if not cls._is_material_financial_update(combined):
                continue
            prefix = "Positive trigger" if item.sentiment.lower() == "positive" else "Negative watch" if item.sentiment.lower() == "negative" else "Monitor"
            for detail in cls._detail_lines_from_news(item, max_points=2):
                text = f"{prefix} · {cls._date_label(item.published_date)} · {item.source}: {detail}"
                key = text.lower()
                if key in seen:
                    continue
                seen.add(key)
                drivers.append(text)
                if len(drivers) >= 5:
                    return drivers

        for item in fundamentals.growth_drivers:
            detail = cls._normalize_text(f"{item.title}: {item.detail}")
            if not cls._is_material_financial_update(detail):
                continue
            key = detail.lower()
            if key in seen:
                continue
            seen.add(key)
            drivers.append(detail)
            if len(drivers) >= 5:
                return drivers

        growth = fundamentals.growth
        if growth is not None:
            quarter_line = []
            if growth.sales_yoy_pct is not None:
                quarter_line.append(f"sales YoY {cls._format_pct(growth.sales_yoy_pct)}")
            if growth.profit_yoy_pct is not None:
                quarter_line.append(f"profit YoY {cls._format_pct(growth.profit_yoy_pct)}")
            if growth.operating_margin_latest_pct is not None:
                quarter_line.append(f"operating margin {cls._format_number(growth.operating_margin_latest_pct, 1)}%")
            if quarter_line:
                drivers.append(f"Latest reported quarter: {', '.join(quarter_line)}.")
        return drivers[:5]

    @classmethod
    def _extract_risk_flags(cls, fundamentals: CompanyFundamentals) -> list[str]:
        flags: list[str] = []
        seen: set[str] = set()
        for item in sorted(
            fundamentals.detailed_news,
            key=lambda news: cls._parse_any_datetime(news.published_date) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        ):
            combined = cls._normalize_text(f"{item.title}. {item.summary}")
            if item.sentiment.lower() != "negative" or not cls._is_material_financial_update(combined):
                continue
            text = f"{cls._date_label(item.published_date)} · {item.source}: {cls._format_update_summary(item.title, item.summary)}"
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            flags.append(text)
            if len(flags) >= 2:
                break

        for item in fundamentals.risks_and_opportunities[:4]:
            text = cls._normalize_text(item.description)
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            flags.append(text)
            if len(flags) >= 4:
                break
        return flags[:4]

    @classmethod
    def _future_growth_summary(cls, fundamentals: CompanyFundamentals) -> str:
        commentary = cls._latest_management_commentary(fundamentals, limit=2)
        if commentary:
            return " ".join(commentary)

        for item in sorted(
            fundamentals.detailed_news,
            key=lambda news: cls._parse_any_datetime(news.published_date) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        ):
            combined = cls._normalize_text(f"{item.title}. {item.summary}")
            if cls._is_results_update(combined):
                continue
            if cls._is_material_financial_update(combined):
                detail = cls._detail_lines_from_news(item, max_points=1)
                if detail:
                    return f"{cls._date_label(item.published_date)} · {item.source}: {detail[0]}"

        for update in sorted(
            fundamentals.recent_updates,
            key=lambda item: cls._parse_any_datetime(item.published_at) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        ):
            combined = cls._normalize_text(f"{update.title}. {update.summary or ''}")
            if update.kind == "results" or cls._is_results_update(combined):
                continue
            if update.kind == "filing" and not cls._normalize_text(update.summary):
                continue
            if cls._is_material_financial_update(combined) and not cls._is_generic_compliance_update(combined):
                return f"{cls._date_label(update.published_at)} · {update.source}: {cls._format_update_summary(update.title, update.summary)}"

        growth = fundamentals.growth
        if growth is not None:
            pieces: list[str] = []
            if growth.sales_yoy_pct is not None:
                pieces.append(f"sales YoY {cls._format_pct(growth.sales_yoy_pct)}")
            if growth.profit_yoy_pct is not None:
                pieces.append(f"profit YoY {cls._format_pct(growth.profit_yoy_pct)}")
            if growth.operating_margin_latest_pct is not None:
                pieces.append(f"operating margin {cls._format_number(growth.operating_margin_latest_pct, 1)}%")
            if pieces:
                return "Fresh management or news commentary is limited; the latest reported quarter showed " + ", ".join(pieces) + "."

        ai_summary = fundamentals.ai_news_summary.summary if fundamentals.ai_news_summary else None
        return cls._clean_sentence(
            fundamentals.strategy_and_outlook or ai_summary or fundamentals.business_summary or fundamentals.about,
            "Future growth commentary is limited in the current dataset.",
        )

    @staticmethod
    def _format_pct(value: float | None, digits: int = 1) -> str:
        if value is None:
            return "NA"
        return f"{value:+.{digits}f}%"

    @staticmethod
    def _format_number(value: float | None, digits: int = 1) -> str:
        if value is None:
            return "NA"
        return f"{value:.{digits}f}"

    @staticmethod
    def _clean_sentence(value: str | None, fallback: str) -> str:
        cleaned = str(value or "").strip()
        return cleaned if cleaned else fallback

    @classmethod
    def _recent_quarter_summary(cls, fundamentals: CompanyFundamentals) -> str:
        latest = fundamentals.quarterly_results[0] if fundamentals.quarterly_results else None
        growth = fundamentals.growth
        if latest is None and growth is None:
            return "Recent quarterly commentary is limited in the current dataset."

        parts: list[str] = []
        if latest is not None:
            parts.append(
                f"{latest.period} sales were {cls._format_number(latest.sales_crore, 0)} crore and net profit was {cls._format_number(latest.net_profit_crore, 0)} crore."
            )
            if latest.operating_margin_pct is not None:
                parts.append(f"Operating margin was {cls._format_number(latest.operating_margin_pct, 1)}%.")
        if growth is not None:
            growth_bits: list[str] = []
            if growth.sales_yoy_pct is not None:
                growth_bits.append(f"sales YoY {cls._format_pct(growth.sales_yoy_pct)}")
            if growth.profit_yoy_pct is not None:
                growth_bits.append(f"profit YoY {cls._format_pct(growth.profit_yoy_pct)}")
            if growth_bits:
                parts.append("Growth snapshot: " + ", ".join(growth_bits) + ".")
        return " ".join(parts)

    @classmethod
    def _valuation_summary(cls, fundamentals: CompanyFundamentals) -> str | None:
        valuation = fundamentals.valuation
        if valuation is None:
            return None
        parts: list[str] = []
        if valuation.pe_ratio is not None:
            parts.append(f"PE {cls._format_number(valuation.pe_ratio, 1)}x")
        if valuation.peg_ratio is not None:
            parts.append(f"PEG {cls._format_number(valuation.peg_ratio, 2)}")
        if valuation.roe_pct is not None:
            parts.append(f"ROE {cls._format_number(valuation.roe_pct, 1)}%")
        if valuation.roce_pct is not None:
            parts.append(f"ROCE {cls._format_number(valuation.roce_pct, 1)}%")
        if valuation.dividend_yield_pct is not None:
            parts.append(f"yield {cls._format_number(valuation.dividend_yield_pct, 1)}%")
        if not parts:
            return None
        return ", ".join(parts)

    @classmethod
    def _key_metrics(cls, snapshot: StockSnapshot, fundamentals: CompanyFundamentals) -> dict[str, float | str]:
        metrics: dict[str, float | str] = {
            "Price": round(snapshot.last_price, 2),
            "3M Return": cls._format_pct(snapshot.stock_return_60d),
            "1Y Return": cls._format_pct(snapshot.stock_return_12m),
            "RS Rating": int(snapshot.rs_rating or 0),
        }
        valuation = fundamentals.valuation
        growth = fundamentals.growth
        if valuation and valuation.pe_ratio is not None:
            metrics["PE"] = round(valuation.pe_ratio, 2)
        if valuation and valuation.peg_ratio is not None:
            metrics["PEG"] = round(valuation.peg_ratio, 2)
        if growth and growth.sales_yoy_pct is not None:
            metrics["Sales YoY"] = cls._format_pct(growth.sales_yoy_pct)
        if growth and growth.profit_yoy_pct is not None:
            metrics["Profit YoY"] = cls._format_pct(growth.profit_yoy_pct)
        return metrics

    @classmethod
    def _sector_context_line(cls, snapshot: StockSnapshot, sector_snapshots: list[StockSnapshot]) -> str | None:
        if len(sector_snapshots) < 2:
            return None

        weighted_1w = cls._weighted_average_return(sector_snapshots, "stock_return_5d")
        weighted_1m = cls._weighted_average_return(sector_snapshots, "stock_return_20d")
        positive_1m = sum(1 for item in sector_snapshots if item.stock_return_20d > 0)
        leaders = [
            item.symbol
            for item in sorted(
                sector_snapshots,
                key=lambda item: (
                    item.rs_rating if item.rs_eligible else 0,
                    item.stock_return_20d,
                    item.market_cap_crore,
                ),
                reverse=True,
            )
            if item.symbol != snapshot.symbol
        ][:2]
        leaders_text = f" Stronger peers include {', '.join(leaders)}." if leaders else ""
        return (
            f"Sector context · {snapshot.sector}: weighted 1W {cls._format_pct(weighted_1w)} and 1M {cls._format_pct(weighted_1m)}; "
            f"{positive_1m}/{len(sector_snapshots)} stocks are positive over 1M.{leaders_text}"
        )

    @classmethod
    def _build_money_flow_stock_idea(
        cls,
        snapshot: StockSnapshot,
        fundamentals: CompanyFundamentals,
        recommendation_type: str,
        setup_score: float,
        setup_summary: str,
        thesis: str,
        sector_context: str | None = None,
    ) -> MoneyFlowStockIdea:
        recent_developments = cls._append_unique_line(
            cls._extract_recent_developments(fundamentals),
            sector_context,
            limit=5,
        )
        return MoneyFlowStockIdea(
            symbol=snapshot.symbol,
            name=snapshot.name,
            exchange=snapshot.exchange,
            sector=snapshot.sector,
            sub_sector=snapshot.sub_sector,
            recommendation_type=recommendation_type,
            last_price=snapshot.last_price,
            change_pct=snapshot.change_pct,
            market_cap_crore=snapshot.market_cap_crore,
            rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
            relative_volume=snapshot.relative_volume,
            stock_return_20d=snapshot.stock_return_20d,
            stock_return_60d=snapshot.stock_return_60d,
            stock_return_12m=snapshot.stock_return_12m,
            pct_from_52w_high=snapshot.pct_from_52w_high,
            pct_from_ath=snapshot.pct_from_ath,
            pullback_depth_pct=snapshot.pullback_depth_pct,
            setup_score=round(setup_score, 2),
            setup_summary=setup_summary,
            thesis=thesis,
            future_growth_summary=cls._future_growth_summary(fundamentals),
            recent_quarter_summary=cls._recent_quarter_summary(fundamentals),
            valuation_summary=cls._valuation_summary(fundamentals),
            recent_developments=recent_developments,
            growth_drivers=cls._extract_growth_driver_lines(fundamentals),
            risk_flags=cls._extract_risk_flags(fundamentals),
            key_metrics=cls._key_metrics(snapshot, fundamentals),
        )

    @staticmethod
    def _money_flow_idea_fields(recommendation_type: str | None = None) -> tuple[str, ...]:
        if recommendation_type == "consolidation":
            return ("consolidating_ideas",)
        if recommendation_type == "value":
            return ("value_ideas",)
        return ("consolidating_ideas", "value_ideas")

    @classmethod
    def _recent_money_flow_symbol_stats(
        cls,
        payloads: dict[str, dict],
        recommendation_date: str,
        recommendation_type: str | None = None,
        lookback_reports: int = 5,
    ) -> tuple[set[str], dict[str, int], dict[str, int]]:
        latest_symbols: set[str] = set()
        recent_counts: dict[str, int] = {}
        last_seen_distance: dict[str, int] = {}
        prior_dates = sorted((key for key in payloads.keys() if key < recommendation_date), reverse=True)

        for distance, date_key in enumerate(prior_dates[:lookback_reports], start=1):
            payload = payloads.get(date_key)
            if not isinstance(payload, dict):
                continue

            report_symbols: set[str] = set()
            for field in cls._money_flow_idea_fields(recommendation_type):
                raw_ideas = payload.get(field)
                if not isinstance(raw_ideas, list):
                    continue
                for item in raw_ideas:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol") or "").strip().upper()
                    if not symbol:
                        continue
                    report_symbols.add(symbol)

            if distance == 1:
                latest_symbols = set(report_symbols)

            for symbol in report_symbols:
                recent_counts[symbol] = recent_counts.get(symbol, 0) + 1
                last_seen_distance.setdefault(symbol, distance)

        return latest_symbols, recent_counts, last_seen_distance

    @classmethod
    def _sort_money_flow_candidates_for_rotation(
        cls,
        candidates: list[dict],
        payloads: dict[str, dict],
        recommendation_date: str,
        recommendation_type: str,
        lookback_reports: int = 5,
    ) -> list[dict]:
        latest_type_symbols, type_counts, type_last_seen = cls._recent_money_flow_symbol_stats(
            payloads,
            recommendation_date,
            recommendation_type=recommendation_type,
            lookback_reports=lookback_reports,
        )
        latest_any_symbols, any_counts, any_last_seen = cls._recent_money_flow_symbol_stats(
            payloads,
            recommendation_date,
            recommendation_type=None,
            lookback_reports=lookback_reports,
        )

        ranked: list[dict] = []
        for candidate in candidates:
            snapshot = candidate["snapshot"]
            symbol = str(snapshot.symbol or "").strip().upper()
            type_count = type_counts.get(symbol, 0)
            any_count = any_counts.get(symbol, 0)
            type_freshness = 2 if type_count == 0 else (1 if symbol not in latest_type_symbols else 0)
            any_freshness = 2 if any_count == 0 else (1 if symbol not in latest_any_symbols else 0)
            nearest_seen = min(
                [distance for distance in (type_last_seen.get(symbol), any_last_seen.get(symbol)) if distance is not None],
                default=lookback_reports + 1,
            )
            recent_penalty = (type_count * 18.0) + (any_count * 7.5)
            ranked.append(
                {
                    **candidate,
                    "rotation_type_freshness": type_freshness,
                    "rotation_any_freshness": any_freshness,
                    "rotation_staleness": nearest_seen,
                    "rotation_score": float(candidate["score"]) - recent_penalty,
                }
            )

        return sorted(
            ranked,
            key=lambda item: (
                item["rotation_type_freshness"],
                item["rotation_any_freshness"],
                item["rotation_staleness"],
                item.get("theme_priority", 0),
                item["rotation_score"],
                item["snapshot"].market_cap_crore,
            ),
            reverse=True,
        )

    @classmethod
    def _build_money_flow_candidate_pool(
        cls,
        candidates: list[dict],
        latest_symbols: set[str],
        *,
        final_limit: int,
        pool_limit: int,
        max_per_sector: int,
    ) -> list[dict]:
        normalized_latest = {symbol.strip().upper() for symbol in latest_symbols if symbol}
        fresh_candidates = [
            candidate
            for candidate in candidates
            if str(candidate["snapshot"].symbol or "").strip().upper() not in normalized_latest
        ]
        repeat_candidates = [
            candidate
            for candidate in candidates
            if str(candidate["snapshot"].symbol or "").strip().upper() in normalized_latest
        ]

        selected = cls._pick_diversified_candidates(
            fresh_candidates,
            limit=pool_limit,
            max_per_sector=max_per_sector,
        )
        if len(selected) >= pool_limit or len(selected) >= final_limit:
            return selected

        repeat_pool = cls._pick_diversified_candidates(
            repeat_candidates,
            limit=pool_limit,
            max_per_sector=max_per_sector,
        )
        selected_symbols = {
            str(candidate["snapshot"].symbol or "").strip().upper()
            for candidate in selected
        }
        for candidate in repeat_pool:
            symbol = str(candidate["snapshot"].symbol or "").strip().upper()
            if symbol in selected_symbols:
                continue
            selected.append(candidate)
            selected_symbols.add(symbol)
            if len(selected) >= pool_limit:
                break
        return selected

    @staticmethod
    def _pick_diversified_candidates(candidates: list[dict], limit: int, max_per_sector: int = 1) -> list[dict]:
        selected: list[dict] = []
        sector_counts: dict[str, int] = {}
        for candidate in candidates:
            sector = str(candidate["snapshot"].sector or "Unknown")
            if sector_counts.get(sector, 0) >= max_per_sector:
                continue
            selected.append(candidate)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            if len(selected) >= limit:
                return selected
        for candidate in candidates:
            if candidate in selected:
                continue
            selected.append(candidate)
            if len(selected) >= limit:
                break
        return selected

    @classmethod
    def _score_consolidation_snapshot(cls, snapshot: StockSnapshot) -> tuple[float, str, str] | None:
        if snapshot.avg_rupee_volume_30d_crore < 8:
            return None
        if snapshot.market_cap_crore < 500:
            return None
        if snapshot.stock_return_60d < 18 or snapshot.stock_return_12m < 20:
            return None
        if snapshot.pct_from_52w_high > 18 or snapshot.pullback_depth_pct > 16:
            return None
        if snapshot.rs_eligible and snapshot.rs_rating < 60:
            return None
        if snapshot.ema20 is None or snapshot.ema50 is None:
            return None
        if snapshot.last_price < snapshot.ema50 * 0.97:
            return None
        if abs(snapshot.stock_return_20d) > 12:
            return None

        score = (
            (snapshot.stock_return_60d * 0.5)
            + (snapshot.stock_return_12m * 0.15)
            + max(0.0, 16 - snapshot.pct_from_52w_high) * 2.0
            + max(0.0, 14 - snapshot.pullback_depth_pct) * 1.2
            + min(snapshot.relative_volume, 2.5) * 4.0
            + (snapshot.rs_rating if snapshot.rs_eligible else 60) * 0.25
            + max(0.0, 10 - abs(snapshot.stock_return_20d)) * 1.4
        )
        setup_summary = (
            f"Strong prior move with {cls._format_pct(snapshot.stock_return_60d)} over 3 months, now only "
            f"{cls._format_number(snapshot.pct_from_52w_high, 1)}% below the 52-week high and consolidating with "
            f"a {cls._format_number(snapshot.pullback_depth_pct, 1)}% pullback."
        )
        thesis = (
            f"{snapshot.symbol} has already shown institutional-style momentum, but the recent 1-month move of "
            f"{cls._format_pct(snapshot.stock_return_20d)} suggests digestion rather than blow-off action."
        )
        return score, setup_summary, thesis

    @classmethod
    def _score_value_candidate(cls, snapshot: StockSnapshot, fundamentals: CompanyFundamentals) -> tuple[float, str, str] | None:
        valuation = fundamentals.valuation
        growth = fundamentals.growth
        if valuation is None or growth is None:
            return None
        pe = valuation.pe_ratio
        peg = valuation.peg_ratio
        growth_ok = (growth.sales_yoy_pct or 0) >= 8 or (growth.profit_yoy_pct or 0) >= 10
        quality_ok = (valuation.roe_pct or 0) >= 12 or (valuation.roce_pct or 0) >= 14
        value_ok = (pe is not None and pe <= 35) or (peg is not None and peg <= 1.8)
        if not growth_ok or not quality_ok or not value_ok:
            return None
        if snapshot.avg_rupee_volume_30d_crore < 6 or snapshot.market_cap_crore < 300:
            return None
        if snapshot.stock_return_12m > 80:
            return None

        score = 0.0
        if pe is not None and pe > 0:
            score += max(0.0, 35 - pe) * 1.4
        if peg is not None and peg > 0:
            score += max(0.0, 2.0 - peg) * 18.0
        score += max(0.0, growth.sales_yoy_pct or 0) * 0.6
        score += max(0.0, growth.profit_yoy_pct or 0) * 0.8
        score += max(0.0, valuation.roe_pct or 0) * 0.35
        score += max(0.0, valuation.roce_pct or 0) * 0.35
        score += max(0.0, 12 - snapshot.pct_from_52w_high) * 0.8

        setup_summary = cls._clean_sentence(
            cls._valuation_summary(fundamentals),
            "Valuation metrics look reasonable relative to the current growth profile.",
        )
        thesis = (
            f"{snapshot.symbol} combines still-positive business growth with a valuation profile that remains reasonable "
            f"for the current profitability and capital efficiency levels."
        )
        return score, setup_summary, thesis

    async def _fetch_fundamentals_for_symbols(self, snapshots: list[StockSnapshot], limit: int) -> dict[str, CompanyFundamentals]:
        selected = snapshots[:limit]
        results: dict[str, CompanyFundamentals] = {}
        cached_loader = getattr(self.provider, "get_fundamentals_cached", None)
        missing: list[StockSnapshot] = []

        for snapshot in selected:
            fundamentals: CompanyFundamentals | None = None
            if callable(cached_loader):
                try:
                    fundamentals = await cached_loader(
                        snapshot.symbol,
                        snapshot=snapshot,
                        max_age_hours=72,
                    )
                except TypeError:
                    try:
                        fundamentals = await cached_loader(snapshot.symbol, snapshot=snapshot)
                    except Exception:
                        fundamentals = None
                except Exception:
                    fundamentals = None

            if fundamentals is not None:
                results[snapshot.symbol] = fundamentals
                continue
            missing.append(snapshot)

        if not missing:
            return results

        semaphore = asyncio.Semaphore(3)

        async def load(snapshot: StockSnapshot):
            async with semaphore:
                try:
                    fundamentals = await self.provider.get_fundamentals(symbol=snapshot.symbol, snapshot=snapshot)
                except Exception:
                    return None
            return snapshot.symbol, fundamentals

        fetched = await asyncio.gather(*[load(snapshot) for snapshot in missing])
        results.update(
            {
                symbol: fundamentals
                for item in fetched
                if item is not None
                for symbol, fundamentals in [item]
            }
        )
        return results

    async def generate_and_store_money_flow_stock_ideas(
        self,
        force: bool = False,
        reference_time: datetime | None = None,
    ) -> MoneyFlowStockIdeasResponse:
        now_local = reference_time or self._money_flow_now()
        recommendation_date = self._money_flow_stock_recommendation_date(now_local, force_today=force)
        cached_payloads = await asyncio.to_thread(self._load_money_flow_stock_payloads)

        if not force:
            cached_payload = cached_payloads.get(recommendation_date)
            if isinstance(cached_payload, dict):
                try:
                    return MoneyFlowStockIdeasResponse(**cached_payload)
                except Exception:
                    pass

        snapshots = await self._snapshots()
        consolidating_candidates: list[dict] = []
        for snapshot in snapshots:
            scored = self._score_consolidation_snapshot(snapshot)
            if scored is None:
                continue
            score, setup_summary, thesis = scored
            theme_priority = self._money_flow_stock_theme_priority(snapshot, "consolidation")
            consolidating_candidates.append(
                {
                    "snapshot": snapshot,
                    "score": score,
                    "theme_priority": theme_priority,
                    "setup_summary": setup_summary,
                    "thesis": thesis,
                }
            )
        consolidating_candidates.sort(
            key=lambda item: (item.get("theme_priority", 0), item["score"], item["snapshot"].market_cap_crore),
            reverse=True,
        )
        consolidating_candidates = self._sort_money_flow_candidates_for_rotation(
            consolidating_candidates,
            cached_payloads,
            recommendation_date,
            recommendation_type="consolidation",
        )
        latest_consolidating_symbols, _, _ = self._recent_money_flow_symbol_stats(
            cached_payloads,
            recommendation_date,
            recommendation_type="consolidation",
        )
        consolidating_candidates = self._build_money_flow_candidate_pool(
            consolidating_candidates,
            latest_consolidating_symbols,
            final_limit=4,
            pool_limit=16,
            max_per_sector=self._money_flow_max_per_sector(),
        )
        sector_snapshot_map: dict[str, list[StockSnapshot]] = {}
        for snapshot in snapshots:
            sector_snapshot_map.setdefault(snapshot.sector or "Unknown", []).append(snapshot)
        consolidating_fundamentals = await self._fetch_fundamentals_for_symbols(
            [item["snapshot"] for item in consolidating_candidates],
            limit=8,
        )
        consolidating_ideas: list[MoneyFlowStockIdea] = []
        for candidate in consolidating_candidates:
            snapshot = candidate["snapshot"]
            fundamentals = consolidating_fundamentals.get(snapshot.symbol)
            if fundamentals is None:
                continue
            consolidating_ideas.append(
                self._build_money_flow_stock_idea(
                    snapshot=snapshot,
                    fundamentals=fundamentals,
                    recommendation_type="consolidation",
                    setup_score=float(candidate["score"]),
                    setup_summary=str(candidate["setup_summary"]),
                    thesis=str(candidate["thesis"]),
                    sector_context=self._sector_context_line(
                        snapshot,
                        sector_snapshot_map.get(snapshot.sector or "Unknown", []),
                    ),
                )
            )
            if len(consolidating_ideas) >= 4:
                break

        value_snapshot_pool = sorted(
            [
                snapshot
                for snapshot in snapshots
                if snapshot.avg_rupee_volume_30d_crore >= 6 and snapshot.market_cap_crore >= 300
            ],
            key=lambda item: (
                self._money_flow_stock_theme_priority(item, "value"),
                item.avg_rupee_volume_30d_crore,
                item.market_cap_crore,
                item.stock_return_20d,
            ),
            reverse=True,
        )
        value_fundamentals = await self._fetch_fundamentals_for_symbols(
            value_snapshot_pool,
            limit=self._money_flow_value_fundamentals_limit(),
        )
        value_candidates: list[dict] = []
        snapshot_by_symbol = {snapshot.symbol: snapshot for snapshot in value_snapshot_pool}
        for symbol, fundamentals in value_fundamentals.items():
            snapshot = snapshot_by_symbol.get(symbol)
            if snapshot is None:
                continue
            scored = self._score_value_candidate(snapshot, fundamentals)
            if scored is None:
                continue
            score, setup_summary, thesis = scored
            theme_priority = self._money_flow_stock_theme_priority(snapshot, "value")
            value_candidates.append(
                {
                    "snapshot": snapshot,
                    "fundamentals": fundamentals,
                    "score": score,
                    "theme_priority": theme_priority,
                    "setup_summary": setup_summary,
                    "thesis": thesis,
                }
            )
        value_candidates.sort(
            key=lambda item: (item.get("theme_priority", 0), item["score"], item["snapshot"].market_cap_crore),
            reverse=True,
        )
        value_candidates = self._sort_money_flow_candidates_for_rotation(
            value_candidates,
            cached_payloads,
            recommendation_date,
            recommendation_type="value",
        )
        latest_value_symbols, _, _ = self._recent_money_flow_symbol_stats(
            cached_payloads,
            recommendation_date,
            recommendation_type="value",
        )
        value_candidates = self._build_money_flow_candidate_pool(
            value_candidates,
            latest_value_symbols,
            final_limit=3,
            pool_limit=10,
            max_per_sector=self._money_flow_max_per_sector(),
        )
        value_ideas: list[MoneyFlowStockIdea] = []
        for candidate in value_candidates:
            value_ideas.append(
                self._build_money_flow_stock_idea(
                    snapshot=candidate["snapshot"],
                    fundamentals=candidate["fundamentals"],
                    recommendation_type="value",
                    setup_score=float(candidate["score"]),
                    setup_summary=str(candidate["setup_summary"]),
                    thesis=str(candidate["thesis"]),
                    sector_context=self._sector_context_line(
                        candidate["snapshot"],
                        sector_snapshot_map.get(candidate["snapshot"].sector or "Unknown", []),
                    ),
                )
            )
            if len(value_ideas) >= 3:
                break

        ai_service = getattr(self.provider, "ai_service", None)
        response = MoneyFlowStockIdeasResponse(
            recommendation_date=recommendation_date,
            generated_at=datetime.now(timezone.utc),
            next_update_at=self._money_flow_stock_next_update(now_local),
            consolidating_ideas=consolidating_ideas,
            value_ideas=value_ideas,
            ai_model="gemini" if ai_service is not None and getattr(ai_service, "available", False) else None,
        )

        cached_payloads[recommendation_date] = response.model_dump(mode="json")
        await asyncio.to_thread(self._save_money_flow_stock_payloads, cached_payloads)
        return response

    async def get_money_flow_stock_ideas_history(self) -> MoneyFlowStockIdeasHistoryResponse:
        payloads = await asyncio.to_thread(self._load_money_flow_stock_payloads)
        reports: list[MoneyFlowStockIdeasResponse] = []
        for key in sorted(payloads.keys(), reverse=True):
            payload = payloads.get(key)
            if not isinstance(payload, dict):
                continue
            try:
                reports.append(MoneyFlowStockIdeasResponse(**payload))
            except Exception:
                continue
        latest = reports[0].recommendation_date if reports else None
        return MoneyFlowStockIdeasHistoryResponse(
            reports=reports,
            latest_recommendation_date=latest,
        )

    async def get_money_flow_stock_ideas(self) -> MoneyFlowStockIdeasResponse:
        now_local = self._money_flow_now()
        recommendation_date = self._money_flow_stock_recommendation_date(now_local)
        payloads = await asyncio.to_thread(self._load_money_flow_stock_payloads)
        payload = payloads.get(recommendation_date)
        if isinstance(payload, dict):
            try:
                return MoneyFlowStockIdeasResponse(**payload)
            except Exception:
                pass

        # If today's payload is unavailable, return the most recent cached payload
        # so the UI stays responsive even before the next scheduled generation.
        for key in sorted(payloads.keys(), reverse=True):
            stale_payload = payloads.get(key)
            if not isinstance(stale_payload, dict):
                continue
            try:
                return MoneyFlowStockIdeasResponse(**stale_payload)
            except Exception:
                continue

        # Do not auto-generate on read. First-time generation can be expensive and
        # block API calls; return a fast empty payload and let scheduler/manual
        # generate endpoint populate ideas.
        return MoneyFlowStockIdeasResponse(
            recommendation_date=recommendation_date,
            generated_at=datetime.now(timezone.utc),
            next_update_at=self._money_flow_stock_next_update(now_local),
            consolidating_ideas=[],
            value_ideas=[],
            ai_model=None,
        )

    async def answer_company_question(self, symbol: str, question: str) -> CompanyQuestionResponse:
        cleaned_question = question.strip()
        if not cleaned_question:
            raise ValueError("Question cannot be empty")
        fundamentals = await self.get_fundamentals(symbol)
        ai_service = getattr(self.provider, "ai_service", None)
        if ai_service is not None and getattr(ai_service, "available", False):
            answer = await asyncio.to_thread(ai_service.answer_company_question, fundamentals, cleaned_question)
            model_name = "gemini"
        else:
            developments = self._extract_recent_developments(fundamentals)
            driver_lines = self._extract_growth_driver_lines(fundamentals)
            answer = "\n\n".join(
                [
                    self._clean_sentence(
                        fundamentals.strategy_and_outlook or fundamentals.business_summary or fundamentals.about,
                        "Current strategic commentary is limited in the dataset.",
                    ),
                    self._recent_quarter_summary(fundamentals),
                    "Recent developments: " + ("; ".join(developments[:3]) if developments else "limited recent updates in cache."),
                    "Growth drivers: " + ("; ".join(driver_lines[:3]) if driver_lines else "not enough detail available."),
                ]
            )
            model_name = None
        return CompanyQuestionResponse(
            symbol=symbol,
            question=cleaned_question,
            answer=answer,
            generated_at=datetime.now(timezone.utc),
            ai_model=model_name,
        )

    async def get_sector_rotation(self) -> "SectorRotationResponse":
        """Rank sectors using a composite of weighted return, breadth vs Nifty, and momentum acceleration.

        Unified rule-set:
        - Compute market-cap-weighted absolute returns for 1D / 5D / 20D.
        - Require at least 5 liquid stocks with positive volume trend for sector eligibility.
        - Composite score per period combines:
            (a) weighted sector return,
            (b) % of sector stocks outperforming Nifty 50 for that period,
            (c) momentum improvement versus prior period baseline.
        """
        from app.models.market import SectorRotationItem, SectorRotationResponse, SectorRotationStock

        snapshots = await self._snapshots()
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._sector_rotation_cache
        if cached is not None and cached.generated_at >= snapshot_updated_at:
            return cached

        # Group stocks by sector
        sector_map: dict[str, list] = {}
        for s in snapshots:
            sector = (s.sector or "Unknown").strip() or "Unknown"
            sector_map.setdefault(sector, []).append(s)

        def _weighted_return(values: list[tuple[float, float]]) -> float:
            total_weight = 0.0
            weighted_sum = 0.0
            for value, weight in values:
                safe_weight = max(float(weight or 0.0), 1.0)
                weighted_sum += float(value or 0.0) * safe_weight
                total_weight += safe_weight
            if total_weight <= 0:
                return 0.0
            return weighted_sum / total_weight

        def _safe_scale(value: float, baseline: float) -> float:
            if abs(baseline) < 0.5:
                baseline = 0.5 if baseline >= 0 else -0.5
            return value / baseline

        nifty_1d = next((float(s.benchmark_return_1d or 0.0) for s in snapshots if s.benchmark_return_1d is not None), 0.0)
        nifty_1w = next((float(s.benchmark_return_5d or 0.0) for s in snapshots if s.benchmark_return_5d is not None), 0.0)
        nifty_1m = next((float(s.benchmark_return_20d or 0.0) for s in snapshots if s.benchmark_return_20d is not None), 0.0)

        items: list[SectorRotationItem] = []
        score_rows: list[dict[str, float | str | bool]] = []

        # Basic liquidity rule: at least 5 stocks with >=5 crore 30D turnover and rising participation (RVOL >= 1).
        min_liquid_stock_count = 5
        min_liquidity_crore = 5.0
        for sector, stocks in sorted(sector_map.items()):
            total = len(stocks)
            if total == 0:
                continue

            weighted_1d = _weighted_return([(float(s.change_pct or 0.0), float(s.market_cap_crore or 0.0)) for s in stocks])
            weighted_1w = _weighted_return([(float(s.stock_return_5d or 0.0), float(s.market_cap_crore or 0.0)) for s in stocks])
            weighted_1m = _weighted_return([(float(s.stock_return_20d or 0.0), float(s.market_cap_crore or 0.0)) for s in stocks])
            weighted_60d = _weighted_return([(float(s.stock_return_60d or 0.0), float(s.market_cap_crore or 0.0)) for s in stocks])

            # Momentum acceleration versus previous comparable pace.
            accel_1d = weighted_1d - _safe_scale(weighted_1w, 5)
            accel_1w = weighted_1w - _safe_scale(weighted_1m, 4)
            accel_1m = weighted_1m - _safe_scale(weighted_60d, 3)

            above_nifty_1d = sum(1 for s in stocks if float(s.change_pct or 0.0) > nifty_1d)
            above_nifty_1w = sum(1 for s in stocks if float(s.stock_return_5d or 0.0) > nifty_1w)
            above_nifty_1m = sum(1 for s in stocks if float(s.stock_return_20d or 0.0) > nifty_1m)

            liquid_positive_volume_count = sum(
                1
                for s in stocks
                if float(s.avg_rupee_volume_30d_crore or 0.0) >= min_liquidity_crore
                and float(s.relative_volume or 0.0) >= 1.0
            )
            eligible = liquid_positive_volume_count >= min_liquid_stock_count

            stock_items = [
                SectorRotationStock(
                    symbol=s.symbol,
                    name=s.name,
                    rs_rating=int(s.rs_rating or 0),
                    return_1d=round(float(s.change_pct or 0.0), 2),
                    return_1w=round(float(s.stock_return_5d or 0.0), 2),
                    return_1m=round(float(s.stock_return_20d or 0.0), 2),
                )
                for s in stocks
            ]
            items.append(SectorRotationItem(
                sector=sector,
                total_stocks=total,
                top_gainers_1d=above_nifty_1d,
                top_gainers_1w=above_nifty_1w,
                top_gainers_1m=above_nifty_1m,
                pct_top_gainers_1d=round(above_nifty_1d / total * 100, 1),
                pct_top_gainers_1w=round(above_nifty_1w / total * 100, 1),
                pct_top_gainers_1m=round(above_nifty_1m / total * 100, 1),
                avg_return_1d=round(weighted_1d, 2),
                avg_return_1w=round(weighted_1w, 2),
                avg_return_1m=round(weighted_1m, 2),
                rank_1d=0,
                rank_1w=0,
                rank_1m=0,
                stocks=sorted(stock_items, key=lambda item: item.rs_rating, reverse=True),
            ))

            score_rows.append(
                {
                    "sector": sector,
                    "eligible": eligible,
                    "return_1d": weighted_1d,
                    "return_1w": weighted_1w,
                    "return_1m": weighted_1m,
                    "breadth_1d": (above_nifty_1d / total) * 100,
                    "breadth_1w": (above_nifty_1w / total) * 100,
                    "breadth_1m": (above_nifty_1m / total) * 100,
                    "accel_1d": accel_1d,
                    "accel_1w": accel_1w,
                    "accel_1m": accel_1m,
                }
            )

        def _normalize(values: list[float]) -> list[float]:
            if not values:
                return []
            min_v = min(values)
            max_v = max(values)
            spread = max(max_v - min_v, 1e-9)
            return [(value - min_v) / spread for value in values]

        def _period_scores(period: str) -> dict[str, float]:
            base_rows = [row for row in score_rows if bool(row["eligible"])]
            if not base_rows:
                return {}

            # Strict gate: strong absolute return, broad participation, and rising momentum.
            gated_rows = [
                row
                for row in base_rows
                if float(row[f"return_{period}"]) > 0
                and float(row[f"breadth_{period}"]) >= 50
                and float(row[f"accel_{period}"]) > 0
            ]
            scoped_rows = gated_rows or base_rows

            returns = _normalize([float(row[f"return_{period}"]) for row in scoped_rows])
            breadth = _normalize([float(row[f"breadth_{period}"]) for row in scoped_rows])
            accel = _normalize([float(row[f"accel_{period}"]) for row in scoped_rows])
            # Composite score: prioritize absolute strength, then participation, then momentum acceleration.
            scores: dict[str, float] = {}
            for idx, row in enumerate(scoped_rows):
                score = (returns[idx] * 0.5) + (breadth[idx] * 0.3) + (accel[idx] * 0.2)
                scores[str(row["sector"])] = score
            return scores

        scores_1d = _period_scores("1d")
        scores_1w = _period_scores("1w")
        scores_1m = _period_scores("1m")

        # Assign ranks by composite score (eligible sectors first), then by weighted return as tie-break.
        items_dict = [item.model_dump() for item in items]

        for scores, return_field, rank_field in [
            (scores_1d, "avg_return_1d", "rank_1d"),
            (scores_1w, "avg_return_1w", "rank_1w"),
            (scores_1m, "avg_return_1m", "rank_1m"),
        ]:
            ranked = sorted(
                items_dict,
                key=lambda x: (
                    x["sector"] in scores,
                    scores.get(x["sector"], -1.0),
                    x[return_field],
                ),
                reverse=True,
            )
            for rank, item_d in enumerate(ranked, 1):
                item_d[rank_field] = rank

        # Final list sorted by daily rank
        sorted_dicts = sorted(items_dict, key=lambda x: x["rank_1d"])
        sorted_items = [SectorRotationItem(**d) for d in sorted_dicts]
        response = SectorRotationResponse(sectors=sorted_items, generated_at=snapshot_updated_at)
        self._sector_rotation_cache = response
        return response

    @staticmethod
    def _downsample_chart_points(points: list[ChartLinePoint], limit: int) -> list[ChartLinePoint]:
        if len(points) <= limit:
            return points
        step = max(len(points) / limit, 1)
        sampled = [points[int(index * step)] for index in range(limit)]
        if sampled[-1].time != points[-1].time:
            sampled[-1] = points[-1]
        return sampled

    def _chart_grid_points_for_timeframe(
        self,
        snapshot: StockSnapshot,
        timeframe: ChartGridTimeframe,
    ) -> list[ChartLinePoint]:
        points = [
            ChartLinePoint(time=int(point.time), value=round(float(point.value), 4))
            for point in (snapshot.chart_grid_points or [])
            if point.value not in (None, 0)
        ]
        if not points:
            return []

        latest_time = int(points[-1].time)
        lookback_days = {
            "3M": 95,
            "6M": 190,
            "1Y": 380,
            "2Y": 760,
        }[timeframe]
        threshold = latest_time - (lookback_days * 24 * 60 * 60)
        scoped = [point for point in points if int(point.time) >= threshold]
        if not scoped:
            scoped = points
        return self._downsample_chart_points(scoped, self._chart_grid_point_limit(timeframe))

    async def _build_chart_grid_card(
        self,
        snapshot: StockSnapshot,
        timeframe: ChartGridTimeframe,
        total_market_cap: float,
        semaphore: asyncio.Semaphore,
    ) -> ChartGridCard:
        sparkline = self._chart_grid_points_for_timeframe(snapshot, timeframe)
        if not sparkline:
            async with semaphore:
                try:
                    bars = await self.provider.get_chart(
                        snapshot.symbol,
                        "1D",
                        bars=self._chart_grid_bar_limit(timeframe),
                    )
                except Exception:
                    bars = []

            points = [
                ChartLinePoint(time=int(bar.time), value=round(float(bar.close), 4))
                for bar in bars
                if bar.close not in (None, 0)
            ]
            sparkline = self._downsample_chart_points(points, self._chart_grid_point_limit(timeframe))
        if not sparkline:
            sparkline = [
                ChartLinePoint(time=0, value=round(float(snapshot.last_price), 4)),
                ChartLinePoint(time=1, value=round(float(snapshot.last_price), 4)),
            ]

        return ChartGridCard(
            symbol=snapshot.symbol,
            name=snapshot.name,
            exchange=snapshot.exchange,
            sector=snapshot.sector,
            sub_sector=snapshot.sub_sector,
            market_cap_crore=snapshot.market_cap_crore,
            last_price=snapshot.last_price,
            change_pct=snapshot.change_pct,
            return_1d=snapshot.change_pct,
            return_1w=snapshot.stock_return_5d,
            return_1m=snapshot.stock_return_20d,
            return_3m=snapshot.stock_return_60d,
            return_6m=snapshot.stock_return_126d,
            return_1y=snapshot.stock_return_12m,
            return_2y=snapshot.stock_return_504d,
            rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
            weight_pct=round((snapshot.market_cap_crore / total_market_cap) * 100, 2) if total_market_cap > 0 else None,
            sparkline=sparkline,
        )

    async def get_chart_grid(
        self,
        *,
        name: str,
        group_kind: SectorGroupKind,
        timeframe: ChartGridTimeframe,
    ) -> ChartGridResponse:
        snapshots = await self._snapshots()
        snapshot_updated_at = self._snapshot_updated_at()
        cache_key = (group_kind, name, timeframe, snapshot_updated_at)
        cached = self._chart_grid_cache.get(cache_key)
        if cached is not None:
            return cached

        members: list[StockSnapshot]
        if group_kind == "sector":
            members = [snapshot for snapshot in snapshots if (snapshot.sector or "Unclassified") == name]
        else:
            index_constituents = await asyncio.to_thread(self._load_index_constituents_map)
            symbols = index_constituents.get(name, set())
            snapshot_by_symbol = {snapshot.symbol.upper(): snapshot for snapshot in snapshots}
            members = [snapshot_by_symbol[symbol] for symbol in symbols if symbol in snapshot_by_symbol]

        if not members:
            raise KeyError(name)

        ordered_members = sorted(
            members,
            key=lambda item: (
                item.market_cap_crore,
                item.rs_rating if item.rs_eligible else 0,
                item.change_pct,
            ),
            reverse=True,
        )
        total_market_cap = sum(max(item.market_cap_crore, 0.0) for item in ordered_members)
        semaphore = asyncio.Semaphore(12)
        cards = await asyncio.gather(
            *[
                self._build_chart_grid_card(
                    snapshot=member,
                    timeframe=timeframe,
                    total_market_cap=total_market_cap,
                    semaphore=semaphore,
                )
                for member in ordered_members
            ]
        )

        response = ChartGridResponse(
            generated_at=snapshot_updated_at,
            name=name,
            group_kind=group_kind,
            timeframe=timeframe,
            total_items=len(cards),
            cards=cards,
        )
        self._chart_grid_cache[cache_key] = response
        return response

    async def get_chart_grid_series(
        self,
        *,
        symbols: list[str],
        timeframe: ChartGridTimeframe,
    ) -> ChartGridSeriesResponse:
        normalized_symbols: list[str] = []
        seen: set[str] = set()
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized_symbols.append(symbol)
            if len(normalized_symbols) >= 48:
                break

        semaphore = asyncio.Semaphore(12)

        async def load_symbol(symbol: str) -> ChartGridSeriesItem | None:
            async with semaphore:
                try:
                    bars = await self.provider.get_chart(
                        symbol,
                        "1D",
                        bars=self._chart_grid_series_bar_limit(),
                    )
                except Exception:
                    return None
            return ChartGridSeriesItem(symbol=symbol, bars=bars)

        items = [item for item in await asyncio.gather(*[load_symbol(symbol) for symbol in normalized_symbols]) if item is not None]
        return ChartGridSeriesResponse(
            generated_at=self._snapshot_updated_at(),
            timeframe=timeframe,
            total_items=len(items),
            items=items,
        )

    async def get_index_quotes(self, symbols: list[str]) -> IndexQuotesResponse:
        normalized_symbols = [symbol.strip() for symbol in symbols if symbol.strip()]
        items = await self.provider.get_index_quotes(normalized_symbols)
        return IndexQuotesResponse(
            generated_at=datetime.now(timezone.utc),
            items=items,
        )

    async def get_fundamentals(self, symbol: str) -> CompanyFundamentals:
        snapshots = await self._snapshots()
        snapshot = next((item for item in snapshots if item.symbol == symbol), None)
        return await self.provider.get_fundamentals(symbol=symbol, snapshot=snapshot)

    async def get_gap_up_openers(
        self,
        min_gap_pct: float,
        min_liquidity_crore: float | None = None,
        include_sector_summaries: bool = True,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        items = self._gap_up_items(
            snapshots,
            min_gap_pct=min_gap_pct,
            min_liquidity_crore=min_liquidity_crore,
        )
        return await self._scan_results_response(
            scan={
                "id": "gap-up-openers",
                "name": "Gap Up Openers",
                "category": "Openers",
                "description": f"Stocks that opened gap-up by at least {min_gap_pct:.0f}%.",
                "hit_count": len(items),
            },
            scan_key="gap-up-openers",
            request_signature=json.dumps(
                {"min_gap_pct": min_gap_pct, "min_liquidity_crore": min_liquidity_crore},
                sort_keys=True,
            ),
            snapshots=snapshots,
            items=items,
            historical_runner=lambda historical_snapshots: self._gap_up_items(
                historical_snapshots,
                min_gap_pct=min_gap_pct,
                min_liquidity_crore=min_liquidity_crore,
            ),
            include_sector_summaries=include_sector_summaries,
        )

    @staticmethod
    def _near_pivot_consolidation(snapshot: StockSnapshot, max_range_pct: float) -> tuple[int, float, float]:
        highs = snapshot.recent_highs[-20:]
        lows = snapshot.recent_lows[-20:]
        available = min(len(highs), len(lows))
        if available < 2:
            return 0, 0.0, snapshot.last_price

        best_days = 0
        best_range_pct = 0.0
        best_pivot = snapshot.last_price
        for days in range(available, 1, -1):
            window_highs = highs[-days:]
            window_lows = lows[-days:]
            pivot_high = max(window_highs)
            pivot_low = min(window_lows)
            if pivot_high <= 0:
                continue
            range_pct = ((pivot_high - pivot_low) / pivot_high) * 100
            if range_pct <= max_range_pct:
                best_days = days
                best_range_pct = round(range_pct, 2)
                best_pivot = round(float(pivot_high), 2)
                break

        return best_days, best_range_pct, best_pivot

    async def get_near_pivot_scan_results(
        self,
        request: NearPivotScanRequest,
        include_sector_summaries: bool = True,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        items = self._near_pivot_items(snapshots, request)
        return await self._scan_results_response(
            scan={
                "id": "near-pivot",
                "name": "Near Pivot",
                "category": "Setups",
                "description": "High-RS stocks near 52-week highs that are holding a tight recent consolidation.",
                "hit_count": len(items),
            },
            scan_key="near-pivot",
            request_signature=json.dumps(request.model_dump(mode="python"), sort_keys=True, default=str),
            snapshots=snapshots,
            items=items[: request.limit],
            historical_runner=lambda historical_snapshots: self._near_pivot_items(historical_snapshots, request)[: request.limit],
            include_sector_summaries=include_sector_summaries,
        )

    async def get_pull_back_scan_results(
        self,
        request: PullBackScanRequest,
        include_sector_summaries: bool = True,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        items = self._pull_back_items(snapshots, request)
        return await self._scan_results_response(
            scan={
                "id": "pull-backs",
                "name": "Pull Backs",
                "category": "Setups",
                "description": "Momentum stocks in a short consolidation with flat resistance, volatility contraction, and EMA support.",
                "hit_count": len(items),
            },
            scan_key="pull-backs",
            request_signature=json.dumps(request.model_dump(mode="python"), sort_keys=True, default=str),
            snapshots=snapshots,
            items=items[: request.limit],
            historical_runner=lambda historical_snapshots: self._pull_back_items(historical_snapshots, request)[: request.limit],
            include_sector_summaries=include_sector_summaries,
        )

    async def get_returns_scan_results(
        self,
        request: ReturnsScanRequest,
        include_sector_summaries: bool = True,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        items = run_returns_scan(request, snapshots)
        return await self._scan_results_response(
            scan={
                "id": "returns",
                "name": "Returns",
                "category": "Returns",
                "description": f"Stocks with returns in the selected range over the {request.timeframe} period.",
                "hit_count": len(items),
            },
            scan_key="returns",
            request_signature=json.dumps(request.model_dump(mode="python"), sort_keys=True, default=str),
            snapshots=snapshots,
            items=items,
            historical_runner=lambda historical_snapshots: run_returns_scan(request, historical_snapshots),
            include_sector_summaries=include_sector_summaries,
        )

    async def get_consolidating_scan_results(
        self,
        request: ConsolidatingScanRequest,
        include_sector_summaries: bool = True,
    ) -> ScanResultsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        items = run_consolidating_scan(request, snapshots)
        return await self._scan_results_response(
            scan={
                "id": "consolidating",
                "name": "Consolidating",
                "category": "Setups",
                "description": "Select long run-up consolidations, near 3-year breakouts, or the union of both.",
                "hit_count": len(items),
            },
            scan_key="consolidating",
            request_signature=json.dumps(request.model_dump(mode="python"), sort_keys=True, default=str),
            snapshots=snapshots,
            items=items,
            historical_runner=lambda historical_snapshots: run_consolidating_scan(request, historical_snapshots),
            include_sector_summaries=include_sector_summaries,
        )

    async def get_sector_tab(self, sort_by: SectorSortBy, sort_order: str) -> SectorTabResponse:
        normalized_sort_order = "asc" if sort_order == "asc" else "desc"
        cache_key = (sort_by, normalized_sort_order)
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._sector_tab_cache.get(cache_key)
        if cached is not None and cached.generated_at >= snapshot_updated_at and self._index_constituents_cache_fresh():
            return cached
        sector_members: dict[str, list[StockSnapshot]] = {}
        sector_sub_groups: dict[str, dict[str, list[StockSnapshot]]] = {}

        for snapshot in snapshots:
            sector = snapshot.sector or "Unclassified"
            sub_sector = snapshot.sub_sector or "Unclassified"
            sector_members.setdefault(sector, []).append(snapshot)
            sector_sub_groups.setdefault(sector, {}).setdefault(sub_sector, []).append(snapshot)

        metric_field_map = {
            "1D": "change_pct",
            "1W": "stock_return_5d",
            "1M": "stock_return_20d",
            "3M": "stock_return_60d",
            "6M": "stock_return_126d",
            "1Y": "stock_return_12m",
            "2Y": "stock_return_504d",
        }
        sort_field = {
            "1D": "return_1d",
            "1W": "return_1w",
            "1M": "return_1m",
            "3M": "return_3m",
            "6M": "return_6m",
            "1Y": "return_1y",
            "2Y": "return_2y",
        }[sort_by]
        reverse = normalized_sort_order != "asc"
        index_constituents = self._cached_index_constituents_or_schedule_refresh()
        can_cache_response = self._index_constituents_cache_fresh()
        index_quote_items = []
        index_quote_symbols = [
            symbol
            for index_name, symbol in INDEX_HEATMAP_SYMBOLS.items()
            if index_name in index_constituents
        ]
        if index_quote_symbols:
            try:
                index_quote_items = await self.provider.get_index_quotes(index_quote_symbols)
            except Exception:
                index_quote_items = []
        index_quotes_by_symbol = {
            item.symbol.upper(): item
            for item in index_quote_items
            if item.price is not None
        }

        sector_cards: list[SectorCard] = []
        for sector, members in sector_members.items():
            sub_sector_groups = sector_sub_groups.get(sector, {})
            sector_cards.append(
                self._build_sector_card(
                    sector,
                    members,
                    sub_sector_groups,
                    "sector",
                    sort_by,
                    sort_field,
                    reverse,
                    metric_field_map,
                )
            )

        snapshot_by_symbol = {snapshot.symbol.upper(): snapshot for snapshot in snapshots}
        for index_name, _ in INDEX_HEATMAP_SOURCES:
            symbols = index_constituents.get(index_name, set())
            if not symbols:
                continue

            members = [snapshot_by_symbol[symbol] for symbol in symbols if symbol in snapshot_by_symbol]
            if not members:
                continue

            quote_symbol = INDEX_HEATMAP_SYMBOLS.get(index_name, "").upper()
            index_quote = index_quotes_by_symbol.get(quote_symbol)

            grouped_members: dict[str, list[StockSnapshot]] = {}
            for member in members:
                group_name = member.sub_sector or "Unclassified"
                grouped_members.setdefault(group_name, []).append(member)

            sector_cards.append(
                self._build_sector_card(
                    index_name,
                    members,
                    grouped_members,
                    "index",
                    sort_by,
                    sort_field,
                    reverse,
                    metric_field_map,
                    last_price=index_quote.price if index_quote is not None else None,
                )
            )

        sector_cards.sort(key=lambda card: getattr(card, sort_field), reverse=reverse)

        response = SectorTabResponse(
            generated_at=snapshot_updated_at,
            total_sectors=len(sector_cards),
            sort_by=sort_by,
            sort_order=normalized_sort_order,
            sectors=sector_cards,
        )
        if can_cache_response:
            self._sector_tab_cache[cache_key] = response
        return response

    async def get_improving_rs(self, window: ImprovingRsWindow) -> ImprovingRsResponse:
        snapshots = self._scan_eligible_snapshots(await self._snapshots())
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._improving_rs_cache.get(window)
        if cached is not None and cached.generated_at >= snapshot_updated_at:
            return cached
        previous_field = {
            "1D": "rs_rating_1d_ago",
            "1W": "rs_rating_1w_ago",
            "1M": "rs_rating_1m_ago",
        }[window]

        items: list[ImprovingRsItem] = []
        for snapshot in snapshots:
            previous_rating = int(getattr(snapshot, previous_field, snapshot.rs_rating) or 0)
            current_rating = int(snapshot.rs_rating or 0)
            if current_rating <= 0 or previous_rating <= 0 or current_rating <= previous_rating:
                continue

            items.append(
                ImprovingRsItem(
                    symbol=snapshot.symbol,
                    name=snapshot.name,
                    exchange=snapshot.exchange,
                    sector=snapshot.sector,
                    sub_sector=snapshot.sub_sector,
                    market_cap_crore=snapshot.market_cap_crore,
                    last_price=snapshot.last_price,
                    change_pct=snapshot.change_pct,
                    rs_rating=current_rating if snapshot.rs_eligible else None,
                    rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
                    rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
                    rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
                    improvement_1d=current_rating - snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
                    improvement_1w=current_rating - snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
                    improvement_1m=current_rating - snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
                )
            )

        if window == "1D":
            sort_key = lambda item: (item.improvement_1d, item.rs_rating, item.change_pct, item.market_cap_crore)
        elif window == "1W":
            sort_key = lambda item: (item.improvement_1w, item.rs_rating, item.change_pct, item.market_cap_crore)
        else:
            sort_key = lambda item: (item.improvement_1m, item.rs_rating, item.change_pct, item.market_cap_crore)

        items.sort(key=sort_key, reverse=True)

        response = ImprovingRsResponse(
            generated_at=snapshot_updated_at,
            window=window,
            total_hits=len(items),
            items=items,
        )
        self._improving_rs_cache[window] = response
        return response

    def _clear_runtime_caches(self) -> None:
        self._dashboard_cache = None
        self._scan_catalog_cache = None
        self._sector_tab_cache.clear()
        self._improving_rs_cache.clear()
        self._chart_grid_cache.clear()
        self._chart_response_cache.clear()
        self._index_constituent_task = None
        self._scan_sector_summary_cache.clear()
        self._market_overview_cache = None
        self._industry_groups_cache = None
        self._market_health_cache = None
        self._sector_rotation_cache = None

    def _market_key(self) -> str:
        default_exchange_getter = getattr(self.provider, "_default_exchange", None)
        default_exchange = default_exchange_getter() if callable(default_exchange_getter) else "NSE"
        return "india" if str(default_exchange or "").upper() in {"NSE", "BSE"} else "us"

    def _watchlists_state_path(self) -> Path:
        backend_root = getattr(self.provider, "backend_root", None)
        if backend_root is None:
            backend_root = Path(__file__).resolve().parents[2]
        data_dir = Path(backend_root) / "data"
        filename = "watchlists_state.json" if self._market_key() == "india" else "watchlists_state_us.json"
        return data_dir / filename

    @staticmethod
    def _normalize_watchlist_item(item: WatchlistItem) -> WatchlistItem | None:
        watchlist_id = str(item.id or "").strip()
        name = str(item.name or "").strip()
        if not watchlist_id or not name:
            return None

        color = str(item.color or "").strip()
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", color):
            color = "#4f8cff"

        symbols = list(dict.fromkeys(str(symbol or "").strip().upper() for symbol in item.symbols if str(symbol or "").strip()))
        return WatchlistItem(
            id=watchlist_id,
            name=name,
            color=color,
            symbols=symbols,
        )

    def _sanitize_watchlists_state(self, payload: WatchlistsStateResponse) -> WatchlistsStateResponse:
        normalized_watchlists: list[WatchlistItem] = []
        seen_ids: set[str] = set()
        for item in payload.watchlists:
            normalized = self._normalize_watchlist_item(item)
            if normalized is None or normalized.id in seen_ids:
                continue
            seen_ids.add(normalized.id)
            normalized_watchlists.append(normalized)

        active_watchlist_id = str(payload.active_watchlist_id or "").strip() or None
        if active_watchlist_id and active_watchlist_id not in seen_ids:
            active_watchlist_id = normalized_watchlists[0].id if normalized_watchlists else None

        return WatchlistsStateResponse(
            market=self._market_key(),
            updated_at=datetime.now(timezone.utc),
            active_watchlist_id=active_watchlist_id or (normalized_watchlists[0].id if normalized_watchlists else None),
            watchlists=normalized_watchlists,
        )

    def get_watchlists_state(self) -> WatchlistsStateResponse:
        path = self._watchlists_state_path()
        if not path.exists():
            return WatchlistsStateResponse(market=self._market_key())

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            state = WatchlistsStateResponse.model_validate(payload)
        except Exception:
            return WatchlistsStateResponse(market=self._market_key())

        return self._sanitize_watchlists_state(state)

    def save_watchlists_state(self, payload: WatchlistsStateResponse) -> WatchlistsStateResponse:
        state = self._sanitize_watchlists_state(payload)
        path = self._watchlists_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state.model_dump(mode="json"), indent=2), encoding="utf-8")
        return state

    def _industry_group_file_paths(self) -> tuple[Path, Path, Path]:
        backend_root = getattr(self.provider, "backend_root", None)
        if backend_root is None:
            backend_root = Path(__file__).resolve().parents[2]
        data_dir = Path(backend_root) / "data"
        if self._market_key() == "india":
            return (
                data_dir / "groups.json",
                data_dir / "group-ranks.json",
                data_dir / "stocks-to-groups.json",
            )
        return (
            data_dir / "groups_us.json",
            data_dir / "group-ranks_us.json",
            data_dir / "stocks-to-groups_us.json",
        )

    def _industry_groups_cache_path(self) -> Path:
        backend_root = getattr(self.provider, "backend_root", None)
        if backend_root is None:
            backend_root = Path(__file__).resolve().parents[2]
        data_dir = Path(backend_root) / "data"
        filename = "industry_groups_cache.json" if self._market_key() == "india" else "industry_groups_cache_us.json"
        return data_dir / filename

    def _load_cached_industry_groups(self, snapshot_updated_at: datetime) -> IndustryGroupsResponse | None:
        cache_path = self._industry_groups_cache_path()
        if not cache_path.exists():
            return self._load_legacy_cached_industry_groups(snapshot_updated_at)

        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            response = IndustryGroupsResponse.model_validate(payload)
        except Exception:
            return self._load_legacy_cached_industry_groups(snapshot_updated_at)

        if response.generated_at < snapshot_updated_at:
            return self._load_legacy_cached_industry_groups(snapshot_updated_at)
        return response

    def _load_legacy_cached_industry_groups(self, snapshot_updated_at: datetime) -> IndustryGroupsResponse | None:
        groups_path, ranks_path, stocks_path = self._industry_group_file_paths()
        if not groups_path.exists() or not ranks_path.exists() or not stocks_path.exists():
            return None

        try:
            groups_payload = json.loads(groups_path.read_text(encoding="utf-8"))
            rank_payload = json.loads(ranks_path.read_text(encoding="utf-8"))
            stock_payload = json.loads(stocks_path.read_text(encoding="utf-8"))
        except Exception:
            return None

        snapshot_date = snapshot_updated_at.astimezone(timezone.utc).date().isoformat()
        as_of_date = str(groups_payload.get("asOfDate") or "").strip()
        if as_of_date and as_of_date < snapshot_date:
            return None

        latest_mtime = max(groups_path.stat().st_mtime, ranks_path.stat().st_mtime, stocks_path.stat().st_mtime)
        generated_at = datetime.fromtimestamp(latest_mtime, tz=timezone.utc)
        base_groups = {
            str(row.get("groupId") or ""): row
            for row in groups_payload.get("groups", [])
            if isinstance(row, dict) and str(row.get("groupId") or "").strip()
        }

        try:
            groups = [
                IndustryGroupRankItem(
                    rank=int(rank_row.get("rank", 0) or 0),
                    rank_label=str(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("rankLabel") or f"#{int(rank_row.get('rank', 0) or 0)}"),
                    rank_change_1w=rank_row.get("rankChange1w"),
                    score_change_1w=rank_row.get("scoreChange1w"),
                    strength_bucket=str(rank_row.get("strengthBucket") or base_groups.get(str(rank_row.get("groupId") or ""), {}).get("strengthBucket") or "Top 40"),
                    trend_label=str(rank_row.get("trendLabel") or base_groups.get(str(rank_row.get("groupId") or ""), {}).get("trendLabel") or "Stable"),
                    group_id=str(rank_row.get("groupId") or ""),
                    group_name=str(rank_row.get("groupName") or base_groups.get(str(rank_row.get("groupId") or ""), {}).get("groupName") or "Unclassified"),
                    parent_sector=str(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("parentSector") or "Unclassified"),
                    description=str(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("description") or ""),
                    stock_count=int(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("stockCount") or len(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("symbols") or [])),
                    score=float(rank_row.get("score", 0.0) or 0.0),
                    return_1m=float(rank_row.get("return1m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("returns", {}).get("1m", 0.0)) or 0.0),
                    return_3m=float(rank_row.get("return3m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("returns", {}).get("3m", 0.0)) or 0.0),
                    return_6m=float(rank_row.get("return6m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("returns", {}).get("6m", 0.0)) or 0.0),
                    relative_return_1m=float(rank_row.get("relativeReturn1m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("relativeReturns", {}).get("1m", 0.0)) or 0.0),
                    relative_return_3m=float(rank_row.get("relativeReturn3m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("relativeReturns", {}).get("3m", 0.0)) or 0.0),
                    relative_return_6m=float(rank_row.get("relativeReturn6m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("relativeReturns", {}).get("6m", 0.0)) or 0.0),
                    median_return_1m=float(rank_row.get("return1m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("returns", {}).get("1m", 0.0)) or 0.0),
                    median_return_3m=float(rank_row.get("return3m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("returns", {}).get("3m", 0.0)) or 0.0),
                    median_return_6m=float(rank_row.get("return6m", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("returns", {}).get("6m", 0.0)) or 0.0),
                    pct_above_50dma=float(rank_row.get("above50dma", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("breadth", {}).get("above50dma", 0.0)) or 0.0),
                    pct_above_200dma=float(rank_row.get("above200dma", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("breadth", {}).get("above200dma", 0.0)) or 0.0),
                    pct_outperform_benchmark_3m=float(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("breadth", {}).get("positive3m", 0.0) or 0.0),
                    pct_outperform_benchmark_6m=float(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("breadth", {}).get("positive6m", 0.0) or 0.0),
                    breadth_score=float(rank_row.get("breadthScore", base_groups.get(str(rank_row.get("groupId") or ""), {}).get("breadth", {}).get("positive3m", 0.0)) or 0.0),
                    trend_health_score=float(rank_row.get("score", 0.0) or 0.0),
                    leaders=list(rank_row.get("leaders") or base_groups.get(str(rank_row.get("groupId") or ""), {}).get("leaders") or []),
                    laggards=list(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("laggards") or []),
                    top_constituents=list(rank_row.get("topConstituents") or []),
                    symbols=list(base_groups.get(str(rank_row.get("groupId") or ""), {}).get("symbols") or []),
                )
                for rank_row in rank_payload
                if isinstance(rank_row, dict) and str(rank_row.get("groupId") or "").strip()
            ]
            master = [
                IndustryGroupMasterItem(
                    group_id=str(item.get("groupId") or ""),
                    group_name=str(item.get("groupName") or ""),
                    parent_sector=str(item.get("parentSector") or "Unclassified"),
                    description=str(item.get("description") or ""),
                    stock_count=int(item.get("stockCount") or len(item.get("symbols") or [])),
                    symbols=list(item.get("symbols") or []),
                )
                for item in groups_payload.get("master", [])
                if isinstance(item, dict) and str(item.get("groupId") or "").strip()
            ]
            stocks = [
                IndustryGroupStockItem(
                    symbol=str(item.get("symbol") or ""),
                    company_name=str(item.get("companyName") or ""),
                    exchange=str(item.get("exchange") or ""),
                    market_cap_cr=float(item.get("marketCapCr", 0.0) or 0.0),
                    avg_traded_value_50d_cr=float(item.get("avgTradedValue50dCr", 0.0) or 0.0),
                    sector=str(item.get("sector") or ""),
                    raw_industry=str(item.get("rawIndustry") or ""),
                    final_group_id=str(item.get("finalGroupId") or ""),
                    final_group_name=str(item.get("finalGroupName") or ""),
                    last_price=float(item.get("lastPrice", 0.0) or 0.0),
                    change_pct=float(item.get("changePct", 0.0) or 0.0),
                    return_1m=float(item.get("return1m", 0.0) or 0.0),
                    return_3m=float(item.get("return3m", 0.0) or 0.0),
                    return_6m=float(item.get("return6m", 0.0) or 0.0),
                    return_1y=float(item.get("return1y", 0.0) or 0.0),
                    rs_rating=item.get("rsRating"),
                )
                for item in stock_payload
                if isinstance(item, dict) and str(item.get("symbol") or "").strip()
            ]
        except Exception:
            return None

        if not groups:
            return None

        return IndustryGroupsResponse(
            generated_at=generated_at,
            as_of_date=as_of_date or generated_at.date().isoformat(),
            benchmark=str(groups_payload.get("benchmark") or "Benchmark"),
            filters=IndustryGroupFilters(
                min_market_cap_cr=float(groups_payload.get("filters", {}).get("minMarketCapCr", 0.0) or 0.0),
                min_avg_daily_value_cr=float(groups_payload.get("filters", {}).get("minAvgDailyValueCr", 0.0) or 0.0),
            ),
            total_groups=len(groups),
            groups=sorted(groups, key=lambda item: item.rank),
            master=master,
            stocks=stocks,
        )

    def _save_industry_groups_cache(self, response: IndustryGroupsResponse) -> None:
        cache_path = self._industry_groups_cache_path()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(response.model_dump(mode="json"), indent=2), encoding="utf-8")

    def _resolve_group_benchmark(self, snapshots: list[StockSnapshot]) -> tuple[str, list[StockSnapshot]]:
        if self._market_key() == "india":
            constituent_map = self._cached_index_constituents_or_schedule_refresh()
            benchmark_symbols = constituent_map.get("Nifty 500", set())
            resolved = [snapshot for snapshot in snapshots if snapshot.symbol.upper() in benchmark_symbols]
            if resolved:
                return "NIFTY 500", resolved
            ordered = sorted(snapshots, key=lambda snapshot: snapshot.market_cap_crore, reverse=True)
            return "NIFTY 500", ordered[:500]

        ordered = sorted(snapshots, key=lambda snapshot: snapshot.market_cap_crore, reverse=True)
        return "US Market Proxy", ordered[:500]

    def _build_historical_snapshots(self, snapshots: list[StockSnapshot], offset_bars: int) -> list[StockSnapshot]:
        history_reader = getattr(self.provider, "_history_frame_from_cached_bars", None)
        history_builder = getattr(self.provider, "_history_to_snapshot", None)
        if not callable(history_reader) or not callable(history_builder):
            return []

        benchmark_close = asyncio.run(self._benchmark_close_series(max(620, 520 + offset_bars)))
        historical_snapshots: list[StockSnapshot] = []
        for snapshot in snapshots:
            history = history_reader(snapshot.symbol, max(620, 520 + offset_bars), allow_legacy=True)
            if history is None or history.empty or len(history) <= offset_bars:
                continue
            trimmed = history.iloc[:-offset_bars] if offset_bars > 0 else history
            if trimmed.empty or len(trimmed) < 30:
                continue
            benchmark_history = benchmark_close[benchmark_close.index <= trimmed.index[-1]] if not benchmark_close.empty else benchmark_close
            instrument = {
                "symbol": snapshot.symbol,
                "name": snapshot.name,
                "exchange": snapshot.exchange,
                "listing_date": snapshot.listing_date.isoformat() if snapshot.listing_date else None,
                "sector": snapshot.sector,
                "sub_sector": snapshot.sub_sector,
                "market_cap_crore": snapshot.market_cap_crore,
                "ticker": snapshot.instrument_key or f"{snapshot.symbol}.NS",
            }
            row = history_builder(instrument, trimmed, benchmark_history)
            if row is None:
                continue
            historical_snapshots.append(StockSnapshot.model_validate(row))
        return historical_snapshots

    async def get_industry_groups(self) -> IndustryGroupsResponse:
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._industry_groups_cache
        if cached is not None and cached.generated_at >= snapshot_updated_at:
            return cached

        disk_cached = await asyncio.to_thread(self._load_cached_industry_groups, snapshot_updated_at)
        if disk_cached is not None:
            self._industry_groups_cache = disk_cached
            return disk_cached

        snapshots = await self._snapshots()
        benchmark_label, benchmark_snapshots = self._resolve_group_benchmark(snapshots)
        historical_snapshots = await asyncio.to_thread(self._build_historical_snapshots, snapshots, 5)
        _, historical_benchmark_snapshots = self._resolve_group_benchmark(historical_snapshots)
        market_key = self._market_key()
        response = await asyncio.to_thread(
            build_industry_groups_response,
            snapshots,
            benchmark_snapshots,
            historical_snapshots,
            historical_benchmark_snapshots,
            generated_at=snapshot_updated_at,
            benchmark_label=benchmark_label,
            market_key=market_key,
        )
        groups_path, ranks_path, stocks_path = self._industry_group_file_paths()
        await asyncio.to_thread(
            write_industry_group_files,
            response,
            groups_path=groups_path,
            ranks_path=ranks_path,
            stocks_path=stocks_path,
        )
        await asyncio.to_thread(self._save_industry_groups_cache, response)
        self._industry_groups_cache = response
        return response

    async def refresh_market_data(self) -> dict[str, object]:
        snapshot_before = self.provider.get_snapshot_updated_at()
        refresh_strategy = None
        try:
            refresh_strategy_method = getattr(self.provider, "preferred_refresh_strategy", None)
            refresh_strategy = refresh_strategy_method() if callable(refresh_strategy_method) else None
            if refresh_strategy == "historical":
                snapshot_loader = self.provider.refresh_snapshots
            elif refresh_strategy == "cache":
                snapshot_loader = self.provider.get_snapshots
            else:
                snapshot_loader = self.provider.get_snapshots

            snapshots = await asyncio.wait_for(
                snapshot_loader(self.settings.market_cap_min_crore),
                timeout=max(self.settings.refresh_timeout_seconds, 15),
            )
            snapshot_after = self.provider.get_snapshot_updated_at()
            if refresh_strategy == "historical":
                refresh_metadata = self.provider.get_last_refresh_metadata()
            else:
                refresh_metadata = {
                    "applied_quote_count": 0,
                    "historical_rebuild": False,
                    "quote_source": None,
                }
            applied_quote_count = int(refresh_metadata.get("applied_quote_count", 0) or 0)
            historical_rebuild = bool(refresh_metadata.get("historical_rebuild", False))
            quote_source = refresh_metadata.get("quote_source")
            if historical_rebuild:
                refresh_mode = "historical-refresh"
                message = "Historical market snapshot refreshed for the latest closed session."
            elif refresh_strategy == "cache":
                refresh_mode = "cached-current"
                message = "Market data is already current for this session."
            else:
                refresh_mode = "cache-fallback"
                message = "Showing the latest cached close snapshot."
        except TimeoutError:
            snapshots = await self.provider.get_snapshots(self.settings.market_cap_min_crore)
            snapshot_after = self.provider.get_snapshot_updated_at()
            refresh_mode = "timeout-fallback"
            message = "Refresh took too long, so the app is still showing the most recent cached market snapshot."
            applied_quote_count = 0
            historical_rebuild = False
            quote_source = None
        except Exception:
            snapshots = await self.provider.get_snapshots(self.settings.market_cap_min_crore)
            snapshot_after = self.provider.get_snapshot_updated_at()
            refresh_mode = "error-fallback"
            message = "Live refresh failed, so the app is showing the most recent cached market snapshot."
            applied_quote_count = 0
            historical_rebuild = False
            quote_source = None

        snapshot_changed = snapshot_after != snapshot_before
        if snapshot_changed or applied_quote_count > 0 or historical_rebuild:
            self._clear_runtime_caches()

        snapshot_timestamp = snapshot_after or snapshot_before or datetime.now(timezone.utc)
        return {
            "ok": True,
            "universe_count": len(snapshots),
            "market_cap_min_crore": self.settings.market_cap_min_crore,
            "refresh_mode": refresh_mode,
            "message": message,
            "snapshot_updated_at": snapshot_timestamp.isoformat(),
            "snapshot_age_minutes": self._snapshot_age_minutes(snapshot_timestamp),
            "applied_quote_count": applied_quote_count,
            "historical_rebuild": historical_rebuild,
            "quote_source": quote_source,
        }

    @staticmethod
    def _weighted_average_return(items: list[StockSnapshot], field: str) -> float:
        total_weight = 0.0
        weighted_sum = 0.0
        for item in items:
            weight = max(item.market_cap_crore, 1.0)
            weighted_sum += float(getattr(item, field, 0.0) or 0.0) * weight
            total_weight += weight
        if total_weight == 0:
            return 0.0
        return round(weighted_sum / total_weight, 2)

    @staticmethod
    def _rs_rating_from_points(points: list[ChartLinePoint], age_days: int) -> int | None:
        if not points:
            return None
        latest_time = int(points[-1].time)
        threshold = latest_time - (age_days * 24 * 60 * 60)
        candidates = [point for point in points if int(point.time) <= threshold]
        target = candidates[-1] if candidates else points[0]
        return int(round(target.value))

    @staticmethod
    def _adr_pct_from_bars(bars: list) -> float | None:
        if len(bars) < 20:
            return None

        recent_bars = bars[-20:]
        ranges = [float(bar.high) - float(bar.low) for bar in recent_bars if getattr(bar, "high", None) is not None and getattr(bar, "low", None) is not None]
        closes = [float(bar.close) for bar in recent_bars if getattr(bar, "close", None) not in (None, 0)]
        if len(ranges) < 20 or len(closes) < 20:
            return None

        average_range = sum(ranges) / len(ranges)
        average_close = sum(closes) / len(closes)
        if average_close <= 0:
            return None
        return round((average_range / average_close) * 100, 2)

    def _with_chart_summary(
        self,
        summary: StockOverview,
        bars: list,
        rs_line: list[ChartLinePoint],
        previous_close: float | None = None,
    ) -> StockOverview:
        payload = summary.model_dump(mode="python")
        latest_close = None
        if bars:
            try:
                latest_close = float(bars[-1].close)
            except (AttributeError, TypeError, ValueError):
                latest_close = None
        if latest_close not in (None, 0):
            payload["last_price"] = round(latest_close, 2)
            # Use the definitively correct T-1 close from history (second to last bar)
            if len(bars) >= 2:
                try:
                    bar_prev_close = float(bars[-2].close)
                    if bar_prev_close > 0:
                        reference_previous_close = bar_prev_close
                except (AttributeError, TypeError, ValueError):
                    pass

            if reference_previous_close not in (None, 0):
                payload["change_pct"] = round(((latest_close / float(reference_previous_close)) - 1) * 100, 2)
        adr_pct_20 = self._adr_pct_from_bars(bars)
        if adr_pct_20 is not None:
            payload["adr_pct_20"] = adr_pct_20
        if rs_line:
            payload["rs_rating"] = int(round(rs_line[-1].value))
            payload["rs_rating_1d_ago"] = self._rs_rating_from_points(rs_line, 1) or payload.get("rs_rating_1d_ago")
            payload["rs_rating_1w_ago"] = self._rs_rating_from_points(rs_line, 7) or payload.get("rs_rating_1w_ago")
            payload["rs_rating_1m_ago"] = self._rs_rating_from_points(rs_line, 30) or payload.get("rs_rating_1m_ago")
        return StockOverview.model_validate(payload)

    async def _build_rs_line(
        self,
        symbol: str,
        timeframe: str,
        bars: list,
        snapshots: list[StockSnapshot],
    ) -> tuple[list[ChartLinePoint], list[ChartLineMarker]]:
        if not bars:
            return [], []

        if timeframe == "1D":
            daily_bars = bars[-self._rs_daily_bar_limit(timeframe=timeframe, bars=bars) :]
        else:
            try:
                daily_bars = await self.provider.get_chart(
                    symbol,
                    "1D",
                    bars=self._rs_daily_bar_limit(timeframe=timeframe, bars=bars),
                )
            except Exception:
                return [], []

        ordered_scores = sorted(snapshot.rs_weighted_score for snapshot in snapshots if snapshot.rs_eligible)
        if not ordered_scores or len(daily_bars) < 253:
            return [], []

        daily_ratings: list[ChartLinePoint] = []
        daily_markers: list[ChartLineMarker] = []
        closes = [float(bar.close) for bar in daily_bars]
        for index, daily_bar in enumerate(daily_bars):
            score = self._weighted_rs_score_for_index(closes, index)
            if score is None:
                continue

            rating = self._score_to_rs_rating(score, ordered_scores)
            daily_ratings.append(ChartLinePoint(time=daily_bar.time, value=rating))

            prior_52w_ratings = [point.value for point in daily_ratings[-253:-1]]
            if len(prior_52w_ratings) >= 252 and rating > max(prior_52w_ratings):
                daily_markers.append(
                    ChartLineMarker(
                        time=daily_bar.time,
                        value=rating,
                        label=f"RS {int(rating)} 52W high",
                        color="#39ff14",
                    )
                )

        if not daily_ratings:
            return [], []

        aligned_points: list[ChartLinePoint] = []
        aligned_markers: list[ChartLineMarker] = []
        cursor = 0
        latest_value: float | None = None
        daily_times = [point.time for point in daily_ratings]
        marker_times = {marker.time: marker for marker in daily_markers}
        used_marker_times: set[int] = set()

        for bar in bars:
            bar_time = int(bar.time)
            while cursor < len(daily_ratings) and daily_times[cursor] <= bar_time:
                latest_value = daily_ratings[cursor].value
                cursor += 1

            if latest_value is None:
                continue

            aligned_points.append(ChartLinePoint(time=bar_time, value=latest_value))

            matched_marker = marker_times.get(bar_time)
            if matched_marker is None:
                matched_marker = self._find_latest_marker_for_bar(marker_times, bar_time)
            if matched_marker is not None and matched_marker.time not in used_marker_times:
                used_marker_times.add(matched_marker.time)
                aligned_markers.append(
                    ChartLineMarker(
                        time=bar_time,
                        value=matched_marker.value,
                        label=matched_marker.label,
                        color=matched_marker.color,
                    )
                )

        return aligned_points, aligned_markers

    @staticmethod
    def _rs_daily_bar_limit(timeframe: str, bars: list) -> int:
        if timeframe == "1D":
            return len(bars) + 280
        if timeframe == "1W":
            return (len(bars) * 5) + 280

        unique_days = {
            datetime.fromtimestamp(int(bar.time), tz=timezone.utc).date().isoformat()
            for bar in bars
        }
        return len(unique_days) + 280

    @staticmethod
    def _weighted_rs_score_for_index(closes: list[float], index: int) -> float | None:
        if index < 252:
            return None
        score = 0.0

        for lookback, weight in ((63, 0.4), (126, 0.2), (189, 0.2), (252, 0.2)):
            baseline = closes[index - lookback]
            current = closes[index]
            if baseline <= 0:
                return None
            score += (((current / baseline) - 1) * 100) * weight

        return round(score, 4)

    @staticmethod
    def _score_to_rs_rating(score: float, ordered_scores: list[float]) -> float:
        if not ordered_scores:
            return 0.0
        if len(ordered_scores) == 1:
            return 99.0

        left = bisect_left(ordered_scores, score)
        right = bisect_right(ordered_scores, score) - 1
        if left >= len(ordered_scores):
            return 99.0

        average_rank = (left + max(left, right)) / 2
        percentile = round((average_rank / (len(ordered_scores) - 1)) * 99)
        return float(max(1, min(99, percentile)))

    @staticmethod
    def _find_latest_marker_for_bar(marker_times: dict[int, ChartLineMarker], bar_time: int) -> ChartLineMarker | None:
        candidates = [timestamp for timestamp in marker_times if timestamp <= bar_time]
        if not candidates:
            return None
        latest_timestamp = max(candidates)
        if datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).date() != datetime.fromtimestamp(bar_time, tz=timezone.utc).date():
            return None
        return marker_times.get(latest_timestamp)
