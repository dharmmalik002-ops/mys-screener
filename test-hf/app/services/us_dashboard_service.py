import asyncio
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from app.models.market import DashboardResponse, MarketHealthResponse, ScanDescriptor, SectorCard, SectorSortBy, SectorTabResponse, StockSnapshot
from app.providers.us_free import US_HISTORICAL_BREADTH_SYMBOL_LIMIT
from app.scanners.definitions import SCANS, scan_catalog_with_counts
from app.services.dashboard_service import DashboardService, build_leader_match


US_INDEX_PROXIES: tuple[tuple[str, str], ...] = (
    ("S&P 500", "SPY"),
    ("Nasdaq 100", "QQQ"),
    ("Dow 30", "DIA"),
)
US_ET = ZoneInfo("America/New_York")
US_MONEY_FLOW_STRONG_KEYWORDS: tuple[str, ...] = (
    "artificial intelligence",
    "ai",
    "semiconductor",
    "chip",
    "cyber",
    "cloud",
    "data processing",
    "robot",
    "automation",
    "space",
    "satellite",
    "aerospace",
    "defense",
    "quantum",
    "biotech",
    "genom",
    "fintech",
    "battery",
    "uranium",
)
US_MONEY_FLOW_BROAD_KEYWORDS: tuple[str, ...] = (
    "software",
    "digital",
    "payments",
    "electronic components",
    "communications equipment",
    "computer hardware",
    "computer manufacturing",
    "medical specialties",
    "pharmaceutical preparations",
    "renewable",
    "solar",
    "electric vehicle",
    "ev",
)


class USDashboardService(DashboardService):
    def __init__(self, provider, settings) -> None:
        super().__init__(provider, settings)
        backend_root = Path(__file__).resolve().parents[2]
        self._money_flow_cache_path = backend_root / "data" / "money_flow_reports_us.json"
        self._money_flow_stock_cache_path = backend_root / "data" / "money_flow_stock_ideas_us.json"
        self._scan_count_cache: tuple[datetime, list[ScanDescriptor]] | None = None
        self._scan_count_task: asyncio.Task[list[ScanDescriptor]] | None = None
        self._scan_count_task_updated_at: datetime | None = None
        self._sector_tab_tasks: dict[tuple[str, str], asyncio.Task[SectorTabResponse]] = {}
        self._sector_tab_task_updated_at: dict[tuple[str, str], datetime] = {}

    async def warm_startup_views(self) -> None:
        await super().warm_startup_views()
        await self.get_scan_counts()

    @staticmethod
    def _normalize_us_symbol(value: str) -> str:
        return str(value or "").strip().upper().replace(".", "-").replace("/", "-")

    def _money_flow_timezone(self):
        return US_ET

    def _money_flow_stock_cutoff(self) -> tuple[int, int]:
        return 16, 30

    def _money_flow_value_fundamentals_limit(self) -> int:
        return 40

    def _money_flow_max_per_sector(self) -> int:
        return 2

    def _money_flow_stock_theme_priority(self, snapshot: StockSnapshot, recommendation_type: str) -> int:
        text = " ".join(
            part
            for part in (
                str(snapshot.name or "").strip(),
                str(snapshot.sector or "").strip(),
                str(snapshot.sub_sector or "").strip(),
            )
            if part
        ).lower()
        if not text:
            return 0

        if any(keyword in text for keyword in US_MONEY_FLOW_STRONG_KEYWORDS):
            return 3 if recommendation_type == "consolidation" else 2
        if any(keyword in text for keyword in US_MONEY_FLOW_BROAD_KEYWORDS):
            return 2 if recommendation_type == "consolidation" else 1
        return 0

    @staticmethod
    def _copy_scan_descriptors(scanners: list[ScanDescriptor]) -> list[ScanDescriptor]:
        return [scanner.model_copy() for scanner in scanners]

    def _cached_scan_counts(self, snapshot_updated_at: datetime) -> list[ScanDescriptor] | None:
        cached = self._scan_count_cache
        if cached is None:
            return None
        cached_updated_at, scanners = cached
        if cached_updated_at < snapshot_updated_at:
            return None
        return self._copy_scan_descriptors(scanners)

    async def _build_scan_counts(
        self,
        snapshots: list[StockSnapshot],
        snapshot_updated_at: datetime,
    ) -> list[ScanDescriptor]:
        scanners, _ = await asyncio.to_thread(scan_catalog_with_counts, snapshots)
        copied = self._copy_scan_descriptors(scanners)
        self._scan_count_cache = (snapshot_updated_at, copied)
        return self._copy_scan_descriptors(copied)

    def _ensure_scan_count_task(
        self,
        snapshots: list[StockSnapshot],
        snapshot_updated_at: datetime,
    ) -> asyncio.Task[list[ScanDescriptor]]:
        current_task = self._scan_count_task
        if (
            current_task is not None
            and not current_task.done()
            and self._scan_count_task_updated_at is not None
            and self._scan_count_task_updated_at >= snapshot_updated_at
        ):
            return current_task

        task = asyncio.create_task(self._build_scan_counts(snapshots, snapshot_updated_at))
        self._scan_count_task = task
        self._scan_count_task_updated_at = snapshot_updated_at

        def clear_task(completed_task: asyncio.Task[list[ScanDescriptor]]) -> None:
            if self._scan_count_task is completed_task:
                self._scan_count_task = None

        task.add_done_callback(clear_task)
        return task

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
            ],
        )
        self._market_health_cache = response
        return response

    def _resolve_market_health_universes(self, snapshots: list[StockSnapshot]) -> list[tuple[str, list[StockSnapshot]]]:
        equity_snapshots = [
            snapshot
            for snapshot in snapshots
            if str(snapshot.sector or "").strip().lower() != "exchange traded funds"
            and str(snapshot.sub_sector or "").strip().lower() != "etf"
        ]
        sorted_equities = sorted(equity_snapshots, key=lambda item: item.market_cap_crore, reverse=True)

        universes: list[tuple[str, list[StockSnapshot]]] = []
        for universe_name in ("NYSE", "NASDAQ"):
            selected = [
                snapshot
                for snapshot in sorted_equities
                if str(snapshot.exchange or "").strip().upper() == universe_name
            ][:US_HISTORICAL_BREADTH_SYMBOL_LIMIT]
            if not selected:
                selected = sorted_equities[:US_HISTORICAL_BREADTH_SYMBOL_LIMIT]
            universes.append((universe_name, selected))
        return universes

    async def build_dashboard(self) -> DashboardResponse:
        snapshots = await self._snapshots()
        snapshot_updated_at = self._snapshot_updated_at()
        cached_dashboard = self._dashboard_cache
        if cached_dashboard is not None and cached_dashboard.generated_at >= snapshot_updated_at:
            return cached_dashboard

        top_gainers = sorted(snapshots, key=lambda item: item.change_pct, reverse=True)[:5]
        top_losers = sorted(snapshots, key=lambda item: item.change_pct)[:5]
        top_volume = sorted(
            [item for item in snapshots if item.avg_volume_20d > 0 and len(item.recent_volumes) >= 5],
            key=lambda item: item.relative_volume,
            reverse=True,
        )[:5]

        scanners = [
            ScanDescriptor(
                id="custom-scan",
                name="Custom Scanner",
                category="Custom",
                description="Build a scan with your own price, RS, volume, and trend filters.",
                hit_count=0,
            ),
            *[
                ScanDescriptor(
                    id=scan.id,
                    name=scan.name,
                    category=scan.category,
                    description=scan.description,
                    hit_count=0,
                )
                for scan in SCANS
            ],
        ]

        response = DashboardResponse(
            app_name=self.settings.app_name,
            generated_at=snapshot_updated_at,
            market_status=self._market_status(snapshot_updated_at),
            data_mode=self.settings.data_mode,
            market_cap_min_crore=self.settings.market_cap_min_crore,
            universe_count=len(snapshots),
            scanners=scanners,
            popular_scan_ids=["breakout-ath", "volume-price", "clean-pullback", "relative-strength", "darvas-box"],
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
            recent_alerts=[],
        )
        self._dashboard_cache = response
        return response

    async def get_scan_counts(self) -> list[ScanDescriptor]:
        snapshots = await self._snapshots()
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._cached_scan_counts(snapshot_updated_at)
        if cached is not None:
            return cached

        task = self._ensure_scan_count_task(snapshots, snapshot_updated_at)
        try:
            return await task
        except Exception:
            return await self._build_scan_counts(snapshots, snapshot_updated_at)

    async def get_sector_tab(self, sort_by: SectorSortBy, sort_order: str) -> SectorTabResponse:
        normalized_sort_order = "asc" if sort_order == "asc" else "desc"
        cache_key = (sort_by, normalized_sort_order)
        snapshots = await self._snapshots()
        snapshot_updated_at = self._snapshot_updated_at()
        cached = self._sector_tab_cache.get(cache_key)
        if cached is not None and cached.generated_at >= snapshot_updated_at:
            return cached

        return await self._get_or_create_sector_tab_task(
            cache_key,
            snapshots,
            snapshot_updated_at,
            sort_by,
            normalized_sort_order,
        )

    async def _build_sector_tab_response(
        self,
        snapshots: list[StockSnapshot],
        snapshot_updated_at: datetime,
        sort_by: SectorSortBy,
        normalized_sort_order: str,
    ) -> SectorTabResponse:
        cache_key = (sort_by, normalized_sort_order)

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

        snapshot_by_symbol = {self._normalize_us_symbol(snapshot.symbol): snapshot for snapshot in snapshots}
        for index_name, proxy_symbol in US_INDEX_PROXIES:
            member = snapshot_by_symbol.get(self._normalize_us_symbol(proxy_symbol))
            if member is None:
                continue

            sector_cards.append(
                self._build_sector_card(
                    index_name,
                    [member],
                    {"Index ETF": [member]},
                    "index",
                    sort_by,
                    sort_field,
                    reverse,
                    metric_field_map,
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
        self._sector_tab_cache[cache_key] = response
        return response

    async def _get_or_create_sector_tab_task(
        self,
        cache_key: tuple[str, str],
        snapshots: list[StockSnapshot],
        snapshot_updated_at: datetime,
        sort_by: SectorSortBy,
        normalized_sort_order: str,
    ) -> SectorTabResponse:
        current_task = self._sector_tab_tasks.get(cache_key)
        current_task_updated_at = self._sector_tab_task_updated_at.get(cache_key)
        if (
            current_task is not None
            and not current_task.done()
            and current_task_updated_at is not None
            and current_task_updated_at >= snapshot_updated_at
        ):
            return await current_task

        task = asyncio.create_task(
            self._build_sector_tab_response(
                snapshots,
                snapshot_updated_at,
                sort_by,
                normalized_sort_order,
            )
        )
        self._sector_tab_tasks[cache_key] = task
        self._sector_tab_task_updated_at[cache_key] = snapshot_updated_at

        def clear_task(completed_task: asyncio.Task[SectorTabResponse]) -> None:
            if self._sector_tab_tasks.get(cache_key) is completed_task:
                self._sector_tab_tasks.pop(cache_key, None)

        task.add_done_callback(clear_task)
        return await task