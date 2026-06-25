from fastapi import APIRouter, HTTPException, Query

from app.models.market import (
    BhavcopyStatusResponse,
    ChartGridResponse,
    ChartGridSeriesResponse,
    ChartGridTimeframe,
    ChartResponse,
    IndustryGroupsResponse,
    CompanyFundamentals,
    ConsolidatingScanRequest,
    CustomScanRequest,
    DemandZoneScanRequest,
    IndexPeHistoryResponse,
    IndexQuotesResponse,
    ImprovingRsResponse,
    ImprovingRsWindow,
    MarketOverviewResponse,
    MomentumBurstScanRequest,
    NearPivotScanRequest,
    PullBackScanRequest,
    ReturnsScanRequest,
    ScanDescriptor,
    SectorGroupKind,
    WatchlistsStateResponse,
)


def build_router(service):
    router = APIRouter(prefix="/api")

    def default_index_symbols(market: str | None) -> list[str]:
        return ["^NSEI", "^BSESN", "^NSEBANK"]

    def resolve_service(market: str):
        normalized_market = str(market or "india").strip().lower()
        if normalized_market not in service:
            raise HTTPException(status_code=400, detail=f"Unsupported market: {market}")
        return service[normalized_market]

    @router.get("/health")
    async def health():
        return {"ok": True, "scanner_patch": "eod-scanners-v10"}

    @router.get("/dashboard")
    async def dashboard(market: str = Query(default="india")):
        return await resolve_service(market).build_dashboard()

    @router.get("/scans")
    async def scans(market: str = Query(default="india")):
        dashboard = await resolve_service(market).build_dashboard()
        return dashboard.scanners

    @router.get("/scan-counts", response_model=list[ScanDescriptor])
    async def scan_counts(market: str = Query(default="india")):
        return await resolve_service(market).get_scan_counts()

    @router.post("/ai/swing-analysis")
    async def ai_swing_analysis(payload: dict, market: str = Query(default="india")):
        symbol = str(payload.get("symbol") or "").strip()
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol is required")
        as_of = str(payload.get("as_of") or "").strip() or None
        return await resolve_service(market).get_ai_swing_analysis(symbol, as_of=as_of)

    @router.post("/ai/journal-review")
    async def ai_journal_review(payload: dict, market: str = Query(default="india")):
        return await resolve_service(market).get_ai_journal_review(payload)

    @router.get("/scanner-scorecard")
    async def scanner_scorecard(market: str = Query(default="india")):
        return await resolve_service(market).get_scanner_scorecard()

    @router.get("/scans/{scan_id}")
    async def scan_results(
        scan_id: str,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
        min_liquidity_crore: float | None = Query(default=None, ge=0.0),
        # Expansion scanner thresholds — exposed so the panel can let users
        # tune the day-change% and 50-day RVOL gates without rebuilding the
        # backend. Falls back to the strict defaults (6.5%, 3.0x) when
        # omitted, preserving the IBD-style screen behaviour.
        expansion_min_change_pct: float | None = Query(default=None, ge=0.0, le=100.0),
        expansion_min_relative_volume: float | None = Query(default=None, ge=0.0, le=50.0),
        # Positive Earnings scanner thresholds — all four reaction gates
        # plus the recency window. Omitted values fall back to the spec
        # defaults (75% / 1% / 2x / 10% / 60 days).
        positive_earnings_min_close_in_range_pct: float | None = Query(default=None, ge=0.0, le=1.0),
        positive_earnings_min_next_day_gap_pct: float | None = Query(default=None, ge=-50.0, le=50.0),
        positive_earnings_min_day_rvol: float | None = Query(default=None, ge=0.0, le=50.0),
        positive_earnings_min_return_5d_pct: float | None = Query(default=None, ge=-100.0, le=200.0),
        positive_earnings_lookback_days: int | None = Query(default=None, ge=1, le=365),
        # Volume screener: lookback window (1m/3m/6m/1y) and optional RVOL gate.
        volume_window: str | None = Query(default=None),
        volume_min_rvol: float | None = Query(default=None, ge=0.0, le=100.0),
    ):
        try:
            return await resolve_service(market).get_scan_results(
                scan_id,
                include_sector_summaries=include_sector_summaries,
                min_liquidity_crore=min_liquidity_crore,
                expansion_min_change_pct=expansion_min_change_pct,
                expansion_min_relative_volume=expansion_min_relative_volume,
                positive_earnings_min_close_in_range_pct=positive_earnings_min_close_in_range_pct,
                positive_earnings_min_next_day_gap_pct=positive_earnings_min_next_day_gap_pct,
                positive_earnings_min_day_rvol=positive_earnings_min_day_rvol,
                positive_earnings_min_return_5d_pct=positive_earnings_min_return_5d_pct,
                positive_earnings_lookback_days=positive_earnings_lookback_days,
                volume_window=volume_window,
                volume_min_rvol=volume_min_rvol,
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown scan: {scan_id}") from error

    @router.post("/custom-scan")
    async def custom_scan(
        request: CustomScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_custom_scan_results(request, include_sector_summaries=include_sector_summaries)

    @router.get("/gap-up-openers")
    async def gap_up_openers(
        market: str = Query(default="india"),
        min_gap_pct: float = Query(default=1.0, ge=0.0),
        min_liquidity_crore: float | None = Query(default=None, ge=0.0),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_gap_up_openers(
            min_gap_pct=min_gap_pct,
            min_liquidity_crore=min_liquidity_crore,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/near-pivot")
    async def near_pivot_scan(
        request: NearPivotScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_near_pivot_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/pull-backs")
    async def pull_back_scan(
        request: PullBackScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_pull_back_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/returns")
    async def returns_scan(
        request: ReturnsScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_returns_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/consolidating")
    async def consolidating_scan(
        request: ConsolidatingScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_consolidating_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/demand-zone")
    async def demand_zone_scan(
        request: DemandZoneScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_demand_zone_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/momentum-burst")
    async def momentum_burst_scan(
        request: MomentumBurstScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market).get_momentum_burst_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.get("/improving-rs", response_model=ImprovingRsResponse)
    async def improving_rs(
        market: str = Query(default="india"),
        window: ImprovingRsWindow = Query(default="1D"),
    ):
        return await resolve_service(market).get_improving_rs(window=window)

    @router.post("/refresh")
    async def refresh_market_data(market: str = Query(default="india")):
        return await resolve_service(market).refresh_market_data()

    @router.get("/index-quotes", response_model=IndexQuotesResponse)
    async def index_quotes(
        market: str = Query(default="india"),
        symbols: str | None = Query(default=None),
    ):
        requested_symbols = [symbol.strip() for symbol in (symbols or "").split(",") if symbol.strip()] or default_index_symbols(market)
        return await resolve_service(market).get_index_quotes(requested_symbols)

    @router.get("/market-overview", response_model=MarketOverviewResponse)
    async def market_overview(market: str = Query(default="india")):
        return await resolve_service(market).get_market_overview()

    @router.get("/index-pe/{symbol}/history", response_model=IndexPeHistoryResponse)
    async def index_pe_history(symbol: str, market: str = Query(default="india")):
        return await resolve_service(market).get_index_pe_history(symbol.upper())

    @router.get("/groups", response_model=IndustryGroupsResponse)
    async def groups(market: str = Query(default="india")):
        return await resolve_service(market).get_industry_groups()

    @router.get("/watchlists", response_model=WatchlistsStateResponse)
    async def watchlists(market: str = Query(default="india")):
        return resolve_service(market).get_watchlists_state()

    @router.put("/watchlists", response_model=WatchlistsStateResponse)
    async def save_watchlists(payload: WatchlistsStateResponse, market: str = Query(default="india")):
        market_service = resolve_service(market)
        normalized = payload.model_copy(update={"market": str(market or "india").strip().lower()})
        return market_service.save_watchlists_state(normalized)

    @router.get("/bhavcopy/status", response_model=BhavcopyStatusResponse)
    async def bhavcopy_status(market: str = Query(default="india")):
        return resolve_service(market).get_bhavcopy_status()

    @router.get("/journal")
    async def get_journal():
        return resolve_service("india").get_journal_data()

    @router.put("/journal")
    async def save_journal(payload: dict):
        return resolve_service("india").save_journal_data(payload)

    @router.get("/chart/{symbol}", response_model=ChartResponse)
    async def chart(
        symbol: str,
        market: str = Query(default="india"),
        timeframe: str = Query(default="1D"),
    ):
        return await resolve_service(market).get_chart(symbol=symbol.upper(), timeframe=timeframe)

    @router.get("/chart/{symbol}/history", response_model=ChartResponse)
    async def chart_history(
        symbol: str,
        market: str = Query(default="india"),
        timeframe: str = Query(default="1D"),
    ):
        return await resolve_service(market).get_chart_history(symbol=symbol.upper(), timeframe=timeframe)

    @router.get("/chart-grid", response_model=ChartGridResponse)
    async def chart_grid(
        market: str = Query(default="india"),
        name: str = Query(..., min_length=1),
        group_kind: SectorGroupKind = Query(default="sector"),
        timeframe: ChartGridTimeframe = Query(default="1Y"),
    ):
        try:
            return await resolve_service(market).get_chart_grid(
                name=name,
                group_kind=group_kind,
                timeframe=timeframe,
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown chart grid: {name}") from error

    @router.get("/chart-grid-series", response_model=ChartGridSeriesResponse)
    async def chart_grid_series(
        market: str = Query(default="india"),
        symbols: str = Query(..., min_length=1),
        timeframe: ChartGridTimeframe = Query(default="1Y"),
    ):
        requested_symbols = [symbol.strip().upper() for symbol in symbols.split(",") if symbol.strip()]
        return await resolve_service(market).get_chart_grid_series(symbols=requested_symbols, timeframe=timeframe)

    @router.get("/fundamentals/{symbol}", response_model=CompanyFundamentals)
    async def fundamentals(symbol: str, market: str = Query(default="india")):
        return await resolve_service(market).get_fundamentals(symbol=symbol.upper())

    @router.get("/earnings/{symbol}")
    async def earnings(symbol: str, market: str = Query(default="india")):
        return await resolve_service(market).get_earnings_summary(symbol=symbol.upper())

    @router.get("/live-news")
    async def live_news(
        market: str = Query(default="india"),
        category: str | None = Query(default=None),
        limit: int = Query(default=150, le=400),
    ):
        from app.services.rss_news_service import get_rss_service

        service_obj = get_rss_service(market)
        items = await service_obj.get_all_news(limit=limit)
        if category and category.lower() != "all":
            items = [item for item in items if str(item.get("category", "")).lower() == category.lower()]
        categories = sorted({str(item.get("category", "General")) for item in items})
        return {"items": items, "count": len(items), "categories": categories}

    @router.get("/article-proxy")
    async def article_proxy(url: str = Query(...)):
        import re as _re
        import urllib.parse as _parse
        import urllib.request as _req

        from fastapi.responses import HTMLResponse

        parsed = _parse.urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise HTTPException(status_code=400, detail="Only http/https URLs allowed")

        try:
            request_obj = _req.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Referer": "https://www.google.com/",
                },
            )
            with _req.urlopen(request_obj, timeout=15) as response:
                html = response.read(2_000_000).decode("utf-8", errors="replace")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Upstream fetch failed: {exc}") from exc

        html = _re.sub(
            r'<meta\s[^>]*http-equiv\s*=\s*["\']?(x-frame-options|content-security-policy)["\']?[^>]*>',
            "",
            html,
            flags=_re.IGNORECASE,
        )
        base_tag = f'<base href="{url}" target="_blank">'
        if _re.search(r"<head", html, _re.IGNORECASE):
            html = _re.sub(r"<head([^>]*)>", rf"<head\1>{base_tag}", html, count=1, flags=_re.IGNORECASE)
        else:
            html = base_tag + html

        return HTMLResponse(
            content=html,
            headers={
                "Content-Security-Policy": "frame-ancestors *; default-src * 'unsafe-inline' 'unsafe-eval' data: blob:",
                "Access-Control-Allow-Origin": "*",
            },
        )

    @router.get("/{market_name}/health")
    async def namespaced_health(market_name: str):
        resolve_service(market_name)
        return {"ok": True, "market": market_name, "scanner_patch": "eod-scanners-v10"}

    @router.get("/{market_name}/dashboard")
    async def namespaced_dashboard(market_name: str):
        return await resolve_service(market_name).build_dashboard()

    @router.get("/{market_name}/scans")
    async def namespaced_scans(market_name: str):
        dashboard = await resolve_service(market_name).build_dashboard()
        return dashboard.scanners

    @router.get("/{market_name}/scan-counts", response_model=list[ScanDescriptor])
    async def namespaced_scan_counts(market_name: str):
        return await resolve_service(market_name).get_scan_counts()

    @router.get("/{market_name}/scans/{scan_id}")
    async def namespaced_scan_results(
        market_name: str,
        scan_id: str,
        include_sector_summaries: bool = Query(default=False),
        min_liquidity_crore: float | None = Query(default=None, ge=0.0),
        expansion_min_change_pct: float | None = Query(default=None, ge=0.0, le=100.0),
        expansion_min_relative_volume: float | None = Query(default=None, ge=0.0, le=50.0),
        positive_earnings_min_close_in_range_pct: float | None = Query(default=None, ge=0.0, le=1.0),
        positive_earnings_min_next_day_gap_pct: float | None = Query(default=None, ge=-50.0, le=50.0),
        positive_earnings_min_day_rvol: float | None = Query(default=None, ge=0.0, le=50.0),
        positive_earnings_min_return_5d_pct: float | None = Query(default=None, ge=-100.0, le=200.0),
        positive_earnings_lookback_days: int | None = Query(default=None, ge=1, le=365),
    ):
        try:
            return await resolve_service(market_name).get_scan_results(
                scan_id,
                include_sector_summaries=include_sector_summaries,
                min_liquidity_crore=min_liquidity_crore,
                expansion_min_change_pct=expansion_min_change_pct,
                expansion_min_relative_volume=expansion_min_relative_volume,
                positive_earnings_min_close_in_range_pct=positive_earnings_min_close_in_range_pct,
                positive_earnings_min_next_day_gap_pct=positive_earnings_min_next_day_gap_pct,
                positive_earnings_min_day_rvol=positive_earnings_min_day_rvol,
                positive_earnings_min_return_5d_pct=positive_earnings_min_return_5d_pct,
                positive_earnings_lookback_days=positive_earnings_lookback_days,
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown scan: {scan_id}") from error

    @router.post("/{market_name}/custom-scan")
    async def namespaced_custom_scan(
        market_name: str,
        request: CustomScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_custom_scan_results(request, include_sector_summaries=include_sector_summaries)

    @router.get("/{market_name}/gap-up-openers")
    async def namespaced_gap_up_openers(
        market_name: str,
        min_gap_pct: float = Query(default=1.0, ge=0.0),
        min_liquidity_crore: float | None = Query(default=None, ge=0.0),
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_gap_up_openers(
            min_gap_pct=min_gap_pct,
            min_liquidity_crore=min_liquidity_crore,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/{market_name}/near-pivot")
    async def namespaced_near_pivot_scan(
        market_name: str,
        request: NearPivotScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_near_pivot_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/{market_name}/pull-backs")
    async def namespaced_pull_back_scan(
        market_name: str,
        request: PullBackScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_pull_back_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/{market_name}/returns")
    async def namespaced_returns_scan(
        market_name: str,
        request: ReturnsScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_returns_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/{market_name}/consolidating")
    async def namespaced_consolidating_scan(
        market_name: str,
        request: ConsolidatingScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_consolidating_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/{market_name}/demand-zone")
    async def namespaced_demand_zone_scan(
        market_name: str,
        request: DemandZoneScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_demand_zone_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.post("/{market_name}/momentum-burst")
    async def namespaced_momentum_burst_scan(
        market_name: str,
        request: MomentumBurstScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        return await resolve_service(market_name).get_momentum_burst_scan_results(
            request=request,
            include_sector_summaries=include_sector_summaries,
        )

    @router.get("/{market_name}/improving-rs", response_model=ImprovingRsResponse)
    async def namespaced_improving_rs(
        market_name: str,
        window: ImprovingRsWindow = Query(default="1D"),
    ):
        return await resolve_service(market_name).get_improving_rs(window=window)

    @router.post("/{market_name}/refresh")
    async def namespaced_refresh_market_data(market_name: str):
        return await resolve_service(market_name).refresh_market_data()

    @router.get("/{market_name}/index-quotes", response_model=IndexQuotesResponse)
    async def namespaced_index_quotes(
        market_name: str,
        symbols: str | None = Query(default=None),
    ):
        requested_symbols = [symbol.strip() for symbol in (symbols or "").split(",") if symbol.strip()] or default_index_symbols(market_name)
        return await resolve_service(market_name).get_index_quotes(requested_symbols)

    @router.get("/{market_name}/market-overview", response_model=MarketOverviewResponse)
    async def namespaced_market_overview(market_name: str):
        return await resolve_service(market_name).get_market_overview()

    @router.get("/{market_name}/index-pe/{symbol}/history", response_model=IndexPeHistoryResponse)
    async def namespaced_index_pe_history(market_name: str, symbol: str):
        return await resolve_service(market_name).get_index_pe_history(symbol.upper())

    @router.get("/{market_name}/groups", response_model=IndustryGroupsResponse)
    async def namespaced_groups(market_name: str):
        return await resolve_service(market_name).get_industry_groups()

    @router.get("/{market_name}/watchlists", response_model=WatchlistsStateResponse)
    async def namespaced_watchlists(market_name: str):
        return resolve_service(market_name).get_watchlists_state()

    @router.put("/{market_name}/watchlists", response_model=WatchlistsStateResponse)
    async def namespaced_save_watchlists(market_name: str, payload: WatchlistsStateResponse):
        market_service = resolve_service(market_name)
        normalized = payload.model_copy(update={"market": str(market_name or "india").strip().lower()})
        return market_service.save_watchlists_state(normalized)

    @router.get("/{market_name}/bhavcopy/status", response_model=BhavcopyStatusResponse)
    async def namespaced_bhavcopy_status(market_name: str):
        return resolve_service(market_name).get_bhavcopy_status()

    @router.get("/{market_name}/chart/{symbol}", response_model=ChartResponse)
    async def namespaced_chart(
        market_name: str,
        symbol: str,
        timeframe: str = Query(default="1D"),
    ):
        return await resolve_service(market_name).get_chart(symbol=symbol.upper(), timeframe=timeframe)

    @router.get("/{market_name}/chart/{symbol}/history", response_model=ChartResponse)
    async def namespaced_chart_history(
        market_name: str,
        symbol: str,
        timeframe: str = Query(default="1D"),
    ):
        return await resolve_service(market_name).get_chart_history(symbol=symbol.upper(), timeframe=timeframe)

    @router.get("/{market_name}/chart-grid", response_model=ChartGridResponse)
    async def namespaced_chart_grid(
        market_name: str,
        name: str = Query(..., min_length=1),
        group_kind: SectorGroupKind = Query(default="sector"),
        timeframe: ChartGridTimeframe = Query(default="1Y"),
    ):
        try:
            return await resolve_service(market_name).get_chart_grid(
                name=name,
                group_kind=group_kind,
                timeframe=timeframe,
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"Unknown chart grid: {name}") from error

    @router.get("/{market_name}/chart-grid-series", response_model=ChartGridSeriesResponse)
    async def namespaced_chart_grid_series(
        market_name: str,
        symbols: str = Query(..., min_length=1),
        timeframe: ChartGridTimeframe = Query(default="1Y"),
    ):
        requested_symbols = [symbol.strip().upper() for symbol in symbols.split(",") if symbol.strip()]
        return await resolve_service(market_name).get_chart_grid_series(symbols=requested_symbols, timeframe=timeframe)

    @router.get("/{market_name}/fundamentals/{symbol}", response_model=CompanyFundamentals)
    async def namespaced_fundamentals(market_name: str, symbol: str):
        return await resolve_service(market_name).get_fundamentals(symbol=symbol.upper())

    @router.get("/{market_name}/earnings/{symbol}")
    async def namespaced_earnings(market_name: str, symbol: str):
        return await resolve_service(market_name).get_earnings_summary(symbol=symbol.upper())

    return router
