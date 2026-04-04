from fastapi import APIRouter, HTTPException, Query

from app.models.market import (
    ChartGridResponse,
    ChartGridSeriesResponse,
    ChartGridTimeframe,
    ChartResponse,
    IndustryGroupsResponse,
    CompanyQuestionRequest,
    CompanyQuestionResponse,
    CompanyFundamentals,
    ConsolidatingScanRequest,
    CustomScanRequest,
    IndexPeHistoryResponse,
    IndexQuotesResponse,
    ImprovingRsResponse,
    ImprovingRsWindow,
    MarketOverviewResponse,
    MoneyFlowHistoryResponse,
    MoneyFlowReport,
    MoneyFlowStockIdeasHistoryResponse,
    MoneyFlowStockIdeasResponse,
    SectorRotationResponse,
    NearPivotScanRequest,
    PullBackScanRequest,
    ReturnsScanRequest,
    ScanDescriptor,
    SectorGroupKind,
    SectorSortBy,
    SectorTabResponse,
    MarketHealthResponse,
    HistoricalBreadthResponse,
    WatchlistsStateResponse,
)


def build_router(service):
    router = APIRouter(prefix="/api")

    def default_index_symbols(market: str | None) -> list[str]:
        normalized_market = str(market or "india").strip().lower()
        if normalized_market == "us":
            return ["^GSPC", "^IXIC", "^DJI"]
        return ["^NSEI", "^BSESN", "^NSEBANK"]

    def resolve_service(market: str):
        normalized_market = str(market or "india").strip().lower()
        if normalized_market not in service:
            raise HTTPException(status_code=400, detail=f"Unsupported market: {market}")
        return service[normalized_market]

    @router.get("/health")
    async def health():
        return {"ok": True}

    @router.get("/dashboard")
    async def dashboard(market: str = Query(default="india")):
        return await resolve_service(market).build_dashboard()

    @router.get("/market-health", response_model=MarketHealthResponse)
    async def market_health(market: str = Query(default="india")):
        return await resolve_service(market).get_market_health()

    @router.get("/market-health/history", response_model=HistoricalBreadthResponse)
    async def market_health_history(market: str = Query(default="india")):
        return resolve_service(market).get_historical_breadth()

    @router.post("/market-health/history/refresh", response_model=HistoricalBreadthResponse)
    async def refresh_market_health_history(market: str = Query(default="india")):
        return await resolve_service(market).refresh_historical_breadth_latest()

    @router.get("/scans")
    async def scans(market: str = Query(default="india")):
        dashboard = await resolve_service(market).build_dashboard()
        return dashboard.scanners

    @router.get("/scan-counts", response_model=list[ScanDescriptor])
    async def scan_counts(market: str = Query(default="india")):
        return await resolve_service(market).get_scan_counts()

    @router.get("/scans/{scan_id}")
    async def scan_results(
        scan_id: str,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
        min_liquidity_crore: float | None = Query(default=None, ge=0.0),
    ):
        try:
            return await resolve_service(market).get_scan_results(
                scan_id,
                include_sector_summaries=include_sector_summaries,
                min_liquidity_crore=min_liquidity_crore,
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

    @router.get("/sectors", response_model=SectorTabResponse)
    async def sectors(
        market: str = Query(default="india"),
        sort_by: SectorSortBy = Query(default="1M"),
        sort_order: str = Query(default="desc"),
    ):
        return await resolve_service(market).get_sector_tab(sort_by=sort_by, sort_order=sort_order)

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

    @router.get("/money-flow/history", response_model=MoneyFlowHistoryResponse)
    async def money_flow_history(market: str = Query(default="india")):
        return await resolve_service(market).get_money_flow_history()

    @router.get("/money-flow/latest", response_model=MoneyFlowReport)
    async def money_flow_latest(market: str = Query(default="india")):
        report = await resolve_service(market).get_money_flow_latest()
        if report is None:
            raise HTTPException(status_code=404, detail="No money flow reports generated yet")
        return report

    @router.post("/money-flow/generate", response_model=MoneyFlowReport)
    async def money_flow_generate(market: str = Query(default="india")):
        try:
            return await resolve_service(market).generate_and_store_money_flow()
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.get("/money-flow/stocks/latest", response_model=MoneyFlowStockIdeasResponse)
    async def money_flow_stocks_latest(market: str = Query(default="india")):
        return await resolve_service(market).get_money_flow_stock_ideas()

    @router.get("/money-flow/stocks/history", response_model=MoneyFlowStockIdeasHistoryResponse)
    async def money_flow_stocks_history(market: str = Query(default="india")):
        return await resolve_service(market).get_money_flow_stock_ideas_history()

    @router.post("/money-flow/stocks/generate", response_model=MoneyFlowStockIdeasResponse)
    async def money_flow_stocks_generate(market: str = Query(default="india")):
        try:
            return await resolve_service(market).generate_and_store_money_flow_stock_ideas(force=True)
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.post("/money-flow/stocks/ask", response_model=CompanyQuestionResponse)
    async def money_flow_stocks_ask(request: CompanyQuestionRequest, market: str = Query(default="india")):
        try:
            return await resolve_service(market).answer_company_question(
                symbol=request.symbol.upper(),
                question=request.question,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/sector-rotation", response_model=SectorRotationResponse)
    async def sector_rotation(market: str = Query(default="india")):
        return await resolve_service(market).get_sector_rotation()

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

    @router.get("/{market_name}/health")
    async def namespaced_health(market_name: str):
        resolve_service(market_name)
        return {"ok": True}

    @router.get("/{market_name}/dashboard")
    async def namespaced_dashboard(market_name: str):
        return await resolve_service(market_name).build_dashboard()

    @router.get("/{market_name}/market-health", response_model=MarketHealthResponse)
    async def namespaced_market_health(market_name: str):
        return await resolve_service(market_name).get_market_health()

    @router.get("/{market_name}/market-health/history", response_model=HistoricalBreadthResponse)
    async def namespaced_market_health_history(market_name: str):
        return resolve_service(market_name).get_historical_breadth()

    @router.post("/{market_name}/market-health/history/refresh", response_model=HistoricalBreadthResponse)
    async def namespaced_refresh_market_health_history(market_name: str):
        return await resolve_service(market_name).refresh_historical_breadth_latest()

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
    ):
        try:
            return await resolve_service(market_name).get_scan_results(
                scan_id,
                include_sector_summaries=include_sector_summaries,
                min_liquidity_crore=min_liquidity_crore,
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

    @router.get("/{market_name}/sectors", response_model=SectorTabResponse)
    async def namespaced_sectors(
        market_name: str,
        sort_by: SectorSortBy = Query(default="1M"),
        sort_order: str = Query(default="desc"),
    ):
        return await resolve_service(market_name).get_sector_tab(sort_by=sort_by, sort_order=sort_order)

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

    @router.get("/{market_name}/money-flow/history", response_model=MoneyFlowHistoryResponse)
    async def namespaced_money_flow_history(market_name: str):
        return await resolve_service(market_name).get_money_flow_history()

    @router.get("/{market_name}/money-flow/latest", response_model=MoneyFlowReport)
    async def namespaced_money_flow_latest(market_name: str):
        report = await resolve_service(market_name).get_money_flow_latest()
        if report is None:
            raise HTTPException(status_code=404, detail="No money flow reports generated yet")
        return report

    @router.post("/{market_name}/money-flow/generate", response_model=MoneyFlowReport)
    async def namespaced_money_flow_generate(market_name: str):
        try:
            return await resolve_service(market_name).generate_and_store_money_flow()
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.get("/{market_name}/money-flow/stocks/latest", response_model=MoneyFlowStockIdeasResponse)
    async def namespaced_money_flow_stocks_latest(market_name: str):
        return await resolve_service(market_name).get_money_flow_stock_ideas()

    @router.get("/{market_name}/money-flow/stocks/history", response_model=MoneyFlowStockIdeasHistoryResponse)
    async def namespaced_money_flow_stocks_history(market_name: str):
        return await resolve_service(market_name).get_money_flow_stock_ideas_history()

    @router.post("/{market_name}/money-flow/stocks/generate", response_model=MoneyFlowStockIdeasResponse)
    async def namespaced_money_flow_stocks_generate(market_name: str):
        try:
            return await resolve_service(market_name).generate_and_store_money_flow_stock_ideas(force=True)
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.post("/{market_name}/money-flow/stocks/ask", response_model=CompanyQuestionResponse)
    async def namespaced_money_flow_stocks_ask(market_name: str, request: CompanyQuestionRequest):
        try:
            return await resolve_service(market_name).answer_company_question(
                symbol=request.symbol.upper(),
                question=request.question,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/{market_name}/sector-rotation", response_model=SectorRotationResponse)
    async def namespaced_sector_rotation(market_name: str):
        return await resolve_service(market_name).get_sector_rotation()

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

    return router
