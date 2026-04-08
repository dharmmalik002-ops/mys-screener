from fastapi import APIRouter, HTTPException, Query

from pydantic import BaseModel
from app.models.market import (
    AiScanResponse,
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


class NaturalLanguageScanRequest(BaseModel):
    query: str


class KnowledgeBaseAddRequest(BaseModel):
    type: str = "text"
    title: str
    content: str
    source_url: str | None = None


class IngestUrlRequest(BaseModel):
    url: str


class AiChartBarInput(BaseModel):
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


class AiChatMessage(BaseModel):
    role: str
    content: str


class AiChartAnalysisRequest(BaseModel):
    symbol: str
    timeframe: str
    query: str
    bars: list[AiChartBarInput] = []
    conversation_history: list[AiChatMessage] = []
    include_knowledge_base: bool = True


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

    @router.post("/ai-scan", response_model=AiScanResponse)
    async def ai_scan(
        request: NaturalLanguageScanRequest,
        market: str = Query(default="india"),
        include_sector_summaries: bool = Query(default=False),
    ):
        svc = resolve_service(market)
        parsed_custom_request = await svc.provider.ai_service.parse_natural_language_scan(request.query)
        # Route to historical chart scan if a specific date or peak-volume lookback was parsed
        if parsed_custom_request.scan_date is not None or parsed_custom_request.highest_vol_lookback_days is not None:
            results = await svc.get_historical_chart_scan_results(parsed_custom_request)
        else:
            results = await svc.get_custom_scan_results(parsed_custom_request, include_sector_summaries=include_sector_summaries)
        return AiScanResponse(results=results, parsed_request=parsed_custom_request)

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

    # ── Journal Data (market-independent, shared across both markets) ──
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

    @router.post("/{market_name}/ai-scan", response_model=AiScanResponse)
    async def namespaced_ai_scan(
        market_name: str,
        request: NaturalLanguageScanRequest,
        include_sector_summaries: bool = Query(default=False),
    ):
        svc = resolve_service(market_name)
        parsed_custom_request = await svc.provider.ai_service.parse_natural_language_scan(request.query)
        if parsed_custom_request.scan_date is not None or parsed_custom_request.highest_vol_lookback_days is not None:
            results = await svc.get_historical_chart_scan_results(parsed_custom_request)
        else:
            results = await svc.get_custom_scan_results(parsed_custom_request, include_sector_summaries=include_sector_summaries)
        return AiScanResponse(results=results, parsed_request=parsed_custom_request)


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

    # ─── Knowledge Base (market-agnostic) ────────────────────────────────────

    @router.get("/knowledge-base")
    async def get_knowledge_base():
        import app.services.knowledge_base_service as kb
        return {"entries": kb.list_entries()}

    @router.post("/knowledge-base")
    async def add_knowledge_base_entry(request: KnowledgeBaseAddRequest):
        import app.services.knowledge_base_service as kb
        return kb.add_entry(request.type, request.title, request.content, request.source_url)

    @router.delete("/knowledge-base/{entry_id}")
    async def delete_knowledge_base_entry(entry_id: str):
        import app.services.knowledge_base_service as kb
        if not kb.delete_entry(entry_id):
            raise HTTPException(status_code=404, detail="Entry not found")
        return {"success": True}

    @router.post("/knowledge-base/ingest-url")
    async def ingest_url(request: IngestUrlRequest):
        import asyncio as _asyncio
        import app.services.knowledge_base_service as kb
        try:
            return await _asyncio.to_thread(kb.fetch_url_content, request.url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    # ─── AI Chart Analysis ────────────────────────────────────────────────────

    @router.post("/ai-chart-analysis")
    async def ai_chart_analysis(
        request: AiChartAnalysisRequest,
        market: str = Query(default="india"),
    ):
        import app.services.knowledge_base_service as kb
        svc = resolve_service(market)
        kb_entries = kb.get_full_entries() if request.include_knowledge_base else []
        try:
            response = await svc.provider.ai_service.analyze_chart(
                symbol=request.symbol,
                market=market,
                timeframe=request.timeframe,
                query=request.query,
                bars=[b.model_dump() for b in request.bars],
                conversation_history=[m.model_dump() for m in request.conversation_history],
                knowledge_base_entries=kb_entries,
            )
        except ValueError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"AI chart analysis unavailable: {exc}") from exc
        return {"response": response}

    # ─── Live News (RSS feed aggregator) ─────────────────────────────────────

    @router.get("/live-news")
    async def live_news(
        market: str = Query(default="india"),
        category: str | None = Query(default=None),
        limit: int = Query(default=150, le=400),
    ):
        from app.services.rss_news_service import get_rss_service
        svc = get_rss_service(market)
        items = await svc.get_all_news(limit=limit)
        if category and category.lower() != "all":
            items = [i for i in items if i.get("category", "").lower() == category.lower()]
        categories = sorted({i.get("category", "General") for i in items})
        return {"items": items, "count": len(items), "categories": categories}

    @router.get("/live-news/debug")
    async def live_news_debug(market: str = Query(default="india")):
        """Diagnostic endpoint — probe each feed and report success/failure."""
        import asyncio, time, httpx
        from app.services.rss_news_service import get_rss_service, _INDIA_FEEDS, _US_FEEDS
        feeds = _INDIA_FEEDS if market.lower() == "india" else _US_FEEDS
        results = []
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(12.0), follow_redirects=True, verify=False,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Newsdesk/2.0)"},
        ) as client:
            sem = asyncio.Semaphore(4)
            async def probe(feed):
                t0 = time.time()
                async with sem:
                    try:
                        r = await client.get(feed.url)
                        return {"feed": feed.id, "status": r.status_code, "bytes": len(r.content), "ms": int((time.time()-t0)*1000), "ok": r.status_code == 200}
                    except Exception as exc:
                        return {"feed": feed.id, "status": 0, "bytes": 0, "ms": int((time.time()-t0)*1000), "ok": False, "error": str(exc)[:200]}
            tasks = [probe(f) for f in feeds]
            results = await asyncio.gather(*tasks)
        svc = get_rss_service(market)
        return {"market": market, "feeds_probed": len(feeds), "results": results, "cache_keys": list(svc._cache.keys())}

    @router.get("/live-news/company/{symbol}")
    async def company_live_news(
        symbol: str,
        market: str = Query(default="india"),
        limit: int = Query(default=30, le=100),
    ):
        from app.services.rss_news_service import get_rss_service
        svc = get_rss_service(market)
        items = await svc.get_company_news(symbol=symbol.upper(), limit=limit)
        return {"items": items, "count": len(items)}

    @router.get("/article-proxy")
    async def article_proxy(url: str = Query(...)):
        """Fetch article HTML, inject <base> tag so relative URLs resolve
        correctly inside the iframe, and strip X-Frame-Options so the iframe
        is allowed to render it.  Only http/https URLs accepted."""
        import re as _re
        import urllib.parse as _parse
        parsed = _parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise HTTPException(status_code=400, detail="Only http/https URLs allowed")
        import urllib.request as _req
        from fastapi.responses import HTMLResponse
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
            with _req.urlopen(request_obj, timeout=15) as resp:
                html = resp.read(2_000_000).decode("utf-8", errors="replace")
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Upstream fetch failed: {exc}") from exc
        # Strip any <meta> tags that enforce X-Frame-Options or CSP
        # (browsers obey these even when the HTTP header is absent)
        html = _re.sub(
            r'<meta\s[^>]*http-equiv\s*=\s*["\']?(x-frame-options|content-security-policy)["\']?[^>]*>',
            '',
            html,
            flags=_re.IGNORECASE,
        )
        # Inject <base> tag so all relative URLs (images, CSS, JS) resolve
        # against the original article URL instead of our proxy domain.
        base_tag = f'<base href="{url}" target="_blank">'
        if _re.search(r'<head', html, _re.IGNORECASE):
            html = _re.sub(r'<head([^>]*)>', rf'<head\1>{base_tag}', html, count=1, flags=_re.IGNORECASE)
        else:
            html = base_tag + html
        return HTMLResponse(
            content=html,
            headers={
                # Omit X-Frame-Options entirely — any value other than DENY/SAMEORIGIN
                # is either ignored or causes unpredictable behaviour in Chrome.
                # Rely on CSP frame-ancestors instead.
                "Content-Security-Policy": "frame-ancestors *; default-src * 'unsafe-inline' 'unsafe-eval' data: blob:",
                "Access-Control-Allow-Origin": "*",
            },
        )

    return router
