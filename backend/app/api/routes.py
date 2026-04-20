import asyncio
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

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
    CustomScanRequest,
    EandCScanRequest,
    EandCScanResponse,
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

    @router.get("/watchdog-status")
    async def watchdog_status(market: str = Query(default="india")):
        """Returns real-time data-freshness diagnostics so you can verify the watchdog is working."""
        from datetime import timezone as _tz
        from app.providers.free import FreeMarketDataProvider

        svc = resolve_service(market)
        prov = svc.provider
        now_utc = __import__("datetime").datetime.now(_tz.utc)

        if not isinstance(prov, FreeMarketDataProvider):
            return {"market": market, "provider": type(prov).__name__, "watchdog": "n/a"}

        watchdog_agent = None
        try:
            from app.services.watchdog_agent import get_active_watchdog_agent

            watchdog_agent = get_active_watchdog_agent()
        except Exception:
            watchdog_agent = None

        snap_age = prov._snapshot_age_seconds()
        is_open = prov._is_market_open_ist()
        snap_updated = prov.get_snapshot_updated_at()
        cached_rows = prov._load_valid_cached_snapshot_rows()
        snapshot_session_date = prov._snapshot_rows_session_date(cached_rows) if cached_rows else None
        latest_session_method = getattr(prov, "_latest_completed_market_session_date", None)
        expected_session_date = latest_session_method() if callable(latest_session_method) else None
        close_refresh_due_method = getattr(prov, "_market_close_refresh_due", None)
        close_refresh_due = bool(close_refresh_due_method()) if callable(close_refresh_due_method) else False

        snapshot_stale = bool(
            (is_open and snap_age > 180)
            or close_refresh_due
            or (snapshot_session_date is None and expected_session_date is not None)
            or (
                snapshot_session_date is not None
                and expected_session_date is not None
                and snapshot_session_date < expected_session_date
            )
        )

        def generated_at_from_cache(value) -> datetime | None:
            if value is None:
                return None
            if isinstance(value, tuple):
                for item in value:
                    if isinstance(item, datetime):
                        return item
                return None
            for attr in ("generated_at", "updated_at"):
                candidate = getattr(value, attr, None)
                if isinstance(candidate, datetime):
                    return candidate
            return None

        site_sections = {
            "dashboard": generated_at_from_cache(getattr(svc, "_dashboard_cache", None)),
            "sectors": generated_at_from_cache((getattr(svc, "_sector_tab_cache", {}) or {}).get(("1D", "desc"))),
            "screeners": generated_at_from_cache(getattr(svc, "_scan_catalog_cache", None)),
            "groups": generated_at_from_cache(getattr(svc, "_industry_groups_cache", None)),
            "market_health": generated_at_from_cache(getattr(svc, "_market_health_cache", None)),
        }
        attention_items = [name for name, generated_at in site_sections.items() if generated_at is None]

        return {
            "market": market,
            "is_market_open": is_open,
            "snapshot_age_seconds": round(snap_age, 1),
            "snapshot_updated_at": snap_updated.isoformat() if snap_updated else None,
            "snapshot_stale": snapshot_stale,
            "snapshot_session_date": snapshot_session_date.isoformat() if snapshot_session_date else None,
            "expected_session_date": expected_session_date.isoformat() if expected_session_date else None,
            "close_refresh_due": close_refresh_due,
            "site_systems_total": len(site_sections),
            "site_systems_ready": len(site_sections) - len(attention_items),
            "site_attention_count": len(attention_items),
            "attention_items": attention_items,
            "watchdog_interval_seconds": getattr(watchdog_agent, "tick_seconds", 90),
            "server_utc": now_utc.isoformat(),
        }

    @router.get("/watchdog-events")
    async def watchdog_events(market: str = Query(default="india")):
        """Server-sent stream of watchdog invalidation and refresh events."""
        try:
            from app.services.watchdog_agent import get_active_watchdog_agent

            watchdog_agent = get_active_watchdog_agent()
        except Exception:
            watchdog_agent = None

        if watchdog_agent is None:
            raise HTTPException(status_code=503, detail="Watchdog event bus is not available")

        normalized_market = str(market or "india").strip().lower()
        queue = await watchdog_agent.signal_bus.subscribe(normalized_market)

        async def event_stream():
            try:
                while True:
                    try:
                        message = await asyncio.wait_for(queue.get(), timeout=25)
                        yield f"data: {message}\n\n"
                    except TimeoutError:
                        heartbeat = {
                            "event": "heartbeat",
                            "market": normalized_market,
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                        yield f"data: {json.dumps(heartbeat)}\n\n"
            except asyncio.CancelledError:
                return
            finally:
                await watchdog_agent.signal_bus.unsubscribe(queue)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @router.get("/watchdog-audit")
    async def watchdog_audit(
        component: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        """Recent watchdog decision log for debugging and health transparency."""
        try:
            from app.services.watchdog_agent import get_active_watchdog_agent

            watchdog_agent = get_active_watchdog_agent()
        except Exception:
            watchdog_agent = None

        if watchdog_agent is None:
            raise HTTPException(status_code=503, detail="Watchdog audit log is not available")

        return {
            "entries": watchdog_agent.recent_audit(component=component, limit=limit),
            "tick_seconds": watchdog_agent.tick_seconds,
        }

    @router.get("/watchdog-tasks")
    async def watchdog_tasks(market: str = Query(default="india")):
        """Return today's watchdog schedule with done/pending/attention details for the home-page popup."""
        from app.providers.free import FreeMarketDataProvider
        try:
            from app.services.watchdog_agent import get_active_watchdog_agent

            watchdog_agent = get_active_watchdog_agent()
        except Exception:
            watchdog_agent = None

        normalized_market = str(market or "india").strip().lower()
        svc = resolve_service(normalized_market)
        provider_obj = svc.provider
        now_utc = datetime.now(timezone.utc)
        local_tz = ZoneInfo("America/New_York") if normalized_market == "us" else ZoneInfo("Asia/Kolkata")
        tz_label = "ET" if normalized_market == "us" else "IST"
        now_local = now_utc.astimezone(local_tz)
        is_trading_day = now_local.weekday() < 5
        day_key = now_local.date().isoformat()
        next_reset_at = (
            now_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        ).isoformat()

        def format_local_clock(value: datetime | None) -> str | None:
            if value is None:
                return None
            return value.astimezone(local_tz).strftime("%I:%M %p").lstrip("0")

        def iso_or_none(value) -> str | None:
            if value is None:
                return None
            return value.isoformat() if hasattr(value, "isoformat") else str(value)

        def age_label(seconds) -> str:
            if not isinstance(seconds, (int, float)) or seconds != seconds or seconds == float("inf"):
                return "unknown age"
            whole_seconds = max(0, int(seconds))
            return f"{whole_seconds}s" if whole_seconds < 3600 else f"{round(whole_seconds / 60)}m"

        def task_item(
            task_id: str,
            title: str,
            schedule: str,
            status: str,
            detail: str,
            last_event_at: str | None = None,
            done_today: bool = False,
            task_source: str = "backend scheduler",
        ) -> dict:
            return {
                "id": task_id,
                "title": title,
                "source": task_source,
                "schedule": schedule,
                "status": status,
                "detail": detail,
                "last_event_at": last_event_at,
                "done_today": done_today,
            }

        if not isinstance(provider_obj, FreeMarketDataProvider):
            return {
                "market": normalized_market,
                "local_timezone": tz_label,
                "local_time": now_local.isoformat(),
                "day_key": day_key,
                "next_reset_at": next_reset_at,
                "tasks": [
                    task_item(
                        "watchdog_unavailable",
                        "Watchdog task board",
                        "Not available",
                        "attention",
                        f"Detailed task tracking is unavailable for provider {type(provider_obj).__name__}.",
                    )
                ],
            }

        snapshot_updated = provider_obj.get_snapshot_updated_at()
        snapshot_updated_local = snapshot_updated.astimezone(local_tz) if snapshot_updated else None
        snapshot_age_seconds = provider_obj._snapshot_age_seconds()
        snapshot_age_text = age_label(snapshot_age_seconds)
        is_market_open = bool(provider_obj._is_market_open_ist())
        close_refresh_due_method = getattr(provider_obj, "_market_close_refresh_due", None)
        close_refresh_due = bool(close_refresh_due_method()) if callable(close_refresh_due_method) else False
        snapshot_stale = bool((is_market_open and snapshot_age_seconds > 180) or close_refresh_due)

        if snapshot_updated is None:
            live_status = "attention"
            live_detail = "No usable snapshot timestamp is available yet, so the watchdog is waiting for the first healthy refresh."
        elif is_market_open and isinstance(snapshot_age_seconds, (int, float)) and snapshot_age_seconds > 180:
            live_status = "attention"
            live_detail = f"Live snapshot is {snapshot_age_text} old, so the watchdog is trying to force a refresh."
        elif is_market_open:
            live_status = "done"
            live_detail = f"Live monitoring is active and the latest snapshot is {snapshot_age_text} old."
        else:
            live_status = "done"
            live_detail = f"Market is closed; watchdog is idle but healthy. Latest snapshot age is {snapshot_age_text}."

        tasks = [
            task_item(
                "live_market_watchdog",
                "Live market watchdog",
                f"Every {getattr(watchdog_agent, 'tick_seconds', 90)} seconds",
                live_status,
                live_detail,
                iso_or_none(snapshot_updated),
                done_today=live_status == "done",
            ),
            task_item(
                "backend_keep_alive",
                "Keep backend warm",
                "Every 5 minutes",
                "done",
                "Backend is responding right now, so the keep-alive ping path is currently healthy.",
                now_utc.isoformat(),
                done_today=True,
                task_source="GitHub Actions",
            ),
        ]

        close_hour, close_minute = ((16, 15) if normalized_market == "us" else (16, 0))
        close_run = now_local.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
        close_schedule = f"Mon–Fri {close_run.strftime('%I:%M %p').lstrip('0')} {tz_label}"
        close_ready_cutoff = close_run - timedelta(minutes=45)

        if not is_trading_day:
            close_status = "scheduled"
            close_detail = "Runs only on trading days, so it will wait for the next market session."
            close_done_today = False
        elif now_local < close_run:
            close_status = "scheduled"
            close_detail = f"Scheduled for later today at {close_run.strftime('%I:%M %p').lstrip('0')} {tz_label}."
            close_done_today = False
        elif snapshot_updated_local and snapshot_updated_local.date() == now_local.date() and snapshot_updated_local >= close_ready_cutoff:
            close_status = "done"
            close_detail = (
                f"Today's fast close snapshot is available from {format_local_clock(snapshot_updated_local)} {tz_label}. "
                f"The official NSE patch may still refresh it later if the exchange file arrives afterward."
            )
            close_done_today = True
        else:
            close_status = "attention"
            close_detail = "It is past the scheduled close refresh time, but today's refreshed close snapshot is not confirmed yet."
            close_done_today = False

        tasks.append(
            task_item(
                "daily_close_refresh",
                "Daily close snapshot refresh",
                close_schedule,
                close_status,
                close_detail,
                iso_or_none(snapshot_updated),
                done_today=close_done_today,
            )
        )

        stock_hour, stock_minute = ((16, 30) if normalized_market == "us" else (18, 0))
        stock_run = now_local.replace(hour=stock_hour, minute=stock_minute, second=0, microsecond=0)
        stock_schedule = f"Mon–Fri {stock_run.strftime('%I:%M %p').lstrip('0')} {tz_label}"

        try:
            stock_payload = await svc.get_money_flow_stock_ideas()
        except Exception:
            stock_payload = None

        stock_recommendation_date = str(getattr(stock_payload, "recommendation_date", "") or "")
        stock_generated_at = getattr(stock_payload, "generated_at", None)
        stock_idea_count = len(getattr(stock_payload, "consolidating_ideas", []) or []) + len(getattr(stock_payload, "value_ideas", []) or [])

        if not is_trading_day:
            stock_status = "scheduled"
            stock_detail = "Runs only on trading days, so stock ideas will refresh on the next market day."
            stock_done_today = False
        elif now_local < stock_run:
            stock_status = "scheduled"
            stock_detail = f"Scheduled for later today at {stock_run.strftime('%I:%M %p').lstrip('0')} {tz_label}."
            stock_done_today = False
        elif stock_recommendation_date == day_key and stock_generated_at is not None:
            stock_status = "done"
            stock_detail = f"Today's money-flow stock ideas refresh completed with {stock_idea_count} idea entries."
            stock_done_today = True
        else:
            stock_status = "attention"
            stock_detail = "It is past the scheduled time, but today's money-flow stock ideas are not stored yet. The watchdog will retry automatically."
            stock_done_today = False

        tasks.append(
            task_item(
                "daily_money_flow_stocks",
                "Daily money-flow stock ideas",
                stock_schedule,
                stock_status,
                stock_detail,
                iso_or_none(stock_generated_at),
                done_today=stock_done_today,
            )
        )

        if normalized_market == "india":
            last_patch_date_fn = getattr(provider_obj, "_last_applied_bhavcopy_date", None)
            last_patch_date = last_patch_date_fn() if callable(last_patch_date_fn) else None
            first_retry = now_local.replace(hour=17, minute=40, second=0, microsecond=0)
            final_retry = now_local.replace(hour=18, minute=55, second=0, microsecond=0)
            if not is_trading_day:
                bhavcopy_status = "scheduled"
                bhavcopy_detail = "Official NSE Bhavcopy checks run only on trading days."
                bhavcopy_done_today = False
            elif now_local < first_retry:
                bhavcopy_status = "scheduled"
                bhavcopy_detail = "The fast close snapshot is already used first; official NSE Bhavcopy retries begin at 5:40 PM IST."
                bhavcopy_done_today = False
            elif last_patch_date == now_local.date():
                bhavcopy_status = "done"
                bhavcopy_detail = f"Official NSE Bhavcopy has already been applied for {day_key}."
                bhavcopy_done_today = True
            elif now_local < final_retry:
                bhavcopy_status = "scheduled"
                bhavcopy_detail = "Waiting for the official NSE file or the next evening retry slot; provisional close prices stay active until then."
                bhavcopy_done_today = False
            else:
                bhavcopy_status = "attention"
                bhavcopy_detail = "Past the final retry window, but today's official NSE Bhavcopy is still not confirmed."
                bhavcopy_done_today = False

            tasks.append(
                task_item(
                    "nse_bhavcopy_patch",
                    "Official NSE close-data patch",
                    "Mon–Fri 5:40 / 5:55 / 6:10 / 6:25 / 6:40 / 6:55 PM IST",
                    bhavcopy_status,
                    bhavcopy_detail,
                    iso_or_none(snapshot_updated),
                    done_today=bhavcopy_done_today,
                    task_source="GitHub Actions + backend patch",
                )
            )

        def generated_at_from_cache(value) -> datetime | None:
            if value is None:
                return None
            if isinstance(value, tuple):
                for item in value:
                    if isinstance(item, datetime):
                        return item
                return None
            for attr in ("generated_at", "updated_at"):
                candidate = getattr(value, attr, None)
                if isinstance(candidate, datetime):
                    return candidate
            return None

        def add_runtime_task(
            task_id: str,
            title: str,
            schedule: str,
            generated_at: datetime | None,
            *,
            source: str = "full-site watchdog",
            on_demand: bool = False,
            missing_detail: str,
        ) -> None:
            if generated_at is None:
                status = "scheduled" if on_demand else ("attention" if snapshot_stale else "scheduled")
                detail = missing_detail
                done_today = False
            else:
                behind_latest_snapshot = snapshot_updated is not None and generated_at + timedelta(minutes=5) < snapshot_updated
                if behind_latest_snapshot:
                    status = "attention"
                    detail = f"Last warmed at {format_local_clock(generated_at)} {tz_label}, but it is behind the latest market snapshot and will be rebuilt automatically."
                    done_today = False
                else:
                    status = "done"
                    detail = f"Warm and synced from {format_local_clock(generated_at)} {tz_label}."
                    done_today = generated_at.astimezone(local_tz).date().isoformat() == day_key
            tasks.append(
                task_item(
                    task_id,
                    title,
                    schedule,
                    status,
                    detail,
                    iso_or_none(generated_at),
                    done_today=done_today,
                    task_source=source,
                )
            )

        add_runtime_task(
            "dashboard_home_cache",
            "Home dashboard and movers",
            "Every live refresh + self-heal",
            generated_at_from_cache(getattr(svc, "_dashboard_cache", None)),
            missing_detail="The home dashboard cache has not been prewarmed yet. The broader watchdog will rebuild it automatically.",
        )
        add_runtime_task(
            "sector_heatmap_sync",
            "Sector heatmap and leaders",
            "Every live refresh + self-heal",
            generated_at_from_cache((getattr(svc, "_sector_tab_cache", {}) or {}).get(("1D", "desc"))),
            missing_detail="The sector heatmap cache is not warm yet, so the watchdog will repopulate it on the next cycle.",
        )
        add_runtime_task(
            "scanner_engine_sync",
            "Screener engine and counts",
            "Every live refresh + self-heal",
            generated_at_from_cache(getattr(svc, "_scan_catalog_cache", None)),
            missing_detail="Scanner results are recalculated from the latest snapshot and will be warmed automatically.",
        )
        add_runtime_task(
            "groups_leadership_sync",
            "Groups and leadership view",
            "Every live refresh + on demand",
            generated_at_from_cache(getattr(svc, "_industry_groups_cache", None)),
            on_demand=True,
            missing_detail="Groups and leadership data stays on-demand, but the full-site watchdog can warm it automatically during self-heal.",
        )
        add_runtime_task(
            "market_health_sync",
            "Market health and breadth",
            "Every live refresh + on demand",
            generated_at_from_cache(getattr(svc, "_market_health_cache", None)),
            on_demand=True,
            missing_detail="Market-health breadth data will refresh automatically when its cache is missing or behind.",
        )

        fundamentals_cache_path = getattr(provider_obj, "fundamentals_cache_path", None)
        fundamentals_generated_at = None
        if fundamentals_cache_path is not None and getattr(fundamentals_cache_path, "exists", lambda: False)():
            fundamentals_generated_at = datetime.fromtimestamp(fundamentals_cache_path.stat().st_mtime, tz=timezone.utc)
        add_runtime_task(
            "fundamentals_earnings_sync",
            "Fundamentals, news, and earnings dates",
            "On demand + 6h refresh window",
            fundamentals_generated_at,
            on_demand=True,
            missing_detail="Deep fundamentals and earnings data refresh on demand and are now included in the broader watchdog self-heal path.",
        )

        chart_cache_dir = getattr(provider_obj, "chart_cache_dir", None)
        chart_generated_at = None
        if chart_cache_dir is not None and getattr(chart_cache_dir, "exists", lambda: False)():
            try:
                chart_mtimes = [item.stat().st_mtime for item in chart_cache_dir.glob("*.json")]
            except Exception:
                chart_mtimes = []
            if chart_mtimes:
                chart_generated_at = datetime.fromtimestamp(max(chart_mtimes), tz=timezone.utc)
        add_runtime_task(
            "charts_prefetch_sync",
            "Charts and background prefetch",
            "On demand + watchdog prewarm",
            chart_generated_at,
            on_demand=True,
            missing_detail="Charts stay partly on demand, but the broader watchdog now prewarms hot symbols after refreshes.",
        )

        return {
            "market": normalized_market,
            "local_timezone": tz_label,
            "local_time": now_local.isoformat(),
            "day_key": day_key,
            "next_reset_at": next_reset_at,
            "tasks": tasks,
        }

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

    @router.post("/watchdog/fix")
    async def watchdog_fix(market: str = Query(default="india")):
        return await resolve_service(market).run_watchdog_fix()

    @router.post("/apply-bhavcopy-eod")
    async def apply_bhavcopy_eod():
        """Apply today's NSE Bhavcopy (official EOD data) to all chart caches.
        Call this after 6:30 PM IST on any trading day to get correct EOD prices.
        """
        return await asyncio.to_thread(resolve_service("india").apply_bhavcopy_eod)

    @router.post("/maintenance/eod-refresh")
    async def maintenance_eod_refresh(
        request: Request,
        market: str = Query(default="india"),
    ):
        from app.core.config import get_settings
        settings_cache = get_settings()
        
        token = request.headers.get("x-maintenance-token")
        if not token or token != settings_cache.maintenance_trigger_token:
            # We return 503 exactly as described so it alerts the user if missing
            raise HTTPException(status_code=503, detail="Maintenance token not configured or incorrect")
        
        svc = resolve_service(market)
        prov = svc.provider
        
        # 1. Apply committed patch first (as the cron has likely already pushed it)
        try:
            patch_fn = getattr(prov, "apply_committed_bhavcopy_patch", None)
            if callable(patch_fn):
                result = await asyncio.to_thread(patch_fn)
                if result.get("status") != "ok" or result.get("snapshots_updated", 0) == 0:
                    live_fn = getattr(prov, "apply_bhavcopy_eod", None)
                    if callable(live_fn):
                        await asyncio.to_thread(live_fn)
            else:
                live_fn = getattr(prov, "apply_bhavcopy_eod", None)
                if callable(live_fn):
                    await asyncio.to_thread(live_fn)
                    
            from app.models.market import StockSnapshot
            snaps: list[StockSnapshot] = await prov.get_snapshots(getattr(settings_cache, "market_cap_min_crore", 0))
        except Exception as e:
             raise HTTPException(status_code=500, detail=str(e))
             
        # 2. Rebuild caches
        svc._clear_runtime_caches()
        summary = await svc.prewarm_watchdog_sections(snaps)
        
        return {"status": "ok", "prewarmed": summary}

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
        return await resolve_service(market).get_watchlists_state()

    @router.put("/watchlists", response_model=WatchlistsStateResponse)
    async def save_watchlists(payload: WatchlistsStateResponse, market: str = Query(default="india")):
        market_service = resolve_service(market)
        normalized = payload.model_copy(update={"market": str(market or "india").strip().lower()})
        return await market_service.save_watchlists_state(normalized)

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

    @router.get("/{market_name}/e-and-c", response_model=EandCScanResponse)
    async def namespaced_e_and_c_scan(
        market_name: str,
        request: EandCScanRequest = Depends(),
    ):
        return await resolve_service(market_name).get_e_and_c_scan_results(request=request)

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

    @router.post("/{market_name}/watchdog/fix")
    async def namespaced_watchdog_fix(market_name: str):
        return await resolve_service(market_name).run_watchdog_fix()

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
