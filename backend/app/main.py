import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware

from app.api.routes import build_router
from app.core.config import get_settings
from app.providers.factory import build_provider
from app.scanners.definitions import scan_catalog_with_counts
from app.services.maintenance import run_market_close_maintenance


class NoCacheMiddleware(BaseHTTPMiddleware):
    """Prevent browsers from caching API responses so refreshes always get fresh data."""

    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
        return response
from app.services.dashboard_service import DashboardService
from app.services.us_dashboard_service import USDashboardService

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
ET = ZoneInfo("America/New_York")

settings = get_settings()
india_provider = build_provider(settings, market="india")
us_provider = build_provider(settings, market="us")
provider = india_provider
service = DashboardService(provider=india_provider, settings=settings)
us_settings = settings.model_copy(update={"market_cap_min_crore": 0})
us_service = USDashboardService(provider=us_provider, settings=us_settings)
services = {
    "india": service,
    "us": us_service,
}
scheduler = AsyncIOScheduler(timezone=IST)


def current_weekday(market_timezone: ZoneInfo) -> int:
    return datetime.now(market_timezone).weekday()


async def daily_listed_universe_refresh_job(market_name: str, market_service: DashboardService, settings_obj, market_timezone: ZoneInfo):
    """Refresh the listed-stock universe and warm fundamentals after the local cash close."""
    from app.providers.free import FreeMarketDataProvider

    provider_obj = market_service.provider

    if not isinstance(provider_obj, FreeMarketDataProvider):
        return
    if current_weekday(market_timezone) >= 5:
        logger.info("Skipping scheduled %s listed-stock refresh: non-trading day", market_name.upper())
        return

    refresh_strategy_method = getattr(provider_obj, "preferred_refresh_strategy", None)
    refresh_strategy = refresh_strategy_method() if callable(refresh_strategy_method) else None
    if refresh_strategy != "historical":
        logger.info("Skipping scheduled %s close refresh: strategy=%s", market_name.upper(), refresh_strategy or "none")
        return

    logger.info(
        "Starting scheduled %s close maintenance for market cap >= %.0f crore",
        market_name.upper(),
        settings_obj.market_cap_min_crore,
    )
    try:
        result = await run_market_close_maintenance(market_name, market_service)
        snapshots = await provider_obj.get_snapshots(settings_obj.market_cap_min_crore)
        logger.info(
            "%s close maintenance complete: mode=%s snapshot=%s charts=%s stocks=%d",
            market_name.upper(),
            result.get("refresh_mode"),
            result.get("snapshot_updated_at"),
            result.get("prewarmed_chart_count"),
            len(snapshots),
        )

        _, scan_results = scan_catalog_with_counts(snapshots)
        logger.info(
            "%s scan warmup complete: 1M=%d hits, 5M=%d hits",
            market_name.upper(),
            len(scan_results.get("minervini-1m", [])),
            len(scan_results.get("minervini-5m", [])),
        )

        if result.get("refresh_mode") != "historical-refresh":
            logger.info("Skipping %s fundamentals warmup: snapshot already current", market_name.upper())
            return

        if not settings_obj.warm_fundamentals_after_refresh:
            logger.info("Skipping %s fundamentals warmup: disabled", market_name.upper())
            return
        if not provider_obj.ai_service.available:
            logger.info("Skipping %s fundamentals warmup: Gemini API key not configured", market_name.upper())
            return

        warmed = 0
        for snapshot in snapshots:
            try:
                await provider_obj.get_fundamentals(snapshot.symbol, snapshot=snapshot)
                warmed += 1
            except Exception as exc:
                logger.warning("Scheduled %s fundamentals refresh failed for %s: %s", market_name.upper(), snapshot.symbol, exc)

        logger.info("%s fundamentals warmup complete: %d/%d stocks", market_name.upper(), warmed, len(snapshots))
    except Exception as exc:
        logger.error("Scheduled %s listed-stock refresh failed: %s", market_name.upper(), exc)


async def weekly_bulk_fundamentals_job(market_name: str, market_service: DashboardService):
    """Refresh bulk fundamentals cache (PE/EPS/ROE screener filters) weekly.
    Fetches yfinance ticker.info for every stock in the universe and saves to
    free_fundamental_cache.json so screener filters reflect latest Q results.
    """
    from app.providers.free import FreeMarketDataProvider
    provider_obj = market_service.provider
    if not isinstance(provider_obj, FreeMarketDataProvider):
        return
    logger.info("Starting weekly %s bulk fundamentals update...", market_name.upper())
    try:
        result = await asyncio.to_thread(provider_obj.update_bulk_fundamentals_cache)
        logger.info("Weekly %s bulk fundamentals update done: %s", market_name.upper(), result)
    except Exception as exc:
        logger.error("Weekly %s bulk fundamentals update failed: %s", market_name.upper(), exc)


async def weekly_money_flow_job(market_name: str, market_service: DashboardService):
    """Generate weekly AI money flow report in the market's local schedule."""
    logger.info("Starting weekly %s money flow report generation...", market_name.upper())
    try:
        report = await market_service.ensure_money_flow_report_current()
        if report is None:
            logger.info("Weekly %s money flow report skipped: no AI-generated report available", market_name.upper())
            return
        logger.info("Weekly %s money flow report is current for week %s", market_name.upper(), report.week_key)
    except Exception as exc:
        logger.error("Weekly %s money flow job failed: %s", market_name.upper(), exc)


async def daily_money_flow_stock_job(market_name: str, market_service: DashboardService, market_timezone: ZoneInfo):
    """Generate daily Money Flow stock ideas after the local cash close."""
    if current_weekday(market_timezone) >= 5:
        logger.info("Skipping scheduled %s Money Flow stock ideas: non-trading day", market_name.upper())
        return
    logger.info("Starting daily %s Money Flow stock idea generation...", market_name.upper())
    try:
        payload = await market_service.ensure_money_flow_stock_ideas_current()
        if payload is None:
            logger.info("Daily %s Money Flow stock ideas skipped: no AI-generated payload available", market_name.upper())
            return
        logger.info(
            "Daily %s Money Flow stock ideas are current for %s with %d consolidation ideas and %d value ideas",
            market_name.upper(),
            payload.recommendation_date,
            len(payload.consolidating_ideas),
            len(payload.value_ideas),
        )
    except Exception as exc:
        logger.error("Daily %s Money Flow stock ideas job failed: %s", market_name.upper(), exc)


async def warm_startup_cache(market_name: str, service_obj) -> None:
    try:
        # Load in-memory caches from whatever data is on disk (even stale).
        # This ensures the server can respond to all endpoints immediately.
        # Never trigger a full historical data download at startup — for the US
        # market that downloads 5800+ stocks and the background thread exhausts
        # CPU/GIL, starving the asyncio event loop and hanging all requests.
        # The scheduled daily jobs (4:00 PM IST / 4:15 PM ET) handle data refresh.
        warm_views = getattr(service_obj, "warm_startup_views", None)
        if callable(warm_views):
            await warm_views()
        else:
            await service_obj.build_dashboard()
            await service_obj.get_sector_tab("1D", "desc")
        logger.info("%s startup cache warm complete", market_name.upper())
    except Exception as exc:
        logger.warning("%s startup cache warm failed: %s", market_name.upper(), exc)


async def apply_startup_bhavcopy(market_name: str, service_obj) -> None:
    """After initial warmup, apply the latest NSE Bhavcopy EOD prices so that
    chart caches and snapshot data reflect the official close — not Yahoo's
    potentially stale/adjusted values.  Only runs for the India service.

    Strategy:
    1. Try the committed bhavcopy_patch.json (fast, no network, always works).
    2. If that is already up-to-date or missing, fall back to a live NSE download.
    """
    try:
        provider = getattr(service_obj, "provider", None)

        # --- Step 1: committed patch file (no NSE network access needed) ---
        patch_fn = getattr(provider, "apply_committed_bhavcopy_patch", None)
        if callable(patch_fn):
            result = await asyncio.to_thread(patch_fn)
            logger.info("Startup committed Bhavcopy patch (%s): %s", market_name.upper(), result)
            if result.get("status") == "ok" and result.get("snapshots_updated", 0) > 0:
                await service_obj.build_dashboard()
                logger.info("%s dashboard rebuilt with committed Bhavcopy prices", market_name.upper())
                return  # patch applied — no need for live download

        # --- Step 2: live NSE download fallback ---
        apply_fn = getattr(provider, "apply_bhavcopy_eod", None)
        if not callable(apply_fn):
            return
        result = await asyncio.to_thread(apply_fn)
        logger.info("Startup live Bhavcopy patch (%s): %s", market_name.upper(), result)
        if result.get("snapshots_updated", 0) > 0:
            await service_obj.build_dashboard()
            logger.info("%s dashboard rebuilt with live Bhavcopy prices", market_name.upper())
    except Exception as exc:
        logger.warning("Startup Bhavcopy patch (%s) failed: %s", market_name.upper(), exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        daily_listed_universe_refresh_job,
        CronTrigger(hour=16, minute=0, timezone=IST),
        args=["india", service, settings, IST],
        id="india_daily_listed_universe_refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        weekly_money_flow_job,
        CronTrigger(day_of_week="sat", hour=9, minute=0, timezone=IST),
        args=["india", service],
        id="india_weekly_money_flow",
        replace_existing=True,
    )
    scheduler.add_job(
        weekly_bulk_fundamentals_job,
        CronTrigger(day_of_week="sun", hour=1, minute=0, timezone=IST),
        args=["india", service],
        id="india_weekly_bulk_fundamentals",
        replace_existing=True,
    )
    scheduler.add_job(
        daily_money_flow_stock_job,
        CronTrigger(day_of_week="mon-fri", hour=18, minute=0, timezone=IST),
        args=["india", service, IST],
        id="india_daily_money_flow_stocks",
        replace_existing=True,
    )
    scheduler.add_job(
        daily_listed_universe_refresh_job,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=15, timezone=ET),
        args=["us", us_service, us_settings, ET],
        id="us_daily_listed_universe_refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        weekly_money_flow_job,
        CronTrigger(day_of_week="sat", hour=9, minute=0, timezone=ET),
        args=["us", us_service],
        id="us_weekly_money_flow",
        replace_existing=True,
    )
    scheduler.add_job(
        weekly_bulk_fundamentals_job,
        CronTrigger(day_of_week="sun", hour=1, minute=0, timezone=ET),
        args=["us", us_service],
        id="us_weekly_bulk_fundamentals",
        replace_existing=True,
    )
    scheduler.add_job(
        daily_money_flow_stock_job,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=30, timezone=ET),
        args=["us", us_service, ET],
        id="us_daily_money_flow_stocks",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Scheduler started — India: close 4:00 PM IST / stocks 6:00 PM IST / money-flow Sat 9 AM IST / fundamentals Sun 1 AM IST; "
        "US: close 4:15 PM ET / stocks 4:30 PM ET / money-flow Sat 9 AM ET / fundamentals Sun 1 AM ET"
    )
    startup_warm_timeout_seconds = min(max(settings.refresh_timeout_seconds, 15), 60)
    try:
        await asyncio.wait_for(
            asyncio.gather(
                warm_startup_cache("india", service),
                warm_startup_cache("us", us_service),
            ),
            timeout=startup_warm_timeout_seconds,
        )
    except TimeoutError:
        logger.warning("Startup cache warm timed out after %ds; serving with on-demand warmup", startup_warm_timeout_seconds)
    # Apply NSE Bhavcopy EOD prices on top of Yahoo data — fire-and-forget,
    # runs in the background so it doesn't block the server from accepting requests.
    asyncio.ensure_future(apply_startup_bhavcopy("india", service))
    yield
    scheduler.shutdown()


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.exception_handler(Exception)
async def _global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Convert any unhandled server exception to 503 so the frontend retries
    instead of surfacing an opaque 'Request failed: 500' to the user."""
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=503,
        content={"detail": f"Service temporarily unavailable: {type(exc).__name__}"},
    )


app.add_middleware(GZipMiddleware, minimum_size=1000)  # compress any response ≥ 1KB
app.add_middleware(NoCacheMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.frontend_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(build_router(services))
