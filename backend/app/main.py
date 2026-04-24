import asyncio
import gc
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.routes import build_router
from app.core.config import get_settings
from app.providers.factory import build_provider
from app.scanners.definitions import scan_catalog_with_counts
from app.services.dashboard_service import DashboardService
from app.services.maintenance import run_market_close_maintenance


class NoCacheMiddleware(BaseHTTPMiddleware):
    """Prevent browsers from caching API responses so refreshes always get fresh data."""

    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
        return response

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
IS_HF_SPACE = bool(
    os.environ.get("IS_HF_SPACE")
    or os.environ.get("SPACE_ID")
    or os.environ.get("SPACE_REPO_NAME")
    or os.environ.get("HF_SPACE_ID")
    or os.environ.get("SYSTEM") == "spaces"
)

settings = get_settings()
india_provider = build_provider(settings, market="india")
provider = india_provider
service = DashboardService(provider=india_provider, settings=settings)
if IS_HF_SPACE:
    gc.collect()
services = {
    "india": service,
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


async def warm_startup_cache(market_name: str, service_obj) -> None:
    try:
        warm_views = getattr(service_obj, "warm_startup_views", None)
        if callable(warm_views):
            await warm_views()
        else:
            await service_obj.build_dashboard()
        logger.info("%s startup cache warm complete", market_name.upper())
    except Exception as exc:
        logger.warning("%s startup cache warm failed: %s", market_name.upper(), exc)


async def warm_startup_cache_with_timeout(market_name: str, service_obj, timeout_seconds: int) -> None:
    try:
        await asyncio.wait_for(warm_startup_cache(market_name, service_obj), timeout=timeout_seconds)
    except TimeoutError:
        logger.warning(
            "%s startup cache warm timed out after %ds; continuing with on-demand warmup",
            market_name.upper(),
            timeout_seconds,
        )
    except asyncio.CancelledError:
        logger.info("%s startup cache warm cancelled during shutdown", market_name.upper())
        raise


def schedule_startup_cache_warm(app: FastAPI, market_name: str, service_obj, timeout_seconds: int) -> None:
    startup_tasks = getattr(app.state, "startup_warm_tasks", None)
    if startup_tasks is None:
        startup_tasks = set()
        app.state.startup_warm_tasks = startup_tasks

    task = asyncio.create_task(
        warm_startup_cache_with_timeout(market_name, service_obj, timeout_seconds),
        name=f"startup-warm-{market_name}",
    )
    startup_tasks.add(task)

    def _cleanup(completed_task: asyncio.Task[None]) -> None:
        startup_tasks.discard(completed_task)
        if completed_task.cancelled():
            return
        try:
            completed_task.result()
        except Exception as exc:
            logger.warning("%s startup warm task failed unexpectedly: %s", market_name.upper(), exc)

    task.add_done_callback(_cleanup)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.startup_warm_tasks = set()
    scheduler.add_job(
        daily_listed_universe_refresh_job,
        CronTrigger(hour=16, minute=0, timezone=IST),
        args=["india", service, settings, IST],
        id="india_daily_listed_universe_refresh",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started — India refresh 4:00 PM IST")
    if settings.startup_cache_warm_enabled:
        startup_warm_timeout_seconds = min(max(settings.refresh_timeout_seconds, 15), 60)
        schedule_startup_cache_warm(app, "india", service, startup_warm_timeout_seconds)
        logger.info(
            "Startup cache warm scheduled in background with %ds timeout; serving immediately",
            startup_warm_timeout_seconds,
        )
    else:
        logger.info("Startup cache warm disabled; serving requests without boot-time cache warming")
    yield
    startup_tasks = list(getattr(app.state, "startup_warm_tasks", set()))
    for task in startup_tasks:
        task.cancel()
    if startup_tasks:
        await asyncio.gather(*startup_tasks, return_exceptions=True)
    scheduler.shutdown()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(NoCacheMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.frontend_origins,
    allow_origin_regex=settings.frontend_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(build_router(services))
