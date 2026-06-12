import asyncio
import gc
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
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


def apply_bhavcopy_patch_on_startup() -> None:
    """Roll any committed bhavcopy patch into the live snapshot before serving.

    The HF deploy excludes the heavy ``free_snapshots.json`` from the bundle,
    so cold starts begin with stale prices from the previous baked snapshot.
    Without this, dashboard / scanner endpoints serve yesterday's prices until
    the next watchdog tick. Running this once on startup keeps prices in sync
    with whatever the daily-bhavcopy GitHub Action last committed.
    """
    try:
        result = india_provider.apply_committed_bhavcopy_patch()
    except Exception as exc:
        logger.warning("Bhavcopy patch apply on startup failed: %s", exc)
        return
    if result.get("status") == "ok" and int(result.get("snapshots_updated", 0) or 0) > 0:
        try:
            service._clear_runtime_caches()
        except Exception:
            pass
        logger.info(
            "Applied bhavcopy patch on startup: date=%s symbols_updated=%s source=%s",
            result.get("date"),
            result.get("snapshots_updated"),
            result.get("source"),
        )
    else:
        logger.info(
            "Bhavcopy patch on startup status=%s reason=%s date=%s",
            result.get("status"),
            result.get("reason"),
            result.get("date"),
        )


def trigger_github_bhavcopy_if_stale() -> None:
    """Watchdog: fire the GitHub daily-bhavcopy workflow when today's EOD data
    is missing.

    GitHub's own cron is best-effort and has dropped ALL six scheduled runs on
    some days (2026-06-12), and even the keep-alive watchdog there only ticks
    every 2-5 hours under throttling. This Space runs 24/7 with a reliable
    APScheduler, so it is the dependable trigger of last resort: weekday
    evenings (IST), if the served bhavcopy date is older than today, send a
    ``repository_dispatch`` (type ``bhavcopy-trigger``) that daily-bhavcopy.yml
    already listens for. Requires a ``GITHUB_PAT`` env/secret on the Space —
    silently skipped when absent. Redundant fires are cheap: the workflow
    no-ops when the patch is already current.
    """
    import json as _json

    import requests as _requests

    try:
        now_ist = datetime.now(IST)
        if now_ist.weekday() >= 5:
            return
        today_iso = now_ist.date().isoformat()
        patch_path = Path(__file__).resolve().parents[1] / "data" / "bhavcopy_patch.json"
        try:
            patch_date = str(_json.loads(patch_path.read_text(encoding="utf-8")).get("date") or "")
        except Exception:
            patch_date = ""
        if patch_date == today_iso:
            logger.info("Bhavcopy trigger watchdog: already current for %s", today_iso)
            return

        token = (os.environ.get("GITHUB_PAT") or "").strip()
        if not token:
            logger.info(
                "Bhavcopy trigger watchdog: data stale (last=%s today=%s) but GITHUB_PAT is not set — skipping",
                patch_date or "<unknown>", today_iso,
            )
            return
        repo = os.environ.get("GITHUB_DATA_REPO", "dharmmalik002-ops/mys-screener")
        response = _requests.post(
            f"https://api.github.com/repos/{repo}/dispatches",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            json={"event_type": "bhavcopy-trigger"},
            timeout=20,
        )
        logger.info(
            "Bhavcopy trigger watchdog: data stale (last=%s today=%s) -> repository_dispatch HTTP %s",
            patch_date or "<unknown>", today_iso, response.status_code,
        )
    except Exception as exc:
        logger.warning("Bhavcopy trigger watchdog failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.startup_warm_tasks = set()
    apply_bhavcopy_patch_on_startup()

    # Multi-worker safety: only the worker that wins this filesystem lock runs
    # the scheduler. Without this, every Uvicorn worker would fire the same
    # cron jobs, doubling/tripling the daily refresh + bhavcopy work.
    scheduler_lock_path = Path("/tmp/scanner_scheduler.lock")
    is_scheduler_owner = False
    try:
        scheduler_lock_fd = os.open(
            scheduler_lock_path,
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
            0o644,
        )
        os.write(scheduler_lock_fd, str(os.getpid()).encode())
        os.close(scheduler_lock_fd)
        is_scheduler_owner = True
        app.state.scheduler_lock_path = scheduler_lock_path
    except FileExistsError:
        is_scheduler_owner = False

    if is_scheduler_owner:
        scheduler.add_job(
            apply_bhavcopy_patch_on_startup,
            CronTrigger(hour=16, minute=45, timezone=IST),
            id="india_bhavcopy_patch_apply",
            replace_existing=True,
        )
        scheduler.add_job(
            daily_listed_universe_refresh_job,
            CronTrigger(hour=16, minute=0, timezone=IST),
            args=["india", service, settings, IST],
            id="india_daily_listed_universe_refresh",
            replace_existing=True,
        )
        # Reliable evening watchdog: GitHub's cron drops runs, so this Space
        # fires the bhavcopy workflow itself whenever today's data is missing.
        # Several attempts across the evening; each no-ops once data is current.
        scheduler.add_job(
            trigger_github_bhavcopy_if_stale,
            CronTrigger(hour="16,17,18,19,21", minute=42, day_of_week="mon-fri", timezone=IST),
            id="india_bhavcopy_github_trigger",
            replace_existing=True,
        )
        scheduler.start()
        logger.info(
            "Scheduler started (pid=%s) — India refresh 4:00 PM IST, bhavcopy patch apply 4:45 PM IST",
            os.getpid(),
        )
    else:
        logger.info("Scheduler skipped on this worker (pid=%s); another worker holds the lock", os.getpid())
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
    if is_scheduler_owner:
        scheduler.shutdown()
        try:
            scheduler_lock_path.unlink(missing_ok=True)
        except OSError:
            pass


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
