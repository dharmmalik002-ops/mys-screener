import asyncio
import logging
import functools
import requests

# Apply global fallback timeout to protect HuggingFace event loop against Yahoo Finance tarpits.
original_request = requests.Session.request
@functools.wraps(original_request)
def timeout_request(self, method, url, **kwargs):
    if "timeout" not in kwargs or kwargs["timeout"] is None:
        kwargs["timeout"] = 15  # 15s global maximum timeout
    return original_request(self, method, url, **kwargs)
requests.Session.request = timeout_request
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
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
from app.services.watchdog_agent import WatchdogAgent, set_active_watchdog_agent
from app.db.neon import init_db_pool, close_db_pool


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
watchdog_agent = WatchdogAgent(
    services=services,
    settings_by_market={"india": settings, "us": us_settings},
    data_dir=Path(__file__).resolve().parents[1] / "data",
    tick_seconds=30,
)
set_active_watchdog_agent(watchdog_agent)
scheduler = AsyncIOScheduler(timezone=IST)


async def autonomous_watchdog_cycle_job() -> None:
    """Run one autonomous watchdog cycle across all registered health contracts."""
    lock_path = Path(__file__).resolve().parents[1] / "data" / "scheduled_maintenance.lock"
    import time
    if lock_path.exists() and (time.time() - lock_path.stat().st_mtime) < 7200:
        logger.info("WATCHDOG: skipping cycle — scheduled_maintenance.lock exists and is recent")
        return
    await watchdog_agent.run_cycle()


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

        # Repopulate sector-tab, scanner, and all runtime caches so that the
        # watchdog health panel shows "done" immediately after close refresh —
        # not "Needs attention" until someone visits the sector heatmap page.
        try:
            prewarm_summary = await market_service.prewarm_watchdog_sections(snapshots)
            logger.info(
                "%s post-close prewarm complete: %d sections warmed",
                market_name.upper(),
                prewarm_summary.get("section_count", 0),
            )
        except Exception as exc:
            logger.warning("%s post-close prewarm failed: %s", market_name.upper(), exc)

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


async def live_market_watchdog_job() -> None:
    """24/7 watchdog — runs every 90 seconds.

    Responsibilities:
    - During India market hours (9:15–15:30 IST Mon-Fri):
        - If snapshot is >180 s old, force a live refresh immediately.
        - Clear in-memory sector-tab/dashboard caches before refresh attempts
          so stale cached responses are not served after stale-snapshot detection,
          even if the upstream refresh fails.
    - During US market hours (9:30-16:00 ET Mon-Fri): same behavior for US.
    - After India market close on a weekday:
        • If today's Bhavcopy patch has not yet been applied → apply it now
          and rebuild the dashboard so sector heatmap shows official EOD prices.
    - Always: emit a brief health log so ops can confirm the watchdog is running.
    """
    from app.providers.free import FreeMarketDataProvider
    from datetime import date as _date, timezone as _tz, timedelta as _td

    now_utc = datetime.now(_tz.utc)
    now_ist = now_utc.astimezone(IST)
    now_et = now_utc.astimezone(ET)

    # ── India ─────────────────────────────────────────────────────────────────
    india_prov = service.provider
    if isinstance(india_prov, FreeMarketDataProvider):
        is_india_open: bool = india_prov._is_market_open_ist()
        snap_age: float = india_prov._snapshot_age_seconds()

        if is_india_open:
            if snap_age > 180:
                logger.warning(
                    "WATCHDOG INDIA: snapshot stale (%.0fs > 180s) — forcing live refresh", snap_age
                )
                try:
                    service._clear_runtime_caches()
                    india_snapshots = await india_prov.refresh_live_snapshots(settings.market_cap_min_crore)
                    prewarm_summary = await service.prewarm_watchdog_sections(india_snapshots)
                    logger.warning(
                        "WATCHDOG INDIA: live refresh OK — warmed %s sections and %s symbols, snapshot age now %.0fs",
                        prewarm_summary.get("section_count", 0),
                        prewarm_summary.get("symbol_count", 0),
                        india_prov._snapshot_age_seconds(),
                    )
                except Exception as exc:
                    logger.error("WATCHDOG INDIA: live refresh FAILED: %s", exc)
            else:
                # Even if mtime is fresh, validate that disk file has a parseable schema.
                # A fresh-mtime but corrupt/empty file won't trigger the age check above,
                # but will cause a cold-restart failure. Detect and fix proactively.
                valid_rows = await asyncio.to_thread(india_prov._load_valid_cached_snapshot_rows)
                if not valid_rows and india_prov.snapshot_cache_path.exists():
                    logger.warning(
                        "WATCHDOG INDIA: snapshot file exists but schema invalid — deleting corrupt file and rebuilding"
                    )
                    try:
                        # Delete the corrupt file so get_snapshots triggers a clean rebuild,
                        # not the complex refresh_snapshots fallback that fails on bad disk.
                        india_prov.snapshot_cache_path.unlink(missing_ok=True)
                        india_prov._snapshots_memory_cache.clear()
                        india_prov._snapshot_request_tasks.clear()
                        india_prov._live_snapshot_refresh_tasks.clear()
                        service._clear_runtime_caches()
                        await india_prov.get_snapshots(settings.market_cap_min_crore)
                        logger.warning("WATCHDOG INDIA: corrupt file deleted and snapshot rebuilt OK")
                    except Exception as exc:
                        logger.error("WATCHDOG INDIA: corrupt file rebuild FAILED: %s", exc)
                else:
                    logger.debug("WATCHDOG INDIA: snapshot fresh (%.0fs)", snap_age)
        else:
            # After close on a weekday: first sync a provisional close snapshot from
            # same-session quotes, then keep retrying the official NSE Bhavcopy once
            # the exchange publishes it later in the evening.
            ist_weekday = now_ist.weekday()  # 0=Mon … 4=Fri
            ist_tot_min = now_ist.hour * 60 + now_ist.minute
            after_close = ist_tot_min >= (15 * 60 + 35)  # after 15:35 IST
            today_ist = now_ist.date()
            if ist_weekday < 5 and after_close:
                close_refresh_due_fn = getattr(india_prov, "_market_close_refresh_due", None)
                close_refresh_due = bool(close_refresh_due_fn()) if callable(close_refresh_due_fn) else False
                if close_refresh_due:
                    try:
                        india_snapshots = await india_prov.refresh_live_snapshots(settings.market_cap_min_crore)
                        refresh_metadata = india_prov.get_last_refresh_metadata()
                        if int(refresh_metadata.get("applied_quote_count", 0) or 0) > 0:
                            service._clear_runtime_caches()
                            prewarm_summary = await service.prewarm_watchdog_sections(india_snapshots)
                            logger.info(
                                "WATCHDOG INDIA: provisional close sync OK — applied %s recent quotes and warmed %s sections",
                                refresh_metadata.get("applied_quote_count", 0),
                                prewarm_summary.get("section_count", 0),
                            )
                    except Exception as exc:
                        logger.error("WATCHDOG INDIA: provisional close sync FAILED: %s", exc)

                # BSE publishes at ~4:24 PM IST, NSE at ~6:30 PM IST.
                # Start retrying at 4:30 PM IST so BSE data is captured on the first attempt.
                first_retry_min = 16 * 60 + 30
                last_patch_date_fn = getattr(india_prov, "_last_applied_bhavcopy_date", None)
                last_patch_date = last_patch_date_fn() if callable(last_patch_date_fn) else None
                if ist_tot_min >= first_retry_min and last_patch_date != today_ist:
                    logger.info("WATCHDOG INDIA: official bhavcopy not yet applied for %s — retrying now", today_ist)
                    # Once the official bhavcopy window opens, always bust runtime
                    # caches first so stale intraday aggregates are not served all evening
                    # if upstream patch endpoints are temporarily unavailable.
                    service._clear_runtime_caches()
                    try:
                        live_patch_fn = getattr(india_prov, "apply_bhavcopy_eod", None)
                        fallback_patch_fn = getattr(india_prov, "apply_committed_bhavcopy_patch", None)
                        result = {}
                        if callable(live_patch_fn):
                            result = await asyncio.to_thread(live_patch_fn)
                        if result.get("status") != "ok" and callable(fallback_patch_fn):
                            fallback_result = await asyncio.to_thread(fallback_patch_fn)
                            if fallback_result.get("status") == "ok" or fallback_result.get("snapshots_updated", 0) > 0:
                                result = fallback_result
                        if result.get("snapshots_updated", 0) > 0:
                            service._clear_runtime_caches()
                            await service.prewarm_watchdog_sections()
                        logger.warning("WATCHDOG INDIA: bhavcopy patch result: %s", result)
                    except Exception as exc:
                        logger.error("WATCHDOG INDIA: bhavcopy patch FAILED: %s", exc)
                elif last_patch_date == today_ist:
                    logger.debug("WATCHDOG INDIA: bhavcopy already applied for %s", today_ist)

            india_money_flow_due = ist_weekday < 5 and ist_tot_min >= (18 * 60)
            if india_money_flow_due:
                try:
                    payload = await service.ensure_money_flow_stock_ideas_current()
                    if payload is not None and payload.recommendation_date == today_ist.isoformat():
                        logger.debug("WATCHDOG INDIA: money-flow stock ideas current for %s", today_ist)
                    else:
                        logger.info("WATCHDOG INDIA: money-flow stock ideas still pending for %s", today_ist)
                except Exception as exc:
                    logger.error("WATCHDOG INDIA: money-flow stock ideas refresh FAILED: %s", exc)

    # ── US ────────────────────────────────────────────────────────────────────
    us_prov = us_service.provider
    if isinstance(us_prov, FreeMarketDataProvider):
        is_us_open: bool = us_prov._is_market_open_ist()   # USFreeMarketDataProvider overrides this for ET
        us_snap_age: float = us_prov._snapshot_age_seconds()

        if is_us_open:
            if us_snap_age > 180:
                logger.warning(
                    "WATCHDOG US: snapshot stale (%.0fs > 180s) — forcing live refresh", us_snap_age
                )
                try:
                    us_service._clear_runtime_caches()
                    us_snapshots = await us_prov.refresh_live_snapshots(us_settings.market_cap_min_crore)
                    prewarm_summary = await us_service.prewarm_watchdog_sections(us_snapshots)
                    logger.warning(
                        "WATCHDOG US: live refresh OK — warmed %s sections and %s symbols, snapshot age now %.0fs",
                        prewarm_summary.get("section_count", 0),
                        prewarm_summary.get("symbol_count", 0),
                        us_prov._snapshot_age_seconds(),
                    )
                except Exception as exc:
                    logger.error("WATCHDOG US: live refresh FAILED: %s", exc)
            else:
                valid_rows_us = await asyncio.to_thread(us_prov._load_valid_cached_snapshot_rows)
                if not valid_rows_us and us_prov.snapshot_cache_path.exists():
                    logger.warning("WATCHDOG US: snapshot file exists but schema invalid — deleting corrupt file and rebuilding")
                    try:
                        us_prov.snapshot_cache_path.unlink(missing_ok=True)
                        us_prov._snapshots_memory_cache.clear()
                        us_prov._snapshot_request_tasks.clear()
                        us_prov._live_snapshot_refresh_tasks.clear()
                        us_service._clear_runtime_caches()
                        await us_prov.get_snapshots(us_settings.market_cap_min_crore)
                        logger.warning("WATCHDOG US: corrupt file deleted and snapshot rebuilt OK")
                    except Exception as exc:
                        logger.error("WATCHDOG US: corrupt file rebuild FAILED: %s", exc)
                else:
                    logger.debug("WATCHDOG US: snapshot fresh (%.0fs)", us_snap_age)
        else:
            us_weekday = now_et.weekday()
            us_tot_min = now_et.hour * 60 + now_et.minute

            # Run one post-close sync shortly after the US cash close so
            # overnight views reflect official closing session prices.
            us_after_close_window = us_weekday < 5 and (16 * 60) < us_tot_min <= (17 * 60)
            if us_after_close_window:
                us_close_refresh_due_fn = getattr(us_prov, "_market_close_refresh_due", None)
                us_close_refresh_due = bool(us_close_refresh_due_fn()) if callable(us_close_refresh_due_fn) else False
                if us_close_refresh_due:
                    try:
                        us_snapshots = await us_prov.refresh_live_snapshots(us_settings.market_cap_min_crore)
                        refresh_metadata = us_prov.get_last_refresh_metadata()
                        if int(refresh_metadata.get("applied_quote_count", 0) or 0) > 0:
                            us_service._clear_runtime_caches()
                            prewarm_summary = await us_service.prewarm_watchdog_sections(us_snapshots)
                            logger.info(
                                "WATCHDOG US: post-close EOD sync OK — applied %s recent quotes and warmed %s sections",
                                refresh_metadata.get("applied_quote_count", 0),
                                prewarm_summary.get("section_count", 0),
                            )
                    except Exception as exc:
                        logger.error("WATCHDOG US: post-close EOD sync FAILED: %s", exc)

            us_money_flow_due = us_weekday < 5 and us_tot_min >= (16 * 60 + 30)
            if us_money_flow_due:
                try:
                    payload = await us_service.ensure_money_flow_stock_ideas_current()
                    if payload is not None:
                        logger.debug("WATCHDOG US: money-flow stock ideas current for %s", payload.recommendation_date)
                except Exception as exc:
                    logger.error("WATCHDOG US: money-flow stock ideas refresh FAILED: %s", exc)

            # If startup cache warm timed out (common on HF free tier), sector tab
            # and scan catalog may be empty.  Re-warm them here during closed hours
            # so the health panel stops showing "Needs attention" even outside market
            # session.  Guard: only run if the snapshot file exists (i.e. real data).
            sector_tab_warm = bool((getattr(us_service, "_sector_tab_cache", {}) or {}).get(("1D", "desc")))
            scan_warm = getattr(us_service, "_scan_catalog_cache", None) is not None
            if us_prov.snapshot_cache_path.exists() and (not sector_tab_warm or not scan_warm):
                try:
                    await us_service.prewarm_watchdog_sections()
                    logger.info("WATCHDOG US: closed-hours cache prewarm complete")
                except Exception as exc:
                    logger.warning("WATCHDOG US: closed-hours prewarm failed: %s", exc)

    logger.debug(
        "WATCHDOG heartbeat — India open=%s age=%.0fs | US open=%s age=%.0fs",
        isinstance(india_prov, FreeMarketDataProvider) and india_prov._is_market_open_ist(),
        india_prov._snapshot_age_seconds() if isinstance(india_prov, FreeMarketDataProvider) else -1,
        isinstance(us_prov, FreeMarketDataProvider) and us_prov._is_market_open_ist(),
        us_prov._snapshot_age_seconds() if isinstance(us_prov, FreeMarketDataProvider) else -1,
    )


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
                service_obj._clear_runtime_caches()
                await service_obj.prewarm_watchdog_sections()
                logger.info("%s full EOD state rebuilt with committed Bhavcopy prices", market_name.upper())
                return  # patch applied — no need for live download

        # --- Step 2: live NSE download fallback ---
        apply_fn = getattr(provider, "apply_bhavcopy_eod", None)
        if not callable(apply_fn):
            return
        result = await asyncio.to_thread(apply_fn)
        logger.info("Startup live Bhavcopy patch (%s): %s", market_name.upper(), result)
        if result.get("snapshots_updated", 0) > 0:
            service_obj._clear_runtime_caches()
            await service_obj.prewarm_watchdog_sections()
            logger.info("%s full EOD state rebuilt with live Bhavcopy prices", market_name.upper())
    except Exception as exc:
        logger.warning("Startup Bhavcopy patch (%s) failed: %s", market_name.upper(), exc)



async def _keep_alive_self_ping() -> None:
    """Ping own health endpoint every 10 min so HF Spaces free tier doesn't
    sleep the container.  Runs as an infinite background task."""
    import httpx

    while True:
        await asyncio.sleep(600)  # 10 minutes
        try:
            async with httpx.AsyncClient() as client:
                await client.get("http://127.0.0.1:7860/api/health", timeout=5)
            logger.debug("keep-alive self-ping OK")
        except Exception:
            pass  # best-effort; failure is harmless


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    scheduler.add_job(
        autonomous_watchdog_cycle_job,
        IntervalTrigger(seconds=watchdog_agent.tick_seconds),
        id="live_market_watchdog",
        replace_existing=True,
    )
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
        "Scheduler started — Watchdog: every %ss (adaptive autonomous cycle); "
        "India: close 4:00 PM IST / stocks 6:00 PM IST / money-flow Sat 9 AM IST / fundamentals Sun 1 AM IST; "
        "US: close 4:15 PM ET / stocks 4:30 PM ET / money-flow Sat 9 AM ET / fundamentals Sun 1 AM ET",
        watchdog_agent.tick_seconds,
    )
    async def run_sequential_startup():
        try:
            # Staggered startup sequential chain — prevents GIL/CPU contention 
            # on resource-constrained hosts (HF free tier).
            import gc
            import os
            
            # Detect Hugging Face Space environment
            is_hf = os.getenv("SPACE_ID") is not None
            
            if is_hf:
                logger.warning("Hugging Face environment detected: skipping all startup warming to conserve memory. Relying on autonomous watchdog for lazy rewarming.")
                return
            
            await warm_startup_cache("india", service)
            gc.collect()
            
            await asyncio.sleep(10)  # Moderate stagger for local/heavy hosts
            await warm_startup_cache("us", us_service)
            gc.collect()
            
            await asyncio.sleep(5)
            await apply_startup_bhavcopy("india", service)
            gc.collect()
            
            logger.info("Sequential startup sequence completed successfully")
        except Exception as exc:
            logger.error("Sequential startup sequence failed: %s", exc)

    # Fire-and-forget the sequential chain so uvicorn starts accepting requests immediately.
    asyncio.ensure_future(run_sequential_startup())
    
    # Keep-alive self-ping — prevents HF Spaces free tier from sleeping
    # the container after ~15 min of inactivity.
    asyncio.ensure_future(_keep_alive_self_ping())
    yield

    scheduler.shutdown()
    await close_db_pool()


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
