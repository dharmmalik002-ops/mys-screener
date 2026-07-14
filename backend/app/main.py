import asyncio
import gc
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
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
        if request.url.path.startswith("/api/"):
            # Self-heal stale EOD data on live traffic — see maybe_self_heal_bhavcopy.
            try:
                maybe_self_heal_bhavcopy()
            except Exception:
                pass
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


def pull_latest_bhavcopy_patch() -> bool:
    """Self-heal: fetch the newest committed bhavcopy patch from the public
    GitHub raw URL and overwrite the local copy when it is for a newer date.

    Daily price commits use ``[skip ci]`` and reach this Space only through the
    workflow's HF push, which can silently fail (expired HF token, dropped run).
    When that happens the running backend stays frozen on the last deployed
    snapshot and every derived number — advance/decline, per-stock %, XP breadth,
    scanners — serves *yesterday's* data. Pulling the committed patch directly
    here (the repo is public, so no token is needed) lets the Space catch up on
    its own each day, with zero redeploy. Returns True when a newer patch was
    written.
    """
    import json as _json

    import requests as _requests

    url = (
        os.environ.get("BHAVCOPY_PATCH_URL")
        or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data/bhavcopy_patch.json"
    ).strip()
    patch_path = Path(__file__).resolve().parents[1] / "data" / "bhavcopy_patch.json"
    try:
        local_date = ""
        try:
            local_date = str(_json.loads(patch_path.read_text(encoding="utf-8")).get("date") or "")
        except Exception:
            local_date = ""
        resp = _requests.get(url, timeout=25)
        if resp.status_code != 200:
            logger.info("Bhavcopy self-update: remote HTTP %s — keeping local %s", resp.status_code, local_date or "<none>")
            return False
        remote = resp.json()
        remote_date = str(remote.get("date") or "")
        symbols = remote.get("symbols")
        if not remote_date or not isinstance(symbols, dict) or not symbols:
            logger.warning("Bhavcopy self-update: remote payload invalid (date=%s) — keeping local", remote_date or "<none>")
            return False
        if local_date and remote_date <= local_date:
            return False  # already current (or older) — nothing to do
        patch_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = patch_path.with_name(patch_path.name + ".tmp")
        tmp.write_text(resp.text, encoding="utf-8")
        tmp.replace(patch_path)
        logger.info(
            "Bhavcopy self-update: pulled %s (was %s, %s symbols) from remote",
            remote_date, local_date or "<none>", len(symbols),
        )
        return True
    except Exception as exc:
        logger.warning("Bhavcopy self-update failed: %s", exc)
        return False


def pull_latest_xp_breadth() -> bool:
    """Pull the committed XP market-breadth history from the public repo.

    The XP score is read from ``xp_breadth_history.json`` (written daily by the
    bhavcopy job). Like the price patch, that file only reaches this Space via
    the workflow's HF push, which can silently fail — leaving the breadth widget
    frozen on an earlier day even when the snapshot itself has self-healed.
    Pulling it directly (public repo, no token) keeps the score in lockstep with
    the daily data. Returns True when a newer file was written.
    """
    import json as _json

    import requests as _requests

    def _latest_date(doc: object) -> str:
        if not isinstance(doc, dict):
            return ""
        latest = doc.get("latest")
        if isinstance(latest, dict) and latest.get("date"):
            return str(latest.get("date"))
        days = doc.get("days")
        if isinstance(days, list) and days and isinstance(days[-1], dict):
            return str(days[-1].get("date") or "")
        return ""

    url = (
        os.environ.get("XP_BREADTH_HISTORY_URL")
        or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data/xp_breadth_history.json"
    ).strip()
    path = Path(__file__).resolve().parents[1] / "data" / "xp_breadth_history.json"
    try:
        local_date = ""
        try:
            local_date = _latest_date(_json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            local_date = ""
        resp = _requests.get(url, timeout=25)
        if resp.status_code != 200:
            logger.info("XP breadth self-update: remote HTTP %s — keeping local %s", resp.status_code, local_date or "<none>")
            return False
        remote = resp.json()
        remote_date = _latest_date(remote)
        days = remote.get("days") if isinstance(remote, dict) else None
        if not remote_date or not isinstance(days, list) or not days:
            logger.warning("XP breadth self-update: remote payload invalid — keeping local")
            return False
        if local_date and remote_date <= local_date:
            return False  # already current (or older) — nothing to do
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(resp.text, encoding="utf-8")
        tmp.replace(path)
        logger.info("XP breadth self-update: pulled %s (was %s) from remote", remote_date, local_date or "<none>")
        return True
    except Exception as exc:
        logger.warning("XP breadth self-update failed: %s", exc)
        return False


def pull_latest_price_bands() -> bool:
    """Pull the committed circuit-limit files from the public repo.

    ``price_bands.json`` (current band per symbol) and
    ``price_band_changes.json`` (revision history for chart markers) are
    rewritten by the daily workflow and committed with ``[skip ci]`` — exactly
    like the price patch, they only reach this Space via the workflow's HF
    push, which can silently fail. Without this pull, charts keep showing the
    circuit limits that were baked in at the last full deploy. Both readers in
    dashboard_service are mtime-cached, so replacing the files is enough for
    the next request to serve fresh bands. Returns True when either file
    advanced.
    """
    import json as _json

    import requests as _requests

    raw_base = (
        os.environ.get("COMMITTED_DATA_RAW_BASE")
        or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data"
    ).strip().rstrip("/")
    data_dir = Path(__file__).resolve().parents[1] / "data"
    updated = False
    for name, payload_key in (("price_bands.json", "bands"), ("price_band_changes.json", "changes")):
        path = data_dir / name
        try:
            local_stamp = ""
            try:
                local_stamp = str(_json.loads(path.read_text(encoding="utf-8")).get("updated_at") or "")
            except Exception:
                local_stamp = ""
            resp = _requests.get(f"{raw_base}/{name}", timeout=25)
            if resp.status_code != 200:
                logger.info("Price-band self-update (%s): remote HTTP %s — keeping local", name, resp.status_code)
                continue
            remote = resp.json()
            remote_stamp = str(remote.get("updated_at") or "") if isinstance(remote, dict) else ""
            payload = remote.get(payload_key) if isinstance(remote, dict) else None
            if not remote_stamp or not isinstance(payload, dict) or not payload:
                logger.warning("Price-band self-update (%s): remote payload invalid — keeping local", name)
                continue
            if local_stamp and remote_stamp <= local_stamp:
                continue  # already current (or older) — nothing to do
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(resp.text, encoding="utf-8")
            tmp.replace(path)
            logger.info("Price-band self-update (%s): pulled %s (was %s)", name, remote_stamp, local_stamp or "<none>")
            updated = True
        except Exception as exc:
            logger.warning("Price-band self-update (%s) failed: %s", name, exc)
    return updated


def pull_latest_eod_bars() -> bool:
    """Pull the rolling authoritative EOD bars from the public repo.

    ``eod_bars/manifest.json`` lists the last ~12 committed session dates and
    each ``eod_bars/<date>.json`` holds that session's per-symbol OHLCV. Yahoo's
    daily chart history intermittently omits recent Indian trading days; these
    bars let the chart builder fill those gaps with authoritative NSE/BSE data.
    Like the other daily artifacts they only reach this Space via the workflow's
    HF push (which can silently fail), so we pull them directly (public repo, no
    token). We fetch the manifest, download any session file we are missing, and
    prune local files no longer listed. Returns True when anything changed.
    """
    import json as _json

    import requests as _requests

    raw_base = (
        os.environ.get("COMMITTED_DATA_RAW_BASE")
        or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data"
    ).strip().rstrip("/")
    eod_dir = Path(__file__).resolve().parents[1] / "data" / "eod_bars"
    try:
        resp = _requests.get(f"{raw_base}/eod_bars/manifest.json", timeout=25)
        if resp.status_code != 200:
            logger.info("EOD-bars self-update: manifest HTTP %s — keeping local", resp.status_code)
            return False
        manifest = resp.json()
        sessions = manifest.get("sessions") if isinstance(manifest, dict) else None
        if not isinstance(sessions, list) or not sessions:
            logger.warning("EOD-bars self-update: manifest invalid — keeping local")
            return False
        wanted = [str(s).strip() for s in sessions if str(s).strip()]
        eod_dir.mkdir(parents=True, exist_ok=True)
        changed = False
        for session_date in wanted:
            target = eod_dir / f"{session_date}.json"
            if target.exists():
                continue
            file_resp = _requests.get(f"{raw_base}/eod_bars/{session_date}.json", timeout=25)
            if file_resp.status_code != 200:
                logger.info("EOD-bars self-update: %s HTTP %s — skipping", session_date, file_resp.status_code)
                continue
            payload = file_resp.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), dict):
                continue
            tmp = target.with_name(target.name + ".tmp")
            tmp.write_text(file_resp.text, encoding="utf-8")
            tmp.replace(target)
            changed = True
        # Prune local session files that dropped out of the rolling window.
        keep = set(wanted)
        for path in eod_dir.glob("*.json"):
            if path.name == "manifest.json":
                continue
            if path.stem not in keep:
                try:
                    path.unlink()
                    changed = True
                except OSError:
                    pass
        # Refresh the manifest copy last (its mtime is not what the reader keys on).
        (eod_dir / "manifest.json").write_text(resp.text, encoding="utf-8")
        if changed:
            logger.info("EOD-bars self-update: synced %s sessions from remote", len(wanted))
        return changed
    except Exception as exc:
        logger.warning("EOD-bars self-update failed: %s", exc)
        return False


def pull_latest_earnings_data() -> bool:
    """Pull the committed earnings artifacts from the public repo.

    ``earnings_metrics.json`` (post-result reaction metrics for the Positive
    Earnings scanner and the chart "E" markers) and ``earnings_calendar.json``
    (upcoming BSE results calendar) are rewritten by the daily workflow and
    committed with ``[skip ci]`` — like every other daily artifact they only
    reach this Space via the workflow's HF push, which can silently fail.
    Without this pull the scanner stays frozen on whatever was baked at the
    last full deploy. The dashboard reader is mtime-cached, so replacing the
    files is enough. Returns True when either file advanced.
    """
    import json as _json

    import requests as _requests

    raw_base = (
        os.environ.get("COMMITTED_DATA_RAW_BASE")
        or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data"
    ).strip().rstrip("/")
    data_dir = Path(__file__).resolve().parents[1] / "data"
    updated = False
    for name, payload_key in (("earnings_metrics.json", "entries"), ("earnings_calendar.json", "upcoming")):
        path = data_dir / name
        try:
            local_stamp = ""
            try:
                local_payload = _json.loads(path.read_text(encoding="utf-8"))
                local_stamp = str(local_payload.get("generated_at") or local_payload.get("updated_at") or "")
            except Exception:
                local_stamp = ""
            resp = _requests.get(f"{raw_base}/{name}", timeout=25)
            if resp.status_code != 200:
                logger.info("Earnings self-update (%s): remote HTTP %s — keeping local", name, resp.status_code)
                continue
            remote = resp.json()
            remote_stamp = str(remote.get("generated_at") or remote.get("updated_at") or "") if isinstance(remote, dict) else ""
            payload = remote.get(payload_key) if isinstance(remote, dict) else None
            if not remote_stamp or not isinstance(payload, dict):
                logger.warning("Earnings self-update (%s): remote payload invalid — keeping local", name)
                continue
            # Guard: never replace populated local metrics with an empty remote.
            if name == "earnings_metrics.json" and not payload:
                try:
                    local_entries = _json.loads(path.read_text(encoding="utf-8")).get("entries") or {}
                except Exception:
                    local_entries = {}
                if local_entries:
                    continue
            if local_stamp and remote_stamp <= local_stamp:
                continue  # already current (or older) — nothing to do
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(resp.text, encoding="utf-8")
            tmp.replace(path)
            logger.info("Earnings self-update (%s): pulled %s (was %s)", name, remote_stamp, local_stamp or "<none>")
            updated = True
        except Exception as exc:
            logger.warning("Earnings self-update (%s) failed: %s", name, exc)
    return updated


def apply_bhavcopy_patch_on_startup() -> None:
    """Roll any committed bhavcopy patch into the live snapshot before serving.

    The HF deploy excludes the heavy ``free_snapshots.json`` from the bundle,
    so cold starts begin with stale prices from the previous baked snapshot.
    Without this, dashboard / scanner endpoints serve yesterday's prices until
    the next watchdog tick. We first pull the freshest committed patch from the
    public repo (self-heal when the daily HF push failed), then apply it so
    prices stay in sync with whatever the daily-bhavcopy GitHub Action committed.
    """
    pulled = pull_latest_bhavcopy_patch()
    xp_pulled = pull_latest_xp_breadth()
    bands_pulled = pull_latest_price_bands()
    eod_bars_pulled = pull_latest_eod_bars()
    earnings_pulled = pull_latest_earnings_data()
    try:
        result = india_provider.apply_committed_bhavcopy_patch(force=pulled)
    except Exception as exc:
        logger.warning("Bhavcopy patch apply on startup failed: %s", exc)
        result = {}
    snapshot_updated = result.get("status") == "ok" and int(result.get("snapshots_updated", 0) or 0) > 0
    # Clear runtime caches when the snapshot, the XP history, the circuit bands,
    # the recent EOD bars, or the earnings artifacts advanced, so charts /
    # dashboard / scanners re-read fresh files.
    if snapshot_updated or xp_pulled or bands_pulled or eod_bars_pulled or earnings_pulled:
        try:
            service._clear_runtime_caches()
        except Exception:
            pass
    if snapshot_updated:
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


# ── Traffic-triggered bhavcopy self-heal ─────────────────────────────────────
# The evening apply cron fires at fixed times, but a free-tier HF Space sleeps
# and silently misses them — leaving the backend frozen on an earlier day's
# snapshot (advance/decline, per-stock %, XP breadth, scanners all stale).
# So we ALSO heal on live traffic: any API request (a user opening the app, or
# the keep-alive ping) after the daily patch is committed brings the Space
# current within minutes — no cron, no restart, no token needed (the patch is
# pulled from the public repo). Throttled per worker and run in a background
# thread so it adds nothing to request latency; the apply bumps free_snapshots
# .json's mtime, which auto-invalidates the in-memory snapshot cache so the very
# next request serves fresh numbers.
_self_heal_lock = threading.Lock()
_last_self_heal_monotonic = 0.0
_SELF_HEAL_MIN_INTERVAL_S = 600  # at most once per 10 minutes per worker


def _latest_expected_bhavcopy_date() -> str:
    now = datetime.now(IST)
    eod_ready = now.hour * 60 + now.minute >= 16 * 60 + 45  # BSE EOD published/applied by ~4:45 PM IST
    cand = now.date() if (now.weekday() < 5 and eod_ready) else now.date() - timedelta(days=1)
    while cand.weekday() >= 5:  # roll Sat/Sun back to Friday's session
        cand -= timedelta(days=1)
    return cand.isoformat()


def maybe_self_heal_bhavcopy() -> None:
    """Pull + apply the latest committed patch if the served data looks stale.

    Cheap and non-blocking: a monotonic throttle gate runs first, so the file
    check + network pull happen at most once per 10 minutes per worker, and the
    actual apply runs in a daemon thread.
    """
    global _last_self_heal_monotonic
    now_m = time.monotonic()
    if now_m - _last_self_heal_monotonic < _SELF_HEAL_MIN_INTERVAL_S:
        return
    _last_self_heal_monotonic = now_m  # advance the gate regardless of outcome

    import json as _json

    try:
        expected = _latest_expected_bhavcopy_date()
        data_dir = Path(__file__).resolve().parents[1] / "data"

        patch_path = data_dir / "bhavcopy_patch.json"
        local_date = str(_json.loads(patch_path.read_text(encoding="utf-8")).get("date") or "")
        bhav_current = bool(local_date) and local_date >= expected

        # The XP breadth history is a SEPARATE committed file that lags on its
        # own schedule — e.g. when BSE is geo-blocked from the CI runner the
        # price patch self-heals via YFINANCE but XP stays behind until a
        # full-coverage run lands. Gate on BOTH files, or a fresh price patch
        # would short-circuit the pull and freeze the breadth score.
        xp_current = False
        try:
            xp_doc = _json.loads((data_dir / "xp_breadth_history.json").read_text(encoding="utf-8"))
            xp_latest = xp_doc.get("latest") if isinstance(xp_doc, dict) else None
            if isinstance(xp_latest, dict) and xp_latest.get("date"):
                xp_local_date = str(xp_latest.get("date"))
            else:
                days = xp_doc.get("days") if isinstance(xp_doc, dict) else None
                xp_local_date = str(days[-1].get("date") or "") if isinstance(days, list) and days else ""
            xp_current = bool(xp_local_date) and xp_local_date >= expected
        except Exception:
            xp_current = False  # can't read XP — let the pull decide

        # Circuit bands are committed by the same evening run; same failure
        # mode — a current price patch must not mask lagging band files.
        bands_current = False
        try:
            bands_doc = _json.loads((data_dir / "price_bands.json").read_text(encoding="utf-8"))
            bands_as_of = str(bands_doc.get("as_of") or "") if isinstance(bands_doc, dict) else ""
            bands_current = bool(bands_as_of) and bands_as_of >= expected
        except Exception:
            bands_current = False  # can't read bands — let the pull decide

        if bhav_current and xp_current and bands_current:
            return  # all current for the latest trading session — skip the pull
    except Exception:
        pass  # can't determine staleness — fall through and let the apply decide

    if not _self_heal_lock.acquire(blocking=False):
        return  # a heal is already running in this worker

    def _run() -> None:
        try:
            apply_bhavcopy_patch_on_startup()
        except Exception as exc:  # never let a heal crash a request worker
            logger.warning("Bhavcopy self-heal failed: %s", exc)
        finally:
            _self_heal_lock.release()

    threading.Thread(target=_run, name="bhavcopy-self-heal", daemon=True).start()


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
        # Pull + apply across the evening (weekdays) so a late or retried
        # daily-bhavcopy commit is picked up the same day even if the workflow's
        # HF push never lands. Each run no-ops once the patch is already current.
        scheduler.add_job(
            apply_bhavcopy_patch_on_startup,
            CronTrigger(hour="16,17,18,19,21", minute=50, day_of_week="mon-fri", timezone=IST),
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
