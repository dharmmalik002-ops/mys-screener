"""The evening digest orchestrator.

Order matters: cheap guards, then the data-freshness gate, then the claim, and
only then any expensive work. Nothing is sent on stale data -- an empty digest
built on a half-applied bhavcopy would also poison tomorrow's dedupe baseline.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from app.services import chart_render
from app.services.telegram_alerts import formatting as fmt
from app.services.telegram_alerts import prefs as prefs_mod
from app.services.telegram_alerts import registry
from app.services.telegram_alerts.client import (
    MEDIA_GROUP_MAX,
    MEDIA_GROUP_MIN,
    TelegramApiError,
    TelegramClient,
)
from app.services.telegram_alerts.prefs_store import AlertPrefsStore
from app.services.telegram_alerts.run_state import DigestRunState

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# Bars FETCHED per symbol. Matches _chart_grid_series_bar_limit, so the digest
# shares the existing on-disk chart cache shape instead of creating a second one.
CHART_FETCH_BARS = 520
# A cold-cache miss funnels through free.py's Semaphore(3) with a 25s timeout;
# skip rather than stall the whole digest.
CHART_FETCH_TIMEOUT_SECONDS = 20.0
CHART_FETCH_CONCURRENCY = 6
SCAN_TIMEOUT_SECONDS = 150.0
# Below this the snapshot rebuild clearly did not happen -- see _freshness_gate.
MIN_ELIGIBLE_SNAPSHOTS = 500
CONFLUENCE_MIN_SCANNERS = 3

HISTORY_KEY_PREFIX = "tg-digest:"

APP_URL = "https://my-screener-theta.vercel.app/"


@dataclass
class DigestResult:
    status: str
    session_date: str | None = None
    chat_id: int | None = None
    scan_ids: list[str] = field(default_factory=list)
    photo_count: int = 0
    match_count: int = 0
    failed_scanners: list[str] = field(default_factory=list)
    skipped_symbols: list[str] = field(default_factory=list)
    durable: bool = True
    detail: str | None = None

    @property
    def ok(self) -> bool:
        """Every expected outcome, including the skips. Only a genuine failure
        should show red in CI."""
        return self.status not in ("error",)


@dataclass
class ScannerBlock:
    scan_id: str
    label: str
    items: list[Any]
    new_symbols: set[str]
    charted: list[Any] = field(default_factory=list)
    photos: list[tuple[str, bytes]] = field(default_factory=list)
    overflow: list[Any] = field(default_factory=list)


def history_key(scan_id: str) -> str:
    return f"{HISTORY_KEY_PREFIX}{scan_id}"


# --------------------------------------------------------------------------
# Store construction
# --------------------------------------------------------------------------


def _state_dir(service, settings) -> Path:
    try:
        return Path(settings.app_state_dir) / "data"
    except Exception:
        backend_root = getattr(service.provider, "backend_root", Path(__file__).resolve().parents[3])
        return Path(backend_root) / "data"


def build_prefs_store(service, settings) -> AlertPrefsStore:
    return AlertPrefsStore(
        settings.database_url, _state_dir(service, settings) / "telegram_alert_prefs.json"
    )


def build_run_state(service, settings) -> DigestRunState:
    return DigestRunState(
        settings.database_url, _state_dir(service, settings) / "telegram_digest_runs.json"
    )


# --------------------------------------------------------------------------
# Guards
# --------------------------------------------------------------------------


def _is_trading_day(service, today: date) -> bool:
    checker = getattr(service.provider, "_is_trading_day_ist", None)
    if callable(checker):
        try:
            return bool(checker(today))
        except Exception:
            pass
    return today.weekday() < 5


async def _freshness_gate(service) -> tuple[str | None, list, str | None]:
    """Return (session_iso, eligible_snapshots, failure_status).

    Three independent checks, because a patch file can read as current while
    the in-memory snapshots are not.
    """
    try:
        status = service.get_bhavcopy_status()
    except Exception as exc:
        logger.warning("digest: bhavcopy status unavailable: %s", exc)
        return None, [], "stale-data"
    if not getattr(status, "updated", False):
        return None, [], "stale-data"

    session_iso = str(getattr(status, "date", "") or "")
    if not session_iso:
        return None, [], "stale-data"

    served = service._current_session_iso()
    if served != session_iso:
        logger.info("digest: snapshots at %s but patch at %s", served, session_iso)
        return session_iso, [], "stale-snapshots"

    eligible = service._scan_eligible_snapshots(await service._snapshots())
    if len(eligible) < MIN_ELIGIBLE_SNAPSHOTS:
        # _scan_eligible_snapshots drops rows whose history_session_date does not
        # match the patch date, so a patch that applied without a snapshot
        # rebuild collapses this set and every scanner returns nothing. Sending
        # then would also record an empty dedupe baseline.
        logger.info("digest: only %s eligible snapshots", len(eligible))
        return session_iso, eligible, "thin-universe"

    return session_iso, eligible, None


# --------------------------------------------------------------------------
# Regime header
# --------------------------------------------------------------------------


async def _build_header(
    service, session_iso: str, eligible: Sequence[Any], *, scanners: int, matches: int, charts: int
) -> str:
    """Cheap sources only.

    Deliberately NOT get_market_environment(): that runs breakout replay, ~25
    per-event chart pulls, a week review and an AI narrative -- minutes of vCPU
    for one line of text.
    """
    regime = xp_score = exposure_band = exposure_pct = None
    try:
        breadth = await asyncio.to_thread(service._load_xp_breadth)
        if breadth is not None:
            regime = getattr(breadth, "regime", None)
            xp_score = getattr(breadth, "xp_score", None)
    except Exception as exc:
        logger.info("digest: xp breadth unavailable: %s", exc)

    try:
        exposure = await service.get_markets_exposure()
        verdict = (exposure or {}).get("verdict") or {}
        if verdict.get("available"):
            exposure_band = verdict.get("band")
            exposure_pct = verdict.get("exposure_pct")
    except Exception as exc:
        logger.info("digest: exposure unavailable: %s", exc)

    advances = sum(1 for s in eligible if (getattr(s, "change_pct", 0) or 0) > 0)
    declines = sum(1 for s in eligible if (getattr(s, "change_pct", 0) or 0) < 0)

    stop_pct = win_pct = horizon = None
    try:
        from app.services.breakout_stats import HORIZON_SESSIONS, STOP_PCT, WIN_PCT

        stop_pct, win_pct, horizon = STOP_PCT, WIN_PCT, HORIZON_SESSIONS
    except Exception:
        pass

    try:
        pretty_date = datetime.fromisoformat(session_iso).strftime("%a %d %b %Y")
    except ValueError:
        pretty_date = session_iso

    return fmt.regime_header(
        session_date=pretty_date,
        regime=regime,
        xp_score=xp_score,
        exposure_band=exposure_band,
        exposure_pct=exposure_pct,
        advances=advances,
        declines=declines,
        scanner_count=scanners,
        match_count=matches,
        chart_count=charts,
        stop_pct=stop_pct,
        win_pct=win_pct,
        horizon_sessions=horizon,
    )


# --------------------------------------------------------------------------
# Scanning, dedupe, ranking
# --------------------------------------------------------------------------


async def _run_scanner(service, scan_id: str) -> list[Any] | None:
    """None signals failure (reported in the footer); [] is a real empty scan."""
    try:
        response = await asyncio.wait_for(
            service.get_scan_results(scan_id, include_sector_summaries=False),
            timeout=SCAN_TIMEOUT_SECONDS,
        )
    except KeyError:
        logger.info("digest: scan %s no longer exists", scan_id)
        return None
    except asyncio.TimeoutError:
        logger.warning("digest: scan %s timed out", scan_id)
        return None
    except Exception as exc:
        logger.warning("digest: scan %s failed: %s", scan_id, exc)
        return None
    return list(getattr(response, "items", []) or [])


def _previous_symbols(store, scan_id: str, session_iso: str, sessions: int) -> set[str]:
    """Symbols seen in the retained window before today.

    keep_days is set alongside keep_dates because a long weekend plus holidays
    would otherwise silently shrink the effective lookback.
    """
    try:
        history = store.load(
            history_key(scan_id), keep_dates=sessions, keep_days=sessions * 3
        )
    except Exception as exc:
        logger.info("digest: history load failed for %s: %s", scan_id, exc)
        return set()
    seen: set[str] = set()
    for date_iso, rows in history.items():
        if date_iso >= session_iso:
            continue
        for row in rows or []:
            if isinstance(row, dict) and row.get("symbol"):
                seen.add(str(row["symbol"]))
    return seen


def _record_history(store, scan_id: str, session_iso: str, items: Sequence[Any], sessions: int) -> None:
    """Record the FULL hit list, not the capped subset that got charted.

    Otherwise a name ranked #12 today (recorded as absent) is flagged NEW
    tomorrow when it climbs to #3, despite having been in the scan both days.
    """
    payload = [
        {
            "symbol": getattr(item, "symbol", None),
            "score": getattr(item, "score", None),
            "last_price": getattr(item, "last_price", None),
        }
        for item in items
        if getattr(item, "symbol", None)
    ]
    try:
        store.record_once(
            history_key(scan_id), session_iso, payload,
            keep_dates=sessions, keep_days=sessions * 3,
        )
    except Exception as exc:
        logger.info("digest: history record failed for %s: %s", scan_id, exc)


def rank_items(items: Sequence[Any], new_symbols: set[str], *, new_first: bool) -> list[Any]:
    """Deterministic, so two runs of the same session produce the same album.
    The trailing symbol term is what guarantees it -- score ties are common."""

    def key(item: Any):
        symbol = str(getattr(item, "symbol", ""))
        return (
            0 if (new_first and symbol in new_symbols) else 1,
            -len(tuple(getattr(item, "also_in", ()) or ())),
            -float(getattr(item, "rs_rating", 0) or 0),
            -float(getattr(item, "score", 0) or 0),
            -float(getattr(item, "relative_volume", 0) or 0),
            symbol,
        )

    return sorted(items, key=key)


# --------------------------------------------------------------------------
# Charts
# --------------------------------------------------------------------------


async def _fetch_bars(service, symbol: str) -> list | None:
    """Through provider.get_chart, not _read_chart_cache: the latter skips the
    EOD gap-fill, which would silently drop the last 1-3 NSE sessions Yahoo
    dropped -- exactly the wrong failure on an EOD digest."""
    try:
        return await asyncio.wait_for(
            service.provider.get_chart(symbol, "1D", bars=CHART_FETCH_BARS),
            timeout=CHART_FETCH_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.info("digest: bars unavailable for %s: %s", symbol, exc)
        return None


def _annotation(item: Any, *, scanner_label: str, is_new: bool, session_iso: str):
    return chart_render.ChartAnnotation(
        symbol=str(getattr(item, "symbol", "")),
        name=getattr(item, "name", None),
        exchange=getattr(item, "exchange", None),
        sector=getattr(item, "sector", None),
        market_cap_crore=getattr(item, "market_cap_crore", None),
        last_price=getattr(item, "last_price", None),
        change_pct=getattr(item, "change_pct", None),
        rs_rating=getattr(item, "rs_rating", None),
        relative_volume=getattr(item, "relative_volume", None),
        scanner_name=scanner_label,
        is_new=is_new,
        also_in=tuple(getattr(item, "also_in", ()) or ()),
        session_date=session_iso,
    )


async def _render_block(
    service, block: ScannerBlock, *, cap: int, session_iso: str, spec, skipped: list[str]
) -> None:
    """Fetch bars and render up to ``cap`` charts for one scanner."""
    candidates = block.items[:cap]
    block.overflow = list(block.items[cap:])

    semaphore = asyncio.Semaphore(CHART_FETCH_CONCURRENCY)

    async def bars_for(item):
        async with semaphore:
            return item, await _fetch_bars(service, str(getattr(item, "symbol", "")))

    fetched = await asyncio.gather(*(bars_for(i) for i in candidates), return_exceptions=True)

    for outcome in fetched:
        if isinstance(outcome, BaseException):
            continue
        item, bars = outcome
        symbol = str(getattr(item, "symbol", ""))
        if not bars:
            skipped.append(symbol)
            block.overflow.append(item)
            continue
        note = _annotation(
            item,
            scanner_label=block.label,
            is_new=symbol in block.new_symbols,
            session_iso=session_iso,
        )
        try:
            png = await chart_render.render_candles_png_async(bars, note, spec)
        except Exception as exc:
            logger.info("digest: render failed for %s: %s", symbol, exc)
            skipped.append(symbol)
            block.overflow.append(item)
            continue
        block.charted.append(item)
        block.photos.append((f"{symbol}.png", png))


# --------------------------------------------------------------------------
# Sending
# --------------------------------------------------------------------------


async def _send_block(client: TelegramClient, chat_id: int, block: ScannerBlock) -> int:
    """Send one scanner's album (or photo, or text) plus any overflow.
    Returns the number of photos actually sent."""
    caption = fmt.album_caption(
        scanner_label=block.label,
        total_matches=len(block.items),
        new_count=len(block.new_symbols & {str(getattr(i, "symbol", "")) for i in block.items}),
        charted=block.charted,
        new_symbols=block.new_symbols,
        overflow_symbols=[str(getattr(i, "symbol", "")) for i in block.overflow],
    )

    sent = 0
    if not block.photos:
        await client.send_message(
            chat_id,
            fmt.text_only_block(block.label, block.items[:20], new_symbols=block.new_symbols),
        )
        return 0

    if len(block.photos) < MEDIA_GROUP_MIN:
        # A one-item media group is a 400; sendPhoto is the real branch.
        filename, payload = block.photos[0]
        await client.send_photo(chat_id, payload, filename=filename, caption=caption)
        return 1

    for index in range(0, len(block.photos), MEDIA_GROUP_MAX):
        chunk = block.photos[index : index + MEDIA_GROUP_MAX]
        if len(chunk) == 1:
            filename, payload = chunk[0]
            await client.send_photo(chat_id, payload, filename=filename)
            sent += 1
            continue
        await client.send_media_group(
            chat_id, chunk, caption=caption if index == 0 else None
        )
        sent += len(chunk)

    if block.overflow:
        for chunk in fmt.split_message(fmt.overflow_table(block.label, block.overflow[:40])):
            await client.send_message(chat_id, chunk)
    return sent


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


async def run_digest(
    service,
    settings,
    *,
    chat_id: int,
    client: TelegramClient | None = None,
    prefs_store: AlertPrefsStore | None = None,
    run_state: DigestRunState | None = None,
    force: bool = False,
    external: bool = False,
    record_history: bool = True,
    only_scan_ids: Sequence[str] | None = None,
    today: date | None = None,
) -> DigestResult:
    """Build and send today's digest for one chat."""
    today = today or datetime.now(IST).date()
    prefs_store = prefs_store or build_prefs_store(service, settings)
    run_state = run_state or build_run_state(service, settings)
    durable = run_state.is_durable()

    # ---- guards (cheapest first) ----
    if client is None:
        if not settings.telegram_enabled:
            return DigestResult(status="disabled", chat_id=chat_id, durable=durable)
        client = TelegramClient(settings.telegram_bot_token)
    if not client.available:
        return DigestResult(status="disabled", chat_id=chat_id, durable=durable)

    if not _is_trading_day(service, today):
        return DigestResult(status="non-trading-day", chat_id=chat_id, durable=durable)

    prefs = prefs_store.load()
    chat = prefs_mod.chat_prefs(prefs, chat_id)
    if not force and prefs_mod.is_muted(chat, today):
        return DigestResult(status="muted", chat_id=chat_id, durable=durable)

    if external and not durable and not force:
        # Without Postgres the claim is ephemeral, so an external trigger after
        # a restart would double-send. Refuse rather than risk it.
        return DigestResult(
            status="not-durable", chat_id=chat_id, durable=False,
            detail="DATABASE_URL is not configured; send-once cannot be guaranteed",
        )

    # ---- freshness gate ----
    session_iso, eligible, failure = await _freshness_gate(service)
    if failure:
        return DigestResult(status=failure, session_date=session_iso, chat_id=chat_id, durable=durable)
    assert session_iso is not None

    scan_ids = list(only_scan_ids) if only_scan_ids else prefs_mod.effective_scan_ids(chat)
    if not scan_ids:
        return DigestResult(
            status="no-scanners", session_date=session_iso, chat_id=chat_id, durable=durable
        )

    # ---- claim the slot before any expensive work ----
    run = run_state.try_claim(chat_id, session_iso, uuid.uuid4().hex)
    if run is None and not force:
        return DigestResult(
            status="already-sent", session_date=session_iso, chat_id=chat_id, durable=durable
        )
    already_sent = set(run.sent_scan_ids) if run else set()

    store = service.scan_history_store
    sessions = prefs_mod.dedupe_sessions(chat)
    new_only = chat.get("repeat_policy") == "new_only"
    charts_enabled = bool(chat.get("charts_enabled", True)) and chart_render.CHART_RENDER_AVAILABLE
    spec = chart_render.RenderSpec(
        display_bars=int(getattr(settings, "telegram_digest_chart_bars", 130) or 130)
    )
    image_budget = int(getattr(settings, "telegram_digest_max_images", 40) or 40)

    failed: list[str] = []
    skipped: list[str] = []
    blocks: list[ScannerBlock] = []
    symbol_scanner_count: dict[str, int] = {}
    total_matches = 0
    fresh_total = 0

    for scan_id in scan_ids:
        items = await _run_scanner(service, scan_id)
        if items is None:
            failed.append(scan_id)
            continue

        previous = _previous_symbols(store, scan_id, session_iso, sessions)
        new_symbols = {
            str(getattr(i, "symbol", "")) for i in items
            if str(getattr(i, "symbol", "")) not in previous
        }
        if record_history:
            _record_history(store, scan_id, session_iso, items, sessions)

        total_matches += len(items)
        fresh_total += len(new_symbols)
        for item in items:
            symbol = str(getattr(item, "symbol", ""))
            if symbol:
                symbol_scanner_count[symbol] = symbol_scanner_count.get(symbol, 0) + 1

        presented = [i for i in items if str(getattr(i, "symbol", "")) in new_symbols] if new_only else list(items)
        if not presented:
            continue

        blocks.append(
            ScannerBlock(
                scan_id=scan_id,
                label=registry.label(scan_id),
                items=rank_items(presented, new_symbols, new_first=not new_only),
                new_symbols=new_symbols,
            )
        )

    # ---- render ----
    photos_planned = 0
    for block in blocks:
        if block.scan_id in already_sent:
            continue
        remaining = max(0, image_budget - photos_planned)
        cap = min(prefs_mod.chart_cap(chat, block.scan_id), remaining)
        if not charts_enabled or cap <= 0:
            block.overflow = list(block.items)
            continue
        await _render_block(
            service, block, cap=cap, session_iso=session_iso, spec=spec, skipped=skipped
        )
        photos_planned += len(block.photos)

    # ---- send ----
    photos_sent = 0
    try:
        header = await _build_header(
            service, session_iso, eligible,
            scanners=len(scan_ids), matches=total_matches, charts=photos_planned,
        )
        if not already_sent:
            for chunk in fmt.split_message(header):
                await client.send_message(chat_id, chunk)

        if not blocks:
            await client.send_message(
                chat_id, f"No setups triggered today across {len(scan_ids)} scanners."
            )

        for block in blocks:
            if block.scan_id in already_sent:
                continue
            sent = await _send_block(client, chat_id, block)
            photos_sent += sent
            run_state.mark_scanner_sent(chat_id, session_iso, block.scan_id, photos=sent)

        confluence = sorted(
            s for s, n in symbol_scanner_count.items() if n >= CONFLUENCE_MIN_SCANNERS
        )
        summary = fmt.closing_summary(
            confluence=confluence,
            fresh_count=fresh_total,
            skipped=skipped,
            photos_sent=photos_sent,
            photo_budget=image_budget,
            failed_scanners=failed,
            app_url=APP_URL,
        )
        for chunk in fmt.split_message(summary):
            await client.send_message(chat_id, chunk)
    except TelegramApiError as exc:
        logger.warning("digest: send failed for chat %s: %s", chat_id, exc)
        return DigestResult(
            status="send-failed", session_date=session_iso, chat_id=chat_id,
            scan_ids=[b.scan_id for b in blocks], photo_count=photos_sent,
            match_count=total_matches, failed_scanners=failed, skipped_symbols=skipped,
            durable=durable, detail=str(exc),
        )

    run_state.mark_complete(chat_id, session_iso)
    prefs_store.save(prefs)
    return DigestResult(
        status="sent",
        session_date=session_iso,
        chat_id=chat_id,
        scan_ids=[b.scan_id for b in blocks],
        photo_count=photos_sent,
        match_count=total_matches,
        failed_scanners=failed,
        skipped_symbols=skipped,
        durable=durable,
    )
