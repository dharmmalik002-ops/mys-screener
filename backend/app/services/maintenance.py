from __future__ import annotations

import asyncio
import logging

from app.services.dashboard_service import DashboardService


LOGGER = logging.getLogger(__name__)

# How many charts to keep warm after market close, and how many to fetch at
# once. Prewarming the most-liquid names means a user's first chart open hits a
# warm cache instead of a cold 15-20s live fetch. Bounded concurrency keeps the
# (resource-constrained) HF Space from being overwhelmed; get_chart hits the
# disk cache first, so repeat runs are cheap once the cache is warm.
PREWARM_CHART_LIMIT = 150
PREWARM_CONCURRENCY = 4


def default_index_symbols(market_name: str) -> list[str]:
    return ["^NSEI", "^BSESN", "^NSEBANK"]


def _unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for symbol in symbols:
        normalized = str(symbol or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


async def run_market_close_maintenance(market_name: str, service: DashboardService) -> dict[str, object]:
    refresh_result = await service.refresh_market_data()

    dashboard = await service.build_dashboard()
    await service.get_scan_counts()

    try:
        await service.get_industry_groups()
    except Exception:
        LOGGER.exception("%s industry-group refresh failed", market_name.upper())

    chart_symbols = _unique_symbols(
        [
            *default_index_symbols(market_name),
            *[item.symbol for item in dashboard.top_gainers],
            *[item.symbol for item in dashboard.top_losers],
            *[item.symbol for item in dashboard.top_volume_spikes],
        ]
    )

    # Widen prewarm coverage to the most liquid stocks (the names users actually
    # click), so their charts are already cached when first opened.
    snap_loader = getattr(service, "_snapshots", None)
    if callable(snap_loader):
        try:
            snapshots = await snap_loader()
            liquid = sorted(
                snapshots,
                key=lambda s: float(getattr(s, "avg_rupee_volume_30d_crore", 0) or 0.0),
                reverse=True,
            )
            chart_symbols = _unique_symbols(chart_symbols + [s.symbol for s in liquid])
        except Exception:
            LOGGER.exception("%s prewarm symbol ranking failed", market_name.upper())

    chart_symbols = chart_symbols[:PREWARM_CHART_LIMIT]
    if chart_symbols:
        semaphore = asyncio.Semaphore(PREWARM_CONCURRENCY)

        async def _warm(sym: str) -> None:
            async with semaphore:
                try:
                    await service.get_chart(sym, "1D")
                except Exception:
                    pass

        await asyncio.gather(*(_warm(sym) for sym in chart_symbols), return_exceptions=True)

    return {
        **refresh_result,
        "prewarmed_chart_count": len(chart_symbols),
        "popular_symbols": chart_symbols[:15],
    }


# The whole universe, not just the most liquid 150: scanner results are mostly
# small caps, and a symbol with no chart_cache file costs a 5-year Yahoo
# download (~1-1.5 s on the Space) the first time anyone opens it. chart_cache/
# is gitignored and the Space's disk is wiped on every deploy, so after each
# restart every chart used to be cold until someone happened to open it.
# One symbol at a time with a pause, so user chart requests (which share the
# provider's fetch semaphore) never queue behind the warm-up for long.
UNIVERSE_WARM_START_DELAY_SECONDS = 90.0
UNIVERSE_WARM_PAUSE_SECONDS = 0.2
UNIVERSE_WARM_MAX_CONSECUTIVE_FAILURES = 8
UNIVERSE_WARM_FAILURE_BACKOFF_SECONDS = 120.0


def _chart_cache_is_warm(provider, symbol: str) -> bool:
    path_for = getattr(provider, "_chart_cache_path", None)
    is_fresh = getattr(provider, "_is_chart_cache_fresh", None)
    if not callable(path_for) or not callable(is_fresh):
        return False
    try:
        return path_for(symbol, "1D").exists() and bool(is_fresh(symbol, "1D"))
    except Exception:
        return False


async def warm_universe_chart_cache(
    market_name: str,
    service: DashboardService,
    *,
    pause_seconds: float = UNIVERSE_WARM_PAUSE_SECONDS,
) -> dict[str, int]:
    """Make sure every symbol in the universe has a fresh daily chart on disk.

    Scan hits go first (they are what gets clicked), then the rest by
    liquidity. Symbols whose cache is already fresh are skipped, so a repeat run
    over a warm disk costs only a stat per symbol.
    """
    provider = service.provider
    snapshots = await service._snapshots()
    ordered = sorted(
        snapshots,
        key=lambda s: float(getattr(s, "avg_rupee_volume_30d_crore", 0) or 0.0),
        reverse=True,
    )
    scan_hits: list[str] = []
    try:
        from app.scanners.definitions import scan_catalog_with_counts

        _, scan_results = await asyncio.to_thread(scan_catalog_with_counts, snapshots)
        scan_hits = [match.symbol for matches in scan_results.values() for match in matches]
    except Exception:
        LOGGER.exception("%s universe chart warm: scan ranking failed", market_name.upper())
    symbols = _unique_symbols([*default_index_symbols(market_name), *scan_hits, *[s.symbol for s in ordered]])

    bar_limit = service._chart_bar_limit("1D")
    warmed = skipped = failed = 0
    consecutive_failures = 0
    for symbol in symbols:
        if _chart_cache_is_warm(provider, symbol):
            skipped += 1
            continue
        try:
            bars = await provider.get_chart(symbol, "1D", bars=bar_limit)
        except asyncio.CancelledError:
            raise
        except Exception:
            bars = []
        if bars:
            warmed += 1
            consecutive_failures = 0
        else:
            failed += 1
            consecutive_failures += 1
            # A run of failures is Yahoo refusing us, not a run of bad symbols:
            # stop hammering it for a while rather than burning the whole list.
            if consecutive_failures >= UNIVERSE_WARM_MAX_CONSECUTIVE_FAILURES:
                LOGGER.warning(
                    "%s universe chart warm: %d consecutive failures, backing off %.0fs",
                    market_name.upper(), consecutive_failures, UNIVERSE_WARM_FAILURE_BACKOFF_SECONDS,
                )
                await asyncio.sleep(UNIVERSE_WARM_FAILURE_BACKOFF_SECONDS)
                consecutive_failures = 0
        await asyncio.sleep(pause_seconds)

    LOGGER.info(
        "%s universe chart warm complete: warmed=%d already_fresh=%d failed=%d of %d",
        market_name.upper(), warmed, skipped, failed, len(symbols),
    )
    return {"warmed": warmed, "skipped": skipped, "failed": failed, "total": len(symbols)}
