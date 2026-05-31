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
