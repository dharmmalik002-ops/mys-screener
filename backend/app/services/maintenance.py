from __future__ import annotations

import asyncio
import logging

from app.services.dashboard_service import DashboardService


LOGGER = logging.getLogger(__name__)


def default_index_symbols(market_name: str) -> list[str]:
    if str(market_name or "india").strip().lower() == "us":
        return ["^GSPC", "^IXIC", "^DJI"]
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

    try:
        await service.refresh_historical_breadth_latest()
    except Exception:
        LOGGER.exception("%s breadth refresh failed", market_name.upper())

    chart_symbols = _unique_symbols(
        [
            *default_index_symbols(market_name),
            *[item.symbol for item in dashboard.top_gainers],
            *[item.symbol for item in dashboard.top_losers],
            *[item.symbol for item in dashboard.top_volume_spikes],
        ]
    )[:15]
    if chart_symbols:
        await asyncio.gather(
            *(service.get_chart(symbol, "1D") for symbol in chart_symbols),
            return_exceptions=True,
        )

    return {
        **refresh_result,
        "prewarmed_chart_count": len(chart_symbols),
        "popular_symbols": chart_symbols,
    }