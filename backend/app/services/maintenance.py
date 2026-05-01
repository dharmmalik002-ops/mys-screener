from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from app.services.dashboard_service import DashboardService


LOGGER = logging.getLogger(__name__)
RS_CHART_WARM_THRESHOLD = 80
CHART_WARM_TIMEFRAME = "1D"
CHART_WARM_BATCH_SIZE = 8
CHART_WARM_BATCH_PAUSE_SECONDS = 0.75


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


def _watchlist_symbols(service: DashboardService) -> list[str]:
    state = service.get_watchlists_state()
    return _unique_symbols(
        [
            symbol
            for watchlist in state.watchlists
            for symbol in watchlist.symbols
        ]
    )


def _rs_leader_symbols(snapshots: list[Any]) -> list[str]:
    leaders = [
        snapshot
        for snapshot in snapshots
        if bool(getattr(snapshot, "rs_eligible", False))
        and int(getattr(snapshot, "rs_rating", 0) or 0) > RS_CHART_WARM_THRESHOLD
    ]
    leaders.sort(
        key=lambda snapshot: (
            int(getattr(snapshot, "rs_rating", 0) or 0),
            float(getattr(snapshot, "market_cap_crore", 0) or 0),
        ),
        reverse=True,
    )
    return _unique_symbols([str(getattr(snapshot, "symbol", "")) for snapshot in leaders])


def _daily_chart_warm_symbols(service: DashboardService, snapshots: list[Any]) -> list[str]:
    return _unique_symbols([*_watchlist_symbols(service), *_rs_leader_symbols(snapshots)])


async def warm_daily_chart_cache(
    market_name: str,
    service: DashboardService,
    snapshots: list[Any],
    *,
    batch_size: int = CHART_WARM_BATCH_SIZE,
    batch_pause_seconds: float = CHART_WARM_BATCH_PAUSE_SECONDS,
) -> dict[str, object]:
    symbols = _daily_chart_warm_symbols(service, snapshots)
    batch_size = max(1, int(batch_size or 1))
    succeeded: list[str] = []
    failed: list[str] = []
    started_at = datetime.now(timezone.utc)

    for batch_start in range(0, len(symbols), batch_size):
        batch = symbols[batch_start:batch_start + batch_size]
        bar_limit = service._chart_bar_limit(CHART_WARM_TIMEFRAME)
        results = await asyncio.gather(
            *(service.provider.get_chart(symbol, CHART_WARM_TIMEFRAME, bars=bar_limit) for symbol in batch),
            return_exceptions=True,
        )
        for symbol, result in zip(batch, results):
            if isinstance(result, Exception):
                failed.append(symbol)
                LOGGER.warning(
                    "%s %s chart warm failed for %s: %s",
                    market_name.upper(),
                    CHART_WARM_TIMEFRAME,
                    symbol,
                    result,
                )
            else:
                succeeded.append(symbol)

        if batch_start + batch_size < len(symbols) and batch_pause_seconds > 0:
            await asyncio.sleep(batch_pause_seconds)

    return {
        "timeframe": CHART_WARM_TIMEFRAME,
        "threshold": RS_CHART_WARM_THRESHOLD,
        "batch_size": batch_size,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "attempted": len(symbols),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "symbols": symbols,
        "failed_symbols": failed,
    }


async def run_market_close_maintenance(market_name: str, service: DashboardService) -> dict[str, object]:
    refresh_result = await service.refresh_market_data()

    await service.build_dashboard()
    await service.get_scan_counts()

    try:
        await service.get_industry_groups()
    except Exception:
        LOGGER.exception("%s industry-group refresh failed", market_name.upper())

    snapshots = await service.provider.get_snapshots(service.settings.market_cap_min_crore)
    chart_warm_result = await warm_daily_chart_cache(market_name, service, snapshots)

    return {
        **refresh_result,
        "prewarmed_chart_count": chart_warm_result["succeeded"],
        "prewarmed_chart_attempt_count": chart_warm_result["attempted"],
        "chart_warm_failed_count": chart_warm_result["failed"],
        "chart_warm_timeframe": chart_warm_result["timeframe"],
        "chart_warm_batch_size": chart_warm_result["batch_size"],
        "chart_warm_symbols": chart_warm_result["symbols"],
        "chart_warm_failed_symbols": chart_warm_result["failed_symbols"],
    }
