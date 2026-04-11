from __future__ import annotations

import asyncio
import fcntl
import logging
import sys
from contextlib import contextmanager
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import get_settings
from app.providers.factory import build_provider
from app.services.dashboard_service import DashboardService
from app.services.maintenance import run_market_close_maintenance
from app.services.us_dashboard_service import USDashboardService


LOGGER = logging.getLogger("stock_scanner.maintenance")
LOCK_PATH = BACKEND_ROOT / "data" / "scheduled_maintenance.lock"


@contextmanager
def single_run_lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = LOCK_PATH.open("w", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.close()
        raise SystemExit(0)
    try:
        yield
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


async def refresh_market_if_due(market_name: str, service: DashboardService) -> None:
    refresh_strategy_method = getattr(service.provider, "preferred_refresh_strategy", None)
    refresh_strategy = refresh_strategy_method() if callable(refresh_strategy_method) else None
    if refresh_strategy != "historical":
        LOGGER.info("%s refresh not due (strategy=%s)", market_name.upper(), refresh_strategy or "none")
        return

    result = await run_market_close_maintenance(market_name, service)
    LOGGER.info(
        "%s refresh mode=%s snapshot_updated_at=%s prewarmed_chart_count=%s",
        market_name.upper(),
        result.get("refresh_mode"),
        result.get("snapshot_updated_at"),
        result.get("prewarmed_chart_count"),
    )


async def ensure_money_flow_outputs(market_name: str, service: DashboardService) -> None:
    report = await service.ensure_money_flow_report_current()
    if report is not None:
        LOGGER.info("%s weekly money flow ready for %s", market_name.upper(), report.week_key)
    else:
        LOGGER.info("%s weekly money flow unavailable", market_name.upper())

    stock_payload = await service.ensure_money_flow_stock_ideas_current()
    if stock_payload is not None:
        LOGGER.info(
            "%s stock ideas ready for %s (%d consolidation, %d value)",
            market_name.upper(),
            stock_payload.recommendation_date,
            len(stock_payload.consolidating_ideas),
            len(stock_payload.value_ideas),
        )
    else:
        LOGGER.info("%s stock ideas unavailable", market_name.upper())


async def run() -> None:
    settings = get_settings()
    india_provider = build_provider(settings, market="india")
    us_provider = build_provider(settings, market="us")
    us_settings = settings.model_copy(update={"market_cap_min_crore": 0})

    services: list[tuple[str, DashboardService]] = [
        ("india", DashboardService(provider=india_provider, settings=settings)),
        ("us", USDashboardService(provider=us_provider, settings=us_settings)),
    ]

    for market_name, service in services:
        try:
            await refresh_market_if_due(market_name, service)
        except Exception:
            LOGGER.exception("%s refresh maintenance failed", market_name.upper())
            continue

        try:
            await ensure_money_flow_outputs(market_name, service)
        except Exception:
            LOGGER.exception("%s money flow maintenance failed", market_name.upper())


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    with single_run_lock():
        asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())