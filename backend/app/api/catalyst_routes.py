"""Catalysts per company, at /api/catalysts/{symbol} (services/catalysts.py)."""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException, Query

from app.services.catalysts import CatalystService

_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9&_.-]{0,29}$")


def build_catalyst_router(service: CatalystService) -> APIRouter:
    router = APIRouter(prefix="/api/catalysts", tags=["catalysts"])

    # Plain def (gotcha 8): loading a record reads disk, and the build itself
    # runs in its own thread, so nothing here should sit on the event loop.
    @router.get("/{symbol}")
    def catalysts(symbol: str, refresh: bool = Query(default=False)):
        symbol = symbol.strip().upper()
        if not _SYMBOL_RE.match(symbol):
            raise HTTPException(status_code=400, detail="Unknown symbol")
        # Returns at once with the last record; a stale or missing one starts
        # a background rebuild and the page polls while `refreshing` is true.
        service.refresh_async(symbol, force=refresh)
        return service.status(symbol)

    return router
