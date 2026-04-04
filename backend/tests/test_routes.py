from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.api.routes import build_router
from app.models.market import (
    MoneyFlowHistoryResponse,
    MoneyFlowReport,
    MoneyFlowSector,
    MoneyFlowStockIdeasHistoryResponse,
    MoneyFlowStockIdeasResponse,
    SectorRotationItem,
    SectorRotationResponse,
    SectorRotationStock,
    WatchlistItem,
    WatchlistsStateResponse,
)


class StubIndiaService:
    def __init__(self) -> None:
        self.watchlists_state = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[
                WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY", "TCS"]),
            ],
        )

    async def get_money_flow_history(self) -> MoneyFlowHistoryResponse:
        return MoneyFlowHistoryResponse(
            reports=[self._report()],
            latest_week_key="2026-W13",
        )

    async def get_money_flow_latest(self) -> MoneyFlowReport:
        return self._report()

    async def get_money_flow_stock_ideas_history(self) -> MoneyFlowStockIdeasHistoryResponse:
        return MoneyFlowStockIdeasHistoryResponse(
            reports=[self._stock_ideas()],
            latest_recommendation_date="2026-03-31",
        )

    async def get_sector_rotation(self) -> SectorRotationResponse:
        return SectorRotationResponse(
            generated_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            sectors=[
                SectorRotationItem(
                    sector="Information Technology",
                    total_stocks=1,
                    top_gainers_1d=1,
                    top_gainers_1w=1,
                    top_gainers_1m=1,
                    pct_top_gainers_1d=100.0,
                    pct_top_gainers_1w=100.0,
                    pct_top_gainers_1m=100.0,
                    avg_return_1d=1.0,
                    avg_return_1w=2.0,
                    avg_return_1m=3.0,
                    rank_1d=1,
                    rank_1w=1,
                    rank_1m=1,
                    stocks=[
                        SectorRotationStock(
                            symbol="INFY",
                            name="Infosys Limited",
                            rs_rating=90,
                            return_1d=1.0,
                            return_1w=2.0,
                            return_1m=3.0,
                        )
                    ],
                )
            ],
        )

    def get_watchlists_state(self) -> WatchlistsStateResponse:
        return self.watchlists_state

    def save_watchlists_state(self, payload: WatchlistsStateResponse) -> WatchlistsStateResponse:
        self.watchlists_state = payload
        return payload

    @staticmethod
    def _report() -> MoneyFlowReport:
        sector = MoneyFlowSector(
            name="Information Technology",
            sentiment="bullish",
            reason="Leadership remains strong.",
            magnitude="moderate",
        )
        return MoneyFlowReport(
            week_key="2026-W13",
            week_start="2026-03-23",
            generated_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            inflows=[sector],
            outflows=[],
            sector_performance=[sector],
            short_term_headwinds=[],
            short_term_tailwinds=[sector],
            long_term_tailwinds=[sector],
            macro_summary="Constructive rotation into leaders.",
            ai_model="gemini",
        )

    @staticmethod
    def _stock_ideas() -> MoneyFlowStockIdeasResponse:
        return MoneyFlowStockIdeasResponse(
            recommendation_date="2026-03-31",
            generated_at=datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            next_update_at=datetime(2026, 4, 1, 12, 30, tzinfo=timezone.utc),
            consolidating_ideas=[],
            value_ideas=[],
            ai_model="gemini",
        )


class MoneyFlowRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        self.service = StubIndiaService()
        app.include_router(build_router({"india": self.service}))
        self.client = TestClient(app)

    def test_money_flow_history_route_uses_india_service(self) -> None:
        response = self.client.get("/api/money-flow/history")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["latest_week_key"], "2026-W13")

    def test_money_flow_stock_history_route_uses_india_service(self) -> None:
        response = self.client.get("/api/money-flow/stocks/history")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["latest_recommendation_date"], "2026-03-31")

    def test_sector_rotation_route_uses_india_service(self) -> None:
        response = self.client.get("/api/sector-rotation")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["sectors"][0]["sector"], "Information Technology")

    def test_watchlists_route_reads_persisted_state(self) -> None:
        response = self.client.get("/api/watchlists")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["active_watchlist_id"], "wl-1")
        self.assertEqual(response.json()["watchlists"][0]["symbols"], ["INFY", "TCS"])

    def test_watchlists_route_saves_state(self) -> None:
        payload = {
            "market": "india",
            "active_watchlist_id": "wl-2",
            "watchlists": [
                {"id": "wl-2", "name": "Breakouts", "color": "#00a389", "symbols": ["DIXON", "CGPOWER"]},
            ],
        }

        response = self.client.put("/api/watchlists", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["active_watchlist_id"], "wl-2")
        self.assertEqual(self.service.watchlists_state.watchlists[0].name, "Breakouts")
