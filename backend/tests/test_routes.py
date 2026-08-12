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
    BhavcopyStatusResponse,
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

    def get_watchlists_state(self) -> WatchlistsStateResponse:
        return self.watchlists_state

    def save_watchlists_state(self, payload: WatchlistsStateResponse) -> WatchlistsStateResponse:
        self.watchlists_state = payload
        return payload

    async def get_markets_exposure(self) -> dict:
        return {
            "available": True,
            "as_of_session": "2026-08-10",
            "verdict": {"exposure_pct": 25, "band": "Defensive", "win_rate": 24.79,
                        "breakeven_win_rate": 37.5, "clears_breakeven": False},
            "context": {}, "sources": {}, "edge_trend": [],
        }

    async def get_historical_breadth(self, *, universe: str = "Nifty 500", days: int = 250):
        from app.models.market import (
            HistoricalBreadthDataPoint,
            HistoricalBreadthResponse,
            HistoricalUniverseBreadth,
        )
        return HistoricalBreadthResponse(
            universes=[
                HistoricalUniverseBreadth(
                    universe=universe,
                    history=[
                        # a warmup session: 20-DMA exists, 200-SMA does not
                        HistoricalBreadthDataPoint(date="2023-09-11", above_ma20_pct=75.84),
                        HistoricalBreadthDataPoint(
                            date="2026-08-10", above_ma20_pct=58.7, above_ma50_pct=64.08,
                            above_sma200_pct=66.17, new_high_52w_pct=16.99, new_low_52w_pct=1.72,
                        ),
                    ],
                )
            ]
        )

    def get_bhavcopy_status(self) -> BhavcopyStatusResponse:
        return BhavcopyStatusResponse(
            market="india",
            updated=True,
            date="2026-04-23",
            updated_at=datetime(2026, 4, 23, 13, 25, tzinfo=timezone.utc),
            source="BSE",
        )

class MarketsExposureRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        self.service = StubIndiaService()
        app.include_router(build_router({"india": self.service}))
        self.client = TestClient(app)

    def test_exposure_route_returns_the_verdict(self) -> None:
        body = self.client.get("/api/markets/exposure").json()
        self.assertTrue(body["available"])
        self.assertEqual(body["verdict"]["exposure_pct"], 25)
        self.assertEqual(body["verdict"]["band"], "Defensive")

    def test_exposure_namespaced_mirror(self) -> None:
        self.assertEqual(
            self.client.get("/api/india/markets/exposure").json(),
            self.client.get("/api/markets/exposure").json(),
        )

    def test_breadth_history_route_allows_null_warmup_metrics(self) -> None:
        """A moving average that does not exist yet must serialise as null, not 0."""
        body = self.client.get("/api/markets/breadth-history").json()
        history = body["universes"][0]["history"]
        self.assertIsNone(history[0]["above_sma200_pct"])
        self.assertEqual(history[0]["above_ma20_pct"], 75.84)
        self.assertEqual(history[-1]["above_sma200_pct"], 66.17)

    def test_breadth_history_namespaced_mirror(self) -> None:
        self.assertEqual(
            self.client.get("/api/india/markets/breadth-history").status_code, 200
        )


class CoreRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        app = FastAPI()
        self.service = StubIndiaService()
        app.include_router(build_router({"india": self.service}))
        self.client = TestClient(app)

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

    def test_bhavcopy_status_route_returns_today_status(self) -> None:
        response = self.client.get("/api/bhavcopy/status")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["updated"])
        self.assertEqual(response.json()["source"], "BSE")
