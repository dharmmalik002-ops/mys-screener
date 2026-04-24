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

    def get_bhavcopy_status(self) -> BhavcopyStatusResponse:
        return BhavcopyStatusResponse(
            market="india",
            updated=True,
            date="2026-04-23",
            updated_at=datetime(2026, 4, 23, 13, 25, tzinfo=timezone.utc),
            source="BSE",
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
