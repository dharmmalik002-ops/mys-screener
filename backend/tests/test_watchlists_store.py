from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import WatchlistItem, WatchlistsStateResponse
from app.services import watchlists_store


class WatchlistsStoreTests(unittest.TestCase):
    def test_store_disables_database_backend_when_psycopg_is_missing(self) -> None:
        state = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY"])],
        )

        with patch.object(watchlists_store, "psycopg", None):
            store = watchlists_store.PostgresWatchlistsStore("postgres://example")

            self.assertFalse(store.is_enabled())
            self.assertIsNone(store.load_state("india"))
            self.assertEqual(store.save_state(state), state)

    def test_merge_watchlists_state_preserves_existing_lists_and_symbols(self) -> None:
        existing = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[
                WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY", "TCS"]),
                WatchlistItem(id="wl-2", name="Leaders", color="#00a389", symbols=["RELIANCE"]),
            ],
        )
        incoming = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 19, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-3",
            watchlists=[
                WatchlistItem(id="wl-1", name="Core Plus", color="#7c5cff", symbols=["INFY", "HDFCBANK"]),
                WatchlistItem(id="wl-3", name="Fresh", color="#ff9f1c", symbols=["SBIN"]),
            ],
        )

        merged = watchlists_store.merge_watchlists_state(existing, incoming)

        self.assertEqual(merged.active_watchlist_id, "wl-3")
        self.assertEqual([item.id for item in merged.watchlists], ["wl-1", "wl-2", "wl-3"])
        self.assertEqual(merged.watchlists[0].name, "Core Plus")
        self.assertEqual(merged.watchlists[0].symbols, ["INFY", "TCS", "HDFCBANK"])
        self.assertEqual(merged.watchlists[1].symbols, ["RELIANCE"])
