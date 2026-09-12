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

    def test_merge_watchlists_state_uses_incoming_as_authoritative(self) -> None:
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

        # Incoming is authoritative: wl-2 was dropped (a delete), wl-1's
        # symbols match incoming exactly (no resurrection of TCS).
        self.assertEqual(merged.active_watchlist_id, "wl-3")
        self.assertEqual([item.id for item in merged.watchlists], ["wl-1", "wl-3"])
        self.assertEqual(merged.watchlists[0].name, "Core Plus")
        self.assertEqual(merged.watchlists[0].symbols, ["INFY", "HDFCBANK"])
        self.assertEqual(merged.watchlists[1].symbols, ["SBIN"])

    def test_merge_watchlists_state_falls_back_to_existing_active_id(self) -> None:
        existing = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY"])],
        )
        incoming = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 19, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id=None,
            watchlists=[WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY", "TCS"])],
        )
        merged = watchlists_store.merge_watchlists_state(existing, incoming)
        self.assertEqual(merged.active_watchlist_id, "wl-1")
        self.assertEqual(merged.watchlists[0].symbols, ["INFY", "TCS"])

    def test_merge_watchlists_state_honours_full_clear(self) -> None:
        existing = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY"])],
        )
        incoming = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 20, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id=None,
            watchlists=[],
        )
        merged = watchlists_store.merge_watchlists_state(existing, incoming)
        self.assertEqual(merged.watchlists, [])
        self.assertIsNone(merged.active_watchlist_id)

    def test_load_state_rereads_the_database_after_another_worker_writes(self) -> None:
        """Production runs two Uvicorn workers against one database.

        A worker that answers reads from its own cache keeps serving the
        state it saw first, so a watchlist deleted through the other worker
        comes back on the next read that lands here. Reads must go to the
        shared database while it is reachable.
        """
        before = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[
                WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY"]),
                WatchlistItem(id="wl-2", name="Doomed", color="#00a389", symbols=["TCS"]),
            ],
        )
        after = before.model_copy(
            deep=True,
            update={"watchlists": [before.watchlists[0]]},  # wl-2 deleted elsewhere
        )

        rows = [[before.model_dump(mode="json")], [after.model_dump(mode="json")]]
        store = watchlists_store.PostgresWatchlistsStore("postgres://example")

        with patch.object(store, "is_enabled", return_value=True), \
                patch.object(store, "_ensure_schema"), \
                patch.object(store, "_connect", side_effect=lambda: _FakeConnection(rows)):
            first = store.load_state("india")
            second = store.load_state("india")

        assert first is not None and second is not None
        self.assertEqual([item.id for item in first.watchlists], ["wl-1", "wl-2"])
        self.assertEqual([item.id for item in second.watchlists], ["wl-1"])

    def test_load_state_falls_back_to_its_cache_when_the_database_is_down(self) -> None:
        state = WatchlistsStateResponse(
            market="india",
            updated_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
            active_watchlist_id="wl-1",
            watchlists=[WatchlistItem(id="wl-1", name="Core", color="#4f8cff", symbols=["INFY"])],
        )
        store = watchlists_store.PostgresWatchlistsStore("postgres://example")

        with patch.object(store, "is_enabled", return_value=True), \
                patch.object(store, "_ensure_schema"), \
                patch.object(store, "_connect", side_effect=lambda: _FakeConnection([[state.model_dump(mode="json")]])):
            self.assertIsNotNone(store.load_state("india"))

        with patch.object(store, "is_enabled", return_value=True), \
                patch.object(store, "_connect", side_effect=RuntimeError("database unreachable")):
            cached = store.load_state("india")

        assert cached is not None
        self.assertEqual([item.id for item in cached.watchlists], ["wl-1"])


class _FakeCursor:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def execute(self, *_args, **_kwargs) -> None:
        return None

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc) -> None:
        return None


class _FakeConnection:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._rows)

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc) -> None:
        return None
