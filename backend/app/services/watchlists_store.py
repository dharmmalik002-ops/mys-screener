from __future__ import annotations

import json
import threading
from typing import Protocol

import psycopg

from app.models.market import WatchlistsStateResponse


class WatchlistsStateStore(Protocol):
    def is_enabled(self) -> bool:
        ...

    def load_state(self, market: str) -> WatchlistsStateResponse | None:
        ...

    def save_state(self, state: WatchlistsStateResponse) -> WatchlistsStateResponse:
        ...


class PostgresWatchlistsStore:
    def __init__(self, database_url: str | None, *, connect_timeout_seconds: int = 10) -> None:
        self._database_url = str(database_url or "").strip() or None
        self._connect_timeout_seconds = max(1, int(connect_timeout_seconds or 10))
        self._schema_ready = False
        self._schema_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._state_cache: dict[str, WatchlistsStateResponse] = {}

    def is_enabled(self) -> bool:
        return bool(self._database_url)

    @staticmethod
    def _normalize_market(market: str) -> str:
        return str(market or "").strip().lower()

    @staticmethod
    def _copy_state(state: WatchlistsStateResponse) -> WatchlistsStateResponse:
        return state.model_copy(deep=True)

    def _get_cached_state(self, market: str) -> WatchlistsStateResponse | None:
        normalized_market = self._normalize_market(market)
        with self._cache_lock:
            cached = self._state_cache.get(normalized_market)
            return self._copy_state(cached) if cached is not None else None

    def _set_cached_state(self, state: WatchlistsStateResponse) -> None:
        normalized_market = self._normalize_market(state.market)
        with self._cache_lock:
            self._state_cache[normalized_market] = self._copy_state(state)

    def _connect(self) -> psycopg.Connection:
        if not self._database_url:
            raise RuntimeError("DATABASE_URL is not configured")
        return psycopg.connect(
            self._database_url,
            autocommit=True,
            connect_timeout=self._connect_timeout_seconds,
        )

    def _ensure_schema_with_cursor(self, cursor: psycopg.Cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlists_state (
                market TEXT PRIMARY KEY,
                updated_at BIGINT,
                active_watchlist_id TEXT,
                payload JSONB NOT NULL,
                storage_version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                server_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS watchlists_state_updated_at_idx
            ON watchlists_state (updated_at DESC)
            """
        )

    def _ensure_schema(self, cursor: psycopg.Cursor) -> None:
        if not self.is_enabled() or self._schema_ready:
            return

        with self._schema_lock:
            if self._schema_ready:
                return
            self._ensure_schema_with_cursor(cursor)
            self._schema_ready = True

    def load_state(self, market: str) -> WatchlistsStateResponse | None:
        if not self.is_enabled():
            return None

        cached = self._get_cached_state(market)
        if cached is not None:
            return cached

        normalized_market = self._normalize_market(market)
        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute(
                "SELECT payload FROM watchlists_state WHERE market = %s",
                (normalized_market,),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        payload = row[0]
        if isinstance(payload, (dict, list)):
            normalized_payload = payload
        else:
            normalized_payload = json.loads(str(payload))
        state = WatchlistsStateResponse.model_validate(normalized_payload)
        self._set_cached_state(state)
        return self._copy_state(state)

    def save_state(self, state: WatchlistsStateResponse) -> WatchlistsStateResponse:
        if not self.is_enabled():
            return state

        payload = state.model_dump(mode="json")
        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute(
                """
                INSERT INTO watchlists_state (
                    market,
                    updated_at,
                    active_watchlist_id,
                    payload,
                    server_updated_at
                )
                VALUES (%s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (market)
                DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    active_watchlist_id = EXCLUDED.active_watchlist_id,
                    payload = EXCLUDED.payload,
                    server_updated_at = NOW()
                """,
                (
                    self._normalize_market(state.market),
                    state.updated_at,
                    state.active_watchlist_id,
                    json.dumps(payload),
                ),
            )
        self._set_cached_state(state)
        return state
