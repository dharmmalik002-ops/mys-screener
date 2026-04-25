from __future__ import annotations

import json
import threading
from typing import Protocol

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some deployments
    psycopg = None

from app.models.market import WatchlistItem, WatchlistsStateResponse


def merge_watchlists_state(
    existing: WatchlistsStateResponse | None,
    incoming: WatchlistsStateResponse,
) -> WatchlistsStateResponse:
    """Apply PUT semantics — the incoming payload is authoritative.

    The frontend always sends the full canonical state on save (every
    watchlist with its complete symbol list), so a delete arrives as an
    "absence". The previous implementation unioned existing + incoming,
    which silently resurrected anything the user had removed. This
    rewrite keeps only what the client sent.

    `existing` is still consulted for one purpose: choosing a fallback
    `active_watchlist_id` if the incoming payload doesn't specify one.
    """
    if existing is None:
        return incoming.model_copy(deep=True)

    incoming_ids = {item.id for item in incoming.watchlists}
    merged_watchlists = [item.model_copy(deep=True) for item in incoming.watchlists]

    active_watchlist_id = incoming.active_watchlist_id if incoming.active_watchlist_id in incoming_ids else None
    if active_watchlist_id is None and existing.active_watchlist_id in incoming_ids:
        active_watchlist_id = existing.active_watchlist_id
    if active_watchlist_id is None and merged_watchlists:
        active_watchlist_id = merged_watchlists[0].id

    updated_at = incoming.updated_at if incoming.updated_at >= existing.updated_at else existing.updated_at
    return WatchlistsStateResponse(
        market=incoming.market,
        updated_at=updated_at,
        active_watchlist_id=active_watchlist_id,
        watchlists=merged_watchlists,
    )


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
        return bool(self._database_url) and psycopg is not None

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
        if psycopg is None:
            raise RuntimeError("psycopg is not installed")
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

        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute(
                "SELECT payload FROM watchlists_state WHERE market = %s",
                (self._normalize_market(state.market),),
            )
            row = cursor.fetchone()
            existing_state: WatchlistsStateResponse | None = None
            if row is not None:
                payload = row[0]
                normalized_payload = payload if isinstance(payload, (dict, list)) else json.loads(str(payload))
                existing_state = WatchlistsStateResponse.model_validate(normalized_payload)
            merged_state = merge_watchlists_state(existing_state, state)
            payload = merged_state.model_dump(mode="json")
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
                    self._normalize_market(merged_state.market),
                    merged_state.updated_at,
                    merged_state.active_watchlist_id,
                    json.dumps(payload),
                ),
            )
        self._set_cached_state(merged_state)
        return merged_state
