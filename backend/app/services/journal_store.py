from __future__ import annotations

import json
import threading

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some deployments
    psycopg = None


class PostgresJournalStore:
    """Durable storage for the trade journal blob, mirroring PostgresWatchlistsStore.

    The journal is a single opaque JSON payload (trades + settings) owned by the
    frontend, not a modeled schema, so this store just persists/retrieves it as-is.
    """

    _ROW_ID = "default"

    def __init__(self, database_url: str | None, *, connect_timeout_seconds: int = 10) -> None:
        self._database_url = str(database_url or "").strip() or None
        self._connect_timeout_seconds = max(1, int(connect_timeout_seconds or 10))
        self._schema_ready = False
        self._schema_lock = threading.Lock()

    def is_enabled(self) -> bool:
        return bool(self._database_url) and psycopg is not None

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

    def _ensure_schema(self, cursor: psycopg.Cursor) -> None:
        if not self.is_enabled() or self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS journal_state (
                    id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    server_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            self._schema_ready = True

    def load_data(self) -> dict | None:
        if not self.is_enabled():
            return None
        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute(
                "SELECT payload FROM journal_state WHERE id = %s",
                (self._ROW_ID,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = row[0]
        return payload if isinstance(payload, dict) else json.loads(str(payload))

    def save_data(self, payload: dict) -> dict:
        if not self.is_enabled():
            return payload
        with self._connect() as connection, connection.cursor() as cursor:
            self._ensure_schema(cursor)
            cursor.execute(
                """
                INSERT INTO journal_state (id, payload, server_updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (id)
                DO UPDATE SET
                    payload = EXCLUDED.payload,
                    server_updated_at = NOW()
                """,
                (self._ROW_ID, json.dumps(payload)),
            )
        return payload
