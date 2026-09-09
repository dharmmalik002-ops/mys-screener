"""Durable storage for the Telegram alert preferences blob.

Postgres primary (survives HF Space rebuilds), JSON file fallback so local dev
and a Space without DATABASE_URL still work. Modelled on
``PostgresJournalStore`` -- same single-row shape, same idempotent schema
creation, same autocommit connection.

IMPORTANT: on Hugging Face the file fallback is EPHEMERAL. Without
DATABASE_URL, scanner selections reset on every Space restart. :meth:`is_durable`
exists so ``/status`` can say so out loud.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

from app.services.telegram_alerts import prefs as prefs_mod

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some deployments
    psycopg = None

logger = logging.getLogger(__name__)


class AlertPrefsStore:
    _ROW_ID = "default"

    def __init__(
        self,
        database_url: str | None,
        file_path: Path,
        *,
        connect_timeout_seconds: int = 10,
    ) -> None:
        self._database_url = str(database_url or "").strip() or None
        self._file_path = Path(file_path)
        self._connect_timeout_seconds = max(1, int(connect_timeout_seconds or 10))
        self._schema_ready = False
        self._lock = threading.Lock()

    # ---- backend selection ------------------------------------------------

    def is_enabled(self) -> bool:
        return bool(self._database_url) and psycopg is not None

    def is_durable(self) -> bool:
        """False means preferences live only on an ephemeral filesystem."""
        return self.is_enabled()

    def _connect(self):
        return psycopg.connect(
            self._database_url,
            autocommit=True,
            connect_timeout=self._connect_timeout_seconds,
        )

    def _ensure_schema(self, cursor) -> None:
        if self._schema_ready:
            return
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_alert_prefs (
                id TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                server_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        self._schema_ready = True

    # ---- public API -------------------------------------------------------

    def load(self) -> dict:
        """Always returns a normalized payload; never raises."""
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        "SELECT payload FROM telegram_alert_prefs WHERE id = %s", (self._ROW_ID,)
                    )
                    row = cursor.fetchone()
                if row is not None:
                    payload = row[0]
                    if not isinstance(payload, dict):
                        payload = json.loads(str(payload))
                    return prefs_mod.normalize_prefs(payload)
                return prefs_mod.empty_prefs()
            except Exception as exc:
                logger.info("telegram prefs postgres read failed (falling back to file): %s", exc)

        return prefs_mod.normalize_prefs(self._read_file())

    def save(self, payload: dict) -> dict:
        """Persist and return the normalized payload. Mirrors to the file even
        when Postgres succeeds, so a later DATABASE_URL outage degrades to the
        last known good state rather than to defaults."""
        normalized = prefs_mod.normalize_prefs(payload)
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        INSERT INTO telegram_alert_prefs (id, payload, server_updated_at)
                        VALUES (%s, %s::jsonb, NOW())
                        ON CONFLICT (id) DO UPDATE SET
                            payload = EXCLUDED.payload,
                            server_updated_at = NOW()
                        """,
                        (self._ROW_ID, json.dumps(normalized)),
                    )
            except Exception as exc:
                logger.info("telegram prefs postgres write failed (falling back to file): %s", exc)
        self._write_file(normalized)
        return normalized

    # ---- file backend -----------------------------------------------------

    def _read_file(self) -> dict:
        try:
            if not self._file_path.exists():
                return {}
            loaded = json.loads(self._file_path.read_text(encoding="utf-8"))
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}

    def _write_file(self, payload: dict) -> None:
        with self._lock:
            try:
                self._file_path.parent.mkdir(parents=True, exist_ok=True)
                # Atomic replace so a crash mid-write cannot leave a truncated
                # blob (the repo's one true atomic-write pattern, free.py:3527).
                temp = self._file_path.with_suffix(f".{os.getpid()}.tmp")
                temp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                os.replace(temp, self._file_path)
            except Exception as exc:
                logger.info("telegram prefs file write failed: %s", exc)
