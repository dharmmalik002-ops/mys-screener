from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some deployments
    psycopg = None

logger = logging.getLogger(__name__)


class ScanHistoryStore:
    """Rolling per-session-date log of scan results.

    Lets a scanner show "everything that triggered in the last N sessions"
    instead of only today's hits. Each session date is written once
    (first-write-wins, so later requests with tweaked thresholds don't
    rewrite the day's pinned list) and dates beyond the retention window are
    pruned automatically.

    Storage: Postgres (same DATABASE_URL as the watchlists store) when
    configured — survives HF Space rebuilds — with a JSON file fallback so
    local/dev deployments work unchanged.
    """

    def __init__(self, database_url: str | None, file_path: Path, *, keep_dates: int = 15) -> None:
        self._database_url = str(database_url or "").strip() or None
        self._file_path = file_path
        self._keep_dates = max(1, int(keep_dates))
        self._schema_ready = False
        self._lock = threading.Lock()

    # ---- Postgres backend -------------------------------------------------

    def _postgres_enabled(self) -> bool:
        return bool(self._database_url) and psycopg is not None

    def _connect(self):
        return psycopg.connect(self._database_url, autocommit=True, connect_timeout=10)

    def _ensure_schema(self, cursor) -> None:
        if self._schema_ready:
            return
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_history (
                scan_key TEXT NOT NULL,
                session_date DATE NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (scan_key, session_date)
            )
            """
        )
        self._schema_ready = True

    # ---- File backend -----------------------------------------------------

    def _read_file(self) -> dict:
        try:
            if not self._file_path.exists():
                return {}
            loaded = json.loads(self._file_path.read_text(encoding="utf-8"))
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}

    def _write_file(self, store: dict) -> None:
        try:
            self._file_path.parent.mkdir(parents=True, exist_ok=True)
            self._file_path.write_text(json.dumps(store, separators=(",", ":")), encoding="utf-8")
        except Exception as exc:
            logger.info("scan-history file write failed: %s", exc)

    # ---- Public API ---------------------------------------------------------

    def _retention_limit(self, keep_dates: int | None = None) -> int:
        return max(1, int(keep_dates if keep_dates is not None else self._keep_dates))

    def record_once(self, scan_key: str, session_date: str, items: list[dict], *, keep_dates: int | None = None) -> None:
        """Pin ``items`` as the results for ``session_date`` unless that date
        is already recorded. Prunes dates beyond the retention window."""
        if not session_date:
            return
        retention_limit = self._retention_limit(keep_dates)
        try:
            if self._postgres_enabled():
                with self._connect() as conn, conn.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        INSERT INTO scan_history (scan_key, session_date, payload)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (scan_key, session_date) DO NOTHING
                        """,
                        (scan_key, session_date, json.dumps(items)),
                    )
                    cursor.execute(
                        """
                        DELETE FROM scan_history
                        WHERE scan_key = %s AND session_date NOT IN (
                            SELECT session_date FROM scan_history
                            WHERE scan_key = %s
                            ORDER BY session_date DESC
                            LIMIT %s
                        )
                        """,
                        (scan_key, scan_key, retention_limit),
                    )
                return
        except Exception as exc:
            logger.info("scan-history postgres write failed (falling back to file): %s", exc)

        with self._lock:
            store = self._read_file()
            per_scan = store.setdefault(scan_key, {})
            if session_date not in per_scan:
                per_scan[session_date] = items
            kept = sorted(per_scan.keys(), reverse=True)[: retention_limit]
            store[scan_key] = {d: per_scan[d] for d in kept}
            self._write_file(store)

    def load(self, scan_key: str, *, keep_dates: int | None = None) -> dict[str, list[dict]]:
        """Return {session_date_iso: items} for the retained window."""
        retention_limit = self._retention_limit(keep_dates)
        try:
            if self._postgres_enabled():
                with self._connect() as conn, conn.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        SELECT session_date, payload FROM scan_history
                        WHERE scan_key = %s
                        ORDER BY session_date DESC
                        LIMIT %s
                        """,
                        (scan_key, retention_limit),
                    )
                    rows = cursor.fetchall()
                result: dict[str, list[dict]] = {}
                for session_date, payload in rows:
                    items = payload if isinstance(payload, list) else json.loads(payload)
                    result[str(session_date)] = items if isinstance(items, list) else []
                return result
        except Exception as exc:
            logger.info("scan-history postgres read failed (falling back to file): %s", exc)

        per_scan = self._read_file().get(scan_key)
        if not isinstance(per_scan, dict):
            return {}
        kept = sorted(per_scan.keys(), reverse=True)[: retention_limit]
        return {d: per_scan[d] if isinstance(per_scan[d], list) else [] for d in kept}
