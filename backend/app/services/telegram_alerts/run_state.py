"""Digest send-once bookkeeping.

The HF scheduler and the GitHub backup workflow can fire concurrently. A
read-modify-write on a JSON blob is a lost-update race (both read
``last_sent=null``, both send), so the claim is an INSERT against a unique
constraint -- the only race-safe primitive available here.

``try_claim`` also implements a CRASH LEASE: a run that claimed the slot and
then died (an HF restart mid-digest) becomes reclaimable after
``LEASE_MINUTES``, while a run that COMPLETED never does. Combined with
``mark_scanner_sent`` checkpointing, an interrupted digest resumes instead of
re-sending.

Without DATABASE_URL this degrades to an ephemeral file, which on HF means the
claim is lost on restart -- so :meth:`is_durable` is False and the digest
refuses externally-triggered runs (see digest.run_digest).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency in some deployments
    psycopg = None

logger = logging.getLogger(__name__)

LEASE_MINUTES = 20

STATUS_IN_FLIGHT = "in-flight"
STATUS_COMPLETE = "complete"


@dataclass
class DigestRun:
    chat_id: int
    session_date: str
    run_id: str
    status: str = STATUS_IN_FLIGHT
    sent_scan_ids: list[str] = field(default_factory=list)
    photo_count: int = 0
    claimed_at: str | None = None
    completed_at: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.status == STATUS_COMPLETE


class DigestRunState:
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

    # ---- backend ----------------------------------------------------------

    def is_enabled(self) -> bool:
        return bool(self._database_url) and psycopg is not None

    def is_durable(self) -> bool:
        return self.is_enabled()

    def _connect(self):
        return psycopg.connect(
            self._database_url, autocommit=True, connect_timeout=self._connect_timeout_seconds
        )

    def _ensure_schema(self, cursor) -> None:
        if self._schema_ready:
            return
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS telegram_digest_runs (
                chat_id       BIGINT      NOT NULL,
                session_date  DATE        NOT NULL,
                run_id        TEXT        NOT NULL,
                status        TEXT        NOT NULL DEFAULT 'in-flight',
                sent_scan_ids JSONB       NOT NULL DEFAULT '[]'::jsonb,
                photo_count   INT         NOT NULL DEFAULT 0,
                claimed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at  TIMESTAMPTZ,
                PRIMARY KEY (chat_id, session_date)
            )
            """
        )
        self._schema_ready = True

    # ---- public API -------------------------------------------------------

    def try_claim(self, chat_id: int, session_date: str, run_id: str) -> DigestRun | None:
        """Claim the (chat, session) slot. Returns the run when this caller owns
        it, or None when someone else does / it already completed."""
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        INSERT INTO telegram_digest_runs (chat_id, session_date, run_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (chat_id, session_date) DO UPDATE
                            SET run_id = EXCLUDED.run_id, claimed_at = NOW()
                            WHERE telegram_digest_runs.status = %s
                              AND telegram_digest_runs.claimed_at
                                  < NOW() - (%s * INTERVAL '1 minute')
                        RETURNING run_id, status, sent_scan_ids, photo_count
                        """,
                        (chat_id, session_date, run_id, STATUS_IN_FLIGHT, LEASE_MINUTES),
                    )
                    row = cursor.fetchone()
                if row is None:
                    return None
                return DigestRun(
                    chat_id=chat_id,
                    session_date=session_date,
                    run_id=str(row[0]),
                    status=str(row[1]),
                    sent_scan_ids=list(row[2] or []),
                    photo_count=int(row[3] or 0),
                )
            except Exception as exc:
                logger.warning("digest claim postgres failed (falling back to file): %s", exc)

        return self._file_claim(chat_id, session_date, run_id)

    def get(self, chat_id: int, session_date: str) -> DigestRun | None:
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        SELECT run_id, status, sent_scan_ids, photo_count, claimed_at, completed_at
                        FROM telegram_digest_runs WHERE chat_id = %s AND session_date = %s
                        """,
                        (chat_id, session_date),
                    )
                    row = cursor.fetchone()
                if row is None:
                    return None
                return DigestRun(
                    chat_id=chat_id,
                    session_date=session_date,
                    run_id=str(row[0]),
                    status=str(row[1]),
                    sent_scan_ids=list(row[2] or []),
                    photo_count=int(row[3] or 0),
                    claimed_at=row[4].isoformat() if row[4] else None,
                    completed_at=row[5].isoformat() if row[5] else None,
                )
            except Exception as exc:
                logger.info("digest run read failed: %s", exc)

        record = self._read_file().get(self._key(chat_id, session_date))
        return self._from_record(chat_id, session_date, record) if record else None

    def mark_scanner_sent(
        self, chat_id: int, session_date: str, scan_id: str, *, photos: int = 0
    ) -> None:
        """Checkpoint after a scanner's messages land, so a resumed run skips
        what already went out."""
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        UPDATE telegram_digest_runs
                        SET sent_scan_ids = (
                                SELECT jsonb_agg(DISTINCT value)
                                FROM jsonb_array_elements(sent_scan_ids || to_jsonb(%s::text)) AS value
                            ),
                            photo_count = photo_count + %s
                        WHERE chat_id = %s AND session_date = %s
                        """,
                        (scan_id, int(photos), chat_id, session_date),
                    )
                return
            except Exception as exc:
                logger.warning("digest checkpoint failed: %s", exc)

        self._file_update(
            chat_id,
            session_date,
            lambda record: {
                **record,
                "sent_scan_ids": sorted(set(record.get("sent_scan_ids", [])) | {scan_id}),
                "photo_count": int(record.get("photo_count", 0)) + int(photos),
            },
        )

    def mark_complete(self, chat_id: int, session_date: str) -> None:
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        UPDATE telegram_digest_runs
                        SET status = %s, completed_at = NOW()
                        WHERE chat_id = %s AND session_date = %s
                        """,
                        (STATUS_COMPLETE, chat_id, session_date),
                    )
                return
            except Exception as exc:
                logger.warning("digest completion write failed: %s", exc)

        self._file_update(
            chat_id,
            session_date,
            lambda record: {
                **record,
                "status": STATUS_COMPLETE,
                "completed_at": _now_iso(),
            },
        )

    def release(self, chat_id: int, session_date: str) -> None:
        """Give the slot back after a run that decided not to send anything, so
        a later ladder slot can retry immediately rather than waiting out the
        lease."""
        if self.is_enabled():
            try:
                with self._connect() as connection, connection.cursor() as cursor:
                    self._ensure_schema(cursor)
                    cursor.execute(
                        """
                        DELETE FROM telegram_digest_runs
                        WHERE chat_id = %s AND session_date = %s AND status = %s
                        """,
                        (chat_id, session_date, STATUS_IN_FLIGHT),
                    )
                return
            except Exception as exc:
                logger.info("digest release failed: %s", exc)

        store = self._read_file()
        key = self._key(chat_id, session_date)
        if store.get(key, {}).get("status") == STATUS_IN_FLIGHT:
            store.pop(key, None)
            self._write_file(store)

    # ---- file backend -----------------------------------------------------

    @staticmethod
    def _key(chat_id: int, session_date: str) -> str:
        return f"{int(chat_id)}:{session_date}"

    @staticmethod
    def _from_record(chat_id: int, session_date: str, record: dict) -> DigestRun:
        return DigestRun(
            chat_id=chat_id,
            session_date=session_date,
            run_id=str(record.get("run_id") or ""),
            status=str(record.get("status") or STATUS_IN_FLIGHT),
            sent_scan_ids=list(record.get("sent_scan_ids") or []),
            photo_count=int(record.get("photo_count") or 0),
            claimed_at=record.get("claimed_at"),
            completed_at=record.get("completed_at"),
        )

    def _file_claim(self, chat_id: int, session_date: str, run_id: str) -> DigestRun | None:
        with self._lock:
            store = self._read_file()
            key = self._key(chat_id, session_date)
            record = store.get(key)
            if record:
                if record.get("status") == STATUS_COMPLETE:
                    return None
                claimed_at = record.get("claimed_at")
                if claimed_at and not _lease_expired(claimed_at):
                    return None
                record = {**record, "run_id": run_id, "claimed_at": _now_iso()}
            else:
                record = {
                    "run_id": run_id,
                    "status": STATUS_IN_FLIGHT,
                    "sent_scan_ids": [],
                    "photo_count": 0,
                    "claimed_at": _now_iso(),
                }
            store[key] = record
            self._write_file(store)
            return self._from_record(chat_id, session_date, record)

    def _file_update(self, chat_id: int, session_date: str, mutate) -> None:
        with self._lock:
            store = self._read_file()
            key = self._key(chat_id, session_date)
            if key not in store:
                return
            store[key] = mutate(store[key])
            self._write_file(store)

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
            temp = self._file_path.with_suffix(f".{os.getpid()}.tmp")
            temp.write_text(json.dumps(store, indent=2), encoding="utf-8")
            os.replace(temp, self._file_path)
        except Exception as exc:
            logger.info("digest run-state file write failed: %s", exc)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lease_expired(claimed_at: str) -> bool:
    try:
        claimed = datetime.fromisoformat(claimed_at)
    except (TypeError, ValueError):
        return True
    if claimed.tzinfo is None:
        claimed = claimed.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - claimed > timedelta(minutes=LEASE_MINUTES)
