"""Tell the user when a watchlist stock closes through the level they set.

Every watchlist note can carry a ``trigger`` (the price that makes the idea
live) and a ``stop`` (where it is wrong). The Watchlists page already shows
"Price has reached your trigger", but only to someone looking at it. This runs
once each evening after the end-of-day closes land and sends one Telegram
message listing the crossings, so the list does its job without being opened.

Rules:

* A crossing is judged on **closes**, newest against the one before, from the
  committed ``close_history.json`` -- the same closes every scanner reads. An
  intraday poke through a level that closed back is not a crossing.
* A trigger fires in whichever direction price crossed it (the note says
  "above it for a breakout, below it for a pullback -- direction is the
  reader's"). A stop fires only on a close below it.
* Each (symbol, kind, level, session) is sent once, recorded in
  ``APP_STATE_DIR/watchlist_alerts.json``, so a re-run or a second worker
  never repeats a message.
* Without ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_ALERT_CHAT_ID`` nothing is sent
  and nothing is marked sent; the run is still recorded so the status endpoint
  can say alerts are not configured.
"""

from __future__ import annotations

import html
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

STATE_FILENAME = "watchlist_alerts.json"
# Dedupe keys kept: a few months of crossings for any realistic watchlist.
MAX_SENT_KEYS = 2000
CLOSE_HISTORY_PATH = Path(__file__).resolve().parents[2] / "data" / "close_history.json"


@dataclass(frozen=True)
class Crossing:
    symbol: str
    kind: str  # "trigger" | "stop"
    level: float
    direction: str  # "up" | "down"
    prev_close: float
    close: float
    session: str
    watchlist: str

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.kind}|{self.level:g}|{self.session}"


def _session_from_epoch(value: Any) -> str | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def load_last_two_closes(path: Path = CLOSE_HISTORY_PATH) -> dict[str, tuple[str, float, float]]:
    """symbol -> (session, previous close, close). Read fresh every run: the
    scanners cache this file per process, and a stale copy here would replay
    yesterday's crossings."""
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("watchlist alerts: close history unavailable (%s)", exc)
        return {}
    out: dict[str, tuple[str, float, float]] = {}
    for symbol, row in (doc.get("symbols") or {}).items():
        closes = row.get("closes") if isinstance(row, dict) else None
        session = _session_from_epoch(row.get("last_time")) if isinstance(row, dict) else None
        if not session or not isinstance(closes, list) or len(closes) < 2:
            continue
        try:
            prev, last = float(closes[-2]), float(closes[-1])
        except (TypeError, ValueError):
            continue
        if prev > 0 and last > 0:
            out[str(symbol).upper()] = (session, prev, last)
    return out


def _level(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def find_crossings(state: Any, closes: dict[str, tuple[str, float, float]]) -> list[Crossing]:
    """Every trigger or stop the latest close went through. ``state`` is a
    ``WatchlistsStateResponse`` (or anything shaped like one)."""
    found: list[Crossing] = []
    seen: set[str] = set()
    for watchlist in getattr(state, "watchlists", None) or []:
        notes = getattr(watchlist, "notes", None) or {}
        for raw_symbol, note in notes.items():
            symbol = str(raw_symbol).upper()
            row = closes.get(symbol)
            if row is None:
                continue
            session, prev, last = row
            trigger = _level(getattr(note, "trigger", None))
            stop = _level(getattr(note, "stop", None))
            candidates: list[Crossing] = []
            if trigger is not None:
                if prev < trigger <= last:
                    candidates.append(Crossing(symbol, "trigger", trigger, "up", prev, last, session, watchlist.name))
                elif prev > trigger >= last:
                    candidates.append(Crossing(symbol, "trigger", trigger, "down", prev, last, session, watchlist.name))
            if stop is not None and prev > stop >= last:
                candidates.append(Crossing(symbol, "stop", stop, "down", prev, last, session, watchlist.name))
            for crossing in candidates:
                # The same symbol and level on two lists is one event.
                if crossing.key not in seen:
                    seen.add(crossing.key)
                    found.append(crossing)
    return found


def _price(value: float) -> str:
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def format_message(crossings: Iterable[Crossing], app_url: str = "https://my-screener-theta.vercel.app/") -> str:
    crossings = list(crossings)
    session = crossings[0].session if crossings else ""
    lines = [f"<b>Watchlist alerts · close of {html.escape(session)}</b>"]
    for c in sorted(crossings, key=lambda x: (x.kind != "stop", x.symbol)):
        if c.kind == "stop":
            what = f"closed below your stop {_price(c.level)}"
        else:
            side = "above" if c.direction == "up" else "below"
            what = f"closed {side} your trigger {_price(c.level)}"
        lines.append(
            f"• <b>{html.escape(c.symbol)}</b> {_price(c.close)} (was {_price(c.prev_close)}) — {what}"
            f" <i>[{html.escape(c.watchlist)}]</i>"
        )
    lines.append(f'\n<a href="{html.escape(app_url)}">Open watchlists</a>')
    return "\n".join(lines)


def _read_state(state_dir: Path | None) -> dict[str, Any]:
    if state_dir is None:
        return {}
    try:
        doc = json.loads((state_dir / STATE_FILENAME).read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_state(state_dir: Path | None, doc: dict[str, Any]) -> None:
    if state_dir is None:
        return
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        tmp = state_dir / (STATE_FILENAME + ".tmp")
        tmp.write_text(json.dumps(doc, indent=1), encoding="utf-8")
        tmp.replace(state_dir / STATE_FILENAME)
    except OSError as exc:
        logger.warning("watchlist alerts: could not save state (%s)", exc)


def telegram_settings() -> tuple[str | None, int | None]:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip() or None
    raw_chat = (os.environ.get("TELEGRAM_ALERT_CHAT_ID") or "").strip()
    try:
        chat_id = int(raw_chat) if raw_chat else None
    except ValueError:
        chat_id = None
    return token, chat_id


async def run_watchlist_alerts(
    state: Any,
    state_dir: Path | None,
    *,
    closes: dict[str, tuple[str, float, float]] | None = None,
    client: Any = None,
    chat_id: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Find new crossings, send them, remember them. Returns the run summary
    (also stored, and served by ``/api/watchlists/alerts``)."""
    if client is None or chat_id is None:
        token, env_chat = telegram_settings()
        if client is None and token:
            from app.services.telegram_alerts.client import TelegramClient

            client = TelegramClient(token)
        chat_id = chat_id if chat_id is not None else env_chat
    configured = bool(client is not None and getattr(client, "available", True) and chat_id is not None)

    closes = closes if closes is not None else load_last_two_closes()
    crossings = find_crossings(state, closes)
    stored = _read_state(state_dir)
    sent_keys: list[str] = list(stored.get("sent_keys") or [])
    sent_set = set(sent_keys)
    fresh = [c for c in crossings if c.key not in sent_set]

    status = "no_crossings"
    error: str | None = None
    if fresh and not configured:
        status = "not_configured"
    elif fresh:
        try:
            await client.send_message(chat_id, format_message(fresh))
            sent_keys.extend(c.key for c in fresh)
            status = "sent"
        except Exception as exc:  # never let a Telegram outage break the scheduler
            status = "error"
            error = str(exc)[:300]
            logger.warning("watchlist alerts: send failed (%s)", exc)

    summary = {
        "ran_at": (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(),
        "configured": configured,
        "status": status,
        "error": error,
        "crossings": [asdict(c) for c in crossings],
        "new": len(fresh),
    }
    _write_state(state_dir, {"sent_keys": sent_keys[-MAX_SENT_KEYS:], "last_run": summary})
    return summary


def last_run(state_dir: Path | None) -> dict[str, Any]:
    token, chat_id = telegram_settings()
    return {
        "configured": bool(token and chat_id is not None),
        "last_run": _read_state(state_dir).get("last_run"),
    }
