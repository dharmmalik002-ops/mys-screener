"""Backend for the Course page: historical bars for the case-study replays and
the learner's saved progress.

Bars
----
The case studies are real trades from 2021-2026. `/api/chart` keeps ~500 daily
bars whatever the timeframe (CLAUDE.md gotcha 149), so it cannot draw January
2021. `fetch_bars` asks Yahoo for an explicit date window instead (`.NS`, then
`.BO`) and keeps the answer on disk: a window that ended more than a week ago
can never change, so it is cached for good, and a window still running is
refetched after `OPEN_WINDOW_TTL_S`.

Prices are as Yahoo serves them, i.e. restated for later splits and bonuses. The
page rescales them onto the price he quoted at entry when the two disagree, so
his buy/sell marks land on the candles; that is a display decision and lives in
the frontend.

Progress
--------
One opaque JSON blob owned by the frontend (lessons studied, notes, flashcard
schedule, drill scores), stored like the trade journal: Postgres when
DATABASE_URL is set (the Space's disk does not survive a restart), else a file
in APP_STATE_DIR. The browser never pushes before it has read the server copy
(gotcha 128's lesson), and `save` refuses a payload that would silently throw
away most of what is stored.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
MAX_WINDOW_DAYS = 3 * 366
CLOSED_AFTER_DAYS = 7
OPEN_WINDOW_TTL_S = 6 * 3600
SYMBOL_RE = re.compile(r"^[A-Z0-9&-]{1,20}$")
IST = timezone(timedelta(hours=5, minutes=30))

Bar = dict[str, float]
Fetcher = Callable[[str, date, date], list[Bar]]


class CourseBarsError(ValueError):
    """A request the bars endpoint refuses (bad symbol or window)."""


def clean_symbol(symbol: str) -> str:
    """'TVSMOTOR (short, futures)' / 'IRCTC FEB FUT' -> the bare NSE ticker."""
    match = re.match(r"[A-Z0-9&-]+", (symbol or "").strip().upper())
    ticker = match.group(0) if match else ""
    if not SYMBOL_RE.match(ticker):
        raise CourseBarsError(f"Not a ticker: {symbol!r}")
    return ticker


def parse_window(start: str, end: str, today: date | None = None) -> tuple[date, date]:
    try:
        lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError as exc:
        raise CourseBarsError("start and end must be YYYY-MM-DD") from exc
    today = today or datetime.now(IST).date()
    hi = min(hi, today)
    if lo >= hi:
        raise CourseBarsError("start must be before end")
    if (hi - lo).days > MAX_WINDOW_DAYS:
        raise CourseBarsError(f"window longer than {MAX_WINDOW_DAYS} days")
    return lo, hi


def _yahoo_bars(ticker: str, lo: date, hi: date) -> list[Bar]:
    import httpx

    period1 = int(datetime(lo.year, lo.month, lo.day, tzinfo=timezone.utc).timestamp())
    period2 = int(datetime(hi.year, hi.month, hi.day, tzinfo=timezone.utc).timestamp()) + 86400
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json,text/plain,*/*", "Referer": "https://finance.yahoo.com/"}
    with httpx.Client(timeout=20, headers=headers, follow_redirects=True) as client:
        response = client.get(
            f"{YAHOO_CHART_URL}/{ticker}",
            params={"period1": period1, "period2": period2, "interval": "1d", "includePrePost": "false", "events": "div,splits"},
        )
        response.raise_for_status()
        payload = response.json()
    return parse_yahoo_chart(payload)


def parse_yahoo_chart(payload: dict[str, Any]) -> list[Bar]:
    """Yahoo's chart JSON -> bars at the IST session date (midnight UTC stamps,
    the same `time` convention the Chart Gym uses). Rows with any missing price
    are dropped rather than zero-filled — a missing price is not a zero price."""
    result = ((payload or {}).get("chart") or {}).get("result") or []
    if not result:
        return []
    block = result[0]
    stamps = block.get("timestamp") or []
    quote = ((block.get("indicators") or {}).get("quote") or [{}])[0]
    out: dict[int, Bar] = {}
    for i, stamp in enumerate(stamps):
        try:
            o, h, l, c = (quote[k][i] for k in ("open", "high", "low", "close"))
        except (KeyError, IndexError, TypeError):
            continue
        if None in (o, h, l, c) or min(o, h, l, c) <= 0:
            continue
        day = datetime.fromtimestamp(int(stamp), IST).date()
        t = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
        vol = (quote.get("volume") or [None] * len(stamps))[i]
        out[t] = {
            "time": t,
            "open": round(float(o), 4),
            "high": round(float(h), 4),
            "low": round(float(l), 4),
            "close": round(float(c), 4),
            "volume": float(vol or 0),
        }
    return [out[t] for t in sorted(out)]


def default_fetcher(symbol: str, lo: date, hi: date) -> list[Bar]:
    for suffix in (".NS", ".BO"):
        try:
            bars = _yahoo_bars(f"{symbol}{suffix}", lo, hi)
        except Exception as exc:  # network, 404 for a symbol that is not on this exchange
            logger.info("course bars: %s%s failed (%s)", symbol, suffix, exc)
            continue
        if bars:
            return bars
    return []


class CourseBars:
    """Date-window bars with a disk cache. Thread-safe enough for two workers:
    the worst case is two concurrent fetches writing the same file."""

    def __init__(self, cache_dir: Path, fetcher: Fetcher | None = None) -> None:
        self._dir = Path(cache_dir)
        self._fetch = fetcher or default_fetcher
        self._lock = threading.Lock()

    def _path(self, symbol: str, lo: date, hi: date) -> Path:
        return self._dir / f"{symbol}_{lo.isoformat()}_{hi.isoformat()}.json"

    def get(self, symbol: str, start: str, end: str, today: date | None = None) -> dict[str, Any]:
        ticker = clean_symbol(symbol)
        lo, hi = parse_window(start, end, today)
        today = today or datetime.now(IST).date()
        closed = (today - hi).days > CLOSED_AFTER_DAYS
        path = self._path(ticker, lo, hi)
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
            fresh = closed or (time.time() - path.stat().st_mtime) < OPEN_WINDOW_TTL_S
            if cached.get("bars") and fresh:
                return cached
        except (OSError, ValueError):
            pass
        bars = self._fetch(ticker, lo, hi)
        document = {"symbol": ticker, "start": lo.isoformat(), "end": hi.isoformat(), "bars": bars, "source": "yahoo"}
        if bars:  # never cache a failure: the next request should try again
            with self._lock:
                try:
                    self._dir.mkdir(parents=True, exist_ok=True)
                    tmp = path.with_suffix(".tmp")
                    tmp.write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
                    tmp.replace(path)
                except OSError as exc:
                    logger.warning("course bars: could not cache %s (%s)", path.name, exc)
        return document


# --------------------------------------------------------------------------- progress

PROGRESS_SECTIONS = ("done", "notes", "cards", "scores")
# A save may lose this many entries without saying so (one un-ticked lesson, a
# cleared note). Anything larger needs `allowShrink` — the "reset" buttons send it.
MAX_SILENT_LOSS = 5


class ProgressShrinkRefused(ValueError):
    pass


def _size(doc: dict[str, Any]) -> int:
    return sum(len(doc.get(k) or {}) for k in ("done", "notes", "cards"))


def normalise_progress(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("progress must be a JSON object")
    out: dict[str, Any] = {}
    for key in PROGRESS_SECTIONS:
        value = payload.get(key) or {}
        if not isinstance(value, dict):
            raise ValueError(f"progress.{key} must be an object")
        out[key] = value
    out["notes"] = {k: str(v)[:4000] for k, v in out["notes"].items() if str(v).strip()}
    return out


class CourseProgressStore:
    _ROW_ID = "default"

    def __init__(self, database_url: str | None, state_dir: Path) -> None:
        self._db = (database_url or "").strip() or None
        self._path = Path(state_dir) / "data" / "course_progress.json"
        self._ready = False
        self._lock = threading.Lock()

    def _use_db(self) -> bool:
        if not self._db:
            return False
        try:
            import psycopg  # noqa: F401
        except Exception:
            return False
        return True

    def _connect(self):
        import psycopg

        return psycopg.connect(self._db, autocommit=True, connect_timeout=10)

    def _ensure(self, cur) -> None:
        if self._ready:
            return
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS course_progress (
                id TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                server_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        self._ready = True

    def load(self) -> dict[str, Any]:
        doc: dict[str, Any] | None = None
        if self._use_db():
            try:
                with self._connect() as conn, conn.cursor() as cur:
                    self._ensure(cur)
                    cur.execute("SELECT payload FROM course_progress WHERE id = %s", (self._ROW_ID,))
                    row = cur.fetchone()
                if row:
                    doc = row[0] if isinstance(row[0], dict) else json.loads(str(row[0]))
            except Exception as exc:
                logger.warning("course progress: database read failed, using the file (%s)", exc)
        if doc is None:
            try:
                doc = json.loads(self._path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                doc = {}
        base = normalise_progress({k: doc.get(k) for k in PROGRESS_SECTIONS})
        base["updated_at"] = doc.get("updated_at")
        return base

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        allow_shrink = bool(isinstance(payload, dict) and payload.get("allowShrink"))
        doc = normalise_progress(payload)
        with self._lock:
            current = self.load()
            if not allow_shrink and _size(current) - _size(doc) > MAX_SILENT_LOSS:
                raise ProgressShrinkRefused(
                    f"This save would drop {_size(current) - _size(doc)} saved items; reload the course page first."
                )
            doc["updated_at"] = datetime.now(timezone.utc).isoformat()
            if self._use_db():
                try:
                    with self._connect() as conn, conn.cursor() as cur:
                        self._ensure(cur)
                        cur.execute(
                            """
                            INSERT INTO course_progress (id, payload, server_updated_at)
                            VALUES (%s, %s::jsonb, NOW())
                            ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, server_updated_at = NOW()
                            """,
                            (self._ROW_ID, json.dumps(doc)),
                        )
                except Exception as exc:
                    logger.warning("course progress: database write failed, file only (%s)", exc)
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self._path.with_suffix(".tmp")
                tmp.write_text(json.dumps(doc), encoding="utf-8")
                tmp.replace(self._path)
            except OSError as exc:
                logger.warning("course progress: file write failed (%s)", exc)
        return doc
