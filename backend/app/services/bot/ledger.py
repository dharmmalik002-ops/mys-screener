"""The trade database — every trade, what the bot was thinking, how it turned out.

This is the thing a professional keeps and an amateur does not: not a list of
P&L, but a record of the *decision* alongside the outcome, so the two can be
separated afterwards. A won trade taken for a bad reason is a bad trade that
paid, and unless the reasoning was written down at entry there is no way to
tell it apart from skill six months later.

Every row therefore stores the state of the world at entry — regime, breadth,
volatility band, how far the index was off its high, how many external series
were hostile, and the expectancy the bot was working from when it committed.
None of that can be reconstructed later: breadth on 2019-04-11 is not something
you can look up once the day has passed, and a "why did I take this?" answered
from memory is a story, not a record.

Three sources share one schema:

    backtest  the 95,286 simulated trades — the statistical base. Seeded so
              the database is useful on day one instead of in three years.
    paper     signals the bot raised and the user tracked without money.
    live      trades actually taken.

They share a schema so any question can be asked across all of them, and carry
a `source` column so live results are never quietly averaged into simulated
ones. `calibration.py` exists precisely to keep comparing them.

SQLite rather than JSON: this is queried by regime, by strategy, by date range
and by outcome, it grows without bound, and it must survive a crash mid-write.
It lives in `APP_STATE_DIR` beside the trade journal — the same place, for the
same reason: the live rows are the user's own and are never regenerable.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
LEDGER_FILENAME = "bot_ledger.db"

SOURCES = ("backtest", "paper", "live")


@dataclass
class LedgerTrade:
    """One trade with the reasoning attached.

    Field order mirrors the table so the insert path stays a straight tuple —
    at 95k rows the dict-per-row overhead is measurable.
    """

    source: str
    strategy: str
    symbol: str
    signal_day: str
    entry_day: str
    exit_day: str | None
    entry: float
    stop: float
    exit_price: float | None
    exit_reason: str
    sessions_held: int
    r_multiple: float
    net_pct: float
    mae_r: float
    mfe_r: float
    risk_pct: float
    atr_pct_at_entry: float
    # --- what the bot was looking at when it committed ---------------------
    regime: str
    volatility_band: str
    breadth_above_200dma: float | None
    pct_from_52w_high: float | None
    vix_percentile: float | None
    macro_headwinds: int | None
    expected_r: float | None
    thesis: str

    def to_dict(self) -> dict:
        return asdict(self)


DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    source               TEXT NOT NULL,
    strategy             TEXT NOT NULL,
    symbol               TEXT NOT NULL,
    signal_day           TEXT NOT NULL,
    entry_day            TEXT NOT NULL,
    exit_day             TEXT,
    entry                REAL NOT NULL,
    stop                 REAL NOT NULL,
    exit_price           REAL,
    exit_reason          TEXT NOT NULL,
    sessions_held        INTEGER NOT NULL,
    r_multiple           REAL NOT NULL,
    net_pct              REAL NOT NULL,
    mae_r                REAL NOT NULL,
    mfe_r                REAL NOT NULL,
    risk_pct             REAL NOT NULL,
    atr_pct_at_entry     REAL NOT NULL,
    regime               TEXT NOT NULL,
    volatility_band      TEXT NOT NULL,
    breadth_above_200dma REAL,
    pct_from_52w_high    REAL,
    vix_percentile       REAL,
    macro_headwinds      INTEGER,
    expected_r           REAL,
    thesis               TEXT NOT NULL DEFAULT '',
    -- A trade is identified by what produced it. The unique index below turns
    -- a re-seed into a no-op instead of a duplicated statistical base.
    UNIQUE (source, strategy, symbol, entry_day)
);

CREATE INDEX IF NOT EXISTS idx_trades_cell   ON trades (strategy, regime);
CREATE INDEX IF NOT EXISTS idx_trades_entry  ON trades (entry_day);
CREATE INDEX IF NOT EXISTS idx_trades_source ON trades (source);

CREATE TABLE IF NOT EXISTS reviews (
    trade_id       INTEGER PRIMARY KEY REFERENCES trades(id) ON DELETE CASCADE,
    verdict        TEXT NOT NULL,
    tags           TEXT NOT NULL,       -- JSON array
    stop_quality   TEXT NOT NULL,
    exit_quality   TEXT NOT NULL,
    r_left_on_table REAL NOT NULL,
    note           TEXT NOT NULL,
    reviewed_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reviews_verdict ON reviews (verdict);

-- One row per (as_of, cell): the lifecycle of every strategy x regime pairing.
-- This table is what makes the system's evolution auditable — a cell that was
-- confirmed in 2023 and retired in 2025 leaves both rows behind, so "why did
-- it stop trading this?" has an answer instead of a shrug.
CREATE TABLE IF NOT EXISTS cell_status (
    as_of        TEXT NOT NULL,
    strategy     TEXT NOT NULL,
    regime       TEXT NOT NULL,
    status       TEXT NOT NULL,
    trades       INTEGER NOT NULL,
    win_rate     REAL NOT NULL,
    avg_r        REAL NOT NULL,
    payoff       REAL NOT NULL,
    recent_avg_r REAL,
    recent_trades INTEGER,
    note         TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (as_of, strategy, regime)
);
"""

INSERT_COLUMNS = (
    "source", "strategy", "symbol", "signal_day", "entry_day", "exit_day",
    "entry", "stop", "exit_price", "exit_reason", "sessions_held", "r_multiple",
    "net_pct", "mae_r", "mfe_r", "risk_pct", "atr_pct_at_entry", "regime",
    "volatility_band", "breadth_above_200dma", "pct_from_52w_high",
    "vix_percentile", "macro_headwinds", "expected_r", "thesis",
)


def ledger_path(state_dir: Path) -> Path:
    return state_dir / LEDGER_FILENAME


@contextmanager
def connect(state_dir: Path) -> Iterator[sqlite3.Connection]:
    """Open the ledger, creating it if needed."""
    path = ledger_path(state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        # WAL so a read (the UI) never blocks behind a seed, and foreign keys
        # so deleting a trade takes its review with it rather than orphaning it.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(DDL)
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        yield conn
        conn.commit()
    finally:
        conn.close()


def record_trades(conn: sqlite3.Connection, trades: Sequence[LedgerTrade]) -> int:
    """Insert trades, ignoring ones already present. Returns rows added."""
    if not trades:
        return 0
    placeholders = ", ".join("?" for _ in INSERT_COLUMNS)
    sql = (
        f"INSERT OR IGNORE INTO trades ({', '.join(INSERT_COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    rows = [tuple(getattr(t, column) for column in INSERT_COLUMNS) for t in trades]
    before = conn.total_changes
    conn.executemany(sql, rows)
    return conn.total_changes - before


def record_review(
    conn: sqlite3.Connection,
    trade_id: int,
    verdict: str,
    tags: Sequence[str],
    stop_quality: str,
    exit_quality: str,
    r_left_on_table: float,
    note: str,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO reviews
            (trade_id, verdict, tags, stop_quality, exit_quality,
             r_left_on_table, note, reviewed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id, verdict, json.dumps(list(tags)), stop_quality, exit_quality,
            round(float(r_left_on_table), 3), note,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def record_cell_status(conn: sqlite3.Connection, as_of: str, rows: Sequence[dict]) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO cell_status
            (as_of, strategy, regime, status, trades, win_rate, avg_r, payoff,
             recent_avg_r, recent_trades, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                as_of, r["strategy"], r["regime"], r["status"], r["trades"],
                r["win_rate"], r["avg_r"], r["payoff"],
                r.get("recent_avg_r"), r.get("recent_trades"), r.get("note", ""),
            )
            for r in rows
        ],
    )


def unreviewed(conn: sqlite3.Connection, limit: int = 0) -> list[dict]:
    """Closed trades with no review yet, oldest first.

    Plain dicts, not `sqlite3.Row`: the review engine reads rows with `.get()`
    so it can be handed a trade from anywhere — the ledger, the backtest, or a
    live fill — and `Row` supports indexing but not `.get`, which made the
    review path work on backtest rows and fail on ledger rows.
    """
    sql = """
        SELECT t.* FROM trades t
        LEFT JOIN reviews r ON r.trade_id = t.id
        WHERE r.trade_id IS NULL AND t.exit_reason != 'open'
        ORDER BY t.entry_day
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(row) for row in conn.execute(sql)]


def counts(conn: sqlite3.Connection) -> dict[str, Any]:
    """How much experience the database holds, by source."""
    by_source = {
        row["source"]: row["n"]
        for row in conn.execute("SELECT source, COUNT(*) AS n FROM trades GROUP BY source")
    }
    reviewed = conn.execute("SELECT COUNT(*) AS n FROM reviews").fetchone()["n"]
    span = conn.execute(
        "SELECT MIN(entry_day) AS first, MAX(entry_day) AS last FROM trades"
    ).fetchone()
    return {
        "by_source": by_source,
        "total": sum(by_source.values()),
        "reviewed": reviewed,
        "first_entry": span["first"],
        "last_entry": span["last"],
    }


def query_trades(
    conn: sqlite3.Connection,
    *,
    source: str | None = None,
    strategy: str | None = None,
    regime: str | None = None,
    verdict: str | None = None,
    since: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """Trades with their review attached, newest first."""
    where: list[str] = ["t.exit_reason != 'open'"]
    params: list[Any] = []
    for column, value in (
        ("t.source", source), ("t.strategy", strategy),
        ("t.regime", regime), ("r.verdict", verdict),
    ):
        if value:
            where.append(f"{column} = ?")
            params.append(value)
    if since:
        where.append("t.entry_day >= ?")
        params.append(since)

    sql = f"""
        SELECT t.*, r.verdict, r.tags, r.stop_quality, r.exit_quality,
               r.r_left_on_table, r.note AS review_note
        FROM trades t
        LEFT JOIN reviews r ON r.trade_id = t.id
        WHERE {' AND '.join(where)}
        ORDER BY t.entry_day DESC, t.id DESC
        LIMIT ?
    """
    params.append(int(limit))
    out: list[dict] = []
    for row in conn.execute(sql, params):
        record = dict(row)
        if record.get("tags"):
            try:
                record["tags"] = json.loads(record["tags"])
            except ValueError:
                record["tags"] = []
        else:
            record["tags"] = []
        out.append(record)
    return out


def latest_cell_status(conn: sqlite3.Connection) -> list[dict]:
    row = conn.execute("SELECT MAX(as_of) AS as_of FROM cell_status").fetchone()
    if not row or not row["as_of"]:
        return []
    return [dict(r) for r in conn.execute(
        "SELECT * FROM cell_status WHERE as_of = ? ORDER BY avg_r DESC", (row["as_of"],)
    )]


def cell_history(conn: sqlite3.Connection, strategy: str, regime: str) -> list[dict]:
    """Every recorded status for one cell — how the bot's view of it changed."""
    return [dict(r) for r in conn.execute(
        "SELECT * FROM cell_status WHERE strategy = ? AND regime = ? ORDER BY as_of",
        (strategy, regime),
    )]


def status_transitions(conn: sqlite3.Connection) -> list[dict]:
    """Only the points where a cell's status actually changed.

    The full history is mostly repetition; what a reader wants is the moments
    the system changed its mind, which is also the only honest evidence that it
    changes its mind at all.
    """
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM cell_status ORDER BY strategy, regime, as_of"
    )]
    out: list[dict] = []
    previous: dict[tuple[str, str], str] = {}
    for row in rows:
        key = (row["strategy"], row["regime"])
        was = previous.get(key)
        if was is not None and was != row["status"]:
            out.append({**row, "from_status": was, "to_status": row["status"]})
        previous[key] = row["status"]
    return sorted(out, key=lambda r: r["as_of"], reverse=True)
