#!/usr/bin/env python3
"""Fetch deep daily history for the backtest engine into `data/deep_history/`.

    python3 scripts/build_deep_history.py                 # universe + indices + macro
    python3 scripts/build_deep_history.py --only-context  # just indices and macro
    python3 scripts/build_deep_history.py --limit 50      # smoke test

Roughly 20 minutes for the full universe on a warm connection. The store is
gitignored and purely local: nothing the live app serves depends on it, so a
failed or partial run degrades the bot's backtest and breaks nothing else.

Re-running is safe and incremental — a symbol already holding bars through the
last completed session is skipped, one that is behind gets only its missing
tail appended, and `--refresh` re-fetches everything in full.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import history as hist  # noqa: E402
from app.services.bot.context_series import CONTEXT_SERIES  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("deep-history")

MAX_WORKERS = 6          # above this Yahoo starts returning empty frames
IST = timezone(timedelta(hours=5, minutes=30))
# A stored close that moves by more than this when re-fetched means Yahoo has
# re-adjusted the whole series (split, bonus, dividend): appending a new tail to
# the old adjusted base would put a false gap at the join, so re-fetch it all.
ADJUSTMENT_TOLERANCE = 0.005
PROVISIONAL_BARS = 2
RETRIES = 3
RETRY_SLEEP = 2.0


def fetch_one(symbol: str, ticker: str, period: str = "max") -> list[dict]:
    """Daily bars, split- and dividend-adjusted, oldest first.

    `auto_adjust=True` matters more than it looks: without it a 1:10 split reads
    as a 90% single-session crash and every trend and volatility filter in the
    engine fires on an event that never happened.
    """
    import yfinance as yf

    last_error: Exception | None = None
    for attempt in range(RETRIES):
        try:
            frame = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=True)
            if frame is None or frame.empty:
                last_error = RuntimeError("empty frame")
                time.sleep(RETRY_SLEEP * (attempt + 1))
                continue
            rows: list[dict] = []
            for ts, row in frame.iterrows():
                day = ts.date() if hasattr(ts, "date") else None
                if day is None:
                    continue
                rows.append(
                    {
                        "date": day,
                        "open": row.get("Open"),
                        "high": row.get("High"),
                        "low": row.get("Low"),
                        "close": row.get("Close"),
                        "volume": row.get("Volume"),
                    }
                )
            return rows
        except Exception as exc:  # noqa: BLE001 - one bad symbol must not kill the run
            last_error = exc
            time.sleep(RETRY_SLEEP * (attempt + 1))
    logger.debug("fetch failed %s (%s): %s", symbol, ticker, last_error)
    return []


def targets(data_dir: Path, args) -> list[tuple[str, str]]:
    """(symbol, ticker) pairs to fetch. Context series always come first so a
    run that is interrupted still leaves the regime engine with what it needs."""
    pairs: list[tuple[str, str]] = [(s.key, s.ticker) for s in CONTEXT_SERIES]
    if args.only_context:
        return pairs

    universe_path = data_dir / "free_universe.json"
    rows = json.loads(universe_path.read_text(encoding="utf-8"))
    equity = [
        (str(r["symbol"]).upper(), str(r["ticker"]))
        for r in rows
        if r.get("symbol") and r.get("ticker")
    ]
    if args.limit:
        equity = equity[: args.limit]
    return pairs + equity


def latest_session(now: datetime | None = None) -> date:
    """The most recent NSE session that has closed (weekdays; holidays are not
    known here, so on one the whole store simply gets a cheap tail top-up)."""
    now = (now or datetime.now(timezone.utc)).astimezone(IST)
    day = now.date()
    if now.hour < 16:  # EOD bars are not final before ~16:00 IST
        day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def already_current(data_dir: Path, symbol: str, cutoff: date) -> bool:
    bars = hist.read_bars(data_dir, symbol)
    return bool(bars and bars.last_date and bars.last_date >= cutoff)


def _bars_to_rows(bars: hist.Bars) -> list[dict]:
    return [
        {"date": d, "open": o, "high": h, "low": l, "close": c, "volume": v}
        for d, o, h, l, c, v in zip(bars.dates, bars.open, bars.high, bars.low, bars.close, bars.volume)
    ]


def merge_tail(stored: hist.Bars, tail: list[dict]) -> list[dict] | None:
    """Stored history plus the freshly fetched tail, or None when the overlap
    disagrees (the series was re-adjusted) and only a full re-fetch is safe."""
    stored_close = {d: float(c) for d, c in zip(stored.dates, stored.close)}
    # The newest stored bars may have been captured mid-session (RELIANCE's
    # 2026-09-24 bar read 1232.60; the close was 1219.20). They are not evidence
    # of a re-adjustment — the fresh tail simply overwrites them.
    provisional = set(stored.dates[-PROVISIONAL_BARS:])
    overlap = [row for row in tail if row.get("date") in stored_close and row["date"] not in provisional]
    if not overlap:
        return None  # nothing to anchor the join on
    for row in overlap:
        try:
            fresh = float(row["close"])
        except (KeyError, TypeError, ValueError):
            return None
        old = stored_close[row["date"]]
        if old <= 0 or abs(fresh / old - 1.0) > ADJUSTMENT_TOLERANCE:
            return None
    return _bars_to_rows(stored) + tail


def drop_unclosed(rows: list[dict], cutoff: date) -> list[dict]:
    """Rows for sessions that have closed. A top-up run before ~16:00 IST gets
    today's half-finished bar from Yahoo; stored, it would make the symbol look
    current at the evening run (`already_current`) and the partial prices would
    never be replaced. Dropping it leaves the symbol one session behind, which
    the next run fills in properly."""
    return [r for r in rows if isinstance(r.get("date"), date) and r["date"] <= cutoff]


def update_one(data_dir: Path, symbol: str, ticker: str, full: bool) -> list[dict]:
    """Rows to write for one symbol: the missing tail appended when that is
    safe, the full history otherwise."""
    stored = None if full else hist.read_bars(data_dir, symbol)
    if stored is not None and len(stored):
        tail = fetch_one(symbol, ticker, period="3mo")
        if tail:
            merged = merge_tail(stored, tail)
            if merged is not None:
                return merged
    return fetch_one(symbol, ticker)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0, help="cap equity symbols (testing)")
    parser.add_argument("--only-context", action="store_true", help="indices and macro only")
    parser.add_argument("--refresh", action="store_true", help="re-fetch symbols already stored")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    hist.store_dir(data_dir).mkdir(parents=True, exist_ok=True)

    pairs = targets(data_dir, args)
    # Current means holding the last CLOSED session. The old rule — anything
    # within five calendar days — skipped every symbol until the store was
    # almost a week stale, so the nightly job re-ran the bot on the same bars
    # and the Bot tab read "these signals are 7 days old". A symbol behind now
    # gets only its missing tail appended (see update_one), so this is cheap.
    cutoff = latest_session()
    if not args.refresh:
        pending = [(s, t) for s, t in pairs if not already_current(data_dir, s, cutoff)]
        logger.info("%d symbols already current — skipping", len(pairs) - len(pending))
        pairs = pending

    if not pairs:
        logger.info("nothing to fetch")
        return 0

    logger.info("fetching %d symbols with %d workers…", len(pairs), args.workers)
    t0 = time.time()
    ok = failed = 0
    total_bars = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(update_one, data_dir, s, t, args.refresh): (s, t) for s, t in pairs}
        for n, future in enumerate(as_completed(futures), start=1):
            symbol, ticker = futures[future]
            try:
                rows = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.debug("worker crashed on %s: %s", symbol, exc)
                rows = []
            rows = drop_unclosed(rows, cutoff)
            if rows:
                written = hist.write_bars(data_dir, symbol, ticker, rows)
                if written:
                    ok += 1
                    total_bars += written
                else:
                    failed += 1
            else:
                failed += 1
            if n % 50 == 0 or n == len(pairs):
                rate = n / max(time.time() - t0, 1e-9)
                remaining = (len(pairs) - n) / max(rate, 1e-9) / 60
                logger.info(
                    "[%d/%d] ok=%d failed=%d bars=%s  (%.1f/s, ~%.1f min left)",
                    n, len(pairs), ok, failed, f"{total_bars:,}", rate, remaining,
                )

    manifest = hist.store_dir(data_dir) / "_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "built_at": datetime.now(timezone.utc).isoformat(),
                "symbols_ok": ok,
                "symbols_failed": failed,
                "total_bars": total_bars,
                "store_version": hist.STORE_VERSION,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    logger.info("done in %.1f min — %d symbols, %s bars", (time.time() - t0) / 60, ok, f"{total_bars:,}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
