#!/usr/bin/env python3
"""One paper-trading session: generate today's signals, then advance the book.

    python3 scripts/run_paper_session.py --equity 100000

This is the piece that makes the bot a bot rather than a study. It runs after
the close, every session, and does three things in order:

  1. rebuilds the signal list from the deep bar store, applying the same
     filters the walk-forward test was measured under — the rolling stop-width
     cap, then the 1-10 confidence rating, then the 8-10 band;
  2. advances `paper.PaperBook` by one session, entering at today's OPEN the
     candidates generated on a previous close, and applying stops, the trail
     and the time ceiling to everything already held;
  3. writes the book back to `APP_STATE_DIR` and a small public summary to
     `data/bot_paper.json` so the UI can show it.

**Why the signals are regenerated rather than read from a file.** The filters
are causal, so recomputing them today gives the same answer it gave yesterday
for yesterday — but the trailing-year stop-width percentile moves, and the
book must act on the value that was true on the session it is trading. Reading
a stale list would quietly trade last week's thresholds.

**Idempotent.** `paper.advance` refuses a session it has already processed, so
a retried workflow, a double cron or a manual re-run is safe. Pass `--force`
only to deliberately replay a day, and expect to be re-entering positions.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot import confidence as cf  # noqa: E402
from app.services.bot import indicators as ind  # noqa: E402
from app.services.bot import paper  # noqa: E402
from app.services.bot import rules as R  # noqa: E402
from app.services.bot import sleeve as sl  # noqa: E402
from app.services.bot.backtest import BacktestConfig, build_context, run_strategies  # noqa: E402
from app.services.bot.engine import ExitModel  # noqa: E402
from app.services.bot.history import available_symbols, read_bars  # noqa: E402


def _state_dir(data_dir: Path) -> Path:
    import os
    return Path(os.environ.get("APP_STATE_DIR", str(data_dir)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--equity", type=float, default=100_000.0,
                    help="book size on the FIRST run; ignored once the book exists")
    ap.add_argument("--session", default="", help="YYYY-MM-DD; defaults to the newest bar")
    ap.add_argument("--force", action="store_true", help="replay a session already processed")
    ap.add_argument("--limit-symbols", type=int, default=0)
    args = ap.parse_args()

    data_dir = BACKEND_ROOT / "data"
    state_dir = _state_dir(data_dir)

    symbols = available_symbols(data_dir)
    if args.limit_symbols:
        symbols = symbols[: args.limit_symbols]
    context = build_context(data_dir, symbols)
    exits = R.exit_model()
    trades = run_strategies(data_dir, context, BacktestConfig(exits=exits), symbols)
    rows = []
    for t in trades:
        r = asdict(t)
        for k in ("signal_day", "entry_day", "exit_day"):
            if r.get(k) is not None:
                r[k] = str(r[k])
        rows.append(r)

    idx = read_bars(data_dir, "NIFTY500")
    index_close = {d: float(c) for d, c in zip(idx.dates, idx.close)}
    closes = np.asarray(idx.close, dtype=float)
    s200 = ind.sma(closes, 200)
    above = {d: (bool(closes[i] > s200[i]) if not np.isnan(s200[i]) else True)
             for i, d in enumerate(idx.dates)}

    cleared = R.accepted_with_rolling_risk(rows)
    for t in cleared:
        t["conf"] = cf.rated(t, above.get(date.fromisoformat(str(t["entry_day"]))))
    picked = [t for t in cleared if t["conf"] >= cf.CONVICTION_BAR]

    # The session is the newest TRADING day in the store, not the newest day a
    # signal happened to fire. Keying it off signals was the first version and
    # it was wrong in a way that would have gone unnoticed for months: on a
    # quiet day the book would simply not advance, so open positions would
    # never age, never trail and never hit their time ceiling.
    sessions_available = [d for d in idx.dates]
    latest = max(sessions_available)
    book = paper.PaperBook.load(state_dir)
    if book is None:
        start_at = date.fromisoformat(args.session) if args.session else latest
        book = paper.PaperBook(started=start_at.isoformat(),
                               starting_equity=args.equity, cash=args.equity)
        todo = [start_at]
    elif args.session:
        todo = [date.fromisoformat(args.session)]
    else:
        # Catch up. A cron that missed three days must not leave the book
        # stranded three days in the past holding stops that were taken out
        # on one of them — it replays every missed session in order.
        last = date.fromisoformat(book.last_session) if book.last_session else latest
        todo = [d for d in sorted(sessions_available) if d > last] or [latest]

    by_day: dict[str, list] = {}
    for t in picked:
        by_day.setdefault(str(t["entry_day"]), []).append(t)
    print(f"signals {len(rows):,}  cleared {len(cleared):,}  "
          f"conviction {cf.CONVICTION_BAR:.0f}-10 {len(picked):,}   "
          f"sessions to run: {len(todo)}")

    # --- the idle-capital sleeve ------------------------------------------
    # Built from the same module the backtest uses, so the two cannot drift.
    # Without it the book holds cash while the study holds the index, which
    # the first replay measured at +5.87% over 2.7 years against a study
    # returning ~40% a year.
    def _series(sym: str) -> dict:
        try:
            import yfinance as yf
            h = yf.Ticker(sym).history(period="max")
            return {x.date(): float(c) for x, c in zip(h.index, h["Close"])}
        except Exception as exc:
            print(f"  (no {sym}: {exc})")
            return {}

    cache = data_dir / "bot_sleeve_cache.json"
    if cache.exists():
        raw = json.loads(cache.read_text())
        gold = {date.fromisoformat(k): v for k, v in raw.get("gold", {}).items()}
        small = {date.fromisoformat(k): v for k, v in raw.get("smallcap", {}).items()}
    else:
        gold, small = _series("GOLDBEES.NS"), _series("NIFTYSMLCAP250.NS")
        if gold and small:
            cache.write_text(json.dumps({
                "gold": {d.isoformat(): v for d, v in gold.items()},
                "smallcap": {d.isoformat(): v for d, v in small.items()},
            }))
    regimes = {d: r.regime for d, r in context.regime_by_day.items()}
    sleeve_on, _book_on = sl.risk_on_days(index_close, regimes)
    level = (sl.build_level(index_close, gold, small, sleeve_on)
             if gold else {d: index_close[d] for d in index_close})

    # Read each symbol's bars once, not once per session.
    need = ({p.symbol for p in book.positions}
            | {str(t["symbol"]) for d in todo for t in by_day.get(d.isoformat(), [])})
    store: dict[str, tuple] = {}
    for sym in need:
        b = read_bars(data_dir, sym)
        if b is not None:
            store[sym] = (b, {d: i for i, d in enumerate(b.dates)})

    out = {}
    for session in todo:
        todays = by_day.get(session.isoformat(), [])
        # A position held today must be priced today even if it was bought
        # long ago, so the set is rebuilt each session rather than once.
        want = {p.symbol for p in book.positions} | {str(t["symbol"]) for t in todays}
        for sym in want - set(store):
            b = read_bars(data_dir, sym)
            if b is not None:
                store[sym] = (b, {d: i for i, d in enumerate(b.dates)})
        bars: dict[str, dict] = {}
        for sym in want:
            entry = store.get(sym)
            if not entry:
                continue
            b, where = entry
            i = where.get(session)
            if i is None:
                continue
            bars[sym] = {"open": float(b.open[i]), "high": float(b.high[i]),
                         "low": float(b.low[i]), "close": float(b.close[i])}
        out = paper.advance(book, session, bars, todays,
                            sleeve_level=level.get(session), force=args.force)
        if "skipped" in out:
            print(f"  {session}  {out['skipped']}")
            continue
        if out["entered"] or out["exited"]:
            print(f"  {session}  +{out['entered']} -{out['exited']}  "
                  f"open {out['open_positions']:2}  equity {out['equity']:>12,.0f}"
                  + (f"   bought {', '.join(out['entered_symbols'])}" if out["entered_symbols"] else "")
                  + (f"   sold {', '.join(out['exited_symbols'])}" if out["exited_symbols"] else ""))
    if not out or "skipped" in out:
        print("  nothing to do — the book is already current")
        return 0
    book.save(state_dir)

    summary = paper.summary(book)
    (data_dir / "bot_paper.json").write_text(json.dumps({
        "summary": summary,
        "positions": [asdict(p) for p in book.positions],
        "recent_closed": [asdict(t) for t in book.closed[-50:]],
        "equity_curve": book.equity_curve[-500:],
    }, indent=2, default=str))
    print(f"  book: {summary['sessions']} sessions, {summary['closed_trades']} closed, "
          f"return {summary['return_pct']:+.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
