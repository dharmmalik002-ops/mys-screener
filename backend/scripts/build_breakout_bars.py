#!/usr/bin/env python3
"""Build the daily bars the breakout replay reads, from nothing but the repo.

`generate_breakout_stats.py` replays scanners over per-symbol daily bars. It was
written against `data/chart_cache/`, which is gitignored — so on a GitHub runner
the directory does not exist and the nightly job failed on its first file read,
every night, from the day it was added. The committed stats file was the one
built by hand on 2026-08-10, and the Markets exposure verdict sat on July data.

This script produces the same file shape (`{SYMBOL}__1D.json` plus the
`_NSEI__3Y.json` benchmark) into its own directory:

* Bars come from chunked `yf.download`, the call shape that works from
  datacenter IPs. Per-ticker `Ticker().history()` is throttled there (see
  `earnings_metrics.batched_yfinance_bars_loader_factory`).
* Yahoo intermittently omits recent Indian sessions, so every series is
  gap-filled from the committed `data/eod_bars/` store — the same authoritative
  overlay the chart endpoints apply (`_with_daily_eod_gaps_filled`), reused
  rather than restated so the two cannot drift.
* It writes to `data/breakout_bars/`, never `chart_cache/`, so running it on a
  workstation cannot clobber the live chart cache.

    python3 scripts/build_breakout_bars.py
    python3 scripts/build_breakout_bars.py --limit-symbols 150   # quick check
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import ChartBar  # noqa: E402
from app.providers.free import FreeMarketDataProvider  # noqa: E402
from app.services.earnings_metrics import _frame_to_bars  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("breakout-bars")

BENCHMARK_TICKER = "^NSEI"
BENCHMARK_FILE = "_NSEI__3Y.json"
# ~500 sessions: a 12-week replay window plus the year of lookback a snapshot
# needs (52-week high, 200-DMA, 12-month RS), with room to spare.
DEFAULT_PERIOD = "2y"
CHUNK_SIZE = 50
CHUNK_RETRIES = 2


def download_chunk(tickers: list[str], period: str) -> dict[str, list[dict]]:
    for attempt in range(CHUNK_RETRIES + 1):
        try:
            frame = yf.download(
                tickers=" ".join(tickers),
                period=period,
                interval="1d",
                auto_adjust=False,
                group_by="ticker",
                threads=False,
                progress=False,
            )
            break
        except Exception as exc:
            logger.warning("chunk download failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(5 * (attempt + 1))
    else:
        return {}
    if frame is None or frame.empty:
        return {}
    out: dict[str, list[dict]] = {}
    if len(tickers) == 1:
        out[tickers[0]] = _frame_to_bars(frame.droplevel(0, axis=1) if isinstance(frame.columns, pd.MultiIndex) else frame)
        return out
    for ticker in tickers:
        try:
            sub = frame[ticker].dropna(how="all")
        except KeyError:
            continue
        out[ticker] = _frame_to_bars(sub)
    return out


def write_bars(out_dir: Path, filename: str, symbol: str, ticker: str, timeframe: str, bars: list[dict]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "timeframe": timeframe,
        "ticker": ticker,
        "source": "build_breakout_bars",
        "bars": bars,
    }
    (out_dir / filename).write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="yfinance period (default 2y)")
    parser.add_argument("--limit-symbols", type=int, default=0, help="cap symbols (testing only)")
    parser.add_argument("--out-dir", default=None, help="output directory (default data/breakout_bars)")
    parser.add_argument(
        "--min-symbols",
        type=int,
        default=800,
        help="exit non-zero below this many usable symbols, so a throttled run fails loudly",
    )
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    out_dir = Path(args.out_dir) if args.out_dir else data_dir / "breakout_bars"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = json.loads((data_dir / "free_universe.json").read_text(encoding="utf-8"))
    instruments = [
        (str(r["symbol"]).upper(), str(r.get("ticker") or f"{r['symbol']}.NS"))
        for r in rows
        if r.get("symbol")
    ]
    if args.limit_symbols:
        instruments = instruments[: args.limit_symbols]
    ticker_to_symbol = {ticker: symbol for symbol, ticker in instruments}

    # Only used for its EOD gap-fill overlay; no network access happens here.
    provider = FreeMarketDataProvider(gemini_api_key=None, eod_only_mode=True)

    benchmark = download_chunk([BENCHMARK_TICKER], args.period).get(BENCHMARK_TICKER) or []
    if len(benchmark) < 250:
        logger.error("benchmark %s returned %d bars — cannot replay without it", BENCHMARK_TICKER, len(benchmark))
        return 1
    write_bars(out_dir, BENCHMARK_FILE, BENCHMARK_TICKER, BENCHMARK_TICKER, "3Y", benchmark)
    bench_last = datetime.fromtimestamp(benchmark[-1]["time"], tz=timezone.utc).date()
    logger.info("benchmark: %d bars through %s", len(benchmark), bench_last)

    tickers = list(ticker_to_symbol)
    written = filled_sessions = 0
    t0 = time.time()
    for start in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[start : start + CHUNK_SIZE]
        for ticker, bars in download_chunk(chunk, args.period).items():
            if not bars:
                continue
            symbol = ticker_to_symbol[ticker]
            before = len(bars)
            filled = provider._with_daily_eod_gaps_filled(symbol, [ChartBar(**bar) for bar in bars])
            filled_sessions += len(filled) - before
            write_bars(out_dir, f"{symbol}__1D.json", symbol, ticker, "1D", [bar.model_dump() for bar in filled])
            written += 1
        logger.info(
            "[%d/%d] %d symbols written (%.1f min)",
            min(start + CHUNK_SIZE, len(tickers)), len(tickers), written, (time.time() - t0) / 60,
        )
        time.sleep(1)  # be polite to Yahoo between chunks

    logger.info(
        "done: %d / %d symbols, %d sessions gap-filled from data/eod_bars",
        written, len(tickers), filled_sessions,
    )
    minimum = min(args.min_symbols, len(tickers))
    if written < minimum:
        logger.error("only %d symbols usable (need %d) — refusing to hand a thin universe to the replay", written, minimum)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
