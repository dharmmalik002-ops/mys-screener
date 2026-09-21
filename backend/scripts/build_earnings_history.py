#!/usr/bin/env python3
"""Fetch historical earnings announcements into `data/earnings_history/`.

    python3 scripts/build_earnings_history.py
    python3 scripts/build_earnings_history.py --limit 100   # smoke test

Every signal this bot trades is a price or volume pattern. That whole family
has now been measured and it does not hold up out of sample, so the remaining
question is whether a *different kind* of information does — which is what the
user meant by micro conditions, and what this fetches.

What matters here is the announcement date. An earnings surprise is only
tradeable from the moment it becomes public, and `earnings_metrics.json` (the
app's existing file) holds one date per symbol — the most recent — which is
useless for a backtest. This stores the full history, so a signal can be
stamped to the session the market first knew.

Post-earnings-announcement drift is the hypothesis: prices under-react to
large surprises and keep drifting for weeks. It is among the most replicated
anomalies in the literature, across decades and markets, which makes it worth
testing properly and also worth suspecting — a well-known effect is a crowded
one, and Indian mid-caps in 2026 are not US large-caps in 1985.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("earnings-history")

MAX_WORKERS = 6
RETRIES = 2
STORE = "earnings_history"


def store_dir(data_dir: Path) -> Path:
    return data_dir / STORE


def fetch_one(symbol: str, ticker: str) -> list[dict]:
    """Announcement rows for one symbol, oldest first."""
    import yfinance as yf

    for attempt in range(RETRIES):
        try:
            frame = yf.Ticker(ticker).get_earnings_dates(limit=60)
            if frame is None or frame.empty:
                return []
            rows: list[dict] = []
            for ts, row in frame.iterrows():
                day = ts.date() if hasattr(ts, "date") else None
                if day is None:
                    continue
                surprise = row.get("Surprise(%)")
                reported = row.get("Reported EPS")
                estimate = row.get("EPS Estimate")
                # A row with no reported figure is a scheduled future date, not
                # an announcement. Keeping it would let the engine trade an
                # event that has not happened.
                if reported is None or (isinstance(reported, float) and reported != reported):
                    continue
                rows.append(
                    {
                        "date": day.isoformat(),
                        "eps_estimate": None if estimate != estimate else float(estimate),
                        "reported_eps": float(reported),
                        "surprise_pct": None if surprise != surprise else float(surprise),
                    }
                )
            rows.sort(key=lambda r: r["date"])
            return rows
        except Exception:  # noqa: BLE001 - one bad symbol must not kill the run
            time.sleep(1.5 * (attempt + 1))
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    out_dir = store_dir(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    universe = json.loads((data_dir / "free_universe.json").read_text(encoding="utf-8"))
    pairs = [
        (str(r["symbol"]).upper(), str(r["ticker"]))
        for r in universe if r.get("symbol") and r.get("ticker")
    ]
    if args.limit:
        pairs = pairs[: args.limit]

    logger.info("fetching earnings history for %d symbols…", len(pairs))
    t0 = time.time()
    ok = empty = 0
    total_rows = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_one, s, t): s for s, t in pairs}
        for n, future in enumerate(as_completed(futures), start=1):
            symbol = futures[future]
            try:
                rows = future.result()
            except Exception:  # noqa: BLE001
                rows = []
            if rows:
                path = out_dir / f"{symbol}.json.gz"
                with gzip.open(path, "wt", encoding="utf-8") as fh:
                    json.dump({"symbol": symbol, "announcements": rows}, fh, separators=(",", ":"))
                ok += 1
                total_rows += len(rows)
            else:
                empty += 1
            if n % 100 == 0 or n == len(pairs):
                rate = n / max(time.time() - t0, 1e-9)
                logger.info(
                    "[%d/%d] ok=%d empty=%d rows=%s (%.1f/s, ~%.1f min left)",
                    n, len(pairs), ok, empty, f"{total_rows:,}", rate,
                    (len(pairs) - n) / max(rate, 1e-9) / 60,
                )

    (out_dir / "_manifest.json").write_text(
        json.dumps({"built_at": datetime.now(timezone.utc).isoformat(),
                    "symbols_ok": ok, "symbols_empty": empty, "announcements": total_rows}, indent=2),
        encoding="utf-8",
    )
    logger.info("done in %.1f min — %d symbols, %s announcements",
                (time.time() - t0) / 60, ok, f"{total_rows:,}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
