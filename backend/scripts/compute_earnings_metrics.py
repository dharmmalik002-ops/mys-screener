"""Compute post-earnings reaction metrics for the Positive Earnings scanner.

Run after the daily bhavcopy patch lands so the chart cache reflects
today's session. Writes ``backend/data/earnings_metrics.json``, which
the dashboard service merges onto each snapshot at read time.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.earnings_metrics import (  # noqa: E402  (sys.path mutated above)
    EARNINGS_LOOKBACK_DAYS,
    chart_cache_bars_loader_factory,
    compute_metrics,
    fetch_bse_result_filings,
    load_metrics_file,
    save_metrics_file,
    yfinance_bars_loader,
)


LOGGER = logging.getLogger("stock_scanner.earnings_metrics")


def _load_universe() -> list[dict]:
    universe_path = BACKEND_ROOT / "data" / "free_universe.json"
    if not universe_path.exists():
        raise SystemExit(f"Universe file missing at {universe_path}")
    return json.loads(universe_path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=EARNINGS_LOOKBACK_DAYS,
        help="Only consider results announced within this many days (default: %(default)d).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent yfinance threads (default: %(default)d).",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge new entries over the existing file instead of replacing it.",
    )
    parser.add_argument(
        "--bars-source",
        choices=("yfinance", "chart-cache"),
        default="yfinance",
        help="Where to read post-earnings OHLCV from (default: %(default)s).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    universe = _load_universe()
    LOGGER.info("computing earnings metrics for %d symbols", len(universe))

    if args.bars_source == "chart-cache":
        bars_loader = chart_cache_bars_loader_factory(BACKEND_ROOT / "data" / "chart_cache")
    else:
        bars_loader = lambda _symbol, ticker: yfinance_bars_loader(ticker)

    bse_filing_dates = fetch_bse_result_filings(args.lookback_days)

    fresh = compute_metrics(
        universe,
        bars_loader=bars_loader,
        lookback_days=args.lookback_days,
        max_workers=args.workers,
        bse_filing_dates=bse_filing_dates,
    )

    if args.merge:
        existing = load_metrics_file(BACKEND_ROOT)
        existing.update(fresh)
        merged = existing
    else:
        merged = fresh

    save_metrics_file(BACKEND_ROOT, merged)
    LOGGER.info(
        "saved %d entries (%d new this run) to %s",
        len(merged),
        len(fresh),
        BACKEND_ROOT / "data" / "earnings_metrics.json",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
