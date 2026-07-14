"""Compute post-earnings reaction metrics for the Positive Earnings scanner.

Run after the daily bhavcopy patch lands. Writes two committed artifacts:

* ``backend/data/earnings_calendar.json`` — BSE's forthcoming-results
  calendar mapped to NSE symbols ("who declares results in the coming
  days"), refreshed on every run so the pipeline tracks each quarter
  automatically.
* ``backend/data/earnings_metrics.json`` — per-symbol reaction metrics
  for names whose results have already landed, which the dashboard
  service merges onto snapshots and the scanner grades A/B.

CI-safety (this script produced an EMPTY file for weeks — root causes and
their fixes):
1. Per-ticker ``yf.Ticker().history()`` is throttled/blocked from GitHub
   runner IPs → bars now come from chunked ``yf.download`` (the same call
   shape the daily bhavcopy job uses successfully from CI), and only for
   symbols that actually have a result anchor (BSE filing or a passed
   board-meeting date) instead of all ~1600.
2. A failed run used to overwrite the good file with zero entries → we
   now always merge over the existing file, prune entries older than the
   lookback, and refuse to write when the result would shrink a populated
   file to nothing.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.earnings_metrics import (  # noqa: E402  (sys.path mutated above)
    EARNINGS_LOOKBACK_DAYS,
    batched_yfinance_bars_loader_factory,
    chart_cache_bars_loader_factory,
    compute_metrics,
    fetch_bse_forthcoming_results,
    fetch_bse_result_filings,
    load_metrics_file,
    save_calendar_file,
    save_metrics_file,
)


LOGGER = logging.getLogger("stock_scanner.earnings_metrics")


def _load_universe() -> list[dict]:
    universe_path = BACKEND_ROOT / "data" / "free_universe.json"
    if not universe_path.exists():
        raise SystemExit(f"Universe file missing at {universe_path}")
    return json.loads(universe_path.read_text(encoding="utf-8"))


def _write_calendar(universe: list[dict], forthcoming: dict[str, date]) -> None:
    """Map the scrip-code-keyed calendar to NSE symbols and persist it."""
    symbol_by_scrip = {
        str(item.get("bse_code") or "").strip(): str(item.get("symbol") or "").upper()
        for item in universe
        if item.get("bse_code") and item.get("symbol")
    }
    upcoming = {
        symbol_by_scrip[scrip]: meeting.isoformat()
        for scrip, meeting in forthcoming.items()
        if scrip in symbol_by_scrip
    }
    save_calendar_file(BACKEND_ROOT, upcoming)
    LOGGER.info("earnings calendar: %d universe symbols scheduled", len(upcoming))


def _prune_stale(entries: dict[str, dict], lookback_days: int) -> dict[str, dict]:
    cutoff = (datetime.now(timezone.utc).astimezone().date() - timedelta(days=lookback_days)).isoformat()
    return {
        sym: entry
        for sym, entry in entries.items()
        if isinstance(entry.get("earnings_date"), str) and entry["earnings_date"] >= cutoff
    }


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
        help="Concurrent scoring threads (default: %(default)d).",
    )
    parser.add_argument(
        "--bars-source",
        choices=("batched", "chart-cache"),
        default="batched",
        help="Where to read post-earnings OHLCV from (default: %(default)s).",
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Score every universe symbol via per-ticker yfinance discovery "
        "(local/residential use only — throttled from CI).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    universe = _load_universe()

    # 1. Upcoming-results calendar — always refreshed, independent of scoring.
    forthcoming = fetch_bse_forthcoming_results()
    if forthcoming:
        _write_calendar(universe, forthcoming)
    else:
        LOGGER.warning("forthcoming calendar unavailable — keeping the committed one")

    # 2. Anchor dates for results that have already landed.
    bse_filing_dates = fetch_bse_result_filings(args.lookback_days)

    # 3. Bars for the anchored names.
    if args.bars_source == "chart-cache":
        bars_loader = chart_cache_bars_loader_factory(BACKEND_ROOT / "data" / "chart_cache")
    else:
        today = datetime.now(timezone.utc).astimezone().date()
        anchored_scrips = set(bse_filing_dates) | {
            scrip for scrip, meeting in forthcoming.items() if meeting <= today
        }
        tickers = [
            str(item.get("ticker") or "").strip()
            for item in universe
            if str(item.get("bse_code") or "").strip() in anchored_scrips
            and item.get("ticker")
        ]
        if args.all_symbols:
            tickers = [str(item.get("ticker") or "").strip() for item in universe if item.get("ticker")]
        LOGGER.info("downloading bars for %d anchored tickers", len(tickers))
        bars_loader = batched_yfinance_bars_loader_factory(tickers)

    fresh = compute_metrics(
        universe,
        bars_loader=bars_loader,
        lookback_days=args.lookback_days,
        max_workers=args.workers,
        bse_filing_dates=bse_filing_dates,
        calendar_dates=forthcoming,
        only_anchored=not args.all_symbols,
        use_yfinance_dates=args.all_symbols,
    )

    # 4. Merge-don't-wipe: new entries over existing, prune the stale, and
    # never replace a populated file with an empty result.
    existing = load_metrics_file(BACKEND_ROOT)
    merged = _prune_stale({**existing, **fresh}, args.lookback_days)
    if not merged and existing:
        LOGGER.warning(
            "computed 0 entries while %d exist — refusing to wipe the file", len(existing)
        )
        return 0

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
