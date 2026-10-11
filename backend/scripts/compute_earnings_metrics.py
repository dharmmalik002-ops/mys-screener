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
    fetch_nse_results_calendar,
    load_calendar_file,
    load_metrics_file,
    merge_upcoming_calendar,
    save_calendar_file,
    save_metrics_file,
)


LOGGER = logging.getLogger("stock_scanner.earnings_metrics")


def _load_universe() -> list[dict]:
    universe_path = BACKEND_ROOT / "data" / "free_universe.json"
    if not universe_path.exists():
        raise SystemExit(f"Universe file missing at {universe_path}")
    return json.loads(universe_path.read_text(encoding="utf-8"))


def _write_calendar(
    universe: list[dict],
    forthcoming: dict[str, date],
    nse_events: dict[str, list[date]],
    today: date,
) -> None:
    """Merge BSE's scrip-keyed calendar and NSE's symbol-keyed one into one
    upcoming date per universe symbol, keeping still-future dates already on
    file that neither source named this run, and persist it."""
    symbol_by_scrip = {
        str(item.get("bse_code") or "").strip(): str(item.get("symbol") or "").upper()
        for item in universe
        if item.get("bse_code") and item.get("symbol")
    }
    universe_symbols = {str(item.get("symbol") or "").upper() for item in universe if item.get("symbol")}
    bse_by_symbol = {
        symbol_by_scrip[scrip]: [meeting]
        for scrip, meeting in forthcoming.items()
        if scrip in symbol_by_scrip
    }
    nse_by_symbol = {sym: dates for sym, dates in nse_events.items() if sym in universe_symbols}
    upcoming = merge_upcoming_calendar(
        (bse_by_symbol, nse_by_symbol), load_calendar_file(BACKEND_ROOT), today
    )
    save_calendar_file(BACKEND_ROOT, upcoming)
    LOGGER.info(
        "earnings calendar: %d universe symbols scheduled (BSE %d, NSE %d)",
        len(upcoming), len(bse_by_symbol), len(nse_by_symbol),
    )


def _nse_anchor_dates(nse_events: dict[str, list[date]], today: date, lookback_days: int) -> dict[str, date]:
    """The latest results meeting already held, per symbol, within the lookback."""
    cutoff = today - timedelta(days=lookback_days)
    anchors: dict[str, date] = {}
    for symbol, dates in nse_events.items():
        held = [d for d in dates if cutoff <= d <= today]
        if held:
            anchors[symbol] = max(held)
    return anchors


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
    today = datetime.now(timezone.utc).astimezone().date()

    # 1. Upcoming-results calendar — always refreshed, independent of scoring.
    # Two exchanges, so one refusing us no longer freezes the calendar.
    forthcoming = fetch_bse_forthcoming_results()
    nse_events = fetch_nse_results_calendar(today - timedelta(days=args.lookback_days), today + timedelta(days=90))
    if forthcoming or nse_events:
        _write_calendar(universe, forthcoming, nse_events, today)
    else:
        LOGGER.warning("BSE and NSE calendars both unavailable — keeping the committed one")
    nse_anchors = _nse_anchor_dates(nse_events, today, args.lookback_days)

    # 2. Anchor dates for results that have already landed.
    bse_filing_dates = fetch_bse_result_filings(args.lookback_days)

    # 3. Bars for the anchored names.
    if args.bars_source == "chart-cache":
        bars_loader = chart_cache_bars_loader_factory(BACKEND_ROOT / "data" / "chart_cache")
    else:
        anchored_scrips = set(bse_filing_dates) | {
            scrip for scrip, meeting in forthcoming.items() if meeting <= today
        }
        tickers = [
            str(item.get("ticker") or "").strip()
            for item in universe
            if item.get("ticker")
            and (
                str(item.get("bse_code") or "").strip() in anchored_scrips
                or str(item.get("symbol") or "").upper() in nse_anchors
            )
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
        symbol_calendar_dates=nse_anchors,
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
