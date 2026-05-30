"""One-time backfill for the XP market breadth score.

Builds backend/data/xp_breadth_history.json (and the initial
xp_rolling_closes.json) from historical daily bhavcopies, so the dashboard
chart has depth from day one. Run this LOCALLY (NSE archives are reachable from
Indian IPs; BSE works from anywhere) — it is intentionally kept off the HF
Space and GitHub Actions because it downloads many days of EOD files.

Usage:
    python backend/scripts/backfill_xp_breadth.py --days 400
    python backend/scripts/backfill_xp_breadth.py --start 2024-01-01 --end 2026-05-29
    # calibrate the regime bands to the author's published values:
    python backend/scripts/backfill_xp_breadth.py --days 400 \
        --anchor 2026-05-27=16.4 --anchor 2026-04-10=11.8

The first ~MA_LONG+WARMUP trading days are used purely to warm the rolling
window and the recursion; they are kept in the file but flagged warmup=True so
the UI can show them faded or hide them.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Reuse the price-fetch helpers and the shared engine from the daily generator.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_bhavcopy_patch as g  # noqa: E402
from app.services.xp_breadth import (  # noqa: E402
    CONST,
    calibrate_const,
    compute_xp_series,
    daily_breadth_metrics,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_xp_breadth")

METRIC_KEYS = ("date", "total", "advancers_4p5", "decliners", "ma10_pct", "ma20_pct")


def _fetch_full_bhav(trade_date: date) -> dict[str, dict] | None:
    """Full all-equity bhavcopy for a date: BSE first (global), then NSE archive."""
    rows = g._fetch_from_bse(trade_date)
    if rows:
        return rows
    csv_text = g._fetch_bhavcopy_csv(trade_date)
    if csv_text:
        parsed = g._parse_bhavcopy_csv(csv_text)
        if parsed:
            return parsed
    return None


def _trading_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri (holidays are skipped when the fetch returns nothing)
            days.append(d)
        d += timedelta(days=1)
    return days


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill the XP market breadth score history.")
    parser.add_argument("--days", type=int, default=400, help="Number of calendar days back from today (if --start omitted).")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: last weekday).")
    parser.add_argument("--anchor", action="append", default=[], help="date=XP published-value pair for band calibration; repeatable.")
    parser.add_argument("--const", type=float, default=None, help="Force a specific calibration constant (overrides --anchor).")
    args = parser.parse_args()

    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else g._last_trading_day()
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
    else:
        start = end - timedelta(days=args.days)

    anchors: dict[str, float] = {}
    for item in args.anchor:
        if "=" in item:
            d, v = item.split("=", 1)
            try:
                anchors[d.strip()] = float(v)
            except ValueError:
                logger.warning("Ignoring bad --anchor %r", item)

    # Fall back to the committed calibration anchors (author's published EM/MBI
    # values) when none are passed on the command line.
    if not anchors and args.const is None:
        anchor_path = g.DATA_DIR / "xp_calibration_anchors.json"
        if anchor_path.exists():
            try:
                doc = json.loads(anchor_path.read_text(encoding="utf-8"))
                loaded = doc.get("anchors") if isinstance(doc, dict) else None
                if isinstance(loaded, dict):
                    anchors = {str(k): float(v) for k, v in loaded.items()}
                    logger.info("Loaded %s calibration anchors from %s", len(anchors), anchor_path.name)
            except Exception as exc:
                logger.warning("Could not read calibration anchors (%s)", exc)

    logger.info("Backfilling XP breadth %s -> %s", start.isoformat(), end.isoformat())

    rolling: dict[str, list] = {}
    metrics_history: list[dict] = []
    fetched = 0
    for d in _trading_days(start, end):
        bhav = _fetch_full_bhav(d)
        if not bhav:
            continue  # holiday or unavailable
        metrics, rolling = daily_breadth_metrics(d.isoformat(), bhav, rolling)
        metrics_history.append(metrics)
        fetched += 1
        if fetched % 25 == 0:
            logger.info("  ...%s days fetched (last=%s, symbols=%s)", fetched, d.isoformat(), metrics["total"])

    if not metrics_history:
        logger.error("No bhavcopy data fetched for the requested range.")
        return 1

    const = args.const if args.const is not None else CONST
    if args.const is None and anchors:
        const = calibrate_const(metrics_history, anchors)
        logger.info("Calibrated const=%s against %s anchor(s): %s", const, len(anchors), anchors)
    else:
        logger.info("Using const=%s (no calibration anchors provided)", const)

    series = compute_xp_series(metrics_history, const=const)
    latest = series[-1] if series else None

    out_doc = {
        "generated_at": datetime.now(g.IST).isoformat(),
        "rolling_date": metrics_history[-1]["date"],
        "source": "BACKFILL",
        "const": const,
        "ma_short": 10,
        "ma_long": 20,
        "universe": "all_bhavcopy_equities",
        "latest": latest,
        "days": series,
    }
    g.XP_HISTORY_PATH.write_text(json.dumps(out_doc, separators=(",", ":")), encoding="utf-8")
    g.XP_ROLLING_PATH.write_text(json.dumps(rolling, separators=(",", ":")), encoding="utf-8")

    live = [r for r in series if not r["warmup"]]
    logger.info(
        "Wrote %s (%s days, %s live after warm-up). Latest %s: XP=%s (%s).",
        g.XP_HISTORY_PATH.name,
        len(series),
        len(live),
        latest["date"] if latest else "-",
        latest["xp_score"] if latest else "-",
        latest["regime"] if latest else "-",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
