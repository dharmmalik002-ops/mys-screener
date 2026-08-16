#!/usr/bin/env python3
"""Build `backend/data/breadth_mcap_history.json` — breadth for the ₹1,000 cr+ universe.

The daily bhavcopy job appends one session per day off the indicator blocks it
already downloads (see `_update_mcap_breadth` in generate_bhavcopy_patch.py).
This script is the other half: the multi-year backfill, and the repair path
when a stretch of the file is wrong or missing. Output is merged, never
replaced, so re-running over a narrow window cannot destroy the archive.

Two sources, because they trade off differently:

  --source cache  reads backend/data/chart_cache/*__1D.json. Offline, seconds,
                  but only as deep as the cache (~520 bars ≈ 2 years, which
                  after the 200-bar warmup yields ~1.3 years of plottable
                  200-DMA breadth).
  --source yf     downloads adjusted daily bars from Yahoo in chunks. Slow
                  (minutes) but goes back as far as you ask, and is the only
                  way to get the full three years.

Usage::

    python backend/scripts/generate_mcap_breadth_history.py                 # cache if present, else yfinance
    python backend/scripts/generate_mcap_breadth_history.py --source yf --years 4
    python backend/scripts/generate_mcap_breadth_history.py --floor 500 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from app.services import mcap_breadth  # noqa: E402

DATA_DIR = _BACKEND_ROOT / "data"
UNIVERSE_PATH = DATA_DIR / "free_universe.json"
CHART_CACHE_DIR = DATA_DIR / "chart_cache"
OUTPUT_PATH = DATA_DIR / mcap_breadth.HISTORY_FILENAME

# (metric key, kind, span). Order matches mcap_breadth.METRIC_KEYS.
AVERAGES = (
    ("above_ema20_pct", "ema", 20),
    ("above_ema21_pct", "ema", 21),
    ("above_sma50_pct", "sma", 50),
    ("above_sma200_pct", "sma", 200),
)
MIN_BARS = min(span for _, _, span in AVERAGES)
YF_CHUNK = 200

# Log-only, so a run says what it produced without decoding key names.
_METRIC_LABELS = (
    ("above_ema20_pct", "20-EMA"),
    ("above_ema21_pct", "21-EMA"),
    ("above_sma50_pct", "50-DMA"),
    ("above_sma200_pct", "200-DMA"),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("mcap-breadth")


def _load_universe(floor_crore: float) -> list[dict]:
    """Universe rows at or above the floor, each with `symbol` and `ticker`."""
    try:
        raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.error("cannot read %s: %s", UNIVERSE_PATH, exc)
        return []
    if not isinstance(raw, list):
        logger.error("%s is not a list", UNIVERSE_PATH)
        return []
    keep = mcap_breadth.universe_symbols(raw, floor_crore)
    rows = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol in keep:
            rows.append({"symbol": symbol, "ticker": str(row.get("ticker") or "").strip()})
    return rows


# ---------------------------------------------------------------------------
# Bar sources
# ---------------------------------------------------------------------------

def _closes_from_chart_cache(rows: list[dict], start: date):
    """{symbol: pandas.Series of closes} from the local chart cache."""
    import pandas as pd

    series: dict[str, "pd.Series"] = {}
    for row in rows:
        path = CHART_CACHE_DIR / f"{row['symbol']}__1D.json"
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        bars = doc.get("bars") if isinstance(doc, dict) else None
        if not isinstance(bars, list) or len(bars) < MIN_BARS:
            continue
        index, values = [], []
        for bar in bars:
            if not isinstance(bar, dict):
                continue
            try:
                stamp = int(bar.get("time"))
                close = float(bar.get("close"))
            except (TypeError, ValueError):
                continue
            if close <= 0:
                continue
            index.append(datetime.fromtimestamp(stamp, timezone.utc).date())
            values.append(close)
        if len(values) < MIN_BARS:
            continue
        s = pd.Series(values, index=pd.to_datetime(index))
        s = s[~s.index.duplicated(keep="last")].sort_index()
        series[row["symbol"]] = s[s.index >= pd.Timestamp(start)]
    logger.info("chart cache: %s/%s symbols have usable bars", len(series), len(rows))
    return series


def _closes_from_yfinance(rows: list[dict], start: date, end: date):
    """{symbol: pandas.Series of split/dividend-adjusted closes} from Yahoo.

    `auto_adjust=True` for the same reason the patch generator uses it: raw
    closes carry split discontinuities straight into every moving average, and
    a single unadjusted 1:5 split reads as an 80% crash below every MA.
    """
    import pandas as pd
    import yfinance as yf

    tickers = [r["ticker"] for r in rows if r["ticker"]]
    by_ticker = {r["ticker"]: r["symbol"] for r in rows if r["ticker"]}
    series: dict[str, "pd.Series"] = {}
    start_str = start.strftime("%Y-%m-%d")
    end_str = (end + timedelta(days=1)).strftime("%Y-%m-%d")

    for i in range(0, len(tickers), YF_CHUNK):
        chunk = tickers[i : i + YF_CHUNK]
        logger.info("yfinance chunk %s/%s (%s tickers)", i // YF_CHUNK + 1,
                    (len(tickers) + YF_CHUNK - 1) // YF_CHUNK, len(chunk))
        try:
            df = yf.download(chunk, start=start_str, end=end_str, auto_adjust=True,
                             progress=False, threads=True)
        except Exception as exc:  # noqa: BLE001 — one bad chunk must not kill the run
            logger.warning("chunk %s failed: %s", i // YF_CHUNK + 1, exc)
            continue
        if df is None or df.empty:
            continue
        for ticker in chunk:
            try:
                close = df.xs(ticker, axis=1, level=1)["Close"] if isinstance(df.columns, pd.MultiIndex) else df["Close"]
            except (KeyError, IndexError):
                continue
            close = close.dropna()
            close = close[close > 0]
            if len(close) < MIN_BARS:
                continue
            series[by_ticker[ticker]] = close.sort_index()
    logger.info("yfinance: %s/%s symbols returned usable bars", len(series), len(tickers))
    return series


# ---------------------------------------------------------------------------
# Breadth arithmetic
# ---------------------------------------------------------------------------

def _breadth_rows(series: dict, min_date: date | None) -> list[dict]:
    """Per-session percentages above each average in `AVERAGES`.

    Warmup is masked per symbol rather than per date: a stock listed last month
    has no 200-DMA even though the universe as a whole does, and counting it as
    "below" would drag the structural line down by exactly the share of recent
    listings in the universe.
    """
    import pandas as pd

    if not series:
        return []
    # pd.concat, not pd.DataFrame(dict): the constructor aligns the ~1,500
    # differently-indexed Series pairwise and takes ~15 minutes on this
    # universe. concat builds the union index once — a couple of seconds.
    close = pd.concat(series.values(), axis=1, keys=series.keys(), sort=True).sort_index()
    close.index = pd.to_datetime(close.index).normalize()
    close = close[~close.index.duplicated(keep="last")]

    observed = close.notna().cumsum()  # bars seen so far, per symbol

    def moving_average(kind: str, span: int):
        if kind == "ema":
            # `.where(observed >= span)` is the EMA's warmup mask: ewm emits a
            # value from the first bar, and a 3-bar-old listing "above its
            # 21-EMA" is really just above its own opening print.
            return close.ewm(span=span, adjust=False).mean().where(observed >= span)
        return close.rolling(window=span, min_periods=span).mean()

    counts: dict[str, tuple] = {}
    for key, kind, span in AVERAGES:
        ma = moving_average(kind, span)
        eligible = ma.notna() & close.notna()
        counts[key] = ((close > ma).where(eligible).sum(axis=1), eligible.sum(axis=1))
    total = close.notna().sum(axis=1)

    # Partial sessions: a date where only a slice of the universe reported is a
    # reading of that slice, not of the market. Measured against the busiest
    # session in the frame rather than the universe size, so a genuinely
    # smaller early-history universe is not filtered out wholesale.
    ceiling = int(total.max() or 0)
    floor_count = ceiling * mcap_breadth.MIN_COVERAGE_FRACTION

    rows: list[dict] = []
    dropped = 0
    for stamp in close.index:
        day = stamp.date()
        if min_date and day < min_date:
            continue
        if int(total.loc[stamp]) < floor_count:
            dropped += 1
            continue
        rows.append(
            mcap_breadth.build_day_row(
                day.isoformat(),
                {
                    key: (int(above.loc[stamp]), int(eligible.loc[stamp]))
                    for key, (above, eligible) in counts.items()
                },
                total=int(total.loc[stamp]),
            )
        )
    if dropped:
        logger.info("dropped %s thin sessions (under %.0f%% coverage)", dropped,
                    mcap_breadth.MIN_COVERAGE_FRACTION * 100)
    return [r for r in rows if any(r.get(k) is not None for k in mcap_breadth.METRIC_KEYS)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("auto", "cache", "yf"), default="auto")
    parser.add_argument("--floor", type=float, default=mcap_breadth.DEFAULT_MCAP_FLOOR_CRORE,
                        help="market-cap floor in ₹ crore (default 1000)")
    parser.add_argument("--years", type=float, default=3.0,
                        help="how far back to fetch; the first ~200 bars are 200-DMA warmup")
    parser.add_argument("--dry-run", action="store_true", help="compute and report, write nothing")
    args = parser.parse_args()

    rows = _load_universe(args.floor)
    if not rows:
        logger.error("no universe symbols at or above %.0f cr — nothing to do", args.floor)
        return 1
    logger.info("universe: %s symbols at or above Rs %.0f cr", len(rows), args.floor)

    today = date.today()
    start = today - timedelta(days=int(args.years * 365) + 300)  # +300 for MA warmup
    min_plot_date = today - timedelta(days=int(args.years * 365))

    source = args.source
    if source == "auto":
        source = "cache" if CHART_CACHE_DIR.is_dir() and any(CHART_CACHE_DIR.iterdir()) else "yf"
    logger.info("source: %s (from %s)", source, start.isoformat())

    series = (_closes_from_chart_cache(rows, start) if source == "cache"
              else _closes_from_yfinance(rows, start, today))
    if not series:
        logger.error("no bars from source %s", source)
        return 1

    days = _breadth_rows(series, min_plot_date)
    if not days:
        logger.error("no plottable sessions produced")
        return 1

    existing: dict = {}
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except ValueError:
            existing = {}
    merged = mcap_breadth.merge_days(existing.get("days") or [], days)

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": mcap_breadth.universe_label(args.floor),
        "market_cap_floor_crore": args.floor,
        "symbols": len(series),
        "source": source,
        "days": merged,
    }
    logger.info("%s sessions computed, %s in file after merge (%s → %s)",
                len(days), len(merged), merged[0]["date"], merged[-1]["date"])
    latest = merged[-1]
    logger.info(
        "latest %s (%s stocks): %s",
        latest["date"], latest.get("total"),
        ", ".join(f"{latest.get(key)}% > {label}" for key, label in _METRIC_LABELS),
    )

    if args.dry_run:
        logger.info("dry run — %s not written", OUTPUT_PATH.name)
        return 0
    OUTPUT_PATH.write_text(json.dumps(doc, separators=(",", ":")), encoding="utf-8")
    logger.info("wrote %s (%.1f KB)", OUTPUT_PATH, OUTPUT_PATH.stat().st_size / 1024)
    return 0


if __name__ == "__main__":
    sys.exit(main())
