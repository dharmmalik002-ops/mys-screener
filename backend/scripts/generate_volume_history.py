"""
Generate volume_history.json — per-symbol windowed daily-volume aggregates that
power the Volume screener's "highest volume over 1M/3M/6M/1Y" detection.

Run by the daily-bhavcopy GitHub Action after the EOD bhavcopy patch. It reads
the NSE-keyed universe, batch-downloads ~1.4 years of daily Volume via yfinance,
and writes COMPACT aggregates (not the full series) so the committed file stays
small:

    {
      "date": "YYYY-MM-DD",
      "updated_at": "...UTC ISO...",
      "windows": [21, 63, 126, 252],
      "symbols": {
        "RELIANCE": {"21": [prior_max, avg, sessions], "63": [...], "126": [...], "252": [...]},
        ...
      }
    }

For each window N (21≈1M, 63≈3M, 126≈6M, 252≈1Y trading sessions):
  - prior_max  = highest daily volume over the N sessions BEFORE the latest bar
  - avg        = average daily volume over the N-session window
  - sessions   = how many sessions were actually available (for honest labels)

The backend flags a stock as a fresh window-high when today's traded volume
(from the bhavcopy snapshot) meets or exceeds ``prior_max`` for the selected
window — i.e. it just printed its highest volume in that window.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
UNIVERSE_PATH = DATA_DIR / "free_universe.json"
OUTPUT_PATH = DATA_DIR / "volume_history.json"
WINDOWS = [21, 63, 126, 252]
MIN_SESSIONS = 5
CHUNK = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("volume_history")


def _load_tickers() -> list[str]:
    if not UNIVERSE_PATH.exists():
        logger.error("universe not found: %s", UNIVERSE_PATH)
        return []
    try:
        universe = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("failed to read universe: %s", exc)
        return []
    if not isinstance(universe, list):
        return []
    return [
        str(item.get("ticker", ""))
        for item in universe
        if str(item.get("ticker", "")).endswith(".NS")
    ]


def _aggregates_from_volumes(vols: list[int]) -> dict[str, list[int]] | None:
    # Keep only positive trading-day volumes (drop zero-volume holidays / halts).
    vols = [v for v in vols if v > 0]
    if len(vols) < MIN_SESSIONS:
        return None
    rec: dict[str, list[int]] = {}
    for n in WINDOWS:
        window = vols[-n:] if len(vols) > n else vols
        if len(window) < MIN_SESSIONS:
            window = vols  # fall back to all available data for short histories
        prior = window[:-1]
        prior_max = max(prior) if prior else 0
        avg = sum(window) / len(window)
        rec[str(n)] = [int(prior_max), int(avg), len(window)]
    return rec


def main() -> int:
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:
        logger.error("yfinance/pandas not installed; cannot build volume history")
        return 1

    tickers = _load_tickers()
    if not tickers:
        logger.error("no .NS tickers in universe; aborting")
        return 1
    logger.info("Computing volume history for %s tickers", len(tickers))

    end = datetime.now(IST).date() + timedelta(days=1)
    start = end - timedelta(days=420)  # ~1.4y calendar → ≥ 252 trading sessions
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    symbols: dict[str, dict] = {}

    def _series_to_int_list(series) -> list[int]:
        out: list[int] = []
        for value in series.tolist():
            if value is None or pd.isna(value):
                continue
            try:
                out.append(int(float(value)))
            except (TypeError, ValueError):
                continue
        return out

    for offset in range(0, len(tickers), CHUNK):
        chunk = tickers[offset : offset + CHUNK]
        try:
            df = yf.download(
                chunk,
                start=start_str,
                end=end_str,
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            logger.warning("batch %s failed: %s", offset // CHUNK, exc)
            continue
        if df is None or df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            try:
                vol_df = df["Volume"]
            except Exception:
                continue
            for ticker in chunk:
                if ticker not in vol_df.columns:
                    continue
                rec = _aggregates_from_volumes(_series_to_int_list(vol_df[ticker].dropna()))
                if rec:
                    symbols[ticker.replace(".NS", "")] = rec
        else:
            # Single-ticker frame: flat columns.
            if "Volume" in df.columns:
                rec = _aggregates_from_volumes(_series_to_int_list(df["Volume"].dropna()))
                if rec:
                    symbols[chunk[0].replace(".NS", "")] = rec

        logger.info("processed %s/%s tickers", min(offset + CHUNK, len(tickers)), len(tickers))

    if not symbols:
        logger.error("no volume history computed; not writing output")
        return 1

    payload = {
        "date": datetime.now(IST).date().isoformat(),
        "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "windows": WINDOWS,
        "symbols": symbols,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    logger.info("wrote %s with %s symbols", OUTPUT_PATH, len(symbols))
    return 0


if __name__ == "__main__":
    sys.exit(main())
