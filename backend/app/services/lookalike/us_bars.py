"""Daily bars for the reference tickers.

The references are mostly US names, which the Indian stores do not carry, so
they come from Yahoo and are cached under `data/lookalike/us_bars/` (gitignored:
raw material, rebuilt on demand). Prices are split- and dividend-adjusted, so a
split inside a base reads as a split and not as a crash.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# Enough history before the earliest reference for the window plus its 50-day
# average (~170 sessions), with room for random control days on either side.
LOOKBACK_DAYS = 900


@dataclass(frozen=True)
class Series:
    ticker: str
    dates: list[date]
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    v: np.ndarray

    def index_on_or_before(self, day: date) -> int | None:
        """Last session on or before `day`. A newsletter dated on a weekend or
        holiday describes the chart as of the prior close."""
        lo, hi = 0, len(self.dates)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.dates[mid] <= day:
                lo = mid + 1
            else:
                hi = mid
        return lo - 1 if lo > 0 else None


def cache_dir(data_dir: Path) -> Path:
    return data_dir / "lookalike" / "us_bars"


def _read(path: Path) -> Series | None:
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return Series(
        raw["ticker"],
        [date.fromordinal(d) for d in raw["d"]],
        *(np.asarray(raw[k], dtype=float) for k in ("o", "h", "l", "c", "v")),
    )


def _fetch(ticker: str, start: date) -> Series | None:
    import yfinance as yf  # optional dependency, only needed to build the library

    for attempt in range(3):
        try:
            # Yahoo spells share classes with a dash: BRK.B -> BRK-B
            df = yf.Ticker(ticker.replace(".", "-")).history(start=start.isoformat(), auto_adjust=True, actions=False)
            break
        except Exception as exc:  # network / throttling — retry, then give up on this ticker
            logger.warning("yahoo %s attempt %d failed: %s", ticker, attempt + 1, exc)
            time.sleep(2 * (attempt + 1))
    else:
        return None
    if df is None or df.empty:
        return None
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return Series(
        ticker,
        [ts.date() for ts in df.index],
        df["Open"].to_numpy(float),
        df["High"].to_numpy(float),
        df["Low"].to_numpy(float),
        df["Close"].to_numpy(float),
        df["Volume"].fillna(0).to_numpy(float),
    )


def load(data_dir: Path, ticker: str, earliest: date, need_through: date) -> Series | None:
    """Bars covering `earliest - LOOKBACK_DAYS` through `need_through`, from the
    cache when it already covers that span, otherwise re-fetched."""
    path = cache_dir(data_dir) / f"{ticker}.json"
    start = earliest - timedelta(days=LOOKBACK_DAYS)
    cached = _read(path) if path.exists() else None
    if cached and cached.dates and cached.dates[0] <= start + timedelta(days=10) and cached.dates[-1] >= need_through:
        return cached
    # A delisted or acquired name never reaches `need_through`; without this it
    # would be re-downloaded on every build. One attempt per day is enough.
    stamp = path.with_suffix(".fetched")
    if stamp.exists() and stamp.read_text().strip() == date.today().isoformat():
        return cached
    series = _fetch(ticker, start)
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text(date.today().isoformat())
    if series is None:
        return cached
    path.write_text(json.dumps({
        "ticker": ticker,
        "d": [d.toordinal() for d in series.dates],
        **{k: [round(float(x), 4) for x in getattr(series, k)] for k in ("o", "h", "l", "c", "v")},
    }))
    return series
