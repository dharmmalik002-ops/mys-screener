"""Price-index series for benchmarks that have no index-fund proxy.

Kept separate from the equity provider in `providers/free.py` on purpose: that
module carries the whole snapshot/scan cache and importing it to fetch one
index series would drag a large amount of state into the funds path. This is a
thin yfinance call with a disk cache, matching how `nav_source` behaves.

These are **price** indices — they exclude dividends and therefore understate
the index by roughly 1.2% a year on large caps. Every consumer surfaces that,
which is why an index-fund NAV is preferred wherever one exists.
"""

from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path
from typing import Any

from . import paths

# Long on purpose. The builder force-refreshes these nightly, so this TTL only
# decides how stale a series may get if the build is skipped — and a benchmark
# line a day or two behind is immaterial to a 3-year comparison chart. A short
# TTL, by contrast, puts a slow yfinance call on a user's request path.
CACHE_TTL_SECONDS = 5 * 24 * 60 * 60


class IndexUnavailable(RuntimeError):
    pass


def _cache_path(symbol: str) -> Path:
    safe = symbol.replace("^", "idx_").replace("/", "_").replace("=", "_")
    return paths.NAV_DIR / f"_index_{safe}.json"


def _read_cache(path: Path, *, require_ohlc: bool = False) -> dict[str, Any] | None:
    try:
        if (time.time() - path.stat().st_mtime) > CACHE_TTL_SECONDS:
            return None
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not (isinstance(payload, dict) and payload.get("dates")):
        return None
    # Caches written before OHLC was captured hold closes only. A candlestick
    # caller has to refetch rather than be handed a series it cannot draw.
    if require_ohlc and not payload.get("highs"):
        return None
    return payload


def _read_stale_cache(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) and payload.get("dates") else None


def fetch_index_series(symbol: str, *, force: bool = False, want_ohlc: bool = False) -> dict[str, Any]:
    """Full available daily history for one index symbol.

    Returns the same `dates`/`navs` shape as `nav_source.fetch_nav_history` so
    the two stay interchangeable everywhere downstream, plus `opens`/`highs`/
    `lows` aligned to the same index for callers that draw candles. Unlike a
    fund NAV, an index really does have an intraday high and low, so these are
    true daily bars rather than an aggregation.
    """
    path = _cache_path(symbol)
    if not force:
        cached = _read_cache(path, require_ohlc=want_ohlc)
        if cached:
            return cached

    try:
        import yfinance
    except ImportError as exc:  # pragma: no cover
        raise IndexUnavailable("yfinance is not installed") from exc

    try:
        frame = yfinance.Ticker(symbol).history(period="max", auto_adjust=False)
    except Exception as exc:
        stale = _read_stale_cache(path)
        if stale:
            return stale
        raise IndexUnavailable(f"{type(exc).__name__} fetching {symbol}") from exc

    dates: list[str] = []
    closes: list[float] = []
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    if frame is not None and len(frame):
        has_ohlc = all(column in frame for column in ("Open", "High", "Low"))
        columns = [frame["Close"]]
        if has_ohlc:
            columns += [frame["Open"], frame["High"], frame["Low"]]
        for row in zip(frame.index, *columns):
            stamp, close = row[0], row[1]
            try:
                value = float(close)
            except (TypeError, ValueError):
                continue
            if value != value or value <= 0:
                continue
            if has_ohlc:
                try:
                    bar = [float(row[2]), float(row[3]), float(row[4])]
                except (TypeError, ValueError):
                    bar = [value, value, value]
                # A NaN or non-positive leg makes an undrawable candle; fall
                # back to a flat bar at the close rather than dropping the day
                # and putting a hole in the close series.
                if any(x != x or x <= 0 for x in bar):
                    bar = [value, value, value]
                # Yahoo occasionally serves a high below the close on thin
                # index days. Clamp so the wick always contains the body.
                bar[1] = max(bar[1], bar[0], value)
                bar[2] = min(bar[2], bar[0], value)
                opens.append(bar[0])
                highs.append(bar[1])
                lows.append(bar[2])
            dates.append(stamp.date().isoformat() if hasattr(stamp, "date") else str(stamp)[:10])
            closes.append(value)

    # Yahoo serves a single trailing bar for some Indian indices (^CNXSC being
    # the reason `benchmarks.py` routes small caps through an index fund). One
    # bar is not a series — refuse it so callers fall back rather than drawing
    # a one-point benchmark line.
    if len(dates) < 60:
        stale = _read_stale_cache(path)
        if stale:
            return stale
        raise IndexUnavailable(f"{symbol} returned only {len(dates)} usable bars")

    payload = {
        "symbol": symbol,
        "dates": dates,
        "navs": closes,
        "is_price_index": True,
    }
    if len(opens) == len(dates):
        payload["opens"] = opens
        payload["highs"] = highs
        payload["lows"] = lows
    paths.ensure_dirs()
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, separators=(",", ":")))
    temp.replace(path)
    return payload
