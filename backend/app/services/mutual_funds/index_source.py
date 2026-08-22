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


def _read_cache(path: Path) -> dict[str, Any] | None:
    try:
        if (time.time() - path.stat().st_mtime) > CACHE_TTL_SECONDS:
            return None
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) and payload.get("dates") else None


def _read_stale_cache(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) and payload.get("dates") else None


def fetch_index_series(symbol: str, *, force: bool = False) -> dict[str, Any]:
    """Full available daily close history for one index symbol.

    Returns the same two-array shape as `nav_source.fetch_nav_history` so the
    two are interchangeable everywhere downstream.
    """
    path = _cache_path(symbol)
    if not force:
        cached = _read_cache(path)
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
    if frame is not None and len(frame):
        for stamp, close in zip(frame.index, frame["Close"]):
            try:
                value = float(close)
            except (TypeError, ValueError):
                continue
            if value != value or value <= 0:
                continue
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
    paths.ensure_dirs()
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, separators=(",", ":")))
    temp.replace(path)
    return payload
