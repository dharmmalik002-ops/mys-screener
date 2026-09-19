"""Deep daily-bar store for the backtest engine.

Why a second bar store when `chart_cache/` already exists: the chart cache holds
~2 years per symbol, which is what the charts need and nowhere near what a
regime study needs. Attributing strategy performance to market conditions means
seeing each condition many times — 2024-2026 contains one uptrend and one
shallow correction, so every "this setup works in a downtrend" claim would rest
on a handful of weeks. This store goes back as far as the vendor will serve
(Nifty to 2007, most large caps to the late 1990s), which covers 2008, 2011,
2013, 2018, 2020, 2022 and 2025 — enough cycles that a regime bucket carries a
real sample.

Format: one gzipped columnar JSON per symbol. Columnar rather than a list of
row dicts because the key names dominate row-wise JSON — the same history is
~4x smaller this way — and dates are stored as ordinals for the same reason.
Prices are split- and dividend-adjusted (`auto_adjust`), so a 1:10 split reads
as a split and not as a 90% crash.

The store is a local research artifact: it is gitignored, rebuilt by
`scripts/build_deep_history.py`, and nothing in the live app depends on it.
"""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator

import numpy as np

logger = logging.getLogger(__name__)

STORE_VERSION = 1
# Below this a symbol cannot support a 200-day trend filter plus a forward
# window, so it would only ever contribute noise.
MIN_BARS = 260


@dataclass(frozen=True)
class Bars:
    """One symbol's daily history as parallel numpy arrays.

    Arrays rather than a DataFrame: the engine walks ~1,500 symbols x ~5,000
    bars per run and pandas' per-access overhead dominates at that size.
    """

    symbol: str
    dates: np.ndarray   # dtype=object, datetime.date, ascending, unique
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray

    def __len__(self) -> int:
        return len(self.dates)

    @property
    def first_date(self) -> date | None:
        return self.dates[0] if len(self.dates) else None

    @property
    def last_date(self) -> date | None:
        return self.dates[-1] if len(self.dates) else None

    def index_of(self, day: date) -> int | None:
        """Position of `day`, or None when the symbol did not trade that session.

        Callers must treat None as "no bar" and skip, never as "use the previous
        close" — carrying a price forward onto a day with no bar invents signals
        on halted and suspended names.
        """
        pos = int(np.searchsorted(self.dates, day))
        if pos < len(self.dates) and self.dates[pos] == day:
            return pos
        return None


def store_dir(data_dir: Path) -> Path:
    return data_dir / "deep_history"


def _path_for(data_dir: Path, symbol: str) -> Path:
    safe = symbol.replace("/", "_").replace("\\", "_")
    return store_dir(data_dir) / f"{safe}.json.gz"


def write_bars(data_dir: Path, symbol: str, ticker: str, rows: list[dict]) -> int:
    """Persist one symbol. `rows` are dicts with date/open/high/low/close/volume.

    Returns the number of bars written. Rows are de-duplicated on date keeping
    the last occurrence and sorted ascending, so a re-fetch that overlaps the
    existing range is idempotent.
    """
    clean: dict[date, tuple[float, float, float, float, float]] = {}
    for row in rows:
        day = row.get("date")
        if not isinstance(day, date):
            continue
        try:
            o, h, l, c = (float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]))
            v = float(row.get("volume") or 0.0)
        except (KeyError, TypeError, ValueError):
            continue
        # A zero or negative price is a vendor artefact, not a trade.
        if not all(np.isfinite(x) and x > 0 for x in (o, h, l, c)):
            continue
        clean[day] = (o, h, l, c, v)

    if not clean:
        return 0

    days = sorted(clean)
    payload = {
        "store_version": STORE_VERSION,
        "symbol": symbol,
        "ticker": ticker,
        "n": len(days),
        "first_date": days[0].isoformat(),
        "last_date": days[-1].isoformat(),
        # Ordinals: ~40% smaller than ISO strings even after gzip, and they
        # decode straight back to date objects.
        "d": [d.toordinal() for d in days],
        "o": [round(clean[d][0], 4) for d in days],
        "h": [round(clean[d][1], 4) for d in days],
        "l": [round(clean[d][2], 4) for d in days],
        "c": [round(clean[d][3], 4) for d in days],
        "v": [int(clean[d][4]) for d in days],
    }

    path = _path_for(data_dir, symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.replace(path)
    return len(days)


def read_bars(data_dir: Path, symbol: str) -> Bars | None:
    path = _path_for(data_dir, symbol)
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as exc:
        logger.warning("deep history: unreadable %s: %s", path.name, exc)
        return None
    try:
        dates = np.array([date.fromordinal(int(x)) for x in payload["d"]], dtype=object)
        return Bars(
            symbol=str(payload.get("symbol") or symbol).upper(),
            dates=dates,
            open=np.asarray(payload["o"], dtype=np.float64),
            high=np.asarray(payload["h"], dtype=np.float64),
            low=np.asarray(payload["l"], dtype=np.float64),
            close=np.asarray(payload["c"], dtype=np.float64),
            volume=np.asarray(payload["v"], dtype=np.float64),
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("deep history: malformed %s: %s", path.name, exc)
        return None


def available_symbols(data_dir: Path) -> list[str]:
    directory = store_dir(data_dir)
    if not directory.exists():
        return []
    return sorted(p.name[: -len(".json.gz")] for p in directory.glob("*.json.gz"))


def iter_bars(data_dir: Path, symbols: list[str] | None = None, min_bars: int = MIN_BARS) -> Iterator[Bars]:
    """Stream the store one symbol at a time.

    Streaming rather than loading a dict of every symbol: the full store as
    numpy arrays is several GB, which the 16 GB Space cannot hold alongside
    everything else.
    """
    for symbol in symbols if symbols is not None else available_symbols(data_dir):
        bars = read_bars(data_dir, symbol)
        if bars is not None and len(bars) >= min_bars:
            yield bars


def store_summary(data_dir: Path) -> dict:
    """Coverage of the store, for the UI's data-health panel."""
    directory = store_dir(data_dir)
    if not directory.exists():
        return {"symbols": 0, "present": False}
    paths = list(directory.glob("*.json.gz"))
    total_bytes = sum(p.stat().st_size for p in paths)
    return {
        "present": bool(paths),
        "symbols": len(paths),
        "megabytes": round(total_bytes / 1024 / 1024, 1),
        "path": str(directory),
    }
