"""Compute and load post-earnings reaction metrics for the universe.

The Positive Earnings scanner needs per-stock numbers that aren't on the
standard snapshot: when the latest result was released and how the price
behaved on / after that day. Recomputing all of that during the snapshot
refresh would be too slow (and is fragile to yfinance rate limits), so
we precompute it as a sidecar file (``data/earnings_metrics.json``) and
merge it onto the snapshot when it's read.

Computation runs on demand via
``backend/scripts/compute_earnings_metrics.py``, typically alongside the
daily bhavcopy patch. Two inputs:

* yfinance ``Ticker.earnings_dates`` → the announcement timestamp for
  the most recent result.
* The persisted daily chart cache (``data/chart_cache/<SYMBOL>__1D.json``)
  → OHLCV around that date for the reaction math.

When yfinance has nothing (small/illiquid names) we fall back to the
trade date on or right after the cached quarterly_results
``result_document_url`` filing day, if available. Anything we can't
resolve confidently is omitted — the scanner treats absence as "no
qualifying result".
"""
from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd
import yfinance as yf

BarsLoader = Callable[[str, str], list[dict[str, Any]]]

LOGGER = logging.getLogger(__name__)

EARNINGS_LOOKBACK_DAYS = 60
EARNINGS_CACHE_VERSION = 1


@dataclass
class EarningsMetrics:
    symbol: str
    earnings_date: date
    close_in_range_pct: float | None
    next_day_gap_pct: float | None
    day_rvol_50d: float | None
    return_5d_pct: float | None
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "earnings_date": self.earnings_date.isoformat(),
            "close_in_range_pct": self.close_in_range_pct,
            "next_day_gap_pct": self.next_day_gap_pct,
            "day_rvol_50d": self.day_rvol_50d,
            "return_5d_pct": self.return_5d_pct,
            "source": self.source,
        }


def metrics_file_path(backend_root: Path) -> Path:
    return backend_root / "data" / "earnings_metrics.json"


def load_metrics_file(backend_root: Path) -> dict[str, dict[str, Any]]:
    """Read the cached metrics file into a symbol-keyed map."""
    path = metrics_file_path(backend_root)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if int(payload.get("cache_version", 0) or 0) != EARNINGS_CACHE_VERSION:
        return {}
    entries = payload.get("entries") or {}
    if not isinstance(entries, dict):
        return {}
    return {str(symbol).upper(): value for symbol, value in entries.items() if isinstance(value, dict)}


def save_metrics_file(backend_root: Path, entries: dict[str, dict[str, Any]]) -> None:
    path = metrics_file_path(backend_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "cache_version": EARNINGS_CACHE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _close_in_range_pct(open_: float, high: float, low: float, close: float) -> float | None:
    if not all(map(_is_finite, (open_, high, low, close))):
        return None
    rng = high - low
    if rng <= 0:
        # Flat candle — treat as a draw; the scanner threshold isn't met.
        return 0.5
    return max(0.0, min(1.0, (close - low) / rng))


def _is_finite(value: Any) -> bool:
    try:
        return pd.notna(value) and value is not None
    except Exception:
        return False


def _read_chart_cache_bars(chart_cache_dir: Path, symbol: str) -> list[dict[str, Any]]:
    """Read the persisted 1D chart cache for `symbol`."""
    safe_symbol = re.sub(r"[^A-Za-z0-9._-]+", "_", symbol.upper())
    path = chart_cache_dir / f"{safe_symbol}__1D.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    bars = payload.get("bars")
    if not isinstance(bars, list):
        return []
    return bars


def _bars_to_frame(bars: list[dict[str, Any]]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()
    frame = pd.DataFrame(bars)
    if "time" not in frame.columns or "close" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["time"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.date
    for col in ("open", "high", "low", "close", "volume"):
        if col not in frame.columns:
            frame[col] = pd.NA
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def _fetch_yfinance_earnings_date(ticker: str, lookback_days: int) -> date | None:
    """Return the announcement date of the most recent result within the
    lookback window, or None."""
    try:
        events = yf.Ticker(ticker).earnings_dates
    except Exception as exc:
        LOGGER.debug("yfinance earnings_dates failed for %s: %s", ticker, exc)
        return None
    if events is None or len(events) == 0:
        return None
    try:
        idx = pd.to_datetime(events.index, utc=True, errors="coerce")
    except Exception:
        return None
    today_ist = datetime.now(timezone.utc).astimezone().date()
    cutoff = today_ist - timedelta(days=lookback_days)
    candidates: list[date] = []
    for timestamp in idx:
        if pd.isna(timestamp):
            continue
        d = timestamp.tz_convert("Asia/Kolkata").date() if timestamp.tzinfo else timestamp.date()
        if cutoff <= d <= today_ist:
            candidates.append(d)
    if not candidates:
        return None
    return max(candidates)


def yfinance_bars_loader(ticker: str, lookback_days: int = 120) -> list[dict[str, Any]]:
    """Fetch enough daily OHLCV from yfinance to score one earnings event.

    Used by the standalone compute script when ``data/chart_cache`` isn't
    available (e.g. GitHub Actions runner). Returns bars in the same
    shape as the chart_cache files: dicts with ``time`` (unix seconds),
    ``open``, ``high``, ``low``, ``close``, ``volume``.
    """
    period = "6mo" if lookback_days <= 120 else "1y"
    try:
        frame = yf.Ticker(ticker).history(period=period, interval="1d", auto_adjust=False)
    except Exception as exc:
        LOGGER.debug("yfinance history failed for %s: %s", ticker, exc)
        return []
    if frame is None or frame.empty:
        return []
    bars: list[dict[str, Any]] = []
    for timestamp, row in frame.iterrows():
        try:
            close = float(row["Close"])
        except Exception:
            continue
        if not _is_finite(close) or close <= 0:
            continue
        ts = pd.Timestamp(timestamp)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        bars.append({
            "time": int(ts.tz_convert("UTC").timestamp()),
            "open": float(row["Open"]) if _is_finite(row.get("Open")) else close,
            "high": float(row["High"]) if _is_finite(row.get("High")) else close,
            "low": float(row["Low"]) if _is_finite(row.get("Low")) else close,
            "close": close,
            "volume": int(row["Volume"]) if _is_finite(row.get("Volume")) else 0,
        })
    return bars


def chart_cache_bars_loader_factory(chart_cache_dir: Path) -> BarsLoader:
    def _load(symbol: str, _ticker: str) -> list[dict[str, Any]]:
        return _read_chart_cache_bars(chart_cache_dir, symbol)
    return _load


def _compute_one(
    symbol: str,
    ticker: str,
    bars_loader: BarsLoader,
    lookback_days: int,
) -> EarningsMetrics | None:
    earnings_date = _fetch_yfinance_earnings_date(ticker, lookback_days)
    if earnings_date is None:
        return None

    bars = bars_loader(symbol, ticker)
    frame = _bars_to_frame(bars)
    if frame.empty:
        return None

    # Locate the trading-day index for the earnings date. If the
    # announcement was after-hours or on a non-trading day, snap to the
    # next available session — that's the day the move actually prints.
    on_or_after = frame[frame["date"] >= earnings_date]
    if on_or_after.empty:
        return None
    event_idx = int(on_or_after.index[0])
    if event_idx < 0 or event_idx >= len(frame):
        return None

    event_row = frame.iloc[event_idx]
    next_row = frame.iloc[event_idx + 1] if event_idx + 1 < len(frame) else None
    plus5_row = frame.iloc[event_idx + 5] if event_idx + 5 < len(frame) else None

    pos_event = _close_in_range_pct(
        float(event_row["open"]), float(event_row["high"]),
        float(event_row["low"]), float(event_row["close"]),
    )
    pos_next = None
    if next_row is not None:
        pos_next = _close_in_range_pct(
            float(next_row["open"]), float(next_row["high"]),
            float(next_row["low"]), float(next_row["close"]),
        )
    # Use the better of the two: spec says "earning day OR next day".
    close_pos = max([p for p in (pos_event, pos_next) if p is not None], default=None)

    next_day_gap_pct: float | None = None
    if next_row is not None and _is_finite(event_row["close"]) and _is_finite(next_row["open"]) and float(event_row["close"]) > 0:
        next_day_gap_pct = round((float(next_row["open"]) / float(event_row["close"]) - 1.0) * 100.0, 3)

    # 50-day avg volume of the 50 sessions BEFORE the event.
    day_rvol: float | None = None
    if _is_finite(event_row["volume"]):
        prior = frame.iloc[max(0, event_idx - 50):event_idx]
        if not prior.empty:
            avg_vol_50 = float(prior["volume"].mean())
            if avg_vol_50 > 0:
                day_rvol = round(float(event_row["volume"]) / avg_vol_50, 3)

    return_5d_pct: float | None = None
    if plus5_row is not None and _is_finite(event_row["close"]) and _is_finite(plus5_row["close"]) and float(event_row["close"]) > 0:
        return_5d_pct = round((float(plus5_row["close"]) / float(event_row["close"]) - 1.0) * 100.0, 3)

    return EarningsMetrics(
        symbol=symbol.upper(),
        earnings_date=earnings_date,
        close_in_range_pct=round(close_pos, 4) if close_pos is not None else None,
        next_day_gap_pct=next_day_gap_pct,
        day_rvol_50d=day_rvol,
        return_5d_pct=return_5d_pct,
        source="yfinance",
    )


def compute_metrics(
    universe: Iterable[dict[str, Any]],
    *,
    bars_loader: BarsLoader,
    lookback_days: int = EARNINGS_LOOKBACK_DAYS,
    max_workers: int = 4,
) -> dict[str, dict[str, Any]]:
    """Compute metrics for every symbol in `universe`."""
    pairs = [
        (str(item.get("symbol") or "").upper(), str(item.get("ticker") or "").strip())
        for item in universe
        if str(item.get("symbol") or "").strip()
    ]
    results: dict[str, dict[str, Any]] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_compute_one, symbol, ticker, bars_loader, lookback_days): symbol
            for symbol, ticker in pairs
            if ticker
        }
        for index, future in enumerate(as_completed(future_map), start=1):
            symbol = future_map[future]
            try:
                metrics = future.result()
            except Exception as exc:
                LOGGER.warning("earnings metrics failed for %s: %s", symbol, exc)
                continue
            if metrics is None:
                continue
            results[metrics.symbol] = metrics.to_dict()
            if index % 100 == 0:
                LOGGER.info("earnings metrics: processed %d / %d", index, len(future_map))

    return results
