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
import requests
import yfinance as yf

BarsLoader = Callable[[str, str], list[dict[str, Any]]]

BSE_ANN_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}

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


def fetch_bse_result_filings(lookback_days: int = 60) -> dict[str, date]:
    """Fetch BSE 'Result' category announcements for the last `lookback_days`.

    Returns a map keyed by BSE scrip code (e.g. "500325") to the most
    recent result-announcement date. BSE is the regulator of record so
    every listed company files there, which makes this a reliable
    quarter-after-quarter source — yfinance and Screener both lag and
    miss small caps. Failure to reach BSE returns an empty dict so the
    rest of the pipeline still works via the volume fallback.
    """
    today = datetime.now(timezone.utc).astimezone().date()
    cutoff = today - timedelta(days=lookback_days)
    params_base = {
        "strCat": "Result",
        "strPrevDate": cutoff.strftime("%Y%m%d"),
        "strToDate": today.strftime("%Y%m%d"),
        # `strSearch=P` is required — empty string returns {} on this endpoint.
        "strSearch": "P",
        "strscrip": "",
        "strType": "C",
    }
    filings: dict[str, date] = {}
    session = requests.Session()
    session.headers.update(BSE_HEADERS)
    # Prime cookies once; the API rejects bare requests sometimes.
    try:
        session.get("https://www.bseindia.com/corporates/ann.html", timeout=15)
    except Exception:
        pass
    total_pages: int | None = None
    for page in range(1, 100):
        if total_pages is not None and page > total_pages:
            break
        params = {**params_base, "pageno": str(page)}
        try:
            response = session.get(BSE_ANN_URL, params=params, timeout=25)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            LOGGER.warning("BSE Result filings page %d failed: %s", page, exc)
            break
        rows = payload.get("Table") or []
        if not rows:
            break
        if total_pages is None:
            try:
                total_pages = int(rows[0].get("TotalPageCnt") or 0) or None
            except Exception:
                total_pages = None
        for row in rows:
            scrip = str(row.get("SCRIP_CD") or "").strip()
            news_dt = str(row.get("NEWS_DT") or row.get("DissemDT") or "").strip()
            if not scrip or not news_dt:
                continue
            try:
                parsed = datetime.fromisoformat(news_dt.split(".")[0]).date()
            except Exception:
                continue
            existing = filings.get(scrip)
            if existing is None or parsed > existing:
                filings[scrip] = parsed
    LOGGER.info(
        "BSE result filings: %d unique scrips in last %d days (across %s pages)",
        len(filings), lookback_days, total_pages,
    )
    return filings


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


def _detect_high_volume_reaction_day(
    frame: pd.DataFrame,
    *,
    lookback_days: int,
    anchor_date: date | None = None,
    anchor_window_days: int = 7,
    min_rvol_50d: float = 1.8,
) -> int | None:
    """Find the most likely earnings-reaction day in the cached chart.

    yfinance's `earnings_dates` is often the period-end or estimate date
    for Indian stocks, not the announcement day. Symptom: rvol on the
    yf-reported day is < 1, which can't be earnings. Detect the real
    print by scanning recent sessions for a volume spike (>= 1.8x the
    prior 50-day average). If `anchor_date` is provided, prefer days
    within ± anchor_window_days of it.
    """
    if frame.empty:
        return None
    recent = frame.tail(lookback_days)
    if recent.empty:
        return None

    best_idx: int | None = None
    best_score = 0.0
    for pos in recent.index:
        prior = frame.iloc[max(0, int(pos) - 50):int(pos)]
        if prior.empty:
            continue
        avg_vol = float(prior["volume"].mean()) if "volume" in prior.columns else 0.0
        if avg_vol <= 0:
            continue
        row = frame.iloc[int(pos)]
        vol = float(row["volume"] or 0)
        if vol <= 0:
            continue
        rvol = vol / avg_vol
        if rvol < min_rvol_50d:
            continue

        score = rvol
        if anchor_date is not None:
            bar_date = row["date"]
            delta_days = abs((bar_date - anchor_date).days)
            if delta_days > anchor_window_days:
                continue
            # Closer to the anchor gets a small bonus so identical-rvol
            # ties resolve to the yfinance-reported neighborhood.
            score += max(0.0, (anchor_window_days - delta_days) / anchor_window_days)

        if score > best_score:
            best_score = score
            best_idx = int(pos)

    return best_idx


def _compute_one(
    symbol: str,
    ticker: str,
    bars_loader: BarsLoader,
    lookback_days: int,
    *,
    bse_filing_date: date | None = None,
) -> EarningsMetrics | None:
    # Source priority for the announcement date:
    #   1. BSE corporate filing (regulator of record, every listed co.)
    #   2. yfinance.earnings_dates (covers liquid NSE names well)
    #   3. Volume-anchor scan (best-effort fallback)
    yf_earnings_date = _fetch_yfinance_earnings_date(ticker, lookback_days)

    bars = bars_loader(symbol, ticker)
    frame = _bars_to_frame(bars)
    if frame.empty:
        return None

    event_idx: int | None = None
    source = "bse"

    def _anchor_to_session(anchor_date: date) -> int | None:
        on_or_after = frame[frame["date"] >= anchor_date]
        if on_or_after.empty:
            return None
        return int(on_or_after.index[0])

    # Step 1: BSE filing date is authoritative for Indian listed stocks.
    if bse_filing_date is not None:
        candidate = _anchor_to_session(bse_filing_date)
        if candidate is not None:
            event_idx = candidate
            source = "bse"

    # Step 2: yfinance fallback, with a sanity check on the day's volume.
    if event_idx is None and yf_earnings_date is not None:
        candidate = _anchor_to_session(yf_earnings_date)
        if candidate is not None:
            prior = frame.iloc[max(0, candidate - 50):candidate]
            avg_vol = float(prior["volume"].mean()) if not prior.empty else 0.0
            day_vol = float(frame.iloc[candidate]["volume"] or 0)
            day_rvol = (day_vol / avg_vol) if avg_vol > 0 else 0.0
            if day_rvol >= 1.5:
                event_idx = candidate
                source = "yfinance"

    # Step 3: when yfinance's day failed the sanity check, search ±7
    # sessions of its hint for a real volume spike. Skipped entirely
    # when both BSE and yfinance had nothing — without a verified
    # anchor, the highest-volume day could be any non-earnings event
    # (block deal, news, sector rally) and would mislabel the scanner.
    if event_idx is None and (bse_filing_date is not None or yf_earnings_date is not None):
        anchor = bse_filing_date or yf_earnings_date
        event_idx = _detect_high_volume_reaction_day(
            frame,
            lookback_days=lookback_days,
            anchor_date=anchor,
            anchor_window_days=7,
            min_rvol_50d=1.8,
        )
        if event_idx is not None:
            source = "anchor+volume"

    if event_idx is None or event_idx < 0 or event_idx >= len(frame):
        return None

    earnings_date_resolved = frame.iloc[event_idx]["date"]
    event_row = frame.iloc[event_idx]
    next_row = frame.iloc[event_idx + 1] if event_idx + 1 < len(frame) else None

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

    # Forward-return: use up to 5 sessions, falling back to whatever
    # we have if the earnings landed within the last week (the user
    # otherwise sees zero matches every Monday morning).
    return_5d_pct: float | None = None
    available_forward = len(frame) - 1 - event_idx
    if available_forward >= 1 and _is_finite(event_row["close"]) and float(event_row["close"]) > 0:
        sessions_used = min(5, available_forward)
        forward_row = frame.iloc[event_idx + sessions_used]
        if _is_finite(forward_row["close"]):
            return_5d_pct = round((float(forward_row["close"]) / float(event_row["close"]) - 1.0) * 100.0, 3)

    return EarningsMetrics(
        symbol=symbol.upper(),
        earnings_date=earnings_date_resolved,
        close_in_range_pct=round(close_pos, 4) if close_pos is not None else None,
        next_day_gap_pct=next_day_gap_pct,
        day_rvol_50d=day_rvol,
        return_5d_pct=return_5d_pct,
        source=source,
    )


def compute_metrics(
    universe: Iterable[dict[str, Any]],
    *,
    bars_loader: BarsLoader,
    lookback_days: int = EARNINGS_LOOKBACK_DAYS,
    max_workers: int = 4,
    bse_filing_dates: dict[str, date] | None = None,
) -> dict[str, dict[str, Any]]:
    """Compute metrics for every symbol in `universe`.

    When `bse_filing_dates` is provided (keyed by BSE scrip code as
    string), each stock's announcement date is sourced from BSE first
    and only falls through to yfinance / volume detection when BSE has
    no entry. Universe entries should include a `bse_code` field.
    """
    triples = [
        (
            str(item.get("symbol") or "").upper(),
            str(item.get("ticker") or "").strip(),
            str(item.get("bse_code") or "").strip(),
        )
        for item in universe
        if str(item.get("symbol") or "").strip()
    ]
    results: dict[str, dict[str, Any]] = {}
    bse_map = bse_filing_dates or {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(
                _compute_one,
                symbol,
                ticker,
                bars_loader,
                lookback_days,
                bse_filing_date=bse_map.get(bse_code),
            ): symbol
            for symbol, ticker, bse_code in triples
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
