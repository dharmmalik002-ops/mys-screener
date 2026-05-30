"""
Generate bhavcopy_patch.json from the NSE Bhavcopy CSV.

This script is called by the GitHub Actions daily-bhavcopy.yml workflow after
India market close.  It fetches the official NSE EOD prices for all listed
equities and writes a compact patch file to backend/data/bhavcopy_patch.json.

The patch file is then committed back to main, which triggers the deploy
workflow and pushes the updated prices to HuggingFace Spaces.  On every cold
start, apply_committed_bhavcopy_patch() reads this file and immediately serves
official EOD prices without any live NSE network access from HF servers (which
are often geo-blocked by NSE).

Format written:
    {
      "date": "YYYY-MM-DD",
      "updated_at": "YYYY-MM-DDTHH:MM:SS+00:00",
      "source": "BSE" | "NSE" | "YFINANCE",
      "symbols": {
        "RELIANCE": {"o": 1340.0, "h": 1355.0, "l": 1330.0, "c": 1347.8, "v": 4500000, "p": 1300.0},
        ...
      }
    }
"""

from __future__ import annotations

import csv
import io
import json
import logging
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUTPUT_PATH = DATA_DIR / "bhavcopy_patch.json"
# XP market breadth score artifacts. The history file is tiny (a handful of
# numbers per day) and is the only one the HF backend reads. The MA-state store
# holds the per-symbol EMA(10)/EMA(20) state needed for the MA% inputs and is
# used only by this generator.
XP_HISTORY_PATH = DATA_DIR / "xp_breadth_history.json"
XP_ROLLING_PATH = DATA_DIR / "xp_ma_state.json"

# Make the `app` package importable so we can reuse the shared XP engine
# (backend/app/services/xp_breadth.py) without duplicating the formula here.
_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

RETRY_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2

BHAV_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_yyyymmdd}_F_0000.csv.zip"
BHAV_URL_ALT = "https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_yyyymmdd}_F_0000.csv.zip"
BHAV_URL_LEGACY = "https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{mon_upper}/cm{dd}{mon_upper}{year}bhav.csv.zip"
BHAV_URL_LEGACY_ALT = "https://archives.nseindia.com/content/historical/EQUITIES/{year}/{mon_upper}/cm{dd}{mon_upper}{year}bhav.csv.zip"

# BSE (Bombay Stock Exchange) bhavcopy — same-day authoritative data, no geo-blocking.
# New format CSV (no zip) available from ~4 PM IST: accessible from any IP globally.
BSE_BHAV_URL = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_yyyymmdd}_F_0000.CSV"

# Live NSE listed-equities master. Refreshed every weekday and reflects new
# listings within hours of their first session. We fetch this so that brand-new
# IPOs land in the daily bhavcopy patch (and therefore in the snapshot file)
# on the day they list, not the day after the universe cache rolls over.
NSE_LISTED_EQUITIES_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NEW_LISTING_LOOKBACK_DAYS = 365  # window inside which we track listings as IPOs

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}

BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.bseindia.com/",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bhavcopy_patch")


def _request_with_retry(
    session: requests.Session | None,
    url: str,
    *,
    headers: dict[str, str],
    timeout: int,
    attempts: int = RETRY_ATTEMPTS,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = (session or requests).get(url, headers=headers, timeout=timeout)
            logger.info("GET %s -> %s (attempt %s/%s)", url, response.status_code, attempt, attempts)
            return response
        except Exception as exc:
            last_error = exc
            logger.warning("GET %s failed on attempt %s/%s: %s", url, attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"request failed after {attempts} attempts: {url}") from last_error


def _fetch_bhavcopy_csv(trade_date: date) -> str | None:
    date_yyyymmdd = trade_date.strftime("%Y%m%d")
    dd = trade_date.strftime("%d")
    mon_upper = trade_date.strftime("%b").upper()
    year = trade_date.strftime("%Y")

    urls = [
        BHAV_URL.format(date_yyyymmdd=date_yyyymmdd),
        BHAV_URL_ALT.format(date_yyyymmdd=date_yyyymmdd),
        BHAV_URL_LEGACY.format(dd=dd, mon_upper=mon_upper, year=year),
        BHAV_URL_LEGACY_ALT.format(dd=dd, mon_upper=mon_upper, year=year),
    ]

    for url in urls:
        try:
            with requests.Session() as s:
                s.headers.update(HEADERS)
                # Prime cookies like a browser; harmless for archive URLs and
                # helps when NSE tightens anti-bot checks.
                _request_with_retry(s, "https://www.nseindia.com", headers=HEADERS, timeout=10)
                r = _request_with_retry(s, url, headers=HEADERS, timeout=30)

            if r.status_code != 200 or not r.content:
                logger.warning("NSE archive unavailable for %s with status %s", url, r.status_code)
                continue

            # NSE usually serves a ZIP, but handle a direct CSV response too.
            is_zip = r.content[:2] == b"PK" or url.endswith(".zip")
            if is_zip:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    name = next((n for n in z.namelist() if n.lower().endswith(".csv")), None)
                    if name:
                        return z.read(name).decode("utf-8", errors="replace")
            else:
                return r.text
        except Exception as exc:
            logger.warning("NSE fetch failed for %s: %s", url, exc)
    return None


def _parse_bhavcopy_csv(csv_text: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return result

    headers_found = set(rows[0].keys())

    if "TckrSymb" in headers_found:
        # Current NSE CM Bhavcopy format
        for row in rows:
            if row.get("SctySrs", "").strip() not in ("EQ", "BE", "BZ", "SM", "ST"):
                continue
            sym = row.get("TckrSymb", "").strip()
            if not sym:
                continue
            try:
                result[sym.upper()] = {
                    "o": float(row.get("OpnPric") or 0),
                    "h": float(row.get("HghPric") or 0),
                    "l": float(row.get("LwPric") or 0),
                    "c": float(row.get("ClsPric") or 0),
                    "v": int(float(row.get("TtlTradgVol") or 0)),
                    "p": float(row.get("PrvsClsgPric") or 0),
                }
            except (ValueError, TypeError):
                continue
    elif "SYMBOL" in headers_found:
        # Legacy NSE Bhavcopy format
        for row in rows:
            if row.get("SERIES", "").strip() not in ("EQ", "BE", "BZ", "SM", "ST"):
                continue
            sym = row.get("SYMBOL", "").strip()
            if not sym:
                continue
            try:
                result[sym.upper()] = {
                    "o": float(row.get("OPEN") or 0),
                    "h": float(row.get("HIGH") or 0),
                    "l": float(row.get("LOW") or 0),
                    "c": float(row.get("CLOSE") or 0),
                    "v": int(float(row.get("TOTTRDQTY") or 0)),
                    "p": float(row.get("PREVCLOSE") or 0),
                }
            except (ValueError, TypeError):
                continue

    return result


def _fetch_from_bse(trade_date: date) -> dict[str, dict] | None:
    """Fetch EOD prices from BSE (Bombay Stock Exchange) bhavcopy CSV.

    BSE publishes the same OHLCV data as NSE and — crucially — does NOT
    geo-block non-Indian IPs.  The new-format CSV is available directly
    (no zip) from ~4 PM IST the same day.

    Returns the same compact symbol-dict format as _parse_bhavcopy_csv, or None.
    """
    date_yyyymmdd = trade_date.strftime("%Y%m%d")
    url = BSE_BHAV_URL.format(date_yyyymmdd=date_yyyymmdd)
    try:
        resp = _request_with_retry(None, url, headers=BSE_HEADERS, timeout=20)
        if resp.status_code != 200 or len(resp.content) < 1000:
            logger.warning("BSE unavailable for %s with status %s", url, resp.status_code)
            return None

        reader = csv.DictReader(io.StringIO(resp.text))
        rows = list(reader)
        if not rows:
            return None

        result: dict[str, dict] = {}
        for row in rows:
            # Only equity series (A, B, T, XT, Z, S etc. — filter by SctySrs presence)
            # BSE uses SctySrs values like A, B, T, Z, S, XT — all are equities
            series = row.get("SctySrs", "").strip()
            if not series or series in ("D", "G", "GB", "GS"):
                # Skip debt / government securities
                continue
            sym = row.get("TckrSymb", "").strip().upper()
            if not sym:
                continue
            try:
                result[sym] = {
                    "o": float(row.get("OpnPric") or 0),
                    "h": float(row.get("HghPric") or 0),
                    "l": float(row.get("LwPric") or 0),
                    "c": float(row.get("ClsPric") or 0),
                    "v": int(float(row.get("TtlTradgVol") or 0)),
                    "p": float(row.get("PrvsClsgPric") or 0),
                }
            except (ValueError, TypeError):
                continue

        if not result:
            return None

        logger.info("BSE fetched %s symbols for %s", len(result), trade_date.isoformat())
        return result
    except Exception as exc:
        logger.warning("BSE fetch failed: %s", exc)
        return None


def _fetch_recent_nse_listings(trade_date: date) -> dict[str, dict]:
    """Fetch the live NSE listed-equities master and return symbols whose
    listing_date falls within NEW_LISTING_LOOKBACK_DAYS of trade_date.

    The intent is to discover IPOs that are NOT yet present in the static
    ``free_universe.json`` file shipped with the repo, so the bhavcopy patch
    can carry their price + metadata on day one. Returns a dict keyed by
    NSE symbol → {listing_date, name, isin}. Empty dict on any failure
    (NSE archive is occasionally geo-blocked from GitHub runners).
    """
    headers = {
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "text/csv,*/*",
        "Referer": "https://www.nseindia.com/",
    }
    try:
        response = requests.get(NSE_LISTED_EQUITIES_URL, headers=headers, timeout=30)
        response.raise_for_status()
        text = response.text
    except Exception as exc:
        logger.warning("Could not fetch NSE listings master (%s); skipping new-IPO discovery", exc)
        return {}

    cutoff = trade_date - timedelta(days=NEW_LISTING_LOOKBACK_DAYS)
    listings: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(text))
    for raw_row in reader:
        row = {str(key).strip(): value for key, value in raw_row.items()}
        series = str(row.get("SERIES") or "").strip().upper()
        if series and series != "EQ":
            continue
        sym = str(row.get("SYMBOL") or "").strip().upper()
        if not sym:
            continue
        raw_listing = (row.get("DATE OF LISTING") or "").strip()
        listing_dt: date | None = None
        # NSE master uses "DD-MMM-YYYY" (e.g. "12-APR-2026"). Be forgiving.
        for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                listing_dt = datetime.strptime(raw_listing, fmt).date()
                break
            except ValueError:
                continue
        if listing_dt is None or listing_dt < cutoff or listing_dt > trade_date:
            continue
        listings[sym] = {
            "listing_date": listing_dt.isoformat(),
            "name": str(row.get("NAME OF COMPANY") or sym).strip(),
            "isin": str(row.get("ISIN NUMBER") or "").strip() or None,
        }
    logger.info("Discovered %s recent NSE listings within %s-day window", len(listings), NEW_LISTING_LOOKBACK_DAYS)
    return listings


def _last_trading_day() -> date:
    """Return the most recent weekday (Mon-Fri). Called after market close."""
    now_ist = datetime.now(IST)
    d = now_ist.date()
    # If it's before 4 PM IST, use yesterday (market hasn't fully closed & bhavcopy not yet posted)
    if now_ist.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _fetch_from_yfinance(trade_date: date, extra_tickers: list[str] | None = None) -> dict[str, dict] | None:
    """Fallback: fetch EOD prices via yfinance when NSE archives are geo-blocked.

    GitHub Actions servers are outside India and NSE often blocks them.  Yahoo
    Finance is globally accessible, so this ensures we always have today's data.
    Returns the same compact symbol-dict format as _parse_bhavcopy_csv, or None.

    ``extra_tickers`` lets the caller inject recently listed `.NS` symbols
    that aren't yet in ``free_universe.json``. They're fetched alongside the
    universe so the patch carries day-1 data for brand-new IPOs.
    """
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        logger.warning("yfinance/pandas not installed; skipping yfinance fallback")
        return None

    universe_path = DATA_DIR / "free_universe.json"
    if not universe_path.exists():
        logger.warning("free_universe.json not found; skipping yfinance fallback")
        return None

    try:
        universe = json.loads(universe_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to read universe: %s", exc)
        return None

    if not isinstance(universe, list):
        return None

    # Extract .NS tickers (already stored in the universe file)
    tickers = [s.get("ticker", "") for s in universe if str(s.get("ticker", "")).endswith(".NS")]
    if extra_tickers:
        already = {t for t in tickers}
        for extra in extra_tickers:
            if extra and extra not in already:
                tickers.append(extra)
                already.add(extra)
    if not tickers:
        return None

    # Fetch a 7-calendar-day window so we have at least 2 trading days. The last
    # bar on/before trade_date is today's close; the bar before that is the
    # authoritative previous close ("p"). Without "p" the apply path falls back
    # to the snapshot's stale last_price which produces wildly inflated %change.
    start_str = (trade_date - timedelta(days=7)).strftime("%Y-%m-%d")
    end_str = (trade_date + timedelta(days=1)).strftime("%Y-%m-%d")

    def _extract_latest_two(sub) -> tuple[dict, float] | None:
        """Return (latest_row_dict, prev_close) or None if not enough data."""
        if sub is None or sub.empty:
            return None
        sub = sub.dropna(subset=["Close"])
        if sub.empty:
            return None
        latest = sub.iloc[-1]
        c = float(latest.get("Close") or 0)
        if c <= 0:
            return None
        prev_close = 0.0
        if len(sub) >= 2:
            try:
                prev_close = float(sub.iloc[-2].get("Close") or 0)
            except (TypeError, ValueError):
                prev_close = 0.0
        return (
            {
                "o": float(latest.get("Open") or 0),
                "h": float(latest.get("High") or 0),
                "l": float(latest.get("Low") or 0),
                "c": c,
                "v": int(float(latest.get("Volume") or 0)),
            },
            prev_close,
        )

    result: dict[str, dict] = {}
    CHUNK = 200  # yfinance handles ~200 tickers per batch well

    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        try:
            df = yf.download(
                chunk,
                start=start_str,
                end=end_str,
                auto_adjust=False,
                progress=False,
                threads=True,
            )
            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                # Multi-ticker result: columns are (field, ticker)
                for ticker in chunk:
                    sym = ticker.replace(".NS", "")
                    try:
                        sub = df.xs(ticker, axis=1, level=1)
                        extracted = _extract_latest_two(sub)
                        if extracted is None:
                            continue
                        rec, prev_close = extracted
                        rec["p"] = prev_close
                        result[sym] = rec
                    except Exception:
                        continue
            else:
                # Single-ticker result: columns are simple field names
                sym = chunk[0].replace(".NS", "")
                extracted = _extract_latest_two(df)
                if extracted is None:
                    continue
                rec, prev_close = extracted
                rec["p"] = prev_close
                result[sym] = rec
        except Exception as exc:
            logger.warning("yfinance chunk %s failed: %s", i // CHUNK + 1, exc)
            continue

    if not result:
        return None

    logger.info("yfinance fetched %s symbols for %s", len(result), trade_date.isoformat())
    return result


def _fetch_yfinance_universe_bars(trade_date: date, extra_tickers: list[str] | None = None) -> dict[str, dict] | None:
    """Fetch OHLCV from yfinance for every `.NS` ticker in the universe at trade_date.

    Returns ``{SYMBOL: {"o","h","l","c","v","p"}}`` or None on failure.

    Used to enrich the BSE bhavcopy with NSE-side volumes (BSE bhavcopy only
    captures the BSE-segment volume, which is a small fraction of total Indian
    trading for NSE-primary names like MTARTECH / MEESHO / HFCL — making RVOL
    look artificially low and breaking the Expansion scanner). Also supplies
    OHLCV for NSE-only names that BSE doesn't list at all (BSE Ltd., CDSL,
    MARINE).
    """
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        logger.warning("yfinance/pandas not installed; skipping volume enrichment")
        return None

    universe_path = DATA_DIR / "free_universe.json"
    if not universe_path.exists():
        return None

    try:
        universe = json.loads(universe_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(universe, list):
        return None

    tickers = [s.get("ticker", "") for s in universe if str(s.get("ticker", "")).endswith(".NS")]
    if extra_tickers:
        already = {t for t in tickers}
        for extra in extra_tickers:
            if extra and extra not in already:
                tickers.append(extra)
                already.add(extra)
    if not tickers:
        return None

    start_str = (trade_date - timedelta(days=7)).strftime("%Y-%m-%d")
    end_str = (trade_date + timedelta(days=1)).strftime("%Y-%m-%d")
    target_str = trade_date.isoformat()

    def _row_for_target(sub) -> tuple[dict, float] | None:
        if sub is None or sub.empty:
            return None
        sub = sub.dropna(subset=["Close"])
        if sub.empty:
            return None
        idx = pd.to_datetime(sub.index).strftime("%Y-%m-%d")
        mask = idx == target_str
        if not mask.any():
            return None
        target_row = sub[mask].iloc[-1]
        c = float(target_row.get("Close") or 0)
        if c <= 0:
            return None
        prev_close = 0.0
        prior = sub[idx < target_str]
        if not prior.empty:
            try:
                prev_close = float(prior.iloc[-1].get("Close") or 0)
            except (TypeError, ValueError):
                prev_close = 0.0
        return (
            {
                "o": float(target_row.get("Open") or 0),
                "h": float(target_row.get("High") or 0),
                "l": float(target_row.get("Low") or 0),
                "c": c,
                "v": int(float(target_row.get("Volume") or 0)),
            },
            prev_close,
        )

    result: dict[str, dict] = {}
    CHUNK = 200
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
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
            logger.warning("yfinance enrich chunk %s failed: %s", i // CHUNK + 1, exc)
            continue
        if df is None or df.empty:
            continue
        if isinstance(df.columns, pd.MultiIndex):
            for ticker in chunk:
                sym = ticker.replace(".NS", "")
                try:
                    sub = df.xs(ticker, axis=1, level=1)
                except Exception:
                    continue
                extracted = _row_for_target(sub)
                if extracted is None:
                    continue
                rec, prev_close = extracted
                rec["p"] = prev_close
                result[sym] = rec
        else:
            sym = chunk[0].replace(".NS", "")
            extracted = _row_for_target(df)
            if extracted is None:
                continue
            rec, prev_close = extracted
            rec["p"] = prev_close
            result[sym] = rec

    if not result:
        return None
    logger.info("yfinance enrichment: %s symbols for %s", len(result), trade_date.isoformat())
    return result


def _is_record_sane(rec: dict, *, sym: str = "") -> bool:
    """Reject obviously broken OHLC records before they pollute the patch.

    Indian equity daily price bands are at most ±20% (most are 5/10/20%). An
    EOD bar showing |close − prev| > 22% is almost always a stale BSE last-tick
    on an illiquid line. We also reject internally inconsistent OHLC (high<low,
    close outside [low,high] beyond a 0.5% rounding margin) and any record
    whose close is non-positive.
    """
    try:
        o = float(rec.get("o") or 0)
        h = float(rec.get("h") or 0)
        l = float(rec.get("l") or 0)
        c = float(rec.get("c") or 0)
        p = float(rec.get("p") or 0)
        v = float(rec.get("v") or 0)
    except (TypeError, ValueError):
        return False

    if c <= 0:
        return False

    if h > 0 and l > 0:
        if h < l:
            return False
        margin = max(0.005 * c, 0.01)
        if c > h + margin or c < l - margin:
            return False
        if o > 0 and (o > h + margin or o < l - margin):
            return False

    if p > 0:
        chg_pct = abs(c - p) / p * 100.0
        if chg_pct > 22.0 and v < 5000:
            # Beyond the 20% circuit limit on negligible volume → almost
            # certainly a stale BSE last-tick or auction print.
            logger.warning(
                "drop %s: |close-prev|=%.2f%% with vol=%s (likely stale tick)",
                sym, chg_pct, int(v),
            )
            return False
        # Even with non-trivial volume, rule out impossible swings (>40% in
        # a single session is not a real Indian equity move).
        if chg_pct > 40.0:
            logger.warning(
                "drop %s: |close-prev|=%.2f%% (exceeds plausible daily range)",
                sym, chg_pct,
            )
            return False

    if h > 0 and l > 0 and c > 0:
        rng_pct = (h - l) / c * 100.0
        if rng_pct > 30.0 and v < 5000:
            logger.warning(
                "drop %s: range=%.2f%% with vol=%s (likely BSE illiquid print)",
                sym, rng_pct, int(v),
            )
            return False

    return True


def _merge_bse_with_yfinance(bse_symbols: dict[str, dict], trade_date: date, extra_tickers: list[str] | None = None) -> dict[str, dict]:
    """Combine BSE bhavcopy with yfinance NSE bars; the universe is NSE-keyed.

    Priority:
      1. yfinance .NS bar (authoritative NSE OHLC + prev close + NSE volume).
      2. BSE row when yfinance has no entry (covers BSE-only listings or YF
         transient gaps; we still validate the row before keeping it).

    BSE OHLC is **not** trusted for NSE-primary names: BSE-segment liquidity is
    usually a small fraction of NSE volume, so BSE last-tick can stay frozen at
    a stale price band hit and produce wildly wrong candles / change_pct on the
    UI even when the same stock traded normally on NSE. This is the bug class
    that previously caused "today's EOD shows strange candlesticks".

    Every record (from either source) is run through ``_is_record_sane`` before
    inclusion so a single bad row can't poison the whole patch.
    """
    yf_symbols = _fetch_yfinance_universe_bars(trade_date, extra_tickers=extra_tickers) or {}
    if not yf_symbols:
        logger.warning("yfinance unavailable; falling back to BSE-only OHLC (illiquid names may show wrong candles)")

    merged: dict[str, dict] = {}
    yf_used = 0
    bse_used = 0
    yf_only = 0
    bse_only = 0
    rejected = 0
    cross_corrected = 0

    all_keys = set(bse_symbols) | set(yf_symbols)
    for sym in all_keys:
        bse_rec = bse_symbols.get(sym)
        yf_rec = yf_symbols.get(sym)

        chosen = None
        if yf_rec and _is_record_sane(yf_rec, sym=sym):
            chosen = dict(yf_rec)
            if bse_rec is None:
                yf_only += 1
            else:
                yf_used += 1
                # Cross-validate close: if yfinance and BSE disagree by more
                # than 5%, log it (this is rare but worth observing).
                try:
                    yc = float(yf_rec.get("c") or 0)
                    bc = float(bse_rec.get("c") or 0)
                    if yc > 0 and bc > 0 and abs(yc - bc) / yc > 0.05:
                        cross_corrected += 1
                        logger.info(
                            "%s: BSE close=%.2f vs YF close=%.2f (>5%%); using YF",
                            sym, bc, yc,
                        )
                except (TypeError, ValueError):
                    pass
        elif bse_rec and _is_record_sane(bse_rec, sym=sym):
            chosen = dict(bse_rec)
            if yf_rec is None:
                bse_only += 1
            else:
                bse_used += 1
        else:
            rejected += 1
            continue

        merged[sym] = chosen

    logger.info(
        "merge for %s: total=%s | yfinance(both)=%s yfinance(only)=%s bse(both)=%s bse(only)=%s rejected=%s cross_diverged=%s",
        trade_date.isoformat(),
        len(merged),
        yf_used,
        yf_only,
        bse_used,
        bse_only,
        rejected,
        cross_corrected,
    )
    return merged


# Backwards-compatible alias (old name used elsewhere).
_enrich_bse_with_yfinance = _merge_bse_with_yfinance


def _patch_already_current(target_date: date) -> bool:
    """Return True if bhavcopy_patch.json already has data for target_date."""
    if not OUTPUT_PATH.exists():
        return False
    try:
        existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        return existing.get("date") == target_date.isoformat()
    except Exception:
        return False


def _write_patch(
    target_date: date,
    symbols: dict,
    source: str,
    *,
    new_listings: dict[str, dict] | None = None,
) -> None:
    patch: dict[str, object] = {
        "date": target_date.isoformat(),
        "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "source": source.upper(),
        "symbols": symbols,
    }
    # Carry day-1 IPO metadata so the apply path can MATERIALIZE snapshot rows
    # for symbols that aren't yet in free_universe.json. We only emit the
    # subset of listings whose ticker also has price data in this patch — the
    # apply path needs both metadata and OHLCV to seed a useful row.
    if new_listings:
        filtered = {sym: meta for sym, meta in new_listings.items() if sym in symbols}
        if filtered:
            patch["new_listings"] = filtered
    OUTPUT_PATH.write_text(json.dumps(patch, separators=(",", ":")), encoding="utf-8")
    logger.info(
        "Written %s date=%s source=%s symbols=%s new_listings=%s",
        OUTPUT_PATH,
        target_date.isoformat(),
        source.upper(),
        len(symbols),
        len(patch.get("new_listings") or {}),
    )


def _update_xp_breadth(trade_date: date, full_bhav: dict[str, dict], source: str) -> None:
    """Compute the day's XP market-breadth score from the FULL bhavcopy and
    persist it. Must be passed the all-market dict (BSE/NSE bhavcopy), NOT the
    universe-only merged patch. Failures are logged and swallowed so a breadth
    hiccup never breaks the price patch.
    """
    try:
        from app.services.xp_breadth import (
            CONST,
            compute_xp_series,
            daily_breadth_metrics,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("XP breadth engine unavailable (%s); skipping breadth update", exc)
        return

    if not full_bhav:
        return

    date_iso = trade_date.isoformat()
    metric_keys = ("date", "total", "advancers_4p5", "decliners", "ma10_pct", "ma20_pct")

    # Load existing artifacts.
    rolling: dict[str, list] = {}
    if XP_ROLLING_PATH.exists():
        try:
            loaded = json.loads(XP_ROLLING_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                rolling = loaded
        except Exception:
            rolling = {}

    hist_doc: dict = {}
    if XP_HISTORY_PATH.exists():
        try:
            loaded = json.loads(XP_HISTORY_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                hist_doc = loaded
        except Exception:
            hist_doc = {}

    prior_days = hist_doc.get("days")
    if not isinstance(prior_days, list):
        prior_days = []
    const = float(hist_doc.get("const", CONST))

    # Base metric history (inputs only), de-duped on date so reruns are idempotent.
    base = [
        {k: d.get(k) for k in metric_keys}
        for d in prior_days
        if isinstance(d, dict) and d.get("date") != date_iso
    ]

    if hist_doc.get("rolling_date") == date_iso:
        # Today's close was already folded into the rolling store on a prior run;
        # reuse the stored metrics and only recompute the score series.
        today = next((d for d in prior_days if isinstance(d, dict) and d.get("date") == date_iso), None)
        if today is None:
            metrics, rolling = daily_breadth_metrics(date_iso, full_bhav, rolling)
        else:
            metrics = {k: today.get(k) for k in metric_keys}
    else:
        metrics, rolling = daily_breadth_metrics(date_iso, full_bhav, rolling)

    base.append(metrics)
    series = compute_xp_series(base, const=const)

    latest = series[-1] if series else None
    out_doc = {
        "generated_at": datetime.now(IST).isoformat(),
        "rolling_date": date_iso,
        "source": source.upper(),
        "const": const,
        "ma_short": 10,
        "ma_long": 20,
        "universe": "all_bhavcopy_equities",
        "latest": latest,
        "days": series,
    }
    XP_HISTORY_PATH.write_text(json.dumps(out_doc, separators=(",", ":")), encoding="utf-8")
    XP_ROLLING_PATH.write_text(json.dumps(rolling, separators=(",", ":")), encoding="utf-8")
    if latest:
        logger.info(
            "XP breadth %s: score=%s regime=%s (advancers=%s decliners=%s ma10%%=%s ma20%%=%s total=%s)",
            date_iso,
            latest.get("xp_score"),
            latest.get("regime"),
            metrics.get("advancers_4p5"),
            metrics.get("decliners"),
            metrics.get("ma10_pct"),
            metrics.get("ma20_pct"),
            metrics.get("total"),
        )


def main() -> int:
    # Belt-and-suspenders: even though every cron in daily-bhavcopy.yml is
    # scheduled after market close (4:23-6:23 PM IST), refuse to run during
    # the active session itself. A morning/intraday run would otherwise stamp
    # a partial mid-day snapshot as if it were the EOD bhavcopy, which the
    # rest of the system trusts as "the closing print".
    now_ist = datetime.now(IST)
    weekday = now_ist.weekday()  # 0=Mon … 4=Fri
    if weekday < 5:
        # NSE/BSE regular session: 09:15-15:30 IST. Add a 30-min grace before
        # market open and 30-min grace after close (BSE bhavcopy publishes
        # ~16:00 IST). Refuse anything between 08:45 and 16:00 IST.
        block_start = now_ist.replace(hour=8, minute=45, second=0, microsecond=0)
        block_end = now_ist.replace(hour=16, minute=0, second=0, microsecond=0)
        if block_start <= now_ist < block_end:
            logger.warning(
                "Refusing to run inside market hours (now=%s IST). EOD bhavcopy "
                "must only be fetched after 4:00 PM IST. Exiting cleanly.",
                now_ist.strftime("%Y-%m-%d %H:%M"),
            )
            return 0

    target = _last_trading_day()
    logger.info("Fetching EOD Bhavcopy for %s", target.isoformat())

    # Discover recently listed NSE equities BEFORE any data fetch so each
    # phase below can include the new IPOs in its ticker list and the patch
    # can carry their metadata for the apply path's seed-row builder. This
    # is what makes a day-1 IPO visible in the IPO scanner.
    new_listings = _fetch_recent_nse_listings(target)
    # ``.NS`` form for yfinance; only those not already in universe will be
    # appended inside _fetch_yfinance_universe_bars / _fetch_from_yfinance.
    extra_yf_tickers = [f"{sym}.NS" for sym in new_listings.keys()]

    # --- Phase 1: BSE bhavcopy (no geo-blocking, available ~4:24 PM IST) merged with yfinance NSE bars. ---
    # The universe is NSE-keyed (.NS), so yfinance .NS data IS the authoritative
    # NSE feed and is preferred over BSE-segment OHLC for any symbol it covers.
    # BSE remains the fallback for BSE-only listings or YF transient gaps; both
    # sources are validated through _is_record_sane before inclusion.
    bse_symbols = _fetch_from_bse(target)
    if bse_symbols:
        if _patch_already_current(target):
            logger.info("Patch already current for %s (%s symbols). No update needed.", target.isoformat(), len(bse_symbols))
            return 0
        merged = _merge_bse_with_yfinance(bse_symbols, target, extra_tickers=extra_yf_tickers)
        if not merged:
            logger.warning("Merged patch for %s is empty after sanity filter; aborting.", target)
            return 1
        _write_patch(target, merged, "YF+BSE", new_listings=new_listings)
        _update_xp_breadth(target, bse_symbols, "BSE")
        return 0

    logger.info("BSE unavailable for %s. Trying NSE archives.", target)

    # --- Phase 2: NSE archives (authoritative, but geo-blocked from non-Indian IPs) ---
    csv_text = _fetch_bhavcopy_csv(target)
    if csv_text:
        symbols = _parse_bhavcopy_csv(csv_text)
        if symbols:
            symbols = {s: r for s, r in symbols.items() if _is_record_sane(r, sym=s)}
            if symbols:
                if _patch_already_current(target):
                    logger.info("Patch already current for %s (%s symbols). No update needed.", target.isoformat(), len(symbols))
                    return 0
                _write_patch(target, symbols, "NSE", new_listings=new_listings)
                _update_xp_breadth(target, symbols, "NSE")
                return 0

    logger.warning("NSE unavailable for %s. Trying yfinance fallback.", target)

    # --- Phase 3: yfinance fallback for today (globally accessible via Yahoo Finance) ---
    yf_symbols = _fetch_from_yfinance(target, extra_tickers=extra_yf_tickers)
    if yf_symbols:
        yf_symbols = {s: r for s, r in yf_symbols.items() if _is_record_sane(r, sym=s)}
        if yf_symbols:
            if _patch_already_current(target):
                logger.info("Patch already current for %s. No update needed.", target.isoformat())
                return 0
            _write_patch(target, yf_symbols, "YFINANCE", new_listings=new_listings)
            return 0

    logger.warning("yfinance unavailable for %s. Trying previous trading day.", target)

    # --- Phase 4: last resort — previous trading day via BSE then NSE (holiday fallback) ---
    prev = target - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    bse_prev = _fetch_from_bse(prev)
    if bse_prev:
        merged_prev = _merge_bse_with_yfinance(bse_prev, prev)
        if merged_prev:
            if _patch_already_current(prev):
                logger.info("Patch already current for %s (%s symbols). No update needed.", prev.isoformat(), len(merged_prev))
                return 0
            _write_patch(prev, merged_prev, "YF+BSE")
            _update_xp_breadth(prev, bse_prev, "BSE")
            return 0
    csv_text = _fetch_bhavcopy_csv(prev)
    if csv_text:
        symbols = _parse_bhavcopy_csv(csv_text)
        if symbols:
            symbols = {s: r for s, r in symbols.items() if _is_record_sane(r, sym=s)}
            if symbols:
                if _patch_already_current(prev):
                    logger.info("Patch already current for %s (%s symbols). No update needed.", prev.isoformat(), len(symbols))
                    return 0
                _write_patch(prev, symbols, "NSE")
                _update_xp_breadth(prev, symbols, "NSE")
                return 0

    logger.error("Could not fetch Bhavcopy from NSE, BSE, or yfinance for any date.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
