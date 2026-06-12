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

# NSE daily price-band-changes report (the data behind
# nseindia.com/reports/price-band-changes). Published on nsearchives — the same
# host as EQUITY_L.csv, reachable from GitHub runners most days. Each file
# lists the band revisions for that date: Symbol, Series, Security Name, From, To.
BAND_CHANGES_URL = "https://nsearchives.nseindia.com/content/equities/eq_band_changes_{ddmmyyyy}.csv"
PRICE_BAND_CHANGES_PATH = DATA_DIR / "price_band_changes.json"
# Re-scan this many calendar days on every run. Files can publish late in the
# evening (after the last workflow retry), so a rolling window self-heals any
# date we missed; already-fetched dates are skipped via the store's index.
BAND_CHANGES_RESCAN_DAYS = 7

# Complete current price-band list for ALL banded securities ("Securities under
# price bands" on nseindia.com). Same nsearchives host. Symbols absent from
# this list have no fixed band (F&O / dynamic names).
PRICE_BANDS_URL = "https://nsearchives.nseindia.com/content/equities/sec_list.csv"
PRICE_BANDS_PATH = DATA_DIR / "price_bands.json"
# Sanity floor: the real list has thousands of rows; refuse to overwrite the
# store with a truncated/garbled download.
PRICE_BANDS_MIN_ROWS = 500

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


# ── Per-symbol indicator blocks ──────────────────────────────────────────────
# The HF Space cold-starts from baked seed snapshots that can be WEEKS or
# MONTHS old. The apply path rolls one bar per applied day, so the recent-bar
# arrays / EMAs / SMAs / RS baselines on the snapshot rows stay frozen at the
# seed build date (verified: ATGL showed 39% from the 10EMA vs ~3% real).
# To heal that, the daily patch carries a compact per-symbol indicator block
# ("i") recomputed from a fresh ~2-year yfinance history; the apply path
# overwrites the stale fields with it. Computations mirror
# FreeDataProvider._history_to_snapshot (backend/app/providers/free.py).
INDICATOR_HISTORY_DAYS = 800  # calendar days (~545 trading bars; covers b504)
INDICATOR_MAX_BAR_LAG_DAYS = 7  # skip blocks whose latest bar is older than this
RECENT_BARS = 20

# Mirrors RS_LOOKBACKS / RETURN_*_BARS in free.py.
_RS_LOOKBACKS = ((63, 0.4), (126, 0.2), (189, 0.2), (252, 0.2))
_MIN_RS_HISTORY_BARS = 63
_BASELINE_LOOKBACKS = {
    "b5": 5, "b20": 21, "b40": 40, "b60": 63, "b63": 63,
    "b126": 126, "b189": 189, "b252": 252, "b504": 504,
}


def _effective_lookback(n_bars: int, lookback: int, offset: int = 0, *, allow_partial: bool = True) -> int | None:
    available = n_bars - offset - 1
    if available < 1:
        return None
    if available >= lookback:
        return lookback
    return available if allow_partial else None


def _baseline_at(closes, lookback: int, offset: int = 0, *, allow_partial: bool = True) -> float | None:
    eff = _effective_lookback(len(closes), lookback, offset, allow_partial=allow_partial)
    if eff is None:
        return None
    value = float(closes[-offset - eff - 1])
    return value if value > 0 else None


def _adaptive_rs_lookbacks(n_bars: int, offset: int) -> list[tuple[int, float]]:
    available = _effective_lookback(n_bars, 252, offset, allow_partial=True)
    if available is None or available < _MIN_RS_HISTORY_BARS:
        return []
    if available >= 252:
        return list(_RS_LOOKBACKS)
    lookbacks: list[tuple[int, float]] = []
    previous = 0
    for fraction, weight in zip((0.25, 0.5, 0.75, 1.0), (0.4, 0.2, 0.2, 0.2)):
        candidate = max(1, int(round(available * fraction)))
        candidate = max(candidate, previous + 1)
        candidate = min(candidate, available)
        lookbacks.append((candidate, weight))
        previous = candidate
    return lookbacks


def _weighted_rs_score_at(closes, offset: int) -> float | None:
    lookbacks = _adaptive_rs_lookbacks(len(closes), offset)
    if not lookbacks:
        return None
    score = 0.0
    for lookback, weight in lookbacks:
        eff = _effective_lookback(len(closes), lookback, offset, allow_partial=False)
        if eff is None:
            return None
        baseline = float(closes[-offset - eff - 1])
        current = float(closes[-offset - 1])
        if baseline <= 0:
            return None
        score += (((current / baseline) - 1) * 100.0) * weight
    return score


def _return_pct_as_of(closes, lookback: int, offset: int) -> float | None:
    eff = _effective_lookback(len(closes), lookback, offset, allow_partial=True)
    if eff is None:
        return None
    baseline = float(closes[-offset - eff - 1])
    if baseline <= 0:
        return None
    return ((float(closes[-offset - 1]) / baseline) - 1) * 100.0


def _window_extreme(values, window: int, *, exclude_last: bool, use_max: bool) -> float | None:
    subset = values[-window:]
    if exclude_last and len(subset) > 1:
        subset = subset[:-1]
    if len(subset) == 0:
        return None
    return float(max(subset) if use_max else min(subset))


def _indicator_block_from_history(sub, trade_date: date) -> dict | None:
    """Build the compact indicator block for one symbol from its (split- and
    dividend-adjusted) daily history DataFrame. Returns None when the data is
    too thin or too stale to be trusted."""
    import pandas as pd

    if sub is None or sub.empty:
        return None
    sub = sub.dropna(subset=["Close"])
    sub = sub[sub["Close"] > 0]
    if len(sub) < 30:
        return None
    # Never let a stray next-session bar contaminate the EOD block.
    idx_dates = pd.to_datetime(sub.index).date
    sub = sub[idx_dates <= trade_date]
    if len(sub) < 30:
        return None
    last_bar_date = pd.to_datetime(sub.index[-1]).date()
    if (trade_date - last_bar_date).days > INDICATOR_MAX_BAR_LAG_DAYS:
        return None

    close = sub["Close"].astype(float)
    high = sub["High"].fillna(close).astype(float)
    low = sub["Low"].fillna(close).astype(float)
    volume = sub["Volume"].fillna(0).astype(float)
    closes = close.tolist()
    highs = high.tolist()
    lows = low.tolist()
    n = len(closes)

    def ema(span: int) -> float | None:
        if n < span:
            return None
        value = close.ewm(span=span, adjust=False).mean().iloc[-1]
        return None if pd.isna(value) else round(float(value), 2)

    def sma(window: int) -> float | None:
        if n < window:
            return None
        value = close.rolling(window=window, min_periods=window).mean().iloc[-1]
        return None if pd.isna(value) else round(float(value), 2)

    block: dict[str, object] = {
        "d": last_bar_date.isoformat(),
        "rc": [round(v, 2) for v in closes[-RECENT_BARS:]],
        "rh": [round(v, 2) for v in highs[-RECENT_BARS:]],
        "rl": [round(v, 2) for v in lows[-RECENT_BARS:]],
        "rv": [max(0, int(v)) for v in volume.tolist()[-RECENT_BARS:]],
    }

    for key, span in (("e10", 10), ("e20", 20), ("e50", 50), ("e200", 200)):
        value = ema(span)
        if value is not None:
            block[key] = value
    for key, window in (("s20", 20), ("s50", 50), ("s150", 150), ("s200", 200)):
        value = sma(window)
        if value is not None:
            block[key] = value
    sma200_series = close.rolling(window=200, min_periods=200).mean().dropna()
    if len(sma200_series) > 21:
        block["s200_1m"] = round(float(sma200_series.iloc[-22]), 2)
    if len(sma200_series) > 105:
        block["s200_5m"] = round(float(sma200_series.iloc[-106]), 2)
    if isinstance(close.index, pd.DatetimeIndex):
        weekly_close = close.resample("W-FRI").last().dropna()
        if len(weekly_close) >= 20:
            value = weekly_close.ewm(span=20, adjust=False).mean().iloc[-1]
            if not pd.isna(value):
                block["we20"] = round(float(value), 2)

    for key, window in (("av20", 20), ("av30", 30), ("av50", 50)):
        block[key] = int(volume.tail(window).mean() or 0)

    previous_close_series = close.shift(1).fillna(close.iloc[0])
    true_range = pd.concat(
        [
            (high - low).abs(),
            (high - previous_close_series).abs(),
            (low - previous_close_series).abs(),
        ],
        axis=1,
    ).max(axis=1)
    block["atr"] = round(float(true_range.rolling(window=14, min_periods=1).mean().iloc[-1]), 2)

    delta = close.diff()
    gain = delta.where(delta > 0, 0.0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = -delta.where(delta < 0, 0.0).ewm(alpha=1 / 14, adjust=False).mean()
    rsi_series = 100 - (100 / (1 + (gain / loss)))
    if not rsi_series.empty and not pd.isna(rsi_series.iloc[-1]):
        block["rsi"] = round(float(rsi_series.iloc[-1]), 2)

    adr_ranges = (high.tail(20) - low.tail(20)).dropna()
    adr_closes = close.tail(20)
    if not adr_ranges.empty and not adr_closes.empty and float(adr_closes.mean()) > 0:
        block["adr"] = round((float(adr_ranges.mean()) / float(adr_closes.mean())) * 100.0, 2)

    for key, lookback in _BASELINE_LOOKBACKS.items():
        value = _baseline_at(closes, lookback)
        if value is not None:
            block[key] = round(value, 4)

    rs_lookbacks = _adaptive_rs_lookbacks(n, 0)
    for key, position in (("q1", 0), ("q2", 1), ("q3", 2), ("q4", 3)):
        if len(rs_lookbacks) > position:
            value = _baseline_at(closes, rs_lookbacks[position][0], allow_partial=False)
            if value is not None:
                block[key] = round(value, 4)
    for key, offset in (("rsw", 0), ("rsw1d", 1), ("rsw1w", 5), ("rsw1m", 21)):
        value = _weighted_rs_score_at(closes, offset)
        if value is not None:
            block[key] = round(value, 4)
    for key, offset in (("r12m_1d", 1), ("r12m_1w", 5), ("r12m_1m", 21)):
        value = _return_pct_as_of(closes, 252, offset)
        if value is not None:
            block[key] = round(value, 2)

    # Rolling-window extremes (mirrors the seed builder's windows; ATH / ATL /
    # multi-year levels need full listing history and are NOT shipped — the
    # apply path keeps its lift-only logic for those).
    for key, values, window, exclude, use_max in (
        ("wh", highs, 5, False, True),
        ("whp", highs, 6, True, True),
        ("wl", lows, 5, False, False),
        ("wlp", lows, 6, True, False),
        ("mh", highs, 21, False, True),
        ("mhp", highs, 22, True, True),
        ("ml", lows, 21, False, False),
        ("mlp", lows, 22, True, False),
        ("h3m", highs, 63, False, True),
        ("h6m", highs, 126, False, True),
        ("h6mp", highs, 127, True, True),
        ("l6m", lows, 126, False, False),
        ("l6mp", lows, 127, True, False),
        ("h52", highs, 252, False, True),
        ("h52p", highs, 253, True, True),
        ("l52", lows, 252, False, False),
        ("l52p", lows, 253, True, False),
        ("rh20", highs, 20, False, True),
        ("rh20p", highs, 21, True, True),
        ("ph", highs, 10, True, True),
        ("dh", highs, 15, True, True),
        ("dl", lows, 15, True, False),
    ):
        value = _window_extreme(values, window, exclude_last=exclude, use_max=use_max)
        if value is not None:
            block[key] = round(value, 2)

    return block


def _attach_indicator_blocks(symbols: dict[str, dict], trade_date: date, extra_tickers: list[str] | None = None) -> int:
    """Fetch ~2y of adjusted daily bars for the universe and attach an "i"
    indicator block to each patch record. Mutates ``symbols`` in place and
    returns the number of blocks attached. Any failure is logged and swallowed
    — the price patch must never be blocked by indicator enrichment."""
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        logger.warning("yfinance/pandas not installed; skipping indicator blocks")
        return 0

    universe_path = DATA_DIR / "free_universe.json"
    if not universe_path.exists():
        return 0
    try:
        universe = json.loads(universe_path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    if not isinstance(universe, list):
        return 0

    tickers = [s.get("ticker", "") for s in universe if str(s.get("ticker", "")).endswith(".NS")]
    if extra_tickers:
        already = set(tickers)
        for extra in extra_tickers:
            if extra and extra not in already:
                tickers.append(extra)
                already.add(extra)
    # Only fetch tickers whose symbol actually has a price record in this patch.
    tickers = [t for t in tickers if t.replace(".NS", "") in symbols]
    if not tickers:
        return 0

    start_str = (trade_date - timedelta(days=INDICATOR_HISTORY_DAYS)).strftime("%Y-%m-%d")
    end_str = (trade_date + timedelta(days=1)).strftime("%Y-%m-%d")

    attached = 0
    CHUNK = 200
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i : i + CHUNK]
        try:
            # auto_adjust=True so split (and dividend) adjusted closes feed the
            # EMAs — raw closes would carry split discontinuities straight into
            # every moving average.
            df = yf.download(
                chunk,
                start=start_str,
                end=end_str,
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            logger.warning("indicator chunk %s download failed: %s", i // CHUNK + 1, exc)
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
                try:
                    block = _indicator_block_from_history(sub, trade_date)
                except Exception:
                    continue
                if block and sym in symbols:
                    symbols[sym]["i"] = block
                    attached += 1
        else:
            sym = chunk[0].replace(".NS", "")
            try:
                block = _indicator_block_from_history(df, trade_date)
            except Exception:
                block = None
            if block and sym in symbols:
                symbols[sym]["i"] = block
                attached += 1

    logger.info("indicator blocks attached: %s/%s universe symbols for %s", attached, len(tickers), trade_date.isoformat())
    return attached


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


# A bhavcopy that covers far fewer NSE symbols than usual (e.g. a truncated
# BSE download) produces wildly skewed breadth inputs — 2026-06-09 landed with
# total=1543 vs the usual ~2010 and inflated ma10% by ~15 points. Days below
# this fraction of the median universe size are rejected on write and filtered
# out of the stored history on every recompute.
XP_PARTIAL_DAY_MIN_RATIO = 0.8
XP_ANCHORS_PATH = DATA_DIR / "xp_calibration_anchors.json"


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


# Trailing window for the partial-day floor. A GLOBAL median misses partial
# days when the universe itself grows over the year (2025-06 had ~1786 NSE
# names vs ~2015 by 2026-06 — the global median of 1892 sat BELOW the
# poisoned 2026-06-09 day's 1543 × 1/0.8). A short trailing window compares
# each day only to its recent neighbourhood.
XP_PARTIAL_DAY_TRAILING_WINDOW = 20
XP_PARTIAL_DAY_MIN_HISTORY = 5


def _xp_partial_day_floor(days: list[dict]) -> float | None:
    """Coverage floor derived from the trailing median of the most recent
    (already accepted) days. Returns None until enough history accumulates."""
    totals = [float(d.get("total") or 0) for d in days if isinstance(d, dict)]
    recent = [t for t in totals if t > 0][-XP_PARTIAL_DAY_TRAILING_WINDOW:]
    if len(recent) < XP_PARTIAL_DAY_MIN_HISTORY:
        return None
    med = _median(recent)
    if med is None:
        return None
    return XP_PARTIAL_DAY_MIN_RATIO * med


def _drop_partial_breadth_days(days: list[dict]) -> list[dict]:
    """Walk the history forward, judging each day against the trailing median
    of the days KEPT so far — so a dropped partial day never contaminates the
    floor used for the days after it."""
    kept: list[dict] = []
    for d in days:
        total = float(d.get("total") or 0)
        floor = _xp_partial_day_floor(kept)
        if floor is not None and 0 < total < floor:
            logger.warning(
                "XP breadth: dropping partial-coverage day %s (total=%s, floor=%.0f)",
                d.get("date"), int(total), floor,
            )
            continue
        kept.append(d)
    return kept


def _load_xp_anchors() -> tuple[dict[str, float], str | None]:
    """Load the author's published EM anchor values plus a stable fingerprint,
    so committing new anchors triggers a one-time recalibration on the next
    daily run."""
    if not XP_ANCHORS_PATH.exists():
        return {}, None
    try:
        doc = json.loads(XP_ANCHORS_PATH.read_text(encoding="utf-8"))
        loaded = doc.get("anchors") if isinstance(doc, dict) else None
        anchors = {str(k): float(v) for k, v in loaded.items()} if isinstance(loaded, dict) else {}
    except Exception as exc:
        logger.warning("Could not read XP calibration anchors: %s", exc)
        return {}, None
    if not anchors:
        return {}, None
    import hashlib

    fingerprint = hashlib.sha256(json.dumps(anchors, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return anchors, fingerprint


def _update_xp_breadth(trade_date: date, full_bhav: dict[str, dict], source: str) -> None:
    """Compute the day's XP market-breadth score from the FULL bhavcopy and
    persist it. Must be passed the all-market dict (BSE/NSE bhavcopy), NOT the
    universe-only merged patch. Failures are logged and swallowed so a breadth
    hiccup never breaks the price patch.
    """
    try:
        from app.services.xp_breadth import (
            CONST,
            OUTPUT_CLAMP,
            apply_output_calibration,
            calibrate_const,
            compute_xp_series,
            daily_breadth_metrics,
            fit_output_calibration,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("XP breadth engine unavailable (%s); skipping breadth update", exc)
        return

    if not full_bhav:
        return

    date_iso = trade_date.isoformat()
    metric_keys = ("date", "total", "advancers_4p5", "decliners", "ma10_pct", "ma20_pct")

    # Restrict breadth to NSE-listed equities (the author's base), filtering out
    # the ~2800 illiquid BSE-only micro-caps that otherwise add noise.
    nse_filter: set[str] | None = None
    nse_path = DATA_DIR / "nse_equity_symbols.json"
    if nse_path.exists():
        try:
            doc = json.loads(nse_path.read_text(encoding="utf-8"))
            syms = doc.get("symbols") if isinstance(doc, dict) else None
            if isinstance(syms, list) and syms:
                nse_filter = {str(s).strip().upper() for s in syms}
        except Exception:
            nse_filter = None
    if nse_filter is None:
        logger.warning("nse_equity_symbols.json missing/unreadable; XP breadth falls back to ALL bhavcopy equities")

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

    # Base metric history (inputs only), de-duped on date so reruns are
    # idempotent, with partial-coverage days filtered out so a truncated
    # bhavcopy can't keep poisoning the recursion.
    base = [
        {k: d.get(k) for k in metric_keys}
        for d in prior_days
        if isinstance(d, dict) and d.get("date") != date_iso
    ]
    base = _drop_partial_breadth_days(base)

    already_rolled = hist_doc.get("rolling_date") == date_iso
    if already_rolled:
        # Today's close was already folded into the rolling store on a prior run;
        # reuse the stored metrics and only recompute the score series.
        today = next((d for d in prior_days if isinstance(d, dict) and d.get("date") == date_iso), None)
        if today is None:
            metrics, rolling = daily_breadth_metrics(date_iso, full_bhav, rolling, symbol_filter=nse_filter)
        else:
            metrics = {k: today.get(k) for k in metric_keys}
    else:
        metrics, rolling = daily_breadth_metrics(date_iso, full_bhav, rolling, symbol_filter=nse_filter)

    # Partial-day guard for TODAY: a truncated bhavcopy must not enter the
    # series. If the rolling store hasn't folded today yet, skip the whole
    # update so a later (complete) run can do it cleanly.
    floor = _xp_partial_day_floor(base)
    today_total = float(metrics.get("total") or 0)
    if floor is not None and 0 < today_total < floor:
        if not already_rolled:
            logger.warning(
                "XP breadth %s: bhavcopy looks partial (total=%s, floor=%.0f); skipping breadth update",
                date_iso, int(today_total), floor,
            )
            return
        logger.warning(
            "XP breadth %s: stored metrics look partial (total=%s, floor=%.0f); excluding today from the series",
            date_iso, int(today_total), floor,
        )
    else:
        base.append(metrics)

    # Output calibration: reuse the stored fit, but when the committed anchors
    # file changes (new published EM values added), refit const + the affine
    # output map against the full stored metric history — pure CPU, no network.
    out_scale = float(hist_doc.get("out_scale", 1.0))
    out_offset = float(hist_doc.get("out_offset", 0.0))
    stored_fingerprint = hist_doc.get("anchors_fingerprint")
    anchors, anchors_fingerprint = _load_xp_anchors()
    if anchors and anchors_fingerprint and stored_fingerprint != anchors_fingerprint:
        base_dates = {str(d.get("date")) for d in base}
        overlap = [a for a in anchors if a in base_dates]
        if len(overlap) >= 2:
            const = calibrate_const(base, anchors)
            recalibrated = compute_xp_series(base, const=const)
            out_scale, out_offset = fit_output_calibration(recalibrated, anchors)
            stored_fingerprint = anchors_fingerprint
            logger.info(
                "XP breadth: anchors changed — recalibrated const=%s out_scale=%s out_offset=%s against %s anchor(s)",
                const, out_scale, out_offset, len(overlap),
            )
        else:
            logger.warning("XP breadth: anchors changed but only %s overlap stored history; keeping previous calibration", len(overlap))

    series = compute_xp_series(base, const=const)
    clamp_doc = hist_doc.get("out_clamp")
    out_clamp = tuple(clamp_doc) if isinstance(clamp_doc, list) and len(clamp_doc) == 2 else OUTPUT_CLAMP
    series = apply_output_calibration(series, out_scale, out_offset, clamp=out_clamp)

    latest = series[-1] if series else None
    out_doc = {
        "generated_at": datetime.now(IST).isoformat(),
        "rolling_date": date_iso,
        "source": source.upper(),
        "const": const,
        "out_scale": out_scale,
        "out_offset": out_offset,
        "out_clamp": list(out_clamp),
        "anchors_fingerprint": stored_fingerprint,
        "ma_short": 10,
        "ma_long": 20,
        "universe": "nse_equities",
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


def _parse_band_value(raw: str):
    """Parse a band % cell ('10', '5', '20', 'No Band') → float or raw string."""
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return text  # e.g. "No Band" — keep verbatim so the UI can show it


def update_price_bands(target: date) -> None:
    """Fetch NSE's complete current price-band list (sec_list.csv) and write
    ``data/price_bands.json``.

    Store shape:
        {"updated_at": iso, "as_of": "YYYY-MM-DD",
         "bands": {"SYMBOL": 5.0 | 10.0 | 20.0 | "No Band", ...}}

    This is the authoritative answer to "what is SYMBOL's band today" for every
    banded security; a symbol ABSENT from the list has no fixed band (F&O /
    dynamic). Failures are logged and swallowed so the price patch never
    breaks; the previous store is kept when the download looks truncated.
    """
    try:
        try:
            resp = requests.get(PRICE_BANDS_URL, headers=HEADERS, timeout=30)
        except Exception as exc:
            logger.info("price-bands fetch failed: %s", exc)
            return
        if resp.status_code != 200 or len(resp.content) < 1000:
            logger.info("price-bands fetch unusable (status=%s, %s bytes)", resp.status_code, len(resp.content))
            return

        bands: dict[str, object] = {}
        reader = csv.DictReader(io.StringIO(resp.text))
        field_names = [str(name or "").strip().lower() for name in (reader.fieldnames or [])]
        symbol_key = next((name for name in field_names if "symbol" in name), None)
        band_key = next((name for name in field_names if "band" in name), None)
        if not symbol_key or not band_key:
            logger.warning("price-bands CSV headers unrecognised: %s", field_names)
            return
        for row in reader:
            cells = {str(k).strip().lower(): str(v).strip() for k, v in row.items() if k}
            sym = (cells.get(symbol_key) or "").upper()
            if not sym:
                continue
            band = _parse_band_value(cells.get(band_key))
            if band is None:
                continue
            bands[sym] = band
        if len(bands) < PRICE_BANDS_MIN_ROWS:
            logger.warning("price-bands list too small (%s rows) — keeping previous store", len(bands))
            return

        store = {
            "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
            "as_of": target.isoformat(),
            "bands": bands,
        }
        PRICE_BANDS_PATH.write_text(json.dumps(store, separators=(",", ":")), encoding="utf-8")
        logger.info("price_bands.json updated: %s symbols as of %s", len(bands), target.isoformat())
    except Exception as exc:
        logger.warning("price-bands update failed: %s", exc)


def update_price_band_changes(target: date, *, days_back: int = BAND_CHANGES_RESCAN_DAYS) -> None:
    """Fetch NSE daily price-band-change CSVs and merge them into
    ``data/price_band_changes.json``.

    Store shape (compact):
        {"updated_at": iso, "fetched_dates": ["YYYY-MM-DD", ...],
         "changes": {"SYMBOL": [["YYYY-MM-DD", from, to], ...], ...}}

    Idempotent per date: a date in ``fetched_dates`` is never re-fetched, and
    per-symbol entries are deduped on date. Holidays / not-yet-published days
    404 and are simply retried inside the rolling window on later runs. Any
    failure is logged and swallowed so band changes never break the price patch.
    """
    try:
        store: dict = {}
        if PRICE_BAND_CHANGES_PATH.exists():
            try:
                loaded = json.loads(PRICE_BAND_CHANGES_PATH.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    store = loaded
            except Exception:
                store = {}
        fetched = set(store.get("fetched_dates") or [])
        changes: dict[str, list] = store.get("changes") if isinstance(store.get("changes"), dict) else {}

        new_rows = 0
        fetched_now: list[str] = []
        for back in range(days_back, -1, -1):
            day = target - timedelta(days=back)
            if day.weekday() >= 5:
                continue
            day_iso = day.isoformat()
            if day_iso in fetched:
                continue
            url = BAND_CHANGES_URL.format(ddmmyyyy=day.strftime("%d%m%Y"))
            try:
                resp = requests.get(url, headers=HEADERS, timeout=15)
            except Exception as exc:
                logger.info("band-changes fetch failed for %s: %s", day_iso, exc)
                continue
            if resp.status_code != 200 or len(resp.content) < 20:
                # Holiday or not yet published — retried on later runs while
                # the date stays inside the rolling window.
                continue
            try:
                reader = csv.DictReader(io.StringIO(resp.text))
                day_count = 0
                for row in reader:
                    cells = {str(k).strip().lower(): str(v).strip() for k, v in row.items() if k}
                    sym = (cells.get("symbol") or "").upper()
                    if not sym:
                        continue
                    from_band = _parse_band_value(cells.get("from"))
                    to_band = _parse_band_value(cells.get("to"))
                    if to_band is None:
                        continue
                    entries = changes.setdefault(sym, [])
                    if any(e and e[0] == day_iso for e in entries):
                        continue
                    entries.append([day_iso, from_band, to_band])
                    day_count += 1
                fetched_now.append(day_iso)
                new_rows += day_count
                logger.info("band-changes %s: %s revisions", day_iso, day_count)
            except Exception as exc:
                logger.warning("band-changes parse failed for %s: %s", day_iso, exc)

        if not fetched_now:
            return

        for entries in changes.values():
            entries.sort(key=lambda e: e[0])
        all_fetched = sorted(fetched | set(fetched_now))
        store = {
            "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
            # Cap the index so the file doesn't grow unbounded; anything older
            # than the cap is far outside the rescan window anyway.
            "fetched_dates": all_fetched[-500:],
            "changes": changes,
        }
        PRICE_BAND_CHANGES_PATH.write_text(json.dumps(store, separators=(",", ":")), encoding="utf-8")
        logger.info(
            "Written %s: +%s revisions across %s new date(s), %s symbols total",
            PRICE_BAND_CHANGES_PATH.name, new_rows, len(fetched_now), len(changes),
        )
    except Exception as exc:  # pragma: no cover — never break the price patch
        logger.warning("price-band-changes update failed: %s", exc)


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

    # Daily NSE price-band revisions (for the chart band-change markers). Runs
    # BEFORE the price phases because each phase returns early; failures are
    # swallowed inside so this can never block the price patch.
    update_price_band_changes(target)
    # Complete current band list (authoritative per-symbol band; absence = no
    # fixed band). Same failure-isolation as above.
    update_price_bands(target)

    # --- Phase 1: BSE bhavcopy (no geo-blocking, available ~4:24 PM IST) merged with yfinance NSE bars. ---
    # The universe is NSE-keyed (.NS), so yfinance .NS data IS the authoritative
    # NSE feed and is preferred over BSE-segment OHLC for any symbol it covers.
    # BSE remains the fallback for BSE-only listings or YF transient gaps; both
    # sources are validated through _is_record_sane before inclusion.
    bse_symbols = _fetch_from_bse(target)
    if bse_symbols:
        # Merge FIRST so breadth can see authoritative NSE (yfinance) closes for
        # NSE-primary names — raw BSE-segment prints are exactly the data class
        # the price merge already distrusts (stale last-ticks on illiquid BSE
        # lines skew advancers/decliners and the MA% inputs). Breadth gets the
        # full BSE coverage overlaid with the merged records, and still runs
        # BEFORE the price-patch "already current" guard so a day where only
        # breadth is behind can't freeze the XP score. _update_xp_breadth is
        # idempotent on date, so recomputing an already-stored day is harmless.
        merged = _merge_bse_with_yfinance(bse_symbols, target, extra_tickers=extra_yf_tickers)
        breadth_src = dict(bse_symbols)
        if merged:
            breadth_src.update(merged)
        _update_xp_breadth(target, breadth_src, "YF+BSE")
        if _patch_already_current(target):
            logger.info("Price patch already current for %s (%s symbols). Breadth refreshed; no price update needed.", target.isoformat(), len(bse_symbols))
            return 0
        if not merged:
            logger.warning("Merged patch for %s is empty after sanity filter; aborting.", target)
            return 1
        _attach_indicator_blocks(merged, target, extra_tickers=extra_yf_tickers)
        _write_patch(target, merged, "YF+BSE", new_listings=new_listings)
        return 0

    logger.info("BSE unavailable for %s. Trying NSE archives.", target)

    # --- Phase 2: NSE archives (authoritative, but geo-blocked from non-Indian IPs) ---
    csv_text = _fetch_bhavcopy_csv(target)
    if csv_text:
        symbols = _parse_bhavcopy_csv(csv_text)
        if symbols:
            symbols = {s: r for s, r in symbols.items() if _is_record_sane(r, sym=s)}
            if symbols:
                _update_xp_breadth(target, symbols, "NSE")
                if _patch_already_current(target):
                    logger.info("Price patch already current for %s (%s symbols). Breadth refreshed; no price update needed.", target.isoformat(), len(symbols))
                    return 0
                _write_patch(target, symbols, "NSE", new_listings=new_listings)
                return 0

    logger.warning("NSE unavailable for %s. Trying yfinance fallback.", target)

    # --- Phase 3: yfinance fallback for today (globally accessible via Yahoo Finance) ---
    yf_symbols = _fetch_from_yfinance(target, extra_tickers=extra_yf_tickers)
    if yf_symbols:
        yf_symbols = {s: r for s, r in yf_symbols.items() if _is_record_sane(r, sym=s)}
        if yf_symbols:
            # BSE is the preferred (full-market) breadth source, but when it is
            # unavailable the yfinance fallback MUST still advance the XP breadth
            # series — otherwise a BSE outage silently freezes the dashboard's
            # breadth score (this is exactly what stranded breadth at 2026-06-02).
            _update_xp_breadth(target, yf_symbols, "YFINANCE")
            if _patch_already_current(target):
                logger.info("Price patch already current for %s. Breadth refreshed; no price update needed.", target.isoformat())
                return 0
            _attach_indicator_blocks(yf_symbols, target, extra_tickers=extra_yf_tickers)
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
            _update_xp_breadth(prev, bse_prev, "BSE")
            if _patch_already_current(prev):
                logger.info("Price patch already current for %s (%s symbols). Breadth refreshed; no price update needed.", prev.isoformat(), len(merged_prev))
                return 0
            _attach_indicator_blocks(merged_prev, prev)
            _write_patch(prev, merged_prev, "YF+BSE")
            return 0
    csv_text = _fetch_bhavcopy_csv(prev)
    if csv_text:
        symbols = _parse_bhavcopy_csv(csv_text)
        if symbols:
            symbols = {s: r for s, r in symbols.items() if _is_record_sane(r, sym=s)}
            if symbols:
                _update_xp_breadth(prev, symbols, "NSE")
                if _patch_already_current(prev):
                    logger.info("Price patch already current for %s (%s symbols). Breadth refreshed; no price update needed.", prev.isoformat(), len(symbols))
                    return 0
                _write_patch(prev, symbols, "NSE")
                return 0

    logger.error("Could not fetch Bhavcopy from NSE, BSE, or yfinance for any date.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
