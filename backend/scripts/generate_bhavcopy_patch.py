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
import sys
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
OUTPUT_PATH = DATA_DIR / "bhavcopy_patch.json"

BHAV_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_yyyymmdd}_F_0000.csv.zip"
BHAV_URL_ALT = "https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_yyyymmdd}_F_0000.csv.zip"
BHAV_URL_LEGACY = "https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{mon_upper}/cm{dd}{mon_upper}{year}bhav.csv.zip"
BHAV_URL_LEGACY_ALT = "https://archives.nseindia.com/content/historical/EQUITIES/{year}/{mon_upper}/cm{dd}{mon_upper}{year}bhav.csv.zip"

# BSE (Bombay Stock Exchange) bhavcopy — same-day authoritative data, no geo-blocking.
# New format CSV (no zip) available from ~4 PM IST: accessible from any IP globally.
BSE_BHAV_URL = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_yyyymmdd}_F_0000.CSV"

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
                s.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
                r = s.get(url, headers=HEADERS, timeout=30)

            if r.status_code != 200 or not r.content:
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
            print(f"  warn: {url} → {exc}", file=sys.stderr)
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
        resp = requests.get(url, headers=BSE_HEADERS, timeout=20)
        if resp.status_code != 200 or len(resp.content) < 1000:
            print(f"  BSE: HTTP {resp.status_code} for {url}", file=sys.stderr)
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

        print(f"BSE fallback: fetched {len(result)} symbols for {trade_date.isoformat()}")
        return result
    except Exception as exc:
        print(f"  BSE fetch failed: {exc}", file=sys.stderr)
        return None


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


def _fetch_from_yfinance(trade_date: date) -> dict[str, dict] | None:
    """Fallback: fetch EOD prices via yfinance when NSE archives are geo-blocked.

    GitHub Actions servers are outside India and NSE often blocks them.  Yahoo
    Finance is globally accessible, so this ensures we always have today's data.
    Returns the same compact symbol-dict format as _parse_bhavcopy_csv, or None.
    """
    try:
        import yfinance as yf
        import pandas as pd
    except ImportError:
        print("yfinance/pandas not installed; skipping yfinance fallback", file=sys.stderr)
        return None

    universe_path = DATA_DIR / "free_universe.json"
    if not universe_path.exists():
        print("free_universe.json not found; skipping yfinance fallback", file=sys.stderr)
        return None

    try:
        universe = json.loads(universe_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Failed to read universe: {exc}", file=sys.stderr)
        return None

    if not isinstance(universe, list):
        return None

    # Extract .NS tickers (already stored in the universe file)
    tickers = [s.get("ticker", "") for s in universe if str(s.get("ticker", "")).endswith(".NS")]
    if not tickers:
        return None

    start_str = trade_date.strftime("%Y-%m-%d")
    end_str = (trade_date + timedelta(days=1)).strftime("%Y-%m-%d")

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
                        if sub.empty:
                            continue
                        row = sub.iloc[-1]
                        c = float(row.get("Close") or 0)
                        if c <= 0:
                            continue
                        result[sym] = {
                            "o": float(row.get("Open") or 0),
                            "h": float(row.get("High") or 0),
                            "l": float(row.get("Low") or 0),
                            "c": c,
                            "v": int(float(row.get("Volume") or 0)),
                            "p": 0.0,  # prev_close not available from yfinance batch
                        }
                    except Exception:
                        continue
            else:
                # Single-ticker result: columns are simple field names
                sym = chunk[0].replace(".NS", "")
                if df.empty:
                    continue
                row = df.iloc[-1]
                c = float(row.get("Close") or 0)
                if c > 0:
                    result[sym] = {
                        "o": float(row.get("Open") or 0),
                        "h": float(row.get("High") or 0),
                        "l": float(row.get("Low") or 0),
                        "c": c,
                        "v": int(float(row.get("Volume") or 0)),
                        "p": 0.0,
                    }
        except Exception as exc:
            print(f"yfinance chunk {i // CHUNK + 1} failed: {exc}", file=sys.stderr)
            continue

    if not result:
        return None

    print(f"yfinance fallback: fetched {len(result)} symbols for {trade_date.isoformat()}")
    return result


def _patch_already_current(target_date: date) -> bool:
    """Return True if bhavcopy_patch.json already has data for target_date."""
    if not OUTPUT_PATH.exists():
        return False
    try:
        existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        return existing.get("date") == target_date.isoformat()
    except Exception:
        return False


def _write_patch(target_date: date, symbols: dict) -> None:
    patch = {"date": target_date.isoformat(), "symbols": symbols}
    OUTPUT_PATH.write_text(json.dumps(patch, separators=(",", ":")), encoding="utf-8")
    print(f"Written {OUTPUT_PATH} — date={target_date.isoformat()}, symbols={len(symbols)}")


def main() -> int:
    target = _last_trading_day()
    print(f"Fetching EOD Bhavcopy for {target.isoformat()} …")

    # --- Phase 1: BSE bhavcopy (authoritative, no geo-blocking, available ~4:24 PM IST) ---
    bse_symbols = _fetch_from_bse(target)
    if bse_symbols:
        if _patch_already_current(target):
            print(f"Patch already current for {target.isoformat()} ({len(bse_symbols)} symbols). No update needed.")
            return 0
        _write_patch(target, bse_symbols)
        return 0

    print(f"  BSE unavailable for {target} — trying NSE archives …")

    # --- Phase 2: NSE archives (authoritative, but geo-blocked from non-Indian IPs) ---
    csv_text = _fetch_bhavcopy_csv(target)
    if csv_text:
        symbols = _parse_bhavcopy_csv(csv_text)
        if symbols:
            if _patch_already_current(target):
                print(f"Patch already current for {target.isoformat()} ({len(symbols)} symbols). No update needed.")
                return 0
            _write_patch(target, symbols)
            return 0

    print(f"  NSE also unavailable for {target} — trying yfinance fallback …", file=sys.stderr)

    # --- Phase 3: yfinance fallback for today (globally accessible via Yahoo Finance) ---
    yf_symbols = _fetch_from_yfinance(target)
    if yf_symbols:
        if _patch_already_current(target):
            print(f"Patch already current for {target.isoformat()}. No update needed.")
            return 0
        _write_patch(target, yf_symbols)
        return 0

    print(f"  yfinance also unavailable — trying BSE for previous trading day …", file=sys.stderr)

    # --- Phase 4: last resort — previous trading day via BSE then NSE (holiday fallback) ---
    prev = target - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    bse_prev = _fetch_from_bse(prev)
    if bse_prev:
        if _patch_already_current(prev):
            print(f"Patch already current for {prev.isoformat()} ({len(bse_prev)} symbols). No update needed.")
            return 0
        _write_patch(prev, bse_prev)
        return 0
    csv_text = _fetch_bhavcopy_csv(prev)
    if csv_text:
        symbols = _parse_bhavcopy_csv(csv_text)
        if symbols:
            if _patch_already_current(prev):
                print(f"Patch already current for {prev.isoformat()} ({len(symbols)} symbols). No update needed.")
                return 0
            _write_patch(prev, symbols)
            return 0

    print("ERROR: Could not fetch Bhavcopy from NSE, BSE, or yfinance for any date.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
