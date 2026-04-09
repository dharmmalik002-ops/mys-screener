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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
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


def main() -> int:
    target = _last_trading_day()
    print(f"Fetching NSE Bhavcopy for {target.isoformat()} …")

    csv_text = _fetch_bhavcopy_csv(target)
    if not csv_text:
        # Try one day earlier (holiday fallback)
        prev = target - timedelta(days=1)
        while prev.weekday() >= 5:
            prev -= timedelta(days=1)
        print(f"  Not found for {target}, trying {prev} …")
        csv_text = _fetch_bhavcopy_csv(prev)
        if csv_text:
            target = prev

    if not csv_text:
        print("ERROR: Could not fetch Bhavcopy CSV from NSE archives.", file=sys.stderr)
        return 1

    symbols = _parse_bhavcopy_csv(csv_text)
    if not symbols:
        print("ERROR: Parsed 0 symbols from Bhavcopy CSV.", file=sys.stderr)
        return 1

    # Check if patch is already current
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
            if existing.get("date") == target.isoformat():
                print(f"Patch already current for {target.isoformat()} ({len(symbols)} symbols). No update needed.")
                return 0
        except Exception:
            pass

    patch = {"date": target.isoformat(), "symbols": symbols}
    OUTPUT_PATH.write_text(json.dumps(patch, separators=(",", ":")), encoding="utf-8")
    print(f"Written {OUTPUT_PATH} — date={target.isoformat()}, symbols={len(symbols)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
