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

BHAV_URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_ddmmyyyy}_F_0000.csv.zip"
BHAV_URL_ALT = "https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_ddmmyyyy}_F_0000.csv.zip"

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
    date_str = trade_date.strftime("%d%m%Y")
    for url_template in (BHAV_URL, BHAV_URL_ALT):
        url = url_template.format(date_ddmmyyyy=date_str)
        try:
            with requests.Session() as s:
                s.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
                r = s.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200 and r.content:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                    name = next((n for n in z.namelist() if n.endswith(".csv")), None)
                    if name:
                        return z.read(name).decode("utf-8", errors="replace")
        except Exception as exc:
            print(f"  warn: {url} → {exc}", file=sys.stderr)
    return None


def _parse_bhavcopy_csv(csv_text: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    lines = csv_text.splitlines()
    if not lines:
        return result
    header = [h.strip().upper() for h in lines[0].split(",")]

    def col(row: list[str], *names: str) -> str:
        for name in names:
            if name in header:
                idx = header.index(name)
                if idx < len(row):
                    return row[idx].strip()
        return ""

    for line in lines[1:]:
        if not line.strip():
            continue
        row = [c.strip() for c in line.split(",")]
        # Only EQ series (regular equity, not derivatives/ETFs)
        series = col(row, "SERIES", "MKT_TYPE")
        if series not in ("EQ", "BE", "BZ", ""):
            continue
        symbol = col(row, "SYMBOL", "TCKR_SYMB")
        if not symbol:
            continue
        try:
            close = float(col(row, "CLOSE_PRICE", "CLOSE", "CLS_PR"))
            prev = float(col(row, "PREV_CL_PR", "PREV_CLOSE", "PREV_CL", "PVS_CL_PR"))
            high = float(col(row, "HIGH_PRICE", "HIGH", "HI_PR"))
            low = float(col(row, "LOW_PRICE", "LOW", "LO_PR"))
            open_ = float(col(row, "OPEN_PRICE", "OPEN", "OPN_PR"))
            vol = int(float(col(row, "TTL_TRD_QNTY", "TOTAL_TRADED_QUANTITY", "TOT_TRD_QTY", "TRDNG_SESS_QTY") or 0))
        except (ValueError, TypeError):
            continue
        result[symbol.upper()] = {"o": open_, "h": high, "l": low, "c": close, "v": vol, "p": prev}
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
