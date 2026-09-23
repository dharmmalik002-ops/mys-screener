"""Results announcement calendar from BSE `Result` filings -> data/results_calendar.json.

BSE is the regulator of record, so every listed company files there; the feed
reaches back to mid-2011. It only answers <=14-day windows (longer ranges come
back empty), so the history is walked in fortnights.

    python3 scripts/build_results_calendar.py                  # last 21 days, merged in
    python3 scripts/build_results_calendar.py --since 2011-07-01   # full rebuild (~40 min)

Each symbol keeps the FIRST filing of each quarter (a results filing is
usually followed by a limited-review report and a press release within days),
with the time of day, because a filing after the close is priced the next
session (see app/services/bot/results_calendar.py).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.earnings_metrics import BSE_ANN_URL, BSE_HEADERS  # noqa: E402

SAME_QUARTER_DAYS = 25


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(BSE_HEADERS)
    try:
        s.get("https://www.bseindia.com/corporates/ann.html", timeout=15)
    except Exception:  # noqa: BLE001 - priming only
        pass
    return s


def fetch_window(s: requests.Session, start: date) -> list[tuple[str, str]] | None:
    """(scrip code, filing datetime) for one 14-day window, or None on failure."""
    end = start + timedelta(days=13)
    base = {"strCat": "Result", "strPrevDate": start.strftime("%Y%m%d"),
            "strToDate": end.strftime("%Y%m%d"), "strSearch": "P", "strscrip": "", "strType": "C"}
    rows: list[tuple[str, str]] = []
    total = None
    for page in range(1, 200):
        payload = None
        for attempt in range(5):
            try:
                r = s.get(BSE_ANN_URL, params={**base, "pageno": str(page)}, timeout=40)
                r.raise_for_status()
                payload = r.json()
                break
            except Exception:  # noqa: BLE001
                time.sleep(2 * (attempt + 1))
        if payload is None:
            return None
        table = payload.get("Table") or []
        if total is None:
            total = int(((payload.get("Table1") or [{}])[0]).get("ROWCNT") or 0)
        rows += [(str(x.get("SCRIP_CD")), str(x.get("NEWS_DT") or "")) for x in table]
        if not table or len(rows) >= total:
            break
    return rows


def merge(existing: dict, filings: list[tuple[str, str]], by_code: dict) -> dict:
    events: dict = defaultdict(set)
    for sym, evs in existing.items():
        for d, m in evs:
            events[sym].add(datetime.fromisoformat(d) + timedelta(minutes=int(m)))
    for code, stamp in filings:
        sym = by_code.get(code)
        if not sym or not stamp:
            continue
        try:
            events[sym].add(datetime.fromisoformat(stamp.split(".")[0]))
        except ValueError:
            continue
    out = {}
    for sym, stamps in events.items():
        kept: list[datetime] = []
        for t in sorted(stamps):
            if kept and (t - kept[-1]).days < SAME_QUARTER_DAYS:
                continue
            kept.append(t)
        out[sym] = [[t.date().isoformat(), t.hour * 60 + t.minute] for t in kept]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--since", default="")
    args = ap.parse_args()
    data_dir = BACKEND_ROOT / "data"
    path = data_dir / "results_calendar.json"
    universe = json.loads((data_dir / "free_universe.json").read_text(encoding="utf-8"))
    by_code = {str(r["bse_code"]): str(r["symbol"]).upper() for r in universe if r.get("bse_code")}
    existing = json.loads(path.read_text()) if path.exists() else {}
    start = date.fromisoformat(args.since) if args.since else date.today() - timedelta(days=args.days)
    s = _session()
    filings, failed = [], 0
    d = start
    while d <= date.today():
        got = fetch_window(s, d)
        if got is None:
            failed += 1
        else:
            filings += got
        d += timedelta(days=14)
    merged = merge(existing, filings, by_code)
    before = sum(len(v) for v in existing.values())
    after = sum(len(v) for v in merged.values())
    if after < before:
        print(f"refusing to shrink the calendar ({before} -> {after})")
        return 1
    path.write_text(json.dumps(merged, separators=(",", ":")))
    print(f"results calendar: {len(merged)} symbols, {after} announcements (+{after - before}); "
          f"{len(filings)} filings read, {failed} window(s) failed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
