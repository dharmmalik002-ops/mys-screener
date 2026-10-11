#!/usr/bin/env python3
"""Consolidated quarterly results from NSE -> data/consolidated_results.json.

    python3 scripts/update_consolidated_results.py                  # since the last listing
    python3 scripts/update_consolidated_results.py --max-seconds 600

Run by .github/workflows/quarterly-results.yml after the standalone feed.
Each run lists NSE's Integrated Filing results since the last successful
listing (one day of overlap; the first run starts in January 2025, when the
filing began), queues every CONSOLIDATED filing of a universe company whose
quarter is not stored yet (or was revised since), and downloads the queue's
XBRL instances newest first until `--max-seconds` runs out. The queue is kept
in the file, so the first backfill (~7 quarters of every group) finishes over a
few runs while each evening's new filings are taken first.

NSE answers only a browser TLS fingerprint from the runners (gotcha 159), so
this goes through `exchange_http.browser_session` like the BSE jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services import bse_quarterly as bq  # noqa: E402
from app.services import consolidated_results as cr  # noqa: E402
from app.services.exchange_http import browser_session  # noqa: E402

LIST_URL = "https://www.nseindia.com/api/integrated-filing-results"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-integrated-filing",
}
IST = timezone(timedelta(hours=5, minutes=30))
FIRST_LISTING = date(2025, 1, 1)
WINDOW_DAYS = 90  # NSE silently truncates wide windows
MAX_ATTEMPTS = 5  # per queued filing, across runs

_local = threading.local()


def _now() -> datetime:
    return datetime.now(IST)


def _session():
    if not hasattr(_local, "session"):
        _local.session = browser_session(HEADERS)
        try:  # cookies, as a browser would have them
            _local.session.get("https://www.nseindia.com/", timeout=20)
        except Exception:  # noqa: BLE001 - priming only
            pass
    return _local.session


def _get(url: str, params: dict | None = None, *, expect_json: bool) -> object | None:
    for attempt in range(3):
        try:
            response = _session().get(url, params=params, timeout=40)
            if response.status_code == 200:
                if not expect_json:
                    return response.content
                text = response.text.strip()
                if text.startswith(("{", "[")):
                    return response.json()
        except Exception:  # noqa: BLE001 - one bad call must not stop the run
            pass
        time.sleep(2 * (attempt + 1))
    return None


def fetch_listing(start: date, end: date) -> list[dict] | None:
    """Every Integrated Filing row filed in [start, end], or None if any page
    failed — a partial listing must not advance `listed_through`."""
    rows: list[dict] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=WINDOW_DAYS - 1))
        got = 0
        for page in range(1, 2000):
            payload = _get(LIST_URL, {"index": "equities", "from_date": cursor.strftime("%d-%m-%Y"),
                                      "to_date": stop.strftime("%d-%m-%Y"), "page": str(page)}, expect_json=True)
            if not isinstance(payload, dict):
                return None
            batch = [r for r in payload.get("data") or [] if isinstance(r, dict)]
            rows.extend(batch)
            got += len(batch)
            total = payload.get("totalCount")
            if not batch or (isinstance(total, int) and got >= total):
                break
        print(f"  integrated filings {cursor} .. {stop}: {got}", flush=True)
        cursor = stop + timedelta(days=1)
    return rows


def download(item: dict) -> tuple[dict, dict | None, bool]:
    """(queue item, parsed row or None, reached NSE)."""
    body = _get(item["xbrl"], expect_json=False)
    if body is None:
        return item, None, False
    expect = date.fromisoformat(item["period_end"])
    return item, cr.parse_instance(body, expect_end=expect), True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=1200)
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    started = time.monotonic()

    path = cr.ARTIFACT_PATH
    doc: dict = {}
    if path.exists():
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            doc = {}
    symbols: dict = doc.setdefault("symbols", {})
    queue: dict = {f"{q['symbol']}|{q['period_end']}": q for q in doc.get("queue") or []}

    universe = json.loads((BACKEND_ROOT / "data" / "free_universe.json").read_text(encoding="utf-8"))
    known = {str(r["symbol"]).upper() for r in universe if r.get("symbol")}

    today = _now().date()
    through = doc.get("listed_through")
    start = date.fromisoformat(through) - timedelta(days=1) if through else FIRST_LISTING
    print(f"listing NSE integrated filings {start} .. {today}", flush=True)
    listing = fetch_listing(start, today)
    listed_ok = listing is not None

    def stored_filed(symbol: str, period: str) -> str:
        for row in (symbols.get(symbol) or {}).get("quarters") or []:
            if row.get("period") == period:
                return str(row.get("filed") or "")
        return ""

    added = 0
    for raw in listing or []:
        item = cr.listing_row(raw)
        if item is None or item["symbol"] not in known:
            continue
        period = cr.period_label(date.fromisoformat(item["period_end"]))
        if stored_filed(item["symbol"], period) >= item["filed"]:
            continue  # already have this filing (or a later revision)
        key = f"{item['symbol']}|{item['period_end']}"
        if key not in queue or queue[key]["filed"] < item["filed"]:
            queue[key] = {**item, "attempts": 0}
            added += 1
    print(f"{added} consolidated filings queued, {len(queue)} waiting", flush=True)

    # Newest filings first: tonight's results before last year's backfill.
    pending = sorted(queue.values(), key=lambda q: q["filed"], reverse=True)
    stored = skipped = failed = 0
    budget = args.max_seconds
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = set()
        it = iter(pending)
        for item in it:
            futures.add(pool.submit(download, item))
            if len(futures) >= args.workers * 2:
                break
        while futures:
            done = next(as_completed(futures))
            futures.discard(done)
            item, row, reached = done.result()
            key = f"{item['symbol']}|{item['period_end']}"
            if row is not None:
                row["filed"] = item["filed"]
                entry = symbols.setdefault(item["symbol"], {"quarters": []})
                entry["quarters"] = sorted(
                    [r for r in entry["quarters"] if r.get("period") != row["period"]] + [row],
                    key=lambda r: bq._period_key(r["period"]),
                    reverse=True,
                )[: cr.QUARTERS_KEPT]
                queue.pop(key, None)
                stored += 1
            elif reached:
                queue.pop(key, None)  # not a usable consolidated quarter (bank schedule, no revenue)
                skipped += 1
            else:
                item["attempts"] = int(item.get("attempts") or 0) + 1
                if item["attempts"] >= MAX_ATTEMPTS:
                    queue.pop(key, None)
                failed += 1
            if time.monotonic() - started < budget:
                nxt = next(it, None)
                if nxt is not None:
                    futures.add(pool.submit(download, nxt))
            n = stored + skipped + failed
            if n % 200 == 0:
                print(f"  {n} done: stored={stored} skipped={skipped} failed={failed} "
                      f"({time.monotonic() - started:.0f}s)", flush=True)

    stamp = datetime.now(timezone.utc).isoformat()
    out = {
        "generated_at": stamp,
        "listed_through": today.isoformat() if listed_ok else doc.get("listed_through"),
        "source": "NSE Integrated Filing XBRL, consolidated (Rs / 1e7 = crore); net profit attributable to owners",
        "queue": sorted(queue.values(), key=lambda q: q["filed"], reverse=True),
        "symbols": dict(sorted(symbols.items())),
    }
    path.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {path.name}: {len(symbols)} companies, stored={stored} skipped={skipped} failed={failed}, "
          f"{len(queue)} still queued, {path.stat().st_size / 1024:.0f} KB", flush=True)
    return 0 if listed_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
