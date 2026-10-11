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


# A rate-limited page can come back as an empty `data` list, which reads
# exactly like the end of the listing. The first live run lost most of two
# quarters that way (40 filings listed for Apr-Jun 2026). So a page is only the
# end when the rows add up to `totalCount`; short of that it is retried, and a
# window still short afterwards is kept in `listing_gaps` and listed again on
# the next run.
LISTING_VERSION = 2
LIST_PAGE_GAP = 0.5
LIST_EMPTY_RETRIES = 3
LIST_BACKOFF = 6.0


def list_window(start: date, stop: date) -> tuple[list[dict], bool] | None:
    """(rows, complete) for one window, or None if NSE did not answer."""
    rows: list[dict] = []
    total: int | None = None
    page = 1
    empty_retries = 0
    while page < 2000:
        payload = _get(LIST_URL, {"index": "equities", "from_date": start.strftime("%d-%m-%Y"),
                                  "to_date": stop.strftime("%d-%m-%Y"), "page": str(page)}, expect_json=True)
        if not isinstance(payload, dict):
            return None
        count = payload.get("totalCount")
        if isinstance(count, int):
            total = count
        batch = [r for r in payload.get("data") or [] if isinstance(r, dict)]
        if batch:
            rows.extend(batch)
            page += 1
            empty_retries = 0
            if total is not None and len(rows) >= total:
                return rows, True
            time.sleep(LIST_PAGE_GAP)
            continue
        if total is None or len(rows) >= total:
            return rows, True
        if empty_retries >= LIST_EMPTY_RETRIES:
            return rows, False
        empty_retries += 1
        time.sleep(LIST_BACKOFF * empty_retries)
    return rows, False


def fetch_listing(start: date, end: date) -> tuple[list[dict], list[tuple[date, date]]] | None:
    """(rows, windows that came back short) for [start, end], or None if any
    window got no answer — a failed listing must not advance `listed_through`."""
    rows: list[dict] = []
    short: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=WINDOW_DAYS - 1))
        result = list_window(cursor, stop)
        if result is None:
            return None
        got, complete = result
        rows.extend(got)
        if not complete:
            short.append((cursor, stop))
        print(f"  integrated filings {cursor} .. {stop}: {len(got)}{'' if complete else ' (SHORT, kept for next run)'}",
              flush=True)
        cursor = stop + timedelta(days=1)
    return rows, short


# nsearchives is not the API host and wants different handling (measured
# 2026-10-01 by the india-xbrl-filings project, and on our first run, where
# 312 of 522 downloads failed in bursts):
# - HTTP/2 fails there (curl INTERNAL_ERROR), and a Chrome-impersonating
#   curl_cffi session speaks HTTP/2 — so plain `requests` (HTTP/1.1) with a
#   browser User-Agent;
# - requests 0.6 s apart failed 8 of 20; 6 s apart per worker with an
#   escalating, jittered backoff succeeded every time. NSE holds connections
#   ~30 s, so throughput comes from workers, not from a shorter gap.
ARCHIVE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
    "Accept": "application/xml, text/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
ARCHIVE_MIN_INTERVAL = 6.0
ARCHIVE_BACKOFF = 6.0
ARCHIVE_ATTEMPTS = 3
_statuses: dict[str, int] = {}
# Past this, no new attempt or backoff starts: the file must be written before
# the workflow's hard `timeout` kills the step, or the whole run is lost.
_deadline = float("inf")
_status_lock = threading.Lock()


def _count(status: str) -> None:
    with _status_lock:
        _statuses[status] = _statuses.get(status, 0) + 1


def _fetch_archive(url: str) -> bytes | None:
    import random

    import requests

    if not hasattr(_local, "archive"):
        _local.archive = requests.Session()
        _local.archive.headers.update(ARCHIVE_HEADERS)
        _local.last = 0.0
    for attempt in range(ARCHIVE_ATTEMPTS):
        if time.monotonic() >= _deadline:
            return None
        wait = ARCHIVE_MIN_INTERVAL - (time.monotonic() - _local.last)
        if wait > 0:
            time.sleep(wait)
        _local.last = time.monotonic()
        try:
            response = _local.archive.get(url, timeout=45)
            _count(str(response.status_code))
            if response.status_code == 200 and response.content.lstrip().startswith(b"<"):
                return response.content
            if response.status_code == 404:
                return None
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the run
            _count(type(exc).__name__)
        backoff = ARCHIVE_BACKOFF * (attempt + 1)
        time.sleep(backoff + random.uniform(0, backoff * 0.25))
    return None


def download(item: dict) -> tuple[dict, dict | None, bool]:
    """(queue item, parsed filing or None, reached NSE)."""
    body = _fetch_archive(item["xbrl"])
    if body is None:
        return item, None, False
    return item, cr.parse_filing(body), True


def queue_key(item: dict) -> str:
    return f"{item['symbol']}|{item['period_end']}|{item.get('basis', 'consolidated')}"


# Version 2 reads every column of a filing (the previous and year-ago
# quarters, not just its own), and standalone filings too. Filings stored
# before it are fetched once more to collect their columns.
HARVEST_VERSION = 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-seconds", type=float, default=1200)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()
    started = time.monotonic()
    global _deadline
    _deadline = started + args.max_seconds

    path = cr.ARTIFACT_PATH
    doc: dict = {}
    if path.exists():
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            doc = {}
    symbols: dict = doc.setdefault("symbols", {})
    queue: dict = {queue_key(q): q for q in doc.get("queue") or []}
    harvest_again = doc.get("harvest_version") != HARVEST_VERSION
    samples: list = list(doc.get("unparsed_samples") or [])[-10:]
    # Filings read and found unusable: not downloaded again unless revised.
    skipped_filings: dict = dict(doc.get("skipped_filings") or {}) if not harvest_again else {}

    universe = json.loads((BACKEND_ROOT / "data" / "free_universe.json").read_text(encoding="utf-8"))
    known = {str(r["symbol"]).upper() for r in universe if r.get("symbol")}

    today = _now().date()
    through = doc.get("listed_through")
    start = date.fromisoformat(through) - timedelta(days=1) if through else FIRST_LISTING
    gaps = [(date.fromisoformat(a), date.fromisoformat(b)) for a, b in doc.get("listing_gaps") or []]
    if (doc.get("listing_version") != LISTING_VERSION or harvest_again) and through:
        # Listed before short pages were caught: list the history once more.
        gaps.append((FIRST_LISTING, date.fromisoformat(through)))
    listing: list[dict] = []
    new_gaps: list[tuple[date, date]] = []
    listed_ok = True
    for gap_start, gap_end in gaps:
        print(f"re-listing a short window {gap_start} .. {gap_end}", flush=True)
        result = fetch_listing(gap_start, gap_end)
        if result is None:
            new_gaps.append((gap_start, gap_end))
            continue
        listing.extend(result[0])
        new_gaps.extend(result[1])
    print(f"listing NSE integrated filings {start} .. {today}", flush=True)
    result = fetch_listing(start, today)
    if result is None:
        listed_ok = False
    else:
        listing.extend(result[0])
        new_gaps.extend(result[1])

    def stored_filed(symbol: str, period: str, basis: str) -> str:
        for row in (symbols.get(symbol) or {}).get(cr.ROW_KEYS[basis]) or []:
            if row.get("period") == period:
                return str(row.get("filed") or "")
        return ""

    added = 0
    for raw in listing:
        item = cr.listing_row(raw)
        if item is None or item["symbol"] not in known:
            continue
        period = cr.period_label(date.fromisoformat(item["period_end"]))
        if not harvest_again and stored_filed(item["symbol"], period, item["basis"]) >= item["filed"]:
            continue  # already have this filing (or a later revision)
        key = queue_key(item)
        if str(skipped_filings.get(key) or "") >= item["filed"]:
            continue
        if key not in queue or queue[key]["filed"] < item["filed"]:
            queue[key] = {**item, "attempts": 0}
            added += 1
    print(f"{added} filings queued (consolidated and standalone), {len(queue)} waiting", flush=True)

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
            item, parsed, reached = done.result()
            key = queue_key(item)
            basis = item.get("basis", "consolidated")
            nature = str((parsed or {}).get("nature") or "").strip().lower()
            if parsed is not None and parsed["periods"] and (not nature or nature == basis):
                entry = symbols.setdefault(item["symbol"], {})
                field = cr.ROW_KEYS[basis]
                entry[field] = cr.merge_filing(entry.get(field) or [], parsed["periods"],
                                               date.fromisoformat(item["period_end"]), item["filed"])
                queue.pop(key, None)
                stored += 1
            elif reached:
                # Not a readable quarter (an unknown schedule, no revenue
                # line): keep its element names so the schedule can be added.
                queue.pop(key, None)
                skipped += 1
                skipped_filings[key] = item["filed"]
                if parsed is not None:
                    names = [n for n in parsed["names"]
                             if any(w in n for w in ("Revenue", "Income", "Interest", "Profit", "Expense", "Earnings"))]
                    samples.append({"symbol": item["symbol"], "basis": basis, "xbrl": item["xbrl"],
                                    "nature": parsed.get("nature"), "names": names[:60]})
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
                      f"({time.monotonic() - started:.0f}s) responses={_statuses}", flush=True)

    stamp = datetime.now(timezone.utc).isoformat()
    out = {
        "generated_at": stamp,
        "listed_through": today.isoformat() if listed_ok else doc.get("listed_through"),
        # Any window not yet listed in full is in listing_gaps, so the
        # one-off re-list is recorded as done whatever this run managed.
        "listing_version": LISTING_VERSION,
        "harvest_version": HARVEST_VERSION,
        "listing_gaps": [[a.isoformat(), b.isoformat()] for a, b in new_gaps],
        "source": ("NSE Integrated Filing XBRL (Rs / 1e7 = crore): 'quarters' consolidated, 'standalone' standalone; "
                   "each filing's own quarter ('filed') and its comparative columns ('via'); "
                   "consolidated net profit attributable to owners"),
        "unparsed_samples": samples[-10:],
        "skipped_filings": dict(sorted(skipped_filings.items())),
        "queue": sorted(queue.values(), key=lambda q: q["filed"], reverse=True),
        "symbols": dict(sorted(symbols.items())),
    }
    path.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {path.name}: {len(symbols)} companies, stored={stored} skipped={skipped} failed={failed}, "
          f"{len(queue)} still queued, {path.stat().st_size / 1024:.0f} KB; responses={_statuses}", flush=True)
    return 0 if listed_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
