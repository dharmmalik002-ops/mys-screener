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
# NSE silently truncates wide windows: a 90-day window once listed 120 filings
# for a whole results season. Fifteen days keeps every window small.
WINDOW_DAYS = 15
# Listing may use this share of --max-seconds; the rest is for downloads.
LIST_BUDGET_SHARE = 0.45
# A filing that failed this many downloads is tried after everything else
# rather than dropped: a dropped filing was never listed again, so a company
# could stay without its quarter for good.
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
    """NSE's API stops answering a session after some minutes of paging (the
    third live run lost its listing at ~10 min), so a failure backs off and
    starts a fresh session, cookies and all."""
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
        if hasattr(_local, "session"):
            del _local.session
        time.sleep((5, 15, 30)[attempt])
    return None


# A rate-limited page can come back as an empty `data` list, which reads
# exactly like the end of the listing. The first live run lost most of two
# quarters that way (40 filings listed for Apr-Jun 2026). So a page is only the
# end when the rows add up to `totalCount`; short of that it is retried, and a
# window still short afterwards is kept in `listing_gaps` and listed again on
# the next run.
LISTING_VERSION = 2
LIST_PAGE_GAP = 1.0
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


def windows(start: date, end: date) -> list[tuple[date, date]]:
    """[start, end] in WINDOW_DAYS pieces, oldest first."""
    out: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=WINDOW_DAYS - 1))
        out.append((cursor, stop))
        cursor = stop + timedelta(days=1)
    return out


def list_windows(todo: list[tuple[date, date]], stop_at: float) -> tuple[dict, list[tuple[date, date]], bool]:
    """List each window in order until `stop_at` (monotonic). Returns
    ({window: rows}, windows still to list, whether any got no answer).

    Nothing listed is thrown away: a window NSE does not answer, or answers
    short of its totalCount, is handed back to be listed on the next run —
    the third live run lost ~12,000 listed filings because one window late in
    a long re-list got no answer and the whole re-list was discarded."""
    done: dict = {}
    left: list[tuple[date, date]] = []
    failed = False
    for index, (start, stop) in enumerate(todo):
        if time.monotonic() >= stop_at:
            left.extend(todo[index:])
            print(f"  listing time used up: {len(todo) - index} windows kept for the next run", flush=True)
            break
        result = list_window(start, stop)
        if result is None:
            failed = True
            left.append((start, stop))
            print(f"  integrated filings {start} .. {stop}: NO ANSWER, kept for the next run", flush=True)
            continue
        rows, complete = result
        done[(start, stop)] = rows
        if not complete:
            left.append((start, stop))
        print(f"  integrated filings {start} .. {stop}: {len(rows)}{'' if complete else ' (SHORT, kept for next run)'}",
              flush=True)
    return done, left, failed


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
HARVEST_VERSION = 5  # 4: a zero owners' profit no longer hides the period profit; 5: insurers' schedules


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
    layouts: list = []
    # Filings read and found unusable: not downloaded again unless revised.
    # (Cleared when a re-read of the history starts: the new reader may read
    # what the old one could not.)
    # A re-read already under way for an older reader starts over, or the
    # filings it stored before this reader existed would keep the old reading.
    restart = harvest_again and (not doc.get("harvesting") or doc.get("harvest_target") != HARVEST_VERSION)
    skipped_filings: dict = {} if restart else dict(doc.get("skipped_filings") or {})

    universe = json.loads((BACKEND_ROOT / "data" / "free_universe.json").read_text(encoding="utf-8"))
    known = {str(r["symbol"]).upper() for r in universe if r.get("symbol")}

    today = _now().date()
    through = doc.get("listed_through")
    start = date.fromisoformat(through) - timedelta(days=1) if through else FIRST_LISTING
    gaps = [w for a, b in doc.get("listing_gaps") or []
            for w in windows(date.fromisoformat(a), date.fromisoformat(b))]
    harvesting = bool(doc.get("harvesting"))
    if restart and through:
        # Read every column of filings stored before: list the history again,
        # spread over runs, and queue its filings whether stored or not.
        gaps.extend(windows(FIRST_LISTING, date.fromisoformat(through)))
        harvesting = True
    main_windows = windows(start, today)
    # Tonight's filings first, then the gaps newest first: recent seasons fill
    # before old ones.
    todo = main_windows + sorted(set(gaps) - set(main_windows), reverse=True)
    print(f"listing NSE integrated filings: {len(main_windows)} new and {len(todo) - len(main_windows)} earlier "
          f"windows", flush=True)
    listed, new_gaps, listing_failed = list_windows(todo, started + args.max_seconds * LIST_BUDGET_SHARE)
    from_gaps = {window for window in listed if window not in main_windows}
    listing = [(row, window in from_gaps) for window, rows in listed.items() for row in rows]
    listed_ok = not listing_failed
    reread_gaps = harvesting  # this run's gap windows are queued even if stored
    if harvesting and not new_gaps:
        harvesting = False  # the history has been listed in full: done

    def stored_filed(symbol: str, period: str, basis: str) -> str:
        for row in (symbols.get(symbol) or {}).get(cr.ROW_KEYS[basis]) or []:
            if row.get("period") == period:
                return str(row.get("filed") or "")
        return ""

    added = 0
    for raw, from_gap in listing:
        item = cr.listing_row(raw)
        if item is None or item["symbol"] not in known:
            continue
        period = cr.period_label(date.fromisoformat(item["period_end"]))
        rereading = from_gap and reread_gaps
        if not rereading and stored_filed(item["symbol"], period, item["basis"]) >= item["filed"]:
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
    pending.sort(key=lambda q: int(q.get("attempts") or 0) >= MAX_ATTEMPTS)  # stable: newest first within each
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
            if parsed is not None and len(layouts) < 3:
                # A few filings' context layouts per run, so the structure of
                # real filings can be read from the file (see gotcha 163).
                periods = {(c["start"], c["end"]) for c in parsed["contexts"] if c["start"] and not c["dims"]}
                layouts.append({"symbol": item["symbol"], "basis": basis, "xbrl": item["xbrl"],
                                "contexts": len(parsed["contexts"]),
                                "undimensioned_periods": sorted(f"{a}..{b}" for a, b in periods),
                                "dimensioned_sample": [c for c in parsed["contexts"] if c["dims"]][:12]})
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
                    # Cash-flow lines say nothing about the schedule; without
                    # them the profit and premium lines fit (insurers, gotcha 163).
                    names = [n for n in parsed["names"]
                             if any(w in n for w in ("Revenue", "Income", "Interest", "Profit", "Expense", "Earnings",
                                                     "Premium", "Surplus"))
                             and not any(w in n for w in ("Classified", "Adjustments", "Cash", "Description"))]
                    samples.append({"symbol": item["symbol"], "basis": basis, "xbrl": item["xbrl"],
                                    "nature": parsed.get("nature"), "names": names[:120]})
            else:
                item["attempts"] = int(item.get("attempts") or 0) + 1
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
        # Every window not listed in full is in listing_gaps, so the date
        # moves on whatever this run managed.
        "listed_through": today.isoformat(),
        # Any window not yet listed in full is in listing_gaps, so the
        # one-off re-list is recorded as done whatever this run managed.
        "listing_version": LISTING_VERSION,
        "harvest_version": doc.get("harvest_version") if harvesting else HARVEST_VERSION,
        "harvesting": harvesting,
        "harvest_target": HARVEST_VERSION if harvesting else None,
        "listing_gaps": [[a.isoformat(), b.isoformat()] for a, b in new_gaps],
        "source": ("NSE Integrated Filing XBRL (Rs / 1e7 = crore): 'quarters' consolidated, 'standalone' standalone; "
                   "each filing's own quarter ('filed') and its comparative columns ('via'); "
                   "consolidated net profit attributable to owners"),
        "unparsed_samples": samples[-10:],
        "context_samples": layouts or doc.get("context_samples") or [],
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
