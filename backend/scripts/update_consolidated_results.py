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
RETRY_DAYS = 5
# Listing may use this share of --max-seconds; the rest is for downloads.
LIST_BUDGET_SHARE = 0.45
# A filing that failed this many downloads is tried after everything else
# rather than dropped: a dropped filing was never listed again, so a company
# could stay without its quarter for good.
MAX_ATTEMPTS = 5  # per queued filing, across runs
RECENT_DAYS = 45  # "reported this season": a filing within this many days

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
LISTING_VERSION = 3  # 3: distinct rows count toward totalCount; every window listed again
LIST_PAGE_GAP = 1.0
LIST_EMPTY_RETRIES = 3
LIST_BACKOFF = 6.0


def list_window(start: date, stop: date) -> tuple[list[dict], bool] | None:
    """(rows, complete) for one window, or None if NSE did not answer."""
    rows: list[dict] = []
    seen: set[str] = set()
    total: int | None = None
    page = 1
    empty_retries = 0
    repeats = 0
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
            # NSE's pages overlap: a row can come back on two pages, so only
            # distinct filings count toward totalCount. Counting raw rows
            # ended windows early and left late July 2026 mostly unlisted
            # (DMART, CANFINHOME, POONAWALLA had no June quarter queued).
            fresh = [r for r in batch if _row_id(r) not in seen]
            repeats += len(batch) - len(fresh)
            for r in fresh:
                seen.add(_row_id(r))
            rows.extend(fresh)
            page += 1
            empty_retries = 0
            if total is not None and len(rows) >= total:
                break
            time.sleep(LIST_PAGE_GAP)
            continue
        if total is None or len(rows) >= total or empty_retries >= LIST_EMPTY_RETRIES:
            break
        empty_retries += 1
        time.sleep(LIST_BACKOFF * empty_retries)
        continue
    else:
        return rows, False
    if repeats:
        print(f"    {start} .. {stop}: {len(rows)} distinct of {total} ({repeats} repeated rows)", flush=True)
    return rows, total is None or len(rows) >= total


def _row_id(row: dict) -> str:
    """A repeated row comes back identical, field for field."""
    return json.dumps(row, sort_keys=True, default=str)


def windows(start: date, end: date, days: int | None = None) -> list[tuple[date, date]]:
    """[start, end] in WINDOW_DAYS (or `days`) pieces, oldest first."""
    out: list[tuple[date, date]] = []
    cursor = start
    step = days or WINDOW_DAYS
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=step - 1))
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
    # Windows listed again because the lister changed (LISTING_VERSION): their
    # new filings are queued, stored ones are not downloaded again.
    # A window retried for coming back short is asked for in RETRY_DAYS pieces:
    # 10-24 Jul 2026 came back short three runs running as one fortnight.
    relist = [w for a, b in doc.get("relist_gaps") or []
              for w in windows(date.fromisoformat(a), date.fromisoformat(b), RETRY_DAYS)]
    if doc.get("listing_version") != LISTING_VERSION and through:
        relist = windows(FIRST_LISTING, date.fromisoformat(through))
    main_windows = windows(start, today)
    # Tonight's filings first, then the gaps newest first: recent seasons fill
    # before old ones.
    earlier = sorted((set(gaps) | set(relist)) - set(main_windows), reverse=True)
    # NSE's listing is not a complete answer: the same fortnight came back
    # with 440 filings on one run and 1,120 on the next (and 3,087 then 220),
    # each time with a totalCount that agreed. So every window is listed again
    # in rotation, least recently listed first, with whatever listing time is
    # left; only filings not yet stored are queued, so the union of listings
    # fills in. A window far below the most it has ever returned is retried
    # on the next run.
    seen_windows: dict = dict(doc.get("window_seen") or {})
    grid = windows(FIRST_LISTING, today)
    rotation = sorted((w for w in grid if w not in set(main_windows) | set(earlier)),
                      key=lambda w: str((seen_windows.get(w[0].isoformat()) or {}).get("at") or ""))
    # The rotation asks in RETRY_DAYS pieces too: a fortnight NSE answers short
    # as one request is the case it exists for.
    piece_of = {piece: window for window in rotation for piece in windows(window[0], window[1], RETRY_DAYS)}
    todo = main_windows + earlier + list(piece_of)
    print(f"listing NSE integrated filings: {len(main_windows)} new, {len(earlier)} earlier and "
          f"{len(rotation)} in rotation", flush=True)
    listed, left, listing_failed = list_windows(todo, started + args.max_seconds * LIST_BUDGET_SHARE)
    # A rotation window counts as listed only when all its pieces were.
    pieces_left = {piece for piece in left if piece in piece_of}
    left = [w for w in left if w not in piece_of]
    for piece, window in piece_of.items():
        if piece in listed:
            rows = listed.pop(piece)
            if window not in {piece_of[p] for p in pieces_left}:
                listed.setdefault(window, []).extend(rows)
            else:
                listed.setdefault(("partial",) + window, []).extend(rows)
    gap_set = set(gaps)
    rotation_set = set(rotation)
    from_gaps = {window for window in listed if window not in main_windows and window in gap_set}
    new_gaps = [w for w in left if w in gap_set or w in main_windows]
    new_relist = [w for w in left if w not in gap_set and w not in main_windows and w not in rotation_set]
    stamp_now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for window, rows in listed.items():
        if window not in set(grid):
            continue
        key = window[0].isoformat()
        best = int((seen_windows.get(key) or {}).get("max") or 0)
        if best >= 20 and len(rows) < 0.9 * best and window not in new_relist and window not in new_gaps:
            new_relist.extend(windows(window[0], window[1], RETRY_DAYS))
            print(f"  {window[0]} .. {window[1]}: {len(rows)} against {best} before, listed again next run",
                  flush=True)
        seen_windows[key] = {"at": stamp_now, "max": max(best, len(rows))}
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
    rejected: list = list(doc.get("rejected_listing_samples") or [])[-10:]
    for raw, from_gap in listing:
        item = cr.listing_row(raw)
        if item is None:
            # A listed filing the reader cannot use: kept so it can be seen.
            if str(raw.get("symbol") or "").upper() in known and len(rejected) < 20:
                rejected.append({k: raw.get(k) for k in ("symbol", "consolidated", "qe_Date", "creation_Date", "xbrl")})
            continue
        if item["symbol"] not in known:
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
    # Companies that reported this season first, all their filings: a card on
    # the page needs the previous and year-ago quarters as well as the new one
    # (ANANDRATHI declared 2026-10-09 and showed no YoY while its Sep 2025
    # filing waited behind 5,000 others). Filings that keep failing go last.
    recent_cut = (today - timedelta(days=RECENT_DAYS)).isoformat()
    recent = {q["symbol"] for q in queue.values() if q["filed"] >= recent_cut}
    for symbol, entry in symbols.items():
        for field in cr.ROW_KEYS.values():
            if any(str(row.get("filed") or "") >= recent_cut for row in entry.get(field) or []):
                recent.add(symbol)
    pending.sort(key=lambda q: (int(q.get("attempts") or 0) >= MAX_ATTEMPTS, q["symbol"] not in recent))
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
        "relist_gaps": [[a.isoformat(), b.isoformat()] for a, b in new_relist],
        "window_seen": dict(sorted(seen_windows.items())),
        "rejected_listing_samples": rejected[-20:],
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
