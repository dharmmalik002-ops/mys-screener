#!/usr/bin/env python3
"""The day's quarterly results from BSE -> data/results_feed.json (Results page).

    python3 scripts/update_results_feed.py            # since the last run (first run: 45 days)
    python3 scripts/update_results_feed.py --days 10  # a fixed look-back
    python3 scripts/update_results_feed.py --if-due   # skip when a run read BSE in the last 20 minutes

Run by .github/workflows/quarterly-results.yml every half hour from 8 AM to
midnight IST, so a result declared during the day is on the page within about
half an hour. Each run:

1. reads BSE's "Result" announcements since the last successful run (one day
   of overlap), which says WHO declared and WHEN, to the minute;
2. for each of those companies (and any still awaiting figures) lists its
   filed quarters and fetches only quarters neither file holds yet;
3. records the declared time against the quarter it announces
   (`results_feed.apply_filing`) — or marks it pending when BSE has the
   filing but not yet the structured numbers, and retries on the next run.

It writes only `results_feed.json`; `quarterly_summary.json` belongs to the
bhavcopy job (see services/results_feed.py for why). BSE answers only a browser
TLS fingerprint (gotcha 159), so this needs curl_cffi like the other BSE jobs.
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
from app.services import results_feed as rf  # noqa: E402
from app.services.exchange_http import browser_session  # noqa: E402

API = "https://api.bseindia.com/BseIndiaAPI/api/"
ANN_URL = API + "AnnSubCategoryGetData/w"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}
IST = timezone(timedelta(hours=5, minutes=30))
FIRST_RUN_DAYS = 45
MAX_LOOKBACK_DAYS = 45
# Beside the day's read, each run reads one earlier stretch until every filing
# since the previous quarter's season began has been read, so the season that
# just closed is dated from BSE itself even where the calendar has holes (it
# missed most of September 2026 while BSE refused the runners, gotcha 159).
BACKFILL_CHUNK_DAYS = 28
WINDOW_DAYS = 14  # BSE's announcement search answers at most a fortnight

_local = threading.local()


def _now() -> datetime:
    return datetime.now(IST)


def _session():
    if not hasattr(_local, "session"):
        _local.session = browser_session(HEADERS)
        try:  # cookies, as the announcements page sets them for a browser
            _local.session.get("https://www.bseindia.com/corporates/ann.html", timeout=15)
        except Exception:  # noqa: BLE001 - priming only
            pass
    return _local.session


def _get(url: str, params: dict) -> dict | None:
    for attempt in range(4):
        try:
            response = _session().get(url, params=params, timeout=40)
            if response.status_code == 200 and response.text.strip().startswith(("{", "[")):
                return response.json()
        except Exception:  # noqa: BLE001 - one bad call must not stop the run
            pass
        time.sleep(1.5 * (attempt + 1))
    return None


def fetch_filings(start: date, end: date) -> list[tuple[str, datetime]] | None:
    """(scrip code, filing time IST) for every "Result" filing, or None if any
    window failed — a partial read must not advance `filings_through`."""
    out: list[tuple[str, datetime]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=WINDOW_DAYS - 1))
        base = {"strCat": "Result", "strPrevDate": cursor.strftime("%Y%m%d"),
                "strToDate": stop.strftime("%Y%m%d"), "strSearch": "P", "strscrip": "", "strType": "C"}
        total = None
        rows = 0
        for page in range(1, 400):
            payload = _get(ANN_URL, {**base, "pageno": str(page)})
            if payload is None:
                return None
            table = payload.get("Table") or []
            if total is None:
                total = int(((payload.get("Table1") or [{}])[0]).get("ROWCNT") or 0)
            for item in table:
                stamp = str(item.get("NEWS_DT") or item.get("DT_TM") or "")
                try:
                    when = datetime.fromisoformat(stamp[:19])
                except ValueError:
                    continue
                out.append((str(item.get("SCRIP_CD") or ""), when))
            rows += len(table)
            if not table or rows >= total:
                break
        print(f"  filings {cursor} .. {stop}: {rows}", flush=True)
        cursor = stop + timedelta(days=1)
    return out


# Stamps every run rewrites; a run that changes nothing else writes nothing.
VOLATILE_KEYS = ("generated_at", "filings_checked_at")


def _unchanged(before: dict, after: dict) -> bool:
    strip = lambda doc: {k: v for k, v in doc.items() if k not in VOLATILE_KEYS}  # noqa: E731
    return bool(before) and json.dumps(strip(before), sort_keys=True) == json.dumps(strip(after), sort_keys=True)


def backfill_window(feed: dict, today: date) -> tuple[date, date] | None:
    """The next earlier stretch to read, or None once every filing since the
    previous quarter's season began has been read (`filings_from`)."""
    current = rf.season_start(today)
    target = rf.season_start(current - timedelta(days=1))
    read_from = feed.get("filings_from")
    if read_from is None:
        through = feed.get("filings_through")
        if through is None:
            return None  # the first run reads its own look-back first
        read_from = (date.fromisoformat(through) - timedelta(days=1)).isoformat()
    first = date.fromisoformat(read_from)
    if first <= target:
        return None
    return max(target, first - timedelta(days=BACKFILL_CHUNK_DAYS)), first - timedelta(days=1)


def process(symbol: str, code: str, filings: list[datetime], feed: dict, known: set[str]) -> tuple[str, list[dict], dict[str, int], bool, list[str]]:
    """List the company's quarters, fetch missing ones, and decide each filing.
    Works on a private copy of the symbol's feed slices; merged by the caller."""
    listing = _get(API + "CorprateResultbeta/w", {"scripcode": code})
    if listing is None:
        return symbol, [], {}, False, []
    wanted = bq.quarter_codes(listing)[: bq.QUARTERS_KEPT]
    listed = {period for period, _qtr, _url in wanted}
    outcome: dict[str, int] = {}
    for filed in sorted(filings):
        verdict = rf.apply_filing(feed, symbol, filed, listed)
        outcome[verdict] = outcome.get(verdict, 0) + 1
    rows: list[dict] = []
    unparsed: list[str] = []
    # The newest eight quarters cover QoQ, YoY and both trailing years for PE.
    for period, quarter_code, url in wanted[:8]:
        if period in known:
            continue
        detail = _get(API + "Corp_detailedResult_Transpose_ng/w", {"Scrip_cd": code, "Qtr": quarter_code})
        if detail is None:
            continue
        table = detail.get("table1") or []
        parsed = bq.parse_quarter(table, period, url)
        if parsed is None and table and not unparsed:
            # A schedule the parser does not read (no revenue line it knows):
            # its line names are kept so the schedule can be added.
            unparsed = [str(item.get("fld_desc") or "") for item in table][:60]
        if parsed is not None:
            parsed.pop("result_document_url", None)
            parsed["qtr"] = quarter_code
            parsed["v"] = 2
            rows.append(parsed)
    return symbol, rows, outcome, True, unparsed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=0, help="look back this many days instead of since the last run")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--if-due", action="store_true")
    args = parser.parse_args()

    path = bq.FEED_PATH
    feed: dict = {}
    if path.exists():
        try:
            feed = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            feed = {}
    previous = json.loads(json.dumps(feed))
    now = _now()
    today = now.date()
    due = not args.if_due or rf.run_due(feed.get("filings_checked_at"), now)
    backfill = backfill_window(feed, today)
    if not due and backfill is None:
        print(f"not due: filings already checked at {feed.get('filings_checked_at')}")
        return 0

    if args.days:
        start = today - timedelta(days=args.days)
    else:
        through = feed.get("filings_through")
        start = date.fromisoformat(through) - timedelta(days=1) if through else today - timedelta(days=FIRST_RUN_DAYS)
    start = max(start, today - timedelta(days=MAX_LOOKBACK_DAYS))

    universe = json.loads((BACKEND_ROOT / "data" / "free_universe.json").read_text(encoding="utf-8"))
    by_code = {str(r["bse_code"]): str(r["symbol"]).upper() for r in universe if r.get("symbol") and r.get("bse_code")}
    code_of = {sym: code for code, sym in by_code.items()}
    summary = (bq.read_cached(bq.ARTIFACT_PATH).get("symbols") or {})

    filings: list[tuple[str, datetime]] | None = []
    if due:
        print(f"reading BSE result filings {start} .. {today}", flush=True)
        filings = fetch_filings(start, today)
    read_ok = filings is not None
    backfill_ok = False
    if backfill is not None:
        print(f"reading earlier filings {backfill[0]} .. {backfill[1]} (season backfill)", flush=True)
        earlier = fetch_filings(*backfill)
        backfill_ok = earlier is not None
        filings = (filings or []) + (earlier or [])
    per_symbol: dict[str, list[datetime]] = {}
    for code, when in filings or []:
        symbol = by_code.get(code)
        if symbol:
            per_symbol.setdefault(symbol, []).append(when)
    # Companies whose figures were not out yet last time: retry from the
    # filing time already recorded.
    for symbol, periods in (feed.get("pending") or {}).items():
        for stamp in periods.values():
            try:
                per_symbol.setdefault(symbol, []).append(datetime.fromisoformat(stamp))
            except ValueError:
                continue
    print(f"{len(per_symbol)} companies in the universe filed results (read ok: {read_ok})", flush=True)

    feed.setdefault("quarters", {})
    lock = threading.Lock()
    totals: dict[str, int] = {}
    fetched = failed = 0

    def known_periods(symbol: str) -> set[str]:
        _code, rows = bq.stored_quarters(symbol)
        held = {row["period"] for row in rows}
        held |= {row["period"] for row in ((feed["quarters"].get(symbol) or {}).get("quarters") or [])}
        return held

    jobs = [(s, code_of[s], times) for s, times in per_symbol.items() if s in code_of]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        local_of: dict = {}
        for symbol, code, times in jobs:
            # Each worker decides against its own copy of this symbol's slices.
            local = {
                "declared": {symbol: dict((feed.get("declared") or {}).get(symbol) or {})},
                "pending": {symbol: dict((feed.get("pending") or {}).get(symbol) or {})},
            }
            local_of[pool.submit(process, symbol, code, times, local, known_periods(symbol))] = local
        for n, future in enumerate(as_completed(local_of), start=1):
            symbol, rows, outcome, ok, unparsed = future.result()
            local = local_of[future]
            with lock:
                if unparsed:
                    samples = [x for x in feed.get("unparsed_samples") or [] if x.get("symbol") != symbol]
                    feed["unparsed_samples"] = (samples + [{"symbol": symbol, "names": unparsed}])[-10:]
                if not ok:
                    failed += 1
                    continue
                for verdict, count in outcome.items():
                    totals[verdict] = totals.get(verdict, 0) + count
                declared = (local.get("declared") or {}).get(symbol) or {}
                if declared:
                    feed.setdefault("declared", {})[symbol] = declared
                pending = (local.get("pending") or {}).get(symbol) or {}
                feed.setdefault("pending", {})
                if pending:
                    feed["pending"][symbol] = pending
                else:
                    feed["pending"].pop(symbol, None)
                if rows:
                    fetched += len(rows)
                    entry = feed["quarters"].setdefault(symbol, {"bse": code_of[symbol], "quarters": []})
                    have = {row["period"] for row in entry["quarters"]}
                    entry["quarters"] = sorted(
                        entry["quarters"] + [bq.compact_row(r) for r in rows if r["period"] not in have],
                        key=lambda row: bq._period_key(row["period"]),
                        reverse=True,
                    )
            if n % 100 == 0 or n == len(local_of):
                print(f"[{n}/{len(local_of)}] quarters fetched={fetched} failed={failed} {totals}", flush=True)

    rf.prune(feed, summary, today)
    stamp = datetime.now(timezone.utc).isoformat()
    feed["generated_at"] = stamp
    if read_ok and due:
        feed["filings_checked_at"] = stamp
        feed["filings_through"] = today.isoformat()
        if start.isoformat() < str(feed.get("filings_from") or "9999"):
            feed["filings_from"] = start.isoformat()
    if backfill is not None and backfill_ok:
        feed["filings_from"] = backfill[0].isoformat()
    feed["source"] = "BSE Result filings (declared time) + standalone results (Rs million / 10 = crore)"
    ordered = {key: feed[key] for key in ("generated_at", "filings_checked_at", "filings_through", "filings_from",
                                          "source", "unparsed_samples") if key in feed}
    for key in ("declared", "pending", "quarters"):
        ordered[key] = dict(sorted((feed.get(key) or {}).items()))
    if _unchanged(previous, ordered):
        # Nothing declared, dated or fetched since the last write: leave the
        # file alone, so a half-hourly run that found nothing makes no commit.
        print("nothing new since the last run: results_feed.json left as it was", flush=True)
        return 0 if read_ok and (backfill is None or backfill_ok) else 1
    path.write_text(json.dumps(ordered, separators=(",", ":"), sort_keys=False), encoding="utf-8")
    print(f"wrote {path.name}: {len(ordered['declared'])} companies dated, "
          f"{len(ordered['pending'])} awaiting figures, {len(ordered['quarters'])} with fresh quarters, "
          f"{path.stat().st_size / 1024:.0f} KB", flush=True)
    return 0 if read_ok and (backfill is None or backfill_ok) else 1


if __name__ == "__main__":
    raise SystemExit(main())
