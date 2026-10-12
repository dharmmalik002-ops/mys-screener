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


def fetch_filings(start: date, end: date, category: str = "Result") -> list[tuple[str, datetime, str]] | None:
    """(scrip code, filing time IST, attachment) for every filing in
    `category`, or None if any window failed — a partial read must not
    advance `filings_through`."""
    out: list[tuple[str, datetime]] = []
    cursor = start
    while cursor <= end:
        stop = min(end, cursor + timedelta(days=WINDOW_DAYS - 1))
        base = {"strCat": category, "strPrevDate": cursor.strftime("%Y%m%d"),
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
                out.append((str(item.get("SCRIP_CD") or ""), when, str(item.get("ATTACHMENTNAME") or "").strip()))
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


DATE_LOOKUPS_PER_RUN = 25
RESULT_SUBJECT = ("financial result", "financial results", "results for the quarter", "unaudited", "audited")


def find_declared_dates(feed: dict, summary: dict, code_of: dict, today: date) -> None:
    """A quarter with figures but no declared time: its Result filing was not
    under BSE's "Result" category (FRONTSP, KESAR, TANFACIND, WPIL, BLUECLOUDS
    for June 2026). Ask BSE for that one company's announcements of every
    category in the quarter's results window and take the earliest whose
    subject names financial results — the real filing time. Each (company,
    quarter) is looked up once (`date_lookups`)."""
    lookups = feed.setdefault("date_lookups", {})
    done = 0
    for symbol, entry in sorted(summary.items()):
        if done >= DATE_LOOKUPS_PER_RUN or symbol not in code_of:
            continue
        rows = entry.get("quarters") or []
        if not rows:
            continue
        period = rows[0].get("period")
        end = rf.period_end(period) if period else None
        if end is None or (today - end).days > rf.MAX_DAYS_AFTER_QUARTER or (today - end).days < 5:
            continue
        if rf.declared_at(symbol, period, feed) or period in (lookups.get(symbol) or {}):
            continue
        done += 1
        start, stop = end + timedelta(days=1), min(today, end + timedelta(days=rf.MAX_DAYS_AFTER_QUARTER))
        found = None
        cursor = start
        while cursor <= stop and found is None:
            window_end = min(stop, cursor + timedelta(days=WINDOW_DAYS - 1))
            payload = _get(ANN_URL, {"strCat": "-1", "strPrevDate": cursor.strftime("%Y%m%d"),
                                     "strToDate": window_end.strftime("%Y%m%d"), "strSearch": "P",
                                     "strscrip": code_of[symbol], "strType": "C", "pageno": "1"})
            hits = []
            for item in (payload or {}).get("Table") or []:
                subject = f"{item.get('NEWSSUB') or ''} {item.get('HEADLINE') or ''} {item.get('SUBCATNAME') or ''}".lower()
                if not any(word in subject for word in RESULT_SUBJECT) or "intimation" in subject:
                    continue
                try:
                    hits.append(datetime.fromisoformat(str(item.get("NEWS_DT") or item.get("DT_TM") or "")[:19]))
                except ValueError:
                    continue
            hits = [h for h in hits if rf.target_period(h) == period]
            if hits:
                found = min(hits)
            cursor = window_end + timedelta(days=1)
        lookups.setdefault(symbol, {})[period] = found.isoformat(timespec="minutes") if found else "none"
        if found:
            feed.setdefault("declared", {}).setdefault(symbol, {})[period] = found.isoformat(timespec="minutes")
            print(f"  {symbol} {period}: declared {found:%Y-%m-%d %H:%M} (found by company search)", flush=True)
    # Keep only lookups that can still matter.
    for symbol in list(lookups):
        for period in list(lookups[symbol]):
            end = rf.period_end(period)
            if end is None or (today - end).days > rf.MAX_DAYS_AFTER_QUARTER:
                lookups[symbol].pop(period)
        if not lookups[symbol]:
            lookups.pop(symbol)


ATTACH_URLS = ("https://www.bseindia.com/xml-data/corpfiling/AttachLive/{name}",
                "https://www.bseindia.com/xml-data/corpfiling/AttachHis/{name}")
MAX_DOCS = 6
PDF_KEEP_DAYS = 45


_download_notes: dict[str, str] = {}


def _download_pdf(name: str) -> bytes | None:
    notes = []
    for template in ATTACH_URLS:
        try:
            response = _session().get(template.format(name=name), timeout=60)
        except Exception as exc:  # noqa: BLE001 - try the archive copy, then give up
            notes.append(type(exc).__name__)
            continue
        if response.status_code == 200 and response.content[:5] == b"%PDF-":
            return response.content
        notes.append(f"{response.status_code} {response.headers.get('Content-Type', '')} {response.content[:12]!r}")
    _download_notes[name] = "; ".join(notes)
    return None


CHECK_KEYS = ("sales_crore", "operating_profit_crore", "net_profit_crore", "eps")


def _record_check(feed: dict, symbol: str, entry: dict, fresh: list, cr) -> None:
    """When the exchange's figures replace a PDF reading, keep how far apart
    they were: the running measure of whether the reader can be trusted."""
    period = entry.get("period")
    exchange = {
        "standalone": next((r for r in fresh if r.get("period") == period), None) or next(
            (r for r in rf.standalone_rows(symbol) if r["period"] == period and r.get("source") != "pdf"), None),
        "consolidated": next((r for r in cr.results_for(symbol) if r["period"] == period), None),
    }
    gaps = {}
    for basis in ("standalone", "consolidated"):
        read, truth = entry.get(basis), exchange.get(basis)
        if not read or not truth:
            continue
        for key in CHECK_KEYS:
            a, b = read.get(key), truth.get(key)
            if a is None or b is None:
                continue
            gaps[f"{basis}.{key}"] = round(a - b, 2)
    checks = feed.setdefault("pdf_checks", [])
    record = {"symbol": symbol, "period": period, "document": entry.get("document"), "gaps": gaps,
              "agrees": all(abs(v) <= 0.6 for k, v in gaps.items() if not k.endswith("eps"))
              and all(abs(v) <= 0.011 for k, v in gaps.items() if k.endswith("eps"))}
    checks[:] = [c for c in checks if (c.get("symbol"), c.get("period")) != (symbol, period)] + [record]
    del checks[:-40]


def read_result_pdfs(feed: dict, docs_of: dict[tuple[str, str], list[str]], today: date) -> None:
    """Read the figures from the results PDF of every filing still waiting
    for BSE's structured numbers (services/result_pdf.py checks each figure
    against the stored previous and year-ago quarters before taking it).
    The exchanges' own figures replace these the moment they exist."""
    from app.services import consolidated_results as cr
    from app.services import result_pdf

    if feed.get("pdf_reader") != result_pdf.READER_VERSION:
        feed.pop("pdf_tried", None)  # a new reader reads every PDF once more
        feed["pdf_reader"] = result_pdf.READER_VERSION
    docs = feed.setdefault("result_docs", {})
    for (symbol, period), names in docs_of.items():
        kept = docs.setdefault(symbol, {}).setdefault(period, [])
        for name in names:
            if name not in kept:
                kept.append(name)
        del kept[MAX_DOCS:]
    pdfs = feed.setdefault("pdf_quarters", {})
    # Waiting for BSE's structured figures, or declared but with no figures
    # anywhere (BSE serves some NBFCs' and AMCs' schedules with no line items).
    targets = {(symbol, period) for symbol, periods in (feed.get("pending") or {}).items() for period in periods}
    for symbol, period in docs_of:
        fresh = ((feed.get("quarters") or {}).get(symbol) or {}).get("quarters") or []
        if not any(r.get("period") == period for r in fresh) and not any(
                r["period"] == period and r.get("source") != "pdf" for r in rf.standalone_rows(symbol)):
            targets.add((symbol, period))
    for symbol, period in sorted(targets):
        done = pdfs.get(symbol) or {}
        if done.get("period") == period and done.get("standalone") and done.get("consolidated"):
            continue
        # Each PDF is read once: a scanned or unmatched one is not
        # downloaded again every half hour while the filing waits.
        tried = feed.setdefault("pdf_tried", {}).setdefault(symbol, {}).setdefault(period, [])
        names = [n for n in (docs.get(symbol) or {}).get(period) or [] if n not in tried]
        if not names:
            continue
        prev_label, year_label = rf._shift(period, -3), rf._shift(period, -12)
        priors = {}
        for basis, rows in (("standalone", rf.standalone_rows(symbol)), ("consolidated", cr.results_for(symbol))):
            by_period = {row["period"]: row for row in rows if row.get("source") != "pdf"}
            priors[basis] = (by_period.get(prev_label), by_period.get(year_label))
        best: dict = {}
        best_name = None
        for name in names:
            pdf = _download_pdf(name)
            if pdf is None:
                # Not marked tried: a failed download is retried next run.
                samples = feed.setdefault("pdf_samples", [])
                note = {"symbol": symbol, "period": period, "document": name,
                        "download_failed": _download_notes.get(name, "")}
                if note not in samples:  # a repeat would make every half-hourly run commit
                    samples.append(note)
                    del samples[:-8]
                continue
            tried.append(name)
            text = result_pdf.extract_text(pdf)
            rows = result_pdf.read_quarter(text, period, priors)
            if not rows:
                # What the PDF held, so a statement the reader misses can be
                # seen and the reader fixed (never guessed at).
                samples = feed.setdefault("pdf_samples", [])
                samples.append({"symbol": symbol, "period": period, "document": name, "chars": len(text),
                                "lines": result_pdf.sample_lines(text),
                                "priors": {b: [r.get("sales_crore") if r else None for r in pr] for b, pr in priors.items()}})
                del samples[:-8]
            if len(rows) > len(best):
                best, best_name = rows, name
            if len(rows) == 2:
                break
        if best:
            pdfs[symbol] = {"period": period, "read_at": today.isoformat(), **best, "document": best_name}
            print(f"  {symbol} {period}: read from the results PDF ({', '.join(k for k in best if k != 'document')})",
                  flush=True)
    # Drop what the exchanges have caught up with, and anything old.
    for symbol, entry in list(pdfs.items()):
        period = entry.get("period")
        # This run's fresh quarters are in `feed` only (not yet on disk).
        fresh = ((feed.get("quarters") or {}).get(symbol) or {}).get("quarters") or []
        have_standalone = any(r.get("period") == period for r in fresh) or any(
            r["period"] == period and r.get("source") != "pdf" for r in rf.standalone_rows(symbol))
        have_consolidated = any(r["period"] == period for r in cr.results_for(symbol))
        stale = (today - date.fromisoformat(str(entry.get("read_at") or "1970-01-01"))).days > PDF_KEEP_DAYS
        if stale or (have_standalone and (not entry.get("consolidated") or have_consolidated)):
            if not stale:
                _record_check(feed, symbol, entry, fresh, cr)
            pdfs.pop(symbol)
    tried_all = feed.get("pdf_tried") or {}
    for symbol in list(tried_all):
        for period in list(tried_all[symbol]):
            if (symbol, period) not in targets:
                tried_all[symbol].pop(period)
        if not tried_all[symbol]:
            tried_all.pop(symbol)
    for symbol in list(docs):
        for period in list(docs[symbol]):
            if (symbol, period) not in targets and (pdfs.get(symbol) or {}).get("period") != period:
                docs[symbol].pop(period)
        if not docs[symbol]:
            docs.pop(symbol)


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
    docs_of: dict[tuple[str, str], list[str]] = {}
    for code, when, *rest in filings or []:
        symbol = by_code.get(code)
        if symbol:
            per_symbol.setdefault(symbol, []).append(when)
            if rest and rest[0]:
                docs_of.setdefault((symbol, rf.target_period(when)), []).append(rest[0])
    # The full statement is often attached to the board meeting's outcome
    # rather than to the "Result" filing (HCLTECH 2026-10-12: the Result
    # filing's PDF was not the statement). Their attachments are only read,
    # never used to date a quarter.
    if due and docs_of:
        meetings = fetch_filings(today - timedelta(days=1), today, "Board Meeting") or []
        for code, when, *rest in meetings:
            symbol = by_code.get(code)
            key = (symbol, rf.target_period(when))
            if symbol and rest and rest[0] and (key in docs_of or symbol in (feed.get("pending") or {})):
                docs_of.setdefault(key, []).append(rest[0])
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

    read_result_pdfs(feed, docs_of, today)
    if read_ok:
        find_declared_dates(feed, summary, code_of, today)
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
    for key in ("result_docs", "pdf_quarters", "pdf_tried", "date_lookups"):
        if not feed.get(key):
            feed.pop(key, None)
    if "pdf_tried" not in feed:
        feed.pop("pdf_reader", None)
    ordered = {key: feed[key] for key in ("generated_at", "filings_checked_at", "filings_through", "filings_from",
                                          "source", "unparsed_samples", "pdf_reader", "pdf_samples", "pdf_checks")
               if key in feed}
    for key in ("declared", "pending", "quarters", "result_docs", "pdf_quarters", "pdf_tried", "date_lookups"):
        if key not in feed and key in ("result_docs", "pdf_quarters", "pdf_tried", "date_lookups"):
            continue
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
