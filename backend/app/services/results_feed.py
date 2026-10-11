"""The Results page: every company's latest quarter, by the day it was declared.

Figures come from `bse_quarterly` (BSE's own standalone filings: the committed
`quarterly_summary.json`, rewritten by the 4:23 PM bhavcopy job, with the
evening `results_feed.json` laid over it). What this module adds is the date
each quarter was DECLARED, which neither BSE's results listing nor the
summary carries:

1. `results_feed.json["declared"]` — the first BSE "Result" filing the evening
   job (`scripts/update_results_feed.py`, 7 PM IST and two late runs) matched
   to the quarter. Exact to the minute.
2. Otherwise `results_calendar.json`, the bot's results calendar from the same
   BSE feed, which goes back to 2011 — so older seasons have dates too.

A quarter with neither is still in the data (the chart's Earnings widget shows
it) but cannot be placed on a day, so it is left off this page rather than
dated by guess.

Why a separate evening file: the bhavcopy job already rewrites
`quarterly_summary.json`, and two workflows committing the same file collide
on `git pull --rebase` — which would put the day's price patch at risk for a
page of results. The feed is only ever written by its own workflow.

PE is computed here rather than read: market cap over the latest four quarters'
net profit. It is STANDALONE, like every figure on the page, so a holding
company or a group with large subsidiaries reads differently from a site that
shows consolidated results.
"""

from __future__ import annotations

import calendar
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from app.services import bse_quarterly
from app.services.quarterly_growth import growth_pct, period_end

logger = logging.getLogger(__name__)

DATA_DIR = bse_quarterly.ARTIFACT_PATH.parent
CALENDAR_PATH = DATA_DIR / "results_calendar.json"
UPCOMING_PATH = DATA_DIR / "earnings_calendar.json"

# A results filing this soon after a quarter ends is about the quarter before
# it: nobody closes their books in four days, but a late filer's previous
# quarter often lands then.
MIN_DAYS_AFTER_QUARTER = 5
# Listed companies must declare within 45 days (60 for the March quarter);
# 150 allows for extensions and late filers without reaching the next season.
MAX_DAYS_AFTER_QUARTER = 150
# A filing whose numbers BSE has not structured yet is retried this long.
PENDING_DAYS = 15
MAX_RANGE_DAYS = 200
# SEBI's ranks: 1-100 large, 101-250 mid, 251+ small. The universe stops at
# Rs 500 cr, so "micro" here is rank 501 onward — smaller small caps, not
# SEBI's term.
CAP_BUCKETS = (("large", 100), ("mid", 250), ("small", 500), ("micro", None))
METRICS = (
    ("sales", "sales_crore"),
    ("operating_profit", "operating_profit_crore"),
    ("net_profit", "net_profit_crore"),
    ("eps", "eps"),
)


# ── Which quarter a filing announces ────────────────────────────────────────

def _quarter_label(end: date) -> str:
    return f"{calendar.month_abbr[end.month]} {end.year}"


def target_period(filed: datetime | date) -> str:
    """The quarter a results filing made on `filed` most likely announces:
    the newest quarter that ended at least `MIN_DAYS_AFTER_QUARTER` earlier."""
    day = filed.date() if isinstance(filed, datetime) else filed
    cutoff = day - timedelta(days=MIN_DAYS_AFTER_QUARTER)
    for month in (12, 9, 6, 3):
        end = date(cutoff.year, month, calendar.monthrange(cutoff.year, month)[1])
        if end <= cutoff:
            return _quarter_label(end)
    return _quarter_label(date(cutoff.year - 1, 12, 31))


_declared_cache: tuple[int, dict[str, dict[str, datetime]]] | None = None


def _calendar_declared_map(symbol: str) -> dict[str, datetime]:
    """period -> first results filing announcing it, from the calendar.
    Parsed once per symbol per calendar file."""
    global _declared_cache
    raw = bse_quarterly.read_cached(CALENDAR_PATH)
    if _declared_cache is None or _declared_cache[0] != id(raw):
        _declared_cache = (id(raw), {})
    table = _declared_cache[1]
    key = symbol.upper()
    if key not in table:
        events: list[datetime] = []
        for row in raw.get(key) or []:
            try:
                events.append(datetime.fromisoformat(row[0]) + timedelta(minutes=int(row[1])))
            except (TypeError, ValueError, IndexError):
                continue
        first: dict[str, datetime] = {}
        for event in sorted(events):
            period = target_period(event)
            end = period_end(period)
            if end is not None and (event.date() - end).days <= MAX_DAYS_AFTER_QUARTER:
                first.setdefault(period, event)
        table[key] = first
    return table[key]


def calendar_declared(symbol: str, period: str) -> datetime | None:
    """First results filing in the calendar that announces `period`."""
    return _calendar_declared_map(symbol).get(period)


def _parse_stamp(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def declared_at(symbol: str, period: str, feed: dict[str, Any] | None = None) -> datetime | None:
    feed = bse_quarterly.load_feed() if feed is None else feed
    stamp = _parse_stamp(((feed.get("declared") or {}).get(symbol.upper()) or {}).get(period))
    return stamp or calendar_declared(symbol, period)


def apply_filing(
    feed: dict[str, Any],
    symbol: str,
    filed: datetime,
    listed_periods: Iterable[str],
) -> str:
    """Record one results filing against the quarter it announces.

    Returns "declared" when the quarter is in BSE's structured listing (its
    numbers can be fetched), "pending" when it is not yet, and "ignored" when
    the quarter already has an earlier declared date — a limited-review report
    or press release filed days after the results is not the announcement.
    """
    period = target_period(filed)
    end = period_end(period)
    if end is None or (filed.date() - end).days > MAX_DAYS_AFTER_QUARTER:
        return "ignored"
    key = symbol.upper()
    declared = feed.setdefault("declared", {}).setdefault(key, {})
    earlier = [s for s in (_parse_stamp(declared.get(period)), calendar_declared(key, period)) if s]
    if earlier and min(earlier) < filed:
        if period not in declared:
            declared[period] = min(earlier).isoformat(timespec="minutes")
        _clear_pending(feed, key, period)
        return "ignored"
    if period not in set(listed_periods):
        pending = feed.setdefault("pending", {}).setdefault(key, {})
        stamp = _parse_stamp(pending.get(period))
        if stamp is None or filed < stamp:
            pending[period] = filed.isoformat(timespec="minutes")
        return "pending"
    declared[period] = filed.isoformat(timespec="minutes")
    _clear_pending(feed, key, period)
    return "declared"


def _clear_pending(feed: dict[str, Any], symbol: str, period: str) -> None:
    pending = (feed.get("pending") or {}).get(symbol)
    if pending and period in pending:
        pending.pop(period, None)
        if not pending:
            feed["pending"].pop(symbol, None)


def prune(feed: dict[str, Any], summary_symbols: dict[str, Any], today: date) -> None:
    """Drop what the summary has caught up with, stale pending filings, and
    declared dates older than two years (the calendar covers those)."""
    for symbol, entry in list((feed.get("quarters") or {}).items()):
        held = {
            (row.get("period"), row.get("qtr"))
            for row in ((summary_symbols.get(symbol) or {}).get("quarters") or [])
        }
        kept = [row for row in entry.get("quarters") or [] if (row.get("period"), row.get("qtr")) not in held]
        if kept:
            entry["quarters"] = kept
        else:
            feed["quarters"].pop(symbol)
    for symbol, periods in list((feed.get("pending") or {}).items()):
        for period, stamp in list(periods.items()):
            when = _parse_stamp(stamp)
            if when is None or (today - when.date()).days > PENDING_DAYS:
                periods.pop(period)
        if not periods:
            feed["pending"].pop(symbol)
    for symbol, periods in list((feed.get("declared") or {}).items()):
        for period in list(periods):
            end = period_end(period)
            if end is None or (today - end).days > 730:
                periods.pop(period)
        if not periods:
            feed["declared"].pop(symbol)


# ── The page ────────────────────────────────────────────────────────────────

def _shift(label: str, months: int) -> str | None:
    end = period_end(label)
    if end is None:
        return None
    index = end.year * 12 + (end.month - 1) + months
    year, month = divmod(index, 12)
    return f"{calendar.month_abbr[month + 1]} {year}"


def cap_buckets(snapshots: Iterable[Any]) -> dict[str, str]:
    ranked = sorted(
        (s for s in snapshots if (getattr(s, "market_cap_crore", 0) or 0) > 0),
        key=lambda s: s.market_cap_crore,
        reverse=True,
    )
    out: dict[str, str] = {}
    for rank, snap in enumerate(ranked, start=1):
        for name, limit in CAP_BUCKETS:
            if limit is None or rank <= limit:
                out[snap.symbol.upper()] = name
                break
    return out


def _ttm(by_period: dict[str, dict[str, Any]], latest: str, back: int = 0) -> float | None:
    total = 0.0
    for step in range(4):
        row = by_period.get(_shift(latest, -3 * (step + back)) or "")
        value = row.get("net_profit_crore") if row else None
        if value is None:
            return None
        total += value
    return total


def valuation(rows: list[dict[str, Any]], market_cap_crore: float | None) -> tuple[float | None, float | None]:
    """(PE, PEG) on the newest four quarters: PE is market cap over trailing
    net profit, PEG is PE over the trailing profit's growth on the four
    quarters before. None where profit or its growth is not positive."""
    if not rows or not market_cap_crore:
        return None, None
    by_period = {row["period"]: row for row in rows if row.get("period")}
    latest = rows[0]["period"]
    ttm = _ttm(by_period, latest)
    if ttm is None or ttm <= 0:
        return None, None
    pe = round(market_cap_crore / ttm, 2)
    growth = growth_pct(ttm, _ttm(by_period, latest, back=4))
    peg = round(pe / growth, 2) if growth is not None and growth > 0 else None
    return pe, peg


def _metric_block(by_period: dict[str, dict[str, Any]], period: str, key: str) -> dict[str, Any]:
    prev_label, year_label = _shift(period, -3), _shift(period, -12)
    current = (by_period.get(period) or {}).get(key)
    prev = (by_period.get(prev_label or "") or {}).get(key)
    year_ago = (by_period.get(year_label or "") or {}).get(key)
    return {
        "value": current,
        "prev": prev,
        "year_ago": year_ago,
        "yoy_pct": growth_pct(current, year_ago),
        "qoq_pct": growth_pct(current, prev),
    }


def result_entry(
    symbol: str,
    period: str,
    declared: datetime,
    rows: list[dict[str, Any]],
    snapshot: Any,
    bucket: str | None,
) -> dict[str, Any] | None:
    by_period = {row["period"]: row for row in rows if row.get("period")}
    current = by_period.get(period)
    if current is None:
        return None
    market_cap = getattr(snapshot, "market_cap_crore", None)
    # Today's valuation, whichever quarter the entry is: what PE means to a
    # reader looking at the page now.
    pe, peg = valuation(rows, market_cap)
    return {
        "symbol": symbol,
        "name": getattr(snapshot, "name", symbol),
        "sector": getattr(snapshot, "sector", None),
        "sub_sector": getattr(snapshot, "sub_sector", None),
        "market_cap_crore": market_cap,
        "cap_bucket": bucket,
        "declared_at": declared.isoformat(timespec="minutes"),
        "declared_date": declared.date().isoformat(),
        "period": period,
        "prev_period": _shift(period, -3),
        "year_ago_period": _shift(period, -12),
        "metrics": {name: _metric_block(by_period, period, key) for name, key in METRICS},
        "operating_margin_pct": current.get("operating_margin_pct"),
        "pe": pe,
        "peg": peg,
        "result_url": current.get("result_document_url"),
    }


def build_quarterly(snapshots: list[Any], date_from: date, date_to: date) -> dict[str, Any]:
    if date_to < date_from:
        date_from, date_to = date_to, date_from
    if (date_to - date_from).days > MAX_RANGE_DAYS:
        date_from = date_to - timedelta(days=MAX_RANGE_DAYS)
    feed = bse_quarterly.load_feed()
    by_symbol = {s.symbol.upper(): s for s in snapshots}
    buckets = cap_buckets(snapshots)
    items: list[dict[str, Any]] = []
    # A quarter declared inside the range ended at most MAX_DAYS before it.
    oldest_end = date_from - timedelta(days=MAX_DAYS_AFTER_QUARTER)
    for symbol in sorted(bse_quarterly.symbols()):
        snapshot = by_symbol.get(symbol)
        if snapshot is None:
            continue
        rows = bse_quarterly.results_for(symbol)
        for row in rows[:3]:
            period = row.get("period")
            end = period_end(period) if period else None
            if end is None or end < oldest_end or end > date_to:
                continue
            declared = declared_at(symbol, period, feed)
            if declared is None or not (date_from <= declared.date() <= date_to):
                continue
            entry = result_entry(symbol, period, declared, rows, snapshot, buckets.get(symbol))
            if entry:
                items.append(entry)
    items.sort(key=lambda item: (item["declared_at"], item["market_cap_crore"] or 0), reverse=True)
    pending = sorted(
        symbol for symbol, periods in (feed.get("pending") or {}).items()
        if symbol in by_symbol and any(
            date_from <= (when.date() if (when := _parse_stamp(s)) else date.min) <= date_to
            for s in periods.values()
        )
    )
    return {
        "from": date_from.isoformat(),
        "to": date_to.isoformat(),
        "items": items,
        "awaiting_figures": [
            {"symbol": s, "name": getattr(by_symbol[s], "name", s)} for s in pending
        ],
        "summary_generated_at": bse_quarterly.read_cached(bse_quarterly.ARTIFACT_PATH).get("generated_at"),
        "feed_checked_at": feed.get("filings_checked_at"),
        "basis": "standalone",
    }


def build_upcoming(snapshots: list[Any], today: date, days: int = 30) -> dict[str, Any]:
    """Companies with a results board meeting ahead (BSE + NSE calendars,
    `earnings_calendar.json`), each with its last reported quarter."""
    payload = bse_quarterly.read_cached(UPCOMING_PATH)
    upcoming = payload.get("upcoming") or {}
    by_symbol = {s.symbol.upper(): s for s in snapshots}
    buckets = cap_buckets(snapshots)
    last_day = today + timedelta(days=days)
    items: list[dict[str, Any]] = []
    for symbol, day_text in upcoming.items():
        snapshot = by_symbol.get(str(symbol).upper())
        try:
            day = date.fromisoformat(str(day_text)[:10])
        except ValueError:
            continue
        if snapshot is None or not (today <= day <= last_day):
            continue
        rows = bse_quarterly.results_for(symbol)
        last = rows[0] if rows else None
        by_period = {row["period"]: row for row in rows if row.get("period")}
        pe, peg = valuation(rows, snapshot.market_cap_crore)
        items.append({
            "symbol": snapshot.symbol,
            "name": snapshot.name,
            "sector": snapshot.sector,
            "sub_sector": snapshot.sub_sector,
            "market_cap_crore": snapshot.market_cap_crore,
            "cap_bucket": buckets.get(snapshot.symbol.upper()),
            "date": day.isoformat(),
            "last_period": last.get("period") if last else None,
            "last_sales_yoy_pct": _metric_block(by_period, last["period"], "sales_crore")["yoy_pct"] if last else None,
            "last_profit_yoy_pct": _metric_block(by_period, last["period"], "net_profit_crore")["yoy_pct"] if last else None,
            "pe": pe,
            "peg": peg,
        })
    items.sort(key=lambda item: (item["date"], -(item["market_cap_crore"] or 0)))
    return {"items": items, "calendar_updated_at": payload.get("updated_at"), "days": days}


def feed_path() -> Path:
    return bse_quarterly.FEED_PATH


def data_stamp() -> tuple[float, ...]:
    """mtimes of every file the page reads — what a cached payload keys on."""
    out = []
    for path in (bse_quarterly.ARTIFACT_PATH, bse_quarterly.FEED_PATH, CALENDAR_PATH, UPCOMING_PATH):
        try:
            out.append(path.stat().st_mtime)
        except OSError:
            out.append(0.0)
    return tuple(out)


# ── Delivery to the Space ───────────────────────────────────────────────────
# The workflow commits with GITHUB_TOKEN and [skip ci], so nothing redeploys
# the Space: it pulls the feed itself, on a timer through the evening and when
# the page is opened (same reason as main.py::pull_latest_breakout_stats).

PULL_MIN_INTERVAL_S = 600
_pull_lock = threading.Lock()
_last_pull = 0.0


def pull_latest_feed(force: bool = False) -> bool:
    """Replace the local feed with the committed one when the committed one
    checked filings more recently. True when a newer file was written."""
    global _last_pull
    import requests

    with _pull_lock:
        now = time.monotonic()
        if not force and now - _last_pull < PULL_MIN_INTERVAL_S:
            return False
        _last_pull = now
    raw_base = (
        os.environ.get("COMMITTED_DATA_RAW_BASE")
        or "https://raw.githubusercontent.com/dharmmalik002-ops/mys-screener/main/backend/data"
    ).strip().rstrip("/")
    path = bse_quarterly.FEED_PATH
    try:
        local = str(bse_quarterly.load_feed().get("generated_at") or "")
        response = requests.get(f"{raw_base}/{path.name}", timeout=25)
        if response.status_code != 200:
            logger.info("Results feed pull: remote HTTP %s — keeping local %s", response.status_code, local or "<none>")
            return False
        remote = response.json()
        stamp = str(remote.get("generated_at") or "") if isinstance(remote, dict) else ""
        if not stamp or not isinstance(remote.get("declared"), dict):
            logger.warning("Results feed pull: remote payload invalid — keeping local")
            return False
        if local and stamp <= local:
            return False
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(response.text, encoding="utf-8")
        tmp.replace(path)
        logger.info("Results feed pull: %s (was %s)", stamp, local or "<none>")
        return True
    except Exception as exc:  # noqa: BLE001 - a failed pull keeps the local copy
        logger.warning("Results feed pull failed: %s", exc)
        return False


def pull_in_background() -> None:
    if time.monotonic() - _last_pull >= PULL_MIN_INTERVAL_S:
        threading.Thread(target=pull_latest_feed, daemon=True, name="results-feed-pull").start()


def today_ist() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()


def season_start(day: date) -> date:
    """First day of `day`'s calendar quarter — the start of the results
    season running then (Oct 1 for the September quarter's results)."""
    return date(day.year, ((day.month - 1) // 3) * 3 + 1, 1)


# Evening slots (IST) for the results job. Shared by the workflow's --if-due
# check and the Space's watchdog so both agree on when a run is owed.
SLOTS = ((19, 0), (21, 30), (23, 45))


def latest_slot(now_ist: datetime) -> datetime | None:
    passed = [now_ist.replace(hour=h, minute=m, second=0, microsecond=0) for h, m in SLOTS]
    passed = [slot for slot in passed if slot <= now_ist]
    return passed[-1] if passed else None


def slot_due(filings_checked_at: Any, now_ist: datetime) -> bool:
    """True when no run has read BSE's filings since the latest slot that has
    passed today (a daytime manual run is always due)."""
    slot = latest_slot(now_ist)
    if slot is None:
        return True
    try:
        checked = datetime.fromisoformat(str(filings_checked_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return True
    if checked.tzinfo is None:
        checked = checked.replace(tzinfo=timezone.utc)
    return checked < slot
