"""Fill a Nifty index series from NSE's own per-session closing file.

Yahoo refuses most Nifty sector indices from datacenter IPs — the Space *and*
the GitHub runners — so `sector_indices.json` kept the last bars a residential
machine had fetched: 13 of 16 sectors sat on 17 Jul 2026 for three months while
the nightly job "succeeded" by keeping what it had.

NSE publishes one CSV per session with the official open/high/low/close of
every index (`ind_close_all_DDMMYYYY.csv`, the same file `providers/free.py`
completes index charts from, gotcha 151). It is reachable from the runners with
plain `requests` and a browser User-Agent (gotcha 162), and a 404 means no
session that day (weekend, holiday, or a file not published yet).

A session is appended only when it joins the series: the file's own previous
close (close minus points change) must match our last close within
`JOIN_TOLERANCE`. A missing file in the middle of a run therefore stops the
extension rather than stitching two sessions together across a hole; the next
run picks up from there.
"""

from __future__ import annotations

import csv
import io
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

CLOSE_URL = "https://nsearchives.nseindia.com/content/indices/ind_close_all_{stamp}.csv"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
JOIN_TOLERANCE = 0.01
# Long enough to close the 3-month gap this module was written for; a series
# further behind than this is re-based or wrong and must come from a full fetch.
MAX_GAP_DAYS = 200
IST = timezone(timedelta(hours=5, minutes=30))

# Labels in `benchmarks.py` are NSE's own index names, so most match directly.
# These are spellings NSE has used for the same index in its files.
ALIASES: dict[str, tuple[str, ...]] = {
    "nifty infrastructure": ("nifty infra",),
    "nifty india consumption": ("nifty consumption",),
    "nifty services sector": ("nifty serv sector", "nifty services"),
    "nifty commodities": ("nifty commodity",),
}

Rows = dict[str, dict[str, Any]]


def _norm(name: str) -> str:
    return " ".join(str(name or "").lower().split())


def fetch_session(day: date, http: Any = None, *, attempts: int = 3) -> Rows | None:
    """That session's closing file keyed by normalised index name, or None
    (no session that day, or the archive could not be read)."""
    import requests

    client = http or requests
    url = CLOSE_URL.format(stamp=day.strftime("%d%m%Y"))
    for attempt in range(attempts):
        try:
            response = client.get(
                url,
                headers={"User-Agent": USER_AGENT, "Referer": "https://www.nseindia.com/"},
                timeout=20,
            )
        except Exception:
            time.sleep(2 * (attempt + 1))
            continue
        if response.status_code == 404:
            return None
        text = response.text or ""
        if response.status_code == 200 and text.lstrip().lower().startswith("index name"):
            return {_norm(r.get("Index Name", "")): r for r in csv.DictReader(io.StringIO(text))}
        time.sleep(2 * (attempt + 1))
    return None


def sessions_after(
    last: date,
    *,
    today: date | None = None,
    fetch: Callable[[date], Rows | None] = fetch_session,
    pause: float = 0.3,
) -> dict[date, Rows]:
    """Every closing file published after ``last`` up to ``today`` (IST).

    Saturdays are asked for too: NSE holds the odd Saturday session (Budget
    day), and skipping it would break the join for the Monday after.
    """
    today = today or datetime.now(IST).date()
    if (today - last).days > MAX_GAP_DAYS:
        last = today - timedelta(days=MAX_GAP_DAYS)
    out: dict[date, Rows] = {}
    day = last + timedelta(days=1)
    while day <= today:
        if day.weekday() < 6:
            rows = fetch(day)
            if rows:
                out[day] = rows
            if pause:
                time.sleep(pause)
        day += timedelta(days=1)
    return out


def _row_for(rows: Rows, label: str) -> dict[str, Any] | None:
    key = _norm(label)
    for name in (key, *ALIASES.get(key, ())):
        if name in rows:
            return rows[name]
    return None


def extend(series: dict[str, list], label: str, files: dict[date, Rows]) -> int:
    """Append to ``series`` (dates/opens/highs/lows/closes, oldest first) every
    session in ``files`` after its last date that joins it. Returns how many."""
    dates = series["dates"]
    if not dates:
        return 0
    last = date.fromisoformat(dates[-1])
    added = 0
    for day in sorted(d for d in files if d > last):
        row = _row_for(files[day], label)
        if row is None:
            break
        try:
            o, h, l, c = (float(row[k]) for k in (
                "Open Index Value", "High Index Value", "Low Index Value", "Closing Index Value"))
            points = float(row.get("Points Change") or 0)
        except (TypeError, ValueError, KeyError):
            break
        prev = float(series["closes"][-1])
        if min(o, h, l, c) <= 0 or prev <= 0 or abs((c - points) / prev - 1) > JOIN_TOLERANCE:
            break
        dates.append(day.isoformat())
        series["opens"].append(o)
        series["highs"].append(max(h, o, c))
        series["lows"].append(min(l, o, c))
        series["closes"].append(c)
        added += 1
    return added
