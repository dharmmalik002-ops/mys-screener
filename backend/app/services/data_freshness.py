"""How old each data feed the site shows is, measured in trading sessions.

Most of this project's outages were quiet: the end-of-day patch reached the
repo but not the Space (gotcha 21), the breakout replay sat on July weeks for
six weeks (section 3.5), index charts ended a session early (gotcha 151). Each
was found by someone noticing a number looked old. This module reads the date
every committed feed says it describes and compares it with the latest NSE
session that should exist by now, so the header can say so before anyone has
to notice.

Rules, declared rather than tuned:

* The date read is the one the data describes (the session), never the time a
  file was rebuilt -- gotcha 129.
* Sessions are weekdays. Exchange holidays are not known here, so every feed
  gets one session of grace: a holiday reads as at most one session behind,
  never as stale.
* A feed that is missing or unreadable reports ``unknown`` rather than being
  dropped, so a broken file is visible rather than absent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

IST = timezone(timedelta(hours=5, minutes=30))

# The bhavcopy job retries until ~18:30 IST; before then the newest closed
# session's data is not expected yet, so "today" only counts from this hour.
EOD_READY_HOUR_IST = 19

STATUS_ORDER = {"ok": 0, "late": 1, "stale": 2, "unknown": 3}


@dataclass(frozen=True)
class Feed:
    key: str
    label: str
    filename: str
    read: Callable[[dict[str, Any]], str | None]
    # Sessions behind the expected session that still read "ok". Feeds built
    # after the close (breakout replay ~20:00 IST, funds ~01:00 IST, the bot
    # overnight) are a session behind for part of every evening by design.
    grace: int
    # What the feed drives on the site, shown beside it.
    used_by: str


def _last_day(doc: dict[str, Any]) -> str | None:
    days = doc.get("days")
    if isinstance(days, list) and days and isinstance(days[-1], dict):
        return days[-1].get("date")
    return None


def _latest_date(doc: dict[str, Any]) -> str | None:
    latest = doc.get("latest")
    return latest.get("date") if isinstance(latest, dict) else None


def _newest_calendar_day(doc: dict[str, Any]) -> str | None:
    calendar = doc.get("calendar")
    if isinstance(calendar, dict) and calendar:
        return max(str(day) for day in calendar)
    return None


FEEDS: tuple[Feed, ...] = (
    Feed("prices", "End-of-day prices", "bhavcopy_patch.json", lambda d: d.get("date"), 1, "Scanners, charts, Home"),
    Feed("breadth", "Advancers / decliners", "nse_breadth_history.json", _last_day, 1, "Home breadth"),
    Feed("xp", "XP breadth score", "xp_breadth_history.json", _latest_date, 1, "Home dial, Markets"),
    Feed("breakouts", "Breakout replay", "breakout_stats.json", lambda d: d.get("as_of_session"), 2, "Markets exposure verdict"),
    Feed("bot", "Bot signals", "bot_signals.json", lambda d: d.get("as_of"), 2, "Bot page"),
    Feed("lookalikes", "Look-alike picks", "lookalike_picks.json", _newest_calendar_day, 2, "Look-alikes page"),
    Feed("funds", "Fund NAVs", "mf_universe.json", lambda d: d.get("as_of"), 2, "Funds page"),
)


def expected_session(now: datetime | None = None) -> date:
    """The newest weekday session whose end-of-day data should exist by now."""
    now = (now or datetime.now(timezone.utc)).astimezone(IST)
    day = now.date()
    if now.hour < EOD_READY_HOUR_IST:
        day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def sessions_between(older: date, newer: date) -> int:
    """Weekday sessions after ``older`` up to and including ``newer`` (0 when
    ``older`` is the same session or later)."""
    if older >= newer:
        return 0
    count = 0
    day = older
    while day < newer:
        day += timedelta(days=1)
        if day.weekday() < 5:
            count += 1
    return count


def _parse(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def feed_status(behind: int | None, grace: int) -> str:
    if behind is None:
        return "unknown"
    if behind <= grace:
        return "ok"
    if behind <= grace + 2:
        return "late"
    return "stale"


def read_feed(data_dir: Path, feed: Feed, expected: date) -> dict[str, Any]:
    as_of: date | None = None
    error: str | None = None
    path = data_dir / feed.filename
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            raise ValueError("not a JSON object")
        as_of = _parse(feed.read(doc))
        if as_of is None:
            error = "no session date in the file"
    except FileNotFoundError:
        error = "file not found"
    except Exception as exc:  # unreadable or malformed: report, never raise
        error = f"unreadable ({type(exc).__name__})"
    behind = sessions_between(as_of, expected) if as_of else None
    return {
        "key": feed.key,
        "label": feed.label,
        "used_by": feed.used_by,
        "as_of": as_of.isoformat() if as_of else None,
        "sessions_behind": behind,
        "status": feed_status(behind, feed.grace),
        "error": error,
    }


def build_report(data_dir: Path, now: datetime | None = None) -> dict[str, Any]:
    expected = expected_session(now)
    feeds = [read_feed(data_dir, feed, expected) for feed in FEEDS]
    worst = max((f["status"] for f in feeds), key=lambda s: STATUS_ORDER[s], default="unknown")
    return {
        "expected_session": expected.isoformat(),
        "checked_at": (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(),
        "status": worst,
        "feeds": feeds,
    }
