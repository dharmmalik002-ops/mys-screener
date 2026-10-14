"""Sector indices are completed from NSE's closing files when Yahoo refuses.

13 of 16 sectors sat on 17 Jul 2026 for three months because Yahoo turned the
runner away and the nightly build kept what it had. These pin the NSE fill.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.mutual_funds import nse_index_close, sector_stages  # noqa: E402
from scripts import build_sector_indices as builder  # noqa: E402


def _row(prev: float, close: float) -> dict:
    return {"Open Index Value": prev, "High Index Value": close + 1, "Low Index Value": prev - 1,
            "Closing Index Value": close, "Points Change": round(close - prev, 2)}


def _series(last_day: str, close: float) -> dict:
    return {"dates": [last_day], "opens": [close], "highs": [close], "lows": [close], "closes": [close]}


def test_sessions_join_on_the_files_own_previous_close():
    series = _series("2026-07-17", 100.0)
    files = {
        date(2026, 7, 20): {"nifty auto": _row(100.0, 101.0)},
        date(2026, 7, 21): {"nifty auto": _row(101.0, 102.0)},
    }
    assert nse_index_close.extend(series, "Nifty Auto", files) == 2
    assert series["dates"] == ["2026-07-17", "2026-07-20", "2026-07-21"]
    assert series["closes"][-1] == 102.0
    assert series["highs"][-1] >= series["closes"][-1] >= series["lows"][-1]


def test_a_missing_session_stops_the_fill_rather_than_bridging_it():
    series = _series("2026-07-17", 100.0)
    # The 20th's file could not be read; the 21st's previous close is the
    # 20th's, so it must not be stitched onto the 17th.
    files = {date(2026, 7, 21): {"nifty auto": _row(103.0, 104.0)}}
    assert nse_index_close.extend(series, "Nifty Auto", files) == 0
    assert series["dates"] == ["2026-07-17"]


def test_names_match_case_insensitively_and_through_aliases():
    series = _series("2026-07-17", 100.0)
    files = {date(2026, 7, 20): {"nifty infra": _row(100.0, 99.0)}}
    assert nse_index_close.extend(series, "Nifty Infrastructure", files) == 1


def test_sessions_after_asks_for_saturdays_but_not_sundays():
    asked: list[date] = []

    def fetch(day):
        asked.append(day)
        return {"nifty 50": {}} if day.weekday() < 5 else None

    out = nse_index_close.sessions_after(date(2026, 1, 30), today=date(2026, 2, 2), fetch=fetch, pause=0)
    assert asked == [date(2026, 1, 31), date(2026, 2, 2)]
    assert list(out) == [date(2026, 2, 2)]


def test_a_kept_entry_is_extended_with_weekly_and_average_recomputed():
    blob = json.loads((ROOT / "data" / "sector_indices.json").read_text())
    entry = blob["sectors"]["niftyauto"]
    daily = entry["daily"]
    last, close = date.fromisoformat(daily["dates"][-1]), daily["closes"][-1]

    files, day = {}, last
    for _ in range(12):
        day += timedelta(days=1)
        while day.weekday() >= 5:
            day += timedelta(days=1)
        files[day] = {"nifty auto": _row(close, round(close * 1.01, 2))}
        close = round(close * 1.01, 2)

    before = len(daily["dates"])
    assert builder._extend_entry(entry, "Nifty Auto", files) == 12
    daily, weekly = entry["daily"], entry["weekly"]
    assert daily["dates"][-1] == day.isoformat()
    assert len({len(daily[k]) for k in ("dates", "opens", "highs", "lows", "closes", "ma30w")}) == 1
    assert len({len(weekly[k]) for k in ("dates", "opens", "highs", "lows", "closes")}) == 1
    assert daily["ma30w"][before - 1] is not None and daily["ma30w"][-1] is not None
    assert weekly["dates"] == sorted(set(weekly["dates"]))
    # The rebuilt tail matches a fold of the daily bars from scratch.
    refold = sector_stages._to_weekly(daily["dates"], daily["closes"], daily["highs"], daily["lows"])
    assert weekly["dates"][-4:] == refold["dates"][-4:]
    assert weekly["closes"][-4:] == [round(v, 2) for v in refold["closes"][-4:]]
