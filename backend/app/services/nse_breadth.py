"""Whole-market advance/decline counts, straight from NSE's official bhavcopy.

The Home page's "Market Breadth" card used to count advancers across the
scan snapshots for today and across the per-stock chart cache for the
previous nine sessions. The chart cache is gitignored, so on the Space it held
four symbols and the 10-day chart plotted four stocks as if they were the
market (0% advancing one day, 75% the next, next to a 49% card).

This series is the market as NSE reports it: every mainboard share traded that
session — EQ, BE and BZ series with an equity ISIN (``INE…``). SME boards,
ETFs, mutual-fund units, bonds and REIT/InvIT units are excluded. The daily
bhavcopy job writes one row per session to ``nse_breadth_history.json``
(committed, so a cold Space serves it), and a row exists only for a date NSE
published a bhavcopy — a holiday has no file, so it can never get a row.
"""

from __future__ import annotations

import csv
import io
from typing import Any, Iterable, Mapping

HISTORY_FILENAME = "nse_breadth_history.json"
MAINBOARD_SERIES = ("EQ", "BE", "BZ")
UNIVERSE_LABEL = "All NSE mainboard stocks"
# Keep a year and a half of sessions; the Home card reads ten.
MAX_DAYS = 400


def parse_rows(csv_text: str) -> dict[str, dict[str, Any]]:
    """{symbol: {"c", "p", "srs", "isin"}} from an NSE CM bhavcopy (UDiFF format)."""
    out: dict[str, dict[str, Any]] = {}
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        symbol = (row.get("TckrSymb") or "").strip().upper()
        if not symbol:
            continue
        try:
            close = float(row.get("ClsPric") or 0)
            prev = float(row.get("PrvsClsgPric") or 0)
        except (TypeError, ValueError):
            continue
        out[symbol] = {
            "c": close,
            "p": prev,
            "srs": (row.get("SctySrs") or "").strip().upper(),
            "isin": (row.get("ISIN") or "").strip().upper(),
        }
    return out


def is_mainboard_share(record: Mapping[str, Any]) -> bool:
    return (
        str(record.get("srs") or "") in MAINBOARD_SERIES
        and str(record.get("isin") or "").startswith("INE")
    )


def day_row(date_iso: str, rows: Mapping[str, Mapping[str, Any]]) -> dict | None:
    """One session's counts over mainboard shares; None when nothing usable."""
    advances = declines = unchanged = 0
    for record in rows.values():
        if not is_mainboard_share(record):
            continue
        try:
            close = float(record.get("c") or 0)
            prev = float(record.get("p") or 0)
        except (TypeError, ValueError):
            continue
        if close <= 0 or prev <= 0:
            continue
        if close > prev:
            advances += 1
        elif close < prev:
            declines += 1
        else:
            unchanged += 1
    total = advances + declines + unchanged
    if not total:
        return None
    return {
        "date": date_iso,
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "total": total,
    }


def merge_days(existing: Iterable[Mapping[str, Any]], fresh: Iterable[Mapping[str, Any]]) -> list[dict]:
    """Union on date (fresh wins), oldest first, trimmed to MAX_DAYS."""
    by_date: dict[str, dict] = {}
    for row in list(existing or []) + list(fresh or []):
        if isinstance(row, Mapping) and row.get("date"):
            by_date[str(row["date"])] = dict(row)
    return [by_date[d] for d in sorted(by_date)][-MAX_DAYS:]


def xp_input(rows: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, float]]:
    """The {symbol: {"c", "p"}} shape `xp_breadth.daily_breadth_metrics` reads."""
    return {sym: {"c": rec.get("c"), "p": rec.get("p")} for sym, rec in rows.items()}
