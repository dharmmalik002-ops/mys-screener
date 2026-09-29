"""Rebuild Expansion hits for sessions nobody opened the scanner on.

The Expansion tracker pins a session's hits only when the scan endpoint is
called that session, so a day the page went unopened simply never got a list —
the stocks that fired that day vanished from the 30-session tracker. Every
session's authoritative OHLCV is already on disk (``data/eod_bars``, the last
~12 sessions), which is everything the scan's gates read: the day's close, the
prior session's close, the day's volume and its 20-session average volume.

The gates are the ones ``_ema_expansion_with_thresholds`` applies to a live
snapshot, evaluated on that day's bars instead of today's.
"""

from __future__ import annotations

from typing import Iterable, Mapping

from app.models.market import ScanMatch, StockSnapshot
from app.scanners.definitions import build_scan_match

MIN_CHANGE_PCT = 6.5
MIN_RELATIVE_VOLUME = 3.0
MIN_AVG_VOLUME_20 = 25_000
MIN_DAY_VOLUME = 50_000
MIN_PRICE = 30.0
AVG_WINDOW = 20
# Fewer known volumes than this in the 20-session window and the average is
# a guess; skip the symbol for that day rather than invent a spike.
MIN_VOLUMES_FOR_AVG = 10


def _dated_volumes(
    snapshot: StockSnapshot,
    calendar: list[str],
    eod: Mapping[str, list],
) -> dict[str, float]:
    """{session: volume}. The snapshot's 20 trailing volumes end at its own
    session; the dated EOD store overrides them wherever it has the day."""
    out: dict[str, float] = {}
    session = snapshot.history_session_date.isoformat() if snapshot.history_session_date else None
    recent = list(snapshot.recent_volumes or [])
    if session and recent and session in calendar:
        end = calendar.index(session)
        start = end - len(recent) + 1
        for offset, volume in enumerate(recent):
            index = start + offset
            if index >= 0 and volume:
                out[calendar[index]] = float(volume)
    for day, ohlcv in eod.items():
        try:
            volume = float(ohlcv[4])
        except (IndexError, TypeError, ValueError):
            continue
        if volume > 0:
            out[day] = volume
    return out


def hits_for_session(
    session: str,
    snapshots: Iterable[StockSnapshot],
    eod_bars: Mapping[str, Mapping[str, list]],
    calendar: list[str],
) -> list[ScanMatch]:
    """Expansion hits for one past ``session``, newest-score first.

    ``calendar`` is the ascending list of trading sessions; the prior close is
    the previous calendar session's close and must be in the EOD store too, or
    the day's move is unknown and the symbol is skipped.
    """
    if session not in calendar:
        return []
    position = calendar.index(session)
    if position == 0:
        return []
    previous = calendar[position - 1]
    window = calendar[max(0, position - AVG_WINDOW + 1) : position + 1]

    matches: list[ScanMatch] = []
    for snapshot in snapshots:
        eod = eod_bars.get(snapshot.symbol)
        if not eod or session not in eod or previous not in eod:
            continue
        try:
            close = float(eod[session][3])
            prior_close = float(eod[previous][3])
            volume = float(eod[session][4])
        except (IndexError, TypeError, ValueError):
            continue
        if close <= MIN_PRICE or prior_close <= 0 or volume <= MIN_DAY_VOLUME:
            continue
        change_pct = (close / prior_close - 1.0) * 100.0
        if change_pct < MIN_CHANGE_PCT:
            continue

        volumes = _dated_volumes(snapshot, calendar, eod)
        known = [volumes[day] for day in window if day in volumes]
        if len(known) < MIN_VOLUMES_FOR_AVG:
            continue
        avg_volume = sum(known) / len(known)
        if avg_volume < MIN_AVG_VOLUME_20:
            continue
        rvol = volume / avg_volume
        if rvol <= MIN_RELATIVE_VOLUME:
            continue

        score = round(75 + rvol * 2 + change_pct, 2)
        reasons = [
            "Expansion setup",
            f"Daily Change: {change_pct:.2f}%",
            f"20-Day RVOL: {rvol:.2f}x",
            f"Price: {close:.2f}",
        ]
        match = build_scan_match("ema-expansion", snapshot, score, reasons)
        matches.append(
            match.model_copy(
                update={
                    "last_price": round(close, 2),
                    "change_pct": round(change_pct, 2),
                    "relative_volume": round(rvol, 2),
                    "gap_pct": None,
                    "session_date": session,
                }
            )
        )
    matches.sort(key=lambda item: item.score, reverse=True)
    return matches
