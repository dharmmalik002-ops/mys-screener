"""One date-aligned daily frame joining index bars, universe breadth and XP.

Why this exists
---------------
`free_historical_breadth.json` is written by unioning every stock's bar index
(`_aggregate_and_save_historical_breadth`, providers/free.py), which drags in
rows for dates the market never traded. Measured on the shipped file: **110
Sunday rows and 1 Saturday row**. They do not look wrong — 2026-08-07 (Fri)
reads 61.90% above the 50-DMA, 2026-08-09 (**Sun**) reads 68.21%, 2026-08-10
(Mon) reads 64.08% — because they are computed over whichever handful of
symbols happened to carry a weekly-stamped bar. The denominator is not
persisted, so nothing downstream can detect them from the JSON alone.

Two consequences, and both are why every consumer must go through this module:

1. Any N-session arithmetic on the raw array is wrong. "10 rows back" is not
   "10 sessions back" when phantom weekend rows sit in between.
2. A chart drawn straight off the file shows weekend spikes that never happened.

Joining to the real `^NSEI` session calendar removes them by construction.

The frame is also where the "missing is not zero" rule is enforced. The breadth
writer does `.fillna(0)`, so a genuine zero and an absent measurement are
indistinguishable in the file; and the XP history lags the breadth file by a
session. Both are represented as `None` here rather than silently becoming 0,
because a 0 would read as "no stocks above their 200-SMA" — a maximally
bearish reading manufactured out of missing data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

DEFAULT_UNIVERSE = "Nifty 500"

# Blend used everywhere as "participation". The 50-DMA carries most of the
# weight because it is the swing-horizon measure the user actually trades
# against; the 200-SMA is a slower structural floor.
PARTICIPATION_MA50_WEIGHT = 0.7
PARTICIPATION_SMA200_WEIGHT = 0.3


@dataclass(frozen=True)
class FrameRow:
    """One real trading session with everything known about it.

    Breadth-derived fields are optional. `free_historical_breadth.json` is only
    written during a full snapshot refresh, which the deployed Space never runs
    (it serves committed snapshots plus the daily bhavcopy patch), and the file
    is gitignored — so in production it is simply absent. An inner join on it
    used to empty the whole frame, which took the XP regime and the distribution
    days down with it even though both of their sources were present and
    current. Everything here degrades independently now.
    """

    date: str  # ISO, guaranteed to be a session present in the index calendar
    close: float
    high: float
    low: float
    volume: float  # 0.0 on the newest bar — see distribution_days for why
    participation: float | None
    participation_source: str | None  # "nifty500-breadth" | "xp-universe"
    above_ma20_pct: float | None
    above_ma50_pct: float | None
    above_sma200_pct: float | None
    new_high_52w_pct: float | None
    new_low_52w_pct: float | None
    xp_score: float | None
    xp_regime: str | None
    ma10_pct: float | None
    ma20_pct: float | None


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None  # NaN check without importing math


def _bar_date(bar: Any) -> str | None:
    """ISO date for a ChartBar-shaped mapping or model."""
    raw = bar.get("time") if isinstance(bar, Mapping) else getattr(bar, "time", None)
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), timezone.utc).date().isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _bar_field(bar: Any, name: str) -> float | None:
    raw = bar.get(name) if isinstance(bar, Mapping) else getattr(bar, name, None)
    return _to_float(raw)


def index_sessions(index_bars: Iterable[Any]) -> dict[str, dict[str, float]]:
    """The authoritative trading calendar: {iso_date: {close, high, low, volume}}.

    Bars with a non-positive close are dropped — a zero close is a data hole,
    and letting one through would put a fake -100% session in every return
    series computed off this frame.
    """
    sessions: dict[str, dict[str, float]] = {}
    for bar in index_bars or []:
        iso = _bar_date(bar)
        close = _bar_field(bar, "close")
        if not iso or close is None or close <= 0:
            continue
        sessions[iso] = {
            "close": close,
            "high": _bar_field(bar, "high") or close,
            "low": _bar_field(bar, "low") or close,
            # Volume is legitimately 0 on the newest index bar; keep it as 0.0
            # and let distribution_days decide what that means.
            "volume": _bar_field(bar, "volume") or 0.0,
        }
    return sessions


def breadth_by_date(breadth_doc: Mapping[str, Any], universe: str = DEFAULT_UNIVERSE) -> dict[str, dict]:
    """Usable breadth rows for one universe, keyed by ISO date.

    `above_sma200_pct > 0` is the validity test rather than a date cutoff: the
    writer fills missing values with 0, so the all-zero warmup rows at the head
    of the file are indistinguishable from real readings by date alone.
    """
    for entry in (breadth_doc or {}).get("universes") or []:
        if str(entry.get("universe")) != universe:
            continue
        rows: dict[str, dict] = {}
        for row in entry.get("history") or []:
            iso = str(row.get("date") or "")
            above200 = _to_float(row.get("above_sma200_pct"))
            if not iso or not above200 or above200 <= 0:
                continue
            rows[iso] = row
        return rows
    return {}


def xp_by_date(xp_doc: Mapping[str, Any]) -> dict[str, dict]:
    """XP rows keyed by ISO date, warmup rows excluded."""
    rows: dict[str, dict] = {}
    for row in (xp_doc or {}).get("days") or []:
        iso = str(row.get("date") or "")
        if not iso or row.get("warmup"):
            continue
        rows[iso] = row
    return rows


def participation_of(above_ma50_pct: float, above_sma200_pct: float) -> float:
    return round(
        PARTICIPATION_MA50_WEIGHT * above_ma50_pct + PARTICIPATION_SMA200_WEIGHT * above_sma200_pct,
        2,
    )


def build_frame(
    index_bars: Sequence[Any],
    breadth_doc: Mapping[str, Any],
    xp_doc: Mapping[str, Any],
    *,
    universe: str = DEFAULT_UNIVERSE,
) -> list[FrameRow]:
    """Join the three sources onto real trading sessions, oldest first.

    Inner join on index ∩ breadth (this is what drops the phantom weekend
    rows), left join on XP (it lags by a session, so the newest row legitimately
    has no XP reading).
    """
    sessions = index_sessions(index_bars)
    breadth = breadth_by_date(breadth_doc, universe)
    xp = xp_by_date(xp_doc)

    rows: list[FrameRow] = []
    # The index calendar defines the frame. Breadth and XP are joined onto it
    # when present rather than gating it.
    for iso in sorted(sessions.keys()):
        bar = sessions[iso]
        b = breadth.get(iso) or {}
        x = xp.get(iso) or {}
        if not b and not x:
            continue  # a bare price bar carries nothing this module is for

        above50 = _to_float(b.get("above_ma50_pct"))
        above200 = _to_float(b.get("above_sma200_pct"))
        ma20 = _to_float(x.get("ma20_pct"))

        # Prefer the Nifty 500 blend; fall back to the XP universe's %-above-
        # 20-EMA, which is a wider universe and a different average, so the
        # source is recorded and surfaced rather than quietly substituted.
        if above50 is not None and above200 is not None:
            participation = participation_of(above50, above200)
            source = "nifty500-breadth"
        elif ma20 is not None:
            participation = round(ma20, 2)
            source = "xp-universe"
        else:
            participation = None
            source = None

        rows.append(
            FrameRow(
                date=iso,
                close=bar["close"],
                high=bar["high"],
                low=bar["low"],
                volume=bar["volume"],
                participation=participation,
                participation_source=source,
                above_ma20_pct=_to_float(b.get("above_ma20_pct")),
                above_ma50_pct=above50,
                above_sma200_pct=above200,
                new_high_52w_pct=_to_float(b.get("new_high_52w_pct")),
                new_low_52w_pct=_to_float(b.get("new_low_52w_pct")),
                xp_score=_to_float(x.get("xp_score")),
                xp_regime=str(x["regime"]) if x.get("regime") else None,
                ma10_pct=_to_float(x.get("ma10_pct")),
                ma20_pct=ma20,
            )
        )
    return rows


def frame_sources(
    index_bars: Sequence[Any],
    breadth_doc: Mapping[str, Any],
    xp_doc: Mapping[str, Any],
    *,
    universe: str = DEFAULT_UNIVERSE,
) -> dict[str, Any]:
    """Provenance + staleness block, shipped with every response built on the frame."""
    sessions = index_sessions(index_bars)
    breadth = breadth_by_date(breadth_doc, universe)
    xp = xp_by_date(xp_doc)
    # "Aligned" now means sessions carrying *any* joined data, since breadth is
    # optional; the breadth-specific count is reported separately.
    aligned = sessions.keys() & (breadth.keys() | xp.keys())

    # Only count non-session breadth rows inside the index calendar's own span;
    # breadth history reaches further back than the index cache, and counting
    # those older rows as "dropped" would wildly overstate the problem.
    dropped_non_session = 0
    if sessions:
        lo, hi = min(sessions), max(sessions)
        dropped_non_session = sum(1 for iso in breadth if lo <= iso <= hi and iso not in sessions)

    index_last = max(sessions) if sessions else None
    xp_last = max(xp) if xp else None
    warning = None
    if index_last and xp_last and xp_last < index_last:
        warning = "XP breadth is behind the price data by at least one session."

    return {
        "universe": universe,
        "index_last_session": index_last,
        "breadth_generated_at": (breadth_doc or {}).get("generated_at"),
        "breadth_last_session": max(breadth) if breadth else None,
        "xp_generated_at": (xp_doc or {}).get("generated_at"),
        "xp_last_session": xp_last,
        "aligned_sessions": len(aligned),
        "breadth_sessions": len(sessions.keys() & breadth.keys()),
        "participation_source": (
            "nifty500-breadth" if sessions.keys() & breadth.keys() else ("xp-universe" if xp else None)
        ),
        "rows_dropped_non_session": dropped_non_session,
        "staleness_warning": warning,
    }


def index_of_date(rows: Sequence[FrameRow], iso: str) -> int | None:
    for i, row in enumerate(rows):
        if row.date == iso:
            return i
    return None


def latest_priced_index(rows: Sequence[FrameRow]) -> int | None:
    """Index of the newest row, or None when the frame is empty."""
    return len(rows) - 1 if rows else None
