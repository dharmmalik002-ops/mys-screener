"""Rotation momentum for industry groups and sectors, measured from prices.

The Groups page's Rotation chart puts each group on two axes:

* across — its ranking score against the median group (the number the
  Rankings table sorts by, so "right of centre" always means "top half of the
  table"), read from the recorded rank history;
* up — this module: is the group gaining on the Nifty 500 or losing to it?

The up axis used to be the change in the ranking score itself. That score is a
cross-sectional percentile of 1-6 month returns, so it moves when an old day
drops out of a 63- or 126-session window, or when *other* groups move, whatever
the group itself is doing. Replayed over 80 sessions of committed closes, 50%
of the groups it called Improving had lost ground to the market over the
previous two weeks. A ratio against a moving average (the usual RS-Ratio
recipe) has the same flaw: in a downtrend the average falls, so the ratio
"improves" while the group keeps losing. So momentum here is the change in the
relative-strength line itself:

    RS        = equal-weight group index / equal-weight Nifty 500 index
    momentum  = % change of EMA(RS, 5) over the last 10 sessions   (daily view)
    momentum_w= % change of EMA(RS, 10) over the last 15 sessions  (weekly view)

Positive means the group's price line has been rising against the market.
The smoothing and lookbacks were set before measuring and not tuned; on the
replay the share of Improving groups that were in fact falling dropped from
50% to 15%, the rest being the gap between a smoothed and a raw two-week line.

Closes come from `close_history.closes_for` (the committed 200-session
artifact, refreshed nightly by daily-bhavcopy.yml). Holiday rows that Yahoo
copies forward with every close unchanged are detected across the universe and
dropped, because one of them shifts every earlier bar by a session.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import median
from typing import Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)

MOMENTUM_SMOOTH = 5
MOMENTUM_LOOKBACK = 10
WEEKLY_MOMENTUM_SMOOTH = 10
WEEKLY_MOMENTUM_LOOKBACK = 15
RS_CHANGE_SESSIONS = 5

DAILY_RETURN_CLIP = 0.20  # a split or bad print, not a move
MIN_MEMBERS = 3
MIN_BARS = 40
PHANTOM_SHARE = 0.5  # share of symbols unchanged on a bar that marks it as a non-session
PHANTOM_MIN_SYMBOLS = 50
DEFAULT_SESSIONS = 90
WARMUP_SESSIONS = 40


def _ema(values: Sequence[float], span: int) -> list[float]:
    alpha = 2.0 / (span + 1)
    out: list[float] = []
    for value in values:
        out.append(value if not out else value * alpha + out[-1] * (1 - alpha))
    return out


def _winsorized_mean(values: list[float]) -> float:
    if len(values) < 4:
        return sum(values) / len(values)
    ordered = sorted(values)
    n = len(ordered)
    lo = ordered[round((n - 1) * 0.05)]
    hi = ordered[round((n - 1) * 0.95)]
    clipped = [min(max(v, lo), hi) for v in values]
    return sum(clipped) / n


def _phantom_positions(bars_from_end: Mapping[str, Sequence[float]]) -> set[int]:
    """Bar positions (0 = newest) where nearly every symbol's close is unchanged."""
    same: Counter[int] = Counter()
    seen: Counter[int] = Counter()
    for closes in bars_from_end.values():
        n = len(closes)
        for pos in range(n - 1):
            a, b = closes[n - 1 - pos], closes[n - 2 - pos]
            if a and b:
                seen[pos] += 1
                if a == b:
                    same[pos] += 1
    return {
        pos for pos, count in seen.items()
        if count >= PHANTOM_MIN_SYMBOLS and same[pos] / count > PHANTOM_SHARE
    }


def _calendar_through(calendar: Iterable[date], end: date, length: int) -> list[date]:
    """The last `length` sessions ending at `end`; weekdays fill any gap past the calendar."""
    days = sorted(d for d in set(calendar) if d <= end)
    if not days or days[-1] < end:
        cursor = (days[-1] + timedelta(days=1)) if days else end - timedelta(days=int(length * 1.6) + 10)
        while cursor <= end:
            if cursor.weekday() < 5 or cursor == end:
                days.append(cursor)
            cursor += timedelta(days=1)
    return days[-length:]


def _index(dates: list[date], closes: Mapping[str, Mapping[date, float]], symbols: Sequence[str]) -> list[float]:
    """Equal-weight index of daily returns (winsorized across members, clipped per stock)."""
    level = [1.0]
    for prev, day in zip(dates, dates[1:]):
        rets: list[float] = []
        for sym in symbols:
            series = closes.get(sym)
            if not series:
                continue
            a, b = series.get(day), series.get(prev)
            if a and b:
                rets.append(max(-DAILY_RETURN_CLIP, min(DAILY_RETURN_CLIP, a / b - 1)))
        level.append(level[-1] * (1 + (_winsorized_mean(rets) if rets else 0.0)))
    return level


def _rotation_points(dates: list[date], group: list[float], bench: list[float], sessions: int) -> list[dict]:
    rs = [g / b for g, b in zip(group, bench)]
    fast = _ema(rs, MOMENTUM_SMOOTH)
    slow = _ema(rs, WEEKLY_MOMENTUM_SMOOTH)
    first = max(len(dates) - sessions, WEEKLY_MOMENTUM_LOOKBACK, MOMENTUM_LOOKBACK, RS_CHANGE_SESSIONS)
    out = []
    for t in range(first, len(dates)):
        out.append({
            "date": dates[t].isoformat(),
            "momentum": round((fast[t] / fast[t - MOMENTUM_LOOKBACK] - 1) * 100, 3),
            "momentum_w": round((slow[t] / slow[t - WEEKLY_MOMENTUM_LOOKBACK] - 1) * 100, 3),
            "rs_change_5d": round((rs[t] / rs[t - RS_CHANGE_SESSIONS] - 1) * 100, 2),
        })
    return out


def build_group_rotation(
    groups: Sequence[Mapping[str, object]],
    closes_by_symbol: Mapping[str, Sequence[float]],
    session_by_symbol: Mapping[str, date | None],
    benchmark_symbols: Sequence[str],
    calendar: Iterable[date],
    *,
    sessions: int = DEFAULT_SESSIONS,
) -> dict[str, object]:
    """Momentum series per group and per parent sector.

    `groups` rows need `group_id`, `parent_sector` and `symbols`.
    `closes_by_symbol` holds contiguous daily closes ending at each symbol's
    `session_by_symbol` date (what `close_history.closes_for` returns).
    """
    sessions_seen = Counter(d for sym, d in session_by_symbol.items() if d and closes_by_symbol.get(sym))
    if not sessions_seen:
        return {"as_of_date": None, "sessions": [], "groups": {}, "sectors": {}, "coverage": {}}
    as_of = max(sessions_seen.items(), key=lambda item: (item[1], item[0]))[0]

    # Only symbols on the modal session line up bar-for-bar with each other.
    usable = {
        sym: list(closes)
        for sym, closes in closes_by_symbol.items()
        if session_by_symbol.get(sym) == as_of and len(closes) >= MIN_BARS
    }
    phantom = _phantom_positions(usable)
    longest = max((len(c) for c in usable.values()), default=0)
    dates = _calendar_through(calendar, as_of, min(longest - len(phantom), sessions + WARMUP_SESSIONS))

    dated: dict[str, dict[date, float]] = {}
    for sym, closes in usable.items():
        kept = [c for pos, c in enumerate(reversed(closes)) if pos not in phantom]
        dated[sym] = {day: close for day, close in zip(reversed(dates), kept)}

    bench_members = [s for s in benchmark_symbols if s in dated]
    if len(bench_members) < MIN_MEMBERS:
        return {"as_of_date": as_of.isoformat(), "sessions": [], "groups": {}, "sectors": {}, "coverage": {}}
    bench = _index(dates, dated, bench_members)

    out_groups: dict[str, list[dict]] = {}
    coverage: dict[str, int] = {}
    by_sector: dict[str, list[str]] = defaultdict(list)
    for row in groups:
        gid = str(row.get("group_id") or "")
        members = [s for s in (row.get("symbols") or []) if s in dated]
        by_sector[str(row.get("parent_sector") or "Unclassified")].extend(members)
        coverage[gid] = len(members)
        if gid and len(members) >= MIN_MEMBERS:
            out_groups[gid] = _rotation_points(dates, _index(dates, dated, members), bench, sessions)

    out_sectors = {
        sector: _rotation_points(dates, _index(dates, dated, members), bench, sessions)
        for sector, members in by_sector.items()
        if len(members) >= MIN_MEMBERS
    }
    window = [d.isoformat() for d in dates[-sessions:]]
    missing = [gid for gid, n in coverage.items() if n < MIN_MEMBERS]
    if missing:
        logger.info("group rotation: %d group(s) lack price history for %d members: %s",
                    len(missing), MIN_MEMBERS, ", ".join(missing[:10]))
    return {
        "as_of_date": as_of.isoformat(),
        "sessions": window,
        "groups": out_groups,
        "sectors": out_sectors,
        "coverage": coverage,
        "non_sessions_dropped": len(phantom),
        "method": {
            "momentum": f"% change of EMA({MOMENTUM_SMOOTH}) of group/benchmark over {MOMENTUM_LOOKBACK} sessions",
            "momentum_w": f"% change of EMA({WEEKLY_MOMENTUM_SMOOTH}) of group/benchmark over {WEEKLY_MOMENTUM_LOOKBACK} sessions",
            "index": "equal weight, daily returns winsorized 5/95 across members and clipped at 20%",
            "median_members_used": median(coverage.values()) if coverage else 0,
        },
    }
