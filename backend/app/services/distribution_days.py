"""Distribution days — institutional selling pressure, O'Neil style.

A distribution day is an index session that closes down at least 0.2% on
volume higher than the prior session. The count over a rolling 25 sessions is
the standard market-direction gauge in the O'Neil / Minervini framework.

Two honesty constraints, both measured against this repo's own data.

**The count is as-of the prior close.** The newest ^NSEI bar carries
`volume == 0` in every cached timeframe (the value is backfilled a session
later), and Smallcap 250 / Midcap 150 carry volume 0 on *every* bar. A day
cannot be judged without its volume, so sessions with no volume — on either
side of the comparison — are skipped and `as_of` reports the last session that
could actually be evaluated. Silently treating 0 as "lower volume" would
permanently hide a real distribution day on the most recent bar.

**The bands are calibrated to what this market does, not the textbook.**
Measured over ~500 aligned sessions the count spends most of its life at 3-5,
so labelling 3-4 "caution" would leave the page permanently amber and the
label would carry no information. More importantly, bucketing the count
against forward 20-session returns here is non-monotone — the 7+ bucket did
*not* underperform — so this is treated as a descriptive pressure gauge and,
in the exposure model, never as a credit.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Sequence

from app.services.market_frame import FrameRow

DROP_PCT = 0.2  # a down day of at least this much counts
LOOKBACK_SESSIONS = 25  # the rolling count window
RALLY_EXPIRY_PCT = 5.0  # a day is cancelled once the index closes this far above it
CLUSTER_WINDOW = 5  # "recent cluster" lookback


@dataclass(frozen=True)
class DistributionDay:
    date: str
    close: float
    change_pct: float
    volume: float
    prior_volume: float


def _pressure_label(count: int) -> str:
    """Bands calibrated to the measured distribution, not the textbook.

    Over 498 aligned sessions the count ran median 5, min 0, max 10. The
    classic "5+ means pressure" cut would therefore label the *typical* session
    as pressure, which makes the word useless. These bands put the median in
    "normal" so the label only speaks up when the count is genuinely unusual.
    """
    if count <= 2:
        return "clean"
    if count <= 5:
        return "normal"
    if count <= 7:
        return "under pressure"
    return "heavy"


def _evaluable(rows: Sequence[FrameRow], i: int) -> bool:
    """Can session `i` be judged at all? Needs volume on both sides."""
    return i > 0 and rows[i].volume > 0 and rows[i - 1].volume > 0


def _is_distribution(rows: Sequence[FrameRow], i: int) -> bool:
    if not _evaluable(rows, i):
        return False
    prev_close = rows[i - 1].close
    if prev_close <= 0:
        return False
    change_pct = (rows[i].close - prev_close) / prev_close * 100.0
    return change_pct <= -DROP_PCT and rows[i].volume > rows[i - 1].volume


def _last_evaluable_index(rows: Sequence[FrameRow]) -> int | None:
    for i in range(len(rows) - 1, 0, -1):
        if _evaluable(rows, i):
            return i
    return None


def live_days(rows: Sequence[FrameRow], as_of_index: int) -> list[DistributionDay]:
    """Unexpired distribution days as at `as_of_index`.

    Expiry is either age (25 sessions) or a 5% rally measured from that day's
    own close — so a strong advance clears the older, lower days first and
    leaves any recent ones standing, which is the point of the rule.
    """
    out: list[DistributionDay] = []
    start = max(1, as_of_index - LOOKBACK_SESSIONS + 1)
    as_of_close = rows[as_of_index].close
    for i in range(start, as_of_index + 1):
        if not _is_distribution(rows, i):
            continue
        event_close = rows[i].close
        if event_close > 0 and as_of_close >= event_close * (1 + RALLY_EXPIRY_PCT / 100.0):
            continue  # rallied away
        prev_close = rows[i - 1].close
        out.append(
            DistributionDay(
                date=rows[i].date,
                close=round(event_close, 2),
                change_pct=round((event_close - prev_close) / prev_close * 100.0, 2),
                volume=rows[i].volume,
                prior_volume=rows[i - 1].volume,
            )
        )
    return out


def count_distribution_days(
    rows: Sequence[FrameRow], *, as_of_index: int | None = None
) -> dict[str, Any] | None:
    """Live count, or None when the frame is too short to judge."""
    if not rows or len(rows) < 2:
        return None
    if as_of_index is None:
        as_of_index = _last_evaluable_index(rows)
    if as_of_index is None or as_of_index < 1:
        return None

    days = live_days(rows, as_of_index)
    as_of_date = rows[as_of_index].date
    cluster = sum(1 for d in days if d.date >= rows[max(0, as_of_index - CLUSTER_WINDOW + 1)].date)
    trails_price = as_of_index < len(rows) - 1

    return {
        "as_of": as_of_date,
        "count": len(days),
        "window_sessions": LOOKBACK_SESSIONS,
        "pressure_label": _pressure_label(len(days)),
        "cluster_last_5": cluster,
        "days": [asdict(d) for d in days],
        "trails_price_by_sessions": (len(rows) - 1 - as_of_index) if trails_price else 0,
        "note": (
            "A distribution day is an index session closing down 0.2% or more on higher "
            "volume than the day before, counted over 25 sessions and dropped after 25 "
            "sessions or once the index closes 5% above it. Index volume here is a "
            "yfinance figure, not NSE cash turnover, and the newest bar carries no volume "
            "— so this count is as of the last session that could be evaluated."
        ),
    }


def distribution_series(
    rows: Sequence[FrameRow], *, sessions: int = 60
) -> list[dict[str, Any]]:
    """[{date, count}] for the sparkline — same rules, evaluated point by point."""
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    start = max(1, len(rows) - sessions)
    for i in range(start, len(rows)):
        if not _evaluable(rows, i):
            continue
        out.append({"date": rows[i].date, "count": len(live_days(rows, i))})
    return out
