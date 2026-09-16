"""Which stage of its own cycle a single stock is in.

Weinstein stage analysis already ships in this app for the sixteen Nifty
sector indices (`mutual_funds/sector_stages.py`). This module applies the very
same classifier to an individual stock's bars.

**Why it reuses that module rather than reimplementing it.** The stage
boundaries are a calibration — a flat-slope band, a near-the-average band, a
two-year range position — and a second copy would drift from the first the
first time either is tuned. The app has already paid for that mistake once
with sector naming (CLAUDE.md: "one sector vocabulary"), so there is exactly
one definition of Stage 2 here and the sector page and the stock badge cannot
disagree about what it means.

**Why 30 weeks and not the 50-DMA the scanners use.** They answer different
questions. The Minervini trend template asks "is this stock in a condition I
would buy today"; stage analysis asks "where is this stock in its multi-year
cycle". The 30-week average is the frame the second question is defined on,
and substituting a faster average would give a different, noisier answer that
is not stage analysis.

**What this is not.** The stage is a description of past price, not a forecast.
Stage 2 does not mean "buy" and Stage 4 does not mean "short" — a Stage 2 stock
can top out the next week. It is reported as a measurement, in keeping with the
rule that this app describes conditions and does not issue trade calls
(CLAUDE.md gotcha 12).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Sequence

from app.services.mutual_funds import sector_stages

logger = logging.getLogger(__name__)

# `classify` needs MIN_WEEKS (56) weekly closes, so roughly 400 calendar days.
# Asking for more than that gives the two-year range position something real to
# measure against, which is what separates a base from a top.
MIN_DAILY_BARS = 300
PREFERRED_DAILY_BARS = 600

STAGE_ACTION_NOTE = {
    1: "Bases are where positions are built, not where they pay. Nothing has been proved "
       "until price clears the base on volume — and some bases fail straight back into decline.",
    2: "This is the only stage in which breakout setups have the wind behind them. It is also "
       "the stage in which people confuse a trend with their own skill.",
    3: "Advances end here, and tops take longer than anyone expects. Existing positions earn "
       "tighter stops; new ones are fighting the stage.",
    4: "Downtrends persist. Almost every 'cheap' purchase that ruins a year is made in Stage 4, "
       "because the stock already looks like it has fallen enough.",
}


def _bar_value(bar: Any, field: str) -> float | None:
    """Read a field off either a pydantic Candle or a plain dict."""
    value = getattr(bar, field, None) if not isinstance(bar, dict) else bar.get(field)
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _bar_date(bar: Any) -> str | None:
    raw = getattr(bar, "time", None) if not isinstance(bar, dict) else bar.get("time")
    try:
        stamp = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(stamp, tz=timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def stage_for_bars(bars: Sequence[Any]) -> dict[str, Any]:
    """Classify one stock's daily bars into a Weinstein stage.

    Returns `{"available": False, "reason": ...}` rather than a guess whenever
    the history is too short — a stage derived from 30 weeks of data the stock
    does not have would be confidently wrong, which is worse than absent.
    """
    dates: list[str] = []
    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []

    for bar in bars:
        day = _bar_date(bar)
        close = _bar_value(bar, "close")
        if not day or close is None:
            continue
        dates.append(day)
        closes.append(close)
        highs.append(_bar_value(bar, "high") or close)
        lows.append(_bar_value(bar, "low") or close)

    if len(closes) < MIN_DAILY_BARS:
        return {
            "available": False,
            "reason": (
                f"Stage analysis needs about {sector_stages.MIN_WEEKS} weeks of history "
                f"and this symbol has {len(closes)} daily bars. Newly listed stocks do not "
                "have a cycle to place yet."
            ),
            "daily_bars": len(closes),
        }

    weekly = sector_stages._to_weekly(dates, closes, highs, lows)
    verdict = sector_stages.classify(weekly)
    stage = verdict.get("stage")
    if stage is None:
        return {
            "available": False,
            "reason": f"Not enough weekly history to place a stage ({verdict.get('reason', 'unknown')}).",
            "daily_bars": len(closes),
        }

    base = sector_stages.base_metrics(weekly, None)
    return {
        "available": True,
        "stage": stage,
        "stage_label": sector_stages.STAGE_LABELS[stage],
        "blurb": sector_stages.STAGE_BLURBS[stage],
        "note": STAGE_ACTION_NOTE[stage],
        "early_advance": verdict.get("early_advance", False),
        "ma30_week": verdict.get("ma30"),
        "price": verdict.get("price"),
        "distance_from_ma_pct": verdict.get("distance_from_ma_pct"),
        "ma_slope_pct_per_week": verdict.get("ma_slope_pct_per_week"),
        "position_in_2y_range_pct": verdict.get("position_in_2y_range_pct"),
        "weeks_of_history": verdict.get("weeks_of_history"),
        "base": {
            "tight": base.get("tight"),
            "range_pct": base.get("range_pct"),
            "position_in_range_pct": base.get("position_in_range_pct"),
            "off_52w_high_pct": base.get("off_52w_high_pct"),
            "base_high": base.get("base_high"),
            "base_low": base.get("base_low"),
        },
    }
