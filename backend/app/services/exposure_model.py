"""The Markets page verdict: how much capital should be at risk right now.

This is a **risk rule about the present, not a forecast.** That framing is not
modesty, it is what the data forced. Five relationships were tested against
this repo's own history before the model was written:

| tested                                    | n         | result                                  |
|-------------------------------------------|-----------|-----------------------------------------|
| XP regime band -> Nifty fwd 20d return    | 376 sess  | "Extremely Strong" was the *worst* band  |
| distribution days -> fwd 20d return       | 455 sess  | 7+ beat 3-6; only <=2 was clean          |
| % above 50-DMA, level and slope -> return | 480 sess  | washed-out best; *falling* beat rising   |
| participation -> fwd 20d max drawdown     | 500 sess  | not monotone on median or 10th pct       |
| environment -> breakout win rate          | 13 weeks  | corr +0.07 (XP), -0.02 (breadth)         |

Nothing forecast index direction, and nothing forecast whether the user's own
setups would pay. So the verdict is driven by the one input that needs no
predictive claim at all: **whether the tape is currently paying breakouts,
measured against the break-even win rate implied by the user's own stop and
target.** That is arithmetic on what already happened. It cannot be wrong about
the future because it does not speak about the future.

Everything else the page shows — participation, XP regime, distribution days —
is context displayed *beside* the verdict with its own measured base rates,
never folded into it. Folding an unproven relationship into the headline number
is precisely how a dashboard ends up looking authoritative and being wrong.

Two rules here exist to stop the number lying:

- **The resolution gate.** A week is only eligible once ~all of its signals have
  resolved. Fast winners resolve first, so a part-finished week reads high:
  2026-W31 shows 43.2% at 76% resolved against 24.9% on the like-for-like
  horizon. Ungated, the model would raise exposure exactly when the evidence is
  thinnest.
- **Smoothing over several weeks.** Lag-1 autocorrelation of the weekly win
  rate is -0.04 over 10 pairs: last week says nothing about next week. A single
  week must not swing the verdict.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

# Exposure ladder, in points of win rate relative to break-even.
FULL_EXPOSURE_MARGIN = 5.0   # at or above breakeven + this -> 100%
AT_BREAKEVEN_EXPOSURE = 75
NEAR_MISS_MARGIN = 5.0       # within this far below breakeven -> 50%
FULL_EXPOSURE = 100
NEAR_MISS_EXPOSURE = 50
FLOOR_EXPOSURE = 25

MIN_RESOLUTION_PCT = 97.0    # a week must be this resolved to count
SMOOTHING_WEEKS = 4          # eligible weeks averaged into the verdict
MIN_WEEKS_REQUIRED = 2       # below this there is no verdict at all

DIRECTION_DEADBAND_PTS = 1.5  # win-rate change smaller than this reads "stable"


@dataclass(frozen=True)
class WeekPoint:
    week: str
    win_rate: float
    resolved: int
    signals: int | None
    resolution_pct: float | None
    eligible: bool


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def breakeven_win_rate(stop_pct: float, win_pct: float) -> float | None:
    """Win rate at which the stop/target pair breaks even, ignoring costs."""
    if stop_pct <= 0 or win_pct <= 0:
        return None
    return round(100.0 * stop_pct / (stop_pct + win_pct), 1)


def weekly_points(stats: Mapping[str, Any] | None) -> list[WeekPoint]:
    """Every week with its resolution, flagged for eligibility.

    Win rates come from the **full-horizon** `weeks` block, not the `comparable`
    one, and that choice is load-bearing. The break-even rate this is judged
    against (37.5% for a 3% stop and a 5% target) describes a complete trade:
    it assumes every trade ends either at the target or at the stop. The
    `comparable` block re-scores each week at a short 4-session horizon, where
    most trades have not finished — measured here it runs about 10 points lower
    (19.2% against 29.5% on the same weeks). Comparing that to a whole-trade
    break-even would understate the edge by construction.

    `comparable` exists so that *part-resolved* weeks can be set beside finished
    ones. This model excludes part-resolved weeks outright, so among the weeks
    that survive the gate the full-horizon numbers are already like-for-like.
    """
    stats = stats or {}
    points: list[WeekPoint] = []
    for full in stats.get("weeks") or []:
        key = str(full.get("week") or "")
        if not key:
            continue
        overall_full = full.get("overall") or {}
        resolved = int(_num(overall_full.get("resolved")) or 0)
        signals = _num(full.get("total_signals"))
        resolution = round(100.0 * resolved / signals, 1) if signals else None

        win_rate = _num(overall_full.get("win_rate"))
        if win_rate is None:
            continue

        points.append(
            WeekPoint(
                week=key,
                win_rate=win_rate,
                resolved=resolved,
                signals=int(signals) if signals else None,
                resolution_pct=resolution,
                eligible=resolution is not None and resolution >= MIN_RESOLUTION_PCT,
            )
        )
    return points


def _weighted_mean(points: Sequence[WeekPoint]) -> float | None:
    """Signal-weighted so a thin week cannot outvote a busy one."""
    total = sum(p.resolved for p in points)
    if not points:
        return None
    if total <= 0:
        return round(sum(p.win_rate for p in points) / len(points), 2)
    return round(sum(p.win_rate * p.resolved for p in points) / total, 2)


def exposure_for(win_rate: float, breakeven: float) -> int:
    if win_rate >= breakeven + FULL_EXPOSURE_MARGIN:
        return FULL_EXPOSURE
    if win_rate >= breakeven:
        return AT_BREAKEVEN_EXPOSURE
    if win_rate >= breakeven - NEAR_MISS_MARGIN:
        return NEAR_MISS_EXPOSURE
    return FLOOR_EXPOSURE


def _band(exposure: int) -> str:
    if exposure >= FULL_EXPOSURE:
        return "Full size"
    if exposure >= AT_BREAKEVEN_EXPOSURE:
        return "Constructive"
    if exposure >= NEAR_MISS_EXPOSURE:
        return "Selective"
    return "Defensive"


def compute_exposure(
    stats: Mapping[str, Any] | None,
    *,
    stop_pct: float,
    win_pct: float,
) -> dict[str, Any]:
    """The verdict. Pure function of the breakout statistics and the user's rules."""
    breakeven = breakeven_win_rate(stop_pct, win_pct)
    points = weekly_points(stats)
    eligible = [p for p in points if p.eligible]
    excluded = [p for p in points if not p.eligible]

    if breakeven is None or len(eligible) < MIN_WEEKS_REQUIRED:
        return {
            "available": False,
            "reason": (
                "Not enough fully-resolved weeks to judge whether the tape is paying."
                if breakeven is not None
                else "Stop and target must both be positive to derive a break-even win rate."
            ),
            "breakeven_win_rate": breakeven,
            "weeks_eligible": [p.week for p in eligible],
            "weeks_excluded_unresolved": [p.week for p in excluded],
        }

    window = eligible[-SMOOTHING_WEEKS:]
    smoothed = _weighted_mean(window)
    assert smoothed is not None  # window is non-empty
    exposure = exposure_for(smoothed, breakeven)

    # Direction: the same smoothing one week earlier, so a single week cannot
    # flip the arrow on its own.
    prior_window = eligible[: -1][-SMOOTHING_WEEKS:] if len(eligible) > SMOOTHING_WEEKS else []
    prior = _weighted_mean(prior_window) if prior_window else None
    if prior is None:
        direction, change = "unknown", None
    else:
        change = round(smoothed - prior, 2)
        if change > DIRECTION_DEADBAND_PTS:
            direction = "improving"
        elif change < -DIRECTION_DEADBAND_PTS:
            direction = "deteriorating"
        else:
            direction = "stable"

    shortfall = round(breakeven - smoothed, 2)
    expected_pct = round((smoothed / 100.0) * win_pct - (1 - smoothed / 100.0) * stop_pct, 2)

    return {
        "available": True,
        "exposure_pct": exposure,
        "band": _band(exposure),
        "direction": direction,
        "direction_change_pts": change,
        "win_rate": smoothed,
        "breakeven_win_rate": breakeven,
        "clears_breakeven": smoothed >= breakeven,
        "shortfall_pts": shortfall if shortfall > 0 else 0.0,
        "expected_pct_per_trade": expected_pct,
        "weeks_used": [p.week for p in window],
        "weeks_excluded_unresolved": [p.week for p in excluded],
        "min_resolution_pct": MIN_RESOLUTION_PCT,
        "rules": {
            "stop_pct": stop_pct,
            "win_pct": win_pct,
            "full_exposure_at": round(breakeven + FULL_EXPOSURE_MARGIN, 1),
            "floor_below": round(breakeven - NEAR_MISS_MARGIN, 1),
            "smoothing_weeks": SMOOTHING_WEEKS,
        },
        "basis": "risk rule from measured present conditions — not a forecast",
        "why": (
            f"Over the last {len(window)} fully-resolved weeks, {smoothed}% of breakouts hit "
            f"+{win_pct}% before -{stop_pct}%, against a {breakeven}% break-even. "
            f"Expected {expected_pct}% per trade."
        ),
        "caveat": (
            "Assumes every win exits at the target and every loss at the stop, and ignores "
            "trades that neither resolved. Weeks below "
            f"{MIN_RESOLUTION_PCT}% resolved are excluded because fast winners resolve first "
            "and would read high."
        ),
    }


def exposure_series(
    stats: Mapping[str, Any] | None, *, stop_pct: float, win_pct: float
) -> list[dict[str, Any]]:
    """Per-week exposure, so the sparkline is the same computation as the headline."""
    breakeven = breakeven_win_rate(stop_pct, win_pct)
    if breakeven is None:
        return []
    points = weekly_points(stats)
    out: list[dict[str, Any]] = []
    seen: list[WeekPoint] = []
    for point in points:
        if point.eligible:
            seen.append(point)
        if len(seen) < MIN_WEEKS_REQUIRED:
            continue
        smoothed = _weighted_mean(seen[-SMOOTHING_WEEKS:])
        if smoothed is None:
            continue
        out.append(
            {
                "week": point.week,
                "win_rate": point.win_rate,
                "smoothed_win_rate": smoothed,
                "exposure_pct": exposure_for(smoothed, breakeven),
                "eligible": point.eligible,
                "resolution_pct": point.resolution_pct,
            }
        )
    return out
