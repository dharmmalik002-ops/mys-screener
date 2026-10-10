"""Chart patterns the AI scanner can ask for that the scanner catalog does not cover.

The catalog already has VCP, high-tight flag, Darvas box, pivot breakouts, tight
closes, power base and the rest (`definitions.SCAN_BY_ID`). These are the other
shapes traders type: cup and handle, "just under resistance" / "near the pivot",
ascending triangle, flat base, double bottom, inside day, NR7 and pocket pivot.

Each detector is a pure function over daily data (oldest -> newest, the last
element is today) so it can be tested on hand-built series, and each has a
snapshot wrapper with the scanner evaluator signature
`(snapshot) -> (score, reasons) | None`.

Multi-month shapes read DAILY closes from `close_history` (~200 sessions). They
never fall back to `chart_grid_points`, which is not a daily series
(gotcha 135): a cup measured on 2-session points would be twice as long as it
looks. No history -> no match, never a guess.

Thresholds follow the published descriptions (O'Neil for cup/handle, flat base
and double bottom; Crabel for NR7; Morales & Kacher for pocket pivots) and were
written down before being run on the universe.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from app.models.market import StockSnapshot
from app.services import close_history

Outcome = tuple[float, list[str]] | None

# --- cup and handle ---------------------------------------------------------
CUP_MIN_SESSIONS = 30          # ~6 weeks
CUP_MAX_SESSIONS = 150         # ~30 weeks (the close history holds ~200)
CUP_MIN_DEPTH_PCT = 12.0
CUP_MAX_DEPTH_PCT = 35.0
CUP_PRIOR_ADVANCE_PCT = 20.0   # the cup must follow an uptrend
CUP_PRIOR_LOOKBACK = 60
CUP_RIGHT_LIP_MIN_RATIO = 0.90  # right side recovers to within 10% of the left lip
CUP_RIGHT_LIP_MAX_RATIO = 1.05
CUP_MIN_LOW_ZONE_SHARE = 0.40  # rounded, not a V: a straight V spends exactly 1/3 of its time in the lower third
HANDLE_MIN_SESSIONS = 3
HANDLE_MAX_SESSIONS = 30
HANDLE_MIN_DEPTH_PCT = 2.0
HANDLE_MAX_DEPTH_PCT = 15.0
HANDLE_MAX_VS_CUP = 0.5        # handle no deeper than half the cup...
HANDLE_UPPER_HALF = 0.5        # ...and it forms in the cup's upper half
CUP_MAX_BELOW_PIVOT_PCT = 8.0
CUP_MAX_ABOVE_PIVOT_PCT = 3.0  # a fresh breakout still counts; extended does not

# --- near resistance / pivot -------------------------------------------------
RESISTANCE_LOOKBACK = 120
RESISTANCE_SWING_WING = 3
RESISTANCE_TOUCH_BAND_PCT = 3.0
RESISTANCE_MIN_TOUCHES = 2
RESISTANCE_MIN_TOUCH_GAP = 5
RESISTANCE_DEFAULT_MAX_BELOW_PCT = 5.0

# --- flat base ----------------------------------------------------------------
FLAT_BASE_MIN_SESSIONS = 25    # 5 weeks
FLAT_BASE_MAX_SESSIONS = 65
FLAT_BASE_MAX_RANGE_PCT = 15.0
FLAT_BASE_PRIOR_ADVANCE_PCT = 20.0
FLAT_BASE_MAX_BELOW_HIGH_PCT = 5.0

# --- double bottom --------------------------------------------------------------
DOUBLE_BOTTOM_MIN_GAP = 15
DOUBLE_BOTTOM_MAX_GAP = 100
DOUBLE_BOTTOM_LOW_BAND_PCT = 4.0
DOUBLE_BOTTOM_MIN_MIDDLE_RISE_PCT = 10.0
DOUBLE_BOTTOM_MAX_BELOW_MIDDLE_PCT = 7.0
DOUBLE_BOTTOM_MAX_ABOVE_MIDDLE_PCT = 3.0
DOUBLE_BOTTOM_PRIOR_DECLINE_PCT = 15.0  # a W forms after a correction
DOUBLE_BOTTOM_PRIOR_LOOKBACK = 60


def _swing_highs(closes: list[float], start: int, end: int, wing: int) -> list[int]:
    out = []
    for index in range(max(start, wing), min(end, len(closes) - wing)):
        window = closes[index - wing : index + wing + 1]
        if closes[index] >= max(window) and closes[index] > 0:
            out.append(index)
    return out


def _swing_lows(closes: list[float], start: int, end: int, wing: int) -> list[int]:
    out = []
    for index in range(max(start, wing), min(end, len(closes) - wing)):
        window = closes[index - wing : index + wing + 1]
        if closes[index] <= min(window) and closes[index] > 0:
            out.append(index)
    return out


def _distinct(indices: list[int], gap: int) -> list[int]:
    kept: list[int] = []
    for index in indices:
        if not kept or index - kept[-1] >= gap:
            kept.append(index)
    return kept


@dataclass(frozen=True)
class CupHandle:
    cup_sessions: int
    cup_depth_pct: float
    handle_sessions: int
    handle_depth_pct: float
    pivot: float
    pct_below_pivot: float  # negative once through the pivot


def detect_cup_handle(closes: list[float]) -> CupHandle | None:
    n = len(closes)
    if n < CUP_MIN_SESSIONS + HANDLE_MIN_SESSIONS + 15:
        return None
    last = closes[-1]
    if last <= 0:
        return None
    best: CupHandle | None = None
    # i_r: the right lip, where the handle starts. The handle is everything after.
    for i_r in range(n - 1 - HANDLE_MIN_SESSIONS, max(n - 2 - HANDLE_MAX_SESSIONS, 0), -1):
        right = closes[i_r]
        handle = closes[i_r + 1 :]
        if right <= 0 or not handle:
            continue
        # The right lip is the handle's high: nothing after it closed higher
        # except a fresh breakout today.
        if max(handle[:-1] or [0.0]) > right or last > right * (1 + CUP_MAX_ABOVE_PIVOT_PCT / 100):
            continue
        left_start = max(0, i_r - CUP_MAX_SESSIONS)
        left_end = i_r - CUP_MIN_SESSIONS
        if left_end <= left_start:
            continue
        i_l = max(range(left_start, left_end + 1), key=lambda index: closes[index])
        left = closes[i_l]
        if left <= 0:
            continue
        ratio = right / left
        if ratio < CUP_RIGHT_LIP_MIN_RATIO or ratio > CUP_RIGHT_LIP_MAX_RATIO:
            continue
        cup = closes[i_l : i_r + 1]
        lip = max(left, right)
        if max(cup) > lip:
            continue
        bottom = min(cup)
        i_b = i_l + cup.index(bottom)
        depth = (left - bottom) / left * 100
        if depth < CUP_MIN_DEPTH_PCT or depth > CUP_MAX_DEPTH_PCT:
            continue
        span = i_r - i_l
        if not (0.2 <= (i_b - i_l) / span <= 0.8):
            continue
        low_zone = bottom + (left - bottom) / 3
        if sum(1 for value in cup if value <= low_zone) / len(cup) < CUP_MIN_LOW_ZONE_SHARE:
            continue
        # Prior uptrend into the left lip, measured only where history exists.
        prior = closes[max(0, i_l - CUP_PRIOR_LOOKBACK) : i_l]
        if len(prior) < 15 or min(prior) <= 0 or (left / min(prior) - 1) * 100 < CUP_PRIOR_ADVANCE_PCT:
            continue
        handle_low = min(handle)
        handle_depth = (right - handle_low) / right * 100
        if handle_depth < HANDLE_MIN_DEPTH_PCT or handle_depth > HANDLE_MAX_DEPTH_PCT:
            continue
        if handle_depth > depth * HANDLE_MAX_VS_CUP:
            continue
        if handle_low < bottom + (left - bottom) * HANDLE_UPPER_HALF:
            continue
        below = (right - last) / right * 100
        if below > CUP_MAX_BELOW_PIVOT_PCT:
            continue
        found = CupHandle(
            cup_sessions=span,
            cup_depth_pct=round(depth, 1),
            handle_sessions=len(handle),
            handle_depth_pct=round(handle_depth, 1),
            pivot=round(right, 2),
            pct_below_pivot=round(below, 1),
        )
        if best is None or abs(found.pct_below_pivot) < abs(best.pct_below_pivot):
            best = found
    return best


@dataclass(frozen=True)
class Resistance:
    level: float
    touches: int
    pct_below: float
    sessions: int  # how long the level has capped price
    higher_lows: int


def detect_resistance(closes: list[float], *, max_below_pct: float = RESISTANCE_DEFAULT_MAX_BELOW_PCT) -> Resistance | None:
    """A ceiling price has turned back from at least twice, with today's close
    just under it. The level is the highest prior close; touches are separate
    swing highs within `RESISTANCE_TOUCH_BAND_PCT` of it."""
    n = len(closes)
    if n < 30:
        return None
    last = closes[-1]
    window_start = max(0, n - 1 - RESISTANCE_LOOKBACK)
    prior = closes[window_start : n - 1]
    if not prior or last <= 0:
        return None
    level = max(prior)
    if level <= 0 or last > level:
        return None
    below = (level - last) / level * 100
    if below > max_below_pct:
        return None
    band = level * (1 - RESISTANCE_TOUCH_BAND_PCT / 100)
    swings = [i for i in _swing_highs(closes, window_start, n - 1, RESISTANCE_SWING_WING) if closes[i] >= band]
    touches = _distinct(swings, RESISTANCE_MIN_TOUCH_GAP)
    if len(touches) < RESISTANCE_MIN_TOUCHES:
        return None
    first = touches[0]
    lows = _swing_lows(closes, first, n - 1, RESISTANCE_SWING_WING)
    lows = _distinct(lows, RESISTANCE_MIN_TOUCH_GAP)
    higher = 0
    for a, b in zip(lows, lows[1:]):
        if closes[b] > closes[a] * 1.01:
            higher += 1
        else:
            higher = 0
    return Resistance(
        level=round(level, 2),
        touches=len(touches),
        pct_below=round(below, 1),
        sessions=n - 1 - first,
        higher_lows=higher,
    )


@dataclass(frozen=True)
class FlatBase:
    sessions: int
    range_pct: float
    pct_below_high: float


def detect_flat_base(closes: list[float]) -> FlatBase | None:
    n = len(closes)
    last = closes[-1] if closes else 0
    if n < FLAT_BASE_MIN_SESSIONS + 20 or last <= 0:
        return None
    best: FlatBase | None = None
    for length in range(FLAT_BASE_MIN_SESSIONS, min(FLAT_BASE_MAX_SESSIONS, n - 20) + 1):
        base = closes[-length:]
        high, low = max(base), min(base)
        if low <= 0:
            continue
        span = (high - low) / high * 100
        if span > FLAT_BASE_MAX_RANGE_PCT:
            break  # longer windows only widen
        below = (high - last) / high * 100
        if below > FLAT_BASE_MAX_BELOW_HIGH_PCT:
            continue
        prior = closes[max(0, n - length - 60) : n - length]
        if len(prior) < 15 or min(prior) <= 0 or (base[0] / min(prior) - 1) * 100 < FLAT_BASE_PRIOR_ADVANCE_PCT:
            continue
        best = FlatBase(sessions=length, range_pct=round(span, 1), pct_below_high=round(below, 1))
    return best


@dataclass(frozen=True)
class DoubleBottom:
    sessions: int
    first_low: float
    second_low: float
    middle_high: float
    pct_below_middle: float
    undercut: bool


def detect_double_bottom(closes: list[float]) -> DoubleBottom | None:
    n = len(closes)
    last = closes[-1] if closes else 0
    if n < DOUBLE_BOTTOM_MIN_GAP + 15 or last <= 0:
        return None
    lows = _swing_lows(closes, max(0, n - 1 - DOUBLE_BOTTOM_MAX_GAP - 40), n - 2, 5)
    best: DoubleBottom | None = None
    for second in reversed(lows):
        for first in lows:
            gap = second - first
            if gap < DOUBLE_BOTTOM_MIN_GAP or gap > DOUBLE_BOTTOM_MAX_GAP:
                continue
            a, b = closes[first], closes[second]
            if abs(b - a) / a * 100 > DOUBLE_BOTTOM_LOW_BAND_PCT:
                continue
            # Both legs are the pattern's lows: nothing between them closed lower.
            if min(closes[first : second + 1]) < min(a, b):
                continue
            middle = max(closes[first : second + 1])
            if (middle / max(a, b) - 1) * 100 < DOUBLE_BOTTOM_MIN_MIDDLE_RISE_PCT:
                continue
            # The second leg is the setup's low: nothing after it broke lower.
            if min(closes[second:]) < b:
                continue
            prior = closes[max(0, first - DOUBLE_BOTTOM_PRIOR_LOOKBACK) : first]
            if len(prior) < 15 or (max(prior) / a - 1) * 100 < DOUBLE_BOTTOM_PRIOR_DECLINE_PCT:
                continue
            below = (middle - last) / middle * 100
            if below > DOUBLE_BOTTOM_MAX_BELOW_MIDDLE_PCT or below < -DOUBLE_BOTTOM_MAX_ABOVE_MIDDLE_PCT:
                continue
            found = DoubleBottom(
                sessions=n - 1 - first,
                first_low=round(a, 2),
                second_low=round(b, 2),
                middle_high=round(middle, 2),
                pct_below_middle=round(below, 1),
                undercut=b < a,
            )
            if best is None or abs(found.pct_below_middle) < abs(best.pct_below_middle):
                best = found
        if best is not None:
            break
    return best


def is_inside_day(highs: list[float], lows: list[float]) -> bool:
    if len(highs) < 2 or len(lows) < 2:
        return False
    return highs[-1] <= highs[-2] and lows[-1] >= lows[-2] and highs[-1] > lows[-1]


def is_nr7(highs: list[float], lows: list[float]) -> bool:
    if len(highs) < 7 or len(lows) < 7:
        return False
    ranges = [h - l for h, l in zip(highs[-7:], lows[-7:])]
    if any(r <= 0 for r in ranges):
        return False
    return ranges[-1] < min(ranges[:-1])


def is_pocket_pivot(closes: list[float], volumes: list[int]) -> bool:
    """An up day whose volume beats every down day's volume of the prior ten."""
    if len(closes) < 12 or len(volumes) < 11:
        return False
    closes = closes[-12:]
    volumes = volumes[-11:]
    if closes[-1] <= closes[-2] or volumes[-1] <= 0:
        return False
    down = [volumes[i] for i in range(10) if closes[i + 1] < closes[i]]
    return bool(down) and volumes[-1] > max(down)


# --- snapshot wrappers -----------------------------------------------------------


def _daily(snapshot: StockSnapshot) -> list[float]:
    return close_history.closes_for(snapshot)


def _cup_handle(snapshot: StockSnapshot) -> Outcome:
    found = detect_cup_handle(_daily(snapshot))
    if found is None:
        return None
    where = (
        f"{found.pct_below_pivot:.1f}% below the {found.pivot:g} pivot"
        if found.pct_below_pivot >= 0
        else f"{abs(found.pct_below_pivot):.1f}% through the {found.pivot:g} pivot"
    )
    score = 80 + max(0.0, 8 - abs(found.pct_below_pivot)) * 2 + max(0.0, 15 - found.handle_depth_pct) * 0.5
    return round(score, 2), [
        f"Cup {found.cup_sessions}d, {found.cup_depth_pct:.0f}% deep",
        f"Handle {found.handle_sessions}d, {found.handle_depth_pct:.1f}% deep",
        where.capitalize(),
    ]


def _near_resistance(snapshot: StockSnapshot) -> Outcome:
    found = detect_resistance(_daily(snapshot))
    if found is None:
        return None
    score = 75 + found.touches * 3 + max(0.0, 5 - found.pct_below) * 2
    return round(score, 2), [
        f"{found.pct_below:.1f}% below resistance at {found.level:g}",
        f"Turned back {found.touches} times over {found.sessions} sessions",
    ]


def _ascending_triangle(snapshot: StockSnapshot) -> Outcome:
    found = detect_resistance(_daily(snapshot))
    if found is None or found.higher_lows < 2:
        return None
    score = 78 + found.higher_lows * 3 + max(0.0, 5 - found.pct_below) * 2
    return round(score, 2), [
        f"Flat top at {found.level:g} ({found.touches} touches), {found.pct_below:.1f}% below",
        f"{found.higher_lows + 1} rising lows underneath",
    ]


def _flat_base(snapshot: StockSnapshot) -> Outcome:
    found = detect_flat_base(_daily(snapshot))
    if found is None:
        return None
    score = 76 + max(0.0, 15 - found.range_pct) + max(0.0, 5 - found.pct_below_high)
    return round(score, 2), [
        f"Flat base {found.sessions}d, {found.range_pct:.1f}% range",
        f"{found.pct_below_high:.1f}% below the base high",
    ]


def _double_bottom(snapshot: StockSnapshot) -> Outcome:
    found = detect_double_bottom(_daily(snapshot))
    if found is None:
        return None
    score = 76 + (3 if found.undercut else 0) + max(0.0, 7 - abs(found.pct_below_middle))
    where = (
        f"{found.pct_below_middle:.1f}% below the middle peak {found.middle_high:g}"
        if found.pct_below_middle >= 0
        else f"{abs(found.pct_below_middle):.1f}% through the middle peak {found.middle_high:g}"
    )
    return round(score, 2), [
        f"Lows {found.first_low:g} and {found.second_low:g}" + (" (undercut)" if found.undercut else ""),
        where.capitalize(),
    ]


def _inside_day(snapshot: StockSnapshot) -> Outcome:
    if not is_inside_day(list(snapshot.recent_highs or []), list(snapshot.recent_lows or [])):
        return None
    return 70.0, ["Inside day: range inside yesterday's"]


def _nr7(snapshot: StockSnapshot) -> Outcome:
    if not is_nr7(list(snapshot.recent_highs or []), list(snapshot.recent_lows or [])):
        return None
    return 70.0, ["NR7: narrowest range of the last 7 sessions"]


def _pocket_pivot(snapshot: StockSnapshot) -> Outcome:
    if not is_pocket_pivot(list(snapshot.recent_closes or []), [int(v) for v in (snapshot.recent_volumes or []) if v is not None]):
        return None
    return 74.0, ["Pocket pivot: up-day volume above every down day of the last 10"]


@dataclass(frozen=True)
class ChartPattern:
    id: str
    name: str
    description: str
    evaluator: Callable[[StockSnapshot], Outcome]


CHART_PATTERNS: tuple[ChartPattern, ...] = (
    ChartPattern(
        "cup-handle",
        "Cup and Handle",
        "Rounded 6-30 week cup 12-35% deep after a 20%+ advance, right side back within 10% of the left lip, "
        "then a 3-30 session handle 2-15% deep in the cup's upper half; price within 8% under the handle high or just through it.",
        _cup_handle,
    ),
    ChartPattern(
        "near-resistance",
        "Near Resistance / Pivot",
        "Close within 5% below a ceiling (the highest close of the last ~6 months) that price has turned back from at least twice.",
        _near_resistance,
    ),
    ChartPattern(
        "ascending-triangle",
        "Ascending Triangle",
        "A flat ceiling touched at least twice with three or more rising lows underneath; price within 5% of the ceiling.",
        _ascending_triangle,
    ),
    ChartPattern(
        "flat-base",
        "Flat Base",
        "5-13 weeks moving sideways in a range of 15% or less after a 20%+ advance; price within 5% of the base high.",
        _flat_base,
    ),
    ChartPattern(
        "double-bottom",
        "Double Bottom",
        "W shape after a 15%+ decline: two lows within 4% of each other 3-20 weeks apart, a 10%+ middle peak, price within 7% under that peak or just through it.",
        _double_bottom,
    ),
    ChartPattern("inside-day", "Inside Day", "Today's high-low range sits inside yesterday's.", _inside_day),
    ChartPattern("nr7", "NR7", "Today's range is the narrowest of the last seven sessions.", _nr7),
    ChartPattern(
        "pocket-pivot",
        "Pocket Pivot",
        "An up day on volume larger than any down day's volume in the prior ten sessions.",
        _pocket_pivot,
    ),
)

CHART_PATTERN_BY_ID = {pattern.id: pattern for pattern in CHART_PATTERNS}
