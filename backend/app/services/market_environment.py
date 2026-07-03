"""Follow-through health of the CURRENT tape — are setups getting paid?

Breadth (the XP score) counts how many stocks went up. This module measures
how the market is treating SETUPS: do breakouts hold, do leaders respect
their EMAs, is strength being bought or sold into. That distinction is what
separates "green tape" from "tradeable tape" — a market can have fine breadth
while every breakout gets distributed.

All inputs come from the day's snapshots (each carries ~20 sessions of
closes/highs/lows/volumes plus EMAs), so every component is a counted fact
from committed EOD data — nothing subjective, nothing estimated.
"""
from __future__ import annotations

from typing import Callable

from app.models.market import StockSnapshot

# Liquidity floor so illiquid prints don't pollute behavioral stats.
_MIN_AVG_VOL = 50_000
_MIN_PRICE = 30.0


def _eligible(snapshots: list[StockSnapshot]) -> list[StockSnapshot]:
    return [
        s for s in snapshots
        if (s.avg_volume_20d or 0) >= _MIN_AVG_VOL and s.last_price > _MIN_PRICE
        and len(s.recent_closes) >= 18 and len(s.recent_highs) >= 18 and len(s.recent_lows) >= 18
    ]


def _breakout_index(closes: list[float], highs: list[float], at: int) -> bool:
    """True when the close AT index `at` cleared the highest prior high in the
    window — a rolling-window proxy for a 20-day-high breakout."""
    if at < 8:
        return False  # not enough prior context inside the window
    prior_high = max(highs[:at])
    return prior_high > 0 and closes[at] > prior_high and closes[at - 1] <= prior_high


def _pct(part: int, whole: int) -> float | None:
    return round(part / whole * 100, 1) if whole > 0 else None


def breakout_followthrough(snapshots: list[StockSnapshot], sessions_ago: int) -> dict:
    """Of the stocks that broke out `sessions_ago` sessions back, what % still
    close above their breakout level today?"""
    held = 0
    total = 0
    for s in snapshots:
        closes = [float(v) for v in s.recent_closes]
        highs = [float(v) for v in s.recent_highs]
        n = min(len(closes), len(highs))
        at = n - 1 - sessions_ago
        if at < 8:
            continue
        if not _breakout_index(closes[:n], highs[:n], at):
            continue
        total += 1
        breakout_level = max(highs[:at])
        if closes[n - 1] > breakout_level:
            held += 1
    return {"held_pct": _pct(held, total), "count": total}


def breakout_close_quality(snapshots: list[StockSnapshot]) -> dict:
    """Today's breakout attempts: strong closes (top quarter of range) vs
    faded closes (below midpoint). Sellers using strength show up here a day
    before follow-through stats roll over."""
    strong = 0
    faded = 0
    total = 0
    for s in snapshots:
        closes = [float(v) for v in s.recent_closes]
        highs = [float(v) for v in s.recent_highs]
        n = min(len(closes), len(highs))
        if not _breakout_index(closes[:n], highs[:n], n - 1):
            continue
        rng = (s.day_high or 0) - (s.day_low or 0)
        if rng <= 0:
            continue
        loc = (s.last_price - s.day_low) / rng
        total += 1
        if loc >= 0.75:
            strong += 1
        elif loc < 0.5:
            faded += 1
    return {"strong_pct": _pct(strong, total), "faded_pct": _pct(faded, total), "count": total}


def leader_ema_health(snapshots: list[StockSnapshot], is_leader: Callable[[StockSnapshot], bool]) -> dict:
    """Stage-2 leaders vs their 10/21 EMAs: holding %, and of those that
    TOUCHED the 21 EMA in the last 3 sessions — bounced or sliced through?"""
    leaders = [s for s in snapshots if is_leader(s)]
    above10 = above20 = 0
    touched = bounced = 0
    for s in leaders:
        e10 = s.ema10 or 0
        e20 = s.ema20 or 0
        if e10 > 0 and s.last_price > e10:
            above10 += 1
        if e20 > 0 and s.last_price > e20:
            above20 += 1
        if e20 > 0:
            recent_lows = [float(v) for v in s.recent_lows][-3:]
            if recent_lows and min(recent_lows) <= e20 * 1.005:
                touched += 1
                if s.last_price > e20:
                    bounced += 1
    return {
        "leaders": len(leaders),
        "above_ema10_pct": _pct(above10, len(leaders)),
        "above_ema21_pct": _pct(above20, len(leaders)),
        "ema21_touches": touched,
        "ema21_bounce_pct": _pct(bounced, touched),
    }


def leader_volume_pressure(snapshots: list[StockSnapshot], is_leader: Callable[[StockSnapshot], bool]) -> dict:
    """Among leaders only: down days on above-average volume (distribution)
    vs up days on above-average volume (accumulation). Institutions leave the
    leaders first — the market-wide A/D line sees it last."""
    dist = accum = 0
    leaders = 0
    for s in snapshots:
        if not is_leader(s):
            continue
        leaders += 1
        avg = s.avg_volume_20d or 0
        if avg <= 0 or s.volume <= avg:
            continue
        if s.change_pct <= -0.5:
            dist += 1
        elif s.change_pct >= 0.5:
            accum += 1
    total = dist + accum
    return {
        "distribution": dist,
        "accumulation": accum,
        "accumulation_share_pct": _pct(accum, total),
        "leaders": leaders,
    }


def range_expansion_direction(snapshots: list[StockSnapshot]) -> dict:
    """Of today's wide-range days (range > 1.5x ATR14): % that closed up.
    Big candles show where committed money pushed."""
    up = down = 0
    for s in snapshots:
        atr = s.atr14 or 0
        rng = (s.day_high or 0) - (s.day_low or 0)
        if atr <= 0 or rng <= atr * 1.5:
            continue
        if s.change_pct > 0:
            up += 1
        elif s.change_pct < 0:
            down += 1
    total = up + down
    return {"up": up, "down": down, "up_share_pct": _pct(up, total)}


def thrust_and_volume(snapshots: list[StockSnapshot]) -> dict:
    """Momentum thrust (4%+ moves up vs down) and up/down volume ratio —
    the raw aggression tape-reading numbers."""
    up4 = sum(1 for s in snapshots if s.change_pct >= 4)
    down4 = sum(1 for s in snapshots if s.change_pct <= -4)
    up_vol = sum(s.volume for s in snapshots if s.change_pct > 0)
    down_vol = sum(s.volume for s in snapshots if s.change_pct < 0)
    fresh_high = 0
    fresh_low = 0
    for s in snapshots:
        highs = [float(v) for v in s.recent_highs]
        lows = [float(v) for v in s.recent_lows]
        if len(highs) >= 10 and s.day_high and s.day_high >= max(highs):
            fresh_high += 1
        if len(lows) >= 10 and s.day_low and s.day_low <= min(lows):
            fresh_low += 1
    return {
        "up_4pct": up4,
        "down_4pct": down4,
        "updown_volume_ratio": round(up_vol / down_vol, 2) if down_vol > 0 else None,
        "fresh_20d_highs": fresh_high,
        "fresh_20d_lows": fresh_low,
    }


def _score_component(value: float | None, lo: float, hi: float) -> float | None:
    """Map value linearly to 0..100 between lo (bad) and hi (good)."""
    if value is None:
        return None
    if hi == lo:
        return None
    frac = (value - lo) / (hi - lo)
    return max(0.0, min(1.0, frac)) * 100


def compute_market_environment(
    snapshots: list[StockSnapshot],
    is_leader: Callable[[StockSnapshot], bool],
) -> dict:
    universe = _eligible(snapshots)

    ft1 = breakout_followthrough(universe, 1)
    ft3 = breakout_followthrough(universe, 3)
    ft5 = breakout_followthrough(universe, 5)
    quality = breakout_close_quality(universe)
    ema = leader_ema_health(universe, is_leader)
    pressure = leader_volume_pressure(universe, is_leader)
    expansion = range_expansion_direction(universe)
    thrust = thrust_and_volume(universe)

    # Composite: weighted blend of the behavior components, each normalized
    # against honest good/bad anchors for this universe.
    parts: list[tuple[float, float]] = []  # (score, weight)
    for score, weight in (
        (_score_component(ft3.get("held_pct"), 30, 70), 0.30),
        (_score_component(quality.get("strong_pct"), 20, 60), 0.15),
        (_score_component(ema.get("above_ema21_pct"), 40, 85), 0.20),
        (_score_component(pressure.get("accumulation_share_pct"), 30, 70), 0.15),
        (_score_component(expansion.get("up_share_pct"), 30, 70), 0.10),
        (_score_component(
            None if thrust.get("up_4pct") is None or (thrust["up_4pct"] + thrust["down_4pct"]) == 0
            else thrust["up_4pct"] / (thrust["up_4pct"] + thrust["down_4pct"]) * 100,
            30, 70,
        ), 0.10),
    ):
        if score is not None:
            parts.append((score, weight))
    total_weight = sum(w for _, w in parts)
    score = round(sum(s * w for s, w in parts) / total_weight, 1) if total_weight else None

    if score is None:
        verdict = "Insufficient data"
    elif score >= 70:
        verdict = "Press"
    elif score >= 55:
        verdict = "Selective"
    elif score >= 40:
        verdict = "Protect"
    else:
        verdict = "Stand Aside"

    return {
        "score": score,
        "verdict": verdict,
        "universe": len(universe),
        "followthrough": {"d1": ft1, "d3": ft3, "d5": ft5},
        "close_quality": quality,
        "ema_health": ema,
        "volume_pressure": pressure,
        "range_expansion": expansion,
        "thrust": thrust,
    }
