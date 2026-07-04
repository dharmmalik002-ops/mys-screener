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


def has_trustworthy_levels(s: StockSnapshot) -> bool:
    """Guards against the stale-seed trap: for ~100 BSE-listed names the daily
    patch carries fresh PRICES but no fresh indicator block, so 52w levels and
    MAs silently come from a months-old baked snapshot. A stock's 52w high can
    never sit below its own 20-day high (nor its 52w low above its 20-day
    low) — stocks failing that consistency test are excluded from every
    level-based statistic instead of being counted wrongly."""
    highs = [float(v) for v in s.recent_highs if v]
    lows = [float(v) for v in s.recent_lows if v]
    if len(highs) < 10 or len(lows) < 10:
        return False
    h52 = s.high_52w or 0
    l52 = s.low_52w or 0
    if h52 <= 0 or l52 <= 0:
        return False
    if h52 < max(highs) * 0.999 or l52 > min(lows) * 1.001:
        return False
    return bool(s.sma200 and s.sma50 and s.ema20)


def _eligible(snapshots: list[StockSnapshot]) -> list[StockSnapshot]:
    return [
        s for s in snapshots
        if (s.avg_volume_20d or 0) >= _MIN_AVG_VOL and s.last_price > _MIN_PRICE
        and len(s.recent_closes) >= 18 and len(s.recent_highs) >= 18 and len(s.recent_lows) >= 18
        and has_trustworthy_levels(s)
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
    events = structural_breakout_events(universe)
    structural = summarize_structural_breakouts(events)
    quality = breakout_close_quality(universe)
    ema = leader_ema_health(universe, is_leader)
    pressure = leader_volume_pressure(universe, is_leader)
    expansion = range_expansion_direction(universe)
    thrust = thrust_and_volume(universe)

    # Composite: weighted blend of the behavior components, each normalized
    # against honest good/bad anchors for this universe.
    parts: list[tuple[float, float]] = []  # (score, weight)
    for score, weight in (
        # Structural (base) breakouts are what a swing trader actually buys —
        # momentum-noise 20d-high clears are excluded from the score entirely.
        (_score_component(structural.get("held_pct"), 35, 75), 0.35),
        (_score_component(quality.get("strong_pct"), 20, 60), 0.15),
        (_score_component(ema.get("above_ema21_pct"), 40, 85), 0.20),
        (_score_component(pressure.get("accumulation_share_pct"), 30, 70), 0.15),
        (_score_component(expansion.get("up_share_pct"), 30, 70), 0.05),
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
        "structural": structural,
        "followthrough": {"d1": ft1, "d3": ft3, "d5": ft5},
        "close_quality": quality,
        "ema_health": ema,
        "volume_pressure": pressure,
        "range_expansion": expansion,
        "thrust": thrust,
    }

def structural_breakout_events(snapshots: list[StockSnapshot], max_sessions_ago: int = 12) -> list[dict]:
    """Base breakouts, not momentum noise: the crossing must end a real
    consolidation (>= 8 sessions with no high at/above the pivot before the
    cross; most bases extend past the 20-session window and are labelled
    "4w+"). Each event carries where the stock stands vs its pivot TODAY —
    the raw material for "breakouts are working / failing" with names."""
    events: list[dict] = []
    for s in snapshots:
        closes = [float(v) for v in s.recent_closes]
        highs = [float(v) for v in s.recent_highs]
        n = min(len(closes), len(highs))
        if n < 18:
            continue
        # most recent qualifying crossing, newest first
        for at in range(n - 1, max(7, n - 1 - max_sessions_ago) - 1, -1):
            if at < 8:
                break
            prior_high = max(highs[:at])
            if prior_high <= 0 or closes[at] <= prior_high or closes[at - 1] > prior_high:
                continue
            # consolidation length: sessions since the window last printed a
            # high at/above the pivot before the crossing day
            last_touch = -1
            for j in range(at - 1, -1, -1):
                if highs[j] >= prior_high * 0.995:
                    last_touch = j
                    break
            consol = at - last_touch - 1 if last_touch >= 0 else at
            window_limited = last_touch < 0
            if consol < 8 and not window_limited:
                break  # continuation pop, not a base breakout — skip stock
            pct_vs_pivot = (closes[n - 1] / prior_high - 1) * 100
            events.append({
                "symbol": s.symbol,
                "sessions_ago": n - 1 - at,
                "base_len_label": "4w+" if window_limited else f"{consol}s",
                "pivot": round(prior_high, 2),
                "pct_vs_pivot": round(pct_vs_pivot, 2),
                "held": closes[n - 1] > prior_high,
                "change_pct_today": round(s.change_pct, 2),
            })
            break  # one event per stock (the most recent)
    return events


def summarize_structural_breakouts(events: list[dict]) -> dict:
    """Aggregate follow-through on REAL base breakouts (excluding today's,
    which have had no chance to fail yet)."""
    seasoned = [e for e in events if e["sessions_ago"] >= 1]
    held = [e for e in seasoned if e["held"]]
    return {
        "events": len(seasoned),
        "held_pct": _pct(len(held), len(seasoned)),
        "back_in_base_pct": _pct(len(seasoned) - len(held), len(seasoned)),
    }


def leader_ema_examples(snapshots: list[StockSnapshot], is_leader) -> dict:
    """Named leaders that tested the 21 EMA in the last 3 sessions — bounced
    (still above) vs sliced (closed below)."""
    bounced: list[dict] = []
    sliced: list[dict] = []
    for s in snapshots:
        if not is_leader(s):
            continue
        e20 = s.ema20 or 0
        if e20 <= 0:
            continue
        recent_lows = [float(v) for v in s.recent_lows][-3:]
        if not recent_lows or min(recent_lows) > e20 * 1.005:
            continue
        entry = {"symbol": s.symbol, "pct_vs_ema21": round((s.last_price / e20 - 1) * 100, 2)}
        (bounced if s.last_price > e20 else sliced).append(entry)
    bounced.sort(key=lambda e: -e["pct_vs_ema21"])
    sliced.sort(key=lambda e: e["pct_vs_ema21"])
    return {"bounced": bounced[:12], "sliced": sliced[:12]}

def market_posture(snapshots: list[StockSnapshot]) -> dict:
    """The context numbers a swing trader checks before anything else:
    advance/decline, fresh 52-week highs vs lows, and % of liquid stocks
    above the 21 EMA / 50 SMA / 200 SMA."""
    universe = [s for s in snapshots if (s.avg_volume_20d or 0) >= 50000 and s.last_price > 30]
    advances = sum(1 for s in universe if s.change_pct > 0)
    declines = sum(1 for s in universe if s.change_pct < 0)
    # Levels (52w extremes, MAs) only from stocks whose indicator data passes
    # the freshness consistency test — stale-seed BSE rows are excluded.
    leveled = [s for s in universe if has_trustworthy_levels(s)]
    new_high = sum(
        1 for s in leveled
        if s.high_52w and s.day_high and s.day_high >= s.high_52w * 0.999
    )
    new_low = sum(
        1 for s in leveled
        if s.low_52w and s.day_low and s.day_low <= s.low_52w * 1.001
    )
    def _above(attr: str) -> float | None:
        vals = [(s.last_price, getattr(s, attr) or 0) for s in leveled if getattr(s, attr)]
        if not vals:
            return None
        return round(sum(1 for price, level in vals if price > level) / len(vals) * 100, 1)
    return {
        "universe": len(universe),
        "leveled_universe": len(leveled),
        "advances": advances,
        "declines": declines,
        "new_52w_highs": new_high,
        "new_52w_lows": new_low,
        "above_ema21_pct": _above("ema20"),
        "above_sma50_pct": _above("sma50"),
        "above_sma200_pct": _above("sma200"),
    }


def classify_position(s: StockSnapshot, event: dict | None) -> tuple[str, str]:
    """Mechanical category + action for an open position, worst condition
    first. `event` is this symbol's structural breakout event if one exists."""
    if not has_trustworthy_levels(s):
        return ("Data limited", "This name's MA/52-week levels aren't refreshed daily (BSE-history gap) — judge it on the chart, not on these rules.")
    price = s.last_price
    sma200 = s.sma200 or 0
    sma50 = s.sma50 or 0
    ema21 = s.ema20 or 0
    if event is not None and not event.get("held", True):
        return ("Failed breakout", f"Back inside the base ({event['pct_vs_pivot']:+.1f}% vs pivot {event['pivot']}) — the setup is invalid; exit or cut to a tracking position.")
    if sma200 > 0 and price < sma200:
        return ("Broken trend", "Below the 200 SMA — Stage 4 territory; this is not a swing hold, exit.")
    if sma50 > 0 and price < sma50:
        return ("Damaged", "Below the 50 SMA — trend damaged; reduce, and demand a fast reclaim.")
    if ema21 > 0 and price < ema21:
        return ("Testing 21 EMA", "Closed below the 21 EMA — normal once, a leak if it lingers; watch for a reclaim within 2-3 sessions.")
    if event is not None and event.get("held"):
        return ("Breakout working", f"{event['pct_vs_pivot']:+.1f}% above its pivot ({event['pivot']}) — hold, stop under the pivot / 21 EMA.")
    if ema21 > 0 and price > ema21 * 1.25:
        return ("Extended", "More than 25% above the 21 EMA — hold the core, consider partials into strength; do not add here.")
    return ("Healthy trend", "Riding above the 21 EMA with the moving averages stacked — hold, trail under the 21 EMA.")


def _focus_candidate(s: StockSnapshot, event: dict | None, min_rs: int) -> dict | None:
    """A single focus row with a reason and a concrete buy plan, or None if
    the stock doesn't clear the uptrend/leadership gates."""
    if (s.avg_volume_20d or 0) < 75000 or s.last_price <= 30:
        return None
    if not has_trustworthy_levels(s):
        return None
    ema21 = s.ema20 or 0
    sma50 = s.sma50 or 0
    sma200 = s.sma200 or 0
    if not (ema21 and sma50 and sma200):
        return None
    if s.last_price < sma50 or s.last_price < sma200 or sma50 < sma200:
        return None
    if not s.rs_eligible or s.rs_rating < min_rs:
        return None
    dist_high = s.pct_from_52w_high
    if dist_high > 18:
        return None

    reasons: list[str] = [f"RS {s.rs_rating}"]
    score = s.rs_rating * 0.5 + max(0.0, 18 - dist_high) * 2
    event = event if event and event.get("held") else None
    near_ema = ema21 > 0 and abs(s.last_price / ema21 - 1) <= 0.03
    tight = bool(s.adr_pct_20 and s.adr_pct_20 <= 4)

    # Setup label + concrete buy plan (entry trigger, stop, note).
    pivot = float(event["pivot"]) if event else None
    if event:
        setup = "Holding breakout"
        score += 12
        reasons.append(f"held breakout {event['pct_vs_pivot']:+.1f}% vs pivot")
        entry = f"add on strength above {s.last_price:.2f}; ideal was the pivot {pivot:.2f}"
        stop = f"below the pivot / 21 EMA ({ema21:.2f})"
        note = "Already extended from the base — starter size only if chasing; full size only on the next tight rest."
    elif near_ema:
        setup = "21 EMA reset"
        score += 9
        reasons.append("pulled back to the 21 EMA")
        entry = f"buy the reclaim of today's high once it turns up off the 21 EMA ({ema21:.2f})"
        stop = f"below the pullback low / under the 21 EMA ({ema21:.2f})"
        note = "Textbook continuation entry — tight stop, add on the follow-through day."
    else:
        setup = "Uptrend, building a base"
        entry = f"wait for a breakout above the recent swing high; don't buy mid-range"
        stop = "below the base low once it breaks out"
        note = "No trigger yet — put it on the watchlist and let it set up."
    if tight:
        score += 3
        reasons.append(f"tight (ADR {s.adr_pct_20:.1f}%)")
    reasons.append(f"{dist_high:.1f}% off 52w high")

    return {
        "symbol": s.symbol,
        "name": s.name,
        "sector": s.sector,
        "last_price": round(s.last_price, 2),
        "change_pct": round(s.change_pct, 2),
        "rs_rating": s.rs_rating,
        "pct_from_52w_high": round(dist_high, 1),
        "ema21": round(ema21, 2),
        "pivot": round(pivot, 2) if pivot else None,
        "setup": setup,
        "score": round(score, 1),
        "reasons": reasons,
        "entry": entry,
        "stop": stop,
        "buy_note": note,
    }


def build_focus_list(snapshots: list[StockSnapshot], events: list[dict], limit: int = 45) -> list[dict]:
    """Strong candidates for the coming week: leaders in intact uptrends near
    their highs, each with a setup label and a concrete buy plan. Held
    breakouts and EMA resets rank first. RS gate relaxes from 80 to 70 only if
    needed to reach `limit` names."""
    event_by_symbol = {e["symbol"]: e for e in events}
    rows: list[dict] = []
    for min_rs in (80, 72):
        rows = []
        for s in snapshots:
            row = _focus_candidate(s, event_by_symbol.get(s.symbol), min_rs)
            if row is not None:
                rows.append(row)
        if len(rows) >= limit:
            break
    rows.sort(key=lambda r: -r["score"])
    return rows[:limit]


def leaders_list(snapshots: list[StockSnapshot], is_leader) -> list[dict]:
    """Every Stage-2 leader (Minervini 5M template) with the trend facts a
    swing trader reads at a glance — for the clickable Leaders grid."""
    out: list[dict] = []
    for s in snapshots:
        if not is_leader(s) or not has_trustworthy_levels(s):
            continue
        ema21 = s.ema20 or 0
        out.append({
            "symbol": s.symbol,
            "name": s.name,
            "sector": s.sector,
            "last_price": round(s.last_price, 2),
            "change_pct": round(s.change_pct, 2),
            "rs_rating": s.rs_rating if s.rs_eligible else None,
            "pct_from_52w_high": round(s.pct_from_52w_high, 1),
            "above_ema21": bool(ema21 and s.last_price > ema21),
        })
    out.sort(key=lambda r: (-(r["rs_rating"] or 0), r["pct_from_52w_high"]))
    return out


def sector_breakout_watch(snapshots: list[StockSnapshot], events: list[dict], top_sectors: list[str], limit: int = 24) -> list[dict]:
    """Stocks in the LEADING sectors that are coiled just under a base pivot
    (within 5% below the recent swing high) — the next breakouts to fire if
    the leading sectors keep working."""
    if not top_sectors:
        return []
    leading = set(top_sectors)
    event_syms = {e["symbol"] for e in events if e.get("held")}
    rows: list[dict] = []
    for s in snapshots:
        if s.sector not in leading:
            continue
        if (s.avg_volume_20d or 0) < 75000 or s.last_price <= 30:
            continue
        if not has_trustworthy_levels(s):
            continue
        sma50 = s.sma50 or 0
        sma200 = s.sma200 or 0
        if not (sma50 and sma200) or s.last_price < sma200 or sma50 < sma200:
            continue
        if s.symbol in event_syms:
            continue  # already broken out — belongs in the working list, not the watch
        highs = [float(v) for v in s.recent_highs if v]
        if len(highs) < 12:
            continue
        pivot = max(highs)
        if pivot <= 0:
            continue
        below = (1 - s.last_price / pivot) * 100
        if below < 0 or below > 5:
            continue  # 0-5% under the pivot = coiled and close
        rows.append({
            "symbol": s.symbol,
            "name": s.name,
            "sector": s.sector,
            "last_price": round(s.last_price, 2),
            "change_pct": round(s.change_pct, 2),
            "rs_rating": s.rs_rating if s.rs_eligible else None,
            "pivot": round(pivot, 2),
            "pct_below_pivot": round(below, 2),
        })
    rows.sort(key=lambda r: r["pct_below_pivot"])
    return rows[:limit]

