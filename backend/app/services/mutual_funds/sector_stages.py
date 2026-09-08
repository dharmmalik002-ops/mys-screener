"""Where each Nifty sector index sits in its own price cycle.

This is Stan Weinstein's stage analysis, applied to the sixteen Nifty sector
indices the fund benchmarks already use. The four stages describe what a price
series is *doing*, not what anyone should do about it:

* **Stage 4 — declining.** Price below a falling 30-week average. The downtrend
  is still in force.
* **Stage 1 — basing.** The decline has stopped. Price oscillates around a 30-week
  average that has gone flat, the range narrows, and sellers are exhausted. This
  is the accumulation phase that precedes an advance — but a base can last
  months, and some bases fail back into Stage 4 rather than advancing.
* **Stage 2 — advancing.** Price above a rising 30-week average, making higher
  highs. The markup phase.
* **Stage 3 — topping.** The advance has stalled. The average flattens after a
  rise and price chops across it.

**Why 30 weeks.** Weinstein's whole framework is built on the 30-week moving
average of weekly closes, and the stage boundaries are calibrated to it. Using
a 50-day or 200-day daily average instead would give different, faster, noisier
answers that are not the thing the framework describes. So this module
resamples daily closes to weekly and works in weeks throughout.

**What "about to start Stage 2" can and cannot mean.** A base that is tightening
with improving relative strength is measurably closer to a breakout than one
that is not, and that is what `breakout_readiness` scores. It is emphatically
not a prediction: bases fail, and the score says how far the *setup* has
formed, never how likely the breakout is. Every number here is arithmetic over
past prices, described as a measurement — consistent with how the equity
scanners in this app already report technical states, and with the rule that
this app does not tell anyone what to buy (CLAUDE.md gotcha 12).
"""

from __future__ import annotations

from typing import Any

# Weinstein's average, in weeks. Everything below is calibrated to it.
MA_WEEKS = 30
# Enough history to have a 30-week average plus a year of context around it.
MIN_WEEKS = MA_WEEKS + 26

# A slope inside this band, as percent of price per week, reads as "flat".
# Wide enough that ordinary weekly noise does not flip a base into a trend.
FLAT_SLOPE_PCT = 0.12
# How far price may sit from the average and still count as "at" it.
NEAR_MA_PCT = 4.0
# A base is tight when its recent range is inside this share of price.
TIGHT_RANGE_PCT = 14.0

STAGE_LABELS = {
    1: "Basing",
    2: "Advancing",
    3: "Topping",
    4: "Declining",
}

STAGE_BLURBS = {
    1: "The decline has stopped and price is oscillating around a flat 30-week average. "
       "This is the base that precedes an advance — though a base can last months, and "
       "some fail back into decline rather than breaking out.",
    2: "Price is above a rising 30-week average. This is the advancing phase of the cycle.",
    3: "The advance has stalled: the 30-week average has flattened after a rise and price "
       "is chopping across it.",
    4: "Price is below a falling 30-week average. The downtrend is still in force.",
}


def _to_weekly(dates: list[str], closes: list[float],
               highs: list[float] | None = None,
               lows: list[float] | None = None) -> dict[str, list]:
    """Resample daily bars to weekly ones, keyed by ISO week.

    The week is stamped with its last trading date and closes at that day's
    close, which is what a weekly chart shows.
    """
    from datetime import date as _date

    buckets: dict[tuple[int, int], dict[str, Any]] = {}
    order: list[tuple[int, int]] = []
    for index, iso in enumerate(dates):
        try:
            day = _date.fromisoformat(iso)
        except ValueError:
            continue
        key = day.isocalendar()[:2]
        close = closes[index]
        high = highs[index] if highs else close
        low = lows[index] if lows else close
        bucket = buckets.get(key)
        if bucket is None:
            buckets[key] = {
                "date": iso, "open": close, "close": close,
                "high": high, "low": low,
            }
            order.append(key)
        else:
            bucket["date"] = iso
            bucket["close"] = close
            bucket["high"] = max(bucket["high"], high)
            bucket["low"] = min(bucket["low"], low)

    return {
        "dates": [buckets[k]["date"] for k in order],
        "opens": [buckets[k]["open"] for k in order],
        "closes": [buckets[k]["close"] for k in order],
        "highs": [buckets[k]["high"] for k in order],
        "lows": [buckets[k]["low"] for k in order],
    }


def _sma(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if len(values) < window:
        return out
    running = sum(values[:window])
    out[window - 1] = running / window
    for index in range(window, len(values)):
        running += values[index] - values[index - window]
        out[index] = running / window
    return out


def _slope_pct_per_week(series: list[float | None], lookback: int = 8) -> float | None:
    """Average weekly change in the average, as a percent of its own level.

    Percent rather than absolute so a 30,000-point index and a 400-point one
    are on the same scale.
    """
    usable = [value for value in series[-(lookback + 1):] if value is not None]
    if len(usable) < 3 or usable[0] <= 0:
        return None
    steps = len(usable) - 1
    return ((usable[-1] - usable[0]) / usable[0]) * 100.0 / steps


def classify(weekly: dict[str, list]) -> dict[str, Any]:
    """The stage this weekly series is in, with the evidence behind it."""
    closes = weekly["closes"]
    if len(closes) < MIN_WEEKS:
        return {"stage": None, "reason": f"only {len(closes)} weeks of history"}

    ma = _sma(closes, MA_WEEKS)
    price = closes[-1]
    average = ma[-1]
    if average is None or average <= 0:
        return {"stage": None, "reason": "no 30-week average yet"}

    slope = _slope_pct_per_week(ma) or 0.0
    distance = (price / average - 1) * 100.0

    # Was the recent past a decline? Separates a flat average that follows a
    # fall (Stage 1) from one that follows a rise (Stage 3) — the two look
    # identical on slope alone and mean opposite things.
    prior = [value for value in ma[-30:-8] if value is not None]
    prior_slope = None
    if len(prior) >= 3 and prior[0] > 0:
        prior_slope = ((prior[-1] - prior[0]) / prior[0]) * 100.0 / (len(prior) - 1)

    rising = slope > FLAT_SLOPE_PCT
    falling = slope < -FLAT_SLOPE_PCT
    above = distance > 0

    # Where price sits in its own two-year range. This is what separates a
    # flat average that follows a decline (a base) from one that follows an
    # advance (a top): a base forms near the lows of the range it fell
    # through, a top near the highs of the one it rose through. The prior-slope
    # window below is kept as a secondary read, but it cannot be the primary
    # one — once a flat period outlasts the window, every top starts looking
    # like a base.
    long_window = closes[-104:] if len(closes) >= 104 else closes
    low, high = min(long_window), max(long_window)
    long_position = ((price - low) / (high - low) * 100.0) if high > low else 50.0

    if rising and above:
        stage = 2
    elif falling and not above:
        stage = 4
    elif not rising and not falling:
        # Flat average. Which side of the cycle it is on is decided by position
        # in the long range first, then by what the average was doing before.
        if long_position >= 60.0:
            stage = 3
        elif long_position <= 40.0:
            stage = 1
        else:
            stage = 3 if (prior_slope is not None and prior_slope > FLAT_SLOPE_PCT) else 1
    elif rising and not above:
        # Average still rising but price has lost it — an advance rolling over.
        stage = 3
    else:
        # Average falling but price has reclaimed it — the first thing that
        # happens when a decline ends.
        stage = 1

    # Price well clear of an average that is still falling is a recovery in
    # progress, not a settled base. Textbook stage analysis still calls this
    # Stage 1 — the average has not turned — but reading it as "quietly
    # basing" would misdescribe a sector that has already moved a long way,
    # so it is flagged rather than reclassified.
    early_advance = bool(stage == 1 and falling and distance > NEAR_MA_PCT * 2)

    return {
        "stage": stage,
        "stage_label": STAGE_LABELS[stage],
        "early_advance": early_advance,
        "ma30": round(average, 2),
        "price": round(price, 2),
        "distance_from_ma_pct": round(distance, 2),
        "ma_slope_pct_per_week": round(slope, 3),
        "prior_slope_pct_per_week": round(prior_slope, 3) if prior_slope is not None else None,
        "position_in_2y_range_pct": round(long_position, 1),
        "weeks_of_history": len(closes),
    }


def base_metrics(weekly: dict[str, list], benchmark_weekly: dict[str, list] | None) -> dict[str, Any]:
    """How far a base has formed, and whether the sector is starting to lead.

    Four things separate a base that is close to resolving from one that has
    only just stopped falling:

    * **Tightness** — the recent range as a share of price. Bases narrow as
      supply dries up.
    * **Position in the base** — price sitting near the top of the range rather
      than the bottom.
    * **Relative strength** — the sector's own move against the broad market
      over the last quarter. A base that is starting to outperform while still
      sideways is the classic Weinstein tell.
    * **Depth of the prior fall** — how far below the old high it still sits,
      which is context for how much repair is left.
    """
    closes = weekly["closes"]
    highs, lows = weekly["highs"], weekly["lows"]
    price = closes[-1]

    window = min(20, len(closes))
    base_high = max(highs[-window:])
    base_low = min(lows[-window:])
    span = base_high - base_low
    range_pct = (span / price * 100.0) if price else None
    # Where in its own range price is sitting, 0 = the low, 100 = the high.
    position = ((price - base_low) / span * 100.0) if span > 0 else 50.0

    high_52w = max(highs[-52:]) if len(highs) >= 2 else price
    off_high = ((price / high_52w - 1) * 100.0) if high_52w else 0.0

    rs_13w = None
    if benchmark_weekly and len(benchmark_weekly["closes"]) >= 14 and len(closes) >= 14:
        own = closes[-1] / closes[-14] - 1 if closes[-14] else None
        market = (benchmark_weekly["closes"][-1] / benchmark_weekly["closes"][-14] - 1
                  if benchmark_weekly["closes"][-14] else None)
        if own is not None and market is not None:
            rs_13w = (own - market) * 100.0

    return {
        "base_high": round(base_high, 2),
        "base_low": round(base_low, 2),
        "range_pct": round(range_pct, 2) if range_pct is not None else None,
        "position_in_range_pct": round(position, 1),
        "off_52w_high_pct": round(off_high, 2),
        "rs_13w_vs_market_pct": round(rs_13w, 2) if rs_13w is not None else None,
        "tight": bool(range_pct is not None and range_pct <= TIGHT_RANGE_PCT),
    }


def breakout_readiness(stage: dict[str, Any], base: dict[str, Any]) -> int:
    """0-100: how fully the Stage-1 setup has formed. Not a probability.

    Deliberately a sum of four visible components rather than a fitted model,
    so every point on the score can be traced to something on the chart. A
    fitted probability would also be a claim about the future, which this is
    not making.
    """
    if stage.get("stage") != 1:
        return 0
    score = 0

    # 1. The average has stopped falling (up to 30).
    slope = stage.get("ma_slope_pct_per_week") or 0.0
    if slope >= 0:
        score += 30
    elif slope > -FLAT_SLOPE_PCT:
        score += int(30 * (1 - abs(slope) / FLAT_SLOPE_PCT))

    # 2. Price has reclaimed the average (up to 25).
    distance = stage.get("distance_from_ma_pct") or 0.0
    if distance >= 0:
        score += min(25, 15 + int(distance * 2))
    elif distance > -NEAR_MA_PCT:
        score += int(15 * (1 - abs(distance) / NEAR_MA_PCT))

    # 3. The base is tight and price sits in its upper half (up to 25).
    range_pct = base.get("range_pct")
    if range_pct is not None and range_pct <= TIGHT_RANGE_PCT:
        score += int(12 * (1 - range_pct / TIGHT_RANGE_PCT)) + 5
    position = base.get("position_in_range_pct") or 0.0
    if position >= 50:
        score += int(8 * min(1.0, (position - 50) / 50))

    # 4. It is beginning to outperform while still sideways (up to 20).
    rs = base.get("rs_13w_vs_market_pct")
    if rs is not None and rs > 0:
        score += min(20, int(rs * 2))

    return max(0, min(100, score))


def bucket_of(stage_no: int | None, readiness: int) -> str:
    """Which of the page's sections a sector belongs in.

    `turning` is the narrow one the reader asked for: a base far enough along
    that the setup is visible. A base that has only just stopped falling is
    still `sideways`, because calling it "about to turn" would be inventing a
    signal that is not on the chart yet.
    """
    if stage_no == 2:
        return "advancing"
    if stage_no == 1:
        return "turning" if readiness >= 55 else "sideways"
    if stage_no == 3:
        return "sideways"
    return "declining"


def describe(row: dict[str, Any]) -> str:
    """One plain sentence stating what was measured. No instruction."""
    stage_no = row.get("stage")
    label = row.get("name")
    if stage_no == 2:
        return (
            f"{label} is above its 30-week average and that average is rising "
            f"({row['ma_slope_pct_per_week']:+.2f}% a week) — the advancing phase. "
            f"It sits {abs(row['off_52w_high_pct']):.0f}% "
            f"{'below' if row['off_52w_high_pct'] < 0 else 'at or above'} its 52-week high."
        )
    if stage_no == 1:
        # The slope decides the verb. Calling a -0.31%/week average "flat"
        # because the stage happens to be 1 would contradict the number
        # printed next to it.
        slope = row["ma_slope_pct_per_week"]
        if slope > FLAT_SLOPE_PCT:
            moved = "has turned up"
        elif slope < -FLAT_SLOPE_PCT:
            moved = "is still edging down"
        else:
            moved = "has gone flat"
        parts = [
            f"{label}'s 30-week average {moved} "
            f"({slope:+.2f}% a week) after a decline, and price is "
            f"{abs(row['distance_from_ma_pct']):.1f}% "
            f"{'above' if row['distance_from_ma_pct'] >= 0 else 'below'} it."
        ]
        if row.get("range_pct") is not None:
            parts.append(
                f"The last 20 weeks have held a {row['range_pct']:.0f}% range, with price "
                f"{row['position_in_range_pct']:.0f}% of the way up it."
            )
        if row.get("rs_13w_vs_market_pct") is not None:
            direction = "ahead of" if row["rs_13w_vs_market_pct"] > 0 else "behind"
            parts.append(
                f"Over 13 weeks it is {abs(row['rs_13w_vs_market_pct']):.1f}% {direction} the "
                "broad market."
            )
        if row.get("early_advance"):
            parts.append(
                "Price has already run well clear of the average while the average itself is "
                "still falling — a recovery under way rather than a quiet base."
            )
        return " ".join(parts)
    if stage_no == 3:
        return (
            f"{label}'s 30-week average has flattened after a rise and price is chopping "
            f"across it ({row['distance_from_ma_pct']:+.1f}%). The advance has stalled; whether "
            "it resumes or rolls over is not settled by the chart."
        )
    return (
        f"{label} is {abs(row['distance_from_ma_pct']):.1f}% below a falling 30-week average "
        f"({row['ma_slope_pct_per_week']:+.2f}% a week) — still in decline."
    )


def build(sectors: list[dict[str, Any]], *, series_for, market_series=None) -> dict[str, Any]:
    """Classify every sector index. `series_for` returns a daily OHLC payload.

    Never raises on one bad index: a sector whose feed is down is reported as
    unavailable and the rest of the page still renders.
    """
    benchmark_weekly = None
    if market_series:
        try:
            benchmark_weekly = _to_weekly(
                market_series["dates"], market_series["navs"],
                market_series.get("highs"), market_series.get("lows"),
            )
        except (KeyError, TypeError):
            benchmark_weekly = None

    rows: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []

    for sector in sectors:
        try:
            daily = series_for(sector["symbol"])
        except Exception as exc:
            unavailable.append({"key": sector["key"], "name": sector["name"],
                                "reason": type(exc).__name__})
            continue
        if not daily or not daily.get("dates"):
            unavailable.append({"key": sector["key"], "name": sector["name"],
                                "reason": "no history"})
            continue

        weekly = _to_weekly(daily["dates"], daily["navs"],
                            daily.get("highs"), daily.get("lows"))
        stage = classify(weekly)
        if stage.get("stage") is None:
            unavailable.append({"key": sector["key"], "name": sector["name"],
                                "reason": stage.get("reason") or "not classifiable"})
            continue

        base = base_metrics(weekly, benchmark_weekly)
        readiness = breakout_readiness(stage, base)
        closes = weekly["closes"]
        row = {
            "key": sector["key"],
            "name": sector["name"],
            "symbol": sector["symbol"],
            **stage,
            **base,
            "readiness": readiness,
            "bucket": bucket_of(stage["stage"], readiness),
            "stage_blurb": STAGE_BLURBS[stage["stage"]],
            "return_13w_pct": round((closes[-1] / closes[-14] - 1) * 100.0, 2) if len(closes) >= 14 else None,
            "return_52w_pct": round((closes[-1] / closes[-53] - 1) * 100.0, 2) if len(closes) >= 53 else None,
            "as_of": weekly["dates"][-1],
        }
        row["summary"] = describe(row)
        rows.append(row)

    # Yahoo serves several of these sector indices intermittently and can stop
    # updating one for weeks while the majors stay current. A stage read on a
    # five-week-old bar is not wrong, but it is not current either, and the
    # page must not imply otherwise. Freshness is measured against the newest
    # bar in the set — the real last trading day — rather than the wall clock,
    # so weekends and holidays do not flag everything at once.
    from datetime import date as _d
    newest = max((row["as_of"] for row in rows), default=None)
    if newest:
        latest = _d.fromisoformat(newest)
        for row in rows:
            behind = (latest - _d.fromisoformat(row["as_of"])).days
            row["days_behind"] = behind
            row["is_stale"] = behind > 7

    buckets: dict[str, list[dict[str, Any]]] = {
        "turning": [], "advancing": [], "sideways": [], "declining": [],
    }
    for row in rows:
        buckets[row["bucket"]].append(row)

    buckets["turning"].sort(key=lambda r: -r["readiness"])
    buckets["advancing"].sort(key=lambda r: -(r.get("return_13w_pct") or 0))
    buckets["sideways"].sort(key=lambda r: -r["readiness"])
    buckets["declining"].sort(key=lambda r: (r.get("return_13w_pct") or 0))

    return {
        "buckets": buckets,
        "counts": {name: len(items) for name, items in buckets.items()},
        "sector_count": len(rows),
        "unavailable": unavailable,
        "as_of": max((row["as_of"] for row in rows), default=None),
        "stale_count": sum(1 for row in rows if row.get("is_stale")),
        "method": {
            "average_weeks": MA_WEEKS,
            "flat_slope_pct": FLAT_SLOPE_PCT,
            "tight_range_pct": TIGHT_RANGE_PCT,
            "readiness_threshold": 55,
        },
    }
