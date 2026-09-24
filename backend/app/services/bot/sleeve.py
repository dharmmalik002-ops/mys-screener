"""The idle-capital sleeve, built once and shared by the backtest and the book.

The account is two things: a selective stock book, and a sleeve holding
whatever capital the stock side is not using. The sleeve is not a detail — on
the yearly-rebuild test it returns +31.63% a year on its own against the full
book's +38.85%, so a paper book that leaves idle cash in cash is paper-trading
a different strategy and would understate the study by most of its return.

That is exactly what happened: the first paper replay returned +5.87% over
2.7 years because it had no sleeve. This module exists so the two paths cannot
drift again — `run_robust_backtest.py` and `run_paper_session.py` both build
the series here rather than each rolling their own.

What it holds, in order of precedence:

  * **gold** when the market is risk-off — a healthy regime has failed AND the
    index is below its 200 DMA AND there has been no follow-through thrust;
  * **small caps** while recovering from a crash (the index has been 20% below
    its 52-week high within the last year and has climbed back above that);
  * **the broad index** otherwise.

It is built as a **compounded level from chained daily returns**, never by
switching between two price maps: the sleeve holds units, and swapping a
~25,000-level index series for a ~70-level gold series would reprice those
units overnight by a factor of 350 (gotcha 89).
"""

from __future__ import annotations

from datetime import date
from typing import Mapping

import numpy as np

from . import indicators as ind
from . import rules as R

HEALTHY_REGIMES = frozenset({"bull_strong", "bull_narrow", "recovery"})


SLEEVE_MODE = __import__("os").environ.get("BOT_SLEEVE_MODE", "regime_map")


def risk_on_days(
    index_close: Mapping[date, float],
    regime_by_day: Mapping[date, str],
    mode: str | None = None,
    small_close: Mapping[date, float] | None = None,
) -> tuple[set, set]:
    """(sleeve risk-on, book risk-on) — deliberately different conditions.

    The sleeve holds equities while the regime is healthy OR the index is
    above its 200-DMA; the book stays invested only while the regime is
    healthy. Each is read at a close and acts on the NEXT session.

    The thrust leg (gotchas 96/97) is gone: it was chosen while the sleeve
    read same-day state, so it was credited with the very rebound that
    triggered it. With the one-session lag it added nothing and deepened the
    drawdown (gotcha 115).
    """
    days = sorted(index_close)
    closes = np.asarray([index_close[d] for d in days], dtype=float)
    s200 = ind.sma(closes, 200)
    above = {d: (bool(closes[i] > s200[i]) if not np.isnan(s200[i]) else True)
             for i, d in enumerate(days)}
    if (mode or SLEEVE_MODE) == "bear_only":
        # Defend only in a genuine bear: gold and a flat stock book while the
        # regime reads `bear`, fully invested through choppy and correction
        # tape. Measured as the higher-return profile (gotcha 116) at a lower
        # win rate; opt in with BOT_SLEEVE_MODE=bear_only.
        on = {d for d in days if regime_by_day.get(d) != "bear"}
        return on, set(on)
    healthy = {d for d in days if regime_by_day.get(d) in HEALTHY_REGIMES}
    sleeve_on = {d for d in days if d in healthy or above.get(d, True)}
    book_on = set(healthy)
    if small_close:
        # The book is ~85% small caps, so it stands aside in any market type
        # where small caps LOST money on the evidence before this January —
        # in practice `bull_narrow`, which reads healthy on the index while
        # small caps fall ~35% a year (gotcha 121).
        #
        # The same test is then applied one level finer, to the market type
        # AND the index's short trend (20-DMA above or below its 50-DMA):
        # "bull_strong, but the 20-day has rolled under the 50-day" is its own
        # kind of market, and where small caps have lost in it the book stands
        # aside too (gotcha 122).
        stack = trend_stack_labels(index_close, regime_by_day)
        losing: dict = {}
        losing_fine: dict = {}
        for d in list(book_on):
            if d.year not in losing:
                losing[d.year] = small_cap_losing_regimes(index_close, small_close, regime_by_day, d.year)
                losing_fine[d.year] = small_cap_losing_regimes(index_close, small_close, stack, d.year)
            if regime_by_day.get(d) in losing[d.year] or stack.get(d) in losing_fine[d.year]:
                book_on.discard(d)
    return sleeve_on, book_on


def trend_stack_labels(index_close, regime_by_day) -> dict:
    """{day: "<regime>|up" or "<regime>|dn"} — the index's 20-DMA against its 50-DMA."""
    days = sorted(index_close)
    closes = np.asarray([index_close[d] for d in days], dtype=float)
    s20, s50 = ind.sma(closes, 20), ind.sma(closes, 50)
    return {d: f"{regime_by_day[d]}|{'up' if s20[i] > s50[i] else 'dn'}"
            for i, d in enumerate(days) if regime_by_day.get(d) and not np.isnan(s50[i])}


def price_level_labels(index_close, regime_by_day) -> dict:
    """{day: "<regime>|near|mid|deep"} — how far the index sits below its 252-day high.

    near: within 5% of the high; mid: 5-15% below it; deep: more than 15%.
    """
    days = sorted(index_close)
    closes = np.asarray([index_close[d] for d in days], dtype=float)
    out = {}
    for i, d in enumerate(days):
        if not regime_by_day.get(d):
            continue
        dd = closes[i] / closes[max(0, i - 251):i + 1].max() - 1.0
        lvl = "near" if dd > -0.05 else ("mid" if dd > -0.15 else "deep")
        out[d] = f"{regime_by_day[d]}|{lvl}"
    return out


def small_cap_losing_regimes(index_close, small_close, regime_by_day, year: int) -> set:
    """Labels whose mean daily small-cap return, on days before `year`, was negative.

    `regime_by_day` may carry any labels — plain regimes, or the finer
    regime x trend-stack labels from `trend_stack_labels`.

    A day's return belongs to the label of the last regime day strictly before
    it, as in `regime_asset_map`; a regime needs MIN_REGIME_DAYS of evidence
    before it can be judged at all.
    """
    import bisect
    import math
    days = [d for d in sorted(set(index_close) & set(small_close)) if d.year < year]
    labels = sorted(regime_by_day)
    acc: dict = {}
    for a, b in zip(days, days[1:]):
        pa, pb = small_close.get(a), small_close.get(b)
        if not (pa and pb):
            continue
        j = bisect.bisect_left(labels, b)
        g = regime_by_day.get(labels[j - 1]) if j else None
        x = acc.setdefault(g, [0.0, 0])
        x[0] += math.log(pb / pa)
        x[1] += 1
    return {g for g, (s, n) in acc.items() if g and n >= MIN_REGIME_DAYS and s < 0}


def clean_series(prices: Mapping[date, float], max_jump: float = 0.5) -> dict:
    """Drop prints that move more than `max_jump` from the last kept price.

    Yahoo's GOLDBEES history reads 0.3355 on 2019-12-19 and 33.65 on
    2019-12-23 — a -99% / +9900% pair around a real ~33 price. With the
    sleeve's state lagged a session, a switch landing between the two would
    book one leg of it. A dropped day carries the previous price instead.
    """
    out: dict = {}
    last = None
    for d in sorted(prices):
        px = prices[d]
        # NaN passes both `not px` and `px <= 0`; Yahoo prints one for a
        # session it has not settled yet, and it poisons every later level.
        if not px or not np.isfinite(px) or px <= 0:
            continue
        if last is not None and abs(px / last - 1.0) > max_jump:
            continue
        out[d] = px
        last = px
    return out


def recovery_days(index_close: Mapping[date, float]) -> set:
    days = sorted(index_close)
    return R.recovery_days(days, [float(index_close[d]) for d in days])


def build_level(
    index_close: Mapping[date, float],
    gold_close: Mapping[date, float],
    smallcap_close: Mapping[date, float] | None,
    sleeve_on: set,
    holdings: dict | None = None,
) -> dict:
    """A single compounded level series the book can hold units of."""
    small = smallcap_close or {}
    recovering = recovery_days(index_close) if small else set()
    days = sorted(set(index_close) | set(gold_close) | set(small))
    level, out = 100.0, {}
    prev_i = prev_g = prev_s = None
    # State is read on index sessions only and carried across days the index
    # does not print (a gold-only print must not flip the sleeve into gold).
    # Day d's return belongs to the state held going INTO d — what the close
    # before it chose (gotcha 115).
    held_on, held_rec = True, False
    cur_on, cur_rec = True, False
    for d in days:
        i = index_close.get(d, prev_i)
        g = gold_close.get(d, prev_g)
        s = small.get(d, prev_s)
        held_on, held_rec = cur_on, cur_rec
        if held_on:
            # Equity leg: small caps while a crash recovery is live, broad
            # index otherwise.
            if held_rec and prev_s and s:
                level *= s / prev_s
            elif prev_i and i:
                level *= i / prev_i
        elif prev_g and g:
            level *= g / prev_g
        if d in index_close:
            cur_on, cur_rec = d in sleeve_on, d in recovering
            if holdings is not None:
                holdings[d] = {("small" if cur_rec and small else "n500") if cur_on else "gold": 1.0}
        # A missing price is not a zero price — carry the last one forward.
        prev_i, prev_g, prev_s = i or prev_i, g or prev_g, s or prev_s
        out[d] = level
    return out


def book_regime(days, index_days, book_on: set) -> dict:
    """Per-day label the account's de-risk reads, carried across non-index days."""
    out, cur = {}, True
    for d in sorted(days):
        if d in index_days:
            cur = d in book_on
        out[d] = "bull_strong" if cur else "bear"
    return out


# --- the market-type sleeve (gotcha 117) --------------------------------------
# Each regime holds whichever of Nifty 500 / Smallcap 250 / gold earned the
# most on days carrying that regime label, measured ONLY on data before the
# current January. Consistent in both halves of history: small caps in
# bull_strong / choppy / recovery, gold in bull_narrow and bear. Re-derived
# yearly rather than fixed, and it ranks at the 96.6th percentile of all 729
# fixed hindsight maps (sleeve alone 2012-2026: +25.5%/yr against +17.2% for
# the previous rule).
REGIME_LABELS = ("bull_strong", "bull_narrow", "choppy", "correction", "bear", "recovery")
MIN_REGIME_DAYS = 60


def regime_asset_map(index_close, small_close, gold_close, regime_by_day, year: int) -> dict:
    """{regime: 'n500' | 'small' | 'gold' | 'default'} from days strictly before `year`."""
    import bisect
    import math
    assets = {"n500": index_close, "small": small_close or {}, "gold": gold_close or {}}
    days = sorted(set(index_close) & set(assets["small"]) & set(assets["gold"]))
    days = [d for d in days if d.year < year]
    labels = sorted(regime_by_day)
    sums = {(g, k): [0.0, 0] for g in REGIME_LABELS for k in assets}
    for a, b in zip(days, days[1:]):
        j = bisect.bisect_left(labels, b)
        g = regime_by_day.get(labels[j - 1]) if j else None
        if g not in REGIME_LABELS:
            continue
        for k, ser in assets.items():
            pa, pb = ser.get(a), ser.get(b)
            if pa and pb:
                acc = sums[(g, k)]
                acc[0] += math.log(pb / pa)
                acc[1] += 1
    out = {}
    for g in REGIME_LABELS:
        best, best_v = "default", None
        for k in assets:
            s, n = sums[(g, k)]
            if n >= MIN_REGIME_DAYS and (best_v is None or s / n > best_v):
                best, best_v = k, s / n
        out[g] = best
    return out


MIN_LEVEL_DAYS = 125


def label_asset_map(index_close, small_close, gold_close, labels, year: int, min_days: int):
    """({label: best asset}, {label: mean daily small-cap log return}) from days before `year`."""
    import bisect
    import math
    assets = {"n500": index_close, "small": small_close or {}, "gold": gold_close or {}}
    days = [d for d in sorted(set(index_close) & set(assets["small"]) & set(assets["gold"])) if d.year < year]
    keys = sorted(labels)
    acc: dict = {}
    for a, b in zip(days, days[1:]):
        j = bisect.bisect_left(keys, b)
        g = labels.get(keys[j - 1]) if j else None
        if g is None:
            continue
        x = acc.setdefault(g, {k: [0.0, 0] for k in assets})
        for k, ser in assets.items():
            x[k][0] += math.log(ser[b] / ser[a])
            x[k][1] += 1
    best = {g: max(x, key=lambda k: x[k][0] / x[k][1]) for g, x in acc.items() if x["n500"][1] >= min_days}
    small = {g: x["small"][0] / x["small"][1] for g, x in acc.items() if x["small"][1] >= min_days}
    return best, small


def better_equity(index_close, small_close, regime_by_day, regime: str, year: int) -> str:
    """'small' or 'n500' — whichever earned more on `regime` days before `year`."""
    import bisect
    import math
    days = [d for d in sorted(set(index_close) & set(small_close or {})) if d.year < year]
    keys = sorted(regime_by_day)
    s_n = s_s = 0.0
    for a, b in zip(days, days[1:]):
        j = bisect.bisect_left(keys, b)
        if not j or regime_by_day.get(keys[j - 1]) != regime:
            continue
        s_n += math.log(index_close[b] / index_close[a])
        s_s += math.log(small_close[b] / small_close[a])
    return "small" if s_s >= s_n else "n500"


def build_regime_level(index_close, gold_close, small_close, regime_by_day, sleeve_on: set,
                       holdings: dict | None = None,
                       cash_close: Mapping[date, float] | None = None) -> dict:
    """Compounded level of the market-type sleeve.

    The asset held over day d is chosen at the previous INDEX close, in this
    order (gotcha 122):

      1. a regime in BLEND_REGIMES holds its fixed mix, rebalanced daily;
      2. otherwise the market type AND the price level (near / mid / deep
         below the 252-day high) hold whatever paid best on such days before
         this January, once there are MIN_LEVEL_DAYS of them;
      3. otherwise the market type alone decides (`regime_asset_map`), and
         `default` falls back to the older rule (equities while healthy or
         above the 200-DMA, gold otherwise).

    Gold is a hedge, so it is held only where small caps have LOST money on
    the prior evidence; where they made money the sleeve holds the better of
    the two equity legs instead. That is what the 2012 map got wrong: three
    years of history had gold ahead in `bull_strong` while small caps were
    also earning +14% a year there.
    """
    assets = {"n500": index_close, "small": small_close or {}, "gold": gold_close or {},
              "cash": cash_close or {}}
    levels = price_level_labels(index_close, regime_by_day)
    maps: dict = {}
    level_maps: dict = {}
    losing: dict = {}
    equity: dict = {}

    def eq(regime, year):
        if (regime, year) not in equity:
            equity[(regime, year)] = better_equity(index_close, small_close, regime_by_day, regime, year)
        return equity[(regime, year)]

    level, out = 100.0, {}
    last = {k: None for k in assets}
    held = {"n500": 1.0}
    for d in sorted(set(index_close) | set(assets["small"]) | set(assets["gold"])):
        growth = 0.0
        for k, w in held.items():
            p0, p1 = last[k], assets[k].get(d)
            if p0 and p1:
                growth += w * (p1 / p0 - 1.0)
        level *= 1.0 + growth
        for k, ser in assets.items():
            if ser.get(d):
                last[k] = ser[d]
        if d in index_close:
            regime = regime_by_day.get(d)
            mix = BLEND_REGIMES.get(regime)
            if mix and not all(assets[k] for k in mix):
                mix = BLEND_FALLBACK.get(regime)
            if mix and all(assets[k] for k in mix):
                held = dict(mix)
            else:
                y = d.year
                if y not in maps:
                    maps[y] = regime_asset_map(index_close, small_close, gold_close, regime_by_day, y)
                    level_maps[y] = label_asset_map(index_close, small_close, gold_close, levels, y, MIN_LEVEL_DAYS)
                best, small_mean = level_maps[y]
                cell = levels.get(d)
                choice = best.get(cell)
                if choice is not None:
                    if choice == "gold" and small_mean.get(cell, -1.0) > 0 and small_close:
                        choice = eq(regime, y)
                else:
                    choice = maps[y].get(regime, "default")
                    if choice == "default":
                        choice = "n500" if d in sleeve_on else "gold"
                    if choice == "gold" and regime is not None and small_close:
                        if y not in losing:
                            losing[y] = small_cap_losing_regimes(index_close, small_close, regime_by_day, y)
                        if regime not in losing[y]:
                            choice = eq(regime, y)
                if not assets[choice]:
                    choice = "n500"
                held = {choice: 1.0}
            if holdings is not None:
                holdings[d] = dict(held)
        out[d] = level
    return out


# Corrections are the one market type with no stable winner: the Nifty 500
# paid best in them over 2008-2018 and gold over 2019-2025, and the yearly map
# picking gold for them is what produced the book's worst drawdown (-33.8% in
# 2013, when gold crashed). Holding both halves instead takes the walk-forward
# drawdown to -27.0% and lifts Sharpe (gotcha 121). Blending by a general rule
# — whenever prior halves disagree, or the winner's lead is not significant —
# was tested and loses in both halves, because it also blends bear markets,
# where gold's win is real.
#
# Gotcha 123: the equity half is now a liquid fund. A correction has no
# dependable winner, so the sleeve holds only defensive assets in one — gold,
# and cash earning a real rate. Walk-forward +32.46% -> +33.67%, the index
# beaten in 15 of 15 years. The older gold/index mix is the fallback when the
# liquid-fund series is unavailable, so a failed fetch degrades, never breaks.
BLEND_REGIMES: dict = {"correction": {"gold": 0.5, "cash": 0.5}}
BLEND_FALLBACK: dict = {"correction": {"gold": 0.5, "n500": 0.5}}

# HDFC Liquid Fund, Regular plan, Growth: daily NAV back to 2006 from AMFI via
# mfapi.in, after the fund's own fees (Direct plans only start in 2013).
LIQUID_FUND_CODE = "100868"


def nav_level(points) -> dict:
    """{date: level} from (date, nav) points, chaining daily returns.

    The fund redenominated its units on 2015-08-30 (NAV x99); a raw lookup
    would book a +9,900% day, so any one-day move over 5% — which a liquid
    fund cannot make — is skipped rather than compounded.
    """
    out, level, prev = {}, 100.0, None
    for d, nav in sorted(points):
        if not nav or nav <= 0:
            continue
        if prev is not None:
            r = nav / prev - 1.0
            if abs(r) <= 0.05:
                level *= 1.0 + r
        out[d] = level
        prev = nav
    return out


def fetch_liquid_fund(code: str = LIQUID_FUND_CODE) -> dict:
    """The liquid fund as a level series, or {} when the source is unreachable.

    Reuses the funds page's AMFI client (`requests`, which carries its own CA
    bundle — the stdlib `urllib` fails certificate checks on a stock macOS
    Python and would silently hand back {} every time).
    """
    import time
    from datetime import date as _date
    for attempt in range(3):     # mfapi.in drops the odd request
        try:
            from app.services.mutual_funds.nav_source import fetch_nav_history
            h = fetch_nav_history(code)
            return nav_level((_date.fromisoformat(d), float(v)) for d, v in zip(h["dates"], h["navs"]))
        except Exception:  # noqa: BLE001 — the sleeve falls back to the gold/index mix
            time.sleep(2 * (attempt + 1))
    return {}


def build_sleeve(index_close, gold_close, small_close, regime_by_day, mode: str | None = None,
                 holdings: dict | None = None, cash_close: Mapping[date, float] | None = None):
    """(sleeve level, book regime map) — the one entry point every runner uses.

    Pass `holdings={}` to have it filled with {index day: {asset: weight}} —
    the mix chosen at that close and held over the NEXT session.
    """
    mode = mode or SLEEVE_MODE
    sleeve_on, book_on = risk_on_days(index_close, regime_by_day, "bear_only" if mode == "bear_only" else None,
                                      small_close=small_close if mode == "regime_map" else None)
    if not gold_close:
        level = dict(index_close)
        if holdings is not None:
            holdings.update({d: {"n500": 1.0} for d in index_close})
    elif mode == "regime_map":
        level = build_regime_level(index_close, gold_close, small_close, regime_by_day, sleeve_on, holdings,
                                   cash_close=cash_close)
    else:
        level = build_level(index_close, gold_close, small_close, sleeve_on, holdings)
    return level, book_regime(level, set(index_close), book_on)
