"""What the world is doing to Indian equities, written as plain English.

Why this module exists
----------------------
The Markets page already answers "how is the Indian tape behaving" from our own
EOD snapshots. It says nothing about *why*. India does not trade in isolation: a
rising dollar and rising crude squeeze margins and pull foreign money out, a
falling US 10-year brings it back, and a bad night on the Nasdaq sets the gap
before a single Indian scanner has run.

This module gathers that outside context — global indices, the six macro prices
that actually move India, FII/DII flows, the event calendar, the day's
headlines — reduces it to counted facts, and turns those facts into *paragraphs*.
The output is deliberately prose-first: the user asked to understand the
environment, not to read another grid of numbers. Every figure the prose may
mention is also returned in `facts` so the UI can show the evidence beside the
sentence that used it.

Division of labour, same strict split as `market_regime`
--------------------------------------------------------
Every number here is computed in Python. The model is handed those numbers and
asked only to write prose over them; it is forbidden, in the prompt and again in
validation, from introducing a figure that was not in the block it was given.
When the model is unavailable the deterministic writer produces the same
paragraphs in plainer English, so the page never degrades to an empty state or
to a bare number grid.

Data sourcing and the datacenter-IP problem
-------------------------------------------
Yahoo serves most of these symbols from residential IPs and turns away parts of
the request from datacenter ranges — the same trap documented in CLAUDE.md
gotcha 14 for sector indices. So every fetch writes its result to
`data/macro_context.json` and every failure falls back to it, marked `stale`
with an explicit age. A blocked fetch degrades to yesterday's context with a
visible warning; it never renders a blank page and never silently reports a
missing series as zero.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

CACHE_FILENAME = "macro_context.json"
# Beyond this the cached context stops being "last night's" and starts being
# misleading, so the UI is told to distrust it rather than quietly showing it.
STALE_WARN_HOURS = 30
STALE_HARD_HOURS = 120
HISTORY_DAYS = 200
CORRELATION_WINDOW = 60


# ─────────────────────────── the instruments ────────────────────────────────


@dataclass(frozen=True)
class Series:
    key: str
    symbol: str
    label: str
    group: str          # "global" | "macro"
    unit: str           # "index" | "pct" | "usd" | "inr"
    blurb: str          # why an Indian trader cares — used by the prose writer
    # Which direction helps Indian equities. The evidence table colours by this
    # rather than by sign: a green "+18.8%" next to prose calling crude a
    # headwind teaches the reader the opposite of the point.
    effect: str = "up_helps"   # "up_helps" | "up_hurts" | "neutral"


# Feature 11 — the overnight board. Ordered the way a trader reads it: where
# the US closed, what Asia is doing right now, where Europe opened.
GLOBAL_SERIES: tuple[Series, ...] = (
    Series("sp500", "^GSPC", "S&P 500", "global", "index",
           "the benchmark global risk appetite is priced off"),
    Series("nasdaq", "^IXIC", "Nasdaq", "global", "index",
           "sets the tone for Indian IT and for high-multiple growth names"),
    Series("nikkei", "^N225", "Nikkei 225", "global", "index",
           "trades during Indian hours, so it moves our open"),
    Series("hangseng", "^HSI", "Hang Seng", "global", "index",
           "the China read — drives metals, and competes with India for EM money"),
    Series("kospi", "^KS11", "Kospi", "global", "index",
           "the other big Asian risk gauge trading alongside us"),
    Series("ftse", "^FTSE", "FTSE 100", "global", "index",
           "Europe's open, which lands mid-session in India"),
    Series("dax", "^GDAXI", "DAX", "global", "index",
           "Europe's cyclical read"),
)

# Feature 12 — the six prices that actually move India.
MACRO_SERIES: tuple[Series, ...] = (
    Series("dxy", "DX-Y.NYB", "Dollar Index", "macro", "index",
           "a strong dollar pulls foreign money out of emerging markets", "up_hurts"),
    Series("usdinr", "USDINR=X", "USD/INR", "macro", "inr",
           "a weak rupee raises import costs and erodes foreign investors' returns", "up_hurts"),
    Series("brent", "BZ=F", "Brent Crude", "macro", "usd",
           "India imports most of its oil, so crude is a direct tax on margins", "up_hurts"),
    Series("ust10y", "^TNX", "US 10-year Yield", "macro", "pct",
           "the global discount rate — rising yields make equities dearer everywhere", "up_hurts"),
    Series("gold", "GC=F", "Gold", "macro", "usd",
           "the fear gauge; gold rising while equities fall is genuine risk-off", "neutral"),
    Series("indiavix", "^INDIAVIX", "India VIX", "macro", "index",
           "what options are charging for Indian risk over the next month", "up_hurts"),
)

NIFTY = Series("nifty", "^NSEI", "Nifty 50", "global", "index", "the home market")

ALL_SERIES: tuple[Series, ...] = (NIFTY,) + GLOBAL_SERIES + MACRO_SERIES
SERIES_BY_KEY: dict[str, Series] = {s.key: s for s in ALL_SERIES}


# ───────────────────────── the event calendar (15) ───────────────────────────
#
# Curated rather than scraped. These dates are published a year ahead by the RBI
# and the Fed and do not move, so a static table is more reliable than any feed
# — and a wrong date here is worse than no date, hence no guessing.

SCHEDULED_EVENTS: tuple[tuple[str, str, str], ...] = (
    # (ISO date, label, kind)
    ("2026-10-07", "RBI monetary policy decision", "rbi"),
    ("2026-12-05", "RBI monetary policy decision", "rbi"),
    ("2027-02-05", "RBI monetary policy decision", "rbi"),
    ("2026-09-17", "US FOMC rate decision", "fed"),
    ("2026-10-28", "US FOMC rate decision", "fed"),
    ("2026-12-09", "US FOMC rate decision", "fed"),
    ("2027-01-27", "US FOMC rate decision", "fed"),
    ("2027-02-01", "Union Budget", "budget"),
)

# Recurring domestic releases. India CPI lands around the 12th, IIP around the
# 28th; both are month-shaped rather than date-fixed, so they are generated
# forward rather than listed.
RECURRING_EVENTS: tuple[tuple[int, str, str], ...] = (
    (12, "India CPI inflation", "data"),
    (28, "India IIP / industrial output", "data"),
)


def _last_thursday(year: int, month: int) -> date:
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    day = nxt - timedelta(days=1)
    while day.weekday() != 3:  # Thursday
        day -= timedelta(days=1)
    return day


def upcoming_events(today: date, *, horizon_days: int = 21) -> list[dict[str, Any]]:
    """Every scheduled market-moving date inside the horizon, nearest first."""
    limit = today + timedelta(days=horizon_days)
    found: list[dict[str, Any]] = []

    for iso, label, kind in SCHEDULED_EVENTS:
        try:
            when = date.fromisoformat(iso)
        except ValueError:
            continue
        if today <= when <= limit:
            found.append({"date": iso, "label": label, "kind": kind})

    # Recurring monthly releases across every month the horizon touches.
    cursor = date(today.year, today.month, 1)
    for _ in range(3):
        for day_of_month, label, kind in RECURRING_EVENTS:
            try:
                when = cursor.replace(day=day_of_month)
            except ValueError:
                continue
            if today <= when <= limit:
                found.append({"date": when.isoformat(), "label": label, "kind": kind})
        expiry = _last_thursday(cursor.year, cursor.month)
        if today <= expiry <= limit:
            found.append({"date": expiry.isoformat(), "label": "Monthly F&O expiry", "kind": "expiry"})
        cursor = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)

    for item in found:
        item["days_away"] = (date.fromisoformat(item["date"]) - today).days
    found.sort(key=lambda e: e["date"])
    return found


# ───────────────────────────── series maths ──────────────────────────────────


def _pct_change(series: Sequence[float], back: int) -> float | None:
    """Percent change over `back` sessions, or None when history is short.

    None rather than 0.0 on purpose: a missing measurement and a flat market are
    different facts, and the prose writer must be able to tell them apart.
    """
    if len(series) <= back or back <= 0:
        return None
    prior = series[-1 - back]
    if not prior:
        return None
    return (series[-1] / prior - 1.0) * 100.0


def _sma(series: Sequence[float], window: int) -> float | None:
    if len(series) < window or window <= 0:
        return None
    tail = series[-window:]
    return sum(tail) / len(tail)


def _correlation(a: Sequence[float], b: Sequence[float]) -> float | None:
    """Pearson correlation of two equal-length return streams."""
    n = min(len(a), len(b))
    if n < 20:
        return None
    xs, ys = list(a[-n:]), list(b[-n:])
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs)
    dy = sum((y - my) ** 2 for y in ys)
    if dx <= 0 or dy <= 0:
        return None
    return num / ((dx ** 0.5) * (dy ** 0.5))


def _returns(closes: Sequence[float]) -> list[float]:
    out: list[float] = []
    for prev, cur in zip(closes, closes[1:]):
        out.append((cur / prev - 1.0) if prev else 0.0)
    return out


def summarise_series(spec: Series, closes: Sequence[float], as_of: str | None) -> dict[str, Any] | None:
    """Reduce one price history to the handful of facts the prose is about."""
    clean = [float(c) for c in closes if c is not None and float(c) > 0]
    if len(clean) < 2:
        return None
    sma50 = _sma(clean, 50)
    last = clean[-1]
    return {
        "key": spec.key,
        "symbol": spec.symbol,
        "label": spec.label,
        "group": spec.group,
        "unit": spec.unit,
        "blurb": spec.blurb,
        "effect": spec.effect,
        "last": round(last, 4),
        "change_1d_pct": _round(_pct_change(clean, 1)),
        "change_5d_pct": _round(_pct_change(clean, 5)),
        "change_20d_pct": _round(_pct_change(clean, 20)),
        "vs_50dma_pct": _round((last / sma50 - 1.0) * 100.0) if sma50 else None,
        "above_50dma": (last > sma50) if sma50 else None,
        "as_of": as_of,
        "sessions": len(clean),
    }


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


# ─────────────────── the verdict: what the world is doing to us ──────────────
#
# Each rule is one counted fact voting on whether the external environment is
# helping or hurting Indian equities. Thresholds are deliberately wide: this is
# a background-conditions read on a multi-week horizon, not a timing signal, and
# a rule that fires on every 0.3% wiggle would make the paragraph meaningless.

@dataclass(frozen=True)
class Rule:
    key: str
    weight: int
    tailwind: str
    headwind: str


def assess_pressure(series: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Score the external environment from −N to +N and say why, in words.

    Returns the reasons as sentences rather than codes, because they are what
    the deterministic writer falls back to when the model is unavailable — and
    a fallback that reads like a debug dump is not a fallback.
    """
    tailwinds: list[str] = []
    headwinds: list[str] = []
    neutral: list[str] = []
    score = 0

    def get(key: str, field: str) -> float | None:
        row = series.get(key)
        if not row:
            return None
        value = row.get(field)
        return None if value is None else float(value)

    # Crude — the most direct macro tax on Indian earnings.
    brent_20 = get("brent", "change_20d_pct")
    if brent_20 is not None:
        if brent_20 >= 6:
            score -= 2
            headwinds.append(
                f"Crude is up {brent_20:.1f}% over the last month, which lifts input and freight costs "
                "across the market and squeezes the import bill"
            )
        elif brent_20 <= -6:
            score += 2
            tailwinds.append(
                f"Crude is down {abs(brent_20):.1f}% over the last month — the single cleanest tailwind "
                "India gets, because it eases both margins and the import bill"
            )
        else:
            neutral.append(f"Crude has gone nowhere in particular ({brent_20:+.1f}% over a month)")

    # Dollar — the EM allocation switch.
    dxy_20 = get("dxy", "change_20d_pct")
    if dxy_20 is not None:
        if dxy_20 >= 1.5:
            score -= 1
            headwinds.append(
                f"The dollar index is up {dxy_20:.1f}% in a month; a firm dollar is usually enough on its "
                "own to keep foreign buyers out of emerging markets"
            )
        elif dxy_20 <= -1.5:
            score += 1
            tailwinds.append(
                f"The dollar index is down {abs(dxy_20):.1f}% in a month, which historically pulls foreign "
                "money back towards emerging markets including India"
            )

    # Rupee — foreign investors earn in dollars, so this is their real return.
    inr_20 = get("usdinr", "change_20d_pct")
    if inr_20 is not None:
        if inr_20 >= 1.0:
            score -= 1
            headwinds.append(
                f"The rupee has weakened {inr_20:.1f}% against the dollar over the month, so a foreign "
                "investor is losing money on the currency even when the index is flat"
            )
        elif inr_20 <= -1.0:
            score += 1
            tailwinds.append(
                f"The rupee has strengthened {abs(inr_20):.1f}% over the month, which quietly adds to "
                "every foreign investor's return here"
            )

    # US 10-year — the global discount rate. ^TNX is quoted in percent.
    y_now = get("ust10y", "last")
    y_chg = get("ust10y", "change_20d_pct")
    if y_now is not None and y_chg is not None:
        move_bps = y_now * (y_chg / 100.0) * 100.0
        if move_bps >= 20:
            score -= 1
            headwinds.append(
                f"The US 10-year yield has risen roughly {move_bps:.0f} basis points in a month to "
                f"{y_now:.2f}%, and rising global yields make every equity look more expensive"
            )
        elif move_bps <= -20:
            score += 1
            tailwinds.append(
                f"The US 10-year yield has fallen roughly {abs(move_bps):.0f} basis points in a month to "
                f"{y_now:.2f}%, which is the kind of backdrop that supports higher valuations"
            )

    # The world's own trend — is global risk appetite even intact?
    sp_50 = series.get("sp500", {}).get("above_50dma")
    sp_20 = get("sp500", "change_20d_pct")
    if sp_50 is not None and sp_20 is not None:
        if sp_50 and sp_20 > 0:
            score += 2
            tailwinds.append(
                f"The S&P 500 is above its 50-day average and up {sp_20:.1f}% over the month, so global "
                "risk appetite is intact and not fighting you"
            )
        elif not sp_50:
            score -= 2
            headwinds.append(
                f"The S&P 500 has lost its 50-day average ({sp_20:+.1f}% over the month) — when the world's "
                "benchmark is broken, Indian breakouts tend to fail more often than they work"
            )

    # Volatility — what options are charging for the next month of Indian risk.
    vix = get("indiavix", "last")
    if vix is not None:
        if vix >= 18:
            score -= 1
            headwinds.append(
                f"India VIX at {vix:.1f} says the options market is pricing real trouble; stops get hit on "
                "noise at this level, so positions have to be smaller"
            )
        elif vix <= 13:
            score += 1
            tailwinds.append(
                f"India VIX at {vix:.1f} is calm, which is the environment breakouts need — moves follow "
                "through instead of being whipsawed out"
            )

    # Genuine risk-off: gold bid while equities are not.
    gold_5 = get("gold", "change_5d_pct")
    nifty_5 = get("nifty", "change_5d_pct")
    if gold_5 is not None and nifty_5 is not None and gold_5 >= 2 and nifty_5 <= -1:
        score -= 1
        headwinds.append(
            f"Gold is up {gold_5:.1f}% over the week while the Nifty is down {abs(nifty_5):.1f}% — money is "
            "actively moving to safety rather than just rotating between sectors"
        )

    if score >= 3:
        stance, verdict = "supportive", "The outside world is helping."
    elif score <= -3:
        stance, verdict = "hostile", "The outside world is working against you."
    else:
        stance, verdict = "mixed", "The outside world is neither helping nor hurting much."

    return {
        "score": score,
        "stance": stance,
        "verdict": verdict,
        "tailwinds": tailwinds,
        "headwinds": headwinds,
        "neutral": neutral,
    }


def assess_linkage(returns_by_key: dict[str, list[float]]) -> dict[str, Any]:
    """Feature 16 — how much the outside world matters to India *right now*.

    A correlation is not a forecast. It answers one narrow question: if the US
    closes badly tonight, is that historically informative about tomorrow here,
    or is India currently trading on its own story? Knowing which regime you're
    in is what stops a trader panicking at the wrong headline.
    """
    nifty = returns_by_key.get("nifty") or []
    pairs = {
        "sp500": "the US market",
        "brent": "crude",
        "dxy": "the dollar",
        "usdinr": "the rupee",
    }
    out: dict[str, Any] = {}
    for key in pairs:
        other = returns_by_key.get(key) or []
        out[key] = _round(_correlation(nifty[-CORRELATION_WINDOW:], other[-CORRELATION_WINDOW:]), 2)

    sp = out.get("sp500")
    if sp is None:
        coupling = None
        note = "There isn't enough overlapping history to say how tightly India is tracking the US right now."
    elif sp >= 0.5:
        coupling = "tight"
        note = (
            f"Over the last {CORRELATION_WINDOW} sessions the Nifty has moved with the S&P 500 about "
            f"{sp:.2f} of the time. That is a tight leash: a bad night in the US is genuinely informative "
            "about your open, and it is worth checking global cues before placing a single order."
        )
    elif sp >= 0.25:
        coupling = "moderate"
        note = (
            f"India's correlation to the US is running at {sp:.2f} over {CORRELATION_WINDOW} sessions — "
            "loose enough that a single bad overseas session is noise, tight enough that a sustained "
            "global downtrend would eventually drag us with it."
        )
    else:
        coupling = "loose"
        note = (
            f"India is barely tracking the US at the moment ({sp:.2f} over {CORRELATION_WINDOW} sessions). "
            "The market is trading on its own story — domestic flows and earnings — so reacting to "
            "overnight headlines is more likely to cost you than to protect you."
        )
    out["coupling"] = coupling
    out["note"] = note
    out["window_sessions"] = CORRELATION_WINDOW
    return out


# Feature 17 — what the macro actually means sector by sector. Each entry is the
# transmission mechanism, not a prediction: it tells the reader *why* a sector
# should respond, so the group rankings become teachable instead of a leaderboard.
SECTOR_DRIVERS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("IT", ("nasdaq", "usdinr"),
     "earns in dollars and sells to US clients, so it likes a weak rupee and a strong Nasdaq"),
    ("Banks & Financials", ("ust10y", "dxy"),
     "lives on rates and on foreign flows, so falling yields and a soft dollar help most"),
    ("Oil & Gas / Paints / Aviation", ("brent",),
     "pays for crude directly, so cheaper oil goes straight to the bottom line"),
    ("Metals", ("hangseng", "dxy"),
     "is a bet on Chinese demand and a weak dollar"),
    ("Autos", ("brent", "usdinr"),
     "is squeezed by expensive crude and expensive imported components"),
    ("Pharma", ("usdinr",),
     "exports in dollars, so a weak rupee flatters its earnings"),
    ("FMCG & Consumer", ("brent",),
     "benefits when cheaper crude eases packaging and freight costs"),
)


def sector_implications(series: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Which sectors the current macro is pushing for and against, with reasons."""
    out: list[dict[str, Any]] = []
    for name, drivers, mechanism in SECTOR_DRIVERS:
        moves: list[str] = []
        votes = 0
        for key in drivers:
            row = series.get(key)
            if not row:
                continue
            chg = row.get("change_20d_pct")
            if chg is None:
                continue
            moves.append(f"{row['label']} {chg:+.1f}% over a month")
            # Direction of help differs by driver, so each is scored explicitly
            # rather than assuming "up is good".
            if key in ("brent", "dxy", "ust10y"):
                votes += -1 if chg > 1.5 else (1 if chg < -1.5 else 0)
            elif key == "usdinr":
                # A weaker rupee helps exporters and hurts importers.
                helps_exporter = name in ("IT", "Pharma")
                votes += (1 if helps_exporter else -1) if chg > 1.0 else 0
                votes += (-1 if helps_exporter else 1) if chg < -1.0 else 0
            else:
                votes += 1 if chg > 1.5 else (-1 if chg < -1.5 else 0)
        if not moves:
            continue
        lean = "favoured" if votes > 0 else ("pressured" if votes < 0 else "neutral")
        out.append({
            "sector": name,
            "lean": lean,
            "mechanism": mechanism,
            "drivers": moves,
        })
    out.sort(key=lambda r: {"favoured": 0, "neutral": 1, "pressured": 2}[r["lean"]])
    return out


# ───────────────────────────── the prose writers ─────────────────────────────
#
# Two writers produce the same shape. `deterministic_sections` is the floor: it
# always runs, needs no API key and no network, and reads like a person wrote it.
# The model, when available, rewrites the same facts more fluently. Neither is
# allowed to introduce a number that is not in `facts`.


def _upper_first(text: str) -> str:
    """Capitalise only the first character — `str.capitalize` would lowercase
    the rest and turn "S&P 500" into "S&p 500"."""
    return text[:1].upper() + text[1:] if text else text


def _signed(prefix: str, value: float) -> str:
    """Format a percent move, calling a rounds-to-zero move flat.

    `f"{-0.04:+.1f}%"` renders "-0.0%", which reads as a fall that did not
    happen — the kind of detail that quietly costs the prose its credibility.
    """
    if abs(value) < 0.05:
        return f"{prefix} flat".strip()
    return f"{prefix} {value:+.1f}%".strip()


def _join_clauses(parts: Sequence[str]) -> str:
    """Join clauses that themselves contain commas, using semicolons."""
    items = [p for p in parts if p]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return "; ".join(items[:-1]) + "; and " + items[-1]


def _lower_first(text: str) -> str:
    """Lowercase the opening character, unless the first word is an acronym.

    The reasons are full sentences that get spliced after a label, so the
    leading capital has to go — but "The S&P 500 has lost..." must not become
    "s&P 500", which is why the first word is checked before touching it.
    """
    if not text:
        return text
    first_word = text.split(" ", 1)[0]
    if any(ch.isupper() for ch in first_word[1:]):
        return text
    return text[0].lower() + text[1:]


def _join(parts: Sequence[str]) -> str:
    items = [p for p in parts if p]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + " and " + items[-1]


def _describe_move(row: dict[str, Any] | None) -> str:
    if not row:
        return ""
    chg = row.get("change_1d_pct")
    if chg is None:
        return f"{row['label']} at {row['last']:,.2f}"
    direction = "up" if chg > 0 else ("down" if chg < 0 else "flat")
    if direction == "flat":
        return f"{row['label']} barely moved"
    return f"{row['label']} {direction} {abs(chg):.1f}%"


def _overnight_paragraphs(series: dict[str, dict[str, Any]]) -> list[str]:
    us = [series.get(k) for k in ("sp500", "nasdaq")]
    asia = [series.get(k) for k in ("nikkei", "hangseng", "kospi")]
    europe = [series.get(k) for k in ("ftse", "dax")]
    us_rows = [r for r in us if r]
    asia_rows = [r for r in asia if r]
    eu_rows = [r for r in europe if r]

    paragraphs: list[str] = []
    if us_rows:
        avg = sum(r.get("change_1d_pct") or 0 for r in us_rows) / len(us_rows)
        mood = "closed higher" if avg > 0.1 else ("closed lower" if avg < -0.1 else "closed more or less flat")
        trend_bits = []
        for row in us_rows:
            if row.get("above_50dma") is True:
                trend_bits.append(f"{row['label']} is still above its 50-day average")
            elif row.get("above_50dma") is False:
                trend_bits.append(f"{row['label']} is below its 50-day average")
        paragraphs.append(
            f"America {mood} last night — {_join([_describe_move(r) for r in us_rows])}. "
            + (_upper_first(_join(trend_bits)) + ", which tells you whether this was a wobble inside an "
               "uptrend or part of something larger. " if trend_bits else "")
            + "That close is the single biggest input into where India opens, because global funds size "
              "their emerging-market risk off what happened on Wall Street."
        )
    if asia_rows:
        asia_avg = sum(r.get("change_1d_pct") or 0 for r in asia_rows) / len(asia_rows)
        mood = ("trading higher" if asia_avg > 0.1
                else ("trading lower" if asia_avg < -0.1 else "going sideways"))
        # The divergence case is the informative one: Asia disagreeing with the
        # US overnight is usually what decides whether our gap holds or fades.
        divergence = ""
        if us_rows:
            us_avg = sum(r.get("change_1d_pct") or 0 for r in us_rows) / len(us_rows)
            if us_avg < -0.1 < asia_avg:
                divergence = (
                    " Note that Asia is shrugging off the weak American close rather than following it — "
                    "when that happens, a gap-down open here often gets bought back through the day."
                )
            elif asia_avg < -0.1 < us_avg:
                divergence = (
                    " Asia is ignoring a firm American close and selling anyway, which is the more "
                    "worrying of the two: it says the problem is regional rather than imported."
                )
            elif (us_avg < -0.1 and asia_avg < -0.1) or (us_avg > 0.1 and asia_avg > 0.1):
                divergence = " Asia is moving the same way the US did, so there is no conflict to resolve at our open."
        paragraphs.append(
            f"Asia is {mood} as we trade — {_join([_describe_move(r) for r in asia_rows])}. "
            "These markets are open during Indian hours, so they are the live read rather than the stale "
            "overnight one."
            + divergence
            + (" Hong Kong in particular is the China proxy, and it feeds straight into how metals and "
               "commodity names trade here." if series.get("hangseng") else "")
        )
    if eu_rows:
        paragraphs.append(
            f"Europe opens into our afternoon — {_join([_describe_move(r) for r in eu_rows])}. "
            "It matters mostly in the last ninety minutes of our session, when a weak European open can "
            "turn an otherwise decent Indian day into a soft close."
        )
    if not paragraphs:
        paragraphs.append(
            "Global index data could not be retrieved for this session, so there is no overnight read "
            "available. Treat the domestic signals on this page as unconfirmed by global context."
        )
    return paragraphs


def _macro_paragraphs(series: dict[str, dict[str, Any]], pressure: dict[str, Any]) -> list[str]:
    paragraphs: list[str] = []

    crude = series.get("brent")
    dxy = series.get("dxy")
    inr = series.get("usdinr")
    if crude or dxy or inr:
        bits = []
        if crude and crude.get("change_20d_pct") is not None:
            bits.append(
                f"Brent is at ${crude['last']:,.1f} and "
                + _signed("", crude["change_20d_pct"]).lstrip() + " over the month"
            )
        if dxy and dxy.get("change_20d_pct") is not None:
            bits.append(_signed("the dollar index is", dxy["change_20d_pct"]))
        if inr and inr.get("change_20d_pct") is not None:
            bits.append(
                f"the rupee is at {inr['last']:.2f} to the dollar, "
                + _signed("", inr["change_20d_pct"]).lstrip()
            )
        paragraphs.append(
            "Start with the three prices India cannot escape: " + _join(bits) + ". "
            "India imports the large majority of its oil, so crude is effectively a tax on the whole "
            "market's margins; the dollar decides whether foreign funds want emerging-market risk at all; "
            "and the rupee decides what they actually earn once they convert back. When all three move "
            "against us at once, domestic earnings can be perfectly fine and the index still goes nowhere."
        )

    yld = series.get("ust10y")
    gold = series.get("gold")
    vix = series.get("indiavix")
    if yld or gold or vix:
        bits = []
        if yld:
            bits.append(f"the US 10-year is at {yld['last']:.2f}%")
        if gold and gold.get("change_20d_pct") is not None:
            bits.append(_signed("gold is", gold["change_20d_pct"]) + " over the month")
        if vix:
            bits.append(f"India VIX is {vix['last']:.1f}")
        paragraphs.append(
            "Then the risk prices: " + _join(bits) + ". "
            "The US 10-year is the world's discount rate — when it rises, every equity on earth is worth "
            "a little less, and the expensive growth names fall the hardest. Gold tells you whether money "
            "is genuinely frightened or merely rotating. And VIX is the practical one for a swing trader: "
            "it is the market's price for a month of Indian risk, and it decides how much room your stop "
            "needs before ordinary noise takes you out of a good trade."
        )

    tail, head = pressure.get("tailwinds") or [], pressure.get("headwinds") or []
    if tail or head:
        # Each reason is already a full sentence, so they are terminated
        # individually. Comma-joining them produced one run-on clause in which
        # three separate arguments blurred into each other.
        def _run(label: str, reasons: Sequence[str]) -> str:
            body = ". ".join(r.rstrip(".") for r in reasons)
            return f"{label} {_lower_first(body)}. " if body else ""

        sentence = _run("Working against you right now:", head) + _run("Working for you:", tail)
        sentence += (
            pressure.get("verdict", "")
            + " Read that as background weather rather than a signal — it does not tell you what to buy, "
              "it tells you how hard the wind is blowing while you do it."
        )
        paragraphs.append(sentence)

    if not paragraphs:
        paragraphs.append(
            "None of the macro prices — crude, the dollar, the rupee, US yields, gold or VIX — could be "
            "retrieved for this session. Without them there is no read on the external environment at all, "
            "so treat the domestic signals elsewhere on this page as unconfirmed and size accordingly."
        )
    return paragraphs


def _flow_paragraphs(flows: dict[str, Any] | None) -> list[str]:
    if not flows or not flows.get("days"):
        return [
            "Foreign and domestic institutional flow data was not available for this session. That is the "
            "one number most Indian traders check first, so treat today's read as incomplete: without it "
            "you cannot tell whether a weak tape is foreign selling or simply a quiet market."
        ]
    days = flows["days"]
    latest = days[-1]
    fii = latest.get("fii_net_crore")
    dii = latest.get("dii_net_crore")
    fii_10 = flows.get("fii_net_10d_crore")
    dii_10 = flows.get("dii_net_10d_crore")

    first = (
        f"On {latest.get('date')}, foreign institutions were net "
        f"{'buyers' if (fii or 0) >= 0 else 'sellers'} of ₹{abs(fii or 0):,.0f} crore in the cash market, "
        f"while domestic institutions were net {'buyers' if (dii or 0) >= 0 else 'sellers'} of "
        f"₹{abs(dii or 0):,.0f} crore. "
    )
    if fii is not None and dii is not None:
        if fii < 0 < dii:
            first += (
                "That is the pattern India has lived with for years: foreigners leaving, domestic SIP money "
                "absorbing it. It holds the index up, but it rarely produces the sustained, broad advances "
                "that foreign buying does."
            )
        elif fii > 0 and dii > 0:
            first += (
                "Both sides buying at once is the strongest flow backdrop there is, and it is usually when "
                "breakouts follow through instead of stalling."
            )
        elif fii > 0 > dii:
            first += (
                "Foreign buying against domestic selling usually favours the large, liquid index names "
                "over the broader market."
            )
        else:
            first += (
                "Both sides selling is the flow picture that precedes most real corrections — when nobody "
                "is bidding, support levels stop holding."
            )

    second = ""
    sessions = int(flows.get("sessions_in_10d") or 0)
    if fii_10 is not None and sessions >= 2:
        direction = "bought" if fii_10 >= 0 else "sold"
        window = f"last {sessions} session{'s' if sessions != 1 else ''}"
        second = (
            f"Over the {window} foreigners have {direction} roughly ₹{abs(fii_10):,.0f} crore net"
            + (f", against domestic institutions at ₹{dii_10:,.0f} crore. " if dii_10 is not None else ". ")
            + "The running total matters far more than any single day: one day of selling is a rebalance, "
              "ten days of it is a change of mind, and sustained foreign selling has preceded almost every "
              "meaningful Indian correction long before breadth turned ugly."
        )
    elif fii_10 is not None:
        # Only one session on file. Saying "over the last ten sessions" here
        # would restate today's number as a trend — the exact distinction this
        # paragraph exists to make.
        second = (
            "Only one session of flow history is on file so far, so there is no ten-day trend to read yet. "
            "The running total builds up as the page is used; until it does, today's figure is a single "
            "data point and not evidence of a change of mind either way."
        )
    return [p for p in (first, second) if p]


def _news_paragraphs(headlines: Sequence[dict[str, Any]]) -> list[str]:
    if not headlines:
        return [
            "No market headlines were available for this session, so there is no narrative layer on top of "
            "the price data today."
        ]
    titles = [h.get("title", "") for h in headlines[:6] if h.get("title")]
    return [
        "What the market is actually talking about today: " + "; ".join(titles[:4]) + ". "
        "Headlines are the weakest evidence on this page and they are placed last on purpose — price and "
        "flows tell you what is happening, news usually only tells you what people have decided it means. "
        "Use it to explain a move you have already seen, not to predict one you haven't.",
    ]


def _calendar_paragraphs(events: Sequence[dict[str, Any]]) -> list[str]:
    if not events:
        return [
            "Nothing scheduled in the next three weeks is likely to move the whole market. That is the "
            "environment in which stock-specific setups work best, because the tape is free to follow "
            "its own trend rather than waiting on an announcement."
        ]
    near = [e for e in events if e["days_away"] <= 7]
    described = _join([
        f"{e['label']} on {e['date']}"
        + (" (today)" if e["days_away"] == 0 else f" (in {e['days_away']} day{'s' if e['days_away'] != 1 else ''})")
        for e in events[:5]
    ])
    first = f"Coming up: {described}."
    second = (
        "Policy dates and inflation prints are the days when a perfectly good setup fails for reasons that "
        "have nothing to do with the chart. "
        + (
            "With something landing inside the next week, the sensible adjustment is smaller starting "
            "positions rather than no positions — you want to be in the market when it resolves, just not "
            "so large that the resolution decides your month."
            if near else
            "With nothing inside the next week, there is no event reason to hold back; size can be decided "
            "purely on the setup and on the breadth signals elsewhere on this page."
        )
    )
    return [first, second]


def _sector_paragraphs(implications: Sequence[dict[str, Any]]) -> list[str]:
    if not implications:
        return [
            "There isn't enough macro data this session to say which sectors the external environment "
            "favours."
        ]
    favoured = [r for r in implications if r["lean"] == "favoured"]
    pressured = [r for r in implications if r["lean"] == "pressured"]
    paragraphs: list[str] = []
    if favoured:
        paragraphs.append(
            "The macro is currently leaning in favour of "
            + _join_clauses([f"{r['sector']} — it {r['mechanism']}" for r in favoured[:3]])
            + ". This is not a recommendation to buy them; it is the reason to expect relative strength "
              "there, and a reason to trust a breakout in those groups a little more than one fighting "
              "its own macro."
        )
    if pressured:
        paragraphs.append(
            "Leaning against "
            + _join_clauses([f"{r['sector']} — it {r['mechanism']}" for r in pressured[:3]])
            + ". A setup in a pressured group can still work, but it is swimming upstream, and it deserves "
              "either a tighter stop or a smaller size than the same chart in a favoured group."
        )
    paragraphs.append(
        "The point of reading sectors this way is that you stop memorising which group is strong and start "
        "understanding what makes it strong — which means you can see the rotation coming next time instead "
        "of reading about it afterwards."
    )
    return paragraphs


def build_checklist(
    pressure: dict[str, Any],
    linkage: dict[str, Any],
    events: Sequence[dict[str, Any]],
    flows: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Feature 19 — the pre-market checklist, derived rather than typed.

    Each item is a question the rest of this page has already answered, so
    ticking it is a two-second confirmation rather than research. It is logged
    by the frontend so the user can later cross it against their journal.
    """
    near = [e for e in events if e["days_away"] <= 2]
    fii_10 = (flows or {}).get("fii_net_10d_crore")
    flow_sessions = int((flows or {}).get("sessions_in_10d") or 0)
    return [
        {
            "id": "global",
            "label": "I have read the overnight global cues",
            "answer": pressure.get("verdict", ""),
        },
        {
            "id": "macro",
            "label": "I know which way crude, the dollar and the rupee are pushing",
            "answer": (
                f"{len(pressure.get('headwinds') or [])} headwind(s), "
                f"{len(pressure.get('tailwinds') or [])} tailwind(s) in play."
            ),
        },
        {
            "id": "flows",
            "label": "I have checked institutional flows",
            "answer": (
                f"Foreign institutions are net {'buyers' if fii_10 >= 0 else 'sellers'} of "
                f"₹{abs(fii_10):,.0f} crore over {flow_sessions} session"
                f"{'s' if flow_sessions != 1 else ''}."
                if fii_10 is not None and flow_sessions
                else "Flow data unavailable — treat conviction as lower today."
            ),
        },
        {
            "id": "events",
            "label": "No scheduled event is about to decide the market for me",
            "answer": (
                _join([e["label"] for e in near]) + " lands inside two sessions — size accordingly."
                if near else "Nothing major inside two sessions."
            ),
        },
        {
            "id": "coupling",
            "label": "I know whether overseas moves matter today",
            "answer": linkage.get("note", ""),
        },
        {
            "id": "stops",
            "label": "Stops on every open position are updated",
            "answer": "Only you can confirm this one — do it before the first order.",
        },
    ]


def deterministic_sections(facts: dict[str, Any]) -> dict[str, Any]:
    """The always-available prose. Written to be read, not to be a fallback."""
    series = facts.get("series") or {}
    pressure = facts.get("pressure") or {}
    linkage = facts.get("linkage") or {}
    events = facts.get("events") or []
    flows = facts.get("flows")
    headlines = facts.get("headlines") or []
    implications = facts.get("sector_implications") or []

    stance = pressure.get("stance", "mixed")
    headline = {
        "supportive": "The global backdrop is on India's side right now.",
        "hostile": "The global backdrop is working against Indian equities.",
        "mixed": "A mixed global backdrop — nothing forcing your hand either way.",
    }[stance]

    drivers = (pressure.get("headwinds") or pressure.get("tailwinds")
               or ["There is no single dominant external driver today"])[:2]
    summary = [
        # Sentences, not fragments: each reason is already a complete sentence,
        # so they are terminated individually rather than run together — and the
        # headline is not repeated here, since it sits directly above.
        ". ".join(d.rstrip(".") for d in drivers) + ".",
        linkage.get("note", ""),
        (
            "Practically, that means "
            + (
                "you can carry normal size and give positions room; the environment is not the thing that "
                "will beat you."
                if stance == "supportive" else
                "cut starting size, take entries only in the strongest groups, and be quicker than usual to "
                "cut anything that doesn't work immediately."
                if stance == "hostile" else
                "trade the setups you would normally trade, but at somewhere between half and full size — "
                "the environment isn't stopping you, it just isn't helping."
            )
        ),
    ]

    return {
        "source": "computed",
        "headline": headline,
        "stance": stance,
        "summary": [p for p in summary if p],
        "sections": [
            {"id": "overnight", "title": "Overnight — what the world did",
             "paragraphs": _overnight_paragraphs(series)},
            {"id": "macro", "title": "The macro prices that move India",
             "paragraphs": _macro_paragraphs(series, pressure)},
            {"id": "flows", "title": "Who is actually buying",
             "paragraphs": _flow_paragraphs(flows)},
            {"id": "linkage", "title": "How much the outside world matters today",
             "paragraphs": [linkage.get("note") or (
                 "There isn't enough overlapping price history to measure how tightly India is tracking "
                 "global markets right now."
             )]},
            {"id": "sectors", "title": "Which sectors the macro favours",
             "paragraphs": _sector_paragraphs(implications)},
            {"id": "calendar", "title": "What is scheduled ahead",
             "paragraphs": _calendar_paragraphs(events)},
            {"id": "news", "title": "The narrative layer",
             "paragraphs": _news_paragraphs(headlines)},
        ],
        "checklist": build_checklist(pressure, linkage, events, flows),
    }


# ─────────────────────────────── the AI layer ────────────────────────────────

SECTION_SPEC: tuple[tuple[str, str, str], ...] = (
    ("overnight", "Overnight — what the world did",
     "what the US close, live Asian markets and the European open imply for the Indian session"),
    ("macro", "The macro prices that move India",
     "crude, dollar, rupee, US 10-year, gold and VIX — what each is doing and why an Indian trader cares"),
    ("flows", "Who is actually buying",
     "FII and DII flows, the single day and the ten-day trend, and what that pattern usually means"),
    ("linkage", "How much the outside world matters today",
     "whether India is currently coupled to global markets or trading on its own story"),
    ("sectors", "Which sectors the macro favours",
     "the transmission mechanism from these macro prices to specific Indian sectors"),
    ("calendar", "What is scheduled ahead",
     "the scheduled events that could override any chart, and how to size around them"),
    ("news", "The narrative layer",
     "what the market is talking about, framed as the weakest evidence on the page"),
)


def build_prompt(facts: dict[str, Any]) -> str:
    """Ask for prose over the facts — and only over the facts."""
    spec = "\n".join(f'  - "{sid}" ({title}): {what}' for sid, title, what in SECTION_SPEC)
    return f"""You are an experienced Indian equity swing trader writing the daily market-context note for
your own trading desk. Your reader trades NSE stocks on a multi-day to multi-week horizon using
Minervini/O'Neil style breakout and pullback setups.

Below is a block of COUNTED FACTS computed from market data. Write plain-English paragraphs that explain
what is happening in the macro and micro environment and what it means for someone deciding whether to
take risk today.

FACTS:
{json.dumps(facts, indent=1, default=str)}

HARD RULES — these are not style preferences:
1. You may ONLY use numbers that appear in the FACTS block above. Never introduce, estimate, round
   differently, or infer a figure that is not there. A note that invents a number is worse than no note.
2. Where a fact is missing or null, say plainly that it is unavailable. Never fill a gap with a guess.
3. Explain mechanisms, not just direction: say WHY a rising dollar or a rising crude price reaches an
   Indian trader's P&L. The reader should finish each paragraph understanding something, not just
   informed.
4. Describe conditions and the sizing implications of those conditions. Do not issue buy or sell calls on
   individual stocks, and do not give personalised investment advice.
5. Write in simple language. No jargon the reader has to decode, no bullet-point telegraphese, no hedging
   filler. Full sentences, 3-6 sentences per paragraph.

Return ONLY valid JSON, no markdown fences:
{{
  "headline": "one plain sentence capturing the external environment today",
  "stance": "supportive|mixed|hostile",
  "summary": ["2-3 paragraphs: the whole picture, ending with what it means for position size today"],
  "sections": [
    {{"id": "<one of the ids below>", "title": "<the title given below>", "paragraphs": ["1-3 paragraphs"]}}
  ]
}}

Produce one entry in "sections" for each of these ids, in this order:
{spec}
"""


def validate_narrative(payload: Any, facts: dict[str, Any]) -> dict[str, Any] | None:
    """Accept the model's prose only if it is structurally complete.

    Checked rather than trusted: a partial response that renders three empty
    sections looks like a broken page, and the computed writer is a perfectly
    good answer — so anything short of a full, well-formed note is rejected in
    favour of it.
    """
    if not isinstance(payload, dict):
        return None
    stance = payload.get("stance")
    if stance not in ("supportive", "mixed", "hostile"):
        return None
    headline = payload.get("headline")
    if not isinstance(headline, str) or len(headline.strip()) < 10:
        return None

    summary = [p.strip() for p in payload.get("summary") or [] if isinstance(p, str) and p.strip()]
    if not summary:
        return None

    wanted = {sid: title for sid, title, _ in SECTION_SPEC}
    by_id: dict[str, dict[str, Any]] = {}
    for raw in payload.get("sections") or []:
        if not isinstance(raw, dict):
            continue
        sid = raw.get("id")
        if sid not in wanted:
            continue
        paragraphs = [p.strip() for p in raw.get("paragraphs") or [] if isinstance(p, str) and p.strip()]
        if not paragraphs:
            continue
        title = raw.get("title")
        by_id[sid] = {
            "id": sid,
            "title": title if isinstance(title, str) and title.strip() else wanted[sid],
            "paragraphs": paragraphs,
        }

    # A note missing half its sections is not a note. Require most of them.
    if len(by_id) < len(wanted) - 1:
        return None

    pressure = facts.get("pressure") or {}
    return {
        "source": "ai",
        "headline": headline.strip(),
        # The stance is a computed verdict, not an opinion: the model may phrase
        # it, it may not overrule the arithmetic that produced it.
        "stance": pressure.get("stance", stance),
        "summary": summary,
        "sections": [by_id[sid] for sid, _, _ in SECTION_SPEC if sid in by_id],
        "checklist": build_checklist(
            pressure,
            facts.get("linkage") or {},
            facts.get("events") or [],
            facts.get("flows"),
        ),
    }


def envelope(facts: dict[str, Any], note: dict[str, Any]) -> dict[str, Any]:
    return {
        "available": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of": facts.get("as_of"),
        "stale": facts.get("stale", False),
        "stale_reason": facts.get("stale_reason"),
        "note": note,
        "facts": facts,
    }


# ────────────────────────────── data sourcing ────────────────────────────────


def fetch_closes(symbols: Sequence[str], *, days: int = HISTORY_DAYS) -> dict[str, dict[str, Any]]:
    """Daily closes per symbol from Yahoo, as {symbol: {"closes": [...], "as_of": iso}}.

    Symbols are fetched in one batched call and failures are per-symbol: Yahoo
    regularly serves some of these and refuses others from the same IP (see the
    module docstring), so one refused ticker must never cost us the other
    thirteen. A symbol that comes back empty is simply absent from the result
    and the caller reports it as unavailable rather than as zero.
    """
    import pandas as pd  # local: keeps import cost off the startup path
    import yfinance as yf

    out: dict[str, dict[str, Any]] = {}
    try:
        frame = yf.download(
            list(symbols),
            period=f"{days}d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        logger.warning("macro-context: batch download failed: %s", exc)
        return out

    if frame is None or frame.empty:
        return out

    for symbol in symbols:
        try:
            if isinstance(frame.columns, pd.MultiIndex):
                if symbol not in frame.columns.get_level_values(0):
                    continue
                col = frame[symbol]["Close"]
            else:
                col = frame["Close"]
            col = col.dropna()
            if col.empty:
                continue
            closes = [float(v) for v in col.tolist()]
            as_of = col.index[-1]
            out[symbol] = {
                "closes": closes,
                "as_of": as_of.date().isoformat() if hasattr(as_of, "date") else str(as_of),
            }
        except Exception as exc:  # one bad frame must not sink the rest
            logger.debug("macro-context: %s unusable: %s", symbol, exc)
            continue
    return out


def fetch_fii_dii(*, timeout: float = 8.0) -> dict[str, Any] | None:
    """Feature 13 — institutional cash-market flows from NSE.

    Best-effort by design. NSE's JSON endpoints require a primed cookie and
    refuse datacenter ranges often enough that this must be allowed to fail;
    when it does, the caller serves the last good value from the cache file and
    the prose says the flow read is missing rather than pretending it is zero.
    """
    import httpx

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/reports/fii-dii",
    }
    try:
        with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
            client.get("https://www.nseindia.com/")  # primes the cookie jar
            resp = client.get("https://www.nseindia.com/api/fiidiiTradeReact")
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        logger.info("macro-context: FII/DII fetch unavailable (%s)", exc)
        return None

    if not isinstance(payload, list) or not payload:
        return None

    def _num(value: Any) -> float | None:
        try:
            return float(str(value).replace(",", ""))
        except (TypeError, ValueError):
            return None

    by_date: dict[str, dict[str, Any]] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        day = row.get("date")
        category = str(row.get("category") or "").upper()
        net = _num(row.get("netValue"))
        if not day or net is None:
            continue
        entry = by_date.setdefault(str(day), {"date": str(day)})
        if "FII" in category or "FPI" in category:
            entry["fii_net_crore"] = net
        elif "DII" in category:
            entry["dii_net_crore"] = net

    days = [v for v in by_date.values() if "fii_net_crore" in v or "dii_net_crore" in v]
    if not days:
        return None
    return {"days": days, "source": "nseindia"}


def merge_flow_history(existing: dict[str, Any] | None, fresh: dict[str, Any] | None) -> dict[str, Any] | None:
    """Accumulate flow days across runs so a ten-day trend can exist at all.

    NSE's endpoint returns only the newest session or two. The ten-day net is
    the number that actually distinguishes a rebalance from a change of mind,
    so history is kept in the cache file and merged forward rather than
    recomputed from a window we are never given.
    """
    days: dict[str, dict[str, Any]] = {}
    for source in (existing, fresh):
        for row in (source or {}).get("days") or []:
            day = row.get("date")
            if day:
                days[str(day)] = {**days.get(str(day), {}), **row}
    if not days:
        return None
    ordered = sorted(days.values(), key=lambda r: str(r.get("date")))[-40:]
    tail = ordered[-10:]
    fii_10 = sum(r.get("fii_net_crore") or 0.0 for r in tail) if tail else None
    dii_10 = sum(r.get("dii_net_crore") or 0.0 for r in tail) if tail else None
    return {
        "days": ordered,
        "fii_net_10d_crore": _round(fii_10, 1),
        "dii_net_10d_crore": _round(dii_10, 1),
        "sessions_in_10d": len(tail),
        "source": (fresh or existing or {}).get("source", "nseindia"),
    }


def load_cache(data_dir: Path) -> dict[str, Any] | None:
    path = data_dir / CACHE_FILENAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("macro-context: cache unreadable: %s", exc)
        return None


def save_cache(data_dir: Path, payload: dict[str, Any]) -> None:
    path = data_dir / CACHE_FILENAME
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
        tmp.replace(path)  # atomic: a killed worker must not leave a half-written cache
    except OSError as exc:
        logger.warning("macro-context: could not write cache: %s", exc)


def build_facts(
    raw_closes: dict[str, dict[str, Any]],
    flows: dict[str, Any] | None,
    headlines: Sequence[dict[str, Any]],
    *,
    today: date | None = None,
) -> dict[str, Any]:
    """Everything the prose is allowed to talk about, and nothing else."""
    today = today or datetime.now(timezone.utc).date()
    series: dict[str, dict[str, Any]] = {}
    returns_by_key: dict[str, list[float]] = {}
    latest_session: str | None = None

    for spec in ALL_SERIES:
        raw = raw_closes.get(spec.symbol)
        if not raw:
            continue
        closes = raw.get("closes") or []
        row = summarise_series(spec, closes, raw.get("as_of"))
        if not row:
            continue
        series[spec.key] = row
        returns_by_key[spec.key] = _returns([float(c) for c in closes if c])
        if row["as_of"] and (latest_session is None or row["as_of"] > latest_session):
            latest_session = row["as_of"]

    pressure = assess_pressure(series)
    return {
        "as_of": latest_session,
        "stale": False,
        "stale_reason": None,
        "missing_series": sorted(s.label for s in ALL_SERIES if s.key not in series),
        "series": series,
        "flows": flows,
        "linkage": assess_linkage(returns_by_key),
        "pressure": pressure,
        "sector_implications": sector_implications(series),
        "events": upcoming_events(today),
        "headlines": [
            {"title": h.get("title"), "source": h.get("source"), "published": h.get("published")}
            for h in list(headlines)[:8]
            if isinstance(h, dict) and h.get("title")
        ],
    }


def mark_stale(facts: dict[str, Any], generated_at: str | None) -> dict[str, Any]:
    """Flag cached facts with their real age so the UI can discount them."""
    facts = dict(facts)
    facts["stale"] = True
    age_hours: float | None = None
    if generated_at:
        try:
            when = datetime.fromisoformat(generated_at)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - when).total_seconds() / 3600.0
        except ValueError:
            age_hours = None
    if age_hours is None:
        facts["stale_reason"] = "Live market data could not be fetched; showing the last saved context."
    elif age_hours >= STALE_HARD_HOURS:
        facts["stale_reason"] = (
            f"Live market data could not be fetched and the saved context is {age_hours / 24:.0f} days old. "
            "Do not trade off it."
        )
    else:
        facts["stale_reason"] = (
            f"Live market data could not be fetched; this context is about {age_hours:.0f} hours old."
        )
    facts["age_hours"] = _round(age_hours, 1)
    return facts
