"""A trader's measurable rules, checked on any chart at any date.

The look-alike model learns a style from pictures; these are the same trader's
WRITTEN rules, expressed as numbers so they can be tested rather than trusted.
Each rule is evaluated three ways in the library build (`pipeline.py`):

* how often the trader's own setups pass it, against random days in the same
  stocks — does the rule actually describe what he buys?
* among his setups, how often the ones that pass went on to work, against the
  ones that fail it — does the rule pick winners?
* on every Indian chart in the daily scan, so each match carries a checklist.

The thresholds are Minervini's published Trend Template (Trade Like a Stock
Market Wizard, 2013) plus the base-tightening ideas from his courses, stated as
plainly as possible and declared here before any of them was measured. Nothing
is fitted. Every rule reads bars up to `end` only.
"""

from __future__ import annotations

import numpy as np

# Sessions, not calendar days.
MONTH = 21
YEAR = 252

RULES: list[tuple[str, str]] = [
    ("above_150_200", "Price above its 150-day and 200-day averages"),
    ("ma150_above_ma200", "150-day average above the 200-day"),
    ("ma200_rising", "200-day average rising for at least a month"),
    ("ma50_above_150_200", "50-day average above the 150-day and 200-day"),
    ("above_50", "Price above its 50-day average"),
    ("off_low_30", "At least 30% above its 52-week low"),
    ("near_high_25", "Within 25% of its 52-week high"),
    ("beats_index", "Outperformed the index over the last 6 months"),
    ("tightening", "Last 2 weeks' range tighter than the base's average"),
    ("volume_dry_up", "Volume over the last 2 weeks below its 50-day average"),
]
TEMPLATE = [k for k, _ in RULES[:8]]
MIN_BARS = YEAR + MONTH


def _sma(c: np.ndarray, end: int, n: int) -> float:
    return float(c[end - n + 1 : end + 1].mean())


def metrics(o, h, l, c, v, end: int, index_return_6m: float | None) -> dict[str, float] | None:
    """The numbers behind the rules at index `end` — what a pick's description
    quotes. None without a year of history."""
    if end < MIN_BARS or end >= len(c):
        return None
    c = np.asarray(c, dtype=float)
    h = np.asarray(h, dtype=float)
    l = np.asarray(l, dtype=float)
    v = np.nan_to_num(np.asarray(v, dtype=float))
    price = c[end]
    ma50, ma150, ma200 = _sma(c, end, 50), _sma(c, end, 150), _sma(c, end, 200)
    hi52 = float(h[end - YEAR + 1 : end + 1].max())
    lo52 = float(l[end - YEAR + 1 : end + 1].min())

    def rng(a: int, b: int) -> float:
        return float((h[a:b].max() - l[a:b].min()) / c[b - 1])

    vol50 = float(v[end - 49 : end + 1].mean())
    return {
        "price": float(price),
        "ma50": ma50,
        "ma150": ma150,
        "ma200": ma200,
        "ma200_month_ago": _sma(c, end - MONTH, 200),
        "below_high_pct": (1 - price / hi52) * 100,
        "above_low_pct": (price / lo52 - 1) * 100,
        "return_6m_pct": (price / c[end - 126] - 1) * 100,
        "index_return_6m_pct": None if index_return_6m is None else index_return_6m * 100,
        "range_2w_pct": rng(end - 9, end + 1) * 100,
        "range_base_pct": float(np.mean([rng(s, s + 10) for s in range(end - 69, end - 9, 10)])) * 100,
        "volume_ratio": float(v[end - 9 : end + 1].mean()) / vol50 if vol50 > 0 else float("nan"),
    }


def check(o, h, l, c, v, end: int, index_return_6m: float | None) -> dict[str, bool] | None:
    """Every rule at index `end`. None when there is not a year of history,
    because half the template cannot be judged on less. `index_return_6m` is the
    benchmark's return over the same 126 sessions (None when unknown — then the
    relative-strength rule is reported as failing rather than guessed)."""
    m = metrics(o, h, l, c, v, end, index_return_6m)
    return None if m is None else flags_from(m)


def flags_from(m: dict[str, float]) -> dict[str, bool]:
    price = m["price"]
    idx = m["index_return_6m_pct"]
    vr = m["volume_ratio"]
    return {
        "above_150_200": bool(price > m["ma150"] and price > m["ma200"]),
        "ma150_above_ma200": bool(m["ma150"] > m["ma200"]),
        "ma200_rising": bool(m["ma200"] > m["ma200_month_ago"]),
        "ma50_above_150_200": bool(m["ma50"] > m["ma150"] and m["ma50"] > m["ma200"]),
        "above_50": bool(price > m["ma50"]),
        "off_low_30": bool(m["above_low_pct"] >= 30.0),
        "near_high_25": bool(m["below_high_pct"] <= 25.0),
        "beats_index": bool(idx is not None and m["return_6m_pct"] > idx),
        "tightening": bool(m["range_2w_pct"] < m["range_base_pct"]),
        "volume_dry_up": bool(np.isfinite(vr) and vr < 1.0),
    }


def template_score(flags: dict[str, bool] | None) -> int | None:
    return None if flags is None else sum(1 for k in TEMPLATE if flags.get(k))


def index_return(dates: list, closes: np.ndarray, day, prior_day) -> float | None:
    """Benchmark return from `prior_day` to `day`, each mapped to the last
    session on or before it. None when the benchmark does not cover both."""
    import bisect

    i = bisect.bisect_right(dates, day) - 1
    j = bisect.bisect_right(dates, prior_day) - 1
    if i < 0 or j < 0 or closes[j] <= 0:
        return None
    return float(closes[i] / closes[j] - 1)
