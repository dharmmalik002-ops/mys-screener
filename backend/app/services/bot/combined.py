"""The two surviving components held together, and what that is worth.

Only two things in this project survived scrutiny: **regime timing** (when to
be in the market at all) and the **defensive breaker** (which cells to stand
down). Everything else measured null or negative. This holds both as sleeves
and measures the result against real fund managers.

They are complementary, which is the reason to hold both rather than the
better one. Over the held-out window the blend's Sharpe beats either sleeve
alone — 1.20 against the book's 0.90 and timing's 0.97 — and its drawdown is
shallower than either. Two rules that are wrong at different times cover for
each other; that is diversification doing its ordinary job, not a new edge.

**Against 691 real Indian equity funds over the exact same three years:**

    fund median CAGR      +11.36%      blend  +7.69%
    fund median drawdown  -27.53%      blend  -7.28%
    fund median Sharpe       0.40      blend   1.15
    funds beating the blend on return AND drawdown: 5 of 691 (0.7%)

Read that honestly, both halves. The blend **earns less than the median fund**
and sits in the 18.5th percentile on return alone; anyone who cares only about
return should prefer most of these funds. What it does is earn near-median
money at a quarter of the drawdown, which is why only five funds dominate it
on the two measures at once. `funds_dominating` is the measure that settles it
precisely because a fund can win either axis by losing the other.

**Two caveats are load-bearing and must travel with the number.** The bot may
sit in cash and a fund may not — a large structural advantage in a falling
market that is not skill. And the bot's returns are simulated while the funds'
are realised money, net of fees actually charged. `benchmark.CAVEATS` carries
the full list; it is not decoration.

**What the learning contributes to the finished product**, measured by running
the identical blend with the breaker switched off:

    held-out:   CAGR +0.22pp   maxDD +2.03pp   Sharpe +0.04
    exact 3y:   CAGR -0.11pp   maxDD +1.29pp   Sharpe -0.01

Drawdown, in both windows, and nothing else. That is the same answer the
breaker gave on its own bench, which is the point of running the control: the
learned component does not grow a return contribution on its way into the
product.
"""

from __future__ import annotations

from datetime import date
from typing import Sequence

import numpy as np

PRIMARY_WEIGHT = 0.50

# Measured by scripts/combined_product.py, exact 3-year fund-matched window.
MEASURED_FUNDS_COUNTED = 691
MEASURED_FUNDS_DOMINATING = 5
MEASURED_BLEND_CAGR = 7.69
MEASURED_BLEND_DRAWDOWN = -7.28
MEASURED_BLEND_SHARPE = 1.15
MEASURED_FUND_MEDIAN_CAGR = 11.36
MEASURED_FUND_MEDIAN_DRAWDOWN = -27.53
MEASURED_FUND_MEDIAN_SHARPE = 0.40
MEASURED_RETURN_PERCENTILE = 18.5

# Contribution of the learned breaker inside the blend.
MEASURED_LEARNING_DRAWDOWN_GAIN_HELD_OUT = 2.03
MEASURED_LEARNING_CAGR_GAIN_HELD_OUT = 0.22
MEASURED_LEARNING_CAGR_GAIN_3Y = -0.11


# --- The risk-frontier limit, measured by scripts/risk_frontier.py ----------
# The obvious objection to "earns less than the median fund" is that the blend
# runs at a quarter of the funds' risk. Turning the risk up does not fix it:
# the account is already ~97% deployed, so a larger risk budget concentrates
# the same capital rather than adding any, and the right-skewed R distribution
# is sampled worse. Return falls, drawdown rises. No margin is ever used.
MEASURED_DEPLOYED_PCT_AT_SHIPPED_RISK = 96.7
MEASURED_BOOK_CAGR_AT_RISK_010 = 5.26
MEASURED_BOOK_CAGR_AT_RISK_030 = 5.04
MEASURED_BOOK_CAGR_AT_RISK_060 = 1.61
MEASURED_BOOK_DRAWDOWN_AT_RISK_010 = -9.18
MEASURED_BOOK_DRAWDOWN_AT_RISK_060 = -14.62


def return_can_be_bought_with_risk() -> bool:
    """False. The advantage is on the risk axis and does not convert."""
    return MEASURED_BOOK_CAGR_AT_RISK_060 > MEASURED_BOOK_CAGR_AT_RISK_010


def curve_to_series(curve: Sequence[dict]) -> tuple[list[date], np.ndarray]:
    days = [date.fromisoformat(str(p["day"])) for p in curve]
    return days, np.asarray([float(p["equity"]) for p in curve], dtype=np.float64)


def blend(book: Sequence[dict], timed: Sequence[dict], weight: float = PRIMARY_WEIGHT):
    """Two sleeves on their shared calendar, each normalised to 1.0 at the start.

    **No rebalancing.** Each sleeve compounds alone and the total is their sum.
    Periodic rebalancing between two positively-performing sleeves manufactures
    a return that depends entirely on the interval chosen, and choosing the
    interval that looks best is how this project produced eight false
    positives.

    Forward-filled onto the union of both calendars. The stock book only prints
    a point on sessions it was active, so intersecting the calendars would drop
    exactly the flat stretches the timing sleeve exists to cover — which would
    silently delete the periods where the blend is supposed to earn its keep.
    """
    a_days, a_val = curve_to_series(book)
    b_days, b_val = curve_to_series(timed)
    if len(a_days) < 2 or len(b_days) < 2:
        return None
    a = dict(zip(a_days, a_val / a_val[0]))
    b = dict(zip(b_days, b_val / b_val[0]))

    out_days, out_val = [], []
    last_a = last_b = 1.0
    for day in sorted(set(a_days) | set(b_days)):
        last_a = a.get(day, last_a)
        last_b = b.get(day, last_b)
        out_days.append(day)
        out_val.append(weight * last_a + (1.0 - weight) * last_b)
    return out_days, np.asarray(out_val, dtype=np.float64)


def stats(days: Sequence[date], values: np.ndarray) -> dict:
    """CAGR, worst peak-to-trough, and a Sharpe taken on monthly marks."""
    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
    cagr = (float(values[-1] / values[0]) ** (1.0 / years) - 1.0) * 100.0
    peak = np.maximum.accumulate(values)
    drawdown = float(((values - peak) / peak).min() * 100.0)

    monthly: dict[str, float] = {}
    for day, value in zip(days, values):
        monthly[f"{day.year}-{day.month:02d}"] = float(value)
    series = [monthly[k] for k in sorted(monthly)]
    sharpe = 0.0
    if len(series) > 3:
        returns = np.diff(series) / np.asarray(series[:-1])
        if returns.std() > 0:
            sharpe = float(returns.mean() / returns.std() * np.sqrt(12))
    return {
        "years": round(years, 2),
        "cagr_pct": round(cagr, 2),
        "max_drawdown_pct": round(drawdown, 2),
        "sharpe": round(sharpe, 2),
        "return_per_drawdown": round(cagr / abs(drawdown), 2) if drawdown else None,
    }


def funds_dominating_pct() -> float:
    """Share of real funds beating the blend on return AND drawdown at once."""
    return round(100.0 * MEASURED_FUNDS_DOMINATING / MEASURED_FUNDS_COUNTED, 1)


def earns_less_than_median_fund() -> bool:
    """True, and it stays in every summary of this result."""
    return MEASURED_BLEND_CAGR < MEASURED_FUND_MEDIAN_CAGR
