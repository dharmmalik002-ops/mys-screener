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

**Against 691 real Indian equity funds over the exact same three years**, each
configuration measured separately — because reporting the comparison for one
weighting only would let the weighting be chosen after seeing the fund result:

                        CAGR      maxDD    percentile   funds beating it on BOTH
    timing alone      +12.63%    -12.12%      59.3        6 of 691  (0.9%)
    blend 50/50        +9.05%     -7.54%      29.5        5 of 691  (0.7%)
    stock book alone   +5.26%     -9.18%       3.2       46 of 691  (6.7%)
    median fund       +11.36%    -27.53%      50.0

**The timing sleeve beats the median fund on return AND drawdown at once**, by
+1.27pp and by 15.4 points respectively, and only six funds of 691 beat it on
both. That is the claim this project supports, and it belongs to the component
that decides *when to be exposed* — not to stock selection, which sits in the
3.2nd percentile and is beaten on both axes by 46 funds.

The 50/50 blend is **not** the return-maximising choice and is not presented as
one: it trades 3.6pp of return for 4.6 points of drawdown and a slightly better
Sharpe. Which of the two is the better product depends on the reader's
tolerance, so both are reported rather than one being declared the winner.

`funds_dominating` is the measure that settles any of these, precisely because
a fund can win either axis by losing the other.

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
MEASURED_BLEND_CAGR = 9.05
MEASURED_BLEND_DRAWDOWN = -7.54
MEASURED_BLEND_SHARPE = 1.22
MEASURED_FUND_MEDIAN_CAGR = 11.36
MEASURED_FUND_MEDIAN_DRAWDOWN = -27.53
MEASURED_FUND_MEDIAN_SHARPE = 0.40
MEASURED_RETURN_PERCENTILE = 29.5

# The timing sleeve, measured on the same fund-matched window. This is the
# configuration that beats the median fund on BOTH axes.
MEASURED_TIMING_CAGR = 12.63
MEASURED_TIMING_DRAWDOWN = -12.12
MEASURED_TIMING_FUNDS_DOMINATING = 6
MEASURED_TIMING_RETURN_PERCENTILE = 59.3

# Stock selection on the same window, for contrast: beaten on both axes by 46
# funds, and in the 3.2nd percentile on return.
MEASURED_BOOK_CAGR = 5.26
MEASURED_BOOK_FUNDS_DOMINATING = 46

# Contribution of the learned breaker inside the blend.
MEASURED_LEARNING_DRAWDOWN_GAIN_HELD_OUT = 2.31
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


def blend_earns_less_than_median_fund() -> bool:
    """True for the 50/50 blend, and it stays in every summary of it.

    The blend buys drawdown with return. That is a legitimate trade and it is
    not a free lunch, so the losing half travels with the winning one.
    """
    return MEASURED_BLEND_CAGR < MEASURED_FUND_MEDIAN_CAGR


def timing_beats_median_fund_on_both() -> bool:
    """True — the claim this project actually supports.

    Return above the median fund by more than `benchmark.CAGR_TIE_BAND`, and a
    drawdown less than half as deep. It belongs to the component that decides
    when to be exposed, not to stock selection.
    """
    return (
        MEASURED_TIMING_CAGR - MEASURED_FUND_MEDIAN_CAGR > 0.5
        and MEASURED_TIMING_DRAWDOWN > MEASURED_FUND_MEDIAN_DRAWDOWN
    )


def selection_is_beaten_by_many_funds() -> bool:
    """Also true, and reported beside the win rather than beneath it."""
    return MEASURED_BOOK_FUNDS_DOMINATING > MEASURED_TIMING_FUNDS_DOMINATING
