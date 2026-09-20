"""Regime timing: hold the index when the tape is healthy, park cash otherwise.

This is the one thing in this project that beats a professional, and it works
because it asks the question the system can actually answer.

Everything else here tries to pick *which* stock to buy, and that fails
comprehensively: under rolling walk-forward the stock-selection book compounds
at -2.6% a year with a signal edge that is negative in nine years out of
eleven. The regime classifier, built from the same data, is not doing that job.
It answers *when to be exposed*, and that turns out to be a question the data
supports.

    held-out 2017-2026        CAGR     max drawdown
    regime timing           +14.75%          -10.9%
    buy and hold Nifty 500  +12.95%          -38.3%
    median equity fund      +11.36%          -27.5%

The rule was chosen the same way every other decision here was: five candidate
regime sets declared in advance, scored on 2007-2017 on return-per-drawdown,
and the second half run once. `bulls + recovery` won the first half and was not
adjusted afterwards.

Three frictions are charged, because without them this would be another of the
false positives this project has produced six of:

  *Cash earns a real rate.* Sitting out 42% of the time at 0% is as unrealistic
  as ignoring slippage. Indian liquid funds returned 6-7% over the period; the
  default here is 6% and the result survives 3%.

  *Switching costs.* 5 bps each way, 34 switches over the held-out decade.

  *Tax is charged on every realised gain*, at the harsher short-term rate when
  a holding lasted under a year — which most do, at an average 151-day hold.
  With 20% STCG and a conservative 4% cash rate it still returns +12.27% at a
  -14.1% drawdown, beating the median fund on both counts.

What it is not: a stock-picking system, or something that avoids sitting in
cash for months at a time while the market rises. The drawdown advantage comes
precisely from being absent, and being absent is the part a person finds hard.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date
from typing import Mapping, Sequence

import numpy as np

logger = logging.getLogger(__name__)

# Regimes to be invested in. Chosen on 2007-2017 by return-per-drawdown from
# five candidates declared in advance; see the module docstring.
INVESTED_REGIMES = frozenset({"bull_strong", "bull_narrow", "recovery"})

# Indian liquid funds / T-bills over the period. Conservative; the result
# survives 3%.
DEFAULT_CASH_RATE_PCT = 6.0
SWITCH_COST_BPS = 5.0
# Short-term capital gains on Indian equity. Charged on every realised gain,
# with the long-term rate applied only where a holding genuinely exceeded a
# year — a timing rule mostly pays the higher one.
STCG_PCT = 20.0
LTCG_FRACTION = 0.625          # 12.5% LTCG as a fraction of the 20% STCG rate
TRADING_DAYS = 252


@dataclass
class TimingResult:
    label: str
    start: str
    end: str
    cagr_pct: float
    max_drawdown_pct: float
    return_per_drawdown: float
    exposure_pct: float
    switches: int
    mean_hold_days: float
    cash_rate_pct: float
    tax_pct: float
    equity_curve: list[dict]

    def to_dict(self) -> dict:
        return asdict(self)


def simulate(
    sessions: Sequence[date],
    closes: Mapping[date, float],
    regime_by_day: Mapping[date, str],
    *,
    invested_regimes: frozenset[str] = INVESTED_REGIMES,
    cash_rate_pct: float = DEFAULT_CASH_RATE_PCT,
    tax_pct: float = STCG_PCT,
    switch_cost_bps: float = SWITCH_COST_BPS,
    label: str = "regime_timing",
) -> TimingResult | None:
    """Run the rule over `sessions`, which must be the index's own calendar."""
    window = [d for d in sessions if d in closes]
    if len(window) < TRADING_DAYS // 2:
        return None

    daily_cash = (1.0 + cash_rate_pct / 100.0) ** (1.0 / TRADING_DAYS) - 1.0
    equity, invested = 1.0, False
    entry_equity: float | None = None
    entry_index: int | None = None
    peak, drawdown = 1.0, 0.0
    switches, holds = 0, []
    curve: list[dict] = []

    for i in range(1, len(window)):
        previous, current = window[i - 1], window[i]
        # Decided on the PRIOR session's label: today's regime is not knowable
        # until today closes, so acting on it would be look-ahead.
        want = regime_by_day.get(previous) in invested_regimes

        if want != invested:
            if invested and entry_equity is not None and entry_index is not None:
                gain = equity / entry_equity - 1.0
                held = (previous - window[entry_index]).days
                if gain > 0 and tax_pct > 0:
                    rate = tax_pct if held < 365 else tax_pct * LTCG_FRACTION
                    equity *= 1.0 - (gain / (1.0 + gain)) * rate / 100.0
                holds.append(held)
            equity *= 1.0 - switch_cost_bps / 10_000.0
            invested = want
            switches += 1
            if invested:
                entry_equity, entry_index = equity, i

        if invested and closes[previous] > 0:
            equity *= closes[current] / closes[previous]
        else:
            equity *= 1.0 + daily_cash

        peak = max(peak, equity)
        drawdown = min(drawdown, (equity - peak) / peak * 100.0)
        curve.append({"day": current.isoformat(), "equity": round(equity, 5), "invested": invested})

    years = max((window[-1] - window[0]).days / 365.25, 1e-9)
    cagr = (equity ** (1.0 / years) - 1.0) * 100.0
    exposure = 100.0 * sum(
        1 for d in window if regime_by_day.get(d) in invested_regimes
    ) / len(window)

    return TimingResult(
        label=label,
        start=window[0].isoformat(),
        end=window[-1].isoformat(),
        cagr_pct=round(cagr, 2),
        max_drawdown_pct=round(drawdown, 2),
        return_per_drawdown=round(cagr / abs(drawdown), 2) if drawdown else 0.0,
        exposure_pct=round(exposure, 1),
        switches=switches,
        mean_hold_days=round(float(np.mean(holds)), 0) if holds else 0.0,
        cash_rate_pct=cash_rate_pct,
        tax_pct=tax_pct,
        equity_curve=curve[:: max(1, len(curve) // 400)],
    )


def buy_and_hold(sessions: Sequence[date], closes: Mapping[date, float]) -> TimingResult | None:
    """The passive alternative, measured identically so the comparison is fair."""
    window = [d for d in sessions if d in closes]
    if len(window) < 2:
        return None
    equity, peak, drawdown = 1.0, 1.0, 0.0
    curve: list[dict] = []
    for i in range(1, len(window)):
        equity *= closes[window[i]] / closes[window[i - 1]]
        peak = max(peak, equity)
        drawdown = min(drawdown, (equity - peak) / peak * 100.0)
        curve.append({"day": window[i].isoformat(), "equity": round(equity, 5), "invested": True})
    years = max((window[-1] - window[0]).days / 365.25, 1e-9)
    cagr = (equity ** (1.0 / years) - 1.0) * 100.0
    return TimingResult(
        label="buy_and_hold", start=window[0].isoformat(), end=window[-1].isoformat(),
        cagr_pct=round(cagr, 2), max_drawdown_pct=round(drawdown, 2),
        return_per_drawdown=round(cagr / abs(drawdown), 2) if drawdown else 0.0,
        exposure_pct=100.0, switches=0, mean_hold_days=0.0,
        cash_rate_pct=0.0, tax_pct=0.0,
        equity_curve=curve[:: max(1, len(curve) // 400)],
    )


@dataclass
class TimingHealth:
    """Whether the timing rule is still doing what the study said it would.

    The rest of this project's learning machinery audits stock cells, which is
    exactly the wrong place to point it: the cells have no edge, so monitoring
    them monitors noise. This points the same discipline at the component that
    actually earns — and the asymmetry from `calibration.py` carries over. A
    rule running *better* than its study is never promoted, because a good
    stretch on a handful of switches is the most seductive noise there is.

    What it checks is not the return, which is too noisy over a few switches to
    say anything. It is whether the rule is still *behaving* as measured: the
    share of time invested, and whether being invested has been better than
    being out. A regime rule that stops distinguishing good tape from bad has
    broken, whatever its recent P&L happens to be.
    """

    completed_switches: int
    exposure_pct: float
    expected_exposure_pct: float
    invested_avg_daily_pct: float
    cash_avg_daily_pct: float
    discrimination_pp: float      # invested minus out-of-market, per day
    status: str                   # tracking | diverging | insufficient
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


# Below this a live record says nothing: a timing rule makes a handful of
# decisions a year, so judging it on a couple of switches is judging noise.
MIN_SWITCHES_TO_JUDGE = 6
# The rule's whole claim is that invested days beat out-of-market days. If that
# gap closes, the classifier has stopped discriminating.
MIN_DISCRIMINATION_PP = 0.0


def assess_health(
    sessions: Sequence[date],
    closes: Mapping[date, float],
    regime_by_day: Mapping[date, str],
    expected_exposure_pct: float,
    *,
    invested_regimes: frozenset[str] = INVESTED_REGIMES,
) -> TimingHealth:
    """Is the rule still separating good tape from bad, on recent sessions?"""
    window = [d for d in sessions if d in closes]
    invested_moves: list[float] = []
    cash_moves: list[float] = []
    switches = 0
    previous_state: bool | None = None

    for i in range(1, len(window)):
        a, b = window[i - 1], window[i]
        state = regime_by_day.get(a) in invested_regimes
        if previous_state is not None and state != previous_state:
            switches += 1
        previous_state = state
        if closes[a] <= 0:
            continue
        move = (closes[b] / closes[a] - 1.0) * 100.0
        (invested_moves if state else cash_moves).append(move)

    exposure = (
        100.0 * len(invested_moves) / max(len(invested_moves) + len(cash_moves), 1)
    )
    invested_avg = float(np.mean(invested_moves)) if invested_moves else 0.0
    cash_avg = float(np.mean(cash_moves)) if cash_moves else 0.0
    discrimination = invested_avg - cash_avg

    if switches < MIN_SWITCHES_TO_JUDGE or not cash_moves:
        return TimingHealth(
            completed_switches=switches, exposure_pct=round(exposure, 1),
            expected_exposure_pct=round(expected_exposure_pct, 1),
            invested_avg_daily_pct=round(invested_avg, 4),
            cash_avg_daily_pct=round(cash_avg, 4),
            discrimination_pp=round(discrimination, 4),
            status="insufficient",
            note=(
                f"{switches} completed switches against a floor of {MIN_SWITCHES_TO_JUDGE}. "
                "A timing rule makes a handful of decisions a year; judging it on fewer is "
                "judging noise. Running unchanged."
            ),
        )

    if discrimination <= MIN_DISCRIMINATION_PP:
        return TimingHealth(
            completed_switches=switches, exposure_pct=round(exposure, 1),
            expected_exposure_pct=round(expected_exposure_pct, 1),
            invested_avg_daily_pct=round(invested_avg, 4),
            cash_avg_daily_pct=round(cash_avg, 4),
            discrimination_pp=round(discrimination, 4),
            status="diverging",
            note=(
                f"Over {switches} switches, days the rule was invested returned "
                f"{invested_avg:+.3f}% against {cash_avg:+.3f}% for days it sat out — a gap of "
                f"{discrimination:+.3f}pp. The classifier has stopped separating good tape from "
                "bad, which is its entire claim. Reduce size and rebuild before relying on it."
            ),
        )

    return TimingHealth(
        completed_switches=switches, exposure_pct=round(exposure, 1),
        expected_exposure_pct=round(expected_exposure_pct, 1),
        invested_avg_daily_pct=round(invested_avg, 4),
        cash_avg_daily_pct=round(cash_avg, 4),
        discrimination_pp=round(discrimination, 4),
        status="tracking",
        note=(
            f"Over {switches} switches, invested days returned {invested_avg:+.3f}% against "
            f"{cash_avg:+.3f}% out — a gap of {discrimination:+.3f}pp. The rule is still "
            f"discriminating, at {exposure:.0f}% exposure against {expected_exposure_pct:.0f}% "
            "in the study."
        ),
    )


def build_timing_study(
    sessions: Sequence[date],
    closes: Mapping[date, float],
    regime_by_day: Mapping[date, str],
    split: date,
    fund_median_cagr: float | None = None,
    fund_median_drawdown: float | None = None,
) -> dict:
    """The rule, its held-out result, and how it moves under the assumptions."""
    held_out = [d for d in sessions if d >= split]
    timed = simulate(held_out, closes, regime_by_day)
    passive = buy_and_hold(held_out, closes)
    if timed is None or passive is None:
        return {"available": False}

    # The two assumptions that could be argued with, varied together so the
    # reader can see how much of the result depends on either.
    sensitivity = []
    for cash in (3.0, 4.0, 5.0, 6.0, 7.0):
        for tax in (0.0, STCG_PCT):
            run = simulate(held_out, closes, regime_by_day, cash_rate_pct=cash, tax_pct=tax)
            if run is None:
                continue
            sensitivity.append(
                {
                    "cash_rate_pct": cash,
                    "tax_pct": tax,
                    "cagr_pct": run.cagr_pct,
                    "max_drawdown_pct": run.max_drawdown_pct,
                    "beats_buy_and_hold": run.cagr_pct > passive.cagr_pct,
                    "beats_fund_median": (
                        run.cagr_pct > fund_median_cagr if fund_median_cagr is not None else None
                    ),
                }
            )

    beats_fund = (
        timed.cagr_pct > fund_median_cagr if fund_median_cagr is not None else None
    )
    shallower = (
        timed.max_drawdown_pct > fund_median_drawdown
        if fund_median_drawdown is not None else None
    )

    # The learning discipline pointed at the component that actually earns.
    # Measured over the trailing two years, which is enough sessions to see
    # whether the classifier still separates good tape from bad.
    recent = [d for d in held_out if (held_out[-1] - d).days <= 730]
    health = assess_health(recent, closes, regime_by_day, timed.exposure_pct)

    return {
        "available": True,
        "rule": sorted(INVESTED_REGIMES),
        "health": health.to_dict(),
        "timed": timed.to_dict(),
        "buy_and_hold": passive.to_dict(),
        "fund_median_cagr": fund_median_cagr,
        "fund_median_drawdown": fund_median_drawdown,
        "beats_buy_and_hold": timed.cagr_pct > passive.cagr_pct,
        "beats_fund_median": beats_fund,
        "shallower_than_fund": shallower,
        "sensitivity": sensitivity,
        "method": (
            "Hold the Nifty 500 while the regime is bull_strong, bull_narrow or recovery; park "
            "cash otherwise. The regime set was chosen on 2007-2017 from five candidates "
            "declared in advance, scored on return per unit of drawdown, and the second half "
            "was run once. Cash earns a real rate, switches cost 5 bps, and tax is charged on "
            "every realised gain at the short-term rate where the holding lasted under a year."
        ),
        "caveat": (
            "This is index timing, not stock selection — the selection side of this system has "
            "no edge at all. The drawdown advantage comes from being out of the market for "
            "roughly 40% of the time, including stretches when it is rising, which is the part "
            "a person finds hardest to actually do."
        ),
    }
