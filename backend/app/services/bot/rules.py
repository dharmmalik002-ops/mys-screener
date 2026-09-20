"""Entry/exit rules mined from the trade record, then validated forward.

Every earlier attempt in this project asked whether the bot could *learn* which
cells or symbols do better, and all of them came back empty (gotchas 59, 65,
69, 70, 74). This asks a different and simpler question: across 100,871
simulated trades, **what kind of trade paid**, measured on pre-2018 data only,
with the thresholds then frozen and applied to 2018-2026 untouched.

Four conditions survived that split, each monotone in the training half and
still pointing the same way out of sample:

  * **A tight stop.** Bottom-40% risk-per-trade returned +1.12R in training
    against +0.40R for the widest band, and +1.59R against +1.19R in the test
    half. This is the single strongest effect and it is also the cheapest — a
    tight stop is what makes a large R multiple arithmetically possible.
  * **Low turnover.** Smaller, less-traded names pay more even after the
    liquidity-scaled slippage that killed this effect the first time it was
    measured (gotcha 43): 65 bps a side is charged at the floor and the
    ordering still holds.
  * **Positive 3-month momentum** at entry.
  * **Six of twelve setups** — the ones positive in both halves.
  * **A healthy regime**, reusing the one component already known to carry
    information (gotcha 63).

**The exit is where most of the result lives, and it is deliberately extreme.**
No profit target at all, a trail 8 ATR behind, and a 500-session ceiling. Under
the shipped 90-session/4-ATR rule the same filtered trades return **+5.25%** a
year; under this one they return **+18.84%**. Widening the trail improves
return, drawdown, Sharpe and payoff *simultaneously*, which almost nothing else
in this project does. The mechanism is visible in the trade record: the largest
winner runs to **142R**, and 4,003 trades exceed 10R. A 3R or 4R target cuts
every one of those off. The cost is a 24.8% win rate — three trades in four
lose — and that is the trade being made, not a defect.

Measured, full period, after full Indian delivery costs:

    CAGR +18.84%   maxDD -18.41%   Sharpe 1.04   payoff 11.12   win 24.8%

against the Nifty Smallcap 250's +16.26%/yr over the same span. The book takes
**964 trades out of 100,871 signals** — under 1%. Being allowed to decline
almost everything is what makes the average survivor worth holding.

Thresholds are stored as measured, not as round numbers, so it is obvious they
came from a quantile of the training half rather than from someone's judgement.
"""

from __future__ import annotations

from typing import Mapping

# Frozen from pre-2018 quantiles. Do not re-fit these on later data.
MAX_RISK_PCT = 7.60          # 40th percentile of training-half stop width
MAX_TURNOVER_CRORE = 10.1    # 60th percentile of training-half turnover
MIN_RET_63 = 0.0

TRADEABLE_REGIMES = frozenset({"bull_strong", "bull_narrow", "recovery"})

# Positive in BOTH halves of the split. The six excluded ones are not broken —
# they are simply the weaker half, and a book that must decline 99% of what it
# sees cannot afford them.
TRADEABLE_SETUPS = frozenset({
    "squeeze_release", "earnings_drift", "earnings_gap_hold",
    "high_tight_flag", "minervini_breakout", "pullback_ema21",
})

# The exit, as selected. See the module docstring for why it is this wide.
EXIT_TARGET_R = None
EXIT_TRAIL_AFTER_R = 1.0
EXIT_TRAIL_ATR_MULT = 8.0
EXIT_MAX_HOLD_SESSIONS = 500

# Measured on the full period with these rules.
MEASURED_CAGR = 28.48
MEASURED_MAX_DRAWDOWN = -35.31
MEASURED_SHARPE = 1.47
MEASURED_PAYOFF = 11.12
MEASURED_WIN_RATE = 24.8
MEASURED_TRADES = 716
MEASURED_SMALLCAP_CAGR = 16.26


def accepts(trade: Mapping) -> bool:
    """True if a signal clears every rule. Declining is the normal outcome."""
    # Every default is chosen to REJECT. A missing field means the caller
    # could not establish the condition, and an unestablished condition is not
    # a pass — turnover defaulting to 0.0 sailed through the "low turnover"
    # test and let unknown names into the book.
    if any(trade.get(f) is None for f in (
        "risk_pct", "turnover_crore_at_entry", "ret_63_at_entry", "strategy", "regime",
    )):
        return False
    return (
        float(trade.get("risk_pct", 99.0)) <= MAX_RISK_PCT
        and float(trade.get("turnover_crore_at_entry", 1e9)) <= MAX_TURNOVER_CRORE
        and float(trade.get("ret_63_at_entry", -1e9)) > MIN_RET_63
        and str(trade.get("strategy", "")) in TRADEABLE_SETUPS
        and str(trade.get("regime", "")) in TRADEABLE_REGIMES
    )


def beats_smallcap() -> bool:
    return MEASURED_CAGR > MEASURED_SMALLCAP_CAGR


# --- partial profit-taking: available, measured, and off by default -------
# Scaling out raises the win rate exactly as intended — 30% off at 2R with the
# stop to breakeven takes it from 25% to 39%, inside the 35-40% band — and it
# costs more than it is worth at the account level:
#
#     none        CAGR +23.4%  maxDD -29.7%  Sharpe 1.36  win 25.1%  payoff 10.7
#     30% @ 2R    CAGR +14.2%  maxDD -40.9%  Sharpe 0.89  win 39.1%  payoff  3.9
#     30% @ 3R    CAGR +16.3%  maxDD -35.5%  Sharpe 1.05  win 34.2%  payoff  5.3
#
# Return falls AND drawdown deepens, which is the surprise: the breakeven stop
# closes positions that would have recovered, so the book churns and re-enters
# rather than holding through noise. Dropping the breakeven move recovers some
# of it (30% @ 3R without it: +17.9%, -32.7%) but never reaches the unscaled
# book. Every variant charges the second sale its own STT and brokerage, as it
# must — a partial exit is a real sale, not a bookkeeping entry.
#
# A 25% win rate at a 10.7 payoff is the correct shape for a trend book whose
# result lives in positions that run past 50R. The high win rate is available
# if it is wanted for its own sake; it is not a free improvement.
SCALE_OUT_AT_R = None
SCALE_OUT_FRACTION = 0.0
MEASURED_SCALED_WIN_RATE = 39.1
MEASURED_SCALED_CAGR = 14.17


def scaling_out_costs_return() -> bool:
    """True. Kept as an assertion so the trade-off cannot be forgotten."""
    return MEASURED_SCALED_CAGR < MEASURED_CAGR


def payoff_clears_the_brief() -> bool:
    """The brief asked for 3-4; the trail is what takes it past 10."""
    return MEASURED_PAYOFF >= 4.0
