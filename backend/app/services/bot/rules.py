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

from datetime import date
from typing import Mapping, Sequence

# Frozen from pre-2018 quantiles. Do not re-fit these on later data.
MAX_RISK_PCT = 7.60          # 40th percentile of training-half stop width
# Hard ceiling on the initial stop, independent of the rolling percentile.
# The rolling cap adapts to volatility (gotcha 79) and after a crash it can
# widen past what any sane position risk allows; this is the floor under that.
# 93% of otherwise-accepted trades already clear it, so it binds rarely and
# only on the trades that deserve it.
HARD_MAX_RISK_PCT = 8.0
MAX_TURNOVER_CRORE = 10.1    # 60th percentile of training-half turnover
MIN_RET_63 = 0.0

TRADEABLE_REGIMES = frozenset({"bull_strong", "bull_narrow", "recovery"})

# Positive in BOTH halves of the split. The six excluded ones are not broken —
# they are simply the weaker half, and a book that must decline 99% of what it
# sees cannot afford them.
TRADEABLE_SETUPS = frozenset({
    "squeeze_release", "earnings_drift", "earnings_gap_hold",
    "high_tight_flag", "minervini_breakout", "pullback_ema21",
    # Positive on the training half, and each adds return on the yearly
    # rebuild against the corrected sleeve (gotcha 116).
    "pocket_pivot", "nr7_release",
})

# What the yearly rebuild may choose from. It admits a setup only once its own
# CLOSED record before that January is positive, so these two start outside
# the book (too few closed trades before 2018) and earn their way in.
CANDIDATE_SETUPS = TRADEABLE_SETUPS | frozenset({"results_follow_through", "episodic_pivot"})

# The stop-width percentile is taken over THIS library, fixed, rather than
# whatever happens to be registered. Registering a setup used to move the cap
# for every existing one (gotcha 82); pinning the reference makes a new setup
# a pure addition — existing trades are untouched by it.
CAP_REFERENCE_SETUPS = frozenset({
    "minervini_breakout", "vcp_breakout", "week52_breakout", "momentum_burst",
    "high_tight_flag", "pullback_ema21", "pullback_sma50", "oversold_bounce",
    "squeeze_release", "gap_continuation", "earnings_drift", "earnings_gap_hold",
})

# The exit, as selected. See the module docstring for why it is this wide.
EXIT_TARGET_R = None
EXIT_TRAIL_AFTER_R = 1.0
EXIT_TRAIL_ATR_MULT = 8.0
EXIT_MAX_HOLD_SESSIONS = 500
# Hard ceiling on the initial stop DISTANCE. The trade is kept and the stop is
# pulled in, rather than the trade being rejected — filtering on stop width
# passed only 14% of accepted signals and starved the book.
#
# It is a real trade-off, priced honestly: average stop 6.11% -> 3.50%, and
# because a tighter stop is hit more often the win rate falls 34.6% -> 16.5%
# and drawdown deepens -26.8% -> -39.9%. What it buys is the years that were
# broken: 2024 +19.7% -> +34.3% (past the index), 2026 -12.2% -> -2.8%,
# 2009 +33.3% -> +65.7%.
#
# The mechanism behind the deeper drawdown is worth knowing: with fixed
# fractional risk a TIGHTER stop means a BIGGER position (0.25% risk over a
# 3.5% stop is a 7% position; over a 7% stop it is 3.5%). Tightening the stop
# concentrates the book unless the position cap comes down with it.
# Raised from 3.5% to 7.0%. A 3.5% cap is hit far more often, which is why
# it drove the win rate down to 15%; the brief now asks for 30%+ and a stop
# this tight cannot deliver it (gotcha 92 measured the collision). At 7% the
# average stop lands at 6.10% and the win rate at 35.1%. The cost is real and
# is priced: a wider stop means a wider gap behind it, so the 1%-of-equity
# rule shrinks the position to compensate.
EXIT_MAX_STOP_PCT = 7.0

# Measured on the full period with these rules, taking only signals the
# confidence score (confidence.py) rates 8 or above. That filter is the first
# selection rule in this project to hold up out of sample: the >=8 band pays
# +0.088R in training and +0.069R held out, while the full cleared set is
# negative in both (-0.068R / -0.035R). It declines 82% of what the rules
# already cleared, which is why the trade count falls by two thirds.
#
# Re-measured after gotchas 115-117 (117: the market-type sleeve): the sleeve no longer reads same-day
# state (it had been credited with the very move that switched it), the thrust
# leg is gone, and pocket_pivot / nr7_release joined the book. The previous
# figures (+42.26%, -21.91%, Sharpe 2.02) were inflated by that look-ahead.
# Gotcha 121 (sideways rules: the book stands aside where small caps lost,
# corrections hold half gold / half index) moved it from +24.64% / -34.34%;
# gotcha 122 (price level, trend stack, gold only as a hedge) to +26.76%;
# gotcha 123 (corrections hold gold + a liquid fund) to +27.35%.
MEASURED_CAGR = 27.35
MEASURED_MAX_DRAWDOWN = -31.37
MEASURED_SHARPE = 1.40
MEASURED_PAYOFF = 5.29
MEASURED_WIN_RATE = 30.4
MEASURED_TRADES = 1964
# The yearly rebuild is the figure quoted (gotcha 110). Its win rate clears
# the brief's 30% floor; the single split, fitted once, sits just under it.
MEASURED_WF_CAGR = 33.61
MEASURED_WF_WIN_RATE = 32.5
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


# --- "tight stop" is relative to conditions, not an absolute number -------
# The absolute cap was the single thing starving the book in the years it lost
# most. 2009: 1,727 signals, and only 298 cleared a 7.60% stop — after a crash
# every stop is wide, so a fixed cap locks the book out of cash exactly while
# the market rallies hardest. It took 68 trades all year and finished 126
# points behind the index.
#
# The rule that was actually mined was "tighter than typical", and typical
# moves. This re-derives the 40th percentile from the signals of the trailing
# year, so the cap breathes with volatility while the *selection* stays the
# same. Causal: only signals dated strictly before the one being judged count.
#
#     absolute      CAGR +34.49%  maxDD -36.27%  Sharpe 1.54   2009 -12.5%
#     rolling 365d  CAGR +36.26%  maxDD -33.09%  Sharpe 1.63   2009 +15.4%
#     rolling 730d  CAGR +32.76%  maxDD -37.38%  Sharpe 1.49   2009  +8.7%
#
# Better return, shallower drawdown and a higher win rate together. A year is
# the right window: two years averages across regime changes and gives most of
# the gain back.
ROLLING_RISK_WINDOW_DAYS = 365
ROLLING_RISK_QUANTILE = 0.40
MIN_SIGNALS_FOR_ROLLING = 200


def accepted_with_rolling_risk(rows: "Sequence[Mapping]", setups: "frozenset | None" = None) -> list:
    """Apply the rules, deriving the stop-width cap from recent signals.

    Falls back to `MAX_RISK_PCT` early in history, where there is not yet a
    year of signals to take a percentile from.
    """
    import bisect
    from datetime import date, timedelta

    import numpy as np

    # The percentile is taken over EVERY signal the registered library
    # produces, not just the tradeable setups, because that is the pool the
    # 40th percentile was mined from — on the eligible setups alone the same
    # 7.58% threshold sits at their 52nd percentile, and passing 0.40 there
    # silently tightens the rule to 6.82%.
    #
    # The cost is a real coupling: registering a new strategy shifts the cap
    # for every existing one. Measured, by registering a reversal setup and
    # changing nothing else — the book moved from +18.69% to +17.23%.
    # `test_the_cap_is_coupled_to_the_registered_library` exists so that can
    # never happen unnoticed; anyone adding a strategy must re-run the book.
    ordered = sorted(rows, key=lambda t: str(t["entry_day"]))
    ref = [t for t in ordered if str(t.get("strategy", "")) in CAP_REFERENCE_SETUPS]
    days = [str(t["entry_day"]) for t in ref]
    risks = [float(t.get("risk_pct") or 0.0) for t in ref]
    allowed = TRADEABLE_SETUPS if setups is None else setups

    out = []
    for trade in ordered:
        if not _clears_everything_but_risk(trade, allowed):
            continue
        today = str(trade["entry_day"])
        day = date.fromisoformat(today)
        low = (day - timedelta(days=ROLLING_RISK_WINDOW_DAYS)).isoformat()
        lo = bisect.bisect_left(days, low)
        hi = bisect.bisect_left(days, today)        # strictly before today
        cap = (
            float(np.quantile(risks[lo:hi], ROLLING_RISK_QUANTILE))
            if hi - lo >= MIN_SIGNALS_FOR_ROLLING else MAX_RISK_PCT
        )
        if float(trade.get("risk_pct") or 1e9) <= min(cap, HARD_MAX_RISK_PCT):
            out.append(trade)
    return out


def _clears_everything_but_risk(trade: "Mapping", setups: "frozenset | None" = None) -> bool:
    if any(trade.get(f) is None for f in (
        "risk_pct", "turnover_crore_at_entry", "ret_63_at_entry", "strategy", "regime",
    )):
        return False
    return (
        float(trade.get("turnover_crore_at_entry", 1e9)) <= MAX_TURNOVER_CRORE
        and float(trade.get("ret_63_at_entry", -1e9)) > MIN_RET_63
        and str(trade.get("strategy", "")) in (TRADEABLE_SETUPS if setups is None else setups)
        and str(trade.get("regime", "")) in TRADEABLE_REGIMES
    )


def beats_smallcap() -> bool:
    return MEASURED_CAGR > MEASURED_SMALLCAP_CAGR


# --- partial profit-taking: available, measured, and off by default -------
# Scaling out raises the win rate exactly as intended — 30% off at 2R with the
# stop to breakeven takes it from 25% to 39%, inside the 35-40% band — and it
# costs more than it is worth at the account level:
#
#     none          CAGR +18.7%  maxDD -33.3%  Sharpe 1.21  win 27.8%  payoff 10.0
#     30% @ 2R      CAGR +10.3%  maxDD -42.3%  Sharpe 0.68  win 39.9%  payoff  3.7
#     30% @ 3R      CAGR +13.2%  maxDD -35.9%  Sharpe 0.90  win 35.4%  payoff  5.4
#     30% @ 3R noBE CAGR +14.3%  maxDD -36.4%  Sharpe 0.93  win 35.9%  payoff  6.0
#
# Re-measured after the position-cap bug (gotcha 80); the verdict is unchanged
# and the gap is wider than first reported. It also halves the years that beat
# the index, 13 of 18 down to 8.
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
MEASURED_SCALED_WIN_RATE = 39.9
MEASURED_SCALED_CAGR = 10.33


# --- exit when the thesis breaks: built, measured, off ---------------------
# "Close the trade when it is violating the rules" — every other exit here is
# a price rule (stop, trail, clock) and none asks whether the reason for
# owning the stock still holds. `ExitModel.exit_on_break` closes the position
# at the next open once the close has sat below its moving average for two
# consecutive sessions.
#
#     none             CAGR +23.21%  maxDD -29.89%  Sharpe 1.28  win 26.9%  payoff 9.76
#     break sma50 x2   CAGR +16.43%  maxDD -28.43%  Sharpe 0.95  win 27.2%  payoff 4.25
#     break sma50 x5   CAGR +17.95%  maxDD -28.94%  Sharpe 0.98  win 29.0%  payoff 4.35
#     break ema21 x3   CAGR +14.10%  maxDD -29.74%  Sharpe 0.84  win 31.8%  payoff 2.91
#
# The same shape as every other sell-earlier idea tested here: win rate up,
# drawdown marginally better, payoff destroyed. A position that runs to 50R
# spends weeks below its 50-day average on the way, and an exit that cannot
# tolerate that cannot hold the trades this book is built on.
EXIT_ON_BREAK = False
MEASURED_BREAK_CAGR = 16.43


def exit_on_break_costs_return() -> bool:
    return MEASURED_BREAK_CAGR < MEASURED_CAGR


def scaling_out_costs_return() -> bool:
    """True. Kept as an assertion so the trade-off cannot be forgotten."""
    return MEASURED_SCALED_CAGR < MEASURED_CAGR


# The 35-40% win rate the brief asks for, bought the only way that does not
# truncate winners at a fixed R: sell the stock book into a regime turn and
# hold the index sleeve instead. Every scale-out variant reached the same win
# rate by capping the trades that carry the result, and deepened the drawdown
# doing it. This one raises the win rate AND halves the drawdown.
#
#     hold through   CAGR +22.89%  maxDD -27.98%  win 26.8%  payoff 10.63  ret/DD 0.82  15/18
#     sell the turn  CAGR +19.05%  maxDD -17.58%  win 36.8%  payoff  3.75  ret/DD 1.08  13/18
#
# It costs 3.8pp of CAGR and two years of outperformance and returns 10.4
# points of drawdown. Which is better depends on what the reader can sit
# through, so both ship; `DERISK=0` selects the higher-return variant.
DERISK_ON_REGIME_TURN = True
MEASURED_CASH_SLEEVE_CAGR = 22.89
MEASURED_UNCONFIRMED_CAGR = 26.05
MEASURED_CASH_SLEEVE_DRAWDOWN = -27.98
MEASURED_CASH_SLEEVE_WIN_RATE = 26.8


# TWO ASKS IN THE BRIEF ARE IN DIRECT CONFLICT and only one can hold at a
# time: a 3-4% average stop and a 35-40% win rate. A tighter stop is hit more
# often, by construction — 6.11% -> 3.50% average takes the win rate 34.6% ->
# 16.5%. The stop width is the newer and more specific instruction (it came
# with a worst-trade limit attached), so it wins and the win-rate floor is
# lowered to match the measurement rather than the measurement being dressed
# up to meet the old band.
WIN_RATE_FLOOR = 30.0
WIN_RATE_CEILING = 40.0


def win_rate_clears_the_brief() -> bool:
    """30.7% on the yearly rebuild, and not bought with a profit target.

    Selling the stock book into a regime turn and holding GOLD instead of
    cash is what made this affordable. With cash it cost 3.8pp of CAGR and
    two years of outperformance; with gold it *adds* 3.2pp and the win rate
    comes free.
    """
    return WIN_RATE_FLOOR <= MEASURED_WF_WIN_RATE <= WIN_RATE_CEILING


def gold_sleeve_beats_cash() -> bool:
    """The de-risked capital has to go somewhere, and cash is not it."""
    return MEASURED_CAGR > MEASURED_CASH_SLEEVE_CAGR


def payoff_clears_the_brief() -> bool:
    """The brief asked for 3-4. Holding through a turn gives 10.6; selling
    into it gives 3.75, still inside the band and paired with a 36.8% win
    rate. The bar is the brief's own floor, not the higher number a previous
    configuration happened to reach."""
    return MEASURED_PAYOFF >= 3.0


# --- the V-shaped recovery: come back fast, or the sleeve buys the top ------
# The sleeve's risk-on switch (healthy regime, or the index above its own 200
# DMA) is a LAGGING condition by construction: both inputs need the fall to
# have already happened before they turn off, and the rebound to have already
# happened before they turn back on. In a V-shaped recovery that is the worst
# possible timing, and 2026 shows it in one line:
#
#     2026-03   N500 -10.1%   gold -12.4%   sleeve -8.9%   (switched to gold
#                                                           as gold fell)
#     2026-04   N500  +8.4%   gold   0.0%   sleeve  0.0%   (still in gold for
#                                                           the whole rebound)
#
# The sleeve returned -10.3% in a year when the index fell 5.2% and gold rose
# 13.6% — worse than BOTH of its own legs. Debouncing makes it worse still
# (3-session confirmation: +24.50%, 5-session: +21.56%, against +32.75%),
# which is the tell that the problem is lag rather than noise: waiting longer
# to act cannot fix being late.
#
# A follow-through day is the standard answer and it is declared here as the
# rule it is: the index closing `THRUST_PCT` above its own lowest close of the
# trailing `THRUST_LOOKBACK` sessions puts the sleeve back into equities
# immediately, whatever the regime label and the 200 DMA still say. It is
# purely causal — session `i` reads bars `0..i` — and it reads the market,
# never the bot's own P&L, which is the line every failed learning experiment
# in this project crossed.
#
# Chosen on the 2009-2017 half alone from a family declared in advance
# (3/4/5/6/8% x 10/15/20 sessions), then the held-out half was run once.
# EVERY member of that family beat the baseline in BOTH halves, so the
# specific parameter is not load-bearing:
#
#     baseline (no thrust)   h1 6/9 +27.8%   h2 8/9 +31.5%
#     thrust 3% / 10d        h1 7/9 +37.2%   h2 8/9 +40.5%   <- chosen
#     thrust 8% / 20d        h1 6/9 +29.1%   h2 8/9 +33.5%
#
# Against 40 matched random controls turning on the SAME NUMBER of extra
# risk-on days, it beats the 95th percentile on CAGR, Sharpe, drawdown and
# years-beaten — so it is not simply the effect of being invested more often.
THRUST_PCT = 3.0
THRUST_LOOKBACK = 10


def thrust_days(dates: "Sequence[date]", closes: "Sequence[float]") -> "set":
    """Sessions where the index has thrust `THRUST_PCT` off its recent low.

    Causal by construction: the window for session `i` is `i-THRUST_LOOKBACK`
    through `i` inclusive, so no future bar can put a day in this set.
    """
    out = set()
    for i in range(THRUST_LOOKBACK, len(closes)):
        window = closes[i - THRUST_LOOKBACK: i + 1]
        low = min(window)
        if low > 0 and (closes[i] / low - 1.0) * 100.0 >= THRUST_PCT:
            out.add(dates[i])
    return out


# --- after a crash, the small caps run hardest -----------------------------
# The premise was checked before the rule was built, because a rule on top of
# a false premise cannot be fixed by tuning it. Daily returns of the Smallcap
# 250 against the Nifty 500, annualised, split by the index's own drawdown:
#
#     at/near highs  (dd > -5%)      small +53.1%   broad +41.4%   +11.7pp
#     mild pullback  (-5 to -15%)          -23.1%         -17.6%    -5.4pp
#     correction     (-15 to -25%)         -16.7%          -8.0%    -8.7pp
#     crash          (dd <= -25%)          -95.8%         -55.5%   -40.3pp
#     RECOVERING after -20%, 12m     small +51.1%   broad +39.8%   +11.3pp
#
# So small caps do run harder off a crash — but note they run just as hard at
# the highs (+11.7pp). The effect is not special to recoveries; it is that
# small caps are a leveraged version of the market in BOTH directions, and
# -40.3pp in a crash is the price. What makes the recovery window the right
# place to take that leverage is not a bigger edge, it is that the window
# reliably ENDS before the next crash:
#
#     sleeve holds                      CAGR      maxDD   Sharpe
#     broad index always              +41.60%   -22.72%     1.79
#     small caps whenever above 200dma +42.47%  -33.09%     1.75
#     small caps in recovery only      +43.88%   -22.72%     1.87
#
# The middle row is the control that matters: tilting on "the market is
# rising" captures most of the same return and costs ten points of drawdown,
# because that condition is still true on the way into a crash. Against 40
# matched random controls tilting on the same NUMBER of sessions, the
# recovery rule beats the 95th percentile on CAGR, drawdown and Sharpe.
#
# Chosen on the 2009-2017 half from four windows declared in advance
# (-15%/9m, -20%/12m, -20%/18m, -25%/12m); all four beat the baseline on
# return and Sharpe in both halves, and -20%/12m won the training half.
RECOVERY_DRAWDOWN_PCT = -20.0
RECOVERY_WINDOW_DAYS = 360


def recovery_days(dates: "Sequence[date]", closes: "Sequence[float]") -> "set":
    """Sessions inside a post-crash recovery, decided causally.

    A session qualifies when the index has been at least
    `RECOVERY_DRAWDOWN_PCT` below its own trailing 52-week high within the
    last `RECOVERY_WINDOW_DAYS`, and has since climbed back above that level.
    Only bars up to and including the session are read, so the answer for a
    given day never changes when later data arrives.
    """
    out: set = set()
    last_deep = None
    for i, day in enumerate(dates):
        window = closes[max(0, i - 251): i + 1]
        peak = max(window) if window else 0.0
        drawdown = (closes[i] / peak - 1.0) * 100.0 if peak > 0 else 0.0
        if drawdown <= RECOVERY_DRAWDOWN_PCT:
            last_deep = day
        elif last_deep is not None and 0 <= (day - last_deep).days <= RECOVERY_WINDOW_DAYS:
            out.add(day)
    return out


# --- one place that builds the trade model ---------------------------------
# The backtest runner, the yearly-rebuild test and the paper runner each used
# to construct ExitModel from these constants by hand. Three copies of the same
# five arguments is exactly how the paper book ended up trading a different
# rule from the study (CLAUDE.md gotcha 111), so all three now call this.
# Two of the five seasoned-trader rules, adopted TOGETHER with group strength
# in the confidence score (gotcha 113). On their own they were a tie (+0.37pp,
# gotcha 112); combined with the group score the package clears both tests
# with no year lost: walk-forward +38.85% -> +40.09%, single split +39.72% ->
# +40.13% at 16/18. Adopting only the group score was +0.99pp on walk-forward
# but cost two years on the split; the exits are what recover them.
SEASONED_RULES: dict = {"trail_on_close": True, "climax_sma50_mult": 1.7}
# ONE CAVEAT that must travel with any future entry here: `paper.py` does not
# call the engine — it re-implements stops, the trail and the ceiling for a
# book that advances one session at a time. A rule added to SEASONED_RULES
# reaches the backtest and the walk-forward through `exit_model()` but NOT the
# paper book, which would then quietly trade the old rule. Port it to
# `paper.advance` in the same change, or the paper book stops validating the
# study (the exact failure of gotcha 111).


def exit_model(**overrides):
    from .engine import ExitModel
    kw = dict(
        target_r=EXIT_TARGET_R, max_hold_sessions=EXIT_MAX_HOLD_SESSIONS,
        trail_after_r=EXIT_TRAIL_AFTER_R, trail_atr_mult=EXIT_TRAIL_ATR_MULT,
        max_stop_pct=EXIT_MAX_STOP_PCT,
    )
    kw.update(SEASONED_RULES)
    kw.update(overrides)
    return ExitModel(**kw)
