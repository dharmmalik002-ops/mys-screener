"""Turn signals into completed trades, one symbol at a time.

Four modelling choices here do most of the work of keeping the results honest,
and each one costs the equity curve something:

*Fill at the next open, not the signal close.* A scan that runs on today's
close can only act tomorrow. Filling at the close of the bar that produced the
signal is the most common way a backtest books a return that was never
available, and it flatters momentum strategies worst of all, because the
signal bar is by construction a strong one.

*Gaps through the stop fill at the open.* If a stock opens 6% below a stop set
2% away, the loss is 6%, not 2%. Assuming stops fill at their level turns every
tail loss into a controlled one, which is precisely backwards: the tail is
where the damage lives. This single detail is the difference between a smooth
equity curve and a truthful one.

*When a bar touches both stop and target, the stop wins.* A daily bar cannot
say which came first. Resolving the ambiguity against the trade biases every
win rate down, which is the safe direction for a number whose job is to decide
whether to risk money.

*Costs on every trade, never netted at the end.* See `costs.py`.

Results are reported in **R** — profit divided by the risk taken at entry. A 3%
gain on a tight stop and a 3% gain on a wide one are not the same trade, and R
is the unit that says so. It is also the unit position sizing is expressed in,
so the backtest and the live sizing speak the same language.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date

import numpy as np

from .costs import CostModel, DEFAULT_COSTS
from .features import Features
from .history import Bars
from .strategies import StrategySpec


@dataclass(frozen=True)
class ExitModel:
    """How a position is managed once it is on.

    No profit target, a trail that engages at 1.5R and runs 4 ATR behind, and a
    90-session ceiling. Chosen on the in-sample period and scored on the
    held-out period unchanged: +0.25R per trade against +0.28R in-sample, with
    the full ranking of five candidate rules *identical* in both periods.

    The 90-session ceiling is load-bearing and was nearly removed. The account
    lags in strong rising markets — 16.6% in a year the index made 26.0% —
    because a time stop sells out of live trends that a fully-invested fund
    rides, so removing it looked like the obvious fix. Trail-only with a 6 ATR
    leash duly scored best on pre-split data and +15.09% on a held-out sweep.
    A full rebuild under that rule returned **5.20%** across book structures
    against the 90-session rule's 11.39%, with -19% and -23% in the last two
    twelve-month periods. A wide trail with no time stop rides a trend
    magnificently (+56 points against the index in 2022-23) and hands it all
    back in a market that stops trending. The ceiling is what caps the
    give-back.

    The sweep was misleading for a specific and now-documented reason: it holds
    the playbook fixed while varying the exit, and the playbook is derived by
    walk-forward validation *on the trades*, which the exit changes. See the
    warning at the top of `scripts/sweep_account_exits.py`.

    Two earlier findings also survive. A profit target scored -0.005R because
    only 11% of trades ever reached 2.5R. Locking in gains after a big move
    lost money too: the round-trip losses are the price of the +5R winners.
    Both remain off.

    Exits are deliberately NOT tuned per strategy: a per-strategy exit fitted on
    the same data used to measure the strategy is how a backtest launders
    overfitting into a headline.
    """

    target_r: float | None = None
    max_hold_sessions: int = 90
    trail_after_r: float | None = 1.5
    trail_atr_mult: float = 4.0
    # Off by default. Intuition says pulling to breakeven at 1R is cheap
    # insurance; the sweep disagreed — every candidate carrying it scored below
    # every candidate without it, because a stop at breakeven gets tagged by
    # ordinary noise and forfeits the few large winners the whole profile
    # depends on. Kept as an option, defaulted off, with the reason recorded.
    breakeven_after_r: float | None = None

    # Profit lock: once the trade has run `lock_trigger_r` in our favour, the
    # stop never again sits below entry + `lock_floor_r`. Distinct from
    # `breakeven_after_r`, which arms at 1R where ordinary noise reaches; this
    # arms only after a move large enough that giving all of it back is a
    # different kind of mistake. Added to test the round-trip leak the trade
    # review surfaced — 21% of trades were over 1R up and finished negative.
    lock_trigger_r: float | None = None
    lock_floor_r: float = 0.0

    # --- partial exit -----------------------------------------------------
    # Sell `scale_out_fraction` of the position the first time the trade is
    # `scale_out_at_r` in front, then move the stop to entry so the remainder
    # cannot lose. This is the ordinary discipline of taking something off a
    # winner, and its main effect is on the WIN RATE rather than expectancy:
    # a trade that banks a third at 1.5R and is then stopped at breakeven
    # finishes slightly positive instead of at zero, which moves it from the
    # loss column to the win column.
    #
    # It is not free. The fraction sold stops compounding, so the few trades
    # that run to 50R or 100R give up that share of their best outcome, and
    # those trades are where the result lives. Both effects are real and the
    # sweep in scripts/sweep_partial_exits.py measures the trade-off rather
    # than assuming it.
    #
    # Breakeven here is safe in a way `breakeven_after_r` is not: that one
    # arms at 1R on the full position, where ordinary noise reaches it and
    # forfeits the whole trade. This arms only after cash is already booked.
    scale_out_at_r: float | None = None
    scale_out_fraction: float = 0.0
    breakeven_after_scale: bool = True

    # --- exit on the thesis breaking, not just on the stop ----------------
    # Every exit above is a price rule: a stop, a trail, a clock. None of them
    # asks whether the reason for owning the stock still holds. `exit_on_break`
    # closes the position when the trend that justified it is gone — the close
    # falls below the moving average the setup was built on, confirmed for
    # `break_confirm_sessions` in a row so a single bad session does not eject
    # a position that is merely breathing.
    #
    # Sold at the next open, like every other decision here: the condition is
    # read on the close, so acting on that same close would be trading on a
    # price that had already printed.
    # Hard ceiling on the initial stop DISTANCE, as a percent of entry. The
    # per-strategy `stop_atr_mult` sets a natural invalidation point, and in a
    # volatile name that can land 10% or more away — a trade whose thesis is
    # only wrong after a 10% fall is not a trade with a 3% risk profile.
    #
    # The trade is NOT rejected when its ATR stop is too wide; the stop is
    # pulled in instead. Rejecting starves the book (a 5% filter passed only
    # 14% of accepted signals), while tightening keeps the entry and changes
    # the risk. It costs win rate — a tighter stop is hit more often — and
    # caps what any single position can lose.
    max_stop_pct: float | None = None

    exit_on_break: bool = False
    break_ma: str = "sma50"           # "sma50" or "ema21"
    break_confirm_sessions: int = 2

    # --- the seasoned-trader set -----------------------------------------
    # Five rules a discretionary trader with two decades behind them would
    # apply by reflex, and which this engine had never modelled. Declared
    # together, before any was measured, and each defaults off so the
    # shipped result is unchanged until one earns its place on the
    # yearly-rebuild test (scripts/rules_walkforward.py) — not on the single
    # split, which has been wrong about this project eight times.
    #
    # ENTRY
    # Do not chase: skip the trade when the next open gaps more than this far
    # above the signal close. Buying a 6% gap puts the stop 6% further from
    # the base and the move is partly spent before the fill.
    max_entry_gap_pct: float | None = None
    # Buy-stop over the signal bar: the order only fills if the next session
    # trades ABOVE the signal day's high, and fills there (or at the open if
    # it gaps through). A breakout that cannot clear its own trigger day is
    # the failure the stop would otherwise have to eat.
    confirm_above_signal_high: bool = False
    # Structural stop: under the signal day's low when that is TIGHTER than
    # the ATR stop. Invalidation is "the breakout day failed", not a
    # volatility multiple.
    stop_at_signal_low: bool = False
    #
    # EXIT
    # Closing-basis trail: once the trail is armed, only a CLOSE below it
    # exits (at the next open). The initial stop stays a hard intraday stop.
    # An intraday wick through a trailing level shakes out positions that
    # close back above it — the standard reason professionals trail on
    # closes and protect capital on the hard stop.
    trail_on_close: bool = False
    # Sell the climax: exit at the next open once the close is this multiple
    # of the 50-day average. O'Neil's "extended" zone; fires only on
    # parabolic blow-offs, which historically give back most of the run.
    climax_sma50_mult: float | None = None
    #
    # RESULTS DAYS (need `RESULTS_CALENDAR`; a symbol without dates is
    # untouched, never guessed at)
    # Sell at the open before a results announcement while the position is
    # still worth less than this many R. The gap a stop cannot catch is
    # mostly a results-day gap; a trade already well in profit can absorb one,
    # a fresh one cannot. float("inf") sells every position.
    results_exit_below_r: float | None = None
    # Skip an entry when a results announcement lands within this many
    # sessions of the fill. Board meetings for results are intimated days in
    # advance (SEBI LODR reg. 29), so a short window is knowable at entry.
    results_entry_blackout: int | None = None


# symbol -> [(announcement date, minutes after midnight IST)]. Filled by the
# runner (scripts/build_results_calendar.py writes the source file); empty
# means the results rules above have nothing to act on.
RESULTS_CALENDAR: dict[str, list[tuple[date, int]]] = {}
MARKET_OPEN_MINUTES = 9 * 60 + 15


def results_exit_sessions(bars: Bars) -> np.ndarray:
    """Boolean per bar: True where the position must be out by the OPEN.

    A filing before the open is priced at that day's open, so the exit is the
    previous session's open; a filing during or after market hours on day D
    is priced on D (intraday) or D+1, and selling at D's open is ahead of both.
    """
    out = np.zeros(len(bars.dates), dtype=bool)
    events = RESULTS_CALENDAR.get(bars.symbol)
    if not events:
        return out
    ords = np.array([d.toordinal() for d in bars.dates])
    for day, minutes in events:
        k = int(np.searchsorted(ords, day.toordinal()))   # first session >= day
        if minutes < MARKET_OPEN_MINUTES or k >= len(ords) or ords[k] != day.toordinal():
            k -= 1                                        # be out by the prior open
        if 0 <= k < len(ords):
            out[k] = True
    return out


@dataclass
class Trade:
    """One completed (or still-open) simulated position."""

    strategy: str
    symbol: str
    signal_day: date
    entry_day: date
    exit_day: date | None
    entry: float
    stop: float
    exit_price: float | None
    exit_reason: str          # stop | target | trail | time | open
    sessions_held: int
    r_multiple: float         # net of costs
    gross_pct: float
    net_pct: float
    mae_r: float              # worst excursion against, in R
    mfe_r: float              # best excursion for, in R
    risk_pct: float           # (entry - stop) / entry * 100
    atr_pct_at_entry: float
    # --- what the stock itself looked like at the signal bar ---------------
    # Recorded because market context alone cannot answer "what kind of setup
    # works": two trades in the same regime, from the same strategy, in a
    # leader and a laggard are different decisions, and without these fields
    # the difference is unrecoverable once the bar has passed.
    ret_63_at_entry: float = 0.0      # the stock's own 3-month momentum, %
    ret_252_at_entry: float = 0.0     # 12-month momentum, %
    dist_52w_high_at_entry: float = 0.0   # % below its own 52-week high
    rel_volume_at_entry: float = 0.0  # volume vs its 50-day average
    turnover_crore_at_entry: float = 0.0
    above_200dma_pct_at_entry: float = 0.0  # % above its own 200 DMA
    regime: str = ""          # stamped by the runner from the regime table
    volatility_band: str = ""

    @property
    def resolved(self) -> bool:
        return self.exit_reason != "open"

    def to_dict(self) -> dict:
        out = asdict(self)
        out["signal_day"] = self.signal_day.isoformat()
        out["entry_day"] = self.entry_day.isoformat()
        out["exit_day"] = self.exit_day.isoformat() if self.exit_day else None
        return out


def _at(series: np.ndarray, index: int) -> float:
    """One indicator's value at the signal bar, or 0.0 when it has no opinion.

    Zero rather than nan because these land in JSON and a nan serialises to a
    literal the JSON parsers in the chain disagree about. Downstream bucketing
    treats 0.0 in these fields as "not measured" — every one of them is a
    percentage or a ratio where exact zero is vanishingly rare in real data.
    """
    if index < 0 or index >= len(series):
        return 0.0
    value = series[index]
    return round(float(value), 2) if np.isfinite(value) else 0.0


def simulate_symbol(
    spec: StrategySpec,
    features: Features,
    signals: np.ndarray,
    exits: ExitModel,
    costs: CostModel = DEFAULT_COSTS,
    position_value: float = 100_000.0,
) -> list[Trade]:
    """Every trade this strategy would have taken in this symbol.

    Overlapping signals are skipped while a position is open: the same setup
    firing three days running is one trade, not three, and counting it three
    times triples the apparent sample from a single decision.
    """
    bars = features.bars
    n = len(bars)
    o, h, l, c = bars.open, bars.high, bars.low, bars.close
    atr = features.atr14

    trades: list[Trade] = []
    blocked_until = -1
    use_results = (exits.results_exit_below_r is not None or exits.results_entry_blackout is not None)
    results_out = results_exit_sessions(bars) if use_results else None

    for i in np.flatnonzero(signals):
        i = int(i)
        if i <= blocked_until or i + 1 >= n:
            continue
        if not np.isfinite(atr[i]) or atr[i] <= 0:
            continue

        entry_idx = i + 1
        if results_out is not None and exits.results_entry_blackout is not None:
            if results_out[entry_idx: entry_idx + exits.results_entry_blackout + 1].any():
                continue                       # results inside the window — wait
        # Slippage scales with the name's liquidity — see costs.py. Measured at
        # the signal bar, which is what was knowable when the order was placed.
        turnover = float(features.turnover_crore[i]) if np.isfinite(features.turnover_crore[i]) else None
        raw_open = float(o[entry_idx])
        if exits.max_entry_gap_pct is not None and float(c[i]) > 0:
            if raw_open > float(c[i]) * (1.0 + exits.max_entry_gap_pct / 100.0):
                continue                       # would be chasing — no trade
        raw_fill = raw_open
        if exits.confirm_above_signal_high:
            trigger = float(h[i])
            if float(h[entry_idx]) <= trigger:
                continue                       # never cleared its trigger day
            raw_fill = max(raw_open, trigger)
        entry = costs.fill_price(raw_fill, "buy", turnover)
        stop = entry - spec.stop_atr_mult * float(atr[i])
        if exits.stop_at_signal_low and float(l[i]) < entry:
            stop = max(stop, float(l[i]) * 0.999)
        if exits.max_stop_pct is not None:
            stop = max(stop, entry * (1.0 - exits.max_stop_pct / 100.0))
        if stop <= 0 or entry <= 0:
            continue
        risk = entry - stop
        if risk <= 0:
            continue

        quantity = max(1.0, position_value / entry)
        target = entry + exits.target_r * risk if exits.target_r else None

        current_stop = stop
        exit_idx: int | None = None
        exit_price: float | None = None
        reason = "open"
        mfe = 0.0
        mae = 0.0
        scale_level = (
            entry + exits.scale_out_at_r * risk
            if exits.scale_out_at_r is not None and exits.scale_out_fraction > 0
            else None
        )
        scaled_qty = 0.0
        scaled_proceeds = 0.0
        break_run = 0
        break_ma = features.sma50 if exits.break_ma == "sma50" else features.ema21
        trail_level: float | None = None

        last_idx = min(entry_idx + exits.max_hold_sessions - 1, n - 1)
        for j in range(entry_idx, last_idx + 1):
            bar_open, bar_high, bar_low = float(o[j]), float(h[j]), float(l[j])

            # Planned sale at the open ahead of results, decided on the prior
            # close (that close's R is what the rule reads).
            if (results_out is not None and exits.results_exit_below_r is not None
                    and j > entry_idx and results_out[j]
                    and (float(c[j - 1]) - entry) / risk < exits.results_exit_below_r):
                exit_idx, exit_price, reason = j, bar_open, "results"
                mae = min(mae, (bar_open - entry) / risk)
                break

            # A gap straight through the stop fills at the open. Checked before
            # anything else, because on that bar nothing else happened first.
            if bar_open <= current_stop:
                exit_idx, exit_price, reason = j, bar_open, "gap_stop"
                mae = min(mae, (bar_open - entry) / risk)
                break

            mfe = max(mfe, (bar_high - entry) / risk)
            mae = min(mae, (bar_low - entry) / risk)

            # Stop before target on the same bar — the ambiguity resolves
            # against the trade. See the module docstring.
            if bar_low <= current_stop:
                exit_idx, exit_price, reason = j, current_stop, "stop"
                break
            # Partial exit. Checked after the stop so the same-bar ambiguity
            # still resolves against the trade, and before the target because
            # scaling out is what the target would otherwise pre-empt.
            if scale_level is not None and scaled_qty == 0.0 and bar_high >= scale_level:
                fill = max(scale_level, bar_open) if bar_open > scale_level else scale_level
                scaled_qty = quantity * exits.scale_out_fraction
                scaled_proceeds = costs.fill_price(float(fill), "sell", turnover) * scaled_qty
                if exits.breakeven_after_scale:
                    current_stop = max(current_stop, entry)

            if target is not None and bar_high >= target:
                # A gap above the target fills at the open, in our favour; that
                # is symmetric with the gap-down case and equally real.
                exit_price = max(target, bar_open) if bar_open > target else target
                exit_idx, reason = j, "target"
                break

            # Thesis check, on the close. Arms an exit for the NEXT open.
            if exits.exit_on_break:
                ma = float(break_ma[j]) if j < len(break_ma) else float("nan")
                if np.isfinite(ma) and float(c[j]) < ma:
                    break_run += 1
                else:
                    break_run = 0
                if break_run >= exits.break_confirm_sessions and j + 1 <= last_idx:
                    exit_idx, exit_price = j + 1, float(o[j + 1])
                    reason = "rule_break"
                    break

            # Climax: a close far above the 50-day average sells at the next open.
            if exits.climax_sma50_mult is not None and j + 1 <= last_idx:
                sma = float(features.sma50[j]) if j < len(features.sma50) else float("nan")
                if np.isfinite(sma) and sma > 0 and float(c[j]) >= exits.climax_sma50_mult * sma:
                    exit_idx, exit_price, reason = j + 1, float(o[j + 1]), "climax"
                    break

            # Closing-basis trail: a close under the trail exits at the next open.
            if exits.trail_on_close and trail_level is not None and float(c[j]) < trail_level:
                if j + 1 <= last_idx:
                    exit_idx, exit_price, reason = j + 1, float(o[j + 1]), "trail"
                    break

            # Stop management, applied on the *close* of the bar so it can only
            # affect subsequent bars — moving a stop using the same bar's high
            # would be acting on information the day had not finished giving.
            run_r = (float(c[j]) - entry) / risk
            if exits.breakeven_after_r is not None and run_r >= exits.breakeven_after_r:
                current_stop = max(current_stop, entry)
            # Armed off the running high rather than the close: the point is to
            # protect a move that actually happened. The stop it sets still
            # only affects later bars, so this stays causal.
            if exits.lock_trigger_r is not None and mfe >= exits.lock_trigger_r:
                current_stop = max(current_stop, entry + exits.lock_floor_r * risk)
            if exits.trail_after_r is not None and run_r >= exits.trail_after_r:
                if np.isfinite(atr[j]):
                    level = float(c[j]) - exits.trail_atr_mult * float(atr[j])
                    if exits.trail_on_close:
                        # Tracked separately; the intraday stop stays the hard
                        # initial stop so only a CLOSE can trip the trail.
                        trail_level = level if trail_level is None else max(trail_level, level)
                    else:
                        current_stop = max(current_stop, level)

        if exit_idx is None:
            # Ran out of horizon (time stop) or out of data (still open).
            if last_idx >= entry_idx and last_idx - entry_idx + 1 >= exits.max_hold_sessions:
                exit_idx, exit_price, reason = last_idx, float(c[last_idx]), "time"
            elif last_idx == n - 1:
                exit_idx, exit_price, reason = last_idx, float(c[last_idx]), "open"

        if exit_idx is None or exit_price is None:
            continue

        realised = costs.fill_price(float(exit_price), "sell", turnover)
        remaining_qty = quantity - scaled_qty
        buy_value = entry * quantity
        # Both legs are charged. A scale-out is a second real sale with its own
        # STT, stamp duty and brokerage, so a partial exit costs more in fees
        # than holding one position to the end — that is part of the trade-off
        # and must not be netted away.
        sell_value = realised * remaining_qty + scaled_proceeds
        charges = costs.charges(buy_value, sell_value)
        net_pnl = sell_value - buy_value - charges
        if scaled_qty > 0 and reason in ("stop", "gap_stop"):
            reason = "stop_after_partial"

        trades.append(
            Trade(
                strategy=spec.id,
                symbol=bars.symbol,
                signal_day=bars.dates[i],
                entry_day=bars.dates[entry_idx],
                exit_day=bars.dates[exit_idx],
                entry=round(entry, 2),
                stop=round(stop, 2),
                exit_price=round(realised, 2),
                exit_reason=reason,
                sessions_held=exit_idx - entry_idx + 1,
                r_multiple=round(net_pnl / (risk * quantity), 3),
                gross_pct=round((realised - entry) / entry * 100.0, 2),
                net_pct=round(net_pnl / buy_value * 100.0, 2),
                mae_r=round(mae, 2),
                mfe_r=round(mfe, 2),
                risk_pct=round(risk / entry * 100.0, 2),
                atr_pct_at_entry=_at(features.atr_pct, i),
                ret_63_at_entry=_at(features.ret_63, i),
                ret_252_at_entry=_at(features.ret_252, i),
                dist_52w_high_at_entry=_at(features.dist_52w_high, i),
                rel_volume_at_entry=_at(features.rel_volume, i),
                turnover_crore_at_entry=_at(features.turnover_crore, i),
                above_200dma_pct_at_entry=_at(
                    np.where(
                        np.isfinite(features.sma200) & (features.sma200 > 0),
                        (c - features.sma200) / np.where(features.sma200 > 0, features.sma200, np.nan) * 100.0,
                        np.nan,
                    ),
                    i,
                ),
            )
        )
        blocked_until = exit_idx

    return trades
