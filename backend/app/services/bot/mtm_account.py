"""An account that marks open positions to market every session.

`portfolio.simulate` books a trade's entire profit on its **exit** day. With a
90-session ceiling that is a small distortion; with the 500-session trail that
`rules.py` needs it is a fatal one. A position opened in March 2023 and closed
in late 2024 puts every rupee it made into 2024, so a year-by-year table built
from that curve reports the wrong year — 2023 reads flat while 2024 reads
enormous, and neither number describes what the account was actually worth at
either year end.

That is not a reporting nicety. It was concealing the answer to "which years
does this struggle in": several apparently terrible years were simply years
whose gains had not been booked yet, and several spectacular ones were the
previous year's profit arriving late.

This module reprices every open position from its symbol's own close each
session, so equity is what the account was genuinely worth that day. Yearly
returns, drawdown and Sharpe all become real. Drawdown in particular gets
**worse** and should — a realised-only curve cannot see a position giving back
40% of its gain, because it never sees the gain until it is over.

Everything else matches `PortfolioConfig`: finite capital, a slot cap, risk-
based sizing, no margin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable, Mapping, Sequence

import numpy as np

from .history import read_bars
from .portfolio import PortfolioConfig


@dataclass
class MTMResult:
    label: str
    start: str
    end: str
    years: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe: float
    trades_taken: int
    signals_declined: int
    win_rate: float
    avg_r: float
    payoff: float
    exposure_pct: float
    worst_trade_equity_pct: float = 0.0   # biggest single-trade hit to equity
    equity_curve: list[dict] = field(default_factory=list)
    yearly: dict[int, float] = field(default_factory=dict)
    # Every position the account actually FILLED. The signal list is not the
    # same thing: the book declines most of what clears its rules, for want of
    # a slot or of cash, so trade-shape statistics computed over signals
    # describe a book that was never run. Reported per fill so a breakdown by
    # strategy or by year can be built from what happened rather than from
    # what was offered.
    fills: list[dict] = field(default_factory=list)


def composite_sleeve(
    risk_asset: Mapping[date, float],
    safe_asset: Mapping[date, float],
    risk_on_days: "set[date]",
) -> dict[date, float]:
    """One price series that tracks the risk asset on risk-on days and the
    safe asset otherwise.

    Built as a compounded level rather than by switching between two price
    maps, because the sleeve holds *units* and swapping the series underneath
    it would reprice those units at an unrelated number overnight. Chaining
    daily returns is the only form that survives the handover.

    A day either asset does not print carries its own last price, so a gap in
    one calendar cannot mark the sleeve to zero — the failure this module has
    now had three times.
    """
    level = 100.0
    prev_risk: float | None = None
    prev_safe: float | None = None
    out: dict[date, float] = {}
    for day in sorted(set(risk_asset) | set(safe_asset)):
        risk_px = risk_asset.get(day)
        safe_px = safe_asset.get(day)
        if day in risk_on_days:
            if risk_px and prev_risk:
                level *= risk_px / prev_risk
        elif safe_px and prev_safe:
            level *= safe_px / prev_safe
        if risk_px:
            prev_risk = risk_px
        if safe_px:
            prev_safe = safe_px
        out[day] = level
    return out


def _closes(data_dir: Path, symbols: set[str]) -> dict[str, dict[date, float]]:
    out: dict[str, dict[date, float]] = {}
    for sym in symbols:
        bars = read_bars(data_dir, sym)
        if bars is not None:
            out[sym] = {d: float(c) for d, c in zip(bars.dates, bars.close)}
    return out


# The worst adverse move a single position is assumed to be able to make
# against us in one go, as a percentage of the entry price. It is NOT the stop
# — a stop is a resting order and a gap jumps straight through it. 14.1% is
# the worst single-trade loss in this record against a 3.5% stop, so a rule
# that sizes on the stop alone understates the true exposure by about four
# times. Declared here as a constant rather than re-read from each run's own
# worst trade, because sizing against the sample's own extreme is fitting to
# it: the next gap is free to be larger.
GAP_ALLOWANCE_PCT = 15.0

# ...and the same quantity as a MULTIPLE of the trade's own stop, because a
# fixed percentage is only right for one stop width. Measured on the worst
# trade in each configuration, the adverse move runs 4x the stop at a 3.5%
# stop and 6.8x at a 7% stop — a name that needs a wide stop is a name that
# can gap a long way. The binding constraint is whichever of the two is
# larger, so widening the stop automatically shrinks the position.
# 10x, not 6x. 6x matched the worst observed gap and therefore let the worst
# trade land at -1.04% of equity, just outside the 1% rule; a limit that the
# sample's own extreme already breaches is not a limit. 10x holds it at
# -0.91% and costs 2.75pp of CAGR.
GAP_ALLOWANCE_STOP_MULT = 10.0


def simulate(
    trades: Sequence[Mapping],
    data_dir: Path,
    config: PortfolioConfig | None = None,
    *,
    sessions: Sequence[date] | None = None,
    label: str = "mtm",
    regime_by_day: Mapping[date, str] | None = None,
    healthy_regimes: frozenset[str] | None = None,
    derisk_losers_only: bool = True,
    risk_scale_by_day: Mapping[date, float] | None = None,
    park_idle_in: Mapping[date, float] | None = None,
    park_only_on: "set[date] | None" = None,
    reserve: Sequence[Mapping] | None = None,
    pyramid: bool = False,
    pyramid_scale: float = 0.30,
    size_by: "Callable[[Mapping], float] | None" = None,
    max_equity_loss_pct: float | None = None,
    gap_allowance_pct: float = GAP_ALLOWANCE_PCT,
    gap_allowance_mult: float | None = GAP_ALLOWANCE_STOP_MULT,
) -> MTMResult | None:
    """Run the account, repricing every open position each session.

    With `regime_by_day` and `healthy_regimes` the book also **de-risks**: the
    session the market leaves the healthy set, every open position is sold at
    that day's close. Gating entries alone is not enough — it stops new risk
    going on and leaves the existing book to ride the decline, which is where
    this account lost its money in 2011, 2018, 2022 and 2025 while sitting at
    98.5% exposure.

    A forced exit is priced from the symbol's own close, not from the trade's
    stored R, because that R belongs to an exit that no longer happens.

    `park_idle_in` is a price series — the index — that uncommitted cash
    tracks instead of sitting flat. Every diagnosis run returned the same
    verdict, `under_deployed`: the filter is selective, so in a year like 2009
    the book holds a fraction of its capital and the rest earns nothing while
    the index compounds. Parking that remainder is the direct answer, and it
    changes what the account IS — a selective book plus an index sleeve, not a
    pure stock picker. Both readings are reported rather than one being
    presented as the bot.

    `size_by` returns a per-trade multiplier on the risk budget — conviction
    sizing. Betting more on a better-scored setup only makes sense if the
    score ranks outcomes, so the score has to be validated before this is
    switched on, not after.

    `pyramid` lets a symbol already held take a second, smaller entry when it
    signals again — adding to a position that is working, at
    `pyramid_scale` of the original size. Without it a re-signal in a name the
    book already owns is simply dropped, which throws away the one piece of
    evidence the book has that its own thesis is playing out.

    The add is only allowed while the existing position is **in profit**;
    averaging down is the opposite trade and is what turns a stop into a
    portfolio.

    `reserve` is a second, looser pool taken **only after every core signal
    for the day has been placed and capacity remains**. Every previous attempt
    at loosening the rules triggered on market state — a broad rally, a strong
    regime — and all of them lost, because they admitted weaker trades while
    the book was already full and simply displaced better ones. The diagnosis
    says the losing years are `starved`, which is a statement about *capacity*
    rather than about the market, so this triggers on capacity instead: the
    reserve is reached for only when the book has room it cannot otherwise
    fill.

    `park_only_on` restricts *new* parking to those sessions while leaving
    `park_idle_in` as the full price series used for valuation. The two must
    stay separate: an earlier version gated by dropping days out of the price
    map, so on an ungated day the lookup returned None and units already held
    were marked at **zero** — a -95.7% drawdown that was pure arithmetic.

    `derisk_losers_only` (the default) sells only the positions that are under
    water when the market turns and leaves the winners running. Liquidating
    everything cuts drawdown from -31.9% to -26.0% but takes the payoff ratio
    from 11.1 to 4.1, because the whole result lives in a handful of positions
    held for a year or more — and a position well in profit in a market that
    has just turned is the last thing to sell. Cutting only the losers keeps
    the tail and still stops the bleeding.
    """
    cfg = config or PortfolioConfig()
    usable = [
        t for t in trades
        if t.get("entry_day") and t.get("exit_day") and t.get("r_multiple") is not None
    ]
    if not usable:
        return None
    for t in usable:
        t["_entry"] = date.fromisoformat(str(t["entry_day"]))
        t["_exit"] = date.fromisoformat(str(t["exit_day"]))
    usable.sort(key=lambda t: (t["_entry"], str(t["symbol"])))

    price = _closes(data_dir, {str(t["symbol"]) for t in usable})
    if sessions is None:
        days = sorted({d for sym in price.values() for d in sym})
        lo, hi = usable[0]["_entry"], max(t["_exit"] for t in usable)
        sessions = [d for d in days if lo <= d <= hi]
    by_day: dict[date, list[Mapping]] = {}
    for t in usable:
        by_day.setdefault(t["_entry"], []).append(t)

    reserve_by_day: dict[date, list[Mapping]] = {}
    for t in (reserve or []):
        if not (t.get("entry_day") and t.get("exit_day") and t.get("r_multiple") is not None):
            continue
        t["_entry"] = date.fromisoformat(str(t["entry_day"]))
        t["_exit"] = date.fromisoformat(str(t["exit_day"]))
        reserve_by_day.setdefault(t["_entry"], []).append(t)
    for day_rows in reserve_by_day.values():
        day_rows.sort(key=lambda t: str(t["symbol"]))
    if reserve_by_day:
        price.update(_closes(data_dir, {
            str(t["symbol"]) for rows_ in reserve_by_day.values() for t in rows_
        }))

    cash = cfg.starting_equity
    equity = cfg.starting_equity
    park_units = 0.0          # units of the parked index held against cash
    last_park_px: float | None = None   # carried across sessions the index misses
    open_pos: list[dict] = []
    taken: list[dict] = []
    declined = 0
    curve: list[dict] = []
    peak = equity
    drawdown = 0.0
    invested_days = 0

    forced = 0
    for day in sessions:
        # Sell the parked index first, so `cash` is the whole uncommitted
        # balance before trades touch it. Parking at the end of the day and
        # restoring from units at the start of the next is the only ordering
        # that conserves money — reading cash back from units *after* a
        # purchase re-creates what the purchase just spent.
        # The index has its own calendar. On a session it does not print,
        # carry the last price forward — looking it up and getting None marked
        # held units at ZERO, which is where a -64% drawdown came from in a
        # book whose worst year was -12%. Same failure as the gating bug, one
        # layer down.
        today_px = None if park_idle_in is None else park_idle_in.get(day)
        if today_px is not None:
            last_park_px = today_px
        park_px = today_px if today_px is not None else last_park_px
        may_park = today_px is not None and (park_only_on is None or day in park_only_on)
        if park_units and park_px:
            cash += park_units * park_px
            park_units = 0.0

        risk_off = (
            regime_by_day is not None and healthy_regimes is not None
            and regime_by_day.get(day) not in healthy_regimes
        )

        # --- close anything due, at its realised R -------------------------
        still: list[dict] = []
        for p in open_pos:
            if p["exit"] <= day:
                cash += p["cost"] + p["risk_amount"] * p["r"]
                taken.append(p)
            elif risk_off:
                # Sold into the turn. Price it from the tape, and record the
                # R actually achieved so win rate and payoff stay truthful.
                series = price.get(p["symbol"], {})
                px = series.get(day)
                if px and p["entry_price"] > 0:
                    value = p["cost"] * (px / p["entry_price"])
                    if derisk_losers_only and value >= p["cost"]:
                        still.append(p)          # in profit: let it run
                        continue
                    p["r"] = (value - p["cost"]) / p["risk_amount"]
                    cash += value
                    taken.append(p)
                    forced += 1
                else:
                    still.append(p)              # no print: cannot price a sale
            else:
                still.append(p)
        open_pos = still

        # --- take new entries, budget permitting ---------------------------
        todays = list(by_day.get(day, []))
        if reserve_by_day and not risk_off:
            # Core first, always. The reserve is what is left when the core
            # could not use the capacity.
            todays = todays + list(reserve_by_day.get(day, []))
        held_syms = {p["symbol"] for p in open_pos}
        for t in ([] if risk_off else todays):
            stop_pct = float(t.get("risk_pct") or 0.0)
            if stop_pct <= 0:
                declined += 1
                continue
            sym = str(t["symbol"])
            adding = False
            if sym in held_syms:
                if not pyramid:
                    declined += 1
                    continue
                # Add only to a winner, and only once.
                existing = [p for p in open_pos if p["symbol"] == sym]
                px_now = price.get(sym, {}).get(day)
                winning = bool(px_now) and all(
                    px_now > p["entry_price"] for p in existing if p["entry_price"] > 0
                )
                if not winning or any(p.get("is_add") for p in existing):
                    declined += 1
                    continue
                adding = True
            if len(open_pos) >= cfg.max_concurrent and not adding:
                declined += 1
                continue
            scale = 1.0 if risk_scale_by_day is None else risk_scale_by_day.get(day, 1.0)
            risk_amount = equity * cfg.risk_per_trade_pct * scale / 100.0
            if size_by is not None:
                risk_amount *= float(size_by(t))
            if adding:
                risk_amount *= pyramid_scale
            cost = risk_amount / (stop_pct / 100.0)
            ceiling = equity * cfg.max_position_pct / 100.0
            if max_equity_loss_pct is not None:
                # The brief's rule: no single trade may cost more than
                # `max_equity_loss_pct` of total equity. A stop does not
                # deliver that on its own, because a stop is a resting order
                # and a gap jumps it — the worst trade in this record lost
                # 14.1% against a 3.5% stop. So the position is sized against
                # an assumed adverse move of `gap_allowance_pct`, not against
                # the stop, and the stop-based size is applied as well.
                allowance = gap_allowance_pct
                if gap_allowance_mult is not None:
                    # The gap is proportional to the stop, not a fixed number
                    # of percent: a name that needs a 7% stop is a name that
                    # can fall 40% overnight, and one that needs 3.5% is not.
                    allowance = max(allowance, gap_allowance_mult * stop_pct)
                ceiling = min(
                    ceiling,
                    equity * max_equity_loss_pct / allowance,
                    equity * max_equity_loss_pct / max(stop_pct, 0.01),
                )
            capped = min(cost, ceiling)
            if capped < cost:
                # The position cap binds, so the money actually at risk is
                # smaller than the budget asked for. Re-derive it, or the trade
                # books P&L on a position it was never allowed to take —
                # measured at ~2x inflation, because every trade here clips.
                risk_amount = capped * stop_pct / 100.0
            cost = capped
            deployed = sum(p["cost"] for p in open_pos)
            if deployed + cost > equity * cfg.max_deployed_pct / 100.0 or cost > cash:
                declined += 1
                continue
            risk_open = sum(p["risk_amount"] for p in open_pos)
            if risk_open + risk_amount > equity * cfg.max_portfolio_risk_pct / 100.0:
                declined += 1
                continue
            cash -= cost
            if adding:
                held_syms.add(sym)
            open_pos.append({
                "symbol": sym, "entry": t["_entry"], "exit": t["_exit"], "is_add": adding,
                # Equity at the moment the position was opened. The hit a trade
                # takes must be measured against the book it was sized from —
                # against STARTING equity a late loss in a compounding run
                # reads as -313%, which is arithmetic, not a risk breach.
                "equity_at_entry": equity,
                "cost": cost, "risk_amount": risk_amount, "r": float(t["r_multiple"]),
                "entry_price": float(t.get("entry") or 0.0),
                # Carried only so `fills` can describe what the account did.
                # `stop_pct` is the stop the position was actually sized on,
                # which is not always the signal's own risk_pct once the
                # engine's max_stop_pct has pulled it in.
                "strategy": t.get("strategy"),
                "stop_pct": stop_pct,
                "sessions_held": t.get("sessions_held"),
            })

        # --- mark the book to market ---------------------------------------
        held = 0.0
        for p in open_pos:
            series = price.get(p["symbol"], {})
            px = series.get(day)
            if px and p["entry_price"] > 0:
                held += p["cost"] * (px / p["entry_price"])
            else:
                held += p["cost"]      # no print today: carry at cost
        if may_park and cash > 0:
            park_units = cash / park_px
            cash = 0.0
        equity = cash + park_units * (park_px or 0.0) + held
        if open_pos:
            invested_days += 1
        peak = max(peak, equity)
        drawdown = min(drawdown, equity / peak - 1.0)
        curve.append({"day": day.isoformat(), "equity": round(equity, 2),
                      "open": len(open_pos)})

    if park_units:                      # liquidate the index sleeve
        last_px = None
        for day in reversed(sessions):
            if park_idle_in and park_idle_in.get(day):
                last_px = park_idle_in[day]
                break
        if last_px:
            cash += park_units * last_px
            park_units = 0.0
    for p in open_pos:                  # settle whatever is still open
        cash += p["cost"] + p["risk_amount"] * p["r"]
        taken.append(p)
    # What the worst trade actually cost the ACCOUNT. A -78% trade on an 8%
    # position is a -6% equity event; quoting the trade number alone makes a
    # gap look like a risk-control failure when it is a sizing question.
    worst_equity = min(
        (p["risk_amount"] * p["r"]) / max(p.get("equity_at_entry") or cfg.starting_equity, 1.0)
        * 100.0
        for p in taken
    ) if taken else 0.0
    equity = cash
    if curve:
        curve[-1]["equity"] = round(equity, 2)
    if not taken:
        return None

    span = max((sessions[-1] - sessions[0]).days / 365.25, 1e-9)
    cagr = ((equity / cfg.starting_equity) ** (1.0 / span) - 1.0) * 100.0
    rs = np.array([p["r"] for p in taken], dtype=float)
    wins, losses = rs[rs > 0], rs[rs <= 0]

    monthly: dict[str, float] = {}
    for pt in curve:
        d = date.fromisoformat(pt["day"])
        monthly[f"{d.year}-{d.month:02d}"] = pt["equity"]
    ms = [monthly[k] for k in sorted(monthly)]
    sharpe = 0.0
    if len(ms) > 3:
        rets = np.diff(ms) / np.asarray(ms[:-1])
        if rets.std() > 0:
            sharpe = float(rets.mean() / rets.std() * np.sqrt(12))

    eq = {date.fromisoformat(p["day"]): p["equity"] for p in curve}
    ds = sorted(eq)
    yearly: dict[int, float] = {}
    for y in range(ds[0].year, ds[-1].year + 1):
        inside = [d for d in ds if d.year == y]
        if len(inside) < 100:
            continue
        prior = [d for d in ds if d < date(y, 1, 1)]
        start = eq[prior[-1]] if prior else eq[inside[0]]
        yearly[y] = round((eq[inside[-1]] / start - 1.0) * 100.0, 2)

    return MTMResult(
        label=label, start=sessions[0].isoformat(), end=sessions[-1].isoformat(),
        years=round(span, 2), cagr_pct=round(cagr, 2),
        max_drawdown_pct=round(drawdown * 100.0, 2), sharpe=round(sharpe, 2),
        trades_taken=len(taken), signals_declined=declined,
        win_rate=round(100.0 * len(wins) / len(rs), 1),
        avg_r=round(float(rs.mean()), 3),
        payoff=round(float(wins.mean() / abs(losses.mean())), 2) if len(wins) and len(losses) else 0.0,
        exposure_pct=round(100.0 * invested_days / len(sessions), 1),
        worst_trade_equity_pct=round(worst_equity, 2),
        equity_curve=curve, yearly=yearly,
        fills=[{
            "symbol": p_["symbol"], "strategy": p_.get("strategy"),
            "entry_day": p_["entry"].isoformat() if hasattr(p_["entry"], "isoformat") else str(p_["entry"]),
            "exit_day": p_["exit"].isoformat() if hasattr(p_["exit"], "isoformat") else str(p_["exit"]),
            "r": round(float(p_["r"]), 3),
            "risk_pct": round(float(p_.get("stop_pct") or 0.0), 3),
            "net_pct": round(float(p_["r"]) * float(p_.get("stop_pct") or 0.0), 3),
            "sessions_held": p_.get("sessions_held"),
            "equity_pct": round(100.0 * (p_["risk_amount"] * p_["r"])
                                / max(p_.get("equity_at_entry") or cfg.starting_equity, 1.0), 3),
            "is_add": bool(p_.get("is_add")),
        } for p_ in taken],
    )
