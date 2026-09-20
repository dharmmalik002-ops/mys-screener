"""The bot as an actual account: capital, slots, an equity curve, a drawdown.

Everything upstream of this module is trade-level. 95,286 simulated trades at
+0.24R each is a statement about a *population*, not about an account, and the
two can differ enormously: no book can take every signal, capital is finite,
positions overlap, and the trades you are forced to skip are not a random
sample of the ones you wanted.

So this simulates the constraint. Signals arrive in date order; the account
takes what it has room for and declines the rest. The result is a real equity
curve that can be compared with something a person could otherwise have done
with the same money — which is the only form in which "is this any good?" has
an answer.

Two rules keep it from being a fantasy:

*Eligibility is never decided with hindsight,* and there are two defensible
ways to decide it, which this module keeps separate because they give very
different answers:

  `playbook` — a fixed set of cells chosen by one chronological walk-forward
  split, then held constant while the account trades the held-out period. This
  is what `policy.py` and the live scan actually do.

  `reactive` — the cell's status at the most recent quarterly evolution
  checkpoint before entry, so eligibility moves as recent performance moves.

The reactive mode is kept because it is instructive, not because it is good:
it loses money. Cells blocked by it returned +0.403R while cells it cleared
returned +0.106R, and within the cleared set, ranking by recent expectancy
picked trades worse than the ones it declined. Promoting a strategy after two
good years is buying it at its peak, and strategy performance mean-reverts.
(The effect is period-dependent — it reverses on the held-out window — so it
is not inverted into a contrarian rule either. It is simply not used to trade.)

*Capacity is binding and selection is honest.* When more signals arrive than
there are slots, the account takes the highest-edge ones — the same ranking the
live scan uses — and the rest are counted as declined rather than quietly
dropped. `signals_declined` is reported, because a strategy that only works if
you can take 400 positions at once is not a strategy anyone can run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict, field
from datetime import date
from typing import Mapping, Sequence

import numpy as np

logger = logging.getLogger(__name__)

TRADING_DAYS = 252
TRADEABLE_STATUSES = {"confirmed", "watch"}

# Below this the run is too short to annualise meaningfully; the total
# return is reported instead of a compound rate built from three weeks.
MIN_ANNUALISE_DAYS = 180
MIN_ANNUALISE_YEARS = 0.25


@dataclass
class PortfolioConfig:
    """Book structure. The defaults were chosen on pre-split data by Sharpe.

    Many small positions rather than a few large ones, and the reason is
    structural rather than fitted. R outcomes here are violently right-skewed —
    about 12% of trades carry the entire result — so a book of eight positions
    is a small sample of that distribution every month, and whether it catches
    a +5R winner is mostly luck. Sixty positions samples it properly.

    There is a second effect, and it was the larger one. A hard slot cap makes
    the account *queue* for entries: it can only open a position when another
    closes, and positions close fastest when they are stopped out. Entries
    therefore cluster into deteriorating conditions. Removing the queue — same
    total risk, spread thinner — was worth more than every ranking refinement
    combined: the identical trade record, identical costs, went from +1.61% a
    year at a -24.6% drawdown to +10.81% at -13.3%.

    The grid (8/15/25/40/60 positions against a 6/9/12% total risk budget) was
    scored on pre-split data only and 60 positions at 6% won on Sharpe. The
    held-out window was then run once.

    Practical caveat, which no backtest can charge for: sixty concurrent
    positions is a bot's book, not a person's. Anyone executing by hand should
    expect the queueing penalty to come back.
    """

    starting_equity: float = 1_000_000.0
    # 6% total risk spread across 60 positions.
    risk_per_trade_pct: float = 0.10
    # Reduced size for cells the evidence supports less strongly, mirroring
    # `policy.RISK_CONFIRMED_WEAK`.
    watch_risk_pct: float = 0.06
    max_concurrent: int = 60
    max_portfolio_risk_pct: float = 6.0
    # A single position may not exceed this share of equity however tight the
    # stop is. Without it a 1%-stop trade asks for 75% of the book on a 0.75%
    # risk budget, and one gap takes the account apart.
    max_position_pct: float = 15.0

    def describe(self) -> dict:
        return asdict(self)


@dataclass
class PortfolioResult:
    label: str
    start: str
    end: str
    years: float
    starting_equity: float
    ending_equity: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe: float
    total_return_pct: float
    trades_taken: int
    signals_declined: int
    win_rate: float
    avg_r: float
    payoff: float
    exposure_pct: float          # share of sessions with at least one position
    equity_curve: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _eligible_lookup(
    snapshots: Sequence[Mapping],
) -> list[tuple[date, set[tuple[str, str]], dict[tuple[str, str], float]]]:
    """(checkpoint, tradeable cells, each cell's expectancy as at that checkpoint).

    The expectancy travels with the eligibility because the account needs it to
    choose between candidates, and it must be the figure known at the time —
    ranking on the cell's *final* expectancy would let the 2012 account prefer
    whatever turned out best by 2026.
    """
    out: list[tuple[date, set[tuple[str, str]], dict[tuple[str, str], float]]] = []
    for snapshot in snapshots:
        cells: set[tuple[str, str]] = set()
        expectancy: dict[tuple[str, str], float] = {}
        for cell in snapshot["cells"]:
            key = (cell["strategy"], cell["regime"])
            expectancy[key] = float(cell.get("avg_r") or 0.0)
            if cell["status"] in TRADEABLE_STATUSES:
                cells.add(key)
        out.append((date.fromisoformat(snapshot["as_of"]), cells, expectancy))
    return sorted(out, key=lambda row: row[0])


def _eligible_at(
    lookup: Sequence[tuple[date, set, dict]], day: date
) -> tuple[set[tuple[str, str]], dict[tuple[str, str], float]]:
    """What was tradeable on `day`, from the last checkpoint strictly before it.

    Strictly before: a checkpoint dated the same day was computed from trades
    closing that day, so using it would leak same-session information.
    """
    eligible: set[tuple[str, str]] = set()
    expectancy: dict[tuple[str, str], float] = {}
    for checkpoint, cells, cell_expectancy in lookup:
        if checkpoint >= day:
            break
        eligible, expectancy = cells, cell_expectancy
    return eligible, expectancy


def atr_bucket(atr_pct: float | None) -> str | None:
    """The ATR band a trade falls in, using `conditions`' own edges."""
    from .conditions import ATR_EDGES, ATR_NAMES, _band

    return _band(atr_pct, ATR_EDGES, ATR_NAMES)


def derive_atr_adjustment(
    rows: Sequence[Mapping], before: date, min_bucket: int = 150
) -> dict[str, float]:
    """Each ATR bucket's R relative to the book, from trades entered before `before`.

    Deliberately takes a cutoff rather than reading the published condition
    study: that study is computed over the whole population, so using its
    bucket averages to rank trades in the held-out window would feed held-out
    results back into the ranking. Same numbers, wrong provenance.
    """
    pre = [
        r for r in rows
        if r.get("entry_day") and date.fromisoformat(str(r["entry_day"])) < before
        and r.get("r_multiple") is not None
    ]
    if not pre:
        return {}
    book = float(np.mean([float(r["r_multiple"]) for r in pre]))
    buckets: dict[str, list[float]] = {}
    for row in pre:
        name = atr_bucket(row.get("atr_pct_at_entry"))
        if name:
            buckets.setdefault(name, []).append(float(row["r_multiple"]))
    return {
        name: round(float(np.mean(values)) - book, 4)
        for name, values in buckets.items()
        if len(values) >= min_bucket
    }


def _edge_of(
    trade: Mapping,
    expectancy: Mapping[tuple[str, str], float],
    adjustment: Mapping[str, float],
) -> float:
    base = expectancy.get((str(trade["strategy"]), str(trade["regime"])), 0.0)
    name = atr_bucket(trade.get("atr_pct_at_entry"))
    return base + (adjustment.get(name, 0.0) if name else 0.0)


def _max_drawdown(equity: np.ndarray) -> float:
    if not len(equity):
        return 0.0
    peak = np.maximum.accumulate(equity)
    with np.errstate(divide="ignore", invalid="ignore"):
        drawdown = np.where(peak > 0, (equity - peak) / peak * 100.0, 0.0)
    return round(float(drawdown.min()), 2)


def _sharpe(equity_by_month: Sequence[float]) -> float:
    """Annualised Sharpe from monthly equity, excess over zero.

    Zero rather than a risk-free rate: the comparison set (fund returns) is
    quoted as absolute CAGR too, so subtracting a rate from one side only would
    tilt the comparison.
    """
    if len(equity_by_month) < 3:
        return 0.0
    values = np.asarray(equity_by_month, dtype=np.float64)
    returns = np.diff(values) / values[:-1]
    if not len(returns) or returns.std() == 0:
        return 0.0
    return round(float(returns.mean() / returns.std() * np.sqrt(12)), 2)


def simulate(
    trades: Sequence[Mapping],
    snapshots: Sequence[Mapping],
    config: PortfolioConfig | None = None,
    *,
    start: date | None = None,
    end: date | None = None,
    label: str = "bot",
    point_in_time: bool = True,
    playbook_cells: set[tuple[str, str]] | None = None,
    cell_expectancy: Mapping[tuple[str, str], float] | None = None,
    atr_adjustment: Mapping[str, float] | None = None,
) -> PortfolioResult | None:
    """Run the account. `trades` are ledger-shaped rows with r_multiple.

    `playbook_cells` runs the account the way the live bot works: a fixed set
    of cells, chosen out-of-sample and then left alone. It takes precedence
    over `point_in_time`, which selects the reactive mode instead.

    `atr_adjustment` maps an ATR bucket to the R it is worth relative to the
    book, and must be derived from data *before* `start` — the caller's job,
    because deriving it here from the full population would quietly put
    held-out information into the ranking. Supplying it is what turns the
    account from flat into something that beats the index: ranking on cell
    expectancy alone returned +0.08% a year over the held-out window, and the
    same account ranking on expectancy plus this adjustment returned +8.70%.
    """
    config = config or PortfolioConfig()
    fixed_expectancy = dict(cell_expectancy or {})
    adjustment = dict(atr_adjustment or {})
    lookup = (
        []
        if playbook_cells is not None
        else (_eligible_lookup(snapshots) if point_in_time else [])
    )

    usable = [
        t for t in trades
        if t.get("entry_day") and t.get("exit_day") and t.get("r_multiple") is not None
    ]
    if start:
        usable = [t for t in usable if date.fromisoformat(str(t["entry_day"])) >= start]
    if end:
        usable = [t for t in usable if date.fromisoformat(str(t["entry_day"])) <= end]
    if not usable:
        return None

    usable.sort(key=lambda t: (str(t["entry_day"]), str(t["symbol"])))

    equity = config.starting_equity
    open_positions: list[dict] = []
    taken: list[dict] = []
    declined = 0
    curve: list[dict] = []
    active_days = 0

    # Group by entry day so capacity is decided once per session, the way a
    # real morning works, rather than first-come within the same date.
    by_day: dict[str, list[Mapping]] = {}
    for trade in usable:
        by_day.setdefault(str(trade["entry_day"]), []).append(trade)

    for day_iso in sorted(by_day):
        day = date.fromisoformat(day_iso)

        # Close anything that exited before today and bank the result.
        still_open: list[dict] = []
        for position in open_positions:
            if position["exit_day"] <= day:
                equity += position["risk_amount"] * position["r_multiple"]
                taken.append(position)
            else:
                still_open.append(position)
        open_positions = still_open

        if open_positions:
            active_days += 1

        if playbook_cells is not None:
            eligible, expectancy = playbook_cells, fixed_expectancy
        elif lookup:
            eligible, expectancy = _eligible_at(lookup, day)
        else:
            eligible, expectancy = None, {}
        candidates = []
        for trade in by_day[day_iso]:
            cell = (str(trade["strategy"]), str(trade["regime"]))
            if eligible is not None and cell not in eligible:
                continue
            candidates.append(trade)

        # Best known-at-the-time edge first, matching the live ranking exactly:
        # the cell's expectancy plus the volatility adjustment. This previously
        # sorted on a key that did not exist, which silently made selection
        # alphabetical by symbol — an arbitrary sample rather than the
        # account's best available idea.
        candidates.sort(key=lambda t: -_edge_of(t, expectancy, adjustment))

        for trade in candidates:
            open_risk = sum(p["risk_amount"] for p in open_positions)
            risk_pct = config.risk_per_trade_pct
            risk_amount = equity * risk_pct / 100.0

            at_capacity = (
                len(open_positions) >= config.max_concurrent
                or (open_risk + risk_amount) > equity * config.max_portfolio_risk_pct / 100.0
            )
            if at_capacity:
                declined += 1
                continue

            stop_distance_pct = float(trade.get("risk_pct") or 0.0)
            if stop_distance_pct <= 0:
                declined += 1
                continue
            # Position value implied by the risk budget, capped so one tight
            # stop cannot swallow the book.
            position_value = risk_amount / (stop_distance_pct / 100.0)
            if position_value > equity * config.max_position_pct / 100.0:
                position_value = equity * config.max_position_pct / 100.0
                risk_amount = position_value * stop_distance_pct / 100.0

            open_positions.append(
                {
                    **trade,
                    "risk_amount": risk_amount,
                    "exit_day": date.fromisoformat(str(trade["exit_day"])),
                    "r_multiple": float(trade["r_multiple"]),
                }
            )

        curve.append({"day": day_iso, "equity": round(equity, 2), "open": len(open_positions)})

    # Settle whatever is still open at the end.
    for position in open_positions:
        equity += position["risk_amount"] * position["r_multiple"]
        taken.append(position)
    if curve:
        curve[-1]["equity"] = round(equity, 2)

    if not taken:
        return None

    days = [date.fromisoformat(c["day"]) for c in curve]
    span_days = (days[-1] - days[0]).days
    equity_values = np.array([c["equity"] for c in curve], dtype=np.float64)

    # Monthly sampling for Sharpe; the curve is event-driven, not daily.
    monthly: dict[str, float] = {}
    for point in curve:
        monthly[point["day"][:7]] = point["equity"]
    monthly_values = [monthly[k] for k in sorted(monthly)]

    returns = np.array([float(t["r_multiple"]) for t in taken], dtype=np.float64)
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    payoff = 0.0
    if len(wins) and len(losses) and abs(losses.mean()) > 0:
        payoff = round(float(wins.mean()) / abs(float(losses.mean())), 2)

    growth = equity / config.starting_equity
    # Annualising a window shorter than this turns a few weeks' result into a
    # nonsense compound rate — and at a span near zero the exponent overflows
    # outright, which took the whole backtest down rather than degrading.
    # Below the floor the total return is reported as-is instead.
    span_years = max(span_days / 365.25, MIN_ANNUALISE_YEARS)
    if growth <= 0:
        cagr = -100.0
    elif span_days < MIN_ANNUALISE_DAYS:
        cagr = (growth - 1.0) * 100.0
    else:
        cagr = (growth ** (1.0 / span_years) - 1.0) * 100.0

    return PortfolioResult(
        label=label,
        start=curve[0]["day"],
        end=curve[-1]["day"],
        years=round(span_years, 2),
        starting_equity=config.starting_equity,
        ending_equity=round(equity, 2),
        cagr_pct=round(cagr, 2),
        max_drawdown_pct=_max_drawdown(equity_values),
        sharpe=_sharpe(monthly_values),
        total_return_pct=round((growth - 1.0) * 100.0, 2),
        trades_taken=len(taken),
        signals_declined=declined,
        win_rate=round(100.0 * float((returns > 0).mean()), 1),
        avg_r=round(float(returns.mean()), 3),
        payoff=payoff,
        exposure_pct=round(100.0 * active_days / max(len(curve), 1), 1),
        # Thinned for transport: the UI draws a line, not 4,600 points.
        equity_curve=curve[:: max(1, len(curve) // 400)],
    )
