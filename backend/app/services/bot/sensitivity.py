"""What the account returns across every reasonable book structure, not one.

Quoting a single configuration's CAGR implies that configuration was chosen
well. It was not, and this module is the measurement that says so: across
thirty sensible books (40-100 positions, 5-9% total risk), the rank
correlation between a configuration's Sharpe on pre-split data and its CAGR on
the held-out window is **-0.69**. Choosing the book on the data available at
the time did not merely fail to help — it pointed the wrong way, the same
mean-reversion that makes `evolution.py`'s reactive gating lose money.

The honest consequence is not to pick the configuration that happened to win
out-of-sample; that is selecting on the held-out window, which is the one thing
no result here is allowed to do. It is to stop pretending the choice can be
made at all, and report the distribution the choice is drawn from. A user
picking any reasonable book structure gets a draw from this distribution, so
this distribution — its middle and its spread — is the honest answer to "what
should I expect".

It also sets the precision. Held-out CAGR ranges from about 8% to 15% across
configurations that are all defensible. Any claim quoted to a tenth of a point
is describing one draw and calling it a measurement.

`by_period` exists for a related and worse problem. The account's return is
extremely lumpy — it beat the index by 30 points in one twelve-month stretch
and lagged it in the next two — so a single CAGR is dominated by which years
the window happens to contain. Comparing a 3.8-year bot figure against a
3-year fund figure, as the first version of this benchmark did, is not a
comparison at all: the extra ten months contained the best run in the sample
and inflated the bot's number by roughly six points. The period table makes
that visible instead of letting one window stand in for the result.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date
from typing import Mapping, Sequence

import numpy as np

from .portfolio import PortfolioConfig, simulate

logger = logging.getLogger(__name__)

# Book structures a reasonable person might choose. Wide enough to cover the
# real decision space, and every one of them defensible on its own terms — a
# grid stuffed with absurd configurations would flatter the spread.
POSITION_COUNTS = (40, 50, 60, 70, 80, 100)
RISK_BUDGETS = (5.0, 6.0, 7.0, 8.0, 9.0)


@dataclass
class ConfigResult:
    positions: int
    total_risk_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe: float
    trades: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Sensitivity:
    configs: list[ConfigResult]
    median_cagr: float
    p25_cagr: float
    p75_cagr: float
    min_cagr: float
    max_cagr: float
    median_drawdown: float
    share_beating_index: float
    share_beating_fund_median: float
    selection_rank_correlation: float | None
    by_period: list[dict]
    note: str

    def to_dict(self) -> dict:
        out = asdict(self)
        out["configs"] = [c.to_dict() for c in self.configs]
        return out


def _spearman(a: Sequence[float], b: Sequence[float]) -> float | None:
    if len(a) < 3 or len(b) != len(a):
        return None
    ra = np.argsort(np.argsort(np.asarray(a, dtype=np.float64)))
    rb = np.argsort(np.argsort(np.asarray(b, dtype=np.float64)))
    if ra.std() == 0 or rb.std() == 0:
        return None
    return float(np.corrcoef(ra, rb)[0, 1])


def annual_periods(start: date, end: date) -> list[tuple[str, date, date]]:
    """Rolling twelve-month windows back from `end`, oldest first."""
    spans: list[tuple[str, date, date]] = []
    cursor = end
    while True:
        previous = date(cursor.year - 1, cursor.month, min(cursor.day, 28))
        if previous < start:
            if (cursor - start).days > 120:
                spans.append((f"{start:%Y-%m} to {cursor:%Y-%m}", start, cursor))
            break
        spans.append((f"{previous:%Y-%m} to {cursor:%Y-%m}", previous, cursor))
        cursor = previous
    return list(reversed(spans))


def measure(
    rows: Sequence[Mapping],
    playbook_cells: set[tuple[str, str]],
    cell_expectancy: Mapping[tuple[str, str], float],
    split: date,
    index_cagr: float | None,
    fund_median_cagr: float | None,
    index_series: Mapping[date, float] | None = None,
    end: date | None = None,
) -> Sensitivity | None:
    """Run every configuration over the held-out window, and over pre-split."""
    if not rows or not playbook_cells:
        return None

    results: list[ConfigResult] = []
    pre_sharpes: list[float] = []
    post_cagrs: list[float] = []

    for positions in POSITION_COUNTS:
        for budget in RISK_BUDGETS:
            config = PortfolioConfig(
                max_concurrent=positions,
                risk_per_trade_pct=budget / positions,
                max_portfolio_risk_pct=budget,
            )
            held = simulate(
                rows, [], config, start=split, label="held",
                playbook_cells=playbook_cells, cell_expectancy=cell_expectancy,
            )
            if held is None:
                continue
            results.append(
                ConfigResult(
                    positions=positions,
                    total_risk_pct=budget,
                    cagr_pct=held.cagr_pct,
                    max_drawdown_pct=held.max_drawdown_pct,
                    sharpe=held.sharpe,
                    trades=held.trades_taken,
                )
            )
            # The pre-split run exists only to test whether choosing on it
            # would have worked. It never selects anything here.
            earlier = simulate(
                rows, [], config, end=split, label="pre",
                playbook_cells=playbook_cells, cell_expectancy=cell_expectancy,
            )
            if earlier is not None:
                pre_sharpes.append(earlier.sharpe)
                post_cagrs.append(held.cagr_pct)

    if len(results) < 5:
        return None

    cagrs = np.asarray([r.cagr_pct for r in results], dtype=np.float64)
    drawdowns = np.asarray([r.max_drawdown_pct for r in results], dtype=np.float64)
    correlation = _spearman(pre_sharpes, post_cagrs)

    # Year by year, so no single window can stand in for the result.
    by_period: list[dict] = []
    if end is not None:
        for label, d0, d1 in annual_periods(split, end):
            period_cagrs: list[float] = []
            for positions in POSITION_COUNTS:
                for budget in RISK_BUDGETS:
                    config = PortfolioConfig(
                        max_concurrent=positions,
                        risk_per_trade_pct=budget / positions,
                        max_portfolio_risk_pct=budget,
                    )
                    run = simulate(
                        rows, [], config, start=d0, end=d1, label="period",
                        playbook_cells=playbook_cells, cell_expectancy=cell_expectancy,
                    )
                    if run:
                        period_cagrs.append(run.cagr_pct)
            if not period_cagrs:
                continue
            values = np.asarray(period_cagrs, dtype=np.float64)
            index_return = None
            if index_series:
                days = sorted(d for d in index_series if d0 <= d <= d1)
                if len(days) >= 2 and index_series[days[0]] > 0:
                    years = max((days[-1] - days[0]).days / 365.25, 1e-9)
                    growth = index_series[days[-1]] / index_series[days[0]]
                    index_return = round((growth ** (1.0 / years) - 1.0) * 100.0, 2)
            by_period.append(
                {
                    "period": label,
                    "bot_median_cagr": round(float(np.median(values)), 2),
                    "bot_p25_cagr": round(float(np.percentile(values, 25)), 2),
                    "bot_p75_cagr": round(float(np.percentile(values, 75)), 2),
                    "index_cagr": index_return,
                    "excess_vs_index": (
                        round(float(np.median(values)) - index_return, 2)
                        if index_return is not None else None
                    ),
                }
            )

    excesses = [p["excess_vs_index"] for p in by_period if p.get("excess_vs_index") is not None]
    lines: list[str] = []
    if len(excesses) >= 3 and (max(excesses) - min(excesses)) > 20.0:
        lines.append(
            f"Returns are extremely lumpy: across {len(excesses)} twelve-month periods the "
            f"account ran from {min(excesses):+.0f} to {max(excesses):+.0f} points against the "
            "index. Any single window's CAGR is mostly a statement about which years it "
            "contains, so the period table below matters more than the headline."
        )
    lines += [
        f"{len(results)} defensible book structures ({POSITION_COUNTS[0]}-{POSITION_COUNTS[-1]} "
        f"positions, {RISK_BUDGETS[0]:.0f}-{RISK_BUDGETS[-1]:.0f}% total risk) run over the "
        f"held-out window. Median {float(np.median(cagrs)):.2f}% a year, "
        f"{float(np.percentile(cagrs, 25)):.1f}% to {float(np.percentile(cagrs, 75)):.1f}% "
        "across the middle half."
    ]
    if correlation is not None and correlation < -0.3:
        lines.append(
            f"Choosing the structure on pre-split data would have pointed the wrong way "
            f"(rank correlation {correlation:+.2f} against held-out return), so no single "
            "configuration's figure is quoted as the result — the distribution is."
        )
    elif correlation is not None:
        lines.append(
            f"Pre-split ranking carried a rank correlation of {correlation:+.2f} with held-out "
            "return, which is too weak to select on."
        )

    return Sensitivity(
        configs=sorted(results, key=lambda r: -r.cagr_pct),
        by_period=by_period,
        median_cagr=round(float(np.median(cagrs)), 2),
        p25_cagr=round(float(np.percentile(cagrs, 25)), 2),
        p75_cagr=round(float(np.percentile(cagrs, 75)), 2),
        min_cagr=round(float(cagrs.min()), 2),
        max_cagr=round(float(cagrs.max()), 2),
        median_drawdown=round(float(np.median(drawdowns)), 2),
        share_beating_index=(
            round(float((cagrs > index_cagr).mean() * 100.0), 1) if index_cagr is not None else 0.0
        ),
        share_beating_fund_median=(
            round(float((cagrs > fund_median_cagr).mean() * 100.0), 1)
            if fund_median_cagr is not None else 0.0
        ),
        selection_rank_correlation=round(correlation, 2) if correlation is not None else None,
        note=" ".join(lines),
    )
