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


def measure(
    rows: Sequence[Mapping],
    playbook_cells: set[tuple[str, str]],
    cell_expectancy: Mapping[tuple[str, str], float],
    split: date,
    index_cagr: float | None,
    fund_median_cagr: float | None,
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

    lines = [
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
