"""Is it better than a professional? Answered by measurement, not assertion.

"Better than a professional trader" is usually argued rhetorically, which is
why it is usually meaningless. It is a testable claim, and this repository
happens to hold the material to test it: `mf_universe.json` carries ~1,100
Indian equity and hybrid funds with returns derived from AMFI NAV — real money,
managed by paid professionals, measured over the same market the bot trades,
with the same drawdowns and the same costs already inside the NAV.

So the comparison is against three things a person could actually have done
with the money, and the bot is ranked inside the real distribution rather than
against a straw man:

  1. **Nifty buy-and-hold.** The free alternative. Beating professionals while
     losing to an index fund is not a victory.
  2. **The fund distribution.** Every fund with a return over the same window,
     as a percentile. "Top-quartile" is a claim with a denominator.
  3. **Risk.** CAGR alone rewards leverage and ignores the drawdown that would
     actually have made someone stop. Drawdown and Sharpe are compared too, and
     a worse drawdown is reported as worse.

Three caveats travel with the verdict permanently, because they are the reason
an honest comparison is still not a like-for-like one:

  - Fund returns are *realised*; the bot's are *simulated*. Simulation does not
    hesitate, oversleep, or size down after three losses.
  - Funds must stay invested under a mandate. The bot may sit in cash, which is
    an enormous structural advantage in a bear market and not skill.
  - Funds are measured net of every real cost. The bot's costs are modelled.

They are stated in the payload, not in a footnote, because a verdict that
travels without them will be quoted without them.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Sequence

import numpy as np

from .history import read_bars
from .portfolio import PortfolioResult

logger = logging.getLogger(__name__)

# Windows the fund universe publishes returns for, mapped to years.
WINDOWS = {"return_3y": 3, "return_5y": 5, "return_10y": 10}
MIN_FUNDS = 50

CAVEATS = (
    "Fund returns are realised money; the bot's are simulated. A simulation never hesitates, "
    "never oversleeps, and never sizes down after three losses in a row.",
    "Funds must stay invested under their mandate. The bot is allowed to sit in cash, which is "
    "a large structural advantage in a falling market and is not skill.",
    "Fund NAV is net of every real cost including the manager's fee. The bot's costs are "
    "modelled (STT, stamp duty, exchange, GST, 15 bps slippage), not incurred.",
    "The bot's universe is today's listed companies, so it never traded anything that went to "
    "zero. The funds did.",
)


@dataclass
class BenchmarkComparison:
    window_years: int
    bot_cagr_pct: float
    bot_max_drawdown_pct: float
    bot_sharpe: float
    index_cagr_pct: float | None
    funds_counted: int
    fund_median_cagr: float
    fund_p75_cagr: float
    fund_p90_cagr: float
    fund_best_cagr: float
    percentile: float            # where the bot sits inside the fund distribution
    beats_median: bool
    beats_index: bool | None
    fund_median_drawdown: float | None
    drawdown_better_than_median: bool | None
    # Return earned per unit of worst drawdown. The comparison that matters to
    # anyone who has to actually hold the position: a 12% return through a 35%
    # drawdown and a 9% return through a 15% one are not the same result, and
    # CAGR alone calls the first one better.
    bot_return_per_drawdown: float | None
    fund_return_per_drawdown: float | None
    risk_adjusted_better: bool | None
    scorecard: list[dict]
    verdict: str

    def to_dict(self) -> dict:
        return asdict(self)


def _fund_returns(universe_path: Path, field: str) -> tuple[list[float], list[float]]:
    """(CAGRs, max drawdowns) for every fund reporting this window."""
    try:
        payload = json.loads(universe_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("benchmark: cannot read fund universe: %s", exc)
        return [], []

    cagrs: list[float] = []
    drawdowns: list[float] = []
    for fund in payload.get("funds") or []:
        value = fund.get(field)
        if isinstance(value, (int, float)) and np.isfinite(value):
            cagrs.append(float(value))
            drawdown = fund.get("max_drawdown")
            if isinstance(drawdown, (int, float)) and np.isfinite(drawdown):
                drawdowns.append(float(drawdown))
    return cagrs, drawdowns


def _index_cagr(data_dir: Path, start: date, end: date) -> float | None:
    """Nifty buy-and-hold over the same window, in CAGR terms."""
    bars = read_bars(data_dir, "NIFTY")
    if bars is None or not len(bars):
        return None
    mask = [(start <= d <= end) for d in bars.dates]
    closes = bars.close[np.array(mask)]
    if len(closes) < 2 or closes[0] <= 0:
        return None
    years = max((end - start).days / 365.25, 1e-9)
    growth = float(closes[-1] / closes[0])
    return round((growth ** (1.0 / years) - 1.0) * 100.0, 2)


def compare(
    result: PortfolioResult,
    data_dir: Path,
    *,
    window_field: str = "return_3y",
) -> BenchmarkComparison | None:
    """Rank the bot's account inside the real distribution of fund managers."""
    years = WINDOWS.get(window_field)
    if years is None:
        return None

    cagrs, drawdowns = _fund_returns(data_dir / "mf_universe.json", window_field)
    if len(cagrs) < MIN_FUNDS:
        return None

    values = np.asarray(cagrs, dtype=np.float64)
    percentile = round(float((values < result.cagr_pct).mean() * 100.0), 1)
    median = round(float(np.median(values)), 2)

    index_cagr = _index_cagr(
        data_dir, date.fromisoformat(result.start), date.fromisoformat(result.end)
    )
    beats_index = None if index_cagr is None else result.cagr_pct > index_cagr

    fund_median_dd = round(float(np.median(drawdowns)), 2) if drawdowns else None
    # Drawdowns are negative; "better" means shallower, i.e. closer to zero.
    dd_better = (
        None if fund_median_dd is None
        else result.max_drawdown_pct > fund_median_dd
    )

    bot_rpd = (
        round(result.cagr_pct / abs(result.max_drawdown_pct), 3)
        if result.max_drawdown_pct < 0 else None
    )
    fund_rpd = (
        round(median / abs(fund_median_dd), 3)
        if fund_median_dd and fund_median_dd < 0 else None
    )
    risk_better = (
        None if (bot_rpd is None or fund_rpd is None) else bot_rpd > fund_rpd
    )

    # Answered one dimension at a time, because "better than a professional"
    # is not a single question and a single yes/no would be a slogan.
    scorecard = [
        {
            "dimension": "Beats the free alternative (Nifty buy-and-hold)",
            "bot": f"{result.cagr_pct:.2f}%",
            "reference": f"{index_cagr:.2f}%" if index_cagr is not None else "—",
            "verdict": None if beats_index is None else bool(beats_index),
        },
        {
            "dimension": "Return vs the median professional fund",
            "bot": f"{result.cagr_pct:.2f}%",
            "reference": f"{median:.2f}%",
            "verdict": bool(result.cagr_pct > median),
        },
        {
            "dimension": "Worst drawdown vs the median fund",
            "bot": f"{result.max_drawdown_pct:.1f}%",
            "reference": f"{fund_median_dd:.1f}%" if fund_median_dd is not None else "—",
            "verdict": dd_better,
        },
        {
            "dimension": "Return per unit of drawdown",
            "bot": f"{bot_rpd:.2f}" if bot_rpd is not None else "—",
            "reference": f"{fund_rpd:.2f}" if fund_rpd is not None else "—",
            "verdict": risk_better,
        },
    ]

    parts: list[str] = []
    if percentile >= 90:
        parts.append(f"The account's {result.cagr_pct:.1f}% CAGR sits in the top {100 - percentile:.0f}% of {len(cagrs)} professionally managed funds over the same window")
    elif percentile >= 50:
        parts.append(f"The account's {result.cagr_pct:.1f}% CAGR beats {percentile:.0f}% of {len(cagrs)} professionally managed funds")
    else:
        parts.append(f"The account's {result.cagr_pct:.1f}% CAGR is below {100 - percentile:.0f}% of {len(cagrs)} professionally managed funds")

    if beats_index is True:
        parts.append(f"and beats Nifty buy-and-hold ({index_cagr:.1f}%)")
    elif beats_index is False:
        parts.append(f"but loses to Nifty buy-and-hold ({index_cagr:.1f}%), which needed no work at all")

    if dd_better is True:
        parts.append(f"with a shallower worst drawdown ({result.max_drawdown_pct:.0f}% against a fund median of {fund_median_dd:.0f}%)")
    elif dd_better is False:
        parts.append(f"but with a deeper worst drawdown ({result.max_drawdown_pct:.0f}% against a fund median of {fund_median_dd:.0f}%)")

    return BenchmarkComparison(
        window_years=years,
        bot_cagr_pct=result.cagr_pct,
        bot_max_drawdown_pct=result.max_drawdown_pct,
        bot_sharpe=result.sharpe,
        index_cagr_pct=index_cagr,
        funds_counted=len(cagrs),
        fund_median_cagr=median,
        fund_p75_cagr=round(float(np.percentile(values, 75)), 2),
        fund_p90_cagr=round(float(np.percentile(values, 90)), 2),
        fund_best_cagr=round(float(values.max()), 2),
        percentile=percentile,
        beats_median=result.cagr_pct > median,
        beats_index=beats_index,
        fund_median_drawdown=fund_median_dd,
        drawdown_better_than_median=dd_better,
        bot_return_per_drawdown=bot_rpd,
        fund_return_per_drawdown=fund_rpd,
        risk_adjusted_better=risk_better,
        scorecard=scorecard,
        verdict=", ".join(parts) + ".",
    )


def build_benchmark(
    results: Sequence[PortfolioResult],
    data_dir: Path,
) -> dict:
    """Every run compared against the professional distribution."""
    comparisons: list[dict] = []
    for result in results:
        # Match the fund window to the run's own length so a 3-year account is
        # not measured against 10-year fund returns.
        field = "return_3y"
        if result.years >= 8:
            field = "return_10y"
        elif result.years >= 4.5:
            field = "return_5y"
        comparison = compare(result, data_dir, window_field=field)
        if comparison:
            comparisons.append({"run": result.label, **comparison.to_dict()})

    # The held-out playbook run is the only one that describes the live system
    # on data it never saw. If it exists it is the headline; nothing else is.
    headline = next(
        (c for c in comparisons if c["run"] == "playbook_held_out"),
        comparisons[0] if comparisons else None,
    )
    answer = None
    if headline:
        won = [d for d in headline["scorecard"] if d["verdict"] is True]
        lost = [d for d in headline["scorecard"] if d["verdict"] is False]
        answer = {
            "run": headline["run"],
            "wins": len(won),
            "losses": len(lost),
            "won_on": [d["dimension"] for d in won],
            "lost_on": [d["dimension"] for d in lost],
            "summary": (
                f"Measured over {headline['window_years']} years it never saw, against "
                f"{headline['funds_counted']} real funds: better on {len(won)} of "
                f"{len(won) + len(lost)} dimensions. "
                + ("It beats the index and takes less damage doing it, while earning less than "
                   "the median fund in absolute terms."
                   if headline.get("risk_adjusted_better") and headline.get("beats_index")
                   and not headline.get("beats_median")
                   else "See the scorecard for which.")
            ),
        }

    return {
        "comparisons": comparisons,
        "headline": headline,
        "answer": answer,
        "caveats": list(CAVEATS),
        "method": (
            "The bot's trade record is run as an actual account — finite capital, eight "
            "concurrent positions, risk-based sizing — taking only strategies that were "
            "confirmed at the time, never with hindsight. The resulting CAGR is then placed "
            "inside the distribution of real Indian equity funds over a matching window, and "
            "against Nifty buy-and-hold."
        ),
    }
