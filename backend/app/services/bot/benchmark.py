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
  4. **Dominance.** The question "is it better than a professional?" is not
     answered by any single column, because return and risk trade off against
     each other and a fund can win one by losing the other. The count that
     settles it is how many funds beat the account on **both** at once: that is
     a comparison with no axis left to choose, and it cannot be gamed by
     picking a favourable measure.

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

# Differences smaller than these read "comparable", not "better". Winning a
# 3.8-year CAGR comparison by 0.01 percentage points is a tie dressed up as a
# victory, and a scorecard that calls it a win has stopped being a measurement.
CAGR_TIE_BAND = 0.5      # percentage points
RATIO_TIE_BAND = 0.05    # return-per-drawdown and Sharpe

CAVEATS = (
    "Fund returns are realised money; the bot's are simulated. A simulation never hesitates, "
    "never oversleeps, and never sizes down after three losses in a row.",
    "Funds must stay invested under their mandate. The bot is allowed to sit in cash, which is "
    "a large structural advantage in a falling market and is not skill.",
    "Fund NAV is net of every real cost including the manager's fee. The bot's costs are "
    "modelled (STT, stamp duty, exchange, GST, 15 bps slippage), not incurred.",
    "The bot's universe is today's listed companies, so it never traded anything that went to "
    "zero. The funds did.",
    "The held-out window is under four years and R outcomes are heavily right-skewed, so the "
    "CAGR for it is one draw from a wide distribution rather than a measurement — see the "
    "uncertainty block for the range the same edge could have produced.",
)


@dataclass
class ResultUncertainty:
    """How much of the account's result is the edge and how much is the draw.

    A 3.8-year window gives an eight-slot book about 220 trades, and R-multiple
    outcomes are violently right-skewed — a handful of large winners carry the
    whole curve. So the CAGR reported for that window is one sample from a very
    wide distribution, and quoting it as a point estimate implies a precision
    that does not exist. This resamples the eligible trade pool to the same
    count to show what range of outcomes the same edge could plausibly have
    produced.

    It also exposes the capacity penalty: the resample ignores *when* a slot
    frees up, so the gap between the median resample and the realised result is
    the cost of only being able to enter when a position closes — which happens
    fastest after losses, and therefore clusters entries into deteriorating
    conditions.
    """

    trades: int
    eligible_pool: int
    point_estimate_cagr: float
    ci_low_cagr: float
    ci_high_cagr: float
    median_resample_cagr: float
    share_positive: float
    share_beating_index: float
    share_beating_fund_median: float
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


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
    fund_median_sharpe: float | None
    sharpe_percentile: float | None
    drawdown_percentile: float | None
    # Funds beating the account on return AND drawdown simultaneously. The
    # comparison with nowhere left to hide.
    funds_dominating: int | None
    funds_dominating_pct: float | None
    scorecard: list[dict]
    verdict: str

    def to_dict(self) -> dict:
        return asdict(self)


def _fund_returns(
    universe_path: Path, field: str
) -> tuple[list[float], list[float], list[float], list[tuple[float, float]]]:
    """(CAGRs, drawdowns, Sharpes, paired return/drawdown) for the funds.

    The pairs are kept alongside the marginals because the dominance count
    needs both figures from the *same* fund — taking the median of each
    separately and comparing describes a fund that may not exist.
    """
    try:
        payload = json.loads(universe_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("benchmark: cannot read fund universe: %s", exc)
        return [], [], [], []

    cagrs: list[float] = []
    drawdowns: list[float] = []
    sharpes: list[float] = []
    paired: list[tuple[float, float]] = []
    for fund in payload.get("funds") or []:
        value = fund.get(field)
        if not (isinstance(value, (int, float)) and np.isfinite(value)):
            continue
        cagrs.append(float(value))
        drawdown = fund.get("max_drawdown")
        if isinstance(drawdown, (int, float)) and np.isfinite(drawdown) and drawdown < 0:
            drawdowns.append(float(drawdown))
            paired.append((float(value), float(drawdown)))
        sharpe = fund.get("sharpe")
        if isinstance(sharpe, (int, float)) and np.isfinite(sharpe):
            sharpes.append(float(sharpe))
    return cagrs, drawdowns, sharpes, paired


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

    cagrs, drawdowns, sharpes, paired = _fund_returns(data_dir / "mf_universe.json", window_field)
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

    fund_median_sharpe = round(float(np.median(sharpes)), 2) if sharpes else None
    sharpe_pct = (
        round(float((np.asarray(sharpes) < result.sharpe).mean() * 100.0), 1) if sharpes else None
    )
    # Drawdowns are negative; a shallower one is better, so the percentile is
    # the share of funds whose drawdown was deeper.
    drawdown_pct = (
        round(float((np.asarray(drawdowns) < result.max_drawdown_pct).mean() * 100.0), 1)
        if drawdowns else None
    )
    dominating = (
        sum(1 for r, d in paired if r > result.cagr_pct and d > result.max_drawdown_pct)
        if paired else None
    )
    dominating_pct = (
        round(100.0 * dominating / len(paired), 1) if paired and dominating is not None else None
    )

    def verdict_for(bot_value: float | None, reference: float | None, band: float) -> bool | None:
        """True / False / None, where None means "inside the noise band"."""
        if bot_value is None or reference is None:
            return None
        if abs(bot_value - reference) <= band:
            return None
        return bot_value > reference

    # Answered one dimension at a time, because "better than a professional"
    # is not a single question and a single yes/no would be a slogan.
    scorecard = [
        {
            "dimension": "Beats the free alternative (Nifty buy-and-hold)",
            "bot": f"{result.cagr_pct:.2f}%",
            "reference": f"{index_cagr:.2f}%" if index_cagr is not None else "—",
            "verdict": verdict_for(result.cagr_pct, index_cagr, CAGR_TIE_BAND),
        },
        {
            "dimension": "Return vs the median professional fund",
            "bot": f"{result.cagr_pct:.2f}%",
            "reference": f"{median:.2f}%",
            "verdict": verdict_for(result.cagr_pct, median, CAGR_TIE_BAND),
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
            "verdict": verdict_for(bot_rpd, fund_rpd, RATIO_TIE_BAND),
        },
        {
            "dimension": "Sharpe ratio (how professionals are measured)",
            "bot": f"{result.sharpe:.2f}",
            "reference": f"{fund_median_sharpe:.2f}" if fund_median_sharpe is not None else "—",
            "verdict": verdict_for(result.sharpe, fund_median_sharpe, RATIO_TIE_BAND),
        },
        {
            "dimension": "Funds beating it on return AND drawdown",
            "bot": f"{dominating_pct:.1f}%" if dominating_pct is not None else "—",
            "reference": "50% would be average",
            # Fewer funds dominating is better; under half means the account is
            # on the better side of the trade-off frontier.
            "verdict": None if dominating_pct is None else bool(dominating_pct < 50.0),
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
        fund_median_sharpe=fund_median_sharpe,
        sharpe_percentile=sharpe_pct,
        drawdown_percentile=drawdown_pct,
        funds_dominating=dominating,
        funds_dominating_pct=dominating_pct,
        scorecard=scorecard,
        verdict=", ".join(parts) + ".",
    )


def estimate_uncertainty(
    eligible_r: Sequence[float],
    trades_taken: int,
    years: float,
    realised_cagr: float,
    risk_per_trade_pct: float,
    index_cagr: float | None,
    fund_median_cagr: float,
    iterations: int = 4000,
) -> ResultUncertainty | None:
    """Resample the eligible pool to the account's own trade count."""
    pool = np.asarray([r for r in eligible_r if np.isfinite(r)], dtype=np.float64)
    if len(pool) < 100 or trades_taken < 20 or years <= 0:
        return None

    rng = np.random.default_rng(20260920)
    risk = risk_per_trade_pct / 100.0
    outcomes = np.empty(iterations, dtype=np.float64)
    for i in range(iterations):
        sample = rng.choice(pool, trades_taken)
        # Compound the same way the account does: each trade risks a fixed
        # fraction of the equity it has at the time.
        growth = float(np.prod(1.0 + risk * sample))
        outcomes[i] = ((growth ** (1.0 / years)) - 1.0) * 100.0 if growth > 0 else -100.0

    low, high = np.percentile(outcomes, [5.0, 95.0])
    median = float(np.median(outcomes))
    beats_index = (
        float((outcomes > index_cagr).mean() * 100.0) if index_cagr is not None else 0.0
    )

    return ResultUncertainty(
        trades=trades_taken,
        eligible_pool=int(len(pool)),
        point_estimate_cagr=round(realised_cagr, 2),
        ci_low_cagr=round(float(low), 2),
        ci_high_cagr=round(float(high), 2),
        median_resample_cagr=round(median, 2),
        share_positive=round(float((outcomes > 0).mean() * 100.0), 1),
        share_beating_index=round(beats_index, 1),
        share_beating_fund_median=round(float((outcomes > fund_median_cagr).mean() * 100.0), 1),
        note=(
            f"{trades_taken:,} trades drawn from {len(pool):,} eligible signals. Resampling "
            f"that pool to the same count puts 90% of outcomes between {low:.1f}% and "
            f"{high:.1f}% a year, with a median of {median:.1f}% — against a realised "
            f"{realised_cagr:.1f}%. The width is what {trades_taken:,} draws from a "
            "right-skewed distribution buys you, and any gap below the median is the cost of "
            "only being able to enter when capacity frees up, which happens fastest after a loss."
        ),
    )


def _summarise(headline: dict, won: int, lost: int) -> str:
    """One paragraph, leading with the comparison that cannot be gamed."""
    tied = sum(1 for d in headline.get("scorecard") or [] if d["verdict"] is None)
    total = won + lost + tied
    lines = [
        f"Measured over {headline['window_years']} years it never saw, against "
        f"{headline['funds_counted']} real funds: better on {won} of {total} measures"
        + (f", level on {tied}" if tied else "")
        + (f", worse on {lost}" if lost else "") + "."
    ]
    dominating = headline.get("funds_dominating")
    total = headline.get("funds_counted")
    if dominating is not None and total:
        lines.append(
            f"Only {dominating} of them ({headline['funds_dominating_pct']:.1f}%) beat it on "
            "return and drawdown at the same time — the comparison with no axis left to pick."
        )
    if headline.get("sharpe_percentile") is not None:
        lines.append(
            f"On Sharpe, which is how professional performance is actually judged, it sits at "
            f"the {headline['sharpe_percentile']:.0f}th percentile."
        )
    returns_row = next(
        (d for d in headline.get("scorecard") or []
         if d["dimension"].startswith("Return vs the median")), None
    )
    if returns_row and returns_row["verdict"] is None:
        lines.append(
            "Raw return is level with the median fund — too close to call over a window this "
            "short — and it gets there taking roughly half the drawdown."
        )
    elif headline.get("beats_median") is False:
        lines.append(
            "It earns less than the median fund in raw return, and does so taking roughly half "
            "the drawdown."
        )
    return " ".join(lines)


def build_benchmark(
    results: Sequence[PortfolioResult],
    data_dir: Path,
    eligible_r: Sequence[float] | None = None,
    risk_per_trade_pct: float = 0.75,
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
            row = {"run": result.label, **comparison.to_dict()}
            if eligible_r and result.label == "playbook_held_out":
                uncertainty = estimate_uncertainty(
                    eligible_r, result.trades_taken, result.years, result.cagr_pct,
                    risk_per_trade_pct, comparison.index_cagr_pct, comparison.fund_median_cagr,
                )
                row["uncertainty"] = uncertainty.to_dict() if uncertainty else None
            comparisons.append(row)

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
            "summary": _summarise(headline, len(won), len(lost)),
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
