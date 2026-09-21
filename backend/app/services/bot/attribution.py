"""Strategy x regime performance, with the statistics done properly.

This is the module the whole project exists to produce, and it is also the
easiest place in the project to fool yourself. Three guards are built in:

*Sample gating.* A cell with nine trades in it has no opinion. `MIN_SAMPLE`
drops thin cells rather than printing a confident number over them — the same
rule `study_coach.py` already applies to the drill log, for the same reason: a
model handed "+1.4R in bear markets (n=6)" will repeat it as fact.

*Multiple testing.* Ten strategies across six regimes is sixty cells. Testing
sixty cells at p<0.05 yields three winners by chance alone even if every
strategy is worthless, and those three will be the ones with the prettiest
numbers. Benjamini-Hochberg controls the false discovery rate across the whole
family, and cells that survive it are marked `significant`; cells that do not
are still shown, with the flag false, because hiding them would itself be a
selection effect.

*Out-of-sample validation.* Every cell is computed twice — on the earlier
portion of history and on the held-out later portion. A cell that is strong in
both is a candidate. A cell that is strong in-sample and dead out-of-sample is
the single most common output of this kind of study and is reported as such,
not quietly averaged into one flattering number.

Bootstrap rather than a t-test: R-multiple distributions are violently
non-normal — capped at -1R on the downside, open-ended on the upside — and the
t-test's assumptions are wrong in exactly the direction that manufactures
significance.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import date

import numpy as np

from .engine import Trade

# Below this a cell is not reported as an edge. Chosen before any results were
# seen; raising it after looking is how a threshold becomes a fitted parameter.
MIN_SAMPLE = 30
# Sample floor for the out-of-sample half specifically, which is smaller.
MIN_SAMPLE_OOS = 15
BOOTSTRAP_ITERATIONS = 2000
FDR_ALPHA = 0.10          # false discovery rate across the whole cell family
RNG_SEED = 20260919       # fixed so a rerun reproduces the same intervals


@dataclass
class CellStats:
    """Performance of one strategy in one regime."""

    strategy: str
    regime: str
    trades: int
    win_rate: float
    avg_r: float
    median_r: float
    total_r: float
    profit_factor: float
    avg_win_r: float
    avg_loss_r: float
    avg_hold: float
    expectancy_pct: float          # average net % per trade, after costs
    r_ci_low: float = 0.0          # bootstrap 90% interval on mean R
    r_ci_high: float = 0.0
    p_value: float = 1.0           # P(mean R <= 0) under the bootstrap
    significant: bool = False      # survives Benjamini-Hochberg at FDR_ALPHA
    reportable: bool = False       # met MIN_SAMPLE

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ValidatedCell:
    """One cell seen in-sample and out-of-sample side by side."""

    strategy: str
    regime: str
    in_sample: CellStats | None
    out_sample: CellStats | None
    verdict: str          # confirmed | decayed | insufficient | negative
    note: str

    def to_dict(self) -> dict:
        return {
            "strategy": self.strategy,
            "regime": self.regime,
            "in_sample": self.in_sample.to_dict() if self.in_sample else None,
            "out_sample": self.out_sample.to_dict() if self.out_sample else None,
            "verdict": self.verdict,
            "note": self.note,
        }


def _bootstrap_mean_ci(values: np.ndarray, iterations: int = BOOTSTRAP_ITERATIONS) -> tuple[float, float, float]:
    """(ci_low, ci_high, p_value) for the mean, by resampling with replacement.

    `p_value` is the fraction of bootstrap means at or below zero — a one-sided
    read of "could this edge be nothing?". One-sided because a strategy that
    loses money is not an interesting finding to protect against; the question
    is only whether the positive result is real.
    """
    n = len(values)
    if n < 2:
        return 0.0, 0.0, 1.0
    rng = np.random.default_rng(RNG_SEED)
    idx = rng.integers(0, n, size=(iterations, n))
    means = values[idx].mean(axis=1)
    lo, hi = np.percentile(means, [5.0, 95.0])
    p = float((means <= 0.0).mean())
    return float(lo), float(hi), p


def summarise_cell(strategy: str, regime: str, trades: list[Trade]) -> CellStats:
    """Aggregate one cell. Open positions are excluded from every statistic."""
    resolved = [t for t in trades if t.resolved]
    n = len(resolved)
    if n == 0:
        return CellStats(strategy, regime, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    r = np.array([t.r_multiple for t in resolved], dtype=np.float64)
    wins = r[r > 0]
    losses = r[r <= 0]
    gross_win = float(wins.sum())
    gross_loss = float(-losses.sum())

    stats = CellStats(
        strategy=strategy,
        regime=regime,
        trades=n,
        win_rate=round(100.0 * len(wins) / n, 1),
        avg_r=round(float(r.mean()), 3),
        median_r=round(float(np.median(r)), 3),
        total_r=round(float(r.sum()), 1),
        profit_factor=round(gross_win / gross_loss, 2) if gross_loss > 0 else float("inf"),
        avg_win_r=round(float(wins.mean()), 2) if len(wins) else 0.0,
        avg_loss_r=round(float(losses.mean()), 2) if len(losses) else 0.0,
        avg_hold=round(float(np.mean([t.sessions_held for t in resolved])), 1),
        expectancy_pct=round(float(np.mean([t.net_pct for t in resolved])), 2),
        reportable=n >= MIN_SAMPLE,
    )
    if n >= MIN_SAMPLE:
        lo, hi, p = _bootstrap_mean_ci(r)
        stats.r_ci_low, stats.r_ci_high, stats.p_value = round(lo, 3), round(hi, 3), round(p, 4)
    return stats


def apply_fdr(cells: list[CellStats], alpha: float = FDR_ALPHA) -> None:
    """Mark cells significant under Benjamini-Hochberg, in place.

    Only reportable cells enter the family — including thin cells would inflate
    the correction and bury the real findings under noise that was never
    testable in the first place.
    """
    family = [c for c in cells if c.reportable]
    if not family:
        return
    ordered = sorted(family, key=lambda c: c.p_value)
    m = len(ordered)
    cutoff_rank = 0
    for rank, cell in enumerate(ordered, start=1):
        if cell.p_value <= alpha * rank / m:
            cutoff_rank = rank
    for rank, cell in enumerate(ordered, start=1):
        cell.significant = rank <= cutoff_rank


def split_date(trades: list[Trade], train_fraction: float = 0.6) -> date | None:
    """The boundary between the in-sample and held-out periods.

    Split on *time*, never at random. A random split puts trades from the same
    week on both sides, and since those trades share a market environment the
    held-out set is not held out at all — it is the same regime wearing a
    disguise, and everything validates.
    """
    if not trades:
        return None
    days = sorted(t.entry_day for t in trades)
    return days[int(len(days) * train_fraction)]


def build_matrix(trades: list[Trade], regimes: tuple[str, ...]) -> list[CellStats]:
    """The full strategy x regime grid over every trade supplied."""
    grouped: dict[tuple[str, str], list[Trade]] = defaultdict(list)
    for t in trades:
        grouped[(t.strategy, t.regime)].append(t)
    cells = [summarise_cell(strategy, regime, group) for (strategy, regime), group in grouped.items()]
    apply_fdr(cells)
    return sorted(cells, key=lambda c: (-c.avg_r if c.reportable else 1e9, c.strategy))


def validate(trades: list[Trade], regimes: tuple[str, ...], train_fraction: float = 0.6) -> list[ValidatedCell]:
    """Walk-forward: build the matrix on early history, check it on later history."""
    boundary = split_date(trades, train_fraction)
    if boundary is None:
        return []

    early = [t for t in trades if t.entry_day < boundary]
    late = [t for t in trades if t.entry_day >= boundary]
    in_cells = {(c.strategy, c.regime): c for c in build_matrix(early, regimes)}
    out_cells = {(c.strategy, c.regime): c for c in build_matrix(late, regimes)}

    results: list[ValidatedCell] = []
    for key in sorted(set(in_cells) | set(out_cells)):
        strategy, regime = key
        ins, outs = in_cells.get(key), out_cells.get(key)

        if ins is None or not ins.reportable:
            verdict, note = "insufficient", "Too few trades in the earlier period to form a view."
        elif ins.avg_r <= 0:
            verdict, note = "negative", "No edge in the earlier period; nothing to validate."
        elif outs is None or outs.trades < MIN_SAMPLE_OOS:
            verdict, note = "insufficient", "Not enough held-out trades to confirm or refute."
        elif outs.avg_r > 0 and ins.significant:
            verdict = "confirmed"
            note = (
                f"Positive in both periods ({ins.avg_r:+.2f}R in-sample on {ins.trades} trades, "
                f"{outs.avg_r:+.2f}R out-of-sample on {outs.trades})."
            )
        elif outs.avg_r > 0:
            verdict = "confirmed_weak"
            note = (
                f"Positive in both periods but the in-sample edge did not survive multiple-testing "
                f"correction ({ins.avg_r:+.2f}R then {outs.avg_r:+.2f}R)."
            )
        else:
            verdict = "decayed"
            note = (
                f"Worked in the earlier period ({ins.avg_r:+.2f}R) and did not hold up "
                f"({outs.avg_r:+.2f}R on {outs.trades} held-out trades)."
            )

        results.append(ValidatedCell(strategy, regime, ins, outs, verdict, note))

    priority = {"confirmed": 0, "confirmed_weak": 1, "decayed": 2, "negative": 3, "insufficient": 4}
    return sorted(
        results,
        key=lambda v: (priority.get(v.verdict, 9), -(v.out_sample.avg_r if v.out_sample else -9)),
    )


def strategy_totals(trades: list[Trade]) -> list[CellStats]:
    """Each strategy across all regimes — the 'does this work at all' read."""
    grouped: dict[str, list[Trade]] = defaultdict(list)
    for t in trades:
        grouped[t.strategy].append(t)
    cells = [summarise_cell(s, "ALL", group) for s, group in grouped.items()]
    apply_fdr(cells)
    return sorted(cells, key=lambda c: -c.avg_r)
