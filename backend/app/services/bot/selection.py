"""Which entry conditions are allowed to influence the ranking, and by how much.

`quality.py` applies one hand-picked adjustment. This module generalises that
to the whole set of recorded entry conditions, and the generalisation is where
the danger is: with a dozen conditions and five buckets each, something will
look monotone by chance, and a ranking built on it will fit the sample it was
measured on and nothing else.

Four rules keep that shut:

*Everything is decided before the held-out window.* Both the *selection* of
which conditions count and the *size* of each adjustment come from trades
entered before the split. The held-out period is then scored once, with no
further choices. Selecting conditions on the full sample and sizing them on
the pre-split half would still leak — the choice is the leak.

*Selection is itself validated, inside the pre-split data.* In-sample
significance is not enough and this was demonstrated the expensive way: three
conditions passed a rank-correlation gate and a bootstrap at p<0.005 on the
pre-split half, and applying all three took the held-out account from +10.95%
to -1.94%. Two of the three were significant and useless. So the pre-split
window is itself divided: a condition is fitted on its earlier portion and must
still point the same way, with a real gap between its extremes, on the later
portion. Only then is it allowed to size an adjustment — using the whole
pre-split window, since by then it has earned it.

*Correlated conditions are collapsed, not stacked.* Stop width is ATR rescaled;
3-month and 12-month momentum largely agree; distance from the 52-week high and
distance above the 200 DMA both measure extension. Adding all of them would
count one effect three times and hand the ranking a confidence it has not
earned. Each family contributes its single strongest member.

*A graded shape, and a magnitude that is not chance.* These are two different
questions and they need two different tests.

Shape is Spearman rank correlation between bucket order and bucket mean.
Strict monotonicity is too brittle — one adjacent inversion in a middle bucket
throws away a real gradient, and which bucket inverts is itself noise — so a
correlation at or beyond `MIN_RHO` is the gate. A U-shape scores near zero and
is rejected.

Magnitude is a bootstrap on the **extreme buckets' underlying trades**, not on
the bucket means. Permuting five summary points sounds rigorous and is badly
underpowered: five buckets admit only 120 orderings, so a near-perfect
correlation of -0.90 still returns p = 0.18 and every real effect is thrown
away. Resampling the thousands of trades inside the top and bottom buckets
tests the thing that actually matters — whether the ends differ — with the
power the sample size affords.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date
from typing import Callable, Mapping, Sequence

import numpy as np

from . import conditions as cond

logger = logging.getLogger(__name__)

MIN_BUCKET = 400          # per bucket, in the pre-split half only
MIN_SPREAD_R = 0.10       # below this the condition cannot move a ranking meaningfully
BOOTSTRAP = 2000
MAX_P_VALUE = 0.05
# Shape gate. In five buckets this admits one adjacent inversion and rejects
# anything less ordered than that.
MIN_RHO = 0.75
# The pre-split window is split again at this fraction: conditions are fitted
# on the earlier part and must survive the later part before being used at all.
INNER_TRAIN_FRACTION = 0.65
# On the inner validation slice the bar is direction plus a real gap, not the
# same gradient strength. That slice is a couple of years — here 2020-08 to
# 2022-11, which is the post-COVID melt-up and about as unrepresentative as a
# window gets — and requiring |rho| >= MIN_RHO on it rejected every condition
# including ones with a -0.90 gradient in the fit. Sign agreement is the
# standard stability test and is what this asks for.
MIN_INNER_SPREAD_R = 0.05


@dataclass(frozen=True)
class ConditionSpec:
    key: str
    label: str
    family: str           # correlated conditions share a family; one wins per family
    edges: tuple[float, ...]
    names: tuple[str, ...]
    nonzero: bool = True  # treat an exact 0.0 as "not measured" (see conditions._band_nonzero)


# Families: `volatility` (ATR and the stop width derived from it), `momentum`
# (3m and 12m), `extension` (distance from the high, distance above the 200
# DMA), `participation` (signal-bar volume), `liquidity` (turnover).
CANDIDATES: tuple[ConditionSpec, ...] = (
    ConditionSpec("atr_pct_at_entry", "Name volatility (ATR %)", "volatility",
                  cond.ATR_EDGES, cond.ATR_NAMES, nonzero=False),
    ConditionSpec("risk_pct", "Stop width", "volatility",
                  cond.RISK_EDGES, cond.RISK_NAMES, nonzero=False),
    ConditionSpec("ret_63_at_entry", "3-month momentum", "momentum",
                  cond.MOM63_EDGES, cond.MOM63_NAMES),
    ConditionSpec("ret_252_at_entry", "12-month momentum", "momentum",
                  cond.MOM252_EDGES, cond.MOM252_NAMES),
    ConditionSpec("dist_52w_high_at_entry", "Distance below 52-week high", "extension",
                  cond.EXTENSION_EDGES, cond.EXTENSION_NAMES),
    ConditionSpec("above_200dma_pct_at_entry", "Distance above 200 DMA", "extension",
                  cond.TREND_EDGES, cond.TREND_NAMES),
    ConditionSpec("rel_volume_at_entry", "Signal-bar volume", "participation",
                  cond.RELVOL_EDGES, cond.RELVOL_NAMES),
    ConditionSpec("turnover_crore_at_entry", "Daily turnover", "liquidity",
                  cond.TURNOVER_EDGES, cond.TURNOVER_NAMES),
)


@dataclass
class SelectedCondition:
    key: str
    label: str
    family: str
    spread_r: float
    rho: float                    # Spearman correlation of bucket order against bucket mean
    p_value: float                # bootstrap p for the gap between the extreme buckets
    buckets: dict[str, float]     # bucket -> R relative to the book
    trades: int
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


def _bucket_of(spec: ConditionSpec, row: Mapping) -> str | None:
    value = row.get(spec.key)
    band = cond._band_nonzero if spec.nonzero else cond._band
    return band(value, spec.edges, spec.names)


def _measure(spec: ConditionSpec, rows: Sequence[Mapping], book: float) -> SelectedCondition | None:
    """One condition's bucket deltas, or None when it fails the tests."""
    buckets: dict[str, list[float]] = {}
    for row in rows:
        name = _bucket_of(spec, row)
        if name:
            buckets.setdefault(name, []).append(float(row["r_multiple"]))

    ordered = [(n, buckets[n]) for n in spec.names if len(buckets.get(n, [])) >= MIN_BUCKET]
    if len(ordered) < 3:
        return None

    means = [float(np.mean(v)) for _, v in ordered]
    spread = max(means) - min(means)
    if spread < MIN_SPREAD_R:
        return None

    rho = _spearman(means)
    if abs(rho) < MIN_RHO:
        return None
    # The ends carry the claim: if the best and worst buckets are not
    # distinguishable, the gradient between them is decoration.
    low = np.asarray(ordered[0][1], dtype=np.float64)
    high = np.asarray(ordered[-1][1], dtype=np.float64)
    p_value = _bootstrap_p(low, high)
    if p_value > MAX_P_VALUE:
        return None

    direction = "rising" if rho > 0 else "falling"
    return SelectedCondition(
        key=spec.key,
        label=spec.label,
        family=spec.family,
        spread_r=round(spread, 3),
        rho=round(rho, 3),
        p_value=round(p_value, 4),
        buckets={name: round(float(np.mean(values)) - book, 4) for name, values in ordered},
        trades=sum(len(v) for _, v in ordered),
        note=(
            f"Graded {direction} across {len(ordered)} buckets (rank correlation {rho:+.2f}, "
            f"extremes differ with p={p_value:.4f}), spread {spread:.2f}R, measured on "
            f"{sum(len(v) for _, v in ordered):,} pre-split trades."
        ),
    )


def _spearman(values: Sequence[float]) -> float:
    """Rank correlation between bucket position and bucket mean.

    Bucket position is already 1..n by construction, so this reduces to the
    correlation of the means' ranks against that sequence.
    """
    n = len(values)
    if n < 3:
        return 0.0
    order = np.argsort(np.argsort(np.asarray(values, dtype=np.float64)))
    positions = np.arange(n, dtype=np.float64)
    if order.std() == 0 or positions.std() == 0:
        return 0.0
    return float(np.corrcoef(order, positions)[0, 1])


def _bootstrap_p(low: np.ndarray, high: np.ndarray, iterations: int = BOOTSTRAP) -> float:
    """Two-sided p for the difference between the extreme buckets' means.

    Resamples the trades themselves rather than the bucket summaries, which is
    where the power is: R-multiple distributions are capped at -1R below and
    open-ended above, so a t-test's assumptions are wrong in exactly the
    direction that manufactures significance.
    """
    if len(low) < 2 or len(high) < 2:
        return 1.0
    rng = np.random.default_rng(20260920)
    observed = float(low.mean() - high.mean())
    diffs = np.array(
        [
            rng.choice(low, len(low)).mean() - rng.choice(high, len(high)).mean()
            for _ in range(iterations)
        ]
    )
    share = float((diffs <= 0).mean()) if observed > 0 else float((diffs >= 0).mean())
    return min(1.0, 2.0 * share)


def _survives_inner_validation(spec: ConditionSpec, pre: Sequence[Mapping]) -> bool:
    """Fit on the earlier part of the pre-split window, check the later part.

    Uses no held-out data at all: both halves sit before the outer split. What
    it tests is whether the condition is stable across time *within* the data
    it is allowed to see, which is the closest thing to an honest forecast of
    whether it will survive the period it has not seen.
    """
    dated = sorted(pre, key=lambda r: str(r["entry_day"]))
    cut = int(len(dated) * INNER_TRAIN_FRACTION)
    if cut < MIN_BUCKET * 3 or len(dated) - cut < MIN_BUCKET * 2:
        return False
    early, late = dated[:cut], dated[cut:]

    def gradient(rows_in: Sequence[Mapping]) -> tuple[float, float] | None:
        buckets: dict[str, list[float]] = {}
        for row in rows_in:
            name = _bucket_of(spec, row)
            if name:
                buckets.setdefault(name, []).append(float(row["r_multiple"]))
        # A smaller floor inside the validation slice, which is a fraction of
        # the pre-split window and would otherwise have no usable buckets.
        floor = max(50, MIN_BUCKET // 4)
        ordered = [(n, buckets[n]) for n in spec.names if len(buckets.get(n, [])) >= floor]
        if len(ordered) < 3:
            return None
        means = [float(np.mean(v)) for _, v in ordered]
        return _spearman(means), max(means) - min(means)

    first = gradient(early)
    second = gradient(late)
    if first is None or second is None:
        return False
    rho_early, _ = first
    rho_late, spread_late = second
    # The fit itself must show a clear gradient…
    if abs(rho_early) < MIN_RHO:
        return False
    # …and the later slice must agree on direction, with a gap worth acting on.
    return (rho_early * rho_late > 0) and spread_late >= MIN_INNER_SPREAD_R


def fit(rows: Sequence[Mapping], before: date) -> list[SelectedCondition]:
    """Choose and size the adjustments, using only trades entered before `before`."""
    pre = [
        r for r in rows
        if r.get("entry_day") and date.fromisoformat(str(r["entry_day"])) < before
        and r.get("r_multiple") is not None
    ]
    if not pre:
        return []
    book = float(np.mean([float(r["r_multiple"]) for r in pre]))

    measured: list[SelectedCondition] = []
    for spec in CANDIDATES:
        if not _survives_inner_validation(spec, pre):
            logger.info("selection: %s rejected by inner validation", spec.key)
            continue
        found = _measure(spec, pre, book)
        if found:
            measured.append(found)

    # One per family, strongest first — see the module docstring on stacking.
    best_by_family: dict[str, SelectedCondition] = {}
    for item in sorted(measured, key=lambda c: -c.spread_r):
        best_by_family.setdefault(item.family, item)

    chosen = sorted(best_by_family.values(), key=lambda c: -c.spread_r)
    logger.info(
        "selection: %d of %d conditions kept (%s)",
        len(chosen), len(CANDIDATES), ", ".join(c.key for c in chosen),
    )
    return chosen


def adjustment_for(selected: Sequence[SelectedCondition], row: Mapping) -> float:
    """Total R adjustment for one trade, summed across the kept conditions."""
    total = 0.0
    by_key = {spec.key: spec for spec in CANDIDATES}
    for condition in selected:
        spec = by_key.get(condition.key)
        if spec is None:
            continue
        name = _bucket_of(spec, row)
        if name:
            total += condition.buckets.get(name, 0.0)
    return total


def ranker(selected: Sequence[SelectedCondition], expectancy: Mapping) -> Callable[[Mapping], float]:
    """A key function for the portfolio's candidate sort."""

    def edge(row: Mapping) -> float:
        base = expectancy.get((str(row["strategy"]), str(row["regime"])), 0.0)
        return base + adjustment_for(selected, row)

    return edge
