"""Does the outside world change what the strategies earn, beyond the regime?

The temptation with macro data is to narrate it: crude is up, so avoid trades.
That is a story, not a finding, and it is unfalsifiable as usually told. This
module asks the question in a form that can come back "no".

For each external series (dollar, crude, US yields, global volatility, and the
rest, declared in `context_series.py` along with which direction is the
headwind), every session is tagged headwind / neutral / tailwind from that
series' own 63-day move measured against its trailing distribution. Trades are
then split on that tag **within the same regime**, so the comparison is not
just rediscovering that bull markets pay better than bear markets. If the two
groups differ by more than noise, the series carries information the regime
label does not already contain. If they do not, it is decoration, and it is
reported as decoration.

The headwind direction for every series was written down before any of this
ran. Without that, a series that predicts returns in either direction is
guaranteed to "work", because the direction gets chosen after the fact.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path

import numpy as np

from . import indicators as ind
from .attribution import MIN_SAMPLE
from .context_series import ContextSeries, external
from .engine import Trade
from .history import read_bars

LOOKBACK = 63              # a quarter — slow enough to be a condition, not a blip
PCTILE_WINDOW = 504        # judged against its own last two years
HEADWIND_PCTILE = 70.0     # move this extreme, in the pressuring direction
TAILWIND_PCTILE = 30.0
MIN_SPLIT_SAMPLE = 50      # per side; a split needs both sides to be real


@dataclass
class MacroFinding:
    series: str
    label: str
    note: str
    headwind_direction: str
    trades_headwind: int
    trades_tailwind: int
    avg_r_headwind: float
    avg_r_tailwind: float
    difference: float          # tailwind minus headwind
    p_value: float
    material: bool
    verdict: str

    def to_dict(self) -> dict:
        return asdict(self)


def tag_sessions(data_dir: Path, spec: ContextSeries, sessions: list[date]) -> dict[date, str]:
    """headwind / tailwind / neutral for each session, from this series alone."""
    bars = read_bars(data_dir, spec.key)
    if bars is None or len(bars) < PCTILE_WINDOW:
        return {}

    change = ind.pct_change(bars.close, LOOKBACK)
    # Rank the move inside its own history so "crude is high" means high for
    # crude, not high in absolute rupees.
    rank = ind.rolling_percentile_rank(np.nan_to_num(change, nan=0.0), PCTILE_WINDOW)

    by_day: dict[date, str] = {}
    for i, day in enumerate(bars.dates):
        value = rank[i]
        if not np.isfinite(value):
            continue
        # headwind_when == +1 means a RISING series is the headwind, so a high
        # percentile is pressure. When it is -1 the sense flips.
        pressure = value if spec.headwind_when > 0 else 100.0 - value
        if pressure >= HEADWIND_PCTILE:
            by_day[day] = "headwind"
        elif pressure <= TAILWIND_PCTILE:
            by_day[day] = "tailwind"
        else:
            by_day[day] = "neutral"

    # Series like the S&P trade on days the NSE does not and vice versa. Carry
    # the last known state forward across gaps: a macro condition does not stop
    # existing because one market was shut.
    filled: dict[date, str] = {}
    last = "neutral"
    for day in sessions:
        last = by_day.get(day, last)
        filled[day] = last
    return filled


def measure_macro_edge(
    data_dir: Path,
    trades: list[Trade],
    sessions: list[date],
    regime_filter: str | None = "bull_strong",
) -> list[MacroFinding]:
    """Split trades on each external series and test whether it matters.

    `regime_filter` holds the regime constant so the split measures the macro
    series and not the regime it correlates with. It defaults to the regime the
    book actually trades in — a macro effect that only shows up in regimes the
    playbook stands down in cannot change any decision.
    """
    pool = [t for t in trades if t.resolved and (regime_filter is None or t.regime == regime_filter)]
    if len(pool) < MIN_SAMPLE * 2:
        return []

    findings: list[MacroFinding] = []
    for spec in external():
        tags = tag_sessions(data_dir, spec, sessions)
        if not tags:
            continue

        head = np.array([t.r_multiple for t in pool if tags.get(t.entry_day) == "headwind"])
        tail = np.array([t.r_multiple for t in pool if tags.get(t.entry_day) == "tailwind"])
        if len(head) < MIN_SPLIT_SAMPLE or len(tail) < MIN_SPLIT_SAMPLE:
            continue

        # Bootstrap the difference directly rather than comparing two intervals:
        # overlapping confidence intervals do not mean the difference is
        # insignificant, and non-overlapping ones do not mean it is.
        difference = float(tail.mean() - head.mean())
        _lo, _hi, p = _difference_bootstrap(tail, head)

        material = p < 0.05 and abs(difference) >= 0.10
        if material:
            better = "tailwind" if difference > 0 else "headwind"
            verdict = (
                f"Matters. Trades taken when {spec.label} is a {better} returned "
                f"{abs(difference):.2f}R more per trade, holding the regime constant."
            )
        else:
            verdict = (
                f"No material effect ({difference:+.2f}R difference, p={p:.2f}). "
                f"{spec.label} does not tell the book anything the regime label has not already said."
            )

        findings.append(
            MacroFinding(
                series=spec.key,
                label=spec.label,
                note=spec.note,
                headwind_direction="rising" if spec.headwind_when > 0 else "falling",
                trades_headwind=len(head),
                trades_tailwind=len(tail),
                avg_r_headwind=round(float(head.mean()), 3),
                avg_r_tailwind=round(float(tail.mean()), 3),
                difference=round(difference, 3),
                p_value=round(p, 4),
                material=material,
                verdict=verdict,
            )
        )

    return sorted(findings, key=lambda f: (not f.material, -abs(f.difference)))


def _difference_bootstrap(a: np.ndarray, b: np.ndarray, iterations: int = 2000) -> tuple[float, float, float]:
    """Two-sided p for mean(a) - mean(b) by resampling each group independently."""
    rng = np.random.default_rng(20260919)
    ia = rng.integers(0, len(a), size=(iterations, len(a)))
    ib = rng.integers(0, len(b), size=(iterations, len(b)))
    diffs = a[ia].mean(axis=1) - b[ib].mean(axis=1)
    observed = float(a.mean() - b.mean())
    # Fraction of resamples on the opposite side of zero from the observation.
    p = float((diffs <= 0).mean()) if observed > 0 else float((diffs >= 0).mean())
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    return float(lo), float(hi), min(1.0, 2.0 * p)
