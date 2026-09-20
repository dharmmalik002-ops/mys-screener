"""What kind of trade works in what condition — measured across buckets.

The trade review produces one-line lessons like "names with ATR under 2% do
better". A single threshold is the weakest possible form of that claim: pick a
different cutoff and the effect can vanish, and the cutoff that produced the
headline was chosen because it produced the headline.

So every entry condition is measured across its whole range instead, in fixed
buckets, and reported with each bucket's sample. A genuine effect shows up as a
*monotone gradient* — every step in the same direction — and that is far harder
to manufacture than one flattering split. A condition that helps at one cutoff
and reverses at the next is noise, and the bucket table makes that visible
where a single number hides it.

Every bucket is then re-measured on the held-out period. An effect that is
monotone in-sample and gone out-of-sample is the normal outcome of this kind of
search, and is reported as such rather than quietly dropped.

Only conditions knowable **before** entry appear here, for the reason set out
in `review.py`: a bucket built on what the trade did afterwards restates the
outcome and reads as a discovery.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date
from typing import Callable, Mapping, Sequence

import numpy as np

# A bucket thinner than this says nothing; same floor and same reasoning as
# `attribution.MIN_SAMPLE`.
MIN_BUCKET = 150
MIN_BUCKET_OOS = 50


@dataclass
class Bucket:
    label: str
    trades: int
    win_rate: float
    avg_r: float
    payoff: float
    oos_trades: int
    oos_avg_r: float | None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ConditionStudy:
    condition: str
    label: str
    question: str
    buckets: list[Bucket]
    monotone: bool
    spread_r: float           # best bucket minus worst, in-sample
    oos_spread_r: float | None
    verdict: str
    # Set when this study measures the same underlying quantity as another,
    # so the UI can mark it instead of presenting it as fresh evidence.
    duplicates: str | None = None

    def to_dict(self) -> dict:
        out = asdict(self)
        out["buckets"] = [b.to_dict() for b in self.buckets]
        return out


def _stats(returns: np.ndarray) -> tuple[float, float, float]:
    """(win rate, average R, payoff) for one bucket."""
    if not len(returns):
        return 0.0, 0.0, 0.0
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    payoff = 0.0
    if len(wins) and len(losses):
        average_loss = abs(float(losses.mean()))
        payoff = round(float(wins.mean()) / average_loss, 2) if average_loss > 0 else 0.0
    return (
        round(100.0 * float((returns > 0).mean()), 1),
        round(float(returns.mean()), 3),
        payoff,
    )


def study_condition(
    rows: Sequence[Mapping],
    *,
    condition: str,
    label: str,
    question: str,
    bucket_of: Callable[[Mapping], str | None],
    order: Sequence[str],
    split: date,
) -> ConditionStudy | None:
    """Bucket the population on one entry condition and score each bucket."""
    grouped: dict[str, list[float]] = {name: [] for name in order}
    grouped_oos: dict[str, list[float]] = {name: [] for name in order}

    for row in rows:
        name = bucket_of(row)
        if name is None or name not in grouped:
            continue
        r = float(row["r_multiple"])
        grouped[name].append(r)
        if date.fromisoformat(str(row["entry_day"])) >= split:
            grouped_oos[name].append(r)

    buckets: list[Bucket] = []
    for name in order:
        values = np.array(grouped[name], dtype=np.float64)
        if len(values) < MIN_BUCKET:
            continue
        oos = np.array(grouped_oos[name], dtype=np.float64)
        win, avg, payoff = _stats(values)
        buckets.append(
            Bucket(
                label=name,
                trades=int(len(values)),
                win_rate=win,
                avg_r=avg,
                payoff=payoff,
                oos_trades=int(len(oos)),
                oos_avg_r=round(float(oos.mean()), 3) if len(oos) >= MIN_BUCKET_OOS else None,
            )
        )

    if len(buckets) < 2:
        return None

    values = [b.avg_r for b in buckets]
    rising = all(b > a for a, b in zip(values, values[1:]))
    falling = all(b < a for a, b in zip(values, values[1:]))
    monotone = rising or falling
    spread = round(max(values) - min(values), 3)

    oos_values = [b.oos_avg_r for b in buckets if b.oos_avg_r is not None]
    oos_spread = round(max(oos_values) - min(oos_values), 3) if len(oos_values) >= 2 else None

    # The held-out check: does the ordering survive? Comparing the extremes
    # rather than the full ranking, because with three or four buckets a single
    # reshuffle in the middle is not evidence of anything either way.
    held = None
    if monotone and len(oos_values) >= 2:
        first = buckets[0].oos_avg_r
        last = buckets[-1].oos_avg_r
        if first is not None and last is not None:
            held = (last > first) if rising else (last < first)

    if not monotone:
        verdict = (
            f"No consistent gradient across buckets (spread {spread:.2f}R). Any single cutoff "
            "here would be a cherry-pick."
        )
    elif held is False:
        verdict = (
            f"Monotone in-sample (spread {spread:.2f}R) but the direction reverses out-of-sample. "
            "Treat as noise."
        )
    elif held is None:
        verdict = (
            f"Monotone in-sample (spread {spread:.2f}R) but too few held-out trades per bucket "
            "to confirm."
        )
    else:
        verdict = (
            f"Monotone in-sample (spread {spread:.2f}R) and the direction holds out-of-sample "
            f"(spread {oos_spread:.2f}R). This is a real entry-time effect."
        )

    return ConditionStudy(
        condition=condition,
        label=label,
        question=question,
        buckets=buckets,
        monotone=monotone,
        spread_r=spread,
        oos_spread_r=oos_spread,
        verdict=verdict,
    )


def _band(value, edges: Sequence[float], names: Sequence[str]) -> str | None:
    """Place a number into a named band; None when it is missing."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    for edge, name in zip(edges, names):
        if number < edge:
            return name
    return names[-1]


ATR_EDGES = (2.0, 3.0, 4.5, 6.0)
ATR_NAMES = ("under 2%", "2-3%", "3-4.5%", "4.5-6%", "over 6%")

BREADTH_EDGES = (30.0, 45.0, 60.0, 75.0)
BREADTH_NAMES = ("under 30%", "30-45%", "45-60%", "60-75%", "over 75%")

DRAWDOWN_EDGES = (-15.0, -8.0, -3.0)
DRAWDOWN_NAMES = ("over 15% off high", "8-15% off", "3-8% off", "within 3% of high")

RISK_EDGES = (3.0, 5.0, 8.0)
RISK_NAMES = ("under 3%", "3-5%", "5-8%", "over 8%")

VOL_ORDER = ("calm", "normal", "stressed")


# Conditions that are not independent measurements of each other. The engine
# sets every stop at `stop_atr_mult x ATR`, so stop width IS the name's
# volatility rescaled — the two studies below must agree, and their agreement
# is arithmetic, not corroboration. Left in as separate rows because the stop
# is what a trader actually sets, but flagged so two views of one effect are
# never read as two effects.
MECHANICALLY_LINKED: tuple[tuple[str, str, str], ...] = (
    (
        "risk_pct",
        "atr_pct_at_entry",
        "Stop width is set as a multiple of ATR, so this is the same effect as the "
        "volatility study above, rescaled. Treat the two as one finding.",
    ),
)


def study_all(rows: Sequence[Mapping], split: date) -> list[ConditionStudy]:
    """Every entry condition the ledger records, each across its full range."""
    studies: list[ConditionStudy | None] = [
        study_condition(
            rows,
            condition="atr_pct_at_entry",
            label="Volatility of the name (ATR as % of price)",
            question="Does buying quieter stocks pay better than buying jumpy ones?",
            bucket_of=lambda r: _band(r.get("atr_pct_at_entry"), ATR_EDGES, ATR_NAMES),
            order=ATR_NAMES,
            split=split,
        ),
        study_condition(
            rows,
            condition="risk_pct",
            label="Stop width at entry (% below entry)",
            question="Do tight stops or wide stops produce more R?",
            bucket_of=lambda r: _band(r.get("risk_pct"), RISK_EDGES, RISK_NAMES),
            order=RISK_NAMES,
            split=split,
        ),
        study_condition(
            rows,
            condition="breadth_above_200dma",
            label="Market breadth at entry (% of universe above its 200 DMA)",
            question="How much does broad participation matter to a single trade?",
            bucket_of=lambda r: _band(r.get("breadth_above_200dma"), BREADTH_EDGES, BREADTH_NAMES),
            order=BREADTH_NAMES,
            split=split,
        ),
        study_condition(
            rows,
            condition="pct_from_52w_high",
            label="Where the index sat at entry (% below its 52-week high)",
            question="Is it better to buy when the index is at highs or off them?",
            bucket_of=lambda r: _band(r.get("pct_from_52w_high"), DRAWDOWN_EDGES, DRAWDOWN_NAMES),
            order=DRAWDOWN_NAMES,
            split=split,
        ),
        study_condition(
            rows,
            condition="volatility_band",
            label="Index volatility band at entry",
            question="Do calm tapes pay better than stressed ones?",
            bucket_of=lambda r: str(r.get("volatility_band") or "") or None,
            order=VOL_ORDER,
            split=split,
        ),
    ]
    found = [s for s in studies if s is not None]
    by_id = {s.condition: s for s in found}
    for dependent, driver, note in MECHANICALLY_LINKED:
        study = by_id.get(dependent)
        if study is not None and driver in by_id:
            study.duplicates = driver
            study.verdict = f"{study.verdict} {note}"

    # Real effects first, then by how much they separate.
    return sorted(found, key=lambda s: (not s.monotone, -s.spread_r))
