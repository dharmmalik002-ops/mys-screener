"""Rank today's candidates by measured evidence, with the arithmetic visible.

Two numbers bear on a candidate and they come from different studies:

  *Cell expectancy* — what this strategy has returned in this regime over the
  held-out period. The base rate for the decision.

  *Volatility adjustment* — the one entry-time condition that survived the
  bucket study monotone and out-of-sample: quieter names return more R. The
  adjustment is the bucket's average minus the book average, so it is the
  marginal effect of the name's volatility, not its absolute return.

They are added, not multiplied, and both are reported alongside the total. A
single opaque "score" would be the easiest thing to build here and the least
useful: the user cannot audit a number whose parts they cannot see, and a
ranking they cannot audit is one they will either follow blindly or ignore.

Only ATR is applied. The stop-width study shows the same gradient, but the
engine derives every stop from ATR, so adding both would count one effect
twice — see `conditions.MECHANICALLY_LINKED`. Conditions that failed the
monotone or out-of-sample test (breadth, index drawdown, volatility band)
contribute nothing, deliberately.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

# The study this reads. Named so a rebuild that renames the condition fails
# loudly here rather than silently dropping the adjustment to zero.
VOLATILITY_CONDITION = "atr_pct_at_entry"


@dataclass(frozen=True)
class QualityModel:
    """The measured adjustments, loaded from the backtest artifact."""

    book_avg_r: float
    atr_buckets: tuple[tuple[str, float], ...]   # (label, avg_r), in study order
    atr_edges: tuple[float, ...]
    atr_names: tuple[str, ...]
    active: bool
    note: str

    def bucket_for(self, atr_pct: float | None) -> str | None:
        if atr_pct is None:
            return None
        try:
            value = float(atr_pct)
        except (TypeError, ValueError):
            return None
        for edge, name in zip(self.atr_edges, self.atr_names):
            if value < edge:
                return name
        return self.atr_names[-1] if self.atr_names else None

    def adjustment(self, atr_pct: float | None) -> tuple[float, str]:
        """(R adjustment, the bucket it came from). Zero when not measurable."""
        if not self.active:
            return 0.0, ""
        bucket = self.bucket_for(atr_pct)
        if bucket is None:
            return 0.0, ""
        for label, avg_r in self.atr_buckets:
            if label == bucket:
                return round(avg_r - self.book_avg_r, 3), bucket
        return 0.0, bucket


def load_quality_model(artifact: Mapping) -> QualityModel:
    """Read the volatility study out of the artifact, or return an inert model.

    Inert rather than raising: the ranking degrades to cell expectancy alone,
    which is exactly what it was before this module existed. A missing study
    must not take the candidate list down.
    """
    from .conditions import ATR_EDGES, ATR_NAMES

    learning = (artifact or {}).get("learning") or {}
    summary = learning.get("review_summary") or {}
    book = summary.get("book_avg_r")
    studies = learning.get("condition_studies") or []

    study = next((s for s in studies if s.get("condition") == VOLATILITY_CONDITION), None)
    if study is None or book is None:
        return QualityModel(
            book_avg_r=0.0, atr_buckets=(), atr_edges=ATR_EDGES, atr_names=ATR_NAMES,
            active=False,
            note="No volatility study in the artifact — candidates ranked on cell expectancy alone.",
        )

    # Only apply an effect the study itself certified. `monotone` plus a
    # surviving held-out direction is the bar `conditions.py` sets; anything
    # weaker is not allowed to move a ranking.
    verdict = str(study.get("verdict") or "")
    certified = bool(study.get("monotone")) and "holds out-of-sample" in verdict
    buckets = tuple(
        (str(b["label"]), float(b["avg_r"]))
        for b in study.get("buckets") or []
        if b.get("label") is not None and b.get("avg_r") is not None
    )
    if not certified or not buckets:
        return QualityModel(
            book_avg_r=float(book), atr_buckets=(), atr_edges=ATR_EDGES, atr_names=ATR_NAMES,
            active=False,
            note=(
                "The volatility effect did not survive out-of-sample validation in this build, "
                "so it is not applied. Candidates are ranked on cell expectancy alone."
            ),
        )

    return QualityModel(
        book_avg_r=float(book),
        atr_buckets=buckets,
        atr_edges=ATR_EDGES,
        atr_names=ATR_NAMES,
        active=True,
        note=(
            "Ranking adds a volatility adjustment to each candidate's cell expectancy. The "
            "adjustment is that ATR bucket's average R minus the book average, measured over "
            "the full trade population and confirmed out-of-sample."
        ),
    )


def score(model: QualityModel, expected_r: float, atr_pct: float | None) -> dict:
    """The ranking number and every part of it."""
    adjustment, bucket = model.adjustment(atr_pct)
    return {
        "cell_expectancy_r": round(float(expected_r), 3),
        "volatility_adjustment_r": adjustment,
        "volatility_bucket": bucket,
        "edge_score_r": round(float(expected_r) + adjustment, 3),
    }


def rank(model: QualityModel, candidates: Sequence[dict]) -> list[dict]:
    """Attach the scoring to each candidate and order by it."""
    scored: list[dict] = []
    for candidate in candidates:
        parts = score(model, candidate.get("expected_r") or 0.0, candidate.get("atr_pct"))
        scored.append({**candidate, **parts})
    # Turnover breaks ties: between two identical edges, take the one that can
    # actually be filled.
    return sorted(scored, key=lambda c: (-c["edge_score_r"], -(c.get("turnover_crore") or 0.0)))
