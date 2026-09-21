"""The bot proposing and validating its own changes.

`memory.recommend()` can only stand something down, and that is correct for
anything learned from *trade outcomes* — every offensive use of that measured
negative here. But it is not the only way a system can improve itself, and
demotion alone is not improvement.

This is the other way: the bot proposes changes to its own configuration,
scores each on a training window, and **adopts one only if it also holds on a
window it did not tune on**. That is the protocol every result in this project
was produced by hand, written down so it can run without a human.

The validation is the whole thing, because config selection is actively
dangerous here: gotcha 53 measured the rank correlation between a
configuration's pre-split score and its held-out return at **-0.70**. Choosing
on the training window alone points the *wrong way*. So a candidate must

  1. beat the incumbent on the training window, AND
  2. beat it on the held-out window as well,

and `adopt()` returns nothing when no candidate does both. "Change nothing" is
the expected output, not a failure — a tuner that always finds an improvement
is fitting noise, which is exactly what this guard exists to catch.

One proposal at a time, and a bounded set of them. An unbounded search over a
20-year sample finds a winner by chance; with a handful of candidates declared
in advance, surviving both windows means something.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Callable, Mapping, Sequence


@dataclass(frozen=True)
class Candidate:
    """One proposed change, as a name and the overrides it applies."""
    name: str
    overrides: Mapping[str, float]


@dataclass
class TrialResult:
    name: str
    train_score: float
    holdout_score: float
    adopted: bool
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


# Declared in advance. Each is a knob a trader would recognise, not a point on
# a grid: how far the trail runs, how long a position may be held, how tight
# the stop is pulled, and how much of a winner is added to.
DEFAULT_CANDIDATES: tuple[Candidate, ...] = (
    Candidate("trail_wider", {"trail_atr_mult": 10.0}),
    Candidate("trail_tighter", {"trail_atr_mult": 6.0}),
    Candidate("hold_longer", {"max_hold_sessions": 750.0}),
    Candidate("stop_looser", {"max_stop_pct": 4.5}),
    Candidate("stop_tighter", {"max_stop_pct": 3.0}),
    Candidate("pyramid_bigger", {"pyramid_scale": 0.50}),
)

MIN_EDGE = 0.25      # percentage points of CAGR; smaller is a tie, not a win


def evaluate(
    candidates: Sequence[Candidate],
    score: Callable[[Mapping[str, float], bool], float],
    baseline: Mapping[str, float],
) -> list[TrialResult]:
    """Score every candidate on both windows against the incumbent.

    `score(overrides, holdout)` returns a CAGR. It is injected so this module
    stays free of the backtest machinery and can be tested against arithmetic
    rather than against a 20-year replay.
    """
    base_train = score(baseline, False)
    base_hold = score(baseline, True)

    out: list[TrialResult] = []
    for cand in candidates:
        merged = {**baseline, **cand.overrides}
        train = score(merged, False)
        hold = score(merged, True)
        wins_train = train - base_train > MIN_EDGE
        wins_hold = hold - base_hold > MIN_EDGE
        if wins_train and wins_hold:
            note = (f"better on both windows: train {train - base_train:+.2f}pp, "
                    f"held-out {hold - base_hold:+.2f}pp")
        elif wins_train:
            note = (f"better in training ({train - base_train:+.2f}pp) and NOT held out "
                    f"({hold - base_hold:+.2f}pp) — the -0.70 trap, rejected")
        else:
            note = f"no edge in training ({train - base_train:+.2f}pp)"
        out.append(TrialResult(cand.name, round(train, 3), round(hold, 3),
                               wins_train and wins_hold, note))
    return out


def adopt(trials: Sequence[TrialResult]) -> TrialResult | None:
    """The best candidate that survived BOTH windows, or None.

    Ranked on the held-out score, never on the training score — the training
    number has already done its job by qualifying the candidate, and letting
    it pick the winner as well reintroduces exactly the bias the second window
    exists to remove.
    """
    survivors = [t for t in trials if t.adopted]
    if not survivors:
        return None
    return max(survivors, key=lambda t: t.holdout_score)
