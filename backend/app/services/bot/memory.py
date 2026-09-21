"""What the bot has already learned about itself, kept between runs.

Each backtest produces a per-year diagnosis — starved, bad_shots,
under_deployed, ok — and without somewhere to put it that finding is
rediscovered from scratch every time, which is exactly what happened here for
six rounds. This is the notebook: append a run, read back what the recurring
complaint has been, and let the next change be aimed at it.

It deliberately stores **diagnoses, not parameters**. A store that remembered
"0.50% risk worked well" would be a fitted parameter wearing a memory's
clothes, and gotchas 40, 53, 65 and 74 all measured that re-fitting on past
results points the wrong way. What is worth remembering is the *shape* of the
failure — "every lagging year was under-deployed" survived and told us where
to look; "last year 0.35% was best" would not have.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

MEMORY_FILENAME = "bot_memory.json"
MAX_RUNS = 50


@dataclass
class Recollection:
    """What the record says the recurring problem is."""
    runs: int
    dominant_verdict: str | None
    chronic_years: list[int]
    note: str
    best_run: dict | None = None
    regressed: bool = False


@dataclass
class Recommendation:
    """A bounded change the record supports, or none."""
    action: str                 # "hold" | "stand_down_setups" | "widen_capacity"
    setups: list[str]
    reason: str
    evidence_runs: int


# A setup must fail across this many runs before the record is allowed to act
# on it. One bad run is noise; this project has measured that repeatedly.
MIN_RUNS_TO_ACT = 3
CHRONIC_YEARS_TO_ACT = 3


def memory_path(state_dir: Path) -> Path:
    return state_dir / MEMORY_FILENAME


def load(state_dir: Path) -> list[dict]:
    path = memory_path(state_dir)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    return data if isinstance(data, list) else []


def record(
    state_dir: Path,
    diagnosis: Sequence[dict],
    headline: dict,
    config: dict | None = None,
) -> None:
    """Append one run. Oldest entries fall off past `MAX_RUNS`.

    `config` is stored beside the result so runs are comparable to each other
    rather than only to the benchmark — without it the store records that
    something got worse but not what was changed, which is the half that
    makes a record worth keeping.
    """
    runs = load(state_dir)
    runs.append({
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "headline": dict(headline),
        "config": dict(config or {}),
        "diagnosis": [dict(d) for d in diagnosis],
    })
    state_dir.mkdir(parents=True, exist_ok=True)
    memory_path(state_dir).write_text(
        json.dumps(runs[-MAX_RUNS:], indent=2, default=str), encoding="utf-8"
    )


def recommend(state_dir: Path) -> Recommendation:
    """Turn the record into an action, or refuse to.

    The asymmetry from `calibration.py` carries over and is the whole design:
    this can **stand something down and can never promote it**. Every
    offensive use of learning measured in this project lost money (gotchas 59,
    65, 69, 70, 74); the one defensive use that held up was the circuit
    breaker. So the only action available here is to stop doing something that
    has failed across several independent runs.

    It also refuses to act on thin evidence. `MIN_RUNS_TO_ACT` exists because
    a single bad run is noise, and acting on noise is how the reactive
    eligibility experiment lost 3.51% a year.
    """
    runs = load(state_dir)
    if len(runs) < MIN_RUNS_TO_ACT:
        return Recommendation(
            "hold", [], f"only {len(runs)} run(s) on record; {MIN_RUNS_TO_ACT} needed "
                        f"before the record may change anything", len(runs))

    bad_years: dict[int, int] = {}
    for run in runs:
        for row in run.get("diagnosis") or []:
            if str(row.get("verdict")) == "bad_shots":
                bad_years[int(row["year"])] = bad_years.get(int(row["year"]), 0) + 1

    chronic = sorted(y for y, n in bad_years.items() if n >= len(runs))
    if len(chronic) >= CHRONIC_YEARS_TO_ACT:
        return Recommendation(
            "stand_down_setups", [],
            f"years {chronic} were bad_shots in every one of {len(runs)} runs — "
            f"the entry rules, not capacity, and the same years each time",
            len(runs))
    return Recommendation(
        "hold", [], "nothing has failed consistently enough across runs to act on",
        len(runs))


def recall(state_dir: Path) -> Recollection:
    """The complaint that keeps coming back, across every run on record."""
    runs = load(state_dir)
    if not runs:
        return Recollection(0, None, [], "nothing recorded yet", None, False)

    counts: dict[str, int] = {}
    behind: dict[int, int] = {}
    for run in runs:
        for row in run.get("diagnosis") or []:
            verdict = str(row.get("verdict") or "")
            if verdict and verdict != "ok":
                counts[verdict] = counts.get(verdict, 0) + 1
            alpha = row.get("alpha")
            if alpha is not None and float(alpha) < 0:
                year = int(row["year"])
                behind[year] = behind.get(year, 0) + 1

    # Which iteration was best, and whether the latest one went backwards.
    # This is what makes the store a record of *iterations* rather than of
    # one run repeated: the bot can see that a change cost it something.
    scored = [r for r in runs if (r.get("headline") or {}).get("cagr") is not None]
    best = max(scored, key=lambda r: r["headline"]["cagr"]) if scored else None
    regressed = bool(
        best and scored and scored[-1] is not best
        and scored[-1]["headline"]["cagr"] < best["headline"]["cagr"]
    )

    dominant = max(counts, key=counts.get) if counts else None
    # A year that has been behind in every run on record is chronic; one that
    # slipped once is noise, and chasing it is how the last six rounds went.
    chronic = sorted(y for y, n in behind.items() if n == len(runs))
    if dominant is None:
        note = "no recurring complaint — every year on record cleared its benchmark"
    else:
        note = (f"the recurring complaint across {len(runs)} run(s) is '{dominant}'"
                + (f"; chronically behind in {chronic}" if chronic else ""))
    if regressed and best:
        note += (f"; best run was {best['headline']['cagr']:+.2f}% CAGR"
                 f" against the latest {scored[-1]['headline']['cagr']:+.2f}%")
    return Recollection(len(runs), dominant, chronic, note, best, regressed)
