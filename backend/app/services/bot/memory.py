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
