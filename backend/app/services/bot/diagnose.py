"""Why did the book lag the index this year? Measured, not guessed.

Six rounds of "fix the bad years" were spent hand-inspecting tables and
guessing at causes, and the first guess was wrong every time — the weak years
turned out to be an accounting artefact (gotcha 76), and the fix that looked
obvious (letting the rule evolve, scaling out of winners) measured worse. This
module does that inspection mechanically so the answer arrives with its
evidence attached.

For each year it separates the three things that can go wrong, because they
have different remedies and are easy to confuse:

  * **No shots.** The filter passed almost nothing, so the book sat in cash
    while the index ran. 2009 is the case in point: 68 signals all year, and
    no exit rule or sizing change can fix a year the bot barely traded.
  * **Bad shots.** Plenty of trades, poor average R. The entry rules picked
    the wrong names.
  * **Good shots, small book.** Decent R but little deployed — capital was
    tied up, or positions were too small to matter.

`starved` is the one that matters most here and is the easiest to misread as
poor stock picking, because a year with 23 trades and a year with 731 produce
equally unimpressive lines in a returns table.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Mapping, Sequence

import numpy as np

MIN_TRADES_FOR_A_VERDICT = 40
WEAK_AVG_R = 0.0


@dataclass
class YearDiagnosis:
    year: int
    signals: int
    accepted: int
    acceptance_pct: float
    avg_r: float
    bot_return: float | None
    index_return: float | None
    alpha: float | None
    verdict: str
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


def diagnose(
    all_signals: Sequence[Mapping],
    accepted: Sequence[Mapping],
    bot_yearly: Mapping[int, float],
    index_yearly: Mapping[int, float],
) -> list[YearDiagnosis]:
    """One verdict per year, with the number that justifies it."""
    years = sorted({int(str(t["entry_day"])[:4]) for t in all_signals})
    out: list[YearDiagnosis] = []
    for year in years:
        sig = [t for t in all_signals if int(str(t["entry_day"])[:4]) == year]
        acc = [t for t in accepted if int(str(t["entry_day"])[:4]) == year]
        rs = np.array([float(t["r_multiple"]) for t in acc]) if acc else np.array([])
        avg_r = float(rs.mean()) if len(rs) else 0.0
        bot = bot_yearly.get(year)
        idx = index_yearly.get(year)
        alpha = None if bot is None or idx is None else bot - idx

        if len(acc) < MIN_TRADES_FOR_A_VERDICT:
            verdict = "starved"
            note = (f"only {len(acc)} trades from {len(sig)} signals — the book was "
                    f"mostly in cash, so nothing about entries or exits explains this year")
        elif alpha is None:
            # No index for this year. Say so rather than guessing a verdict
            # from the bot's own return, which cannot distinguish a good year
            # from a year the whole market rose.
            verdict = "no_benchmark"
            note = f"{len(acc)} trades at {avg_r:+.2f}R — no index return for this year"
        elif alpha >= 0:
            verdict = "ok"
            note = f"beat the index by {alpha:+.1f}pp on {len(acc)} trades"
        elif avg_r <= WEAK_AVG_R:
            verdict = "bad_shots"
            note = (f"{len(acc)} trades at {avg_r:+.2f}R — enough shots, wrong names; "
                    f"this is an entry-rule problem")
        else:
            verdict = "under_deployed"
            note = (f"{len(acc)} trades at a healthy {avg_r:+.2f}R but still "
                    f"{alpha:+.1f}pp behind — capital, not selection")
        out.append(YearDiagnosis(
            year=year, signals=len(sig), accepted=len(acc),
            acceptance_pct=round(100.0 * len(acc) / len(sig), 1) if sig else 0.0,
            avg_r=round(avg_r, 3),
            bot_return=None if bot is None else round(bot, 2),
            index_return=None if idx is None else round(idx, 2),
            alpha=None if alpha is None else round(alpha, 2),
            verdict=verdict, note=note,
        ))
    return out


def summarise(rows: Sequence[YearDiagnosis]) -> dict:
    """What the bot should actually work on, ranked by how much it cost."""
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.verdict] = counts.get(r.verdict, 0) + 1
    losing = [r for r in rows if r.alpha is not None and r.alpha < 0]
    worst = sorted(losing, key=lambda r: r.alpha)[:3]
    return {
        "verdict_counts": counts,
        "years_behind": len(losing),
        "years_total": sum(1 for r in rows if r.alpha is not None),
        "worst": [r.to_dict() for r in worst],
        "starved_years": [r.year for r in rows if r.verdict == "starved"],
        "bad_shot_years": [r.year for r in rows if r.verdict == "bad_shots"],
    }
