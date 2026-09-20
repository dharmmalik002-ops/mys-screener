"""What actually happened in a trade, judged from its own path — not its P&L.

The distinction this module exists to make: **outcome is not decision quality**.
A trade that made 2R after sitting at -0.95R for a week was a near-miss that
paid, and counting it as a success teaches the wrong lesson. A trade that was
up 2.5R and stopped out flat lost nothing on the books and was a genuine error.
Sorting on `r_multiple` alone can never see either.

So every closed trade gets three readings taken from MAE, MFE and the exit,
all of which the engine already records:

  *Stop quality* — how close it came to being stopped, and whether the stop was
  where the trade needed it. `survived_near_stop` is the flag that matters:
  win or lose, the position was one ordinary session from dead.

  *Exit quality* — how much of the best excursion was actually kept. A trail
  that gives back 60% of MFE on every winner is a structural leak, invisible in
  the win rate and plainly visible here.

  *Verdict* — the two together, which is what makes the aggregate useful:
  "62% of your wins in choppy markets are round-trips you happened to escape"
  is a sentence about process, and process is the only part that can be fixed.

Every judgement here is arithmetic on recorded numbers. Nothing is inferred,
nothing is generated, and the thresholds are named constants declared once so
a shift in what counts as "clean" is a visible edit rather than a drift.

**Tags come in two kinds and must never be mixed**, because one kind can teach
and the other only flatters. An *outcome* tag describes what the trade did —
`stop_hit`, `survived_near_stop`, `profit_handed_back`. Averaging R over an
outcome tag is circular: `survived_near_stop` cannot include a stopped-out
trade, so it "discovers" +1.88R and means nothing. An *entry* tag describes
what was on the screen before the position existed — the volatility band,
breadth, the name's ATR, how hostile the external tape was. Only entry tags can
change a decision, so only entry tags are allowed to produce a lesson. Outcome
tags stay, because the verdict breakdown and the structural read are built from
them, but they are quarantined out of `lessons` by `ENTRY_TAGS`.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Mapping

# --- thresholds, in R ------------------------------------------------------
SHALLOW_MAE = -0.35        # never really in trouble
DEEP_MAE = -0.75           # one bad session from a stop
ROUND_TRIP_MFE = 1.0       # was up this much and still lost
GAVE_BACK_FRACTION = 0.45  # kept less than this share of the best excursion
BIG_LEFT_ON_TABLE = 1.5    # R surrendered between MFE and the exit
SCRATCH_BAND = 0.25        # |R| below this is neither win nor loss in practice

VERDICTS = (
    "clean_win", "gave_back_win", "lucky_win",
    "round_trip_loss", "clean_loss", "slow_bleed", "gap_loss",
)

VERDICT_LABELS = {
    "clean_win": "Clean win",
    "gave_back_win": "Won, gave back most of it",
    "lucky_win": "Won after nearly stopping out",
    "round_trip_loss": "Was well in profit, lost it all",
    "clean_loss": "Clean loss — wrong fast, cheap",
    "slow_bleed": "Went nowhere, timed out",
    "gap_loss": "Gapped through the stop",
}

VERDICT_NOTES = {
    "clean_win": "Never in serious trouble and kept most of the move. This is the shape the system is built to produce.",
    "gave_back_win": "Profitable, but most of the best excursion was surrendered on the way out. A structural leak if it is common.",
    "lucky_win": "Came within a fraction of the stop before working. Won, but the decision was closer to wrong than the result suggests.",
    "round_trip_loss": "The worst shape in the book: a real profit handed back in full. Not a market problem — an exit problem.",
    "clean_loss": "Wrong quickly and cheaply. Nothing to fix; this is what the stop is for.",
    "slow_bleed": "Never moved either way and died of old age. Costs little but occupies capital and attention.",
    "gap_loss": "Opened through the stop. Unavoidable and the reason position size, not the stop, is the real risk control.",
}


# Knowable at entry, before the position exists. A lesson drawn from anything
# outside this set is the outcome explaining itself — see the module docstring.
ENTRY_TAGS = frozenset({
    "hostile_external_tape",
    "clear_external_tape",
    "high_volatility_entry",
    "weak_breadth_entry",
    "wide_atr_entry",
    "tight_atr_entry",
})


@dataclass
class TradeReview:
    trade_id: int
    verdict: str
    label: str
    tags: list[str]
    stop_quality: str
    exit_quality: str
    r_left_on_table: float
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


def _f(row: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = row.get(key)
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _stop_quality(mae_r: float, r_multiple: float, exit_reason: str) -> tuple[str, list[str]]:
    tags: list[str] = []
    if exit_reason == "gap_stop":
        return "gapped_through", ["stop_gapped"]
    if r_multiple <= 0 and mae_r <= -0.95:
        return "hit", ["stop_hit"]
    if mae_r <= DEEP_MAE:
        tags.append("survived_near_stop")
        return "nearly_hit", tags
    if mae_r >= SHALLOW_MAE:
        tags.append("never_in_trouble")
        return "untested", tags
    return "tested", tags


def _exit_quality(mfe_r: float, r_multiple: float, exit_reason: str) -> tuple[str, list[str], float]:
    left = max(0.0, mfe_r - r_multiple)
    tags: list[str] = []

    if mfe_r <= 0.1:
        # It never went anywhere. Nothing was left behind because there was
        # nothing to leave.
        return "never_moved", ["no_favourable_excursion"], round(left, 2)

    kept = r_multiple / mfe_r if mfe_r > 0 else 0.0
    if r_multiple > 0 and kept >= 0.7:
        quality = "captured"
    elif r_multiple > 0 and kept >= GAVE_BACK_FRACTION:
        quality = "partial"
    elif r_multiple > 0:
        quality = "gave_back"
        tags.append("gave_back_most_of_move")
    else:
        quality = "round_trip" if mfe_r >= ROUND_TRIP_MFE else "never_worked"
        if mfe_r >= ROUND_TRIP_MFE:
            tags.append("profit_handed_back")

    if left >= BIG_LEFT_ON_TABLE:
        tags.append("left_over_1.5R_on_table")
    if exit_reason == "time":
        tags.append("closed_by_time_stop")
    return quality, tags, round(left, 2)


def _context_tags(row: Mapping[str, Any]) -> list[str]:
    """What the environment was doing — recorded at entry, not reconstructed."""
    tags: list[str] = []
    headwinds = row.get("macro_headwinds")
    if headwinds is not None:
        try:
            if int(headwinds) >= 4:
                tags.append("hostile_external_tape")
            elif int(headwinds) == 0:
                tags.append("clear_external_tape")
        except (TypeError, ValueError):
            pass
    band = str(row.get("volatility_band") or "")
    if band == "stressed":
        tags.append("high_volatility_entry")
    breadth = row.get("breadth_above_200dma")
    if breadth is not None and _f(row, "breadth_above_200dma") < 40.0:
        tags.append("weak_breadth_entry")
    atr = _f(row, "atr_pct_at_entry")
    if atr >= 5.0:
        tags.append("wide_atr_entry")
    elif 0 < atr <= 2.0:
        tags.append("tight_atr_entry")
    return tags


def review_trade(row: Mapping[str, Any]) -> TradeReview:
    """Judge one closed trade from its recorded path."""
    r = _f(row, "r_multiple")
    mae = _f(row, "mae_r")
    mfe = _f(row, "mfe_r")
    exit_reason = str(row.get("exit_reason") or "")

    stop_quality, stop_tags = _stop_quality(mae, r, exit_reason)
    exit_quality, exit_tags, left = _exit_quality(mfe, r, exit_reason)

    if r > SCRATCH_BAND:
        if "survived_near_stop" in stop_tags:
            verdict = "lucky_win"
        elif exit_quality == "gave_back":
            verdict = "gave_back_win"
        else:
            verdict = "clean_win"
    elif exit_reason == "gap_stop":
        verdict = "gap_loss"
    elif mfe >= ROUND_TRIP_MFE:
        verdict = "round_trip_loss"
    elif exit_reason == "time" and abs(r) <= SCRATCH_BAND:
        verdict = "slow_bleed"
    else:
        verdict = "clean_loss"

    tags = stop_tags + exit_tags + _context_tags(row)
    return TradeReview(
        trade_id=int(row["id"]),
        verdict=verdict,
        label=VERDICT_LABELS[verdict],
        tags=sorted(set(tags)),
        stop_quality=stop_quality,
        exit_quality=exit_quality,
        r_left_on_table=left,
        note=VERDICT_NOTES[verdict],
    )


# --- aggregation: the part that becomes a lesson ---------------------------

# A pattern seen fewer times than this is an anecdote. Same floor, same reason,
# as `attribution.MIN_SAMPLE` and `study_coach.MIN_SAMPLE`.
MIN_PATTERN_SAMPLE = 25


def summarise_reviews(rows: list[Mapping[str, Any]]) -> dict:
    """Turn a pile of reviews into the handful of statements worth acting on."""
    closed = [r for r in rows if r.get("verdict")]
    if not closed:
        return {"trades": 0, "verdicts": [], "lessons": [], "tags": []}

    total = len(closed)
    by_verdict: dict[str, list[float]] = {}
    by_tag: dict[str, list[float]] = {}
    for row in closed:
        r = _f(row, "r_multiple")
        by_verdict.setdefault(str(row["verdict"]), []).append(r)
        for tag in row.get("tags") or []:
            by_tag.setdefault(str(tag), []).append(r)

    verdicts = sorted(
        (
            {
                "verdict": name,
                "label": VERDICT_LABELS.get(name, name),
                "trades": len(values),
                "pct_of_trades": round(100.0 * len(values) / total, 1),
                "avg_r": round(sum(values) / len(values), 3),
                "note": VERDICT_NOTES.get(name, ""),
            }
            for name, values in by_verdict.items()
        ),
        key=lambda v: -v["trades"],
    )

    def _tag_rows(names: set[str]) -> list[dict]:
        return sorted(
            (
                {
                    "tag": name,
                    "trades": len(values),
                    "avg_r": round(sum(values) / len(values), 3),
                    "kind": "entry" if name in ENTRY_TAGS else "outcome",
                }
                for name, values in by_tag.items()
                if name in names and len(values) >= MIN_PATTERN_SAMPLE
            ),
            key=lambda t: t["avg_r"],
        )

    entry_tags = _tag_rows(set(by_tag) & ENTRY_TAGS)
    outcome_tags = _tag_rows(set(by_tag) - ENTRY_TAGS)
    tags = entry_tags + outcome_tags

    baseline = sum(_f(r, "r_multiple") for r in closed) / total
    lessons: list[dict] = []

    # Each lesson is a comparison against the book's own average, with the
    # sample attached. Anything thinner than MIN_PATTERN_SAMPLE never reaches
    # this loop, so a lesson can always be checked.
    for tag in entry_tags:
        delta = tag["avg_r"] - baseline
        if abs(delta) < 0.15:
            continue
        lessons.append(
            {
                "tag": tag["tag"],
                "trades": tag["trades"],
                "avg_r": tag["avg_r"],
                "delta_vs_book": round(delta, 3),
                "direction": "worse" if delta < 0 else "better",
                "text": _lesson_text(tag["tag"], tag["avg_r"], delta, tag["trades"]),
            }
        )
    lessons.sort(key=lambda l: l["delta_vs_book"])

    round_trips = by_verdict.get("round_trip_loss", [])
    gave_back = by_verdict.get("gave_back_win", [])
    leak = len(round_trips) + len(gave_back)
    structural = None
    if leak / total >= 0.15:
        structural = (
            f"{round(100.0 * leak / total)}% of trades either handed back a real profit or kept "
            f"less than half their best excursion ({len(round_trips)} round-trip losses, "
            f"{len(gave_back)} give-back wins). That is an exit problem, not a selection problem — "
            "the entries were right often enough."
        )

    return {
        "trades": total,
        "book_avg_r": round(baseline, 3),
        "verdicts": verdicts,
        "tags": tags,
        "entry_tags": entry_tags,
        "outcome_tags": outcome_tags,
        "lessons": lessons[:8],
        "lesson_basis": (
            "Lessons are drawn only from conditions knowable before entry. Tags describing what "
            "the trade did afterwards are shown separately and deliberately excluded: averaging "
            "R over them restates the outcome and reads as a discovery."
        ),
        "structural_note": structural,
    }


_TAG_PHRASES = {
    "survived_near_stop": "trades that came within a whisker of the stop before resolving",
    "never_in_trouble": "trades that were never meaningfully underwater",
    "profit_handed_back": "trades that were over 1R up and still lost",
    "gave_back_most_of_move": "trades that kept less than half their best excursion",
    "left_over_1.5R_on_table": "trades that surrendered more than 1.5R between the high and the exit",
    "closed_by_time_stop": "trades closed by the time stop rather than by price",
    "hostile_external_tape": "trades entered with four or more external series hostile",
    "clear_external_tape": "trades entered with no external headwinds",
    "high_volatility_entry": "trades entered in the stressed volatility band",
    "weak_breadth_entry": "trades entered with breadth under 40%",
    "wide_atr_entry": "trades in names with ATR above 5% of price",
    "tight_atr_entry": "trades in names with ATR under 2% of price",
    "stop_gapped": "trades where price opened through the stop",
    "no_favourable_excursion": "trades that never traded above the entry at all",
}


def _lesson_text(tag: str, avg_r: float, delta: float, n: int) -> str:
    phrase = _TAG_PHRASES.get(tag, f"trades tagged {tag}")
    direction = "below" if delta < 0 else "above"
    return (
        f"On {n} trades, {phrase} averaged {avg_r:+.2f}R — "
        f"{abs(delta):.2f}R {direction} the book average."
    )
