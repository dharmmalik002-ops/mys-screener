"""How the bot's view of each strategy changes as evidence arrives.

There are two things people mean by a "self-improving" trading system, and
only one of them works.

The version that does not work re-fits its parameters on recent results. It
feels like learning and it is curve-fitting with a feedback loop: the system
tunes itself to the last few months, the last few months do not repeat, and it
tunes again. Each round destroys a little more of the out-of-sample evidence
that made the original edge believable, and the failure is invisible until real
money is on it.

The version that works keeps the rules fixed and lets the *evidence* move. Each
strategy x regime cell is re-scored as new trades close, and the cell's standing
is promoted or demoted on what it has done lately versus what it did overall.
Nothing is refitted — a cell that stops paying is retired rather than repaired.
That is the sense in which this module learns, and the distinction is the whole
reason it is safe to run unattended.

`replay_evolution` makes it checkable rather than claimed: it walks forward
through history at a fixed cadence and, at each point, scores every cell using
**only trades that had closed by that date**. The resulting timeline is what
the system would genuinely have believed on each of those days, including the
occasions it was wrong and later changed its mind. A system that never changes
its mind in that replay is not adaptive, whatever its documentation says.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from typing import Iterable, Mapping, Sequence

import numpy as np

logger = logging.getLogger(__name__)

# Sample floors. A status change on a handful of trades is noise promoted to
# policy, which is exactly the failure this module exists to avoid.
MIN_TRADES_FOR_STATUS = 30
MIN_RECENT_TRADES = 20

# The trailing window that defines "lately". Two years is long enough to span
# more than one regime and short enough that a cell which died three years ago
# does not keep collecting credit for it.
RECENT_WINDOW_DAYS = 730

# How far recent performance must fall below the long-run figure before a
# confirmed cell is demoted. Declared, not fitted.
DECAY_MARGIN_R = 0.15

STATUSES = ("confirmed", "watch", "retired", "candidate", "rejected")

STATUS_LABELS = {
    "confirmed": "Confirmed",
    "watch": "On watch",
    "retired": "Retired",
    "candidate": "Candidate",
    "rejected": "Rejected",
}

STATUS_NOTES = {
    "confirmed": "Positive over the long run and still positive lately. Cleared to trade.",
    "watch": "Still positive overall, but recent trades are materially worse. Size down and keep measuring.",
    "retired": "Was positive once and is not any more. Stood down until the evidence returns.",
    "candidate": "Looks positive but has not traded enough to be trusted yet.",
    "rejected": "No edge over the sample available.",
}


@dataclass
class CellVerdict:
    strategy: str
    regime: str
    status: str
    trades: int
    win_rate: float
    avg_r: float
    payoff: float
    recent_trades: int
    recent_avg_r: float | None
    note: str

    def to_dict(self) -> dict:
        out = asdict(self)
        out["status_label"] = STATUS_LABELS.get(self.status, self.status)
        return out


def _payoff(returns: np.ndarray) -> float:
    """Average win divided by average loss, in R — the reward-to-risk actually achieved."""
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    if not len(wins) or not len(losses):
        return 0.0
    average_loss = abs(float(losses.mean()))
    return round(float(wins.mean()) / average_loss, 2) if average_loss > 0 else 0.0


def score_cell(
    strategy: str,
    regime: str,
    trades: Sequence[Mapping],
    as_of: date,
    recent_window_days: int = RECENT_WINDOW_DAYS,
) -> CellVerdict | None:
    """Standing of one cell using only trades closed on or before `as_of`."""
    closed = [
        t for t in trades
        if t.get("exit_day") and date.fromisoformat(str(t["exit_day"])) <= as_of
    ]
    if len(closed) < MIN_TRADES_FOR_STATUS:
        return None

    returns = np.array([float(t["r_multiple"]) for t in closed], dtype=np.float64)
    cutoff = as_of - timedelta(days=recent_window_days)
    recent = np.array(
        [
            float(t["r_multiple"]) for t in closed
            if date.fromisoformat(str(t["exit_day"])) >= cutoff
        ],
        dtype=np.float64,
    )

    avg_r = float(returns.mean())
    win_rate = round(100.0 * float((returns > 0).mean()), 1)
    payoff = _payoff(returns)
    recent_avg = float(recent.mean()) if len(recent) >= MIN_RECENT_TRADES else None

    if avg_r <= 0:
        status = "rejected"
    elif recent_avg is None:
        # Positive overall but nothing recent to confirm it still holds. That
        # is a candidate, not a confirmation — the long-run figure may be
        # carried entirely by conditions that have not recurred in two years.
        status = "candidate"
    elif recent_avg <= 0:
        status = "retired"
    elif recent_avg < avg_r - DECAY_MARGIN_R:
        status = "watch"
    else:
        status = "confirmed"

    if recent_avg is None:
        note = (
            f"{len(closed)} trades at {avg_r:+.2f}R overall, but fewer than {MIN_RECENT_TRADES} "
            f"in the last {recent_window_days // 365} years — nothing recent to confirm it."
        )
    elif status == "retired":
        note = (
            f"{avg_r:+.2f}R across {len(closed)} trades overall, but {recent_avg:+.2f}R on the "
            f"{len(recent)} most recent. The edge is not currently there."
        )
    elif status == "watch":
        note = (
            f"{avg_r:+.2f}R long-run against {recent_avg:+.2f}R on the last {len(recent)} trades — "
            f"a {avg_r - recent_avg:.2f}R deterioration. Still positive; watch it."
        )
    else:
        note = (
            f"{avg_r:+.2f}R across {len(closed)} trades, {recent_avg:+.2f}R on the last "
            f"{len(recent)}. Holding up."
        )

    return CellVerdict(
        strategy=strategy,
        regime=regime,
        status=status,
        trades=len(closed),
        win_rate=win_rate,
        avg_r=round(avg_r, 3),
        payoff=payoff,
        recent_trades=int(len(recent)),
        recent_avg_r=round(recent_avg, 3) if recent_avg is not None else None,
        note=note,
    )


def score_all(trades: Iterable[Mapping], as_of: date) -> list[CellVerdict]:
    """Every cell's standing as at one date."""
    grouped: dict[tuple[str, str], list[Mapping]] = {}
    for trade in trades:
        key = (str(trade["strategy"]), str(trade["regime"]))
        grouped.setdefault(key, []).append(trade)

    out: list[CellVerdict] = []
    for (strategy, regime), rows in grouped.items():
        verdict = score_cell(strategy, regime, rows, as_of)
        if verdict is not None:
            out.append(verdict)
    return sorted(out, key=lambda c: -c.avg_r)


def replay_evolution(
    trades: Sequence[Mapping],
    *,
    cadence_days: int = 91,
    warmup_trades: int = 500,
) -> list[dict]:
    """The bot's changing view of every cell, quarter by quarter.

    At each checkpoint only trades already closed are visible, so the output is
    an honest record of what the system believed at the time — including the
    cells it backed and later stood down. Anything else would be hindsight
    wearing a timestamp.
    """
    dated = [t for t in trades if t.get("exit_day")]
    if len(dated) < warmup_trades:
        return []

    days = sorted(date.fromisoformat(str(t["exit_day"])) for t in dated)
    # Start once enough trades have closed to say anything at all.
    start = days[warmup_trades]
    end = days[-1]

    checkpoints: list[date] = []
    cursor = start
    while cursor <= end:
        checkpoints.append(cursor)
        cursor += timedelta(days=cadence_days)
    if checkpoints and checkpoints[-1] != end:
        checkpoints.append(end)

    snapshots: list[dict] = []
    for as_of in checkpoints:
        verdicts = score_all(dated, as_of)
        if not verdicts:
            continue
        counts: dict[str, int] = {}
        for verdict in verdicts:
            counts[verdict.status] = counts.get(verdict.status, 0) + 1
        snapshots.append(
            {
                "as_of": as_of.isoformat(),
                "cells": [v.to_dict() for v in verdicts],
                "counts": counts,
                "tradeable": sum(counts.get(s, 0) for s in ("confirmed", "watch")),
            }
        )
    return snapshots


def summarise_changes(snapshots: Sequence[Mapping]) -> list[dict]:
    """Only the moments a cell's status changed — the system changing its mind."""
    changes: list[dict] = []
    previous: dict[tuple[str, str], str] = {}
    for snapshot in snapshots:
        for cell in snapshot["cells"]:
            key = (cell["strategy"], cell["regime"])
            was = previous.get(key)
            if was is not None and was != cell["status"]:
                changes.append(
                    {
                        "as_of": snapshot["as_of"],
                        "strategy": cell["strategy"],
                        "regime": cell["regime"],
                        "from_status": was,
                        "to_status": cell["status"],
                        "from_label": STATUS_LABELS.get(was, was),
                        "to_label": STATUS_LABELS.get(cell["status"], cell["status"]),
                        "trades": cell["trades"],
                        "avg_r": cell["avg_r"],
                        "recent_avg_r": cell["recent_avg_r"],
                        "note": cell["note"],
                    }
                )
            previous[key] = cell["status"]
    return sorted(changes, key=lambda c: c["as_of"], reverse=True)
