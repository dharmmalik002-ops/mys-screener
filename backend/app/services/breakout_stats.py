"""Breakout follow-through statistics via scanner replay.

Answers the question the Markets page actually needs: *when a setup triggered,
what happened next* — win rate, how far it ran, how long it held, and whether it
closed strong. Aggregated per setup per week so the regime read is grounded in
measured outcomes rather than impressions.

Replay rather than a signal log
-------------------------------
`scan_history.json` only keeps ~15 sessions and is written as a side effect of
someone running a scanner, so it is sparse and biased toward days the app
happened to be used. Instead we reconstruct history: for each past session we
truncate each symbol's daily bars at that date and rebuild the snapshot through
the *same* `_history_to_snapshot` the live path uses, then run the *same*
scanner evaluators. A replayed signal is therefore what the scanner would have
shown that day, not an approximation of it.

Cost: ~16ms per symbol-date, so a 12-week window over ~1,350 symbols is roughly
20 minutes. That is a nightly job (see scripts/generate_breakout_stats.py), far
too slow to serve from a request — and well inside the HF Spaces memory floor
because bars are streamed per symbol rather than held all at once.
"""

from __future__ import annotations

import json
import logging
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Iterator, Sequence

import pandas as pd

from app.scanners.definitions import SCANS

logger = logging.getLogger(__name__)


# --- Trade simulation rules ------------------------------------------------
# Chosen deliberately and kept in one place: every published number depends on
# them, so they belong in the payload too (see `rules` in build_breakout_stats).
STOP_PCT = 3.0          # a loss once price trades this far below entry
WIN_PCT = 5.0           # a win once it *closes* this far above entry
HORIZON_SESSIONS = 10   # give up after this many sessions; neither win nor loss
NEAR_HIGH_FRACTION = 0.75  # "closed strong" = in the top quartile of its range
BIG_MOVE_PCT = 10.0     # the ceiling the regime read talks about

# Match `_history_to_snapshot`'s own floor rather than imposing a stricter one.
# An earlier value of 250 quietly deleted the entire IPO cohort: a stock listed
# inside the last year has at most ~250 sessions, so every recent listing was
# filtered out before it could ever produce a signal. Indicators that need more
# history (sma200 and friends) come back None on their own, and the scanners
# that depend on them simply do not match — exactly as they behave live.
MIN_BARS_FOR_SNAPSHOT = 30
IPO_MAX_AGE_DAYS = 365


@dataclass(frozen=True)
class Signal:
    """One scanner trigger on one symbol on one session."""

    setup: str
    symbol: str
    trigger_date: date
    entry: float
    rs_rating: int
    is_ipo: bool
    group_top_decile: bool


@dataclass
class Outcome:
    """What happened after a signal, under the rules above."""

    signal: Signal
    result: str          # "win" | "loss" | "timeout"
    max_favourable_pct: float
    final_pct: float
    sessions_held: int
    closed_near_high: bool


@dataclass
class SetupStats:
    setup: str
    label: str
    signals: int = 0
    resolved: int = 0
    open_positions: int = 0
    wins: int = 0
    losses: int = 0
    timeouts: int = 0
    median_max_move_pct: float = 0.0
    median_sessions_held: float = 0.0
    pct_closed_near_high: float = 0.0
    pct_reached_big_move: float = 0.0
    pct_big_move_held: float = 0.0
    median_final_pct: float = 0.0
    examples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        """Over resolved signals only — see `summarise`."""
        return round(100.0 * self.wins / self.resolved, 1) if self.resolved else 0.0


# --- Bar loading -----------------------------------------------------------


def _bars_frame(payload: dict[str, Any]) -> pd.DataFrame | None:
    bars = payload.get("bars") or []
    if len(bars) < MIN_BARS_FOR_SNAPSHOT:
        return None
    frame = pd.DataFrame(bars)
    if not {"time", "open", "high", "low", "close", "volume"} <= set(frame.columns):
        return None
    frame["Date"] = pd.to_datetime(frame["time"], unit="s")
    frame = frame.rename(
        columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    )
    frame = frame.set_index("Date")[["Open", "High", "Low", "Close", "Volume"]]
    return frame[~frame.index.duplicated(keep="last")].sort_index()


def iter_symbol_bars(chart_cache_dir: Path) -> Iterator[tuple[str, pd.DataFrame]]:
    """Stream (symbol, daily bars) one file at a time.

    Streaming rather than building a dict of every frame keeps peak memory flat;
    the whole cache as DataFrames would not fit comfortably in the Space.
    """
    for path in sorted(chart_cache_dir.glob("*__1D.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning("breakout-stats: unreadable chart cache %s: %s", path.name, exc)
            continue
        symbol = str(payload.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        frame = _bars_frame(payload)
        if frame is not None:
            yield symbol, frame


# --- Outcome simulation ----------------------------------------------------


def simulate(signal: Signal, forward: pd.DataFrame, horizon: int = HORIZON_SESSIONS) -> Outcome | None:
    """Walk forward bar by bar and resolve the trade.

    Order within a day matters and daily bars cannot tell us which came first,
    so when a bar both breaches the stop and closes at target we call it a loss.
    That biases every win rate *down* — the honest direction for a number whose
    whole purpose is to say whether the tape is paying.
    """
    if forward.empty or signal.entry <= 0:
        return None

    window = forward.iloc[:horizon]
    if window.empty:
        return None

    entry = signal.entry
    stop = entry * (1.0 - STOP_PCT / 100.0)
    target = entry * (1.0 + WIN_PCT / 100.0)

    running_high = float("-inf")
    running_low = float("inf")
    # A signal from the last two weeks has not had time to resolve. Calling that
    # a timeout would quietly understate recent win rates, so it gets its own
    # bucket and is excluded from win-rate denominators.
    result = "timeout" if len(window) >= horizon else "open"
    sessions_held = len(window)
    resolution_close = float(window["Close"].iloc[-1])

    for i, (_, bar) in enumerate(window.iterrows(), start=1):
        high, low, close = float(bar["High"]), float(bar["Low"]), float(bar["Close"])
        running_high = max(running_high, high)
        running_low = min(running_low, low)
        if low <= stop:
            result, sessions_held, resolution_close = "loss", i, close
            break
        if close >= target:
            result, sessions_held, resolution_close = "win", i, close
            break

    max_favourable = (running_high - entry) / entry * 100.0
    final_pct = (resolution_close - entry) / entry * 100.0

    span = running_high - running_low
    closed_near_high = bool(span > 0 and (resolution_close - running_low) / span >= NEAR_HIGH_FRACTION)

    return Outcome(
        signal=signal,
        result=result,
        max_favourable_pct=round(max_favourable, 2),
        final_pct=round(final_pct, 2),
        sessions_held=sessions_held,
        closed_near_high=closed_near_high,
    )


# --- Aggregation -----------------------------------------------------------


def _pct(numerator: int, denominator: int) -> float:
    return round(100.0 * numerator / denominator, 1) if denominator else 0.0


def summarise(setup: str, label: str, outcomes: Sequence[Outcome]) -> SetupStats:
    stats = SetupStats(setup=setup, label=label, signals=len(outcomes))
    if not outcomes:
        return stats

    stats.open_positions = sum(1 for o in outcomes if o.result == "open")
    # Every rate below is computed over *resolved* signals only. Mixing in
    # still-open ones would drag recent weeks toward zero purely because they
    # are recent.
    resolved = [o for o in outcomes if o.result != "open"]
    stats.resolved = len(resolved)
    if not resolved:
        return stats

    stats.wins = sum(1 for o in resolved if o.result == "win")
    stats.losses = sum(1 for o in resolved if o.result == "loss")
    stats.timeouts = sum(1 for o in resolved if o.result == "timeout")
    stats.median_max_move_pct = round(median(o.max_favourable_pct for o in resolved), 2)
    stats.median_final_pct = round(median(o.final_pct for o in resolved), 2)
    stats.median_sessions_held = round(median(o.sessions_held for o in resolved), 1)
    stats.pct_closed_near_high = _pct(sum(1 for o in resolved if o.closed_near_high), len(resolved))

    big = [o for o in resolved if o.max_favourable_pct >= BIG_MOVE_PCT]
    stats.pct_reached_big_move = _pct(len(big), len(resolved))
    # Of the names that *did* reach the big move, how many actually kept it —
    # the "erased half its progress" behaviour the regime read describes.
    stats.pct_big_move_held = _pct(sum(1 for o in big if o.closed_near_high), len(big))

    stats.examples = [
        {
            "symbol": o.signal.symbol,
            "trigger_date": o.signal.trigger_date.isoformat(),
            "max_move_pct": o.max_favourable_pct,
            "final_pct": o.final_pct,
            "sessions_held": o.sessions_held,
            "result": o.result,
        }
        for o in sorted(resolved, key=lambda x: x.max_favourable_pct, reverse=True)[:5]
    ]
    return stats


def slim_comparable(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep only what the comparable block is actually read for.

    `comparable` is a second full aggregation of every week at a short horizon,
    so unslimmed it doubles the file. Only `horizon_sessions` and each week's
    `overall` are ever consumed (market_regime.build_facts uses them for the
    week-over-week deltas), and this file is committed nightly — carrying the
    unused per-setup and per-cohort duplicates would add hundreds of KB to git
    history every single day for nothing.
    """
    weeks = payload.get("weeks") or []
    return {
        "horizon_sessions": payload.get("horizon_sessions"),
        "weeks": [
            {
                "week": week.get("week"),
                "overall": {
                    key: value
                    for key, value in (week.get("overall") or {}).items()
                    if key != "examples"  # illustrative only; the full-horizon block has them
                },
            }
            for week in weeks
        ],
    }


def week_key(day: date) -> str:
    """ISO week label, so "last week" is unambiguous across month boundaries."""
    iso = day.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def aggregate(outcomes: Iterable[Outcome], labels: dict[str, str]) -> dict[str, Any]:
    """Group outcomes by ISO week, then by setup, plus the requested cohorts.

    Truncation bias, and why `comparable` exists
    --------------------------------------------
    A signal fired three sessions ago has only three sessions of forward data.
    Under a 3%-stop / 5%-target rule a *loss* needs a 3% adverse move and a
    *win* needs a 5% favourable close, so in a short window losses resolve
    disproportionately often. Reading the newest week's full-horizon win rate
    against older, fully-resolved weeks therefore makes the present look worse
    than it is — every time, structurally.

    So the caller also evaluates every week at one short common horizon (the
    horizon the newest week actually has) and puts it under `comparable`. Those
    numbers are the ones to use for "is this week worse than last week"; the
    per-week block is the fuller picture for weeks that have run their course.
    """
    by_week: dict[str, list[Outcome]] = defaultdict(list)
    for outcome in outcomes:
        by_week[week_key(outcome.signal.trigger_date)].append(outcome)

    weeks: list[dict[str, Any]] = []
    for week in sorted(by_week):
        rows = by_week[week]
        per_setup: dict[str, list[Outcome]] = defaultdict(list)
        for outcome in rows:
            per_setup[outcome.signal.setup].append(outcome)

        setups = [
            summarise(key, labels.get(key, key), items)
            for key, items in sorted(per_setup.items())
        ]
        setups.sort(key=lambda s: (-s.win_rate, -s.signals))

        ipo_rows = [o for o in rows if o.signal.is_ipo]
        leader_rows = [o for o in rows if o.signal.group_top_decile]
        laggard_rows = [o for o in rows if not o.signal.group_top_decile]

        weeks.append(
            {
                "week": week,
                "sessions": sorted({o.signal.trigger_date.isoformat() for o in rows}),
                "total_signals": len(rows),
                "overall": vars_of(summarise("all", "All setups", rows)),
                "setups": [vars_of(s) for s in setups],
                "cohorts": {
                    "ipo": vars_of(summarise("ipo", "Recent IPOs", ipo_rows)),
                    "leading_groups": vars_of(summarise("leading", "Top-decile groups", leader_rows)),
                    "lagging_groups": vars_of(summarise("lagging", "Everything else", laggard_rows)),
                },
            }
        )
    return {"weeks": weeks}


def vars_of(stats: SetupStats) -> dict[str, Any]:
    payload = {
        "setup": stats.setup,
        "label": stats.label,
        "signals": stats.signals,
        "resolved": stats.resolved,
        "open_positions": stats.open_positions,
        "win_rate": stats.win_rate,
        "wins": stats.wins,
        "losses": stats.losses,
        "timeouts": stats.timeouts,
        "median_max_move_pct": stats.median_max_move_pct,
        "median_final_pct": stats.median_final_pct,
        "median_sessions_held": stats.median_sessions_held,
        "pct_closed_near_high": stats.pct_closed_near_high,
        "pct_reached_big_move": stats.pct_reached_big_move,
        "pct_big_move_held": stats.pct_big_move_held,
        "examples": stats.examples,
    }
    return payload


# --- Setup roster ----------------------------------------------------------


def setup_roster() -> list[Any]:
    """The breakout setups worth measuring.

    The "Core" category is excluded on purpose: `day-high`, `week-high` and
    friends are state descriptions ("is at its high"), not entry setups, and
    scoring them as trades would pad the table with thousands of meaningless
    signals.
    """
    return [scan for scan in SCANS if scan.category == "Setups"]


def scan_labels() -> dict[str, str]:
    return {scan.id: scan.name for scan in SCANS}


def is_recent_ipo(listing_date: Any, as_of: date) -> bool:
    if not listing_date:
        return False
    if isinstance(listing_date, str):
        try:
            listing_date = datetime.fromisoformat(listing_date).date()
        except ValueError:
            return False
    if isinstance(listing_date, datetime):
        listing_date = listing_date.date()
    if not isinstance(listing_date, date):
        return False
    age = (as_of - listing_date).days
    return 0 <= age <= IPO_MAX_AGE_DAYS


def sessions_in_window(frame: pd.DataFrame, start: date, end: date) -> list[pd.Timestamp]:
    index = frame.index
    lo = bisect_right(list(index), pd.Timestamp(start)) - 1
    return [ts for ts in index if start <= ts.date() <= end]


def rules_payload() -> dict[str, Any]:
    """Shipped alongside the numbers so a reader can see what they mean."""
    return {
        "entry": "close of the session the setup triggered",
        "stop_pct": STOP_PCT,
        "win_pct": WIN_PCT,
        "horizon_sessions": HORIZON_SESSIONS,
        "big_move_pct": BIG_MOVE_PCT,
        "near_high_fraction": NEAR_HIGH_FRACTION,
        "tie_break": "a bar that both breaches the stop and closes at target counts as a loss",
        "note": (
            "Signals are reconstructed by replaying each scanner against daily bars "
            "truncated at the trigger date, using the same snapshot builder as the live path."
        ),
    }
