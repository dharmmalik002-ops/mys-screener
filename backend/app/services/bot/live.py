"""Today's decision: the regime now, the playbook it implies, and the candidates.

This is the only module that produces something actionable, and it is built to
refuse as readily as it recommends. Three gates sit between a pattern firing
and a name reaching the user:

  1. *Regime gate.* The current regime must have a playbook with cleared
     strategies. In `recovery` and `bear` it does not, and the answer is cash.
  2. *Strategy gate.* Only strategies that survived out-of-sample validation in
     *this specific regime* may fire. A setup that works in a strong bull does
     not get to trade a narrow one on the strength of its overall average.
  3. *Macro gate.* Measured, not assumed — `macro.py` found that within the
     same regime, trades taken while global volatility and the dollar were a
     tailwind returned ~0.5R more than the same setups into a headwind. When
     the external tape is hostile the book still trades, at reduced size.

Candidates are ranked by the out-of-sample expectancy of the cell that produced
them, not by how striking the chart looks. The number attached to each name is
that cell's held-out average, which is the only honest estimate available.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path

import numpy as np

from . import macro as mc
from . import policy as pol
from .context_series import external
from .features import build_features
from .history import iter_bars, read_bars
from .strategies import BY_ID, STRATEGIES

logger = logging.getLogger(__name__)

# How recent a symbol's last bar must be to be considered live. A stock whose
# data stops three weeks ago is a data problem, not an opportunity.
MAX_STALENESS_SESSIONS = 5
MAX_CANDIDATES = 40

# Macro gate. Both numbers are judgement calls about risk appetite rather than
# fitted parameters, and both are stated in the UI.
MACRO_HEADWIND_COUNT_CAUTION = 2   # this many hostile external series -> half size
MACRO_HEADWIND_COUNT_DEFENSIVE = 4  # this many -> quarter size


@dataclass
class Candidate:
    symbol: str
    strategy: str
    strategy_label: str
    signal_day: str
    close: float
    entry_hint: float          # the level a market order would likely fill near
    stop: float
    risk_pct: float
    atr_pct: float
    turnover_crore: float
    expected_r: float          # the cell's out-of-sample average
    verdict: str
    sizing: dict

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MacroGate:
    headwinds: list[str]
    tailwinds: list[str]
    size_multiplier: float
    stance: str
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


def assess_macro_now(data_dir: Path, as_of: date) -> MacroGate:
    """Where the external tape sits today, and what it does to position size."""
    headwinds: list[str] = []
    tailwinds: list[str] = []

    for spec in external():
        tags = mc.tag_sessions(data_dir, spec, [as_of])
        state = tags.get(as_of)
        if state == "headwind":
            headwinds.append(spec.label)
        elif state == "tailwind":
            tailwinds.append(spec.label)

    count = len(headwinds)
    if count >= MACRO_HEADWIND_COUNT_DEFENSIVE:
        multiplier, stance = 0.25, "defensive"
        note = (
            f"{count} external series are pressuring Indian equities. The measured effect of a "
            "hostile external tape inside an otherwise healthy regime is roughly half a unit of R "
            "per trade, so size is cut hard rather than the book being closed."
        )
    elif count >= MACRO_HEADWIND_COUNT_CAUTION:
        multiplier, stance = 0.5, "cautious"
        note = f"{count} external series are headwinds. Trading at half size."
    else:
        multiplier, stance = 1.0, "clear"
        note = "The external tape is not pressuring Indian equities. Full size available."

    return MacroGate(headwinds, tailwinds, multiplier, stance, note)


def _playbook_for(artifact: dict, regime: str) -> dict | None:
    for book in artifact.get("playbooks") or []:
        if book.get("regime") == regime:
            return book
    return None


def scan_today(
    data_dir: Path,
    artifact: dict,
    equity: float = 1_000_000.0,
    symbols: list[str] | None = None,
) -> dict:
    """The live read: regime, playbook, macro gate, and ranked candidates."""
    current = artifact.get("current_regime") or {}
    regime = str(current.get("regime") or "")
    as_of = date.fromisoformat(current["day"]) if current.get("day") else None
    if not regime or as_of is None:
        return {"error": "backtest artifact has no current regime — rebuild it"}

    book = _playbook_for(artifact, regime)
    macro_gate = assess_macro_now(data_dir, as_of)

    if not book or book.get("stance") == "stand_down" or not book.get("entries"):
        return {
            "as_of": as_of.isoformat(),
            "regime": current,
            "playbook": book,
            "macro": macro_gate.to_dict(),
            "stance": "stand_down",
            "candidates": [],
            "message": (
                book.get("rationale")
                if book else
                "No validated playbook exists for the current regime."
            ),
        }

    cleared = {e["strategy"]: e for e in book["entries"]}
    specs = [BY_ID[s] for s in cleared if s in BY_ID]

    # The last session the benchmark traded — a symbol must be current to it.
    cutoff_index = as_of
    candidates: list[Candidate] = []

    for bars in iter_bars(data_dir, symbols):
        if bars.last_date is None:
            continue
        # Staleness in calendar days is a poor proxy for sessions but errs the
        # safe way: a symbol that missed a week is excluded rather than traded.
        if (cutoff_index - bars.last_date).days > MAX_STALENESS_SESSIONS * 2:
            continue
        features = build_features(bars)
        if features is None:
            continue

        last = len(bars) - 1
        if not bool(features.liquid[last]):
            continue

        for spec in specs:
            try:
                signals = spec.generate(features)
            except Exception:
                continue
            if not bool(signals[last]):
                continue

            atr = float(features.atr14[last])
            close = float(bars.close[last])
            if not np.isfinite(atr) or atr <= 0 or close <= 0:
                continue
            stop = close - spec.stop_atr_mult * atr
            if stop <= 0:
                continue

            entry_cell = cleared[spec.id]
            risk_pct = entry_cell["risk_per_trade_pct"] * macro_gate.size_multiplier
            sizing = pol.position_size(equity, close, stop, risk_pct)

            candidates.append(
                Candidate(
                    symbol=bars.symbol,
                    strategy=spec.id,
                    strategy_label=spec.label,
                    signal_day=bars.dates[last].isoformat(),
                    close=round(close, 2),
                    entry_hint=round(close, 2),
                    stop=round(stop, 2),
                    risk_pct=round((close - stop) / close * 100.0, 2),
                    atr_pct=round(float(features.atr_pct[last]), 2),
                    turnover_crore=round(float(features.turnover_crore[last]), 1),
                    expected_r=entry_cell["out_sample_r"],
                    verdict=entry_cell["verdict"],
                    sizing=sizing,
                )
            )

    candidates.sort(key=lambda c: (-c.expected_r, -c.turnover_crore))
    trimmed = candidates[:MAX_CANDIDATES]

    return {
        "as_of": as_of.isoformat(),
        "regime": current,
        "playbook": book,
        "macro": macro_gate.to_dict(),
        "stance": book.get("stance"),
        "equity": equity,
        "candidates": [c.to_dict() for c in trimmed],
        "candidates_found": len(candidates),
        "message": (
            f"{len(candidates)} candidate(s) from {len(specs)} cleared setup(s) in "
            f"{book['label']}. Sizing at {macro_gate.size_multiplier:.0%} of book risk "
            f"({macro_gate.stance} external tape)."
            if candidates else
            f"{len(specs)} setup(s) are cleared to trade in {book['label']}, but none fired today."
        ),
    }
