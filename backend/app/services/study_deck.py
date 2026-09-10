"""Chart-reading drill: a deck of historical setups you grade before seeing the outcome.

The deck is mined offline by `scripts/generate_study_deck.py`, which replays the
VCP and flag scanners across `chart_cache` history and records what each signal
did next under the same rules the regime brief uses (3% stop, 5% target, 10
sessions). This module only *serves* that file — no scanning, no simulation.

Two rules shape the selection and both matter more than they look:

1. Wins and losses are drawn in equal measure. A deck of winners teaches the eye
   that every base breaks out, which is the opposite of the skill being trained.
2. A card is never repeated until the whole deck has been dealt. The permutation
   is seeded once from the deck itself, so "today's cards" are the same all day
   and every day advances one slice — no server-side state to keep.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DECK_FILENAME = "study_deck.json"
# How many sessions of context the card shows before the trigger bar. Enough to
# see the prior advance that a VCP is digesting (a 90-day base plus its run-up).
CONTEXT_BARS = 180
# How far past the trigger a card can run. The drill lets you wait before
# committing and then holds the trade, so the window is both halves back to
# back: WAIT_BARS of "not yet" plus HOLD_BARS of an open position.
WAIT_BARS = 15
HOLD_BARS = 15
REVEAL_BARS = WAIT_BARS + HOLD_BARS
# Bars are served in small slices as the user steps, never in one lump. The
# outcome is the thing being tested, so it must not sit in the browser before
# it has been earned — see the deck gotcha in CLAUDE.md.
FORWARD_CHUNK = 4
DEFAULT_DECK_SIZE = 20
EPOCH = date(2026, 1, 1)


@dataclass(frozen=True)
class DeckCard:
    """One card, as stored in the deck file."""

    id: str
    setup: str
    label: str
    symbol: str
    name: str
    trigger_date: str
    entry: float
    stop: float | None
    risk_pct: float | None
    score: float
    rs_rating: int
    group_top_decile: bool
    reasons: list[str]
    result: str
    max_favourable_pct: float
    final_pct: float
    sessions_held: int

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "DeckCard | None":
        try:
            return cls(
                id=str(row["id"]),
                setup=str(row["setup"]),
                label=str(row.get("label") or row["setup"]),
                symbol=str(row["symbol"]).upper(),
                name=str(row.get("name") or row["symbol"]),
                trigger_date=str(row["trigger_date"]),
                entry=float(row["entry"]),
                stop=float(row["stop"]) if row.get("stop") is not None else None,
                risk_pct=float(row["risk_pct"]) if row.get("risk_pct") is not None else None,
                score=float(row.get("score") or 0.0),
                rs_rating=int(row.get("rs_rating") or 0),
                group_top_decile=bool(row.get("group_top_decile")),
                reasons=[str(r) for r in (row.get("reasons") or [])],
                result=str(row["result"]),
                max_favourable_pct=float(row.get("max_favourable_pct") or 0.0),
                final_pct=float(row.get("final_pct") or 0.0),
                sessions_held=int(row.get("sessions_held") or 0),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def question(self) -> dict[str, Any]:
        """The half of the card you are allowed to see before you call it.

        Deliberately omits result, reasons and the scanner's own stop: the whole
        exercise is reading the chart, and a leaked stop level is a leaked answer.
        """
        return {
            "id": self.id,
            "setup": self.setup,
            "label": self.label,
            "symbol": self.symbol,
            "name": self.name,
            "trigger_date": self.trigger_date,
            "entry": self.entry,
        }

    def answer(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "setup": self.setup,
            "label": self.label,
            "symbol": self.symbol,
            "trigger_date": self.trigger_date,
            "entry": self.entry,
            "scanner_stop": self.stop,
            "scanner_risk_pct": self.risk_pct,
            "score": self.score,
            "rs_rating": self.rs_rating,
            "group_top_decile": self.group_top_decile,
            "reasons": self.reasons,
            "result": self.result,
            "max_favourable_pct": self.max_favourable_pct,
            "final_pct": self.final_pct,
            "sessions_held": self.sessions_held,
        }


class StudyDeck:
    """Loads the mined deck once and deals a fresh, balanced slice per day."""

    def __init__(self, data_dir: Path) -> None:
        self._data_dir = Path(data_dir)
        self._cards: dict[str, DeckCard] = {}
        self._wins: list[str] = []
        self._losses: list[str] = []
        self._meta: dict[str, Any] = {}
        self._loaded = False

    # --- Loading ----------------------------------------------------------

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        path = self._data_dir / DECK_FILENAME
        if not path.exists():
            logger.warning("study deck missing at %s — run scripts/generate_study_deck.py", path)
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("study deck unreadable: %s", exc)
            return

        self._meta = {
            "generated_at": payload.get("generated_at"),
            "sessions_replayed": payload.get("sessions_replayed"),
            "symbols_replayed": payload.get("symbols_replayed"),
            "window_start": payload.get("window_start"),
            "window_end": payload.get("window_end"),
            "rules": payload.get("rules") or {},
            "setups": payload.get("setups") or [],
        }

        for row in payload.get("cards") or []:
            card = DeckCard.from_row(row)
            # "open" signals never resolved inside the horizon; grading a card
            # whose answer does not exist would be scoring noise.
            if card is None or card.result not in ("win", "loss", "timeout"):
                continue
            self._cards[card.id] = card
            (self._wins if card.result == "win" else self._losses).append(card.id)

        # One stable shuffle for the life of the file. Seeded from the deck's own
        # contents so a rebuild reshuffles, but a restart does not.
        seed = hashlib.sha256(
            f"{self._meta.get('generated_at')}|{len(self._cards)}".encode()
        ).hexdigest()
        random.Random(seed).shuffle(self._wins)
        random.Random(seed + "l").shuffle(self._losses)
        logger.info(
            "study deck loaded: %d cards (%d win / %d loss)",
            len(self._cards), len(self._wins), len(self._losses),
        )

    # --- Dealing ----------------------------------------------------------

    @staticmethod
    def _slice(pool: list[str], day_index: int, want: int) -> list[str]:
        """Deal `want` ids from `pool`, wrapping once the pool is exhausted."""
        if not pool or want <= 0:
            return []
        start = (day_index * want) % len(pool)
        out = [pool[(start + i) % len(pool)] for i in range(min(want, len(pool)))]
        return out

    def deal(self, day: date, count: int = DEFAULT_DECK_SIZE, setup: str | None = None) -> list[DeckCard]:
        self._load()
        if not self._cards:
            return []

        wins, losses = self._wins, self._losses
        if setup:
            wins = [cid for cid in wins if self._cards[cid].setup == setup]
            losses = [cid for cid in losses if self._cards[cid].setup == setup]

        day_index = (day - EPOCH).days
        half = max(1, count // 2)
        ids = self._slice(wins, day_index, half) + self._slice(losses, day_index, count - half)

        # Shuffle the day's hand so wins and losses do not alternate predictably.
        random.Random(f"{day.isoformat()}|{setup or 'all'}").shuffle(ids)
        return [self._cards[cid] for cid in ids if cid in self._cards]

    def card(self, card_id: str) -> DeckCard | None:
        self._load()
        return self._cards.get(card_id)

    def meta(self) -> dict[str, Any]:
        self._load()
        counts: dict[str, int] = {}
        for card in self._cards.values():
            counts[card.setup] = counts.get(card.setup, 0) + 1
        return {
            **self._meta,
            "total_cards": len(self._cards),
            "wins": len(self._wins),
            "losses": len(self._losses),
            "by_setup": counts,
            "context_bars": CONTEXT_BARS,
            "reveal_bars": REVEAL_BARS,
            "wait_bars": WAIT_BARS,
            "hold_bars": HOLD_BARS,
            "served_at": datetime.now(timezone.utc).isoformat(),
        }


# --- Bars -----------------------------------------------------------------
# Bars are NOT stored in the deck: the same 500-bar series already sits in
# chart_cache, and duplicating it per card would turn a 2 MB file into 400 MB.


def _cache_path(data_dir: Path, symbol: str) -> Path:
    return Path(data_dir) / "chart_cache" / f"{symbol.upper()}__1D.json"


def _read_bars(data_dir: Path, symbol: str) -> list[dict[str, Any]]:
    path = _cache_path(data_dir, symbol)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    bars = payload.get("bars")
    return bars if isinstance(bars, list) else []


def _bar_time(bar: Any) -> float | None:
    ts = bar.get("time") if isinstance(bar, dict) else getattr(bar, "time", None)
    return float(ts) if isinstance(ts, (int, float)) else None


def _as_dict(bar: Any) -> dict[str, Any]:
    """Normalise a bar to the shape the drill chart expects.

    Bars arrive either as chart_cache dicts or as ChartBar models from the live
    provider, and the frontend must not have to tell the two apart.
    """
    if isinstance(bar, dict):
        source = bar
    else:
        source = {key: getattr(bar, key, None) for key in ("time", "open", "high", "low", "close", "volume")}
    return {
        "time": int(source.get("time") or 0),
        "open": float(source.get("open") or 0.0),
        "high": float(source.get("high") or 0.0),
        "low": float(source.get("low") or 0.0),
        "close": float(source.get("close") or 0.0),
        "volume": float(source.get("volume") or 0.0),
    }


def _trigger_index(bars: list[Any], trigger_date: str) -> int:
    """Index of the trigger bar, or -1.

    Matched on the UTC date of the bar's epoch-second timestamp, which is how
    both chart_cache and the live provider stamp daily bars.
    """
    try:
        target = date.fromisoformat(trigger_date)
    except ValueError:
        return -1
    for i, bar in enumerate(bars):
        ts = _bar_time(bar)
        if ts is None:
            continue
        if datetime.fromtimestamp(ts, tz=timezone.utc).date() == target:
            return i
    return -1


def split_series(
    bars: list[Any],
    trigger_date: str,
    context: int = CONTEXT_BARS,
    forward: int = REVEAL_BARS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (bars up to and including the trigger, bars after it).

    The first list is what the drill shows; the second is the answer and must
    not travel with it.
    """
    idx = _trigger_index(bars, trigger_date)
    if idx < 0:
        return [], []
    start = max(0, idx - context + 1)
    return (
        [_as_dict(b) for b in bars[start: idx + 1]],
        [_as_dict(b) for b in bars[idx + 1: idx + 1 + forward]],
    )


def split_bars(
    data_dir: Path,
    symbol: str,
    trigger_date: str,
    context: int = CONTEXT_BARS,
    forward: int = REVEAL_BARS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """`split_series` against the local chart_cache.

    Returns empty lists when the cache has nothing for this symbol, which is the
    normal case on a cold Space — chart_cache is gitignored and rebuilt on
    demand, so callers must be ready to fall back to the live provider.
    """
    return split_series(_read_bars(data_dir, symbol), trigger_date, context, forward)
