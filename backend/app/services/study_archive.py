"""A searchable archive of past scanner signals — including the ones that failed.

Most chart-pattern teaching shows the winners. That is precisely backwards: a
library of successful breakouts trains the eye to see a breakout in every base,
which is the same failure mode the Chart Gym deck balances against (CLAUDE.md
gotcha 15). The value is in being able to ask "show me fifty VCPs that failed"
and look at what they had in common.

Corpus
------
The same mined file the drill deals from — `study_deck.json`, built offline by
`scripts/generate_study_deck.py`. Every row is a real signal reconstructed by
replaying the scanner against bars truncated at its trigger date, then scored
under the regime brief's rules (3% stop, 5% target, 10 sessions). Reusing it
rather than mining a second corpus means the library and the drill cannot
disagree about what happened, and the expensive replay is paid for once.

What makes this teach rather than just list
-------------------------------------------
Every query returns the outcome statistics for the WHOLE filtered set beside
the same statistics for the unfiltered corpus. A win rate on its own is a
number; a win rate next to its baseline is a lesson — "VCPs with RS ≥ 90 won
31% against 22% for all VCPs" says something a raw list never will. The
comparison is arithmetic over resolved past signals and is described as such:
it is a measurement of what happened, not a claim about what will.

Drill integrity
---------------
Today's dealt hand is excluded from every query. The drill's whole premise is
that the outcome is not in the browser before it has been earned, and a
library that let you look up the answer to the card on screen would quietly
dismantle it.
"""

from __future__ import annotations

import logging
from datetime import date
from statistics import median
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)

MAX_PAGE = 200
DEFAULT_PAGE = 50

SORTS: dict[str, tuple[str, bool]] = {
    # key -> (card attribute, descending)
    "recent": ("trigger_date", True),
    "oldest": ("trigger_date", False),
    "best": ("final_pct", True),
    "worst": ("final_pct", False),
    "biggest_run": ("max_favourable_pct", True),
    "score": ("score", True),
    "rs": ("rs_rating", True),
}


def _matches(card: Any, filters: dict[str, Any]) -> bool:
    if (setup := filters.get("setup")) and card.setup != setup:
        return False
    if (result := filters.get("result")) and card.result != result:
        return False
    if (symbol := filters.get("symbol")) and symbol not in card.symbol:
        return False
    if (rs_min := filters.get("rs_min")) is not None and card.rs_rating < rs_min:
        return False
    if (score_min := filters.get("score_min")) is not None and card.score < score_min:
        return False
    if (risk_max := filters.get("risk_max")) is not None:
        # A signal with no recorded stop has no risk to compare; excluding it is
        # the honest reading of "show me setups risking under N%".
        if card.risk_pct is None or card.risk_pct > risk_max:
            return False
    if filters.get("group_top_decile") is True and not card.group_top_decile:
        return False
    if (date_from := filters.get("date_from")) and card.trigger_date < date_from:
        return False
    if (date_to := filters.get("date_to")) and card.trigger_date > date_to:
        return False
    return True


def summarise(cards: Sequence[Any]) -> dict[str, Any]:
    """Outcome statistics over a set of resolved signals.

    Timeouts are counted separately and kept OUT of the win rate denominator's
    numerator but inside its denominator, because a signal that went nowhere for
    ten sessions is a real outcome the trader lived through — folding it into
    either bucket would flatter or damn the setup for free.
    """
    total = len(cards)
    if total == 0:
        return {
            "count": 0, "wins": 0, "losses": 0, "timeouts": 0, "win_rate": None,
            "avg_final_pct": None, "median_final_pct": None,
            "avg_max_favourable_pct": None, "avg_sessions_held": None,
        }
    wins = sum(1 for c in cards if c.result == "win")
    losses = sum(1 for c in cards if c.result == "loss")
    timeouts = sum(1 for c in cards if c.result == "timeout")
    finals = [c.final_pct for c in cards]
    runs = [c.max_favourable_pct for c in cards]
    held = [c.sessions_held for c in cards]
    return {
        "count": total,
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "win_rate": round(wins / total * 100, 1),
        "avg_final_pct": round(sum(finals) / total, 2),
        "median_final_pct": round(median(finals), 2),
        "avg_max_favourable_pct": round(sum(runs) / total, 2),
        "avg_sessions_held": round(sum(held) / total, 1),
    }


def _sorted(cards: list[Any], sort: str) -> list[Any]:
    attribute, descending = SORTS.get(sort, SORTS["recent"])
    return sorted(cards, key=lambda c: getattr(c, attribute), reverse=descending)


def dealt_today(deck: Any, today: date) -> set[str]:
    """Card ids on screen in the drill right now, across every setup filter.

    Cheap (tens of ids out of thousands) and the only thing standing between the
    library and a user reading the answer to the card they are being asked to
    grade.
    """
    ids: set[str] = set()
    setups: Iterable[str | None] = [None, *(deck.meta().get("setups") or [])]
    for setup in setups:
        try:
            ids.update(card.id for card in deck.deal(today, setup=setup))
        except Exception as exc:  # a deck that cannot deal must not break search
            logger.warning("study-library: could not read today's hand (%s)", exc)
            break
    return ids


def query(
    deck: Any,
    *,
    today: date,
    filters: dict[str, Any] | None = None,
    sort: str = "recent",
    limit: int = DEFAULT_PAGE,
    offset: int = 0,
) -> dict[str, Any]:
    """Filter the corpus, and report what the whole match did — not just the page."""
    deck._load()  # noqa: SLF001 — the deck's loader is idempotent and internal by convention
    all_cards = list(getattr(deck, "_cards", {}).values())
    if not all_cards:
        return {
            "available": False,
            "reason": (
                "The study deck has not been generated yet. "
                "Run scripts/generate_study_deck.py to build it."
            ),
        }

    hidden = dealt_today(deck, today)
    corpus = [card for card in all_cards if card.id not in hidden]

    filters = filters or {}
    matched = [card for card in corpus if _matches(card, filters)]
    ordered = _sorted(matched, sort)

    limit = max(1, min(int(limit or DEFAULT_PAGE), MAX_PAGE))
    offset = max(0, int(offset or 0))
    page = ordered[offset:offset + limit]

    # The baseline is the same corpus under the same setup filter but with the
    # discretionary filters dropped. Comparing "VCPs with RS >= 90" against all
    # signals of every setup would credit the RS filter with the difference
    # between two different scanners.
    baseline_filters = {"setup": filters.get("setup")} if filters.get("setup") else {}
    baseline = [card for card in corpus if _matches(card, baseline_filters)]

    return {
        "available": True,
        "total": len(matched),
        "offset": offset,
        "limit": limit,
        "sort": sort if sort in SORTS else "recent",
        "rows": [{**card.answer(), "name": card.name} for card in page],
        "stats": summarise(matched),
        "baseline": summarise(baseline),
        "baseline_label": (
            f"all {filters['setup']} signals" if filters.get("setup") else "every signal in the archive"
        ),
        "hidden_from_todays_drill": len(hidden),
        "meta": deck.meta(),
    }
