"""Tests for the annotated example library.

Two things carry real weight here: the library must not leak the answers to
the drill running beside it, and the baseline it compares a filter against must
be the right baseline — a comparison against the wrong denominator teaches the
opposite of what it claims.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.services import study_deck as sd
from app.services import study_archive as sl


def _card(**overrides):
    row = {
        "id": "vcp|AAA|2026-01-01",
        "setup": "vcp",
        "label": "VCP",
        "symbol": "AAA",
        "name": "Aaa Ltd",
        "trigger_date": "2026-01-01",
        "entry": 100.0,
        "stop": 97.0,
        "risk_pct": 3.0,
        "score": 100.0,
        "rs_rating": 90,
        "group_top_decile": False,
        "reasons": ["2 contractions"],
        "result": "win",
        "max_favourable_pct": 7.0,
        "final_pct": 5.5,
        "sessions_held": 6,
    }
    row.update(overrides)
    return sd.DeckCard.from_row(row)


class FakeDeck:
    """Stands in for StudyDeck: same duck-typed surface the library uses."""

    def __init__(self, cards, dealt=()):
        self._cards = {c.id: c for c in cards}
        self._dealt = list(dealt)
        self._loaded = True

    def _load(self):
        return None

    def meta(self):
        return {"setups": ["vcp", "high-tight-flag"], "total_cards": len(self._cards)}

    def deal(self, day, setup=None):
        return [c for c in self._dealt if setup is None or c.setup == setup]


TODAY = date(2026, 6, 1)


# ── Drill integrity ──────────────────────────────────────────────────────────


def test_todays_dealt_cards_are_not_searchable():
    """The drill's premise is that the outcome is not in the browser before it
    is earned. A library that answers the card on screen dismantles that."""
    dealt = _card(id="vcp|DEALT|2026-01-02", symbol="DEALT")
    deck = FakeDeck([_card(), dealt], dealt=[dealt])
    result = sl.query(deck, today=TODAY)
    assert result["available"] is True
    assert [row["symbol"] for row in result["rows"]] == ["AAA"]
    assert result["hidden_from_todays_drill"] == 1


def test_hidden_cards_are_excluded_from_the_statistics_too():
    """Excluding a card from the rows but counting it in the win rate would
    still leak — you could infer the answer from the numbers moving."""
    dealt = _card(id="vcp|DEALT|2026-01-02", symbol="DEALT", result="loss", final_pct=-3.0)
    deck = FakeDeck([_card(), dealt], dealt=[dealt])
    stats = sl.query(deck, today=TODAY)["stats"]
    assert stats["count"] == 1
    assert stats["losses"] == 0


def test_a_deck_that_cannot_deal_still_serves_the_library():
    class Broken(FakeDeck):
        def deal(self, day, setup=None):
            raise RuntimeError("deck unavailable")

    result = sl.query(Broken([_card()]), today=TODAY)
    assert result["available"] is True
    assert result["total"] == 1


# ── Filtering ────────────────────────────────────────────────────────────────


def test_failures_are_searchable_which_is_the_whole_point():
    cards = [
        _card(id="a", result="win", final_pct=6.0),
        _card(id="b", result="loss", final_pct=-3.1),
        _card(id="c", result="loss", final_pct=-3.4),
        _card(id="d", result="timeout", final_pct=0.4),
    ]
    result = sl.query(FakeDeck(cards), today=TODAY, filters={"result": "loss"})
    assert result["total"] == 2
    assert all(row["result"] == "loss" for row in result["rows"])


def test_a_signal_with_no_recorded_stop_fails_a_risk_filter():
    """"Show me setups risking under 3%" cannot honestly include one whose risk
    was never recorded."""
    cards = [_card(id="a", risk_pct=2.0), _card(id="b", risk_pct=None, stop=None)]
    result = sl.query(FakeDeck(cards), today=TODAY, filters={"risk_max": 3.0})
    assert [row["id"] for row in result["rows"]] == ["a"]


def test_symbol_search_is_a_substring_match():
    cards = [_card(id="a", symbol="TITAN"), _card(id="b", symbol="INFY")]
    result = sl.query(FakeDeck(cards), today=TODAY, filters={"symbol": "TIT"})
    assert [row["symbol"] for row in result["rows"]] == ["TITAN"]


# ── The teaching layer ───────────────────────────────────────────────────────


def test_the_baseline_holds_the_setup_constant():
    """Comparing "VCPs with RS >= 90" against every signal of every setup would
    credit the RS filter with the difference between two scanners."""
    cards = [
        _card(id="v1", setup="vcp", rs_rating=95, result="win"),
        _card(id="v2", setup="vcp", rs_rating=60, result="loss"),
        _card(id="f1", setup="high-tight-flag", label="Flag", rs_rating=95, result="loss"),
        _card(id="f2", setup="high-tight-flag", label="Flag", rs_rating=95, result="loss"),
    ]
    result = sl.query(FakeDeck(cards), today=TODAY, filters={"setup": "vcp", "rs_min": 90})
    assert result["stats"]["count"] == 1
    # Baseline is all VCPs (2), not all four signals.
    assert result["baseline"]["count"] == 2
    assert "vcp" in result["baseline_label"]


def test_the_baseline_is_the_whole_archive_when_no_setup_is_chosen():
    cards = [_card(id="a", setup="vcp"), _card(id="b", setup="high-tight-flag", label="Flag")]
    result = sl.query(FakeDeck(cards), today=TODAY, filters={"rs_min": 0})
    assert result["baseline"]["count"] == 2
    assert result["baseline_label"] == "every signal in the archive"


def test_timeouts_count_against_the_win_rate_without_becoming_losses():
    """A signal that went nowhere for ten sessions is a real outcome the trader
    sat through. Dropping it would flatter the setup; calling it a loss would
    damn it."""
    cards = [_card(id="a", result="win"), _card(id="b", result="timeout")]
    stats = sl.query(FakeDeck(cards), today=TODAY)["stats"]
    assert stats["count"] == 2
    assert stats["wins"] == 1
    assert stats["timeouts"] == 1
    assert stats["losses"] == 0
    assert stats["win_rate"] == 50.0


def test_statistics_cover_the_whole_match_not_just_the_page():
    """A win rate computed over the first 50 rows of a 400-row match would
    change as you paged, which is worse than showing none."""
    cards = [_card(id=f"w{i}", result="win") for i in range(30)]
    cards += [_card(id=f"l{i}", result="loss", final_pct=-3.0) for i in range(70)]
    result = sl.query(FakeDeck(cards), today=TODAY, limit=10)
    assert len(result["rows"]) == 10
    assert result["total"] == 100
    assert result["stats"]["count"] == 100
    assert result["stats"]["win_rate"] == 30.0


def test_empty_summary_reports_nothing_rather_than_zero():
    """A 0% win rate over no trades reads as a catastrophic setup."""
    stats = sl.summarise([])
    assert stats["count"] == 0
    assert stats["win_rate"] is None
    assert stats["avg_final_pct"] is None


# ── Paging and sorting ───────────────────────────────────────────────────────


@pytest.mark.parametrize("sort,expected_first", [
    ("best", "b"),          # highest final_pct
    ("worst", "c"),         # lowest final_pct
    ("recent", "a"),        # latest trigger_date
    ("oldest", "b"),        # earliest trigger_date
    ("biggest_run", "a"),   # highest max_favourable_pct
])
def test_sorts(sort, expected_first):
    cards = [
        _card(id="a", trigger_date="2026-03-01", final_pct=1.0, max_favourable_pct=2.0),
        _card(id="b", trigger_date="2026-01-01", final_pct=8.0, max_favourable_pct=1.0),
        _card(id="c", trigger_date="2026-02-01", final_pct=-4.0, max_favourable_pct=0.5),
    ]
    rows = sl.query(FakeDeck(cards), today=TODAY, sort=sort)["rows"]
    assert rows[0]["id"] == expected_first


def test_page_size_is_capped():
    cards = [_card(id=f"c{i}") for i in range(500)]
    result = sl.query(FakeDeck(cards), today=TODAY, limit=10_000)
    assert len(result["rows"]) == sl.MAX_PAGE


def test_an_ungenerated_deck_says_so():
    result = sl.query(FakeDeck([]), today=TODAY)
    assert result["available"] is False
    assert "generate_study_deck" in result["reason"]
