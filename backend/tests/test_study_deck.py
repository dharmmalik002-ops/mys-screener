"""Tests for the chart-reading drill deck.

Two properties carry the whole exercise and neither is visible by inspecting a
card: that wins and losses are dealt in equal measure, and that the answer never
travels with the question. Both are pinned here.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from app.services import study_deck as sd


def card_row(index: int, result: str, setup: str = "vcp") -> dict:
    return {
        "id": f"{setup}|SYM{index}|2026-05-{(index % 28) + 1:02d}",
        "setup": setup,
        "label": "VCP" if setup == "vcp" else "High Tight Flag",
        "symbol": f"SYM{index}",
        "name": f"Symbol {index}",
        "trigger_date": f"2026-05-{(index % 28) + 1:02d}",
        "entry": 100.0 + index,
        "stop": 95.0 + index,
        "risk_pct": 5.0,
        "score": 90.0,
        "rs_rating": 85,
        "group_top_decile": False,
        "reasons": ["3 contractions: 20% -> 9% -> 4%"],
        "result": result,
        "max_favourable_pct": 8.0,
        "final_pct": 5.0,
        "sessions_held": 4,
    }


def write_deck(directory: Path, rows: list[dict]) -> None:
    (directory / sd.DECK_FILENAME).write_text(
        json.dumps(
            {
                "generated_at": datetime(2026, 9, 1, tzinfo=timezone.utc).isoformat(),
                "window_start": "2025-09-01",
                "window_end": "2026-09-01",
                "cards": rows,
            }
        ),
        encoding="utf-8",
    )


class DeckDealingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        rows = [card_row(i, "win") for i in range(40)] + [card_row(100 + i, "loss") for i in range(40)]
        write_deck(self.dir, rows)
        self.deck = sd.StudyDeck(self.dir)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_hand_is_balanced_between_wins_and_losses(self):
        """A deck of winners teaches that every base breaks out. It must not be."""
        cards = self.deck.deal(date(2026, 9, 10), count=20)
        self.assertEqual(len(cards), 20)
        wins = sum(1 for c in cards if c.result == "win")
        self.assertEqual(wins, 10, "expected an even split of wins and losses")

    def test_consecutive_days_do_not_repeat_cards(self):
        first = {c.id for c in self.deck.deal(date(2026, 9, 10), count=20)}
        second = {c.id for c in self.deck.deal(date(2026, 9, 11), count=20)}
        self.assertEqual(first & second, set())

    def test_same_day_deals_the_same_hand(self):
        """The deck must not reshuffle under a page refresh mid-session."""
        first = [c.id for c in self.deck.deal(date(2026, 9, 10), count=20)]
        second = [c.id for c in self.deck.deal(date(2026, 9, 10), count=20)]
        self.assertEqual(first, second)

    def test_question_never_carries_the_answer(self):
        card = self.deck.deal(date(2026, 9, 10), count=2)[0]
        question = card.question()
        for leaked in ("result", "reasons", "stop", "scanner_stop", "max_favourable_pct", "final_pct", "score"):
            self.assertNotIn(leaked, question, f"{leaked} leaked into the question payload")
        self.assertIn("entry", question)

    def test_answer_carries_the_scanner_reasoning(self):
        answer = self.deck.deal(date(2026, 9, 10), count=2)[0].answer()
        self.assertIn("reasons", answer)
        self.assertIn(answer["result"], ("win", "loss", "timeout"))

    def test_setup_filter_restricts_the_hand(self):
        rows = [card_row(i, "win") for i in range(10)] + [
            card_row(200 + i, "loss", setup="high-tight-flag") for i in range(10)
        ]
        write_deck(self.dir, rows)
        deck = sd.StudyDeck(self.dir)
        cards = deck.deal(date(2026, 9, 10), count=6, setup="high-tight-flag")
        self.assertTrue(cards)
        self.assertTrue(all(c.setup == "high-tight-flag" for c in cards))


class DeckResilienceTests(unittest.TestCase):
    def test_missing_deck_file_deals_nothing_instead_of_raising(self):
        with tempfile.TemporaryDirectory() as tmp:
            deck = sd.StudyDeck(Path(tmp))
            self.assertEqual(deck.deal(date(2026, 9, 10)), [])
            self.assertEqual(deck.meta()["total_cards"], 0)

    def test_unresolved_signals_are_dropped(self):
        """An "open" signal ran past the end of the data — there is no answer to
        grade against, so it must never be dealt."""
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            write_deck(directory, [card_row(1, "win"), card_row(2, "open"), card_row(3, "loss")])
            deck = sd.StudyDeck(directory)
            self.assertEqual(deck.meta()["total_cards"], 2)


class SplitBarsTests(unittest.TestCase):
    def test_split_puts_the_trigger_bar_last_in_the_question(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "chart_cache").mkdir()
            # 2026-05-04 .. 2026-05-13, one bar per day at midnight UTC.
            base = int(datetime(2026, 5, 4, tzinfo=timezone.utc).timestamp())
            bars = [
                {"time": base + i * 86400, "open": 10, "high": 11, "low": 9, "close": 10 + i, "volume": 100}
                for i in range(10)
            ]
            (directory / "chart_cache" / "SYM__1D.json").write_text(
                json.dumps({"symbol": "SYM", "bars": bars}), encoding="utf-8"
            )
            context, forward = sd.split_bars(directory, "SYM", "2026-05-07", context=100, forward=3)
            self.assertEqual(len(context), 4)
            self.assertEqual(len(forward), 3)
            self.assertLess(context[-1]["time"], forward[0]["time"])

    def test_forward_window_covers_waiting_plus_holding(self):
        """The drill lets the user wait before entering and then hold, so a card
        has to carry both halves — a 10-bar window would strand anyone who
        waited more than a session or two."""
        self.assertEqual(sd.REVEAL_BARS, sd.WAIT_BARS + sd.HOLD_BARS)
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "chart_cache").mkdir()
            base = int(datetime(2026, 5, 4, tzinfo=timezone.utc).timestamp())
            bars = [
                {"time": base + i * 86400, "open": 10, "high": 11, "low": 9, "close": 10 + i, "volume": 100}
                for i in range(60)
            ]
            (directory / "chart_cache" / "SYM__1D.json").write_text(
                json.dumps({"symbol": "SYM", "bars": bars}), encoding="utf-8"
            )
            trigger = datetime.fromtimestamp(base + 5 * 86400, tz=timezone.utc).date().isoformat()
            _, forward = sd.split_bars(directory, "SYM", trigger)
            self.assertEqual(len(forward), sd.REVEAL_BARS)

    def test_unknown_symbol_returns_empty_rather_than_raising(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(sd.split_bars(Path(tmp), "NOPE", "2026-05-07"), ([], []))


if __name__ == "__main__":
    unittest.main()
