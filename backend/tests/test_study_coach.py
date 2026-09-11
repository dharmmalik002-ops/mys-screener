"""Tests for the Chart Gym coach's arithmetic.

The model is handed this output and told to trust it, so a wrong number here
becomes a confident, wrong sentence in front of the user. These pin the parts
that would be invisible if they broke: the sample gate, the selection benchmark
and the feature parsing.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass

from app.services import study_coach as sc


@dataclass
class FakeCard:
    setup: str = "vcp"
    rs_rating: int = 88
    group_top_decile: bool = True
    result: str = "win"
    reasons: tuple[str, ...] = (
        "4 contractions: 16.4% → 6.9% → 6.1% → 2.1%",
        "Base 38d, depth 16.4%, -1.2% below pivot",
        "5D volume 0.42x of 50D avg — drying up",
    )


def entry(i: int, *, r: float | None = 1.0, took: bool = True, waited: int = 3,
          risk: float = 5.0, deck: str = "win") -> dict:
    return {
        "cardId": f"vcp|SYM{i}|2026-05-01",
        "setup": "vcp",
        "symbol": f"SYM{i}",
        "gradedAt": f"2026-05-{(i % 28) + 1:02d}T00:00:00Z",
        "action": "entered" if took else "passed",
        "waited": waited,
        "entry": 100.0,
        "stop": 95.0,
        "riskPct": risk,
        "r": r if took else None,
        "officialResult": deck,
    }


def cards_for(log: list[dict], card: FakeCard | None = None) -> dict:
    card = card or FakeCard()
    return {row["cardId"]: card for row in log}


class SampleGateTests(unittest.TestCase):
    def test_nothing_graded_is_reported_as_not_ready(self):
        out = sc.build([], {})
        self.assertFalse(out["ready"])
        self.assertEqual(out["graded"], 0)

    def test_a_handful_of_trades_is_not_enough_to_be_ready(self):
        log = [entry(i) for i in range(sc.MIN_SAMPLE - 1)]
        out = sc.build(log, cards_for(log))
        self.assertFalse(out["ready"])

    def test_thin_slices_are_dropped_rather_than_reported(self):
        """A three-trade bucket must not become 'you are -0.9R on wide stops'."""
        log = [entry(i, risk=5.0) for i in range(8)] + [entry(100 + i, risk=9.0) for i in range(3)]
        out = sc.build(log, cards_for(log))
        labels = [s["label"] for s in out["slices"]["by_stop_width"]]
        self.assertIn("normal stop (4-7%)", labels)
        self.assertNotIn("wide stop (over 7%)", labels)


class ArithmeticTests(unittest.TestCase):
    def test_overall_numbers_are_plain_averages(self):
        log = [entry(i, r=2.0) for i in range(4)] + [entry(10 + i, r=-1.0) for i in range(4)]
        out = sc.build(log, cards_for(log))
        self.assertTrue(out["ready"])
        self.assertEqual(out["overall"]["taken"], 8)
        self.assertEqual(out["overall"]["avg_r"], 0.5)
        self.assertEqual(out["overall"]["total_r"], 4.0)
        self.assertEqual(out["overall"]["hit_rate_pct"], 50.0)

    def test_selection_edge_is_measured_against_the_decks_coin_flip(self):
        """The deck is 50/50 by construction, so that is the only honest baseline."""
        log = [entry(i, deck="win") for i in range(6)] + [entry(10 + i, deck="loss") for i in range(2)]
        out = sc.build(log, cards_for(log))
        self.assertEqual(out["selection"]["cards_that_were_winners_pct"], 75.0)
        self.assertEqual(out["selection"]["edge_pts"], 25.0)

    def test_passes_are_scored_on_what_the_card_went_on_to_do(self):
        log = [entry(i, took=False, deck="loss") for i in range(5)] + \
              [entry(10 + i, took=False, deck="win") for i in range(3)] + \
              [entry(20 + i) for i in range(sc.MIN_SAMPLE)]
        out = sc.build(log, cards_for(log))
        self.assertEqual(out["pass_quality"]["passes"], 8)
        self.assertEqual(out["pass_quality"]["winners_missed"], 3)
        self.assertEqual(out["pass_quality"]["correct_pct"], 62.5)

    def test_trajectory_compares_the_two_halves(self):
        log = [entry(i, r=-1.0) for i in range(sc.MIN_SAMPLE)] + \
              [entry(50 + i, r=1.0) for i in range(sc.MIN_SAMPLE)]
        out = sc.build(log, cards_for(log))
        self.assertEqual(out["trend"]["direction"], "improving")
        self.assertEqual(out["trend"]["avg_r_change"], 2.0)


class FeatureParsingTests(unittest.TestCase):
    def test_scanner_reasons_yield_the_sliceable_features(self):
        feats = sc._features(FakeCard().reasons)
        self.assertEqual(feats["contractions"], 4)
        self.assertEqual(feats["base_depth_pct"], 16.4)
        self.assertEqual(feats["dryup"], 0.42)

    def test_a_flags_reason_lines_also_parse(self):
        feats = sc._features([
            "High tight flag: steep pole, shallow flag",
            "Pole +61% over 40 sessions",
            "Flag: 14 sessions, 22.0% deep (pivot 248.75)",
        ])
        self.assertEqual(feats["base_depth_pct"], 22.0)

    def test_unparseable_reasons_give_none_rather_than_a_guess(self):
        feats = sc._features(["something the scanner never said"])
        self.assertIsNone(feats["contractions"])
        self.assertIsNone(feats["base_depth_pct"])

    def test_a_missing_card_does_not_drop_the_users_own_decision(self):
        """The deck is rebuilt periodically; an old card id must not erase history."""
        log = [entry(i) for i in range(sc.MIN_SAMPLE)]
        out = sc.build(log, {})
        self.assertTrue(out["ready"])
        self.assertEqual(out["overall"]["taken"], sc.MIN_SAMPLE)


if __name__ == "__main__":
    unittest.main()
