"""Tests for the self-tuning loop.

The loop's value is entirely in what it REFUSES. Config selection on one
window correlates -0.70 with held-out return here, so a tuner that adopts a
training-window winner is worse than no tuner at all. Most of this file is
that one guard.
"""

from __future__ import annotations

import unittest

from app.services.bot import autotune as at

BASE = {"trail_atr_mult": 8.0, "max_stop_pct": 3.5,
        "max_hold_sessions": 500.0, "pyramid_scale": 0.30}


def scorer(train_gain=0.0, hold_gain=0.0, knob="max_stop_pct", trigger=3.0):
    """A candidate that moves `knob` to `trigger` gets the stated gains."""
    def score(overrides, holdout):
        base = 20.0
        if abs(overrides.get(knob, BASE[knob]) - trigger) < 1e-9:
            base += hold_gain if holdout else train_gain
        return base
    return score


class ValidationTests(unittest.TestCase):

    def test_a_training_only_winner_is_rejected(self):
        """The -0.70 trap, which is the whole reason this module exists."""
        trials = at.evaluate(at.DEFAULT_CANDIDATES, scorer(+5.0, -5.0), BASE)
        tight = next(t for t in trials if t.name == "stop_tighter")
        self.assertFalse(tight.adopted)
        self.assertIn("rejected", tight.note)
        self.assertIsNone(at.adopt(trials))

    def test_a_candidate_winning_both_windows_is_adopted(self):
        trials = at.evaluate(at.DEFAULT_CANDIDATES, scorer(+5.0, +5.0), BASE)
        self.assertEqual(at.adopt(trials).name, "stop_tighter")

    def test_a_held_out_only_winner_is_also_rejected(self):
        """Not a near-miss to be waved through — it never qualified."""
        trials = at.evaluate(at.DEFAULT_CANDIDATES, scorer(-5.0, +5.0), BASE)
        self.assertIsNone(at.adopt(trials))

    def test_a_tie_is_not_a_win(self):
        trials = at.evaluate(at.DEFAULT_CANDIDATES,
                             scorer(at.MIN_EDGE / 2, at.MIN_EDGE / 2), BASE)
        self.assertIsNone(at.adopt(trials))

    def test_adopting_nothing_is_the_normal_outcome(self):
        trials = at.evaluate(at.DEFAULT_CANDIDATES, scorer(), BASE)
        self.assertIsNone(at.adopt(trials))
        self.assertTrue(all(not t.adopted for t in trials))

    def test_the_winner_is_ranked_on_the_held_out_score(self):
        """Training has done its job by qualifying; it must not also pick."""
        def score(overrides, holdout):
            if abs(overrides["max_stop_pct"] - 3.0) < 1e-9:
                return 40.0 if not holdout else 21.0     # huge in train, small out
            if abs(overrides["max_stop_pct"] - 4.5) < 1e-9:
                return 21.0 if not holdout else 30.0     # modest in train, big out
            return 20.0
        trials = at.evaluate(at.DEFAULT_CANDIDATES, score, BASE)
        self.assertEqual(at.adopt(trials).name, "stop_looser")

    def test_the_candidate_set_is_bounded_and_declared(self):
        """An unbounded search over 20 years finds a winner by chance."""
        self.assertLessEqual(len(at.DEFAULT_CANDIDATES), 10)
        self.assertEqual(len({c.name for c in at.DEFAULT_CANDIDATES}),
                         len(at.DEFAULT_CANDIDATES))


if __name__ == "__main__":
    unittest.main()
