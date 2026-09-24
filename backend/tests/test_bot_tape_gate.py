"""Gotcha 122: the learned tape gate — no new buys while the index 20-DMA sits
under its 50-DMA, switched on only by the bot's own CLOSED record."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "rules_walkforward", Path(__file__).resolve().parents[1] / "scripts" / "rules_walkforward.py")
wf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wf)


def pick(day, r, weak, exit_day="2015-06-01", reason="trail"):
    return {"signal_day": day, "entry_day": day, "exit_day": exit_day, "exit_reason": reason,
            "r_multiple": r, "_weak": weak}


WEAK = lambda t: t["_weak"]  # noqa: E731


class TapeGateTests(unittest.TestCase):

    def test_it_blocks_when_weak_tape_buys_earned_less(self):
        picks = [pick("2014-01-02", 0.5, True)] * 25 + [pick("2014-01-02", 2.0, False)] * 50
        self.assertTrue(wf.learn_tape_gate(picks, "2016-01-01", WEAK)["block"])

    def test_it_stays_off_when_weak_tape_buys_did_as_well(self):
        picks = [pick("2014-01-02", 2.5, True)] * 25 + [pick("2014-01-02", 2.0, False)] * 50
        self.assertFalse(wf.learn_tape_gate(picks, "2016-01-01", WEAK)["block"])

    def test_it_needs_enough_evidence(self):
        picks = [pick("2014-01-02", -1.0, True)] * 5 + [pick("2014-01-02", 2.0, False)] * 50
        self.assertFalse(wf.learn_tape_gate(picks, "2016-01-01", WEAK)["block"])

    def test_only_trades_closed_before_the_cut_count(self):
        # The losing weak-tape trades close AFTER the cut, so they are the future.
        picks = ([pick("2014-01-02", -1.0, True, exit_day="2016-03-01")] * 25
                 + [pick("2014-01-02", -1.0, True, reason="open")] * 25
                 + [pick("2014-01-02", 2.0, False)] * 50)
        gate = wf.learn_tape_gate(picks, "2016-01-01", WEAK)
        self.assertFalse(gate["block"])
        self.assertEqual(gate["weak_n"], 0)


if __name__ == "__main__":
    unittest.main()
