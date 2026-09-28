"""The XP condition label is steadied; the XP score is not.

The label used to follow the band the day's score sat in, so a score grazing a
band line flipped the market condition every day or two (100 changes in 453
sessions). It now changes only once the score has held a new band for
STABLE_HOLD_DAYS sessions, or at once on a DECISIVE_MOVE past the line.
Smoothing the score itself was measured and rejected (it moved the fit to the
author's published EM from MAE 1.26 to 1.94), so the number must stay raw.

Run: `cd backend && pytest tests/test_xp_stable_condition.py`
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from app.services.xp_breadth import (
    DECISIVE_MOVE,
    STABLE_HOLD_DAYS,
    apply_output_calibration,
    regime_for,
    stable_regime_indices,
)

AVOID, CHOPPY, PROGRESSIVE, SWING = 0, 1, 2, 3


class StableConditionTests(unittest.TestCase):
    def test_a_one_day_graze_of_a_band_line_does_not_flip_the_label(self) -> None:
        self.assertEqual(stable_regime_indices([13.0, 11.9, 13.0]), [PROGRESSIVE] * 3)

    def test_a_new_band_held_for_the_hold_period_is_adopted(self) -> None:
        self.assertEqual(STABLE_HOLD_DAYS, 2)
        self.assertEqual(stable_regime_indices([13.0, 11.0, 11.0]), [PROGRESSIVE, PROGRESSIVE, CHOPPY])

    def test_a_decisive_move_is_adopted_the_same_day(self) -> None:
        # 9.5 is the Avoid line; landing DECISIVE_MOVE below it is a crash.
        crash = 9.5 - DECISIVE_MOVE - 0.1
        self.assertEqual(stable_regime_indices([13.0, crash]), [PROGRESSIVE, AVOID])
        # 15 is the Swing line; a thrust past it by the margin also switches at once.
        self.assertEqual(stable_regime_indices([13.0, 15.0 + DECISIVE_MOVE]), [PROGRESSIVE, SWING])

    def test_it_is_causal(self) -> None:
        scores = [13.0, 11.9, 11.0, 5.0, 13.5, 20.0, 12.5, 12.7, 9.0, 9.1]
        full = stable_regime_indices(scores)
        for i in range(1, len(scores) + 1):
            self.assertEqual(stable_regime_indices(scores[:i]), full[:i])

    def test_calibration_keeps_the_score_raw_and_records_the_band(self) -> None:
        series = [{"date": f"d{i}", "xp_score": v} for i, v in enumerate([13.0, 11.9, 13.0])]
        out = apply_output_calibration(series, 1.0, 0.0)
        self.assertEqual([r["xp_score"] for r in out], [13.0, 11.9, 13.0])
        self.assertEqual(out[1]["band_regime"], regime_for(11.9)[0])
        self.assertEqual(out[1]["regime"], regime_for(13.0)[0])

    def test_committed_history_is_steadier_than_its_bands(self) -> None:
        path = Path(__file__).resolve().parents[1] / "data" / "xp_breadth_history.json"
        days = [d for d in json.loads(path.read_text())["days"] if not d.get("warmup")]
        changes = lambda key: sum(1 for a, b in zip(days, days[1:]) if a[key] != b[key])  # noqa: E731
        self.assertLess(changes("regime"), changes("band_regime") * 0.75)


if __name__ == "__main__":
    unittest.main()
