"""Tests for the exposure verdict.

The resolution-gate test is the one that matters most: without it the model
raises exposure on the least-resolved, most upward-biased week in the file.
"""

from __future__ import annotations

import unittest

from app.services import exposure_model as em

STOP = 3.0
TARGET = 5.0
BREAKEVEN = 37.5  # 100 * 3 / (3 + 5)


def stats(weeks, *, comparable_offset=-10.0):
    """weeks: [(name, win_rate, resolved, total_signals)]

    Mirrors the real file: the full-horizon `weeks` block carries the win rate
    the verdict uses, and `comparable` carries a systematically lower
    short-horizon figure (measured ~10 points lower on the shipped data). The
    offset is here so a regression back to reading `comparable` fails loudly
    instead of quietly shifting every number.
    """
    return {
        "weeks": [
            {
                "week": w,
                "total_signals": total,
                "overall": {"resolved": resolved, "win_rate": wr},
            }
            for w, wr, resolved, total in weeks
        ],
        "comparable": {
            "horizon_sessions": 4,
            "weeks": [
                {"week": w, "overall": {"win_rate": max(0.0, wr + comparable_offset)}}
                for w, wr, _r, _t in weeks
            ],
        },
    }


def full(week, win_rate, n=1000):
    """A fully-resolved week."""
    return (week, win_rate, n, n)


class BreakevenTests(unittest.TestCase):
    def test_matches_the_users_rules(self):
        self.assertEqual(em.breakeven_win_rate(3.0, 5.0), 37.5)

    def test_symmetric_stop_and_target(self):
        self.assertEqual(em.breakeven_win_rate(5.0, 5.0), 50.0)

    def test_invalid_inputs_return_none(self):
        self.assertIsNone(em.breakeven_win_rate(0, 5))
        self.assertIsNone(em.breakeven_win_rate(3, 0))
        self.assertIsNone(em.breakeven_win_rate(-3, 5))


class LadderTests(unittest.TestCase):
    def test_ladder_steps(self):
        self.assertEqual(em.exposure_for(BREAKEVEN + 6, BREAKEVEN), 100)
        self.assertEqual(em.exposure_for(BREAKEVEN + 5, BREAKEVEN), 100)  # boundary
        self.assertEqual(em.exposure_for(BREAKEVEN + 1, BREAKEVEN), 75)
        self.assertEqual(em.exposure_for(BREAKEVEN, BREAKEVEN), 75)      # boundary
        self.assertEqual(em.exposure_for(BREAKEVEN - 4, BREAKEVEN), 50)
        self.assertEqual(em.exposure_for(BREAKEVEN - 5, BREAKEVEN), 50)  # boundary
        self.assertEqual(em.exposure_for(BREAKEVEN - 6, BREAKEVEN), 25)

    def test_ladder_is_monotone(self):
        prev = 0
        for wr in [x / 2 for x in range(0, 200)]:
            cur = em.exposure_for(wr, BREAKEVEN)
            self.assertGreaterEqual(cur, prev)
            prev = cur


class ResolutionGateTests(unittest.TestCase):
    def test_partially_resolved_weeks_are_excluded(self):
        """The real trap: W31 reads 43.2% at 76% resolved."""
        s = stats([
            full("2026-W28", 20.0), full("2026-W29", 20.0), full("2026-W30", 20.0),
            ("2026-W31", 43.2, 760, 1000),   # 76% resolved
            ("2026-W32", 33.2, 380, 1000),   # 38% resolved
        ])
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertNotIn("2026-W31", out["weeks_used"])
        self.assertNotIn("2026-W32", out["weeks_used"])
        self.assertIn("2026-W31", out["weeks_excluded_unresolved"])
        self.assertIn("2026-W32", out["weeks_excluded_unresolved"])
        self.assertEqual(out["win_rate"], 20.0)

    def test_gate_boundary_at_97_percent(self):
        s = stats([full("A", 20.0), full("B", 20.0), ("C", 90.0, 970, 1000)])
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertIn("C", out["weeks_used"])  # exactly 97% is eligible

    def test_unavailable_below_two_eligible_weeks(self):
        s = stats([full("A", 40.0), ("B", 90.0, 500, 1000)])
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertFalse(out["available"])
        self.assertIn("resolved", out["reason"])

    def test_missing_total_signals_is_not_eligible(self):
        """No denominator means resolution is unknown, which is not the same as 100%."""
        s = {
            "weeks": [{"week": "A", "overall": {"resolved": 100}}],
            "comparable": {"weeks": [{"week": "A", "overall": {"win_rate": 90.0}}]},
        }
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertFalse(out["available"])


class HorizonBasisTests(unittest.TestCase):
    """The verdict must read the full-horizon win rate, never `comparable`.

    Break-even (37.5% for a 3/5 pair) describes a finished trade. The
    `comparable` block scores every week at 4 sessions, where most trades have
    not finished, and runs ~10 points lower on the real data — judging it
    against a whole-trade break-even would understate the edge by construction.
    """

    def test_uses_full_horizon_not_comparable(self):
        s = stats([full(f"W{i}", 45.0) for i in range(4)], comparable_offset=-20.0)
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(out["win_rate"], 45.0)   # not 25.0
        self.assertTrue(out["clears_breakeven"])

    def test_absent_comparable_block_is_harmless(self):
        s = stats([full(f"W{i}", 45.0) for i in range(4)])
        s.pop("comparable")
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertTrue(out["available"])
        self.assertEqual(out["win_rate"], 45.0)


class VerdictTests(unittest.TestCase):
    def test_below_breakeven_gives_selective(self):
        s = stats([full(f"W{i}", 33.2) for i in range(4)])
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertTrue(out["available"])
        self.assertEqual(out["exposure_pct"], 50)
        self.assertEqual(out["band"], "Selective")
        self.assertFalse(out["clears_breakeven"])
        self.assertAlmostEqual(out["shortfall_pts"], 4.3, places=2)

    def test_expectancy_sign_matches_breakeven(self):
        below = em.compute_exposure(stats([full(f"W{i}", 33.2) for i in range(4)]),
                                    stop_pct=STOP, win_pct=TARGET)
        above = em.compute_exposure(stats([full(f"W{i}", 45.0) for i in range(4)]),
                                    stop_pct=STOP, win_pct=TARGET)
        self.assertLess(below["expected_pct_per_trade"], 0)
        self.assertGreater(above["expected_pct_per_trade"], 0)
        self.assertEqual(above["shortfall_pts"], 0.0)

    def test_expectancy_is_zero_at_breakeven(self):
        out = em.compute_exposure(stats([full(f"W{i}", BREAKEVEN) for i in range(4)]),
                                  stop_pct=STOP, win_pct=TARGET)
        self.assertAlmostEqual(out["expected_pct_per_trade"], 0.0, places=2)

    def test_smoothing_uses_only_the_last_four_weeks(self):
        s = stats([full("W1", 90.0), full("W2", 20.0), full("W3", 20.0),
                   full("W4", 20.0), full("W5", 20.0)])
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(out["weeks_used"], ["W2", "W3", "W4", "W5"])
        self.assertEqual(out["win_rate"], 20.0)  # the 90% week is outside the window

    def test_weighting_is_by_resolved_signals(self):
        """A thin week must not outvote a busy one."""
        s = stats([("A", 10.0, 100, 100), ("B", 50.0, 900, 900)])
        out = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertAlmostEqual(out["win_rate"], 46.0, places=1)  # not the 30.0 simple mean

    def test_direction_needs_more_than_the_deadband(self):
        flat = em.compute_exposure(
            stats([full(f"W{i}", 30.0) for i in range(6)]), stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(flat["direction"], "stable")

        rising = em.compute_exposure(
            stats([full("W1", 10.0), full("W2", 10.0), full("W3", 10.0),
                   full("W4", 10.0), full("W5", 60.0)]),
            stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(rising["direction"], "improving")

    def test_direction_unknown_without_a_prior_window(self):
        out = em.compute_exposure(stats([full("A", 30.0), full("B", 30.0)]),
                                  stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(out["direction"], "unknown")
        self.assertIsNone(out["direction_change_pts"])

    def test_rules_are_echoed_for_display(self):
        out = em.compute_exposure(stats([full(f"W{i}", 33.2) for i in range(4)]),
                                  stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(out["rules"]["stop_pct"], STOP)
        self.assertEqual(out["rules"]["full_exposure_at"], 42.5)
        self.assertEqual(out["rules"]["floor_below"], 32.5)

    def test_empty_and_none_stats_are_safe(self):
        for bad in (None, {}, {"weeks": [], "comparable": {}}):
            out = em.compute_exposure(bad, stop_pct=STOP, win_pct=TARGET)
            self.assertFalse(out["available"])


class SeriesTests(unittest.TestCase):
    def test_series_matches_the_headline_computation(self):
        s = stats([full(f"W{i}", 20.0 + i * 3) for i in range(8)])
        series = em.exposure_series(s, stop_pct=STOP, win_pct=TARGET)
        headline = em.compute_exposure(s, stop_pct=STOP, win_pct=TARGET)
        self.assertEqual(series[-1]["exposure_pct"], headline["exposure_pct"])
        self.assertEqual(series[-1]["smoothed_win_rate"], headline["win_rate"])

    def test_series_is_empty_without_enough_weeks(self):
        self.assertEqual(em.exposure_series(stats([full("A", 30.0)]), stop_pct=STOP, win_pct=TARGET), [])

    def test_series_carries_resolution_for_greying_out(self):
        s = stats([full("A", 30.0), full("B", 30.0), ("C", 90.0, 380, 1000)])
        series = em.exposure_series(s, stop_pct=STOP, win_pct=TARGET)
        last = [p for p in series if p["week"] == "C"]
        self.assertTrue(last)
        self.assertFalse(last[0]["eligible"])
        self.assertEqual(last[0]["resolution_pct"], 38.0)


if __name__ == "__main__":
    unittest.main()
