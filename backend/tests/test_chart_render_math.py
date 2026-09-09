from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import chart_render as cr


def make_bars(count: int, *, start: int = 1_700_000_000, step: int = 86_400, base: float = 100.0):
    """Deterministic ascending bars; close walks up by 1 per bar."""
    bars = []
    for i in range(count):
        close = base + i
        bars.append(
            {
                "time": start + i * step,
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000 + i,
            }
        )
    return bars


class SmaOverlayTests(unittest.TestCase):
    def test_emits_none_until_window_minus_one(self) -> None:
        out = cr.sma_overlay(list(range(1, 101)), 50)
        self.assertIsNone(out[48])
        self.assertIsNotNone(out[49])
        self.assertAlmostEqual(out[49], 25.5, places=9)
        self.assertAlmostEqual(out[99], 75.5, places=9)

    def test_rolling_sum_does_not_drift_after_a_large_value(self) -> None:
        values = [1e9] + [1.0] * 60
        out = cr.sma_overlay(values, 5)
        # Once the big value leaves the window every mean is exactly 1.0.
        self.assertAlmostEqual(out[10], 1.0, places=6)
        self.assertAlmostEqual(out[60], 1.0, places=6)

    def test_zero_or_negative_window_is_all_none(self) -> None:
        self.assertEqual(cr.sma_overlay([1.0, 2.0, 3.0], 0), [None, None, None])


class EmaOverlayTests(unittest.TestCase):
    def test_seed_is_sma_written_at_index_span_minus_one(self) -> None:
        out = cr.ema_overlay(list(range(1, 11)), 10)
        # An off-by-one that writes the seed at index `span` passes a bare
        # "is not None" check, so assert the exact indices.
        self.assertTrue(all(v is None for v in out[:9]), out[:9])
        self.assertIsNotNone(out[9])
        self.assertAlmostEqual(out[9], 5.5, places=12)

    def test_returns_all_none_when_history_is_shorter_than_span(self) -> None:
        self.assertEqual(cr.ema_overlay([1.0, 2.0], 10), [None, None])

    def test_recurrence_uses_k_equals_two_over_span_plus_one(self) -> None:
        values = [1.0] * 10 + [11.0]
        out = cr.ema_overlay(values, 10)
        expected = 11.0 * (2 / 11) + 1.0 * (9 / 11)
        self.assertAlmostEqual(out[10], expected, places=12)

    def test_hand_computed_chain_for_span_two(self) -> None:
        out = cr.ema_overlay([2.0, 4.0, 6.0], 2)
        self.assertIsNone(out[0])
        self.assertAlmostEqual(out[1], 3.0, places=12)          # seed = (2+4)/2
        # k = 2/3 -> 6*2/3 + 3*1/3 = 5.0
        self.assertAlmostEqual(out[2], 5.0, places=12)


class ComputeVersusDisplayWindowTests(unittest.TestCase):
    def test_computing_over_full_series_then_slicing_keeps_every_point(self) -> None:
        bars = make_bars(520)
        closes = [b["close"] for b in bars]

        full = cr.sma_overlay(closes, 200)
        window = full[-130:]
        self.assertTrue(all(v is not None for v in window))

        # The naive "slice the bars first" version is entirely empty. This is
        # the bug the compute/display split exists to prevent.
        naive = cr.sma_overlay(closes[-130:], 200)
        self.assertTrue(all(v is None for v in naive))


class FormatScalePriceTests(unittest.TestCase):
    def test_matches_the_frontend_branches(self) -> None:
        cases = [
            (9.5, "9.50"),
            (99.99, "99.99"),
            (100.0, "100.0"),
            (999.94, "999.9"),
            (1000.0, "1000"),
            # The branch is chosen on the raw value, so this stays ungrouped.
            (9999.6, "10000"),
            (10000.0, "10,000"),
            (100000.0, "1,00,000"),
            (1942100.0, "19,42,100"),
            (10000000.0, "1,00,00,000"),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(cr.format_scale_price(value), expected)

    def test_indian_grouping_directly(self) -> None:
        self.assertEqual(cr._indian_group(100), "100")
        self.assertEqual(cr._indian_group(1000), "1,000")
        self.assertEqual(cr._indian_group(100000), "1,00,000")
        self.assertEqual(cr._indian_group(-1942100), "-19,42,100")


class PriceTicksTests(unittest.TestCase):
    def test_residual_at_least_two_picks_step_two(self) -> None:
        # spread 100 / 5 = 20 -> magnitude 10, residual 2 -> step 20
        self.assertEqual(cr.price_ticks(100, 200, 4), [100.0, 120.0, 140.0, 160.0, 180.0, 200.0])

    def test_residual_at_least_five_picks_step_five(self) -> None:
        # spread 300 / 5 = 60 -> magnitude 10, residual 6 -> step 50
        ticks = cr.price_ticks(0, 300, 4)
        self.assertEqual(ticks[:4], [0.0, 50.0, 100.0, 150.0])

    def test_small_residual_picks_step_one(self) -> None:
        # spread 55 / 5 = 11 -> magnitude 10, residual 1.1 -> step 10
        self.assertEqual(cr.price_ticks(0, 55, 4)[:3], [0.0, 10.0, 20.0])

    def test_sub_unit_spread_uses_negative_exponent_magnitude(self) -> None:
        ticks = cr.price_ticks(10.02, 10.08, 4)
        self.assertTrue(ticks)
        self.assertTrue(all(10.02 <= t <= 10.08 for t in ticks), ticks)

    def test_degenerate_equal_bounds_does_not_hang_or_raise(self) -> None:
        self.assertIsInstance(cr.price_ticks(50.0, 50.0, 4), list)


class SanitizeBarsTests(unittest.TestCase):
    def test_dedupes_keeping_last_clamps_ohlc_and_sorts(self) -> None:
        raw = [
            {"time": 300, "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 5},
            {"time": 100, "open": 1, "high": 1, "low": 5, "close": 3, "volume": 1},   # inverted
            {"time": 300, "open": 20, "high": 21, "low": 19, "close": 20.5, "volume": 7},  # dupe
            {"time": 200, "open": 5, "high": 6, "low": 4, "close": 0, "volume": 2},    # close<=0
            {"time": 250, "open": 5, "high": 6, "low": 4, "close": float("nan"), "volume": 2},
        ]
        out = cr.sanitize_bars(raw)
        self.assertEqual([b["time"] for b in out], [100, 300])       # sorted, bad rows dropped
        self.assertEqual(out[1]["close"], 20.5)                       # last dupe won
        self.assertEqual(out[0]["high"], 3)                           # max(high, open, close)
        self.assertEqual(out[0]["low"], 1)                            # min(low, open, close)

    def test_accepts_pydantic_style_objects(self) -> None:
        class Bar:
            def __init__(self):
                self.time, self.open, self.high = 100, 1.0, 2.0
                self.low, self.close, self.volume = 0.5, 1.5, 10

        self.assertEqual(len(cr.sanitize_bars([Bar()])), 1)

    def test_empty_input(self) -> None:
        self.assertEqual(cr.sanitize_bars([]), [])


class MonthAxisLabelsTests(unittest.TestCase):
    @staticmethod
    def _x_of(px_per_index: float):
        return lambda i: i * px_per_index

    def test_one_label_per_month_change_no_year_for_short_span(self) -> None:
        bars = cr.sanitize_bars(make_bars(130))
        labels = cr.month_axis_labels(bars, self._x_of(9.3))
        self.assertGreaterEqual(len(labels), 4)
        self.assertEqual(labels[0][0], 0)
        self.assertTrue(all(len(text) == 3 for _, text in labels), labels)
        self.assertTrue(all(text == text.upper() for _, text in labels))

    def test_long_span_appends_two_digit_year(self) -> None:
        bars = cr.sanitize_bars(make_bars(520))
        labels = cr.month_axis_labels(bars, self._x_of(2.2))
        self.assertTrue(any(len(text) > 3 for _, text in labels), labels)

    def test_series_inside_one_month_yields_one_label(self) -> None:
        # Anchor to the 2nd of a month so 10 daily bars cannot cross into the
        # next one (a mid-month start would, and that is correct behaviour).
        start = int(datetime(2024, 3, 2, 12, 0, tzinfo=cr.IST).timestamp())
        bars = cr.sanitize_bars(make_bars(10, start=start))
        self.assertEqual(len(cr.month_axis_labels(bars, self._x_of(30.0))), 1)

    def test_thinning_drops_labels_closer_than_min_gap(self) -> None:
        bars = cr.sanitize_bars(make_bars(130))
        wide = cr.month_axis_labels(bars, self._x_of(40.0), min_gap_px=34.0)
        narrow = cr.month_axis_labels(bars, self._x_of(1.0), min_gap_px=34.0)
        self.assertGreater(len(wide), len(narrow))
        # The invariant that matters: every kept label clears min_gap from the
        # previous KEPT one (not from the previous candidate).
        for label, previous in zip(narrow[1:], narrow):
            self.assertGreaterEqual(label[0] - previous[0], 34.0)

    def test_empty_bars(self) -> None:
        self.assertEqual(cr.month_axis_labels([], self._x_of(9.0)), [])


class MaSegmentTests(unittest.TestCase):
    def test_none_gap_breaks_the_line_instead_of_interpolating(self) -> None:
        segs = cr._ma_segments([1.0, 2.0, None, 4.0, 5.0])
        self.assertEqual(len(segs), 2)
        self.assertEqual(segs[0], [(0.0, 1.0), (1.0, 2.0)])
        self.assertEqual(segs[1], [(3.0, 4.0), (4.0, 5.0)])

    def test_leading_nones_are_skipped(self) -> None:
        segs = cr._ma_segments([None, None, 3.0, 4.0])
        self.assertEqual(segs, [[(2.0, 3.0), (3.0, 4.0)]])

    def test_single_point_run_is_not_a_segment(self) -> None:
        self.assertEqual(cr._ma_segments([None, 2.0, None]), [])

    def test_all_none(self) -> None:
        self.assertEqual(cr._ma_segments([None] * 5), [])


if __name__ == "__main__":
    unittest.main()
