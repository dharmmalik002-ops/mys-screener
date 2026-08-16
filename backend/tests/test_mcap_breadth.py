"""Tests for the ₹1,000 cr+ breadth store.

The merge test is the important one: the daily bhavcopy job writes one session
at a time into a committed file holding three years, so a merge that replaced
instead of merging would silently delete the archive on the next weekday run.
"""

from __future__ import annotations

import unittest

from app.services import mcap_breadth as mb


class UniverseTests(unittest.TestCase):
    def test_floor_is_inclusive_and_symbols_are_uppercased(self):
        universe = [
            {"symbol": "reliance", "market_cap_crore": 1_770_056.0},
            {"symbol": "EXACTLY", "market_cap_crore": 1000.0},
            {"symbol": "TOOSMALL", "market_cap_crore": 999.9},
            {"symbol": "", "market_cap_crore": 5000.0},
            {"symbol": "NOCAP"},
        ]
        self.assertEqual(mb.universe_symbols(universe), {"RELIANCE", "EXACTLY"})

    def test_floor_is_configurable(self):
        universe = [{"symbol": "A", "market_cap_crore": 600.0}]
        self.assertEqual(mb.universe_symbols(universe, 500.0), {"A"})
        self.assertEqual(mb.universe_symbols(universe, 1000.0), set())

    def test_label_reads_as_a_universe(self):
        self.assertEqual(mb.universe_label(1000.0), "NSE stocks over Rs 1,000 cr")


class DayRowTests(unittest.TestCase):
    def test_missing_average_is_null_not_zero(self):
        """0% would read as "no stock is above its 200-DMA" — maximally bearish
        data invented out of an average that has not warmed up yet."""
        row = mb.build_day_row(
            "2026-08-14",
            {
                "above_ema20_pct": (700, 1400),
                "above_ema21_pct": (700, 1400),
                "above_sma50_pct": (650, 1300),
                "above_sma200_pct": (0, 0),
            },
            total=1400,
        )
        self.assertEqual(row["above_ema20_pct"], 50.0)
        self.assertEqual(row["above_ema21_pct"], 50.0)
        self.assertEqual(row["above_sma50_pct"], 50.0)
        self.assertIsNone(row["above_sma200_pct"])
        self.assertEqual(row["total"], 1400)

    def test_key_absent_from_counts_is_null(self):
        row = mb.build_day_row("2026-08-14", {"above_ema21_pct": (700, 1400)}, total=1400)
        self.assertEqual(row["above_ema21_pct"], 50.0)
        self.assertIsNone(row["above_ema20_pct"])
        self.assertIsNone(row["above_sma50_pct"])

    def test_every_metric_key_is_present(self):
        row = mb.build_day_row("2026-08-14", {}, total=0)
        self.assertEqual(set(mb.METRIC_KEYS) - set(row), set())

    def test_genuine_zero_survives(self):
        row = mb.build_day_row(
            "2026-08-14",
            {key: (0, 1400) for key in mb.METRIC_KEYS},
            total=1400,
        )
        self.assertEqual(row["above_ema21_pct"], 0.0)
        self.assertEqual(row["above_ema20_pct"], 0.0)


class RowsByDateTests(unittest.TestCase):
    def test_all_null_rows_are_dropped(self):
        doc = {"days": [
            {"date": "2026-08-13", "above_ema21_pct": None, "above_sma50_pct": None,
             "above_sma200_pct": None},
            {"date": "2026-08-14", "above_ema21_pct": 47.7},
        ]}
        self.assertEqual(sorted(mb.rows_by_date(doc)), ["2026-08-14"])

    def test_missing_document_is_safe(self):
        self.assertEqual(mb.rows_by_date(None), {})
        self.assertEqual(mb.rows_by_date({}), {})


class MergeTests(unittest.TestCase):
    def test_new_session_is_appended_and_history_is_kept(self):
        existing = [{"date": "2026-08-12", "above_ema21_pct": 40.0},
                    {"date": "2026-08-13", "above_ema21_pct": 45.0}]
        merged = mb.merge_days(existing, [{"date": "2026-08-14", "above_ema21_pct": 47.7}])
        self.assertEqual([r["date"] for r in merged],
                         ["2026-08-12", "2026-08-13", "2026-08-14"])

    def test_rewriting_a_stored_session_corrects_it(self):
        """A session first written off a partial feed must be fixable."""
        existing = [{"date": "2026-08-14", "above_ema21_pct": 12.0, "total": 300}]
        merged = mb.merge_days(existing, [{"date": "2026-08-14", "above_ema21_pct": 47.7, "total": 1497}])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["above_ema21_pct"], 47.7)
        self.assertEqual(merged[0]["total"], 1497)

    def test_output_is_date_sorted_regardless_of_input_order(self):
        merged = mb.merge_days(
            [{"date": "2026-08-14"}, {"date": "2026-08-12"}],
            [{"date": "2026-08-13"}],
        )
        self.assertEqual([r["date"] for r in merged],
                         ["2026-08-12", "2026-08-13", "2026-08-14"])

    def test_rows_without_a_date_are_ignored(self):
        merged = mb.merge_days([{"above_ema21_pct": 1.0}], [{"date": "2026-08-14"}])
        self.assertEqual([r["date"] for r in merged], ["2026-08-14"])


if __name__ == "__main__":
    unittest.main()
