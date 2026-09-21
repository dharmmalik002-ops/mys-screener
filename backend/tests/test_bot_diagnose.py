"""Tests for the year-by-year self-diagnosis.

The distinction that matters is `starved` versus `bad_shots`: a year with 23
trades and a year with 731 both produce an unimpressive line in a returns
table, and they need opposite fixes. Confusing them is what sent six rounds of
work at the entry rules when the problem was that the book was in cash.
"""

from __future__ import annotations

import unittest

from app.services.bot import diagnose as dg


def sig(year, r=1.0, n=1):
    return [{"entry_day": f"{year}-06-01", "r_multiple": r} for _ in range(n)]


class VerdictTests(unittest.TestCase):

    def test_a_year_with_almost_no_trades_reads_starved(self):
        rows = dg.diagnose(sig(2011, n=500), sig(2011, r=2.0, n=10),
                           {2011: 1.0}, {2011: 40.0})
        self.assertEqual(rows[0].verdict, "starved")

    def test_starved_beats_the_other_verdicts_even_with_terrible_returns(self):
        """Thin years must not be blamed on stock picking."""
        rows = dg.diagnose(sig(2011, n=500), sig(2011, r=-1.0, n=5),
                           {2011: -30.0}, {2011: 50.0})
        self.assertEqual(rows[0].verdict, "starved")

    def test_many_trades_at_negative_r_reads_bad_shots(self):
        rows = dg.diagnose(sig(2019, n=900), sig(2019, r=-0.4, n=200),
                           {2019: -10.0}, {2019: -2.0})
        self.assertEqual(rows[0].verdict, "bad_shots")

    def test_good_r_but_still_behind_reads_under_deployed(self):
        rows = dg.diagnose(sig(2023, n=9000), sig(2023, r=3.0, n=700),
                           {2023: 37.0}, {2023: 48.0})
        self.assertEqual(rows[0].verdict, "under_deployed")
        self.assertIn("capital", rows[0].note)

    def test_beating_the_index_reads_ok(self):
        rows = dg.diagnose(sig(2024, n=9000), sig(2024, r=2.0, n=300),
                           {2024: 62.0}, {2024: 26.0})
        self.assertEqual(rows[0].verdict, "ok")

    def test_a_missing_index_year_does_not_invent_an_alpha(self):
        rows = dg.diagnose(sig(2026, n=900), sig(2026, r=1.0, n=100), {2026: 5.0}, {})
        self.assertIsNone(rows[0].alpha)


class SummaryTests(unittest.TestCase):

    def test_summary_counts_and_ranks_the_worst_years(self):
        rows = dg.diagnose(
            sig(2023, n=9000) + sig(2024, n=9000),
            sig(2023, r=3.0, n=700) + sig(2024, r=2.0, n=300),
            {2023: 37.0, 2024: 62.0}, {2023: 48.0, 2024: 26.0},
        )
        s = dg.summarise(rows)
        self.assertEqual(s["years_behind"], 1)
        self.assertEqual(s["years_total"], 2)
        self.assertEqual(s["worst"][0]["year"], 2023)


if __name__ == "__main__":
    unittest.main()
