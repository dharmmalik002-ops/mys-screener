"""Tests for distribution-day counting."""

from __future__ import annotations

import unittest

from app.services import distribution_days as dd
from app.services.market_frame import FrameRow


def row(date: str, close: float, volume: float) -> FrameRow:
    return FrameRow(
        date=date, close=close, high=close, low=close, volume=volume,
        participation=60.0, participation_source="nifty500-breadth",
        above_ma20_pct=60.0, above_ma50_pct=60.0,
        above_sma200_pct=60.0, new_high_52w_pct=0.0, new_low_52w_pct=0.0,
        xp_score=None, xp_regime=None, ma10_pct=None, ma20_pct=None,
    )


def series(specs) -> list[FrameRow]:
    return [row(f"2026-01-{i + 1:02d}", c, v) for i, (c, v) in enumerate(specs)]


class DistributionDayTests(unittest.TestCase):
    def test_down_day_on_higher_volume_counts(self):
        rows = series([(100.0, 1000), (99.0, 1500)])  # -1.0% on rising volume
        out = dd.count_distribution_days(rows)
        self.assertEqual(out["count"], 1)

    def test_down_day_on_lower_volume_does_not_count(self):
        rows = series([(100.0, 1500), (99.0, 1000)])
        self.assertEqual(dd.count_distribution_days(rows)["count"], 0)

    def test_shallow_drop_does_not_count(self):
        rows = series([(100.0, 1000), (99.9, 1500)])  # -0.1%, under the 0.2% floor
        self.assertEqual(dd.count_distribution_days(rows)["count"], 0)

    def test_exactly_at_threshold_counts(self):
        rows = series([(100.0, 1000), (99.8, 1500)])  # exactly -0.2%
        self.assertEqual(dd.count_distribution_days(rows)["count"], 1)

    def test_up_day_never_counts(self):
        rows = series([(100.0, 1000), (101.0, 5000)])
        self.assertEqual(dd.count_distribution_days(rows)["count"], 0)

    def test_zero_volume_newest_bar_falls_back_to_prior_session(self):
        """The real ^NSEI case: newest bar has volume 0 and must not hide a day."""
        rows = series([(100.0, 1000), (99.0, 1500), (98.5, 0)])
        out = dd.count_distribution_days(rows)
        self.assertEqual(out["as_of"], "2026-01-02")   # not the volume-less bar
        self.assertEqual(out["count"], 1)              # the real day still counted
        self.assertEqual(out["trails_price_by_sessions"], 1)

    def test_all_zero_volume_yields_none(self):
        rows = series([(100.0, 0), (99.0, 0)])
        self.assertIsNone(dd.count_distribution_days(rows))

    def test_day_expires_after_25_sessions(self):
        specs = [(100.0, 1000), (99.0, 1500)]                      # distribution at index 1
        specs += [(99.0 + i * 0.001, 1000) for i in range(30)]     # flat drift, no rally
        rows = series(specs)
        early = dd.count_distribution_days(rows, as_of_index=10)
        late = dd.count_distribution_days(rows, as_of_index=28)
        self.assertEqual(early["count"], 1)
        self.assertEqual(late["count"], 0)  # aged out of the 25-session window

    def test_five_percent_rally_cancels_the_day(self):
        rows = series([(100.0, 1000), (99.0, 1500), (104.0, 1000)])  # 104 >= 99*1.05
        self.assertEqual(dd.count_distribution_days(rows, as_of_index=2)["count"], 0)

    def test_rally_cancels_only_the_days_it_clears(self):
        """A rally clears older, lower days but leaves a recent higher one standing."""
        rows = series([
            (100.0, 1000),
            (90.0, 1500),    # low distribution day
            (99.0, 1000),
            (98.0, 1500),    # higher distribution day
            (95.0, 1000),    # 95 >= 90*1.05 clears the first, not the second (98*1.05=102.9)
        ])
        out = dd.count_distribution_days(rows, as_of_index=4)
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["days"][0]["date"], "2026-01-04")

    def test_cluster_counts_only_last_five_sessions(self):
        specs = [(100.0, 1000), (99.0, 1500)]                  # old day
        specs += [(99.0, 1000)] * 8                            # quiet stretch
        specs += [(98.0, 1500)]                                # recent day
        rows = series(specs)
        out = dd.count_distribution_days(rows)
        self.assertEqual(out["count"], 2)
        self.assertEqual(out["cluster_last_5"], 1)

    def test_pressure_labels(self):
        """Bands are calibrated so the measured median (5) reads 'normal'."""
        self.assertEqual(dd._pressure_label(0), "clean")
        self.assertEqual(dd._pressure_label(2), "clean")
        self.assertEqual(dd._pressure_label(3), "normal")
        self.assertEqual(dd._pressure_label(5), "normal")
        self.assertEqual(dd._pressure_label(6), "under pressure")
        self.assertEqual(dd._pressure_label(7), "under pressure")
        self.assertEqual(dd._pressure_label(8), "heavy")

    def test_empty_and_single_bar_are_safe(self):
        self.assertIsNone(dd.count_distribution_days([]))
        self.assertIsNone(dd.count_distribution_days(series([(100.0, 1000)])))

    def test_series_skips_unevaluable_sessions(self):
        rows = series([(100.0, 1000), (99.0, 1500), (98.5, 0)])
        out = dd.distribution_series(rows, sessions=10)
        self.assertEqual([p["date"] for p in out], ["2026-01-02"])

    def test_series_matches_pointwise_count(self):
        specs = [(100.0, 1000)]
        for i in range(20):
            specs.append((100.0 - i * 0.5, 1500 if i % 3 == 0 else 800))
        rows = series(specs)
        for point in dd.distribution_series(rows, sessions=30):
            idx = next(i for i, r in enumerate(rows) if r.date == point["date"])
            self.assertEqual(point["count"], dd.count_distribution_days(rows, as_of_index=idx)["count"])


if __name__ == "__main__":
    unittest.main()
