"""A stock that set a new high today and closed near it is AT the high.

BLISSGVS on 2026-09-28: high 739.90 over a prior all-time high of 728.00,
close 737.65 (0.30% off the high), +4.7% on 2.8M shares. "ATH Breakouts"
listed it; "All-Time High" came back empty because it demanded a close within
0.2% of the intraday high, and the name was filed under "Near ATH" instead.

Run: `cd backend && pytest tests/test_new_high_scanners.py`
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.scanners import definitions as d


def _snap(**overrides):
    base = dict(
        last_price=737.65, day_high=739.90, ath=739.90, ath_breakout_level=728.00,
        high_52w=739.90, previous_high_52w_level=728.00,
        change_pct=4.68, relative_volume=1.8, stock_return_20d=6.0, stock_return_60d=12.0,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class NewHighScannerTests(unittest.TestCase):
    def test_a_fresh_ath_closing_just_off_the_high_is_an_ath(self):
        self.assertIsNotNone(d._all_time_high(_snap()))
        self.assertIsNotNone(d._high_52w(_snap()))

    def test_it_is_not_also_listed_as_near(self):
        self.assertIsNone(d._near_ath(_snap()))
        self.assertIsNone(d._near_52w_high(_snap()))

    def test_a_new_high_that_faded_hard_is_not_at_the_high(self):
        faded = _snap(last_price=715.0, change_pct=1.5)  # 3.4% off the high
        self.assertIsNone(d._all_time_high(faded))
        self.assertIsNone(d._high_52w(faded))

    def test_without_a_new_high_the_old_band_still_applies(self):
        below = _snap(day_high=727.0, ath=728.00, high_52w=728.00, last_price=725.0)
        self.assertIsNone(d._all_time_high(below))  # 0.41% under an old high

    def test_a_new_high_that_misses_the_volume_rule_stays_near(self):
        # Excluded from Near only when it actually made the at-high list.
        quiet = _snap(relative_volume=0.9, change_pct=0.6)
        self.assertIsNone(d._high_52w(quiet))
        self.assertIsNotNone(d._near_52w_high(quiet))


if __name__ == "__main__":
    unittest.main()
