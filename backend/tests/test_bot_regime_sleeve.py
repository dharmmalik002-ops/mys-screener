"""The market-type sleeve (gotcha 117): causal map, lagged holding."""

from __future__ import annotations

import unittest
from datetime import date, timedelta

from app.services.bot import sleeve as sl


def series(days, f):
    return {d: f(i) for i, d in enumerate(days)}


class RegimeSleeveTests(unittest.TestCase):

    def setUp(self):
        self.days = [date(2010, 1, 1) + timedelta(days=i) for i in range(900)]

    def test_the_map_only_reads_years_before_it(self):
        # Before 2011 gold wins bull_strong days; from 2011 small caps do.
        idx = series(self.days, lambda i: 100.0)
        small = series(self.days, lambda i: 100.0 * (1.003 ** max(0, i - 365)))
        gold = series(self.days, lambda i: 100.0 * (1.001 ** min(i, 365)))
        reg = {d: "bull_strong" for d in self.days}
        self.assertEqual(sl.regime_asset_map(idx, small, gold, reg, 2011)["bull_strong"], "gold")
        self.assertEqual(sl.regime_asset_map(idx, small, gold, reg, 2012)["bull_strong"], "small")

    def test_thin_evidence_falls_back_to_the_default_rule(self):
        idx = series(self.days, lambda i: 100.0 + i)
        reg = {d: ("bear" if i == 10 else "bull_strong") for i, d in enumerate(self.days)}
        m = sl.regime_asset_map(idx, idx, idx, reg, 2012)
        self.assertEqual(m["bear"], "default")

    def test_the_asset_changes_the_session_after_the_label(self):
        y2010 = [d for d in self.days if d.year == 2010]
        tail = [date(2011, 3, 1) + timedelta(days=i) for i in range(4)]
        days = y2010 + tail
        reg = {d: ("bear" if i % 3 == 0 else "bull_strong") for i, d in enumerate(y2010)}
        # 2010: gold rises the session AFTER a bear label, the index the
        # session after a bull one — a day's move belongs to the prior label.
        gold, idx, g, x, prev = {}, {}, 100.0, 100.0, None
        for d in y2010:
            if prev is not None:
                if reg[prev] == "bear":
                    g *= 1.01
                else:
                    x *= 1.01
            gold[d], idx[d], prev = g, x, d
        # 2011: bull, then the label flips to bear on tail[1] — the same day
        # gold jumps 10%. The sleeve held the index into that day.
        for k, d in enumerate(tail):
            reg[d] = "bull_strong" if k < 1 else "bear"
            idx[d] = x
            gold[d] = g * (1.10 if k >= 1 else 1.0)
        lv = sl.build_regime_level(idx, gold, idx, reg, set(days))
        self.assertAlmostEqual(lv[tail[1]] / lv[tail[0]], 1.0)      # not credited the jump
        gold[tail[3]] = g * 1.10 * 1.05
        lv = sl.build_regime_level(idx, gold, idx, reg, set(days))
        self.assertAlmostEqual(lv[tail[3]] / lv[tail[2]], 1.05)     # owns gold from the next session

if __name__ == "__main__":
    unittest.main()
