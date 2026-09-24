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


class SidewaysMarketTests(unittest.TestCase):
    """Gotcha 121: corrections hold a mix; the book stands aside where small caps lost."""

    def setUp(self):
        self.days = [date(2010, 1, 1) + timedelta(days=i) for i in range(900)]

    def test_a_correction_holds_half_gold_half_index(self):
        days = self.days[:5]
        reg = {d: "correction" for d in days}
        idx = {d: 100.0 * (1.02 ** i) for i, d in enumerate(days)}
        gold = {d: 100.0 for d in days}
        lv = sl.build_regime_level(idx, gold, idx, reg, set(days))
        # From the second session on, each +2% index day is worth +1%.
        self.assertAlmostEqual(lv[days[3]] / lv[days[2]], 1.01)

    def test_the_book_stands_aside_only_on_prior_evidence(self):
        # bull_narrow every third day; small caps fall on the day after it.
        reg = {d: ("bull_narrow" if i % 3 == 0 else "bull_strong") for i, d in enumerate(self.days)}
        small, x, prev = {}, 100.0, None
        for d in self.days:
            if prev is not None:
                x *= 0.99 if reg[prev] == "bull_narrow" else 1.01
            small[d], prev = x, d
        idx = {d: 100.0 for d in self.days}
        self.assertEqual(sl.small_cap_losing_regimes(idx, small, reg, 2010), set())
        self.assertEqual(sl.small_cap_losing_regimes(idx, small, reg, 2011), {"bull_narrow"})
        _, book_on = sl.risk_on_days(idx, reg, small_close=small)
        narrow_2010 = [d for d in self.days if d.year == 2010 and reg[d] == "bull_narrow"]
        narrow_2011 = [d for d in self.days if d.year == 2011 and reg[d] == "bull_narrow"]
        self.assertTrue(all(d in book_on for d in narrow_2010))      # no evidence yet
        self.assertFalse(any(d in book_on for d in narrow_2011))     # judged on 2010
        self.assertTrue(all(d in book_on for d in self.days if reg[d] == "bull_strong"))

    def test_without_small_caps_the_book_rule_is_unchanged(self):
        reg = {d: "bull_narrow" for d in self.days}
        idx = {d: 100.0 for d in self.days}
        _, book_on = sl.risk_on_days(idx, reg)
        self.assertEqual(book_on, set(self.days))


if __name__ == "__main__":
    unittest.main()
