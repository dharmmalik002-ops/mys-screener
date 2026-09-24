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
        # Small caps fall after a bear label: gold is only a hedge where
        # small caps have lost (gotcha 122), so the fixture must show it.
        gold, idx, small, g, x, sm, prev = {}, {}, {}, 100.0, 100.0, 100.0, None
        for d in y2010:
            if prev is not None:
                if reg[prev] == "bear":
                    g *= 1.01
                    sm *= 0.99
                else:
                    x *= 1.01
                    sm *= 1.01
            gold[d], idx[d], small[d], prev = g, x, sm, d
        # 2011: bull, then the label flips to bear on tail[1] — the same day
        # gold jumps 10%. The sleeve held the index into that day.
        for k, d in enumerate(tail):
            reg[d] = "bull_strong" if k < 1 else "bear"
            idx[d] = x
            small[d] = sm
            gold[d] = g * (1.10 if k >= 1 else 1.0)
        lv = sl.build_regime_level(idx, gold, small, reg, set(days))
        self.assertAlmostEqual(lv[tail[1]] / lv[tail[0]], 1.0)      # not credited the jump
        gold[tail[3]] = g * 1.10 * 1.05
        lv = sl.build_regime_level(idx, gold, small, reg, set(days))
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


class MarketConditionTests(unittest.TestCase):
    """Gotcha 122: price level, trend stack, and gold only as a hedge."""

    def setUp(self):
        self.days = [date(2010, 1, 1) + timedelta(days=i) for i in range(800)]

    def test_price_level_reads_distance_from_the_252_day_high(self):
        closes = [100.0] * 300 + [96.0, 90.0, 80.0]
        days = self.days[:len(closes)]
        lab = sl.price_level_labels(dict(zip(days, closes)), {d: "correction" for d in days})
        self.assertEqual(lab[days[300]], "correction|near")
        self.assertEqual(lab[days[301]], "correction|mid")
        self.assertEqual(lab[days[302]], "correction|deep")

    def test_trend_stack_compares_the_20_and_50_day_averages(self):
        closes = [100.0 + i for i in range(60)] + [159.0 - 3 * i for i in range(40)]
        days = self.days[:len(closes)]
        lab = sl.trend_stack_labels(dict(zip(days, closes)), {d: "bull_strong" for d in days})
        self.assertNotIn(days[10], lab)                       # no 50-day average yet
        self.assertEqual(lab[days[55]], "bull_strong|up")
        self.assertEqual(lab[days[99]], "bull_strong|dn")

    def test_gold_is_not_held_where_small_caps_made_money(self):
        # Gold beats both equity legs on bull_strong days, but small caps are
        # up too: gold is a hedge, so the sleeve holds the better equity leg.
        days = self.days
        reg = {d: "bull_strong" for d in days}
        idx = {d: 100.0 * 1.0005 ** i for i, d in enumerate(days)}
        small = {d: 100.0 * 1.001 ** i for i, d in enumerate(days)}
        gold = {d: 100.0 * 1.002 ** i for i, d in enumerate(days)}
        lv = sl.build_regime_level(idx, gold, small, reg, set(days))
        d0, d1 = [d for d in days if d.year == 2011][:2]
        self.assertAlmostEqual(lv[d1] / lv[d0], 1.001)       # small caps, not gold

    def test_the_book_stands_aside_where_the_short_trend_lost(self):
        # Healthy regime throughout; small caps fall on days after the index
        # 20-DMA sits under its 50-DMA, and rise otherwise.
        days = self.days
        closes, c = [], 100.0
        for i in range(len(days)):
            c *= 1.01 if (i // 60) % 2 == 0 else 0.99
            closes.append(c)
        idx = dict(zip(days, closes))
        reg = {d: "bull_strong" for d in days}
        stack = sl.trend_stack_labels(idx, reg)
        small, sm, prev = {}, 100.0, None
        for d in days:
            if prev is not None and prev in stack:
                sm *= 0.99 if stack[prev].endswith("dn") else 1.02
            small[d], prev = sm, d
        _, book_on = sl.risk_on_days(idx, reg, small_close=small)
        later = [d for d in days if d.year == 2011 and d in stack]
        self.assertTrue(any(stack[d].endswith("dn") for d in later))
        self.assertFalse(any(d in book_on for d in later if stack[d].endswith("dn")))
        self.assertTrue(all(d in book_on for d in later if stack[d].endswith("up")))


class LiquidFundTests(unittest.TestCase):
    """Gotcha 123: corrections hold gold and a liquid fund."""

    def test_a_redenomination_is_not_a_return(self):
        d = [date(2015, 8, 27) + timedelta(days=i) for i in range(4)]
        lv = sl.nav_level([(d[0], 20.0), (d[1], 20.002), (d[2], 1982.0), (d[3], 1982.2)])
        self.assertAlmostEqual(lv[d[1]] / lv[d[0]], 1.0001)
        self.assertAlmostEqual(lv[d[2]] / lv[d[1]], 1.0)            # the x99 jump is skipped
        self.assertAlmostEqual(lv[d[3]] / lv[d[2]], 1982.2 / 1982.0)

    def test_a_correction_earns_half_the_gold_move_and_half_the_cash_move(self):
        days = [date(2012, 1, 2) + timedelta(days=i) for i in range(5)]
        reg = {d: "correction" for d in days}
        idx = {d: 100.0 * 0.98 ** i for i, d in enumerate(days)}        # equities falling
        gold = {d: 100.0 * 1.02 ** i for i, d in enumerate(days)}
        cash = {d: 100.0 * 1.0004 ** i for i, d in enumerate(days)}
        lv = sl.build_regime_level(idx, gold, idx, reg, set(days), cash_close=cash)
        self.assertAlmostEqual(lv[days[3]] / lv[days[2]], 1 + 0.5 * 0.02 + 0.5 * 0.0004)


if __name__ == "__main__":
    unittest.main()


class SleeveHoldingsTests(unittest.TestCase):
    """The paper book shows what the sleeve holds, so the record must be right."""

    def setUp(self):
        self.days = [date(2010, 1, 1) + timedelta(days=i) for i in range(900)]

    def test_holdings_record_the_mix_chosen_at_each_close(self):
        idx = series(self.days, lambda i: 100.0 + i)
        gold = series(self.days, lambda i: 100.0)
        reg = {d: "correction" for d in self.days}
        held: dict = {}
        sl.build_sleeve(idx, gold, idx, reg, mode="regime_map", holdings=held)
        self.assertEqual(set(held), set(idx))
        # No liquid-fund series given: corrections fall back to gold/index.
        self.assertEqual(held[self.days[-1]], sl.BLEND_FALLBACK["correction"])
        cash = series(self.days, lambda i: 100.0 * 1.0002 ** i)
        held = {}
        sl.build_sleeve(idx, gold, idx, reg, mode="regime_map", holdings=held, cash_close=cash)
        self.assertEqual(held[self.days[-1]], sl.BLEND_REGIMES["correction"])

    def test_holdings_are_optional_and_do_not_change_the_level(self):
        idx = series(self.days, lambda i: 100.0 + (i % 7))
        gold = series(self.days, lambda i: 100.0 + (i % 5))
        reg = {d: ("bear" if i % 11 == 0 else "bull_strong") for i, d in enumerate(self.days)}
        plain, _ = sl.build_sleeve(idx, gold, idx, reg)
        held: dict = {}
        recorded, _ = sl.build_sleeve(idx, gold, idx, reg, holdings=held)
        self.assertEqual(plain, recorded)
        self.assertTrue(all(abs(sum(m.values()) - 1.0) < 1e-9 for m in held.values()))

    def test_a_nan_print_is_dropped_not_carried(self):
        # Yahoo prints NaN for an unsettled session; it passes `not px` and
        # `px <= 0`, and one NaN turned the whole sleeve level into NaN.
        days = self.days[:3]
        cleaned = sl.clean_series({days[0]: 10.0, days[1]: float("nan"), days[2]: 11.0})
        self.assertEqual(sorted(cleaned), [days[0], days[2]])
