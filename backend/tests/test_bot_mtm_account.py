"""Tests for the marked-to-market account.

The bug this module exists to fix was invisible and inverted a whole report:
booking a trade's profit on its exit day put a 2023 position's entire gain
into 2024, so "which years does the bot struggle in" had the wrong answer.
These pin the properties that make the yearly table mean what it says.
"""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from app.services.bot import mtm_account as mtm
from app.services.bot.portfolio import PortfolioConfig


def write_bars(data_dir: Path, symbol: str, days, closes):
    store = data_dir / "deep_history"
    store.mkdir(parents=True, exist_ok=True)
    # The store's own schema: date ordinals under "d", OHLCV under o/h/l/c/v.
    payload = {
        "symbol": symbol,
        "d": [d.toordinal() for d in days],
        "o": list(closes), "h": list(closes),
        "l": list(closes), "c": list(closes),
        "v": [1000.0] * len(closes),
    }
    with gzip.open(store / f"{symbol}.json.gz", "wt") as fh:
        json.dump(payload, fh)


class MarkToMarketTests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(400)]

    def tearDown(self):
        self._tmp.cleanup()

    def _cfg(self):
        return PortfolioConfig(risk_per_trade_pct=1.0, watch_risk_pct=1.0,
                               max_concurrent=10, max_portfolio_risk_pct=50.0,
                               max_deployed_pct=100.0, max_position_pct=50.0)

    def test_profit_shows_up_while_the_trade_is_still_open(self):
        """The whole point. A rising position lifts equity before it closes."""
        closes = [100.0 + i for i in range(400)]          # straight up
        write_bars(self.dir, "AAA", self.days, closes)
        trade = {"symbol": "AAA", "entry_day": "2020-01-02", "exit_day": "2020-12-31",
                 "r_multiple": 5.0, "risk_pct": 5.0, "entry": 101.0}
        r = mtm.simulate([trade], self.dir, self._cfg())
        self.assertIsNotNone(r)
        mid = [p for p in r.equity_curve if p["day"] == "2020-06-01"][0]
        self.assertGreater(mid["equity"], 1_000_000.0,
                           "open profit was not marked into equity")

    def test_drawdown_sees_a_round_trip_the_realised_curve_would_miss(self):
        up = [100.0 + i * 4 for i in range(200)]
        down = [up[-1] - i * 4 for i in range(200)]
        write_bars(self.dir, "BBB", self.days, up + down)
        trade = {"symbol": "BBB", "entry_day": "2020-01-02", "exit_day": "2021-01-30",
                 "r_multiple": 0.0, "risk_pct": 5.0, "entry": 104.0}
        r = mtm.simulate([trade], self.dir, self._cfg())
        self.assertLess(r.max_drawdown_pct, -1.0,
                        "a position that doubled and gave it all back showed no drawdown")

    def test_yearly_returns_split_at_the_year_boundary(self):
        """A position flat through 2020 must not borrow 2021's gain."""
        flat = (date(2021, 1, 1) - date(2020, 1, 1)).days      # 366
        closes = [100.0] * flat + [200.0] * (len(self.days) - flat)
        write_bars(self.dir, "CCC", self.days, closes)
        trade = {"symbol": "CCC", "entry_day": "2020-01-02", "exit_day": "2021-01-30",
                 "r_multiple": 3.0, "risk_pct": 5.0, "entry": 100.0}
        r = mtm.simulate([trade], self.dir, self._cfg())
        self.assertIn(2020, r.yearly)
        self.assertLess(abs(r.yearly[2020]), 1.0, "flat year should read flat")


class PositionCapTests(unittest.TestCase):
    """The cap must shrink the RISK too, not just the capital deployed."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(120)]

    def tearDown(self):
        self._tmp.cleanup()

    def test_a_capped_position_books_pnl_on_what_it_was_allowed_to_take(self):
        """The bug this test exists for inflated every result by ~2x.

        A 0.25% risk budget behind a 1% stop asks for 25% of equity. Capped at
        5%, the money actually at risk is a fifth of the budget — but the trade
        was still booking `risk_amount * r` on the uncapped figure. Every trade
        in the real book clipped, so every number was wrong.
        """
        write_bars(self.dir, "GGG", self.days, [100.0] * 120)
        trade = {"symbol": "GGG", "entry_day": "2020-01-02", "exit_day": "2020-03-01",
                 "r_multiple": 10.0, "risk_pct": 1.0, "entry": 100.0}
        cfg = PortfolioConfig(risk_per_trade_pct=0.25, watch_risk_pct=0.25,
                              max_concurrent=10, max_portfolio_risk_pct=50.0,
                              max_deployed_pct=100.0, max_position_pct=5.0)
        r = mtm.simulate([trade], self.dir, cfg)
        # 5% of equity behind a 1% stop risks 0.05% of equity; at +10R that is
        # a 0.5% gain, not the 2.5% the uncapped budget would have booked.
        gain = r.equity_curve[-1]["equity"] / cfg.starting_equity - 1.0
        self.assertLess(gain, 0.01, "P&L was booked on an uncapped position")
        self.assertGreater(gain, 0.001)


class ParkedCashTests(unittest.TestCase):
    """Idle capital riding an index. Two attempts at this created money."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(200)]

    def tearDown(self):
        self._tmp.cleanup()

    def _trade(self):
        return {"symbol": "HHH", "entry_day": "2020-02-01", "exit_day": "2020-04-01",
                "r_multiple": 1.0, "risk_pct": 5.0, "entry": 100.0}

    def test_a_flat_index_leaves_the_result_unchanged(self):
        """Parking in something that never moves must add exactly nothing.

        The first version restored cash from parked units *after* a purchase
        had spent it, so a flat index printed money. This is that bug's test.
        """
        write_bars(self.dir, "HHH", self.days, [100.0] * 200)
        flat = {d: 50.0 for d in self.days}
        plain = mtm.simulate([self._trade()], self.dir, PortfolioConfig())
        parked = mtm.simulate([self._trade()], self.dir, PortfolioConfig(),
                              park_idle_in=flat)
        self.assertAlmostEqual(plain.equity_curve[-1]["equity"],
                               parked.equity_curve[-1]["equity"], delta=1.0)

    def test_a_rising_index_lifts_idle_capital(self):
        write_bars(self.dir, "HHH", self.days, [100.0] * 200)
        rising = {d: 50.0 + i for i, d in enumerate(self.days)}
        plain = mtm.simulate([self._trade()], self.dir, PortfolioConfig())
        parked = mtm.simulate([self._trade()], self.dir, PortfolioConfig(),
                              park_idle_in=rising)
        self.assertGreater(parked.equity_curve[-1]["equity"],
                           plain.equity_curve[-1]["equity"])

    def test_a_session_the_index_misses_does_not_zero_the_sleeve(self):
        """The index has its own calendar; a gap is not a price of zero.

        Looking up a missing session returned None and marked held units at
        zero, producing a -64% drawdown in a book whose worst year was -12%.
        """
        write_bars(self.dir, "HHH", self.days, [100.0] * 200)
        holey = {d: 50.0 for i, d in enumerate(self.days) if i % 3 != 0}
        r = mtm.simulate([self._trade()], self.dir, PortfolioConfig(),
                         park_idle_in=holey)
        self.assertGreater(r.max_drawdown_pct, -5.0,
                           "a missing index print was treated as a zero price")

    def test_units_held_on_a_non_parking_day_are_still_valued(self):
        """Gating must not mark held units at zero.

        Gating by removing days from the price map did exactly that, and
        produced a -95.7% drawdown made entirely of arithmetic.
        """
        write_bars(self.dir, "HHH", self.days, [100.0] * 200)
        prices = {d: 50.0 for d in self.days}
        allowed = {d for d in self.days if d < date(2020, 3, 1)}
        r = mtm.simulate([self._trade()], self.dir, PortfolioConfig(),
                         park_idle_in=prices, park_only_on=allowed)
        self.assertGreater(r.max_drawdown_pct, -5.0,
                           "held index units were marked at zero on a gated day")


class CompositeSleeveTests(unittest.TestCase):
    """Index when risk-on, gold when not — as one continuous price series."""

    def setUp(self):
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(10)]

    def test_it_chains_returns_rather_than_swapping_price_levels(self):
        """The sleeve holds units; swapping series would reprice them.

        Risk asset near 1000, safe asset near 10. A switch that changed the
        quoted level would move the sleeve by 100x overnight.
        """
        risk = {d: 1000.0 for d in self.days}
        safe = {d: 10.0 for d in self.days}
        on = set(self.days[:5])
        out = mtm.composite_sleeve(risk, safe, on)
        levels = [out[d] for d in self.days]
        self.assertAlmostEqual(min(levels), max(levels), places=6,
                               msg="a flat pair produced a jump at the handover")

    def test_it_tracks_the_risk_asset_on_risk_on_days(self):
        risk = {d: 100.0 * (1.10 ** i) for i, d in enumerate(self.days)}
        safe = {d: 50.0 for d in self.days}
        out = mtm.composite_sleeve(risk, safe, set(self.days))
        self.assertGreater(out[self.days[-1]], out[self.days[0]] * 2)

    def test_it_tracks_the_safe_asset_when_risk_off(self):
        risk = {d: 100.0 * (0.9 ** i) for i, d in enumerate(self.days)}
        safe = {d: 50.0 * (1.05 ** i) for i, d in enumerate(self.days)}
        out = mtm.composite_sleeve(risk, safe, set())
        self.assertGreater(out[self.days[-1]], out[self.days[0]])

    def test_a_gap_in_one_calendar_does_not_zero_the_sleeve(self):
        risk = {d: 100.0 for i, d in enumerate(self.days) if i % 2 == 0}
        safe = {d: 50.0 for d in self.days}
        out = mtm.composite_sleeve(risk, safe, set(self.days))
        self.assertTrue(all(v > 0 for v in out.values()))


class DeriskTests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(300)]

    def tearDown(self):
        self._tmp.cleanup()

    def test_a_losing_position_is_cut_when_the_market_turns(self):
        write_bars(self.dir, "DDD", self.days, [100.0 - i * 0.2 for i in range(300)])
        trade = {"symbol": "DDD", "entry_day": "2020-01-02", "exit_day": "2020-10-01",
                 "r_multiple": -1.0, "risk_pct": 5.0, "entry": 99.8}
        regimes = {d: ("bull_strong" if d < date(2020, 3, 1) else "bear") for d in self.days}
        r = mtm.simulate([dict(trade)], self.dir, PortfolioConfig(),
                         regime_by_day=regimes,
                         healthy_regimes=frozenset({"bull_strong"}))
        self.assertIsNotNone(r)

    def test_a_winning_position_is_kept_when_only_losers_are_cut(self):
        """Selling a winner into a turn is what took payoff from 11 to 4.

        Asserted on whether the position is still held after the turn rather
        than on return: which of the two earns more depends entirely on what
        the price does next, and that is not the behaviour under test.
        """
        write_bars(self.dir, "EEE", self.days, [100.0 + i * 2 for i in range(300)])
        trade = {"symbol": "EEE", "entry_day": "2020-01-02", "exit_day": "2020-10-01",
                 "r_multiple": 8.0, "risk_pct": 5.0, "entry": 102.0}
        regimes = {d: ("bull_strong" if d < date(2020, 3, 1) else "bear") for d in self.days}

        def open_on(result, day):
            return [p for p in result.equity_curve if p["day"] == day][0]["open"]

        kept = mtm.simulate([dict(trade)], self.dir, PortfolioConfig(),
                            regime_by_day=regimes,
                            healthy_regimes=frozenset({"bull_strong"}),
                            derisk_losers_only=True)
        cut = mtm.simulate([dict(trade)], self.dir, PortfolioConfig(),
                           regime_by_day=regimes,
                           healthy_regimes=frozenset({"bull_strong"}),
                           derisk_losers_only=False)
        self.assertEqual(open_on(kept, "2020-06-01"), 1, "the winner was sold")
        self.assertEqual(open_on(cut, "2020-06-01"), 0, "the winner was not sold")

    def test_no_entries_are_taken_while_the_market_is_unhealthy(self):
        write_bars(self.dir, "FFF", self.days, [100.0 + i for i in range(300)])
        trade = {"symbol": "FFF", "entry_day": "2020-06-01", "exit_day": "2020-09-01",
                 "r_multiple": 4.0, "risk_pct": 5.0, "entry": 250.0}
        regimes = {d: "bear" for d in self.days}
        self.assertIsNone(mtm.simulate([trade], self.dir, PortfolioConfig(),
                                       regime_by_day=regimes,
                                       healthy_regimes=frozenset({"bull_strong"})))


if __name__ == "__main__":
    unittest.main()


class EquityLossLimitTests(unittest.TestCase):
    """`max_equity_loss_pct` — the brief's hard 1%-of-equity rule.

    The rule it replaces was "1% of equity, enforced by an 8% stop", which is
    only true if the stop fills at the stop. It does not: the worst trade in
    this record lost 14.1% against a 3.5% stop, a gap straight through it.
    So the position is sized against a declared adverse move as well.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(120)]
        write_bars(self.dir, "AAA", self.days, [100.0] * 120)

    def tearDown(self):
        self._tmp.cleanup()

    def _cfg(self):
        # A deliberately reckless book: 5% risk behind a 2% stop asks for a
        # 250%-of-equity position, and the position cap alone would allow 50%.
        return PortfolioConfig(risk_per_trade_pct=5.0, watch_risk_pct=5.0,
                               max_concurrent=10, max_portfolio_risk_pct=500.0,
                               max_deployed_pct=100.0, max_position_pct=50.0)

    def _trade(self, r):
        return {"symbol": "AAA", "entry_day": "2020-01-02", "exit_day": "2020-03-01",
                "r_multiple": r, "risk_pct": 2.0, "entry": 100.0}

    def test_a_gap_through_the_stop_still_costs_at_most_the_limit(self):
        """The test that matters. -8R on a 2% stop is a 16% adverse move —
        past the 15% allowance — and it must still not cost more than ~1%."""
        r = mtm.simulate([self._trade(-8.0)], self.dir, self._cfg(),
                         max_equity_loss_pct=1.0)
        self.assertIsNotNone(r)
        self.assertGreaterEqual(
            r.worst_trade_equity_pct, -1.25,
            "a gap through the stop breached the equity limit by more than "
            "the allowance permits",
        )

    def test_without_the_rule_the_same_trade_is_far_worse(self):
        """The control: the limit is doing the work, not the position cap."""
        loose = mtm.simulate([self._trade(-8.0)], self.dir, self._cfg())
        tight = mtm.simulate([self._trade(-8.0)], self.dir, self._cfg(),
                             max_equity_loss_pct=1.0)
        self.assertLess(loose.worst_trade_equity_pct, tight.worst_trade_equity_pct)

    def test_the_limit_binds_through_the_stop_as_well_as_the_gap(self):
        """A trade with a very wide stop is sized down by the stop leg of the
        rule, not only by the gap allowance."""
        wide = {"symbol": "AAA", "entry_day": "2020-01-02", "exit_day": "2020-03-01",
                "r_multiple": -1.0, "risk_pct": 40.0, "entry": 100.0}
        r = mtm.simulate([wide], self.dir, self._cfg(), max_equity_loss_pct=1.0)
        self.assertIsNotNone(r)
        self.assertGreaterEqual(r.worst_trade_equity_pct, -1.05)


class NoSameDayLookAheadTests(unittest.TestCase):
    """Gotcha 115: a switch decided on day D's close earns nothing on day D."""

    def setUp(self):
        self.days = [date(2020, 1, 1) + timedelta(days=i) for i in range(6)]

    def test_the_sleeve_does_not_earn_the_move_that_switched_it_on(self):
        # Flat, then a +10% day that is itself what flips the state risk-on.
        risk = {d: (100.0 if i < 3 else 110.0) for i, d in enumerate(self.days)}
        safe = {d: 50.0 for d in self.days}
        out = mtm.composite_sleeve(risk, safe, set(self.days[3:]))
        self.assertAlmostEqual(out[self.days[3]], out[self.days[2]])

    def test_the_sleeve_does_not_dodge_the_crash_that_switched_it_off(self):
        risk = {d: (100.0 if i < 3 else 80.0) for i, d in enumerate(self.days)}
        safe = {d: 50.0 for d in self.days}
        out = mtm.composite_sleeve(risk, safe, set(self.days[:3]))
        self.assertAlmostEqual(out[self.days[3]] / out[self.days[2]], 0.8)

    def test_build_level_lags_its_state_by_one_session(self):
        from app.services.bot import sleeve as sl
        idx = {d: (100.0 if i < 3 else 110.0) for i, d in enumerate(self.days)}
        gold = {d: 50.0 for d in self.days}
        out = sl.build_level(idx, gold, None, set(self.days[3:]))
        self.assertAlmostEqual(out[self.days[3]], out[self.days[2]])
