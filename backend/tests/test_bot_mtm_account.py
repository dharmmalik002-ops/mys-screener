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
