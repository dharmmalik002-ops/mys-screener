"""The paper book — the piece that has to work before a broker is connected.

An execution layer on top of a book that miscounts its positions places real
orders it should not place, so these tests are about bookkeeping integrity
rather than about returns.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from app.services.bot import paper
from app.services.bot import rules as R


def bar(o, h, l, c):
    return {"open": o, "high": h, "low": l, "close": c}


def fresh(equity=100_000.0):
    return paper.PaperBook(started="2026-01-01", starting_equity=equity, cash=equity)


CAND = {"symbol": "AAA", "strategy": "pullback_ema21", "risk_pct": 5.0,
        "atr": 2.0, "conf": 9.0}


class IdempotencyTests(unittest.TestCase):
    """The guard that matters most: a cron firing twice must change nothing.

    A duplicate entry is invisible in the ledger — it looks exactly like a
    legitimate second position — so this cannot be left to operational care.
    """

    def test_reprocessing_a_session_is_a_no_op(self):
        book = fresh()
        bars = {"AAA": bar(100, 104, 99, 103)}
        first = paper.advance(book, date(2026, 1, 2), bars, [CAND])
        self.assertEqual(first["entered"], 1)
        again = paper.advance(book, date(2026, 1, 2), bars, [CAND])
        self.assertIn("skipped", again)
        self.assertEqual(len(book.positions), 1, "the day was replayed and doubled up")

    def test_an_earlier_session_is_also_refused(self):
        """Out-of-order replay is the same hazard wearing a different hat."""
        book = fresh()
        paper.advance(book, date(2026, 1, 5), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        out = paper.advance(book, date(2026, 1, 2), {"AAA": bar(90, 91, 89, 90)}, [CAND])
        self.assertIn("skipped", out)
        self.assertEqual(len(book.positions), 1)

    def test_force_exists_for_a_deliberate_replay(self):
        book = fresh()
        d = date(2026, 1, 2)
        paper.advance(book, d, {"AAA": bar(100, 104, 99, 103)}, [CAND])
        out = paper.advance(book, d, {"AAA": bar(100, 104, 99, 103)}, [], force=True)
        self.assertNotIn("skipped", out)


class PersistenceTests(unittest.TestCase):

    def test_the_book_survives_a_restart_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            book = fresh()
            paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
            book.save(Path(tmp))
            back = paper.PaperBook.load(Path(tmp))
            self.assertIsNotNone(back)
            self.assertEqual(back.last_session, "2026-01-02")
            self.assertEqual(len(back.positions), 1)
            held = back.positions[0]
            self.assertEqual(held.symbol, "AAA")
            self.assertAlmostEqual(held.stop_price, 100 * 0.95, places=6)
            self.assertAlmostEqual(back.cash, book.cash, places=2)

    def test_a_missing_book_loads_as_none_rather_than_crashing(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(paper.PaperBook.load(Path(tmp)))


class ExitTests(unittest.TestCase):

    def test_a_gap_through_the_stop_fills_at_the_open(self):
        """Gotcha 106, enforced in the live path rather than only the backtest.

        TEXRAIL's stop sat at -6.7% and the stock opened -41.7%. A paper book
        that fills at the stop would report a loss the account could not have
        achieved, and would flatter every risk number downstream.
        """
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        paper.advance(book, date(2026, 1, 3), {"AAA": bar(60, 62, 55, 58)}, [])
        self.assertEqual(len(book.positions), 0)
        self.assertEqual(len(book.closed), 1)
        done = book.closed[0]
        self.assertEqual(done.reason, "gap")
        self.assertAlmostEqual(done.exit_price, 60.0, places=6)
        self.assertLess(done.net_pct, -30.0, "filled at the stop instead of the open")

    def test_an_ordinary_stop_fills_at_the_stop(self):
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        paper.advance(book, date(2026, 1, 3), {"AAA": bar(102, 103, 94, 96)}, [])
        self.assertEqual(book.closed[0].reason, "stop")
        self.assertAlmostEqual(book.closed[0].exit_price, 95.0, places=6)

    def test_the_stop_never_moves_down(self):
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        start = book.positions[0].stop_price
        for i, px in enumerate([140, 160, 150, 145], start=4):
            paper.advance(book, date(2026, 1, i), {"AAA": bar(px, px + 1, px - 1, px)}, [])
            if book.positions:
                self.assertGreaterEqual(book.positions[0].stop_price, start)
                start = book.positions[0].stop_price

    def test_a_symbol_that_does_not_print_is_carried_not_dropped(self):
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        held_before = book.positions[0].sessions_held
        paper.advance(book, date(2026, 1, 3), {}, [])
        self.assertEqual(len(book.positions), 1)
        self.assertEqual(book.positions[0].sessions_held, held_before,
                         "a missing bar aged the position it could not price")


class SizingTests(unittest.TestCase):

    def test_it_matches_the_study_and_the_equity_rule_binds(self):
        """2.5% of equity at a 6% stop — the equity rule, not the 35% cap."""
        shares = paper.position_size(100_000.0, 100.0, 6.0)
        self.assertAlmostEqual(shares * 100.0 / 100_000.0, 0.025, places=4)

    def test_a_wider_stop_takes_a_smaller_position(self):
        small = paper.position_size(100_000.0, 100.0, 7.0)
        big = paper.position_size(100_000.0, 100.0, 3.0)
        self.assertLess(small, big)

    def test_the_hard_stop_ceiling_is_enforced_on_entry(self):
        book = fresh()
        wide = dict(CAND, risk_pct=20.0)
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [wide])
        # 20% is capped to EXIT_MAX_STOP_PCT, which is inside the hard ceiling,
        # so the trade is taken at the capped stop rather than rejected.
        self.assertEqual(len(book.positions), 1)
        self.assertLessEqual(book.positions[0].initial_stop_pct, R.HARD_MAX_RISK_PCT)

    def test_the_book_cannot_spend_more_cash_than_it_has(self):
        book = fresh(equity=5_000.0)
        cands = [dict(CAND, symbol=f"S{i}") for i in range(60)]
        bars = {f"S{i}": bar(100, 104, 99, 103) for i in range(60)}
        paper.advance(book, date(2026, 1, 2), bars, cands)
        self.assertGreaterEqual(book.cash, -0.01, "the book went overdrawn")
        self.assertLessEqual(len(book.positions), paper.MAX_CONCURRENT)


class AccountingTests(unittest.TestCase):

    def test_a_flat_market_neither_creates_nor_destroys_money(self):
        """The first test written for the sleeve (gotcha 83) and the same one
        that matters here: nothing moving must change equity."""
        book = fresh()
        flat = {"AAA": bar(100, 100, 100, 100)}
        paper.advance(book, date(2026, 1, 2), flat, [CAND])
        start = book.equity({"AAA": 100.0})
        for i in range(3, 9):
            paper.advance(book, date(2026, 1, i), flat, [])
        self.assertAlmostEqual(book.equity({"AAA": 100.0}), start, places=2)

    def test_declined_signals_are_recorded_with_a_reason(self):
        """The study declines 99% of what it sees. A book that only logs its
        fills cannot be audited against it."""
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        paper.advance(book, date(2026, 1, 3), {"AAA": bar(103, 105, 102, 104)}, [CAND])
        self.assertTrue(book.declined)
        self.assertEqual(book.declined[-1]["why"], "already held")

    def test_summary_reports_nothing_rather_than_zero_on_an_empty_book(self):
        s = paper.summary(fresh())
        self.assertIsNone(s["win_rate"])
        self.assertEqual(s["closed_trades"], 0)
        self.assertEqual(s["return_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()


class SleeveTests(unittest.TestCase):
    """Idle capital rides the index. The bugs here have all been money bugs."""

    def test_a_flat_sleeve_changes_nothing(self):
        """The first test written for this in the backtest (gotcha 83) and the
        one that catches the whole class: parking in something that does not
        move must leave equity exactly where it was."""
        book = fresh()
        for i in range(2, 10):
            paper.advance(book, date(2026, 1, i), {}, [], sleeve_level=100.0)
        self.assertAlmostEqual(book.equity({}, 100.0), 100_000.0, places=2)

    def test_the_sleeve_is_liquidated_before_trades_can_spend_the_cash(self):
        """Restoring parked cash at the END of a session, after trades had
        already spent it, made a flat index print money and read +1345% in
        2009. Equity after a rising sleeve must reflect the rise once."""
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {}, [], sleeve_level=100.0)
        self.assertAlmostEqual(book.sleeve_units, 1000.0, places=6)
        paper.advance(book, date(2026, 1, 3), {}, [], sleeve_level=110.0)
        self.assertAlmostEqual(book.equity({}, 110.0), 110_000.0, places=2)

    def test_entries_can_draw_on_the_freed_sleeve_cash(self):
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {}, [], sleeve_level=100.0)
        self.assertAlmostEqual(book.cash, 0.0, places=6)   # all parked
        out = paper.advance(book, date(2026, 1, 5),
                            {"AAA": bar(100, 104, 99, 103)}, [CAND], sleeve_level=100.0)
        self.assertEqual(out["entered"], 1, "could not buy because cash was still parked")

    def test_a_missing_sleeve_price_does_not_zero_the_units(self):
        """A missing price is not a zero price — the failure that appeared
        four times in one feature, always as an impossible drawdown."""
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {}, [], sleeve_level=100.0)
        before = book.equity({}, 100.0)
        paper.advance(book, date(2026, 1, 3), {}, [], sleeve_level=None)
        self.assertAlmostEqual(book.equity({}, 100.0), before, places=2)


class TrailMustActuallyArmTests(unittest.TestCase):
    """The bug paper trading existed to catch, and did.

    `atr_at_entry` was read from a field the signal does not have, so it was
    always 0.0 and `gain_r >= 1 and atr > 0` never fired. The trail was dead:
    169 closed trades showed 132 stop, 30 gap, 7 ceiling and NOT ONE trail
    exit, and the win rate read 4.1% against the study's 32%. Nothing raised
    an error — a rule was simply off.
    """

    def _protective_level(self, pos):
        """Where the trail sits, whichever trail rule is live. Under the
        close-basis trail (SEASONED_RULES) the hard stop stays put and a
        separate trail level rises; under the intraday trail the stop itself
        rises. These tests were first written for the intraday rule."""
        return pos.trail_level if R.SEASONED_RULES.get("trail_on_close") else pos.stop_price

    def test_the_trail_raises_the_stop_once_the_trade_is_a_winner(self):
        book = fresh()
        cand = dict(CAND, atr_pct_at_entry=2.0)      # 2% of price
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [cand])
        held = book.positions[0]
        self.assertGreater(held.atr_at_entry, 0.0, "ATR did not survive entry")
        initial = held.stop_price
        # Run it up well past 1R so the trail must arm.
        for i, px in enumerate([120, 140, 160], start=5):
            paper.advance(book, date(2026, 1, i), {"AAA": bar(px, px + 1, px - 1, px)}, [])
        self.assertTrue(book.positions, "position closed unexpectedly")
        level = self._protective_level(book.positions[0])
        self.assertIsNotNone(level, "the trail never armed — check the ATR field name")
        self.assertGreater(level, initial, "the trail never armed — check the ATR field name")

    def test_a_trailed_winner_exits_in_profit(self):
        """The end-to-end version: run up, then fall back, and the trade must
        close ABOVE entry rather than at the original stop. Under the
        close-basis trail the breach is seen on a close and sold at the NEXT
        open, so one more session is needed than under the intraday rule."""
        book = fresh()
        cand = dict(CAND, atr_pct_at_entry=2.0)
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [cand])
        for i, px in enumerate([130, 160, 190], start=5):
            paper.advance(book, date(2026, 1, i), {"AAA": bar(px, px + 1, px - 1, px)}, [])
        paper.advance(book, date(2026, 1, 20), {"AAA": bar(150, 150, 120, 125)}, [])
        paper.advance(book, date(2026, 1, 21), {"AAA": bar(124, 126, 122, 123)}, [])
        self.assertEqual(len(book.closed), 1)
        done = book.closed[0]
        self.assertGreater(done.net_pct, 0.0,
                           "a winner that trailed up still closed at a loss")
        self.assertGreater(done.r_multiple, 0.0)

    def test_the_signal_field_is_the_one_the_backtest_writes(self):
        """Guards the specific mistake: the engine writes `atr_pct_at_entry`,
        as a percent. Anything reading `atr` gets None and silently zeroes."""
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)},
                      [dict(CAND, atr_pct_at_entry=3.0)])
        self.assertAlmostEqual(book.positions[0].atr_at_entry, 3.0, places=6)


class RIsMeasuredAgainstInitialRiskTests(unittest.TestCase):

    def test_a_trailed_winner_does_not_report_negative_r(self):
        """Once the trail lifts the stop above entry, `entry - stop` is
        negative. Dividing by it flipped the sign of every trailed winner:
        a +50% trade reported -0.68R. R is measured against what the trade
        risked at ENTRY, which cannot go negative."""
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)},
                      [dict(CAND, atr_pct_at_entry=2.0)])
        held = book.positions[0]
        risk_at_entry = held.risk_amount()
        self.assertGreater(risk_at_entry, 0.0)
        held.stop_price = 174.0                      # as the trail would leave it
        self.assertAlmostEqual(held.risk_amount(), risk_at_entry, places=6,
                               msg="R denominator moved with the trailing stop")


class AdoptedExitRulesTests(unittest.TestCase):
    """The paper book must trade the rules the study adopted (gotcha 113),
    not the ones it had before — the drift that gotcha 111 was about."""

    def test_a_close_breach_sells_at_the_next_open_not_the_same_bar(self):
        if not R.SEASONED_RULES.get("trail_on_close"):
            self.skipTest("close-basis trail not adopted")
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)},
                      [dict(CAND, atr_pct_at_entry=2.0)])
        for i, px in enumerate([130, 160, 190], start=5):
            paper.advance(book, date(2026, 1, i), {"AAA": bar(px, px + 1, px - 1, px)}, [])
        paper.advance(book, date(2026, 1, 20), {"AAA": bar(150, 150, 120, 125)}, [])
        self.assertEqual(len(book.closed), 0, "sold on the close that breached")
        self.assertEqual(book.positions[0].pending_exit, "trail")
        paper.advance(book, date(2026, 1, 21), {"AAA": bar(124, 126, 122, 123)}, [])
        self.assertAlmostEqual(book.closed[0].exit_price, 124.0, places=6)

    def test_an_intraday_wick_through_the_trail_does_not_sell(self):
        if not R.SEASONED_RULES.get("trail_on_close"):
            self.skipTest("close-basis trail not adopted")
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)},
                      [dict(CAND, atr_pct_at_entry=2.0)])
        for i, px in enumerate([130, 160, 190], start=5):
            paper.advance(book, date(2026, 1, i), {"AAA": bar(px, px + 1, px - 1, px)}, [])
        # Wicks to 150 — below the 174 trail — and closes back at 188.
        paper.advance(book, date(2026, 1, 20), {"AAA": bar(189, 190, 150, 188)}, [])
        self.assertEqual(len(book.closed), 0)
        self.assertIsNone(book.positions[0].pending_exit)

    def test_a_climax_close_sells_at_the_next_open(self):
        mult = R.SEASONED_RULES.get("climax_sma50_mult")
        if not mult:
            self.skipTest("climax exit not adopted")
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)}, [CAND])
        b = dict(bar(200, 205, 198, 200), sma50=200 / (mult * 1.01))   # just past the line
        paper.advance(book, date(2026, 1, 5), {"AAA": b}, [])
        self.assertEqual(book.positions[0].pending_exit, "climax")
        paper.advance(book, date(2026, 1, 6), {"AAA": bar(199, 201, 197, 198)}, [])
        self.assertEqual(book.closed[0].reason, "climax")
        self.assertAlmostEqual(book.closed[0].exit_price, 199.0, places=6)

    def test_the_trail_uses_todays_atr_when_the_runner_supplies_it(self):
        """The engine trails off each bar's ATR; a frozen entry ATR drifts
        from it the longer a winner runs."""
        book = fresh()
        paper.advance(book, date(2026, 1, 2), {"AAA": bar(100, 104, 99, 103)},
                      [dict(CAND, atr_pct_at_entry=2.0)])
        paper.advance(book, date(2026, 1, 5), {"AAA": dict(bar(150, 151, 149, 150), atr=5.0)}, [])
        level = (book.positions[0].trail_level if R.SEASONED_RULES.get("trail_on_close")
                 else book.positions[0].stop_price)
        self.assertAlmostEqual(level, 150 - R.EXIT_TRAIL_ATR_MULT * 5.0, places=6)
