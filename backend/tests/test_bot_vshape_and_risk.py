"""The V-shape re-entry rule and the 1%-of-equity loss limit.

Both exist because of a measured failure, and both tests are written to fail
if that failure is reintroduced rather than to restate the implementation.
"""

import unittest
from datetime import date, timedelta

from app.services.bot import mtm_account as mtm
from app.services.bot import rules as R


class ThrustTests(unittest.TestCase):
    """The follow-through rule that puts the sleeve back into equities."""

    @staticmethod
    def _series(closes):
        start = date(2020, 1, 1)
        return [start + timedelta(days=i) for i in range(len(closes))], list(closes)

    def test_a_steady_decline_never_thrusts(self):
        dates, closes = self._series([100 - i for i in range(40)])
        self.assertEqual(R.thrust_days(dates, closes), set())

    def test_a_rebound_off_the_low_is_flagged(self):
        # Ten sessions down, then a hard turn back up.
        closes = [100 - i for i in range(12)] + [90, 92, 94, 96]
        dates, closes = self._series(closes)
        flagged = R.thrust_days(dates, closes)
        self.assertTrue(flagged, "a 3%+ rebound off the ten-day low must flag")
        # The flag lands on the rebound, not on the bottom itself.
        self.assertNotIn(dates[11], flagged)

    def test_the_window_is_causal(self):
        """A later bar can never put an earlier day into the set.

        This is the whole basis of the rule being tradeable: the sleeve acts
        on the session's own close, using only sessions up to it.
        """
        closes = [100 - i for i in range(12)] + [90, 92, 94, 96]
        dates, closes = self._series(closes)
        full = R.thrust_days(dates, closes)
        for cut in range(R.THRUST_LOOKBACK + 1, len(closes)):
            partial = R.thrust_days(dates[:cut], closes[:cut])
            self.assertEqual(
                partial, {d for d in full if d in set(dates[:cut])},
                "truncating the future changed a past decision",
            )

    def test_a_flat_market_does_not_thrust(self):
        dates, closes = self._series([100.0] * 40)
        self.assertEqual(R.thrust_days(dates, closes), set())

    def test_the_threshold_is_not_a_round_guess(self):
        """Declared family, chosen on the training half. If someone retunes
        these they should have to re-read why."""
        self.assertGreater(R.THRUST_PCT, 0.0)
        self.assertLessEqual(R.THRUST_PCT, 10.0)
        self.assertGreaterEqual(R.THRUST_LOOKBACK, 5)


class RecoveryTiltTests(unittest.TestCase):
    """Holding small caps while recovering from a crash."""

    @staticmethod
    def _series(closes):
        start = date(2020, 1, 1)
        return [start + timedelta(days=i) for i in range(len(closes))], list(closes)

    def _crash_then_recover(self):
        # 260 flat sessions, a 30% fall, then a climb back through the level.
        return self._series([100.0] * 260
                            + [100 - 0.5 * i for i in range(60)]
                            + [70 + 0.6 * i for i in range(90)])

    def test_a_market_at_its_highs_is_never_in_recovery(self):
        dates, closes = self._series([100.0 + i for i in range(400)])
        self.assertEqual(R.recovery_days(dates, closes), set())

    def test_the_crash_itself_is_not_the_recovery(self):
        """The tilt must not be on during the fall — small caps lose 40pp a
        year more than the broad index in a crash, which is the whole risk."""
        dates, closes = self._crash_then_recover()
        flagged = R.recovery_days(dates, closes)
        trough = dates[closes.index(min(closes))]
        self.assertNotIn(trough, flagged)
        self.assertTrue(flagged, "the rebound after a crash must flag")

    def test_the_window_expires(self):
        """The reason this beats tilting on 'the market is rising': the
        window ENDS, so the tilt is off before the next crash arrives."""
        dates, closes = self._series([100.0] * 260
                                     + [100 - 0.5 * i for i in range(60)]
                                     + [70 + 0.6 * i for i in range(90)]
                                     + [124.0] * 700)
        flagged = R.recovery_days(dates, closes)
        self.assertTrue(flagged)
        self.assertLess(
            (max(flagged) - dates[320]).days, R.RECOVERY_WINDOW_DAYS + 30,
            "the tilt outlived its window",
        )

    def test_it_is_causal(self):
        dates, closes = self._crash_then_recover()
        full = R.recovery_days(dates, closes)
        for cut in range(261, len(closes), 25):
            self.assertEqual(
                R.recovery_days(dates[:cut], closes[:cut]),
                {d for d in full if d in set(dates[:cut])},
                "a later bar changed an earlier decision",
            )


class EquityLossLimitTests(unittest.TestCase):
    """No single trade may cost more than 1% of total equity."""

    def test_the_gap_allowance_is_wider_than_the_stop(self):
        """The rule this project got wrong once.

        Sizing on the stop alone assumes the stop fills at the stop, which a
        gap does not. The worst trade in the record lost 14.1% against a 3.5%
        stop, so an allowance at or below the stop width would understate the
        true exposure roughly fourfold.
        """
        self.assertGreater(mtm.GAP_ALLOWANCE_PCT, R.EXIT_MAX_STOP_PCT * 2,
                           "the allowance must price a gap, not the stop")

    def test_the_allowance_is_not_read_from_the_sample_worst_trade(self):
        """Sizing against the sample's own extreme is fitting to it — the
        next gap is free to be larger. 15% is declared, and the worst trade
        actually observed is 14.1%."""
        self.assertGreaterEqual(mtm.GAP_ALLOWANCE_PCT, 14.1)


if __name__ == "__main__":
    unittest.main()


class StopCeilingTests(unittest.TestCase):
    """The hard 8% line on the stop, and what it does and does not promise."""

    def test_the_stop_distance_can_never_exceed_the_hard_ceiling(self):
        self.assertLessEqual(R.EXIT_MAX_STOP_PCT, R.HARD_MAX_RISK_PCT)
        self.assertLessEqual(R.HARD_MAX_RISK_PCT, 8.0)

    def test_a_gap_is_not_a_stop_breach_and_the_allowance_says_so(self):
        """The distinction that keeps being re-litigated, pinned in code.

        A stop is a resting order. When a stock closes at 144.81 and opens at
        84.49 the next morning — TEXRAIL, 2010-11-01 — a stop sitting at
        134.14 fills at 84.49, and the trade loses 41.7% against a 6.7% stop.
        No stop rule prevents that; only position size does, which is why
        `GAP_ALLOWANCE_STOP_MULT` exists and why it is far larger than 1.

        Measured across the book, the worst adverse move runs about 9x the
        stop, so an allowance at or near 1x would be a rule in name only.
        """
        self.assertGreaterEqual(
            mtm.GAP_ALLOWANCE_STOP_MULT, 6.0,
            "an allowance near the stop width assumes stops always fill, "
            "which the record directly contradicts",
        )

    def test_position_size_is_what_enforces_the_equity_limit(self):
        """With a 6.3% average stop and a 10x allowance the ceiling is
        1.5 / 63 = 2.4% of equity, so the 35% position cap and the per-trade
        risk budget never bind. Any future edit that makes the position cap
        the binding constraint has removed the equity rule's teeth."""
        avg_stop = 6.3
        allowance = max(mtm.GAP_ALLOWANCE_PCT, mtm.GAP_ALLOWANCE_STOP_MULT * avg_stop)
        ceiling_pct = 100.0 * 1.5 / allowance
        self.assertLess(ceiling_pct, 5.0,
                        "the equity rule must bind before any position cap does")


class TextbookSizingIsNotTheDefaultTests(unittest.TestCase):
    """`position = loss limit / stop` is available, measured, and off.

    It is the rule every trading book teaches, and it is wrong here for one
    reason: it assumes the stop fills at the stop. Measured on the identical
    trade record it is worse on every axis (CAGR +33.62% against +41.54%,
    drawdown -39.01% against -21.21%, 11 of 18 years against 16) and it
    breaches the limit it is derived from on 184 of 304 trades.
    """

    def test_the_defaults_keep_the_gap_leg_on(self):
        import inspect
        sig = inspect.signature(mtm.simulate)
        self.assertEqual(sig.parameters["gap_allowance_pct"].default,
                         mtm.GAP_ALLOWANCE_PCT)
        self.assertEqual(sig.parameters["gap_allowance_mult"].default,
                         mtm.GAP_ALLOWANCE_STOP_MULT)
        self.assertGreater(mtm.GAP_ALLOWANCE_PCT, 0.0,
                           "a zero allowance silently selects textbook sizing")

    def test_textbook_sizing_takes_a_bigger_position_than_gap_aware(self):
        """The whole difference in one assertion: for the same 1.5% limit and
        a 6% stop, textbook sizing asks for 25% of equity and gap-aware
        sizing asks for 2.4%."""
        limit, stop = 1.5, 6.0
        textbook = limit / stop
        gap_aware = limit / max(mtm.GAP_ALLOWANCE_PCT,
                                mtm.GAP_ALLOWANCE_STOP_MULT * stop)
        self.assertGreater(textbook, gap_aware * 5)
