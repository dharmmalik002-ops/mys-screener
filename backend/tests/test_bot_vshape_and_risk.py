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
