"""Tests for the breakout follow-through simulator.

The aggregate numbers on the Markets page are only as trustworthy as `simulate`,
so these pin the rules down with hand-built bars where the right answer is
obvious by inspection.
"""

from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from app.services import breakout_stats as bs


def bars(rows: list[tuple[float, float, float]]) -> pd.DataFrame:
    """(high, low, close) per session, indexed by consecutive days."""
    index = pd.date_range("2026-01-05", periods=len(rows), freq="D")
    return pd.DataFrame(
        {
            "Open": [r[2] for r in rows],
            "High": [r[0] for r in rows],
            "Low": [r[1] for r in rows],
            "Close": [r[2] for r in rows],
            "Volume": [1000] * len(rows),
        },
        index=index,
    )


def signal(entry: float = 100.0, **kwargs) -> bs.Signal:
    return bs.Signal(
        setup=kwargs.get("setup", "vcp"),
        symbol=kwargs.get("symbol", "TEST"),
        trigger_date=kwargs.get("trigger_date", date(2026, 1, 2)),
        entry=entry,
        rs_rating=kwargs.get("rs_rating", 90),
        is_ipo=kwargs.get("is_ipo", False),
        group_top_decile=kwargs.get("group_top_decile", False),
    )


class SimulateTests(unittest.TestCase):
    def test_close_above_target_is_a_win(self):
        # Day 2 closes +6%, clear of the +5% target and never near the stop.
        outcome = bs.simulate(signal(), bars([(102, 99, 101), (107, 100, 106)]))
        assert outcome is not None
        self.assertEqual(outcome.result, "win")
        self.assertEqual(outcome.sessions_held, 2)

    def test_low_through_stop_is_a_loss(self):
        # Day 2 trades to 96, through the 97 stop.
        outcome = bs.simulate(signal(), bars([(101, 99, 100), (101, 96, 98)]))
        assert outcome is not None
        self.assertEqual(outcome.result, "loss")
        self.assertEqual(outcome.sessions_held, 2)

    def test_stop_and_target_on_the_same_bar_counts_as_a_loss(self):
        # Daily bars cannot say which came first, so the pessimistic reading
        # wins. This keeps published win rates from being flattered.
        outcome = bs.simulate(signal(), bars([(106, 96, 106)]))
        assert outcome is not None
        self.assertEqual(outcome.result, "loss")

    def test_full_horizon_without_resolution_is_a_timeout(self):
        drift = [(101.0, 99.0, 100.0)] * bs.HORIZON_SESSIONS
        outcome = bs.simulate(signal(), bars(drift))
        assert outcome is not None
        self.assertEqual(outcome.result, "timeout")
        self.assertEqual(outcome.sessions_held, bs.HORIZON_SESSIONS)

    def test_short_forward_window_is_open_not_timeout(self):
        # Only three sessions of data exist. Calling this a timeout would count
        # an unfinished trade as a non-win and drag recent weeks down.
        outcome = bs.simulate(signal(), bars([(101, 99, 100)] * 3))
        assert outcome is not None
        self.assertEqual(outcome.result, "open")

    def test_horizon_argument_shortens_the_window(self):
        # Same bars, shorter horizon: the win on day 5 is out of reach. Three
        # bars against a 3-session horizon is a *complete* window, so this is a
        # timeout — "open" is reserved for not having enough data yet.
        rows = [(101, 99, 100)] * 4 + [(107, 100, 106)]
        self.assertEqual(bs.simulate(signal(), bars(rows)).result, "win")
        self.assertEqual(bs.simulate(signal(), bars(rows), horizon=3).result, "timeout")

    def test_open_means_not_enough_forward_data_for_the_horizon(self):
        # Two bars against a 3-session horizon: genuinely unfinished.
        self.assertEqual(bs.simulate(signal(), bars([(101, 99, 100)] * 2), horizon=3).result, "open")

    def test_max_favourable_tracks_the_running_high(self):
        outcome = bs.simulate(signal(), bars([(112, 99, 100), (108, 96, 97)]))
        assert outcome is not None
        # 112 is +12% even though the trade ultimately stopped out.
        self.assertAlmostEqual(outcome.max_favourable_pct, 12.0, places=2)
        self.assertEqual(outcome.result, "loss")

    def test_closed_near_high_reflects_position_in_range(self):
        strong = bs.simulate(signal(), bars([(110, 100, 109.5)]))
        weak = bs.simulate(signal(), bars([(110, 100, 100.5)]))
        assert strong is not None and weak is not None
        self.assertTrue(strong.closed_near_high)
        self.assertFalse(weak.closed_near_high)

    def test_empty_or_invalid_input_returns_none(self):
        self.assertIsNone(bs.simulate(signal(), bars([])))
        self.assertIsNone(bs.simulate(signal(entry=0.0), bars([(101, 99, 100)])))


class SummariseTests(unittest.TestCase):
    def make_outcome(self, result: str, mfe: float = 0.0, near_high: bool = False) -> bs.Outcome:
        return bs.Outcome(
            signal=signal(),
            result=result,
            max_favourable_pct=mfe,
            final_pct=0.0,
            sessions_held=2,
            closed_near_high=near_high,
        )

    def test_win_rate_excludes_open_signals(self):
        # 1 win, 1 loss, 8 unresolved. The win rate is 50%, not 10%.
        outcomes = [self.make_outcome("win"), self.make_outcome("loss")] + [self.make_outcome("open")] * 8
        stats = bs.summarise("vcp", "VCP", outcomes)
        self.assertEqual(stats.signals, 10)
        self.assertEqual(stats.resolved, 2)
        self.assertEqual(stats.open_positions, 8)
        self.assertEqual(stats.win_rate, 50.0)

    def test_all_open_yields_zero_rate_without_dividing_by_zero(self):
        stats = bs.summarise("vcp", "VCP", [self.make_outcome("open")] * 4)
        self.assertEqual(stats.resolved, 0)
        self.assertEqual(stats.win_rate, 0.0)

    def test_big_move_held_is_measured_against_big_movers_only(self):
        outcomes = [
            self.make_outcome("win", mfe=15.0, near_high=True),
            self.make_outcome("loss", mfe=12.0, near_high=False),
            self.make_outcome("loss", mfe=1.0, near_high=False),
        ]
        stats = bs.summarise("vcp", "VCP", outcomes)
        # Two of three reached 10%; one of those two held it.
        self.assertAlmostEqual(stats.pct_reached_big_move, 66.7, places=1)
        self.assertEqual(stats.pct_big_move_held, 50.0)

    def test_empty_input_is_safe(self):
        stats = bs.summarise("vcp", "VCP", [])
        self.assertEqual(stats.signals, 0)
        self.assertEqual(stats.win_rate, 0.0)


class CohortTests(unittest.TestCase):
    def test_recent_ipo_window(self):
        as_of = date(2026, 8, 5)
        self.assertTrue(bs.is_recent_ipo("2026-03-01", as_of))
        self.assertFalse(bs.is_recent_ipo("2020-03-01", as_of))
        # A listing dated after the session is data corruption, not an IPO.
        self.assertFalse(bs.is_recent_ipo("2026-12-01", as_of))
        self.assertFalse(bs.is_recent_ipo(None, as_of))
        self.assertFalse(bs.is_recent_ipo("not-a-date", as_of))

    def test_week_key_is_iso_and_crosses_year_end(self):
        # 2025-12-29 is ISO week 1 of 2026 — a naive year+week would say 2025.
        self.assertEqual(bs.week_key(date(2025, 12, 29)), "2026-W01")
        self.assertEqual(bs.week_key(date(2026, 8, 5)), bs.week_key(date(2026, 8, 3)))


class AggregateTests(unittest.TestCase):
    def test_groups_by_iso_week_and_splits_cohorts(self):
        def make(day: date, result: str, ipo: bool, leading: bool) -> bs.Outcome:
            return bs.Outcome(
                signal=signal(trigger_date=day, is_ipo=ipo, group_top_decile=leading),
                result=result,
                max_favourable_pct=6.0,
                final_pct=4.0,
                sessions_held=2,
                closed_near_high=True,
            )

        outcomes = [
            make(date(2026, 8, 3), "win", True, True),
            make(date(2026, 8, 4), "loss", False, True),
            make(date(2026, 7, 27), "win", False, False),
        ]
        payload = bs.aggregate(outcomes, {"vcp": "VCP"})
        weeks = payload["weeks"]
        self.assertEqual([w["week"] for w in weeks], ["2026-W31", "2026-W32"])

        latest = weeks[-1]
        self.assertEqual(latest["total_signals"], 2)
        self.assertEqual(latest["overall"]["win_rate"], 50.0)
        self.assertEqual(latest["cohorts"]["ipo"]["resolved"], 1)
        self.assertEqual(latest["cohorts"]["leading_groups"]["resolved"], 2)
        self.assertEqual(latest["cohorts"]["lagging_groups"]["resolved"], 0)

    def test_no_outcomes_yields_no_weeks(self):
        self.assertEqual(bs.aggregate([], {})["weeks"], [])


if __name__ == "__main__":
    unittest.main()
