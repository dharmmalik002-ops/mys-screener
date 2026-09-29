"""Momentum Burst counts every window in sessions (10/21 EMA, burst and
consolidation windows) and pairs the trailing daily volumes with the last
closes one-for-one, so its close series must be DAILY. `chart_grid_points`
is ~2.17 sessions per point for a full history, which made its "10 EMA" a
~22-session EMA — the same bug already fixed in VCP / Power Base / Qullamaggie.

Run: `cd backend && pytest tests/test_momentum_burst_daily_series.py`
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from app.scanners import definitions as d

DAY = 86400


def _grid(step_days: float, n: int):
    return [SimpleNamespace(time=int(1_700_000_000 + i * step_days * DAY), value=100.0 + i) for i in range(n)]


def _snap(grid, recent):
    return SimpleNamespace(
        symbol="X", chart_grid_points=grid, recent_closes=recent, last_price=recent[-1] if recent else 0.0,
        change_pct=0.0, previous_close=recent[-2] if len(recent) > 1 else None,
        history_session_date=None,
    )


class MomentumBurstDailySeriesTests(unittest.TestCase):
    def test_the_committed_daily_closes_win(self):
        daily = [float(v) for v in range(1, 300)]
        with mock.patch.object(d.close_history, "closes_for", return_value=daily):
            self.assertEqual(d._mb_closes(_snap(_grid(3.0, 240), [1.0] * 20)), daily)

    def test_a_sparse_grid_is_never_read_as_daily(self):
        recent = [float(v) for v in range(100, 120)]
        with mock.patch.object(d.close_history, "closes_for", return_value=[]):
            closes = d._mb_closes(_snap(_grid(3.0, 240), recent))
        self.assertEqual(closes, recent)  # the 20 true daily closes, not 240 sparse points

    def test_a_one_to_one_grid_may_stand_in(self):
        grid = _grid(1.0, 120)  # short history: sampled daily
        with mock.patch.object(d.close_history, "closes_for", return_value=[]):
            self.assertEqual(len(d._mb_closes(_snap(grid, [1.0] * 20))), 120)


if __name__ == "__main__":
    unittest.main()


class DemandZoneDailySeriesTests(unittest.TestCase):
    """The demand-zone fallback builds bars from closes and counts them as
    sessions; it had the same sparse-grid mistake."""

    def _points(self, snap):
        from app.services.dashboard_service import DashboardService
        return DashboardService._daily_close_points(snap)

    def test_a_sparse_grid_falls_back_to_the_daily_history(self):
        from datetime import date
        snap = _snap(_grid(3.0, 240), [1.0] * 20)
        snap.history_session_date = date(2026, 9, 25)  # a Friday
        daily = [float(v) for v in range(1, 61)]
        with mock.patch.object(d.close_history, "closes_for", return_value=daily):
            points = self._points(snap)
        self.assertEqual([p.value for p in points], daily)
        # dated back by weekday from the session: last point is the session itself
        from datetime import datetime, timezone
        self.assertEqual(datetime.fromtimestamp(points[-1].time, tz=timezone.utc).date(), date(2026, 9, 25))
        self.assertEqual(datetime.fromtimestamp(points[-2].time, tz=timezone.utc).date(), date(2026, 9, 24))

    def test_no_daily_source_means_no_bars(self):
        with mock.patch.object(d.close_history, "closes_for", return_value=[]):
            self.assertEqual(self._points(_snap(_grid(3.0, 240), [1.0] * 20)), [])
