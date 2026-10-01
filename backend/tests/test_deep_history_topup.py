"""The nightly bot job must top the history store up to the last CLOSED
session. It used to treat anything within five calendar days as current, so it
skipped every symbol until the store was a week stale and the Bot tab read
"these signals are 7 days old" after a run that had "succeeded".

Run: `cd backend && pytest tests/test_deep_history_topup.py`
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))
_spec = importlib.util.spec_from_file_location("build_deep_history", BACKEND_ROOT / "scripts" / "build_deep_history.py")
bdh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bdh)  # type: ignore[union-attr]

from app.services.bot.history import Bars  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))


def _bars(days: list[date], closes: list[float]) -> Bars:
    arr = np.asarray(closes, dtype=np.float64)
    return Bars(symbol="X", dates=np.array(days, dtype=object), open=arr, high=arr, low=arr, close=arr,
                volume=np.ones_like(arr))


def _row(day: date, close: float) -> dict:
    return {"date": day, "open": close, "high": close, "low": close, "close": close, "volume": 1.0}


D = [date(2026, 9, 21) + timedelta(days=i) for i in range(4)]  # Mon..Thu


class LatestSessionTests(unittest.TestCase):
    def test_after_the_close_today_counts(self):
        self.assertEqual(bdh.latest_session(datetime(2026, 9, 28, 19, 53, tzinfo=IST)), date(2026, 9, 28))

    def test_before_the_close_it_is_the_previous_session(self):
        # Monday morning: Friday is the last closed session, not Sunday.
        self.assertEqual(bdh.latest_session(datetime(2026, 9, 28, 10, 0, tzinfo=IST)), date(2026, 9, 25))

    def test_a_weekend_rolls_back_to_friday(self):
        self.assertEqual(bdh.latest_session(datetime(2026, 9, 27, 20, 0, tzinfo=IST)), date(2026, 9, 25))


class MergeTailTests(unittest.TestCase):
    def test_the_missing_tail_is_appended(self):
        stored = _bars(D[:3], [100.0, 101.0, 102.0])
        merged = bdh.merge_tail(stored, [_row(D[0], 100.0), _row(D[3], 104.0)])
        self.assertIsNotNone(merged)
        self.assertEqual(merged[-1]["date"], D[3])

    def test_a_provisional_last_bar_is_overwritten_not_treated_as_a_readjustment(self):
        stored = _bars(D[:3], [100.0, 101.0, 102.5])  # last bar caught mid-session
        tail = [_row(D[0], 100.0), _row(D[2], 101.2), _row(D[3], 103.0)]
        merged = bdh.merge_tail(stored, tail)
        self.assertIsNotNone(merged)
        closes = {r["date"]: r["close"] for r in merged}  # write_bars keeps the last occurrence
        self.assertEqual(closes[D[2]], 101.2)

    def test_a_readjusted_series_forces_a_full_refetch(self):
        stored = _bars(D[:3], [100.0, 101.0, 102.0])
        # A 1:2 split re-adjusts every historical close.
        self.assertIsNone(bdh.merge_tail(stored, [_row(D[0], 50.0), _row(D[3], 52.0)]))

    def test_no_overlap_forces_a_full_refetch(self):
        stored = _bars(D[:2], [100.0, 101.0])
        self.assertIsNone(bdh.merge_tail(stored, [_row(D[3], 104.0)]))



class UnclosedSessionTests(unittest.TestCase):
    def test_a_mid_session_bar_is_never_stored(self):
        """A 15:18 IST run fetched today's half-finished bar; stored, it made
        every symbol look current at the evening run and the partial prices
        stayed. Only closed sessions may be written."""
        cutoff = bdh.latest_session(datetime(2026, 10, 1, 15, 18, tzinfo=IST))
        self.assertEqual(cutoff, date(2026, 9, 30))
        rows = [{"date": date(2026, 9, 30), "close": 1.0}, {"date": date(2026, 10, 1), "close": 2.0}]
        self.assertEqual([r["date"] for r in bdh.drop_unclosed(rows, cutoff)], [date(2026, 9, 30)])


if __name__ == "__main__":
    unittest.main()
