"""The Rotation chart's vertical axis: price momentum of each group against the benchmark.

The old axis (change in the ranking score) called a group Improving when an
old day fell out of its 63/126-session return window, even while the group was
still losing to the market — half its Improving groups were in fact falling.
These tests pin the property that replaced it: momentum follows the group's own
relative-strength line.

Run: `cd backend && pytest tests/test_group_rotation.py`
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from app.models.market import StockSnapshot
from app.services.dashboard_service import DashboardService
from app.services.group_rotation import build_group_rotation

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import generate_bhavcopy_patch as generator  # noqa: E402

AS_OF = date(2026, 10, 8)
N = 120


def _calendar(n: int = N) -> list[date]:
    days, cursor = [], AS_OF
    while len(days) < n:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(days)


def _path(daily: list[float], start: float = 100.0) -> list[float]:
    out = [start]
    for r in daily[1:]:
        out.append(out[-1] * (1 + r))
    return out


def _universe(group_daily: list[float], members: int = 4, bench_size: int = 60) -> dict[str, list[float]]:
    closes: dict[str, list[float]] = {}
    for i in range(bench_size):
        # A flat-ish benchmark with a little cross-sectional noise.
        closes[f"B{i}"] = _path([0.0005 * ((i % 5) - 2) * (1 if t % 2 else -1) for t in range(N)])
    for i in range(members):
        closes[f"G{i}"] = _path(group_daily, 100.0 + i)
    return closes


def _run(closes: dict[str, list[float]], sessions: dict[str, date] | None = None) -> dict:
    bench = [s for s in closes if s.startswith("B")]
    groups = [{"group_id": "g", "parent_sector": "Sector", "symbols": [s for s in closes if s.startswith("G")]}]
    return build_group_rotation(
        groups, closes, sessions or {s: AS_OF for s in closes}, bench, _calendar(), sessions=60
    )


class MomentumFollowsTheRelativeLine(unittest.TestCase):
    def test_a_group_gaining_on_the_market_has_positive_momentum(self) -> None:
        out = _run(_universe([0.0] * 100 + [0.004] * 20))
        self.assertGreater(out["groups"]["g"][-1]["momentum"], 0)
        self.assertGreater(out["groups"]["g"][-1]["rs_change_5d"], 0)

    def test_a_group_losing_to_the_market_has_negative_momentum(self) -> None:
        out = _run(_universe([0.0] * 100 + [-0.004] * 20))
        self.assertLess(out["groups"]["g"][-1]["momentum"], 0)

    def test_a_decline_that_slows_but_continues_is_not_improving(self) -> None:
        """The case the old axis got wrong: still falling, just less steeply.
        A ratio against a moving average 'rises' here because the average
        falls faster; the relative line itself is still going down."""
        out = _run(_universe([-0.006] * 90 + [-0.001] * 30))
        last = out["groups"]["g"][-1]
        self.assertLess(last["momentum"], 0)
        self.assertLess(last["momentum_w"], 0)

    def test_weekly_momentum_is_slower_than_daily(self) -> None:
        """A turn that is days old shows on the daily axis before the weekly one."""
        out = _run(_universe([-0.003] * 108 + [0.006] * 12))
        last = out["groups"]["g"][-1]
        self.assertGreater(last["momentum"], 0)
        self.assertLess(last["momentum_w"], last["momentum"])


class DataHygiene(unittest.TestCase):
    def test_a_holiday_row_copied_forward_is_dropped(self) -> None:
        """Yahoo returns some NSE holidays with every close unchanged; kept,
        the row shifts every earlier bar by one session."""
        daily = [0.0] * 100 + [0.004] * 20
        clean = _run(_universe(daily))
        with_holiday = {}
        for sym, closes in _universe(daily).items():
            # Insert a copy of the bar 6 sessions back, then drop the oldest bar
            # so the series still ends on the same session with the same length.
            pos = len(closes) - 6
            with_holiday[sym] = (closes[:pos] + [closes[pos - 1]] + closes[pos:])[1:]
        dirty = _run(with_holiday)
        self.assertEqual(dirty["non_sessions_dropped"], 1)
        self.assertEqual(dirty["groups"]["g"][-1]["momentum"], clean["groups"]["g"][-1]["momentum"])
        self.assertEqual(dirty["sessions"][-1], AS_OF.isoformat())

    def test_symbols_on_another_session_are_left_out(self) -> None:
        closes = _universe([0.0] * 100 + [0.004] * 20)
        sessions = {s: AS_OF for s in closes}
        for s in ("G0", "G1"):
            sessions[s] = AS_OF - timedelta(days=1)
        out = _run(closes, sessions)
        self.assertEqual(out["coverage"]["g"], 2)
        self.assertNotIn("g", out["groups"])  # 2 members is below MIN_MEMBERS

    def test_sector_momentum_is_measured_not_averaged(self) -> None:
        out = _run(_universe([0.0] * 100 + [0.004] * 20))
        self.assertIn("Sector", out["sectors"])
        self.assertGreater(out["sectors"]["Sector"][-1]["momentum"], 0)


def _snapshot(symbol: str, session: date | None) -> StockSnapshot:
    return StockSnapshot.model_validate({
        "symbol": symbol, "name": symbol, "exchange": "NSE", "sector": "Industrials",
        "sub_sector": "Capital Goods", "market_cap_crore": 2000.0, "last_price": 100.0,
        "change_pct": 0.0, "volume": 100000, "avg_volume_20d": 100000,
        "day_high": 101.0, "day_low": 99.0, "ath": 200.0, "high_52w": 190.0, "low_52w": 60.0,
        "range_high_20d": 110.0, "benchmark_return_20d": 1.0, "sector_return_20d": 1.0,
        "pivot_high": 110.0, "darvas_high": 110.0, "darvas_low": 95.0,
        "pullback_depth_pct": 2.0, "trend_strength": 0.8,
        "history_session_date": session,
    })


class GroupsSkipStaleRows(unittest.TestCase):
    def _service(self, patch_date: str | None) -> DashboardService:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        status = Path(temp.name) / "bhavcopy_status.json"
        if patch_date:
            status.write_text(json.dumps({"date": patch_date}), encoding="utf-8")

        class Provider:
            backend_root = Path(temp.name)

            def _bhavcopy_status_path(self) -> Path:
                return status

        service = DashboardService.__new__(DashboardService)
        service.provider = Provider()
        return service

    def test_a_months_old_row_does_not_rank_or_paint(self) -> None:
        """JBCHEPHARM, HEG, GSPL ... sat on an April session for six months
        and were ranked and painted with April's returns."""
        service = self._service("2026-10-08")
        rows = [_snapshot("FRESH", AS_OF), _snapshot("RECENT", AS_OF - timedelta(days=1)),
                _snapshot("APRIL", date(2026, 4, 8)), _snapshot("NEVER", None)]
        kept = [s.symbol for s in service._group_eligible_snapshots(rows)]
        self.assertEqual(kept, ["FRESH", "RECENT"])

    def test_no_patch_on_disk_keeps_everything(self) -> None:
        service = self._service(None)
        rows = [_snapshot("A", date(2026, 4, 8)), _snapshot("B", AS_OF)]
        self.assertEqual(len(service._group_eligible_snapshots(rows)), 2)


class NightlyCloseHistory(unittest.TestCase):
    def _rows(self, n_symbols: int, holiday: date | None = None) -> dict:
        days = _calendar(80)
        if holiday:
            days = sorted(days + [holiday])
        rows = {}
        for i in range(n_symbols):
            out, close = [], 100.0 + i
            for day in days:
                if day == holiday:
                    out.append((day, close, 0.0))  # copied forward, nothing traded
                else:
                    close *= 1.001
                    out.append((day, close, 1000.0))
            rows[f"S{i}"] = out
        return rows

    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.original = generator.CLOSE_HISTORY_PATH
        generator.CLOSE_HISTORY_PATH = Path(temp.name) / "close_history.json"
        self.addCleanup(setattr, generator, "CLOSE_HISTORY_PATH", self.original)

    def test_holiday_rows_are_dropped_and_the_format_is_unchanged(self) -> None:
        holiday = date(2026, 10, 3)  # a Saturday, so it is not already a weekday session
        written = generator._write_close_history(self._rows(1200, holiday), AS_OF)
        self.assertEqual(written, 1200)
        payload = json.loads(generator.CLOSE_HISTORY_PATH.read_text())
        entry = payload["symbols"]["S0"]
        self.assertEqual(len(entry["closes"]), 80)
        self.assertEqual(
            entry["last_time"], int(datetime(2026, 10, 8, tzinfo=timezone.utc).timestamp())
        )
        self.assertEqual(payload["non_sessions_dropped"], ["2026-10-03"])

    def test_a_partial_download_never_replaces_the_artifact(self) -> None:
        generator.CLOSE_HISTORY_PATH.write_text('{"symbols": {"KEEP": {"last_time": 1, "closes": [1]}}}')
        self.assertEqual(generator._write_close_history(self._rows(20), AS_OF), 0)
        self.assertIn("KEEP", json.loads(generator.CLOSE_HISTORY_PATH.read_text())["symbols"])

    def test_symbols_missing_from_tonights_download_are_kept(self) -> None:
        generator.CLOSE_HISTORY_PATH.write_text('{"symbols": {"OLD": {"last_time": 1, "closes": [1]}}}')
        generator._write_close_history(self._rows(1200), AS_OF)
        symbols = json.loads(generator.CLOSE_HISTORY_PATH.read_text())["symbols"]
        self.assertIn("OLD", symbols)
        self.assertEqual(len(symbols), 1201)


if __name__ == "__main__":
    unittest.main()
