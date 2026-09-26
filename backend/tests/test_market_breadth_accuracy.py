"""Market breadth must be the exchange's numbers, not an artefact of our caches.

Every case here is a defect that shipped:
* the Home page's 10-session chart counted 4 stocks (the Space's chart cache),
* the XP inputs used Yahoo's previous close, which is two sessions old whenever
  Yahoo drops a day (2026-09-18: 230 stocks "up 4.5%" against NSE's 162),
* a split stock read "below its EMA" for weeks because the EMA was never
  restated,
* holidays got rows because the Yahoo fallback returned the prior session.

Run: `cd backend && pytest tests/test_market_breadth_accuracy.py`
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.services import nse_breadth
from app.services.xp_breadth import daily_breadth_metrics

HEADER = "TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,TckrSymb,SctySrs,XpryDt,FininstrmActlXpryDt,StrkPric,OptnTp,FinInstrmNm,OpnPric,HghPric,LwPric,ClsPric,LastPric,PrvsClsgPric\n"


def _line(symbol: str, series: str, isin: str, close: float, prev: float) -> str:
    return f"2026-09-25,2026-09-25,CM,NSE,STK,1,{isin},{symbol},{series},,,,,{symbol} Ltd,{prev},{close},{close},{close},{close},{prev}\n"


class NseBreadthRowTests(unittest.TestCase):
    def test_counts_only_mainboard_shares(self) -> None:
        csv_text = HEADER + "".join([
            _line("UPEQ", "EQ", "INE000A01011", 110, 100),
            _line("DOWNBE", "BE", "INE000A01012", 90, 100),
            _line("FLATBZ", "BZ", "INE000A01013", 100, 100),
            _line("NIFTYBEES", "EQ", "INF000A01014", 110, 100),  # ETF: fund unit ISIN
            _line("SMESTOCK", "SM", "INE000A01015", 110, 100),   # SME board
            _line("GOVBOND", "GS", "IN0000A01016", 110, 100),     # government security
        ])
        row = nse_breadth.day_row("2026-09-25", nse_breadth.parse_rows(csv_text))
        assert row is not None
        self.assertEqual(
            (row["advances"], row["declines"], row["unchanged"], row["total"]),
            (1, 1, 1, 3),
        )

    def test_merge_is_idempotent_on_date(self) -> None:
        first = {"date": "2026-09-25", "advances": 1, "declines": 2, "unchanged": 0, "total": 3}
        rerun = {"date": "2026-09-25", "advances": 2, "declines": 1, "unchanged": 0, "total": 3}
        merged = nse_breadth.merge_days([first], [rerun])
        self.assertEqual(merged, [rerun])

    def test_committed_history_has_no_non_session_rows(self) -> None:
        path = Path(__file__).resolve().parents[1] / "data" / nse_breadth.HISTORY_FILENAME
        doc = json.loads(path.read_text())
        dates = [row["date"] for row in doc["days"]]
        self.assertEqual(dates, sorted(set(dates)))
        # 14 Sep 2026 was Ganesh Chaturthi; NSE published no bhavcopy.
        self.assertNotIn("2026-09-14", dates)
        for row in doc["days"]:
            self.assertEqual(row["advances"] + row["declines"] + row["unchanged"], row["total"])
            # The whole market, not a sliver of it.
            self.assertGreater(row["total"], 2000)


class XpSplitRestatementTests(unittest.TestCase):
    def test_a_split_does_not_read_as_a_collapse_below_the_ema(self) -> None:
        state: dict[str, list[float]] = {}
        # Ten flat sessions at 1,000.
        for i in range(10):
            _, state = daily_breadth_metrics(f"d{i}", {"X": {"c": 1000.0, "p": 1000.0}}, state)
        # 1:2 split, then a small up day: the exchange restates prev to 500.
        metrics, state = daily_breadth_metrics("split", {"X": {"c": 505.0, "p": 500.0}}, state)
        self.assertEqual(metrics["ma10_pct"], 100.0)
        self.assertEqual(metrics["ma20_pct"], 100.0)
        self.assertAlmostEqual(state["X"][2], 505.0)

    def test_old_two_float_state_is_still_read(self) -> None:
        metrics, state = daily_breadth_metrics("d", {"X": {"c": 110.0, "p": 100.0}}, {"X": [100.0, 100.0]})
        self.assertEqual(metrics["ma10_pct"], 100.0)
        self.assertEqual(len(state["X"]), 3)


class DashboardBreadthSourceTests(unittest.TestCase):
    def _service(self, data_dir: Path):
        from app.services.dashboard_service import DashboardService

        service = DashboardService.__new__(DashboardService)
        service._legacy_data_dir = lambda: data_dir  # type: ignore[method-assign]
        return service

    def test_exchange_rows_are_served_newest_last(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            rows = [
                {"date": f"2026-09-{d:02d}", "advances": 1200, "declines": 1300, "unchanged": 30, "total": 2530}
                for d in (21, 22, 23, 24, 25)
            ]
            (Path(tmp) / nse_breadth.HISTORY_FILENAME).write_text(
                json.dumps({"universe": nse_breadth.UNIVERSE_LABEL, "days": rows})
            )
            out = self._service(Path(tmp))._load_exchange_breadth(10)
        self.assertEqual([r.date for r in out][-1], "2026-09-25")
        self.assertEqual(out[-1].universe, nse_breadth.UNIVERSE_LABEL)
        self.assertEqual(len(out), 5)

    def test_missing_file_sends_the_caller_to_the_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(self._service(Path(tmp))._load_exchange_breadth(10), [])

    def test_fallback_history_drops_sessions_counted_off_a_sliver(self) -> None:
        from datetime import datetime, timezone

        from app.models.market import BreadthDayCounts

        service = self._service(Path("."))
        service._breadth_history_cache = None
        sliver = BreadthDayCounts(date="2026-09-24", advances=0, declines=4, unchanged=0, total=4)
        full = BreadthDayCounts(date="2026-09-25", advances=500, declines=480, unchanged=20, total=1000)
        snapshots = [object()] * 1000
        with mock.patch.object(type(service), "_breadth_history_from_chart_cache", return_value=[sliver, full]):
            out = service._breadth_history_cached(snapshots, datetime.now(timezone.utc), 10)  # type: ignore[arg-type]
        self.assertEqual([r.date for r in out], ["2026-09-25"])


if __name__ == "__main__":
    unittest.main()
