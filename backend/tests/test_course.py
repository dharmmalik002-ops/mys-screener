"""The Course page's backend: historical bars for the case studies and the
setup lessons' real examples, and saved progress (app/services/course.py)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import course  # noqa: E402


def _yahoo(rows):
    """Yahoo chart JSON for (unix stamp, o, h, l, c, v) rows."""
    return {
        "chart": {
            "result": [
                {
                    "timestamp": [r[0] for r in rows],
                    "indicators": {
                        "quote": [
                            {
                                "open": [r[1] for r in rows],
                                "high": [r[2] for r in rows],
                                "low": [r[3] for r in rows],
                                "close": [r[4] for r in rows],
                                "volume": [r[5] for r in rows],
                            }
                        ]
                    },
                }
            ]
        }
    }


# 2021-01-04 09:15 IST and the next session, as Yahoo stamps them.
JAN4 = 1609731900
JAN5 = JAN4 + 86400


class ParseTests(unittest.TestCase):
    def test_bars_are_keyed_to_the_ist_session_at_midnight_utc(self):
        bars = course.parse_yahoo_chart(_yahoo([(JAN4, 170, 175, 168, 173, 1000)]))
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0]["time"], 1609718400)  # 2021-01-04T00:00Z
        self.assertEqual(bars[0]["close"], 173)

    def test_a_missing_price_drops_the_row_rather_than_zeroing_it(self):
        bars = course.parse_yahoo_chart(_yahoo([(JAN4, 170, 175, 168, None, 1000), (JAN5, 173, 180, 172, 179, None)]))
        self.assertEqual([b["close"] for b in bars], [179])
        self.assertEqual(bars[0]["volume"], 0.0)

    def test_empty_or_malformed_payloads_give_no_bars(self):
        self.assertEqual(course.parse_yahoo_chart({}), [])
        self.assertEqual(course.parse_yahoo_chart({"chart": {"result": None}}), [])


class SymbolAndWindowTests(unittest.TestCase):
    def test_case_labels_reduce_to_the_ticker(self):
        self.assertEqual(course.clean_symbol("TVSMOTOR (short, futures)"), "TVSMOTOR")
        self.assertEqual(course.clean_symbol("IRCTC FEB FUT"), "IRCTC")
        self.assertEqual(course.clean_symbol("m&m"), "M&M")

    def test_junk_is_refused(self):
        for bad in ("", "../etc", "   "):
            with self.assertRaises(course.CourseBarsError):
                course.clean_symbol(bad)

    def test_window_is_clamped_to_today_and_bounded(self):
        lo, hi = course.parse_window("2026-09-01", "2027-01-01", today=date(2026, 10, 9))
        self.assertEqual(hi, date(2026, 10, 9))
        with self.assertRaises(course.CourseBarsError):
            course.parse_window("2015-01-01", "2026-01-01", today=date(2026, 10, 9))
        with self.assertRaises(course.CourseBarsError):
            course.parse_window("2026-01-05", "2026-01-01", today=date(2026, 10, 9))


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.calls = []

    def tearDown(self):
        self.tmp.cleanup()

    def _bars(self, payload):
        def fetch(symbol, lo, hi):
            self.calls.append((symbol, lo, hi))
            return payload

        return course.CourseBars(Path(self.tmp.name), fetcher=fetch)

    def test_a_closed_window_is_fetched_once(self):
        bars = self._bars([{"time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 0}])
        first = bars.get("SEQUENT", "2020-11-01", "2021-03-01", today=date(2026, 10, 9))
        second = bars.get("SEQUENT", "2020-11-01", "2021-03-01", today=date(2026, 10, 9))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(first["bars"], second["bars"])

    def test_a_failed_fetch_is_not_cached(self):
        bars = self._bars([])
        bars.get("GONE", "2020-11-01", "2021-03-01", today=date(2026, 10, 9))
        bars.get("GONE", "2020-11-01", "2021-03-01", today=date(2026, 10, 9))
        self.assertEqual(len(self.calls), 2)


class ProgressTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = course.CourseProgressStore(None, Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def test_round_trip_through_the_file(self):
        self.assertEqual(self.store.load()["done"], {})
        self.store.save({"done": {"setups-1": True}, "notes": {"setups-1": "tight!", "x": "   "}, "cards": {}, "scores": {}})
        loaded = self.store.load()
        self.assertEqual(loaded["done"], {"setups-1": True})
        self.assertEqual(loaded["notes"], {"setups-1": "tight!"})  # blank notes are dropped
        self.assertTrue(loaded["updated_at"])

    def test_a_save_that_would_wipe_the_record_is_refused(self):
        full = {f"lesson-{i}": True for i in range(30)}
        self.store.save({"done": full})
        with self.assertRaises(course.ProgressShrinkRefused):
            self.store.save({"done": {}})
        self.assertEqual(len(self.store.load()["done"]), 30)

    def test_small_edits_and_explicit_resets_go_through(self):
        full = {f"lesson-{i}": True for i in range(30)}
        self.store.save({"done": full})
        fewer = dict(list(full.items())[:-2])
        self.assertEqual(len(self.store.save({"done": fewer})["done"]), 28)
        self.assertEqual(self.store.save({"done": {}, "allowShrink": True})["done"], {})

    def test_wrong_shapes_are_rejected(self):
        with self.assertRaises(ValueError):
            self.store.save({"done": ["setups-1"]})


class RouteTests(unittest.TestCase):
    """The /api/course routes, mounted on a bare app."""

    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.routes import build_router

        self.tmp = tempfile.TemporaryDirectory()
        app = FastAPI()
        app.include_router(build_router({"india": object()}))
        self.client = TestClient(app)

    def tearDown(self):
        self.tmp.cleanup()

    def test_bars_fall_back_to_the_providers_full_history(self):
        """Yahoo refusing the exact window must not blank the chart: the route
        falls back to the provider's own downloader for the whole history."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.routes import build_router
        from app.core.config import get_settings

        day = 86400
        jan4 = 1609718400  # 2021-01-04T00:00Z

        import pandas as pd

        frame = pd.DataFrame(
            {"Open": [1, 170, 173], "High": [1, 175, 180], "Low": [1, 168, 172], "Close": [1, 173, 179], "Volume": [1, 1000, 900]},
            index=pd.to_datetime([jan4 - 400 * day, jan4, jan4 + day], unit="s"),  # the first is outside the window
        )

        class Provider:
            def _resolve_ticker(self, symbol):
                return f"{symbol}.NS"

            def _download_history_frame(self, ticker, period, interval):
                assert (ticker, period, interval) == ("SEQUENT.NS", "max", "1d")
                return frame

            def _split_adjusted_history(self, history):
                return history

        class Service:
            provider = Provider()

            async def get_chart(self, symbol, timeframe):
                return None  # the provider's ~2-year chart cannot reach 2021

        get_settings.cache_clear()
        try:
            with mock.patch.dict("os.environ", {"APP_STATE_DIR": self.tmp.name, "DATABASE_URL": ""}), \
                    mock.patch.object(course, "default_fetcher", return_value=[]):
                app = FastAPI()
                app.include_router(build_router({"india": Service()}))
                client = TestClient(app)
                body = client.get("/api/course/bars", params={"symbol": "SEQUENT", "start": "2020-12-01", "end": "2021-02-01"}).json()
                self.assertEqual([b["close"] for b in body["bars"]], [173, 179])
                self.assertEqual(body["source"], "site-history")
                again = client.get("/api/course/bars", params={"symbol": "SEQUENT", "start": "2020-12-01", "end": "2021-02-01"}).json()
                self.assertEqual(len(again["bars"]), 2)  # served from the disk cache
        finally:
            get_settings.cache_clear()

    def test_local_chart_cache_answers_without_any_network(self):
        """Recent examples sit inside the Space's warmed chart_cache; they must
        load from it instantly, never queue on Yahoo."""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.routes import build_router
        from app.core.config import get_settings
        from app.services import study_deck

        day = 86400
        start = 1735689600  # 2025-01-01T00:00Z
        cached = [{"time": start + i * day, "open": 10, "high": 11, "low": 9, "close": 10 + i * 0.01, "volume": 5} for i in range(600)]

        class Service:
            async def get_chart(self, symbol, timeframe):
                raise AssertionError("provider must not be asked when the cache covers the window")

            async def get_chart_full_history(self, symbol, timeframe):
                raise AssertionError("full history must not be asked when the cache covers the window")

        yahoo = mock.Mock(return_value=[])
        get_settings.cache_clear()
        try:
            with mock.patch.dict("os.environ", {"APP_STATE_DIR": self.tmp.name, "DATABASE_URL": ""}), \
                    mock.patch.object(course, "default_fetcher", yahoo), \
                    mock.patch.object(study_deck, "_read_bars", return_value=cached):
                app = FastAPI()
                app.include_router(build_router({"india": Service()}))
                body = TestClient(app).get(
                    "/api/course/bars", params={"symbol": "ABC", "start": "2025-03-01", "end": "2026-05-01"}
                ).json()
            self.assertEqual(body["source"], "chart-cache")
            self.assertGreater(len(body["bars"]), 300)
            yahoo.assert_not_called()
        finally:
            get_settings.cache_clear()

    def test_bars_route_rejects_a_bad_window(self):
        response = self.client.get("/api/course/bars", params={"symbol": "SEQUENT", "start": "2021-02-01", "end": "2021-01-01"})
        self.assertEqual(response.status_code, 400)

    def test_progress_round_trip_and_shrink_guard(self):
        from app.core.config import get_settings

        get_settings.cache_clear()
        try:
            with mock.patch.dict("os.environ", {"APP_STATE_DIR": self.tmp.name, "DATABASE_URL": ""}):
                full = {"done": {f"l-{i}": True for i in range(20)}, "notes": {}, "cards": {}, "scores": {}}
                self.assertEqual(self.client.put("/api/course/progress", json=full).status_code, 200)
                self.assertEqual(len(self.client.get("/api/course/progress").json()["done"]), 20)
                self.assertEqual(self.client.put("/api/course/progress", json={"done": {}}).status_code, 409)
                self.assertTrue((Path(self.tmp.name) / "data" / "course_progress.json").exists())
        finally:
            get_settings.cache_clear()

if __name__ == "__main__":
    unittest.main()
