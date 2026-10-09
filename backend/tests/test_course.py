"""The Course page's backend: historical bars for case replays, archived
signals drawn whole, and saved progress (app/services/course.py)."""

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
    """The example-bars route must never hand out today's Chart Gym answer."""

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

    def test_todays_hand_is_refused_and_other_cards_are_drawn_whole(self):
        from app.services import study_archive, study_deck

        deck = study_deck.StudyDeck(BACKEND_ROOT / "data")
        deck._load()  # noqa: SLF001
        cards = list(deck._cards.values())  # noqa: SLF001
        if len(cards) < 2:
            self.skipTest("study deck not present")
        hidden = study_archive.dealt_today(deck, date.today())
        dealt = next(c for c in cards if c.id in hidden)
        free = next(c for c in cards if c.id not in hidden)

        refused = self.client.get("/api/course/example-bars", params={"card_id": dealt.id})
        self.assertEqual(refused.status_code, 403)

        context = [{"time": i, "open": 1, "high": 1, "low": 1, "close": 1} for i in range(120)]
        forward = [{"time": 200 + i, "open": 1, "high": 1, "low": 1, "close": 1} for i in range(30)]
        with mock.patch.object(study_deck, "split_bars", return_value=(context, forward)):
            ok = self.client.get("/api/course/example-bars", params={"card_id": free.id})
        self.assertEqual(ok.status_code, 200)
        body = ok.json()
        self.assertEqual(body["trigger_index"], 119)  # every context bar (<=139), then the answer
        self.assertEqual(len(body["bars"]), 150)
        self.assertEqual(body["bars"][120]["time"], 200)
        self.assertEqual(body["card"]["id"], free.id)

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
