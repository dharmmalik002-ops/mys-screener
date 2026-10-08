"""Setup gallery: Indian history entries, their stored charts, and the endpoints."""

from __future__ import annotations

import gzip
import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

import numpy as np

from app.services.lookalike import history, outcome


@dataclass
class Bars:
    symbol: str
    dates: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray


def _bars(symbol: str, n: int = 400, seed: int = 0) -> Bars:
    rng = np.random.default_rng(seed)
    c = 100 * np.exp(np.cumsum(rng.normal(0.001, 0.02, n)))
    days, d = [], date(2020, 1, 1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return Bars(symbol, np.array(days), c * 0.995, c * 1.01, c * 0.99, c, rng.uniform(1e5, 2e5, n))


class HistoryTests(unittest.TestCase):
    def test_keeps_the_strongest_matches_once_per_gap_with_charts(self):
        bars = {s: _bars(s, seed=i) for i, s in enumerate("ABCDE")}
        syms = list(bars)
        day = bars["A"].dates[250]
        logits = {"zanger_flag": np.array([5.0, 4.0, 3.0, 2.0, 1.0])}
        pct = {"zanger_flag": np.array([99.0, 98.0, 97.5, 96.0, 99.9])}
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            hist = history.load(data)
            n = history.add_day(data, hist, day, syms, [1.0] * 5, logits, pct, ["zanger_flag"], bars)
            self.assertEqual(n, 3)  # top three by score, each above the percentile floor
            self.assertEqual([r["symbol"] for r in hist["styles"]["zanger_flag"]], ["A", "B", "C"])
            # a week later the same stocks are not repeated
            n2 = history.add_day(data, hist, bars["A"].dates[255], syms, [1.0] * 5, logits, pct, ["zanger_flag"], bars)
            self.assertEqual(n2, 0)
            history.save(data, hist)
            saved = json.loads((data / history.HISTORY_FILE).read_text())
            month = day.isoformat()[:7]
            key = f"zanger_flag/{month}"
            self.assertIn(key, saved["files"])
            charts = json.loads(gzip.decompress((data / history.CHARTS_DIR / "zanger_flag" / f"{month}.json.gz").read_bytes()))["charts"]
            ch = charts[f"A@{day.isoformat()}"]
            self.assertEqual(ch["setup_index"], 119)
            self.assertGreater(len(ch["c"]), 120)  # the sessions that followed are drawn too
            self.assertIn(saved["styles"]["zanger_flag"][0]["label"], (outcome.WORKED, outcome.FAILED))

    def test_a_pending_entry_is_graded_and_redrawn_later(self):
        full = _bars("A", seed=3)
        short = Bars("A", full.dates[:260], full.open[:260], full.high[:260], full.low[:260], full.close[:260], full.volume[:260])
        day = full.dates[255]
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            hist = history.load(data)
            history.add_day(data, hist, day, ["A"], [1.0], {"s": np.array([1.0])}, {"s": np.array([99.0])}, ["s"], {"A": short})
            self.assertEqual(hist["styles"]["s"][0]["label"], outcome.PENDING)
            history.save(data, hist)
            hist = history.load(data)
            self.assertEqual(history.regrade(data, hist, {"A": full}), 1)
            self.assertNotEqual(hist["styles"]["s"][0]["label"], outcome.PENDING)
            history.save(data, hist)
            charts = json.loads(gzip.decompress((data / history.CHARTS_DIR / "s" / f"{day.isoformat()[:7]}.json.gz").read_bytes()))["charts"]
            self.assertGreater(len(charts[f"A@{day.isoformat()}"]["c"]), 125)


class GalleryRouteTests(unittest.TestCase):
    def test_index_trader_and_india_endpoints(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.lookalike_routes import build_lookalike_router

        bars = {"A": _bars("A", seed=1)}
        day = bars["A"].dates[250]
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            hist = history.load(data)
            history.add_day(data, hist, day, ["A"], [1.0], {"zanger_flag": np.array([1.0])}, {"zanger_flag": np.array([99.0])},
                            ["zanger_flag"], bars)
            history.save(data, hist)
            (data / "lookalike_refs.json").write_text(json.dumps({
                "generated_at": "x", "shards": {}, "styles": {"zanger_flag": [["XYZ", "2010-01-04", "worked"]]},
                "about": {"zanger_flag": {"shown": True, "recognition": 0.73, "notes": {"setup": "flag", "summary": "s"}}},
            }))
            app = FastAPI()
            app.include_router(build_lookalike_router(data, None, data / "state"))
            with mock.patch.dict(os.environ, {"LOOKALIKE_SELF_UPDATE": "0"}):
                c = TestClient(app)
                idx = c.get("/api/lookalikes/gallery").json()
                self.assertTrue(idx["available"])
                self.assertEqual(idx["styles"]["zanger_flag"]["trader_charts"], 1)
                self.assertEqual(idx["styles"]["zanger_flag"]["india_history"], 1)
                self.assertTrue(idx["styles"]["zanger_flag"]["shown"])
                india = c.get("/api/lookalikes/gallery/zanger_flag/india?page=0&size=10").json()
                self.assertEqual(india["total"], 1)
                self.assertEqual(india["rows"][0]["symbol"], "A")
                self.assertEqual(len(india["rows"][0]["chart"]["c"]) > 120, True)
                none = c.get("/api/lookalikes/gallery/zanger_flag/india?outcome=pending").json()
                self.assertEqual(none["total"], 0)


if __name__ == "__main__":
    unittest.main()


class AiReviewRouteTests(unittest.TestCase):
    def test_reviews_a_match_once_and_caches_it(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.lookalike_routes import build_lookalike_router
        from app.services.lookalike import ai_review

        window = {k: list(np.linspace(0.1, 0.9, 120)) for k in ("o", "h", "l", "c", "sma", "v")}
        calls = []

        def fake_review(self, key, chart, label, description):
            calls.append((key, label))
            out = {"verdict": "yes", "score": 4, "why": "tight flag", "look_for": "last two weeks"}
            self._load()[key] = out
            return out

        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            (data / "lookalikes.json").write_text(json.dumps({
                "generated_at": "x", "session": "2026-10-02",
                "styles": {"zanger_flag": {"matches": [{"symbol": "ABC", "window": window}]}},
            }))
            app = FastAPI()
            app.include_router(build_lookalike_router(data, None, data / "state", llm_api_key="test-key"))
            with mock.patch.dict(os.environ, {"LOOKALIKE_SELF_UPDATE": "0"}), mock.patch.object(ai_review.Reviewer, "review", fake_review):
                c = TestClient(app)
                r = c.get("/api/lookalikes/ai-review?style=zanger_flag&symbol=abc").json()
                self.assertEqual(r["review"]["verdict"], "yes")
                self.assertEqual(calls, [("zanger_flag|ABC|2026-10-02", "flag & pennant")])
                c.get("/api/lookalikes/ai-review?style=zanger_flag&symbol=ABC")
                self.assertEqual(len(calls), 1)  # served from the cache
                self.assertEqual(c.get("/api/lookalikes/ai-review?style=zanger_flag&symbol=XYZ").status_code, 404)

    def test_the_prompt_forbids_advice(self):
        from app.services.lookalike import ai_review

        text = ai_review.prompt("flag & pennant", "a short pause after a strong move")
        self.assertIn("Do not give buy or sell advice", text)
        self.assertIn("flag & pennant", text)
