"""Daily picks: the rules that keep the record honest.

Picks need both the picture and the trader's rules; a stock is not re-picked
while it is the same setup; a pick's result is not evidence until it is known;
and the learner is not allowed to rank anything until it has beaten a coin flip
on picks it did not learn from.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np

from app.services.lookalike import model, outcome, picks, rules
from app.services.lookalike.scoring import Library, Scored

FULL = {k: True for k, _ in rules.RULES}
METRICS = {
    "price": 100.0, "ma50": 95.0, "ma150": 90.0, "ma200": 85.0, "ma200_month_ago": 84.0,
    "below_high_pct": 3.0, "above_low_pct": 80.0, "return_6m_pct": 40.0, "index_return_6m_pct": 10.0,
    "range_2w_pct": 5.0, "range_base_pct": 9.0, "volume_ratio": 0.7,
}


def _library(n_refs: int = 3) -> Library:
    refs = [{
        "key": f"R{i}@2020-01-01", "style": "minervini", "ticker": f"R{i}", "date": "2020-01-01",
        "label": "worked" if i == 0 else "failed", "max_gain_pct": 25.0, "max_loss_pct": -9.0, "days_to_result": 12,
    } for i in range(n_refs)]
    return Library(
        styles={"minervini": {"worked_rate_pct": {"setups": 25.0}}},
        refs={"minervini": refs},
        clf={"minervini": model.Logistic(np.zeros(4), 0.0, np.zeros(4))},
        X_ref={"minervini": np.eye(n_refs, 4)},
        cal_logits={"minervini": np.zeros(10)},
    )


def _scored(day: date, symbols, pct, logits, flags=None) -> Scored:
    n = len(symbols)
    return Scored(
        as_of=day,
        symbols=list(symbols),
        sessions=[day] * n,
        closes=[100.0] * n,
        turnover=[10.0] * n,
        windows=[{}] * n,
        flags=flags or [dict(FULL) for _ in range(n)],
        metrics=[dict(METRICS) for _ in range(n)],
        X=np.random.default_rng(0).normal(size=(n, 4)),
        logits={"minervini": np.array(logits, dtype=float)},
        percentile={"minervini": np.array(pct, dtype=float)},
        sims={"minervini": np.tile(np.array([0.9, 0.5, 0.1]), (n, 1))},
    )


class ChooseTests(unittest.TestCase):
    def test_needs_both_the_picture_and_all_eight_rules(self):
        ledger, fps = {"picks": {}}, {}
        seven = dict(FULL, beats_index=False)
        s = _scored(date(2025, 1, 3), ["A", "B", "C"], [99, 90, 99], [3, 2, 1], [dict(FULL), dict(FULL), seven])
        made = picks.choose(ledger, fps, s, _library(), source="live")
        self.assertEqual([p["symbol"] for p in made], ["A"])  # B fails the picture, C fails a rule

    def test_at_most_ten_best_first(self):
        ledger, fps = {"picks": {}}, {}
        syms = [f"S{i}" for i in range(15)]
        s = _scored(date(2025, 1, 3), syms, [99] * 15, list(range(15)))
        made = picks.choose(ledger, fps, s, _library(), source="live")
        self.assertEqual(len(made), picks.MAX_PICKS)
        self.assertEqual(made[0]["symbol"], "S14")

    def test_same_setup_is_not_picked_twice(self):
        ledger, fps = {"picks": {}}, {}
        d0 = date(2025, 1, 3)
        picks.choose(ledger, fps, _scored(d0, ["A"], [99], [1]), _library(), source="live")
        again = picks.choose(ledger, fps, _scored(d0 + timedelta(days=7), ["A"], [99], [1]), _library(), source="live")
        later = picks.choose(ledger, fps, _scored(d0 + timedelta(days=picks.REPICK_GAP_DAYS), ["A"], [99], [1]), _library(), source="live")
        self.assertEqual((len(again), len(later)), (0, 1))

    def test_rerunning_a_day_replaces_it(self):
        ledger, fps = {"picks": {}}, {}
        d = date(2025, 1, 3)
        picks.choose(ledger, fps, _scored(d, ["A", "B"], [99, 99], [2, 1]), _library(), source="live")
        picks.choose(ledger, fps, _scored(d, ["C"], [99], [1]), _library(), source="live")
        self.assertEqual(sorted(p["symbol"] for p in ledger["picks"].values()), ["C"])

    def test_every_pick_explains_itself_with_numbers_and_links(self):
        ledger, fps = {"picks": {}}, {}
        made = picks.choose(ledger, fps, _scored(date(2025, 1, 3), ["M&M"], [99], [1]), _library(), source="live")
        p = made[0]
        self.assertIn("99%", p["reason"])
        self.assertIn("3.0% below its high", p["reason"])
        self.assertIn("R0 · 2020-01-01", p["reason"])
        self.assertIn("NSE%3AM_M", p["links"]["tradingview"])
        self.assertTrue(p["nearest"][0]["link"].startswith("https://www.tradingview.com/"))
        self.assertEqual(p["outcome"]["label"], outcome.PENDING)


@dataclass
class _Bars:
    symbol: str
    dates: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray


class ReviewTests(unittest.TestCase):
    def _bars(self, symbol, start, path):
        n = len(path)
        dates = np.array([start + timedelta(days=i) for i in range(n)], dtype=object)
        c = np.array(path, dtype=float)
        return _Bars(symbol, dates, c.copy(), c * 1.001, c * 0.999, c)

    def test_a_pick_is_graded_once_its_result_is_known(self):
        start = date(2025, 1, 1)
        ledger = {"picks": {"x": {"id": "x", "symbol": "A", "session": start.isoformat(), "outcome": {"label": "pending"}}}}
        bars = self._bars("A", start, [100] * 3 + [125] * 5)
        picks.review(ledger, [bars], start + timedelta(days=10))
        o = ledger["picks"]["x"]["outcome"]
        self.assertEqual(o["label"], outcome.WORKED)
        self.assertEqual(o["resolved_on"], (start + timedelta(days=3)).isoformat())

    def test_an_unfinished_pick_stays_open(self):
        start = date(2025, 1, 1)
        ledger = {"picks": {"x": {"id": "x", "symbol": "A", "session": start.isoformat(), "outcome": {"label": "pending"}}}}
        picks.review(ledger, [self._bars("A", start, [100] * 10)], start + timedelta(days=10))
        self.assertEqual(ledger["picks"]["x"]["outcome"]["label"], outcome.PENDING)


class FeedbackTests(unittest.TestCase):
    def _ledger(self, n_worked, n_failed, resolved=date(2025, 6, 1)):
        ledger, fps = {"picks": {}}, {}
        rng = np.random.default_rng(1)
        for i in range(n_worked + n_failed):
            label = outcome.WORKED if i < n_worked else outcome.FAILED
            pid = f"p{i}"
            ledger["picks"][pid] = {
                "id": pid, "date": (date(2024, 1, 1) + timedelta(days=i)).isoformat(), "rules": dict(FULL),
                "outcome": {"label": label, "resolved_on": resolved.isoformat()},
            }
            fps[pid] = rng.normal(size=8) + (1.5 if label == outcome.WORKED else 0)
        return ledger, fps

    def test_watches_until_there_is_enough_evidence(self):
        ledger, fps = self._ledger(10, 10)
        fb = picks.feedback_status(ledger, fps)
        self.assertFalse(fb.in_use)
        self.assertIn("Watching", fb.status)

    def test_a_result_is_not_evidence_before_it_was_known(self):
        ledger, fps = self._ledger(80, 80, resolved=date(2025, 6, 1))
        before = picks.feedback_status(ledger, fps, as_of=date(2025, 6, 1))
        after = picks.feedback_status(ledger, fps, as_of=date(2025, 6, 2))
        self.assertEqual(sum(before.train.values()) + sum(before.test.values()), 0)
        self.assertGreater(sum(after.train.values()), 0)

    def test_it_is_only_used_once_it_beats_a_coin_flip_out_of_sample(self):
        ledger, fps = self._ledger(80, 80)
        # interleave so both halves of the chronological split have both labels
        for i, p in enumerate(sorted(ledger["picks"].values(), key=lambda p: p["id"])):
            p["date"] = (date(2024, 1, 1) + timedelta(days=(i % 80) * 2 + (i >= 80))).isoformat()
        fb = picks.feedback_status(ledger, fps)
        self.assertTrue(fb.in_use, fb.status)
        noise = {k: np.random.default_rng(int(k[1:])).normal(size=8) for k in fps}
        self.assertFalse(picks.feedback_status(ledger, noise).in_use)

    def test_lessons_need_both_sides_populated(self):
        ledger, _ = self._ledger(5, 5)
        for p in ledger["picks"].values():
            p.update({"metrics": {"below_high_pct": 2.0}, "percentile": 99.5, "nearest": [], "rank": 1})
        self.assertTrue(all(not row["enough"] for row in picks.lessons(ledger)))


class FingerprintStoreTests(unittest.TestCase):
    def test_per_day_files_round_trip_and_an_unchanged_day_is_not_rewritten(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            rng = np.random.default_rng(3)
            fps = {f"2025-01-0{i}:minervini:S{j}": rng.normal(size=8).astype(np.float32) for i in (3, 4) for j in range(3)}
            picks.save_fingerprints(d, fps)
            back = picks.load_fingerprints(d)
            self.assertEqual(set(back), set(fps))
            self.assertTrue(all(np.array_equal(back[k], fps[k]) for k in fps))
            day = picks._lib_dir(d) / picks.FINGERPRINTS_DIR / "2025-01-03.npy"
            before = day.stat().st_mtime_ns
            fps["2025-01-05:minervini:NEW"] = rng.normal(size=8).astype(np.float32)
            picks.save_fingerprints(d, fps)
            self.assertEqual(day.stat().st_mtime_ns, before)
            self.assertEqual(len(picks.load_fingerprints(d)), 7)


class DayFileTests(unittest.TestCase):
    def test_unchanged_content_is_not_rewritten(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "d.json"
            self.assertTrue(picks._write_if_changed(path, {"b": 1, "a": [1, 2]}))
            self.assertFalse(picks._write_if_changed(path, {"a": [1, 2], "b": 1}))
            self.assertTrue(picks._write_if_changed(path, {"a": [1, 3], "b": 1}))

    def test_a_stale_day_file_is_fetched_once_its_checksum_differs(self):
        import json
        import tempfile
        import zlib
        from pathlib import Path
        from unittest import mock

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api import lookalike_routes as lr

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            fresh = json.dumps({"date": "2025-01-03", "picks": [{"id": "x", "nearest": []}]}).encode()
            (d / lr.DAYS_DIR).mkdir()
            (d / lr.DAYS_DIR / "2025-01-03.json").write_text(json.dumps({"date": "2025-01-03", "picks": []}))
            (d / lr.PICKS_FILE).write_text(json.dumps({"calendar": {"2025-01-03": {"stamp": f"{zlib.crc32(fresh):08x}"}}}))
            app = FastAPI()
            app.include_router(lr.build_lookalike_router(d))
            with mock.patch.dict("os.environ", {"LOOKALIKE_SELF_UPDATE": "1"}), \
                 mock.patch("requests.get", lambda url, timeout: mock.Mock(status_code=200, content=fresh)), \
                 mock.patch.object(lr, "_maybe_pull_in_background", lambda data_dir: None):
                body = TestClient(app).get("/api/lookalikes/picks/2025-01-03").json()
            self.assertEqual([p["id"] for p in body["picks"]], ["x"])
            self.assertEqual((d / lr.DAYS_DIR / "2025-01-03.json").read_bytes(), fresh)


class EveningTriggerTests(unittest.TestCase):
    def test_only_a_publish_after_six_pm_today_counts_as_done(self):
        from datetime import datetime

        from app import main as m

        now = datetime(2026, 10, 1, 19, 30, tzinfo=m.IST)
        self.assertTrue(m.lookalike_ran_today("2026-10-01T13:00:00+00:00", now))   # 18:30 IST today
        self.assertFalse(m.lookalike_ran_today("2026-10-01T10:32:53+00:00", now))  # 16:02 IST, a manual run
        self.assertFalse(m.lookalike_ran_today("2026-09-30T13:00:00+00:00", now))  # yesterday
        self.assertFalse(m.lookalike_ran_today("", now))


class GithubPythonCompatTests(unittest.TestCase):
    def test_no_f_string_reuses_its_own_quote(self):
        """The daily run is on GitHub's Python 3.11; development is on 3.12+,
        which accepts an f-string that reuses its own quote inside {}. 3.11
        refuses to load the whole script — the first evening's run failed on
        exactly this — so the pattern is caught here instead."""
        import re
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        files = sorted((root / "app/services/lookalike").glob("*.py")) + sorted((root / "scripts").glob("*lookalike*.py")) + [
            root / "app/api/lookalike_routes.py",
            root / "scripts/import_chart_extracts.py",
            root / "scripts/build_deep_history.py",
        ]
        bad = []
        for f in files:
            for n, line in enumerate(f.read_text().splitlines(), 1):
                for m in re.finditer(r"""\bf(["'])""", line):
                    quote, i, depth = m.group(1), m.end(), 0
                    while i < len(line):
                        ch = line[i]
                        if ch == "{":
                            depth += 1
                        elif ch == "}":
                            depth = max(0, depth - 1)
                        elif ch == quote:
                            if depth:
                                bad.append(f"{f.name}:{n}")
                            break
                        i += 1
        self.assertEqual(bad, [])


class SelfUpdateTests(unittest.TestCase):
    def test_only_a_newer_readable_copy_replaces_the_file(self):
        import json
        import tempfile
        from pathlib import Path
        from unittest import mock

        from app.api import lookalike_routes as lr

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / lr.RESULT_FILE).write_text(json.dumps({"generated_at": "2026-09-30T10:00:00+00:00", "v": "local"}))
            (d / lr.PICKS_FILE).write_text(json.dumps({"generated_at": "2026-09-30T10:00:00+00:00", "v": "local"}))
            remote = {
                lr.RESULT_FILE: {"generated_at": "2026-10-01T10:00:00+00:00", "v": "remote"},  # newer
                lr.PICKS_FILE: {"generated_at": "2026-09-29T10:00:00+00:00", "v": "remote"},   # older
            }

            def fake_get(url, timeout):
                name = url.rsplit("/", 1)[-1]
                return mock.Mock(status_code=200, json=lambda: remote[name])

            with mock.patch("requests.get", fake_get):
                changed = lr.pull_latest(d)
            self.assertEqual(changed, [lr.RESULT_FILE])
            self.assertEqual(json.loads((d / lr.RESULT_FILE).read_text())["v"], "remote")
            self.assertEqual(json.loads((d / lr.PICKS_FILE).read_text())["v"], "local")


if __name__ == "__main__":
    unittest.main()
