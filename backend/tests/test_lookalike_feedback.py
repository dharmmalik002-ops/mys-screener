"""👍/👎 feedback: stored once per match, a 👎 hides at once, and learning is
only allowed to change the ranking when it predicts unseen votes better."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from app.services.lookalike import feedback, shape


class StoreTests(unittest.TestCase):
    def test_a_vote_replaces_the_last_and_zero_removes_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = feedback.FeedbackStore(None, Path(tmp))
            store.record("abc", "2026-10-01", "ref", "minervini:X@2020-01-02", 1)
            store.record("ABC", "2026-10-01", "ref", "minervini:X@2020-01-02", -1)
            self.assertEqual([v["vote"] for v in store.for_symbol("ABC")], [-1])
            store.record("ABC", "2026-10-01", "ref", "minervini:X@2020-01-02", 0)
            self.assertEqual(store.for_symbol("ABC"), [])

    def test_bad_votes_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = feedback.FeedbackStore(None, Path(tmp))
            for args in (("A", "2026-10-01", "nope", "T", 1), ("A", "not-a-date", "peer", "T", 1), ("A", "2026-10-01", "peer", "T", 5)):
                with self.assertRaises(ValueError):
                    store.record(*args)

    def test_a_restart_without_a_database_restores_the_committed_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            backup = Path(tmp) / "votes.json"
            backup.write_text(json.dumps({"votes": [{"id": "A|2026-10-01|peer|B", "query": "A", "session": "2026-10-01", "kind": "peer", "target": "B", "vote": -1}]}))
            store = feedback.FeedbackStore(None, Path(tmp) / "fresh-state", backup=backup)
            self.assertEqual(len(store.for_symbol("A")), 1)


class HideTests(unittest.TestCase):
    def test_a_recent_thumbs_down_hides_the_match_an_old_one_does_not(self):
        today = date(2026, 10, 2)
        votes = [
            {"query": "A", "session": "2026-10-01", "kind": "peer", "target": "B", "vote": -1},
            {"query": "A", "session": (today - timedelta(days=200)).isoformat(), "kind": "peer", "target": "C", "vote": -1},
            {"query": "A", "session": "2026-10-01", "kind": "peer", "target": "D", "vote": 1},
        ]
        self.assertEqual(feedback.hidden_for(votes, "A", today), {("peer", "B")})


class LearnTests(unittest.TestCase):
    def _examples(self, n, informative: bool, seed=0):
        rng = np.random.default_rng(seed)
        out = []
        for _ in range(n):
            gaps = rng.uniform(0, 3, len(shape.NAMES))
            look = rng.uniform(0, 1)
            if informative:
                # this user only cares about base depth (feature 1)
                vote = 1 if gaps[1] < 1.0 else -1
            else:
                vote = int(rng.choice([-1, 1]))
            out.append({"look": look, "gaps": list(gaps), "vote": vote})
        return out

    def test_waits_for_enough_votes_of_each_kind(self):
        m = feedback.learn(self._examples(15, True))
        self.assertFalse(m["in_use"])
        self.assertIn("starts at", m["status"])

    def test_learns_the_feature_a_user_cares_about(self):
        m = feedback.learn(self._examples(300, True))
        self.assertTrue(m["in_use"], m["status"])
        self.assertEqual(int(np.argmax(m["weights"])), 1)

    def test_random_votes_do_not_change_the_ranking(self):
        m = feedback.learn(self._examples(300, False, seed=3))
        self.assertFalse(m["in_use"], m["status"])

    def test_weights_change_the_shape_distance(self):
        A = np.array([[0.0, 0.0]]); B = np.array([[1.0, 3.0]])
        scale = np.ones(2)
        self.assertGreater(float(shape.distance(A, B, scale, np.array([0.1, 1.9]))[0, 0]),
                           float(shape.distance(A, B, scale, np.array([1.9, 0.1]))[0, 0]))


class RouteTests(unittest.TestCase):
    def test_similar_hides_rejected_matches_and_refills_the_list(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from unittest import mock

        from app.api import lookalike_routes as lr

        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            session = date.today().isoformat()
            peers = [[f"P{i}", 0.9 - i / 100] for i in range(10)]
            (d / lr.INDEX_FILE).write_text(json.dumps({"session": session, "symbols": {
                "A": {"session": session, "styles": {"minervini": {"percentile": 99, "near": [[f"minervini:R{i}@2020-01-01", 0.9] for i in range(8)]}}, "peers": peers},
                **{p: {"session": session, "closes": [1, 2]} for p, _ in peers},
            }}))
            app = FastAPI()
            app.include_router(lr.build_lookalike_router(d, None, d / "state"))
            with mock.patch.object(lr, "_maybe_pull_in_background", lambda data_dir: None):
                c = TestClient(app)
                r = c.post("/api/lookalikes/feedback", json={"query": "A", "session": session, "kind": "peer", "target": "P0", "vote": -1})
                self.assertEqual(r.status_code, 200)
                c.post("/api/lookalikes/feedback", json={"query": "A", "session": session, "kind": "ref", "target": "minervini:R0@2020-01-01", "vote": 1})
                body = c.get("/api/lookalikes/similar/A").json()
            self.assertEqual([p["symbol"] for p in body["peers"]], ["P1", "P2", "P3", "P4", "P5", "P6"])
            self.assertEqual(len(body["styles"]["minervini"]["near"]), 5)
            self.assertEqual(body["styles"]["minervini"]["votes"]["minervini:R0@2020-01-01"], 1)
            self.assertEqual(body["hidden"], 1)


if __name__ == "__main__":
    unittest.main()
