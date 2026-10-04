"""Library storage v2: one row per reference however many styles cite it."""

from __future__ import annotations

import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import numpy as np

from app.services.lookalike import pipeline, render, scoring, shape


def _row(style: str, key: str, n: int = 150) -> dict:
    rng = np.random.default_rng(abs(hash(key)) % 2**32)
    chart = {name: [round(float(x), 3) for x in rng.random(n)] for name in pipeline.CHART_SERIES}
    chart.update({"setup_index": 119, "lo": 1.0, "hi": 2.0, "dates": {"start": "2020-01-01", "setup": "2020-06-01", "end": "2020-08-01"}})
    ticker, day = key.split("@")
    return {"key": key, "style": style, "ticker": ticker, "date": day, "session": day, "label": "worked",
            "max_gain_pct": 25.0, "max_loss_pct": -3.0, "days_to_result": 9, "rules": None,
            "chart": chart, "window": {k: chart[k][:120] for k in pipeline.CHART_SERIES}}


def _arrays(style: str, rows: list[dict]) -> dict:
    rng = np.random.default_rng(len(rows) + len(style))
    n = len(rows)
    out = {f"{style}__w": rng.random(768), f"{style}__b": np.array([0.1]), f"{style}__mean": rng.random(768).astype(np.float32),
           f"{style}__cal_logits": rng.random(5), f"{style}__F_ref": rng.random((n, len(shape.NAMES)))}
    # the same reference must carry the same fingerprint in every style
    for sc in render.SCALES:
        X = np.stack([np.random.default_rng(abs(hash((r["key"], sc))) % 2**32).random(768) for r in rows]).astype(np.float32)
        out[f"{style}__X_ref" if sc == render.WINDOW else f"{style}__X_ref{sc}"] = X
    return out


class StorageTests(unittest.TestCase):
    def test_shared_reference_is_stored_once_and_loads_back_per_style(self):
        a = [_row("zanger", "AAA@2010-01-04"), _row("zanger", "BBB@2011-02-01"), _row("zanger", "CCC@2012-03-01")]
        b = [_row("zanger_flag", "BBB@2011-02-01")]
        arrays = {**_arrays("zanger", a), **_arrays("zanger_flag", b)}
        arrays["zanger_flag__F_ref"] = arrays["zanger__F_ref"][1:2]
        summaries = {"zanger": {"skipped": [{"ticker": "X", "date": "2010-01-01", "reason": "no price history"}]}, "zanger_flag": {}}
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            out = pipeline.library_dir(data)
            out.mkdir(parents=True)
            pipeline.save_library(out, summaries, a + b, arrays)
            meta = json.loads((out / "library.json").read_text())
            self.assertEqual(len(meta["references"]), 3)  # BBB once, not twice
            self.assertNotIn("chart", meta["references"][0])
            self.assertEqual(meta["styles"]["zanger"]["skipped_reasons"], {"no price history": 1})
            lib = scoring.load_library(data)
            self.assertEqual([r["key"] for r in lib.refs["zanger_flag"]], ["BBB@2011-02-01"])
            self.assertEqual(lib.refs["zanger_flag"][0]["style"], "zanger_flag")
            np.testing.assert_allclose(lib.X_ref["zanger_flag"][0], lib.X_ref["zanger"][1], atol=1e-3)
            chart = lib.chart(lib.refs["zanger"][0])
            self.assertEqual(chart["dates"]["setup"], "2020-06-01")
            self.assertAlmostEqual(chart["c"][7], a[0]["chart"]["c"][7], places=2)
            self.assertEqual(len(lib.window(lib.refs["zanger"][0])["c"]), 120)


class ShardedRefsTests(unittest.TestCase):
    def test_exported_refs_are_served_from_their_shards(self):
        import os

        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.lookalike_routes import build_lookalike_router
        from app.services.lookalike import picks

        rows = [_row("zanger", f"T{i}@2010-01-{i + 1:02d}") for i in range(20)]
        flag = [_row("zanger_flag", "T3@2010-01-04")]
        arrays = {**_arrays("zanger", rows), **_arrays("zanger_flag", flag)}
        arrays["zanger_flag__F_ref"] = arrays["zanger__F_ref"][3:4]
        with tempfile.TemporaryDirectory() as tmp:
            data = Path(tmp)
            out = pipeline.library_dir(data)
            out.mkdir(parents=True)
            pipeline.save_library(out, {"zanger": {"built_at": "2026-10-02T00:00:00+00:00"}, "zanger_flag": {}}, rows + flag, arrays)
            lib = scoring.load_library(data)
            manifest = picks.export_refs(data, lib)
            self.assertEqual(manifest["refs_count"], 20)  # T3 once, though two styles cite it
            self.assertGreater(len(manifest["shards"]), 1)
            wanted = ["zanger:T3@2010-01-04", "zanger:T17@2010-01-18", "zanger_flag:T3@2010-01-04"]
            (data / "lookalike_index.json").write_text(json.dumps({
                "generated_at": "x", "session": "2026-10-01",
                "symbols": {"ABC": {"session": "2026-10-01", "styles": {"zanger": {"near": [[k, 0.9] for k in wanted]}}, "peers": []}},
            }))
            app = FastAPI()
            app.include_router(build_lookalike_router(data, None, data / "state"))
            with mock.patch.dict(os.environ, {"LOOKALIKE_SELF_UPDATE": "0"}):
                body = TestClient(app).get("/api/lookalikes/similar/ABC").json()
            self.assertEqual(sorted(body["refs"]), sorted(wanted))
            self.assertEqual(body["refs"][wanted[0]]["ticker"], "T3")
            self.assertEqual(body["refs"]["zanger_flag:T3@2010-01-04"]["style"], "zanger_flag")
            self.assertEqual(len(body["refs"][wanted[0]]["chart"]["c"]), 150)


if __name__ == "__main__":
    unittest.main()
