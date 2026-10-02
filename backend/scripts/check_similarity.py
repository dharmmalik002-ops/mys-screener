#!/usr/bin/env python3
"""Before vs after: do the new similarity layers find charts that look more alike?

    python3 scripts/check_similarity.py

On today's market, for every Indian chart, takes the closest Minervini example
and the closest Indian peer twice — once the old way (raw 120-session
fingerprint) and once through the three layers — and measures how alike each
pair really is:

* price path, 120 and 60 sessions: root-mean-square gap between the two
  closing-price curves after each is scaled to its own range (0 = identical)
* measured shape: the gap across distance-from-high, base depth, pullbacks,
  tightness, volume, trend. The new ranking optimises this, so it is reported
  but it is not the independent test — the price paths are.

Writes data/lookalike_state/similarity_check.json.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import projection, render, scoring, shape, similarity  # noqa: E402
from app.services.lookalike.pipeline import library_dir  # noqa: E402

STYLE = "minervini"


def curve(c, n):
    c = np.asarray(c[-n:], dtype=float)
    lo, hi = c.min(), c.max()
    return (c - lo) / (hi - lo or 1.0)


def rms(a, b):
    return float(np.sqrt(np.mean((a - b) ** 2)))


def summary(old, new):
    old, new = np.array(old), np.array(new)
    return {
        "old_median": round(float(np.median(old)), 4),
        "new_median": round(float(np.median(new)), 4),
        "improvement_pct": round(float((np.median(old) - np.median(new)) / np.median(old) * 100), 1),
        "new_closer_pct": round(float((new < old).mean() * 100), 1),
        "same_match_pct": round(float((new == old).mean() * 100), 1),
    }


def main() -> int:
    data_dir = ROOT / "data"
    library = scoring.load_library(data_dir)
    universe, index = scoring.load_universe(data_dir)
    s = scoring.score(universe, index, library)
    refs = library.refs[STYLE]
    ref_curves = [r["window"]["c"] for r in refs]
    raw_ref = library.X_ref[STYLE]
    F_ref = library.F_ref.get(STYLE)
    scale = shape.robust_scale(s.F)

    res = {k: ([], []) for k in ("ref_path_120", "ref_path_60", "ref_shape", "peer_path_120", "peer_path_60", "peer_shape")}
    raw_cos = s.X @ raw_ref.T
    peer_raw = s.X @ s.X.T
    np.fill_diagonal(peer_raw, -np.inf)
    peer_new = s.peers(1)
    for i in range(len(s.symbols)):
        q120, q60 = curve(s.windows[i]["c"], 120), curve(s.windows[i]["c"], 60)
        j_old = int(np.argmax(raw_cos[i]))
        j_new = int(s.nearest(STYLE, i, 1)[0])
        for j, slot in ((j_old, 0), (j_new, 1)):
            res["ref_path_120"][slot].append(rms(q120, curve(ref_curves[j], 120)))
            res["ref_path_60"][slot].append(rms(q60, curve(ref_curves[j], 60)))
            if F_ref is not None and s.F is not None:
                res["ref_shape"][slot].append(float(shape.distance(s.F[i : i + 1], F_ref[j : j + 1], scale)[0, 0]))
        p_old = int(np.argmax(peer_raw[i]))
        p_new = int(peer_new[i][0])
        for j, slot in ((p_old, 0), (p_new, 1)):
            res["peer_path_120"][slot].append(rms(q120, curve(s.windows[j]["c"], 120)))
            res["peer_path_60"][slot].append(rms(q60, curve(s.windows[j]["c"], 60)))
            res["peer_shape"][slot].append(float(shape.distance(s.F[i : i + 1], s.F[j : j + 1], scale)[0, 0]))

    head_report = {}
    rp = library_dir(data_dir) / projection.REPORT_FILE
    if rp.exists():
        head_report = json.loads(rp.read_text())
    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session": s.as_of.isoformat(),
        "charts": len(s.symbols),
        "trained_layer_in_use": library.head is not None,
        "trained_layer_test": head_report.get("by_scale"),
        "closest_minervini_example": {k.replace("ref_", ""): summary(*res[k]) for k in ("ref_path_120", "ref_path_60", "ref_shape") if res[k][0]},
        "closest_indian_peer": {k.replace("peer_", ""): summary(*res[k]) for k in ("peer_path_120", "peer_path_60", "peer_shape")},
        "note": "Lower is more alike. 'path' measures compare the closing-price curves and are the independent test; "
                "'shape' is what the new ranking optimises.",
    }
    (library_dir(data_dir) / "similarity_check.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
