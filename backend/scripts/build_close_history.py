"""Build `data/close_history.json` — daily closes for the scan universe.

Why this exists: `chart_grid_points` on a Space-built snapshot is a stub. The
daily bhavcopy patch keeps prices, returns and MAs current but only ever
APPENDS to an existing grid, so a snapshot built without history never gains
one — measured 2026-09-19, every live Qullamaggie row was running on the 20
trailing closes, and Power Base and VCP (which need months) returned nothing at
all. The history exists on the Space (`provider.get_chart` serves 508 daily bars
per symbol) but it is not in the snapshot, and no scan can afford a per-symbol
fetch across ~1,900 names.

So the closes ship as a committed artifact, the same reason `sector_indices.json`
ships (CLAUDE.md gotcha 14). Built from the local `data/chart_cache/`, which is
gitignored and therefore only ever populated on a workstation.

Refresh it at least every ~4 weeks: the runtime splices the artifact with the
snapshot's 20 trailing closes by matching the overlap, so up to 20 sessions of
drift heal themselves, and beyond that a gap opens.

Run: `cd backend && python3 scripts/build_close_history.py`
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

SESSIONS = 200  # VCP needs 90 base + 63 run-up; the rest is slack
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
CACHE_DIR = DATA_DIR / "chart_cache"
OUT_PATH = DATA_DIR / "close_history.json"
MIN_SESSIONS = 60  # below this a symbol cannot support any base scan


def build() -> dict:
    closes_by_symbol: dict[str, dict] = {}
    skipped = 0
    for path in sorted(CACHE_DIR.glob("*__1D.json")):
        symbol = path.name.split("__", 1)[0]
        try:
            payload = json.loads(path.read_text())
        except (ValueError, OSError):
            skipped += 1
            continue
        bars = payload.get("bars") or []
        rows = [
            (int(bar["time"]), round(float(bar["close"]), 2))
            for bar in bars
            if bar.get("time") and bar.get("close")
        ]
        rows = [row for row in rows if row[1] > 0][-SESSIONS:]
        if len(rows) < MIN_SESSIONS:
            skipped += 1
            continue
        closes_by_symbol[symbol] = {
            "last_time": rows[-1][0],
            "closes": [value for _, value in rows],
        }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sessions": SESSIONS,
        "symbols": closes_by_symbol,
        "skipped": skipped,
    }


if __name__ == "__main__":
    artifact = build()
    # Never let a partial crawl replace a good artifact (same guard the mutual
    # fund refresh uses on its universe).
    if OUT_PATH.exists():
        existing = json.loads(OUT_PATH.read_text())
        old_count = len(existing.get("symbols") or {})
        if len(artifact["symbols"]) < old_count * 0.8:
            raise SystemExit(
                f"refusing to shrink close_history: {len(artifact['symbols'])} symbols vs {old_count} on disk"
            )
    OUT_PATH.write_text(json.dumps(artifact, separators=(",", ":")))
    size_mb = OUT_PATH.stat().st_size / 1_000_000
    print(f"wrote {OUT_PATH} — {len(artifact['symbols'])} symbols, {size_mb:.1f} MB, skipped {artifact['skipped']}")
