"""Build the committed sector-index artifact.

**Why this file exists.** Yahoo serves the Nifty sector indices to ordinary
residential IPs but refuses most of them from datacenter ranges — the
Hugging Face Space gets `^CNXIT`, `^NSEBANK` and `^CNXPHARMA` and is turned
away from the other thirteen. Fetching at request time therefore produced a
sector page with three sectors on it in production and sixteen in development,
which is the worst possible failure: it looks like it works.

So the history ships with the app, exactly like `mf_universe.json` and for
exactly the same reason — a cold Space has to serve the page immediately.
Runtime still prefers a live fetch when one succeeds, so a machine that *can*
reach the feed gets today's bar; the artifact is the floor, not a ceiling.

Run from `backend/`:

    python3 scripts/build_sector_indices.py

Refuses to shrink the file: a throttled run that fetched two symbols must not
overwrite sixteen good ones. Merge-and-keep is the rule, same as the fund
universe's commit guard.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.mutual_funds import benchmarks, index_source, paths, sector_stages  # noqa: E402

OUT_PATH = paths.DATA_DIR / "sector_indices.json"

# Five years of daily bars is enough for every range the chart offers plus the
# 150-day average drawn on it; the full weekly series carries the rest, and
# stage analysis only ever reads weekly.
DAILY_YEARS = 5
MARKET_SYMBOL = "^NSEI"


def _round(values: list[float], digits: int = 2) -> list[float]:
    return [round(float(v), digits) for v in values]


def _pack(payload: dict) -> dict | None:
    dates = payload.get("dates") or []
    closes = payload.get("navs") or []
    if len(dates) < 200:
        return None
    highs = payload.get("highs") or closes
    lows = payload.get("lows") or closes
    opens = payload.get("opens") or closes

    weekly = sector_stages._to_weekly(dates, closes, highs, lows)
    cutoff = f"{datetime.now(timezone.utc).year - DAILY_YEARS}-01-01"
    start = next((i for i, d in enumerate(dates) if d >= cutoff), 0)

    # The average is computed on the *full* series then windowed, so the first
    # day of the stored window already has one — computing it on the window
    # would leave the first 150 days blank on every chart.
    ma = sector_stages._sma(closes, sector_stages.MA_WEEKS * 5)

    return {
        "weekly": {
            "dates": weekly["dates"],
            "opens": _round(weekly["opens"]),
            "highs": _round(weekly["highs"]),
            "lows": _round(weekly["lows"]),
            "closes": _round(weekly["closes"]),
        },
        "daily": {
            "dates": dates[start:],
            "opens": _round(opens[start:]),
            "highs": _round(highs[start:]),
            "lows": _round(lows[start:]),
            "closes": _round(closes[start:]),
            "ma30w": [round(v, 2) if v is not None else None for v in ma[start:]],
        },
    }


def main() -> int:
    existing: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text())
        except ValueError:
            existing = {}
    sectors: dict = dict(existing.get("sectors") or {})

    targets = [(b.key, b.label, b.yahoo_symbol) for b in benchmarks.SECTOR_BENCHMARKS if b.yahoo_symbol]
    fetched, kept, failed = 0, 0, 0

    for key, label, symbol in targets:
        try:
            packed = _pack(index_source.fetch_index_series(symbol, force=True, want_ohlc=True))
        except Exception as exc:
            packed = None
            print(f"  {label:26s} FAILED {type(exc).__name__}")
        if packed is None:
            if key in sectors:
                kept += 1
                print(f"  {label:26s} kept existing ({sectors[key]['daily']['dates'][-1]})")
            else:
                failed += 1
            continue
        sectors[key] = {"symbol": symbol, "name": label, **packed}
        fetched += 1
        print(f"  {label:26s} {packed['daily']['dates'][-1]}  "
              f"{len(packed['weekly']['dates'])}w / {len(packed['daily']['dates'])}d")

    market = (existing.get("market") or None)
    try:
        packed_market = _pack(index_source.fetch_index_series(MARKET_SYMBOL, force=True, want_ohlc=True))
        if packed_market:
            market = {"symbol": MARKET_SYMBOL, "name": "Nifty 50", **packed_market}
    except Exception as exc:
        print(f"  market {MARKET_SYMBOL} FAILED {type(exc).__name__}; keeping existing")

    if not sectors:
        print("Refusing to write an empty artifact.")
        return 1
    if existing.get("sectors") and len(sectors) < len(existing["sectors"]):
        print("Refusing to write fewer sectors than are already committed.")
        return 1

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sector_count": len(sectors),
        "sectors": sectors,
        "market": market,
    }
    temp = OUT_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(payload, separators=(",", ":")))
    temp.replace(OUT_PATH)

    size_mb = OUT_PATH.stat().st_size / 1e6
    print(f"\nWrote {OUT_PATH.name}: {len(sectors)} sectors "
          f"({fetched} fresh, {kept} kept, {failed} missing), {size_mb:.2f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
