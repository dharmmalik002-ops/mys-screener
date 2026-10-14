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

**The runners are refused too.** This nightly build kept 13 sectors on
17 Jul 2026 for three months because Yahoo turned the GitHub runner away and
the builder kept what it had. Every series — fetched or kept — is now
completed from NSE's own per-session closing file (`nse_index_close`), so a
refused Yahoo costs nothing but the deep history it already shipped.

Run from `backend/`:

    python3 scripts/build_sector_indices.py

Refuses to shrink the file: a throttled run that fetched two symbols must not
overwrite sixteen good ones. Merge-and-keep is the rule, same as the fund
universe's commit guard.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.mutual_funds import (  # noqa: E402
    benchmarks,
    index_source,
    nse_index_close,
    paths,
    sector_stages,
)

OUT_PATH = paths.DATA_DIR / "sector_indices.json"

# Five years of daily bars is enough for every range the chart offers plus the
# 150-day average drawn on it; the full weekly series carries the rest, and
# stage analysis only ever reads weekly.
DAILY_YEARS = 5
MARKET_SYMBOL = "^NSEI"
MARKET_LABEL = "Nifty 50"


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


def _raw(payload: dict) -> dict | None:
    """A fetched series as plain OHLC lists, oldest first."""
    dates = list(payload.get("dates") or [])
    closes = [float(v) for v in (payload.get("navs") or [])]
    if len(dates) < 200 or len(closes) != len(dates):
        return None
    pick = lambda key: [float(v) for v in (payload.get(key) or closes)]  # noqa: E731
    return {"dates": dates, "opens": pick("opens"), "highs": pick("highs"),
            "lows": pick("lows"), "closes": closes}


def _extend_entry(entry: dict, label: str, files: dict) -> int:
    """Append NSE sessions to a committed entry Yahoo would not refresh.

    The entry holds five years of daily bars and the full weekly series, so
    only the tail is recomputed: the 30-week average for the new days (on the
    stored daily closes, which hold far more than 150 sessions) and the weeks
    from the first new bar's ISO week on, so a week that was part-filled when
    last built is re-folded with its remaining days.
    """
    daily = entry["daily"]
    raw = {k: list(daily[k]) for k in ("dates", "opens", "highs", "lows", "closes")}
    before = len(raw["dates"])
    added = nse_index_close.extend(raw, label, files)
    if not added:
        return 0

    ma = sector_stages._sma(raw["closes"], sector_stages.MA_WEEKS * 5)
    entry["daily"] = {
        **{k: _round(v) if k != "dates" else v for k, v in raw.items()},
        "ma30w": list(daily.get("ma30w") or [None] * before)
        + [round(v, 2) if v is not None else None for v in ma[before:]],
    }

    first_week = date.fromisoformat(raw["dates"][before]).isocalendar()[:2]
    start = before
    while start > 0 and date.fromisoformat(raw["dates"][start - 1]).isocalendar()[:2] == first_week:
        start -= 1
    tail = sector_stages._to_weekly(raw["dates"][start:], raw["closes"][start:],
                                    raw["highs"][start:], raw["lows"][start:])
    weekly = entry["weekly"]
    keep = sum(1 for d in weekly["dates"] if date.fromisoformat(d).isocalendar()[:2] < first_week)
    entry["weekly"] = {
        key: list(weekly[key][:keep]) + (_round(tail[key]) if key != "dates" else tail[key])
        for key in ("dates", "opens", "highs", "lows", "closes")
    }
    return added


def main() -> int:
    existing: dict = {}
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text())
        except ValueError:
            existing = {}
    sectors: dict = dict(existing.get("sectors") or {})

    targets = [(b.key, b.label, b.yahoo_symbol) for b in benchmarks.SECTOR_BENCHMARKS if b.yahoo_symbol]
    targets.append(("__market__", MARKET_LABEL, MARKET_SYMBOL))
    entries: dict = {**sectors}
    if existing.get("market"):
        entries["__market__"] = existing["market"]

    # 1. Yahoo, which has the full history but refuses most of these symbols
    #    from datacenter IPs (the runners included).
    fresh: dict[str, dict] = {}
    for key, label, symbol in targets:
        try:
            raw = _raw(index_source.fetch_index_series(symbol, force=True, want_ohlc=True))
        except Exception as exc:
            raw = None
            print(f"  {label:26s} Yahoo FAILED {type(exc).__name__}")
        if raw is not None:
            fresh[key] = raw

    # 2. NSE's closing files for every session after the stalest series —
    #    fetched once and shared, since one file carries every index.
    lasts = [fresh[k]["dates"][-1] if k in fresh else entries[k]["daily"]["dates"][-1]
             for k, _, _ in targets if k in fresh or k in entries]
    files = nse_index_close.sessions_after(date.fromisoformat(min(lasts))) if lasts else {}
    print(f"  NSE closing files read: {len(files)}"
          + (f" ({min(files)} .. {max(files)})" if files else ""))

    fetched = extended = kept = failed = 0
    for key, label, symbol in targets:
        if key in fresh:
            raw = fresh[key]
            added = nse_index_close.extend(raw, label, files)
            packed = _pack({"dates": raw["dates"], "navs": raw["closes"], "opens": raw["opens"],
                            "highs": raw["highs"], "lows": raw["lows"]})
            if packed is not None:
                entries[key] = {"symbol": symbol, "name": label, **packed}
                fetched += 1
                print(f"  {label:26s} {packed['daily']['dates'][-1]}  (Yahoo"
                      f"{f' + {added} sessions from NSE' if added else ''})")
                continue
        if key in entries:
            added = _extend_entry(entries[key], label, files)
            last = entries[key]["daily"]["dates"][-1]
            if added:
                extended += 1
                print(f"  {label:26s} {last}  (+{added} sessions from NSE)")
            else:
                kept += 1
                print(f"  {label:26s} {last}  kept existing")
        else:
            failed += 1
            print(f"  {label:26s} MISSING")

    market = entries.pop("__market__", None)
    sectors = entries

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
    print(f"\nWrote {OUT_PATH.name}: {len(sectors)} sectors + market "
          f"({fetched} from Yahoo, {extended} extended from NSE, {kept} unchanged, {failed} missing), "
          f"{size_mb:.2f} MB")
    stalest = min(entry["daily"]["dates"][-1] for entry in sectors.values())
    newest = max(entry["daily"]["dates"][-1] for entry in sectors.values())
    if stalest < newest:
        behind = [e["name"] for e in sectors.values() if e["daily"]["dates"][-1] == stalest]
        print(f"WARNING: {len(behind)} sector(s) end on {stalest}, behind {newest}: {', '.join(behind)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
