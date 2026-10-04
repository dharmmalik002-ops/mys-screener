"""Indian charts in history that looked like each setup — the eye-training
gallery's "Indian charts → History" branch.

For each scan date (every Friday since HISTORY_FROM when backfilled, then each
evening run), each shown style keeps its TOP_PER_DAY strongest matches that
resemble the style more than MIN_PERCENTILE of ordinary charts, skipping a
stock already kept for that style in the last REPEAT_GAP_DAYS (the same base
week after week is one example, not five). Each entry is graded by what the
stock did next with the same rule as everything else here (+20% before -8%
within 40 sessions).

This is a gallery, not a track record: the library behind it includes
references dated after many of these charts, so these are "charts that look
like the setup", never "picks the system would have made then". The pick
ledger (picks.py) is the walk-forward record.

Storage (committed; the Space pulls the manifest hourly and fetches chart
files on demand, checked against the manifest's checksums):
  data/lookalike_history.json                 every entry, no prices
  data/lookalike_history/<style>/<YYYY-MM>.json   each entry's chart at its
                                              date plus what followed
The site's own chart API keeps ~500 daily bars, too few to draw 2021, which is
why the charts are stored here. Monthly files keep the daily commit small: an
evening run rewrites only the months whose entries changed.
"""

from __future__ import annotations

import gzip
import json
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from . import outcome, render

HISTORY_FILE = "lookalike_history.json"
CHARTS_DIR = "lookalike_history"
HISTORY_FROM = date(2001, 1, 1)
TOP_PER_DAY = 3
MIN_PERCENTILE = 97.0
REPEAT_GAP_DAYS = 56


def load(data_dir: Path) -> dict:
    try:
        hist = json.loads((data_dir / HISTORY_FILE).read_text())
    except (OSError, ValueError):
        hist = {}
    hist.setdefault("styles", {})
    hist.setdefault("files", {})
    hist["_dirty"] = set()
    hist["_charts"] = {}
    return hist


def _month_path(data_dir: Path, style: str, month: str) -> Path:
    # gzipped: 25 years of charts are ~4x smaller, and the Space unzips them
    return data_dir / CHARTS_DIR / style / f"{month}.json.gz"


def min_turnover_crore(day: date) -> float:
    """The scan's ₹2 cr/day floor in today's money, deflated ~8% a year for
    older dates — in 2001 ₹2 cr a day was a large stock and the floor would
    leave almost nothing to compare."""
    from .scoring import MIN_TURNOVER_CRORE

    return MIN_TURNOVER_CRORE * 0.92 ** max(0, date.today().year - day.year)


def _charts(data_dir: Path, hist: dict, style: str, month: str) -> dict:
    key = f"{style}/{month}"
    if key not in hist["_charts"]:
        try:
            hist["_charts"][key] = json.loads(gzip.decompress(_month_path(data_dir, style, month).read_bytes()))["charts"]
        except (OSError, ValueError, KeyError):
            hist["_charts"][key] = {}
    return hist["_charts"][key]


def _chart(bars, end: int) -> dict | None:
    ch = render.extended(bars.open, bars.high, bars.low, bars.close, bars.volume, end, list(bars.dates))
    if ch is None:
        return None
    ch["v"] = [round(x, 2) for x in ch["v"]]
    return ch


def save(data_dir: Path, hist: dict) -> None:
    for key in sorted(hist["_dirty"]):
        style, month = key.split("/")
        path = _month_path(data_dir, style, month)
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = json.dumps({"charts": hist["_charts"][key]}, separators=(",", ":"), sort_keys=True).encode()
        path.write_bytes(gzip.compress(raw, mtime=0))  # mtime=0: same charts, same bytes, no needless commit
        hist["files"][key] = f"{zlib.crc32(path.read_bytes()):08x}"
    hist["_dirty"] = set()
    out = {k: v for k, v in hist.items() if not k.startswith("_")}
    out["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out["rule"] = {
        "from": HISTORY_FROM.isoformat(),
        "top_per_day": TOP_PER_DAY, "min_percentile": MIN_PERCENTILE, "repeat_gap_days": REPEAT_GAP_DAYS,
        "target_pct": outcome.TARGET_PCT, "stop_pct": outcome.STOP_PCT, "horizon_sessions": outcome.HORIZON,
    }
    for rows in out["styles"].values():
        rows.sort(key=lambda r: (r["date"], -r["pct"]), reverse=True)
    (data_dir / HISTORY_FILE).write_text(json.dumps(out, separators=(",", ":")))


def add_day(data_dir: Path, hist: dict, day: date, symbols: list[str], closes: list[float],
            logits: dict[str, np.ndarray], pct: dict[str, np.ndarray], styles: list[str], by_symbol: dict) -> int:
    """Record one scan date's strongest matches per style, with their charts.
    Returns how many were added."""
    added = 0
    iso = day.isoformat()
    month = iso[:7]
    for style in styles:
        rows = hist["styles"].setdefault(style, [])
        rows[:] = [r for r in rows if r["date"] != iso]  # a re-run replaces the day
        recent = {r["symbol"] for r in rows if 0 <= (day - date.fromisoformat(r["date"])).days <= REPEAT_GAP_DAYS}
        charts = _charts(data_dir, hist, style, month)
        for k in [k for k in charts if k.endswith(f"@{iso}")]:
            del charts[k]
        order = np.argsort(-logits[style])
        kept = 0
        for i in order:
            if pct[style][i] < MIN_PERCENTILE or kept == TOP_PER_DAY:
                break
            sym = symbols[i]
            bars = by_symbol.get(sym)
            if sym in recent or bars is None:
                continue
            end = int(np.searchsorted(bars.dates, day, side="right")) - 1
            ch = _chart(bars, end) if end >= 0 else None
            if ch is None:
                continue
            g = outcome.grade(bars.open, bars.high, bars.low, end)
            rows.append({"symbol": sym, "date": iso, "session": bars.dates[end].isoformat(),
                         "pct": round(float(pct[style][i]), 1), "close": round(float(closes[i]), 2),
                         "label": g.label, "gain": g.max_gain_pct, "loss": g.max_loss_pct, "days": g.days_to_result})
            charts[f"{sym}@{iso}"] = ch
            recent.add(sym)
            kept += 1
            added += 1
        hist["_dirty"].add(f"{style}/{month}")
    return added


def regrade(data_dir: Path, hist: dict, by_symbol: dict) -> int:
    """Grade every entry that has not finished, and redraw its chart so the
    part after the date shows the sessions that have happened since."""
    changed = 0
    for style, rows in hist["styles"].items():
        for r in rows:
            if r.get("label") not in (None, outcome.PENDING):
                continue
            bars = by_symbol.get(r["symbol"])
            if bars is None:
                continue
            end = int(np.searchsorted(bars.dates, date.fromisoformat(r["date"]), side="right")) - 1
            if end < 0:
                continue
            g = outcome.grade(bars.open, bars.high, bars.low, end)
            r.update(label=g.label, gain=g.max_gain_pct, loss=g.max_loss_pct, days=g.days_to_result)
            ch = _chart(bars, end)
            if ch is not None:
                _charts(data_dir, hist, style, r["date"][:7])[f"{r['symbol']}@{r['date']}"] = ch
                hist["_dirty"].add(f"{style}/{r['date'][:7]}")
            changed += 1
    return changed


def fridays(start: date, end: date):
    d = start + timedelta(days=(4 - start.weekday()) % 7)
    while d <= end:
        yield d
        d += timedelta(days=7)
