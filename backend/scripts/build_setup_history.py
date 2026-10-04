#!/usr/bin/env python3
"""Backfill the gallery's Indian history: every Friday since 2021, the charts
that looked most like each setup (`lookalike/history.py`).

    python3 scripts/build_setup_history.py                # 2001 on; ~5-60 s a Friday the first time
    python3 scripts/build_setup_history.py --from 2024-01-01

Only the 120-session picture is drawn and fingerprinted per stock — that is
all the style classifiers read — and each Friday's fingerprints are cached in
data/lookalike/history_cache/ (private), so a rebuild after the library
changes takes minutes. Needs torch + transformers (workstation only).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import embed, history, model, render, scoring  # noqa: E402


def scan(universe, day: date, cache: Path):
    """(symbols, closes, X) for every stock scorable on `day`, cached."""
    path = cache / f"{day.isoformat()}.npz"
    if path.exists():
        z = np.load(path, allow_pickle=False)
        return list(z["symbols"]), list(z["closes"]), z["X"].astype(np.float32)
    rows, images = [], []
    for bars in universe:
        end = int(np.searchsorted(bars.dates, day, side="right")) - 1
        if end < render.min_bars_needed() - 1 or (day - bars.dates[end]).days > scoring.STALE_DAYS:
            continue
        if scoring._turnover_crore(bars.close, bars.volume, end) < history.min_turnover_crore(day):
            continue
        pic = render.picture(bars.open, bars.high, bars.low, bars.close, bars.volume, end)
        if pic is None:
            continue
        rows.append((bars.symbol, float(bars.close[end])))
        images.append(pic[0])
    if not rows:
        return [], [], np.zeros((0, 768), dtype=np.float32)
    X = embed.fingerprints(images, batch_size=32)
    try:  # hand the Apple GPU's cached memory back between Fridays (an
        # overnight run otherwise ran out at Friday ~300)
        import torch

        if torch.backends.mps.is_available():
            torch.mps.empty_cache()
    except Exception:
        pass
    cache.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, symbols=np.array([r[0] for r in rows]), closes=np.array([r[1] for r in rows]), X=X.astype(np.float16))
    return [r[0] for r in rows], [r[1] for r in rows], X


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=ROOT / "data")
    ap.add_argument("--from", dest="start", type=date.fromisoformat, default=history.HISTORY_FROM)
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    t0 = time.time()
    library = scoring.load_library(args.data_dir)
    styles = scoring.shown(library)
    universe, _ = scoring.load_universe(args.data_dir)
    latest = max(b.dates[-1] for b in universe)
    cache = args.data_dir / "lookalike" / "history_cache"
    hist = history.load(args.data_dir)
    by_symbol = {b.symbol: b for b in universe}
    days = list(history.fridays(args.start, latest))
    print(f"{len(universe)} stocks, {len(days)} Fridays {days[0]} .. {days[-1]}, styles {styles}", flush=True)
    for n, day in enumerate(days, 1):
        symbols, closes, X = scan(universe, day, cache)
        if not symbols:
            continue
        logits = {s: library.clf[s].logit(X) for s in styles}
        pct = {s: model.percentile_against(logits[s], library.cal_logits[s]) for s in styles}
        history.add_day(args.data_dir, hist, day, symbols, closes, logits, pct, styles, by_symbol)
        if n % 10 == 0 or n == len(days):
            print(f"  {n}/{len(days)} {day} ({time.time() - t0:.0f}s)", flush=True)
            history.save(args.data_dir, hist)
    graded = history.regrade(args.data_dir, hist, by_symbol)
    history.save(args.data_dir, hist)
    counts = {s: len(v) for s, v in hist["styles"].items()}
    print(f"graded {graded}; entries per style {counts} ({time.time() - t0:.0f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
