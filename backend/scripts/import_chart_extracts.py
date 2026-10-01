#!/usr/bin/env python3
"""Turn charts read off course slides / PDFs into look-alike references.

    python3 scripts/import_chart_extracts.py --style minervini --name mtp2022 extracts/*.jsonl

Input: JSON lines with ticker, buy_date (YYYY-MM-DD), date_precision
(day/week/month), example_type and confidence — one per chart, as written by
the page readers. Each reading is pinned to its exact breakout session
(`lookalike/refine.py`) and saved as plain `TICKER,YYYY-MM-DD` under
`data/lookalike/sources/<style>/<name>.txt`. Charts the slide presents as a
FAILURE (a violation, what not to buy) are kept out of the style: teaching a
style from its own counter-examples would blur it. They are written to
`data/lookalike/extracts/<name>_failures.txt` for the record, with any
paraphrased rules beside them. Nothing here is published.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import references, refine, us_bars  # noqa: E402

MIN_CONFIDENCE = 0.5
DUPLICATE_DAYS = 10


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--style", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)

    rows, rules = [], []
    for path in args.files:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if "rule" in obj:
                rules.append(obj["rule"])
            elif obj.get("ticker") and obj.get("buy_date"):
                rows.append(obj)

    stats = Counter()
    kept: dict[str, list[tuple[date, dict]]] = {}
    failures = []
    for obj in rows:
        stats["read"] += 1
        try:
            day = date.fromisoformat(str(obj["buy_date"])[:10])
        except ValueError:
            stats["bad date"] += 1
            continue
        ticker = str(obj["ticker"]).strip().upper().lstrip("$")
        if float(obj.get("confidence") or 0) < MIN_CONFIDENCE:
            stats["low confidence"] += 1
            continue
        if obj.get("example_type") == "failure":
            failures.append(f"{ticker},{day.isoformat()}")
            stats["failure example"] += 1
            continue
        near = [d for d, _ in kept.get(ticker, []) if abs((d - day).days) <= DUPLICATE_DAYS]
        if near:
            stats["duplicate"] += 1
            continue
        kept.setdefault(ticker, []).append((day, obj))

    refs = []
    refined_count = 0
    for ticker, items in sorted(kept.items()):
        earliest = min(d for d, _ in items)
        series = us_bars.load(args.data_dir, ticker, earliest, need_through=date.today() - timedelta(days=4))
        if series is None or not series.dates:
            stats["no price history"] += len(items)
            continue
        for day, obj in items:
            approx = series.index_on_or_before(day)
            if approx is None or (day - series.dates[approx]).days > 6:
                stats["date outside history"] += 1
                continue
            r = refine.refine(series.h, series.c, series.v, approx, str(obj.get("date_precision") or "month"))
            refined_count += r.refined
            refs.append(references.Reference(ticker, series.dates[r.end], obj.get("file", ""), args.style))

    path = references.save_source(args.data_dir, args.style, args.name, sorted(refs, key=lambda r: (r.day, r.ticker)))
    out = args.data_dir / "lookalike" / "extracts"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{args.name}_failures.txt").write_text("\n".join(failures) + "\n")
    (out / f"{args.name}_rules.txt").write_text("\n".join(sorted(set(rules))) + "\n")

    print(f"charts read: {stats['read']}")
    for k, v in stats.items():
        if k != "read":
            print(f"  {k}: {v}")
    print(f"references saved: {len(refs)} ({refined_count} pinned to a breakout session) -> {path}")
    print(f"failure examples kept aside: {len(failures)}; rules noted: {len(set(rules))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
