#!/usr/bin/env python3
"""Learn from a library of reference charts, one model per style.

    # add charts to a style, then rebuild every style
    python3 scripts/build_lookalike_library.py --style zanger ~/Charts/zanger/
    python3 scripts/build_lookalike_library.py --style zanger --url https://chartpattern.com/sample-newsletter.cfm
    python3 scripts/build_lookalike_library.py --style minervini ideas.txt
    # rebuild from everything already added
    python3 scripts/build_lookalike_library.py

Added lists are saved as plain `TICKER,YYYY-MM-DD` lines under
`data/lookalike/sources/<style>/`, and every run rebuilds from all of them.

Only filenames are read (`NVDA-08-12-26.gif` = NVDA on 12 Aug 2026); every chart
is redrawn from Yahoo prices. Writes `data/lookalike/` (gitignored). Needs
torch + transformers + yfinance, which the Space does not install.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import pipeline, references  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="*", type=Path, help="folders of chart images, saved newsletter pages, or text lists")
    parser.add_argument("--url", action="append", default=[], help="a newsletter page to read chart names from")
    parser.add_argument("--style", help="whose setups these are, e.g. zanger or minervini (required when adding)")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.paths or args.url:
        if not args.style:
            parser.error("--style is required when adding charts")
        added, unparsed = references.collect(args.paths)
        if unparsed:
            print(f"WARNING: {len(unparsed)} image names did not parse as TICKER-MM-DD-YY, e.g. {unparsed[:5]}")
        for url in args.url:
            # requests, not urllib: stdlib urllib fails certificate checks on stock
            # macOS Python (the same trap as the liquid-fund NAV, gotcha 123).
            import requests

            html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30).text
            added.extend(references.from_html_text(html, url))
        added = sorted({r.key: r for r in added}.values(), key=lambda r: (r.day, r.ticker))
        if added:
            name = "+".join(p.stem for p in args.paths) or args.url[0].split("/")[-1]
            path = references.save_source(args.data_dir, args.style, name, added)
            print(f"added {len(added)} charts to style '{args.style}' -> {path}")

    refs, unparsed = references.collect_sources(args.data_dir)
    by_style: dict[str, int] = {}
    for r in refs:
        by_style[r.style] = by_style.get(r.style, 0) + 1
    print("library:", ", ".join(f"{k} {v}" for k, v in sorted(by_style.items())) or "empty")
    if not refs:
        print("nothing to learn from")
        return 1

    summaries = pipeline.build_library(args.data_dir, refs)
    for style, summary in summaries.items():
        print(f"\n=== {style} ===")
        print(json.dumps({k: v for k, v in summary.items() if k != "skipped"}, indent=2))
        if summary["skipped"]:
            reasons: dict[str, int] = {}
            for row in summary["skipped"]:
                reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1
            print(f"skipped {len(summary['skipped'])}: {reasons}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
