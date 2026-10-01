#!/usr/bin/env python3
"""Score every Indian stock's latest chart against the reference library.

    python3 scripts/scan_lookalikes.py
    python3 scripts/scan_lookalikes.py --show-reference-names   # private use only

Reads `data/deep_history/` and `data/lookalike/`, writes `data/lookalikes.json`
— the only file the website reads. Reference names are hidden unless asked for,
because that file is served publicly.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import pipeline  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--top", type=int, default=pipeline.TOP_MATCHES)
    parser.add_argument("--show-reference-names", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    payload = pipeline.scan_india(args.data_dir, top=args.top, show_reference_names=args.show_reference_names)
    print(f"session {payload['session']}  scanned {payload['scanned']}  wrote {pipeline.RESULT_FILE}")
    for style, block in payload["styles"].items():
        print(f"\n=== {style} ===")
        for m in block["matches"][:10]:
            near = payload["references"][m["nearest"][0]["key"]]
            print(f"{m['rank']:>3} {m['symbol']:<12} pct {m['percentile']:>5.1f}  score {m['score']:>7.3f}  "
                  f"nearest {near['name']} ({near['label']}, sim {m['nearest'][0]['similarity']:.3f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
