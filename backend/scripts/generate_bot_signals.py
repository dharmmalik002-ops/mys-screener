#!/usr/bin/env python3
"""Write today's bot candidates to `data/bot_signals.json`.

    python3 scripts/generate_bot_signals.py

Run after `build_deep_history.py`. The output is small (a few KB) and committed,
because the Space has no `deep_history/` — that store is ~100 MB and gitignored,
so a live scan there would find nothing and the Bot tab would render empty. This
is the same shape as the committed `sector_indices.json` fallback and for the
same reason: compute where the data is, ship the result.

The file carries `as_of`, and the API refuses to present signals older than a
few sessions rather than showing a stale list as if it were today's.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.services.bot.live import scan_today  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bot-signals")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity", type=float, default=1_000_000.0,
                        help="book size used for the sizing column")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    artifact_path = data_dir / "bot_backtest.json"
    if not artifact_path.exists():
        logger.error("no bot_backtest.json — run scripts/run_bot_backtest.py first")
        return 1

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload = scan_today(data_dir, artifact, equity=args.equity)
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    payload["source"] = "offline"

    out_path = Path(args.out) if args.out else data_dir / "bot_signals.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "wrote %s — regime=%s stance=%s candidates=%d",
        out_path, (payload.get("regime") or {}).get("regime"), payload.get("stance"),
        len(payload.get("candidates") or []),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
