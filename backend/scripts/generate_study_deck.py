#!/usr/bin/env python3
"""Mine a chart-reading drill deck: historical VCP / flag setups plus what they did next.

Replays the chosen setup scanners across `data/chart_cache` exactly the way
`generate_breakout_stats.py` does — same point-in-time snapshot rebuild, same
forward simulation — but keeps every individual signal instead of aggregating
them away. The result is `data/study_deck.json`, which the Study page deals from.

    python3 scripts/generate_study_deck.py --weeks 104
    python3 scripts/generate_study_deck.py --weeks 8 --limit-symbols 200   # quick check

Run this on a workstation, never on the Space: rebuilding one symbol-date
snapshot costs ~16ms, so two years over ~1,700 symbols is a few hours of CPU and
far more memory headroom than the 16 GB box has to spare.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

BACKEND_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

import generate_breakout_stats as gbs  # noqa: E402  (reuses its replay machinery)

from app.models.market import StockSnapshot  # noqa: E402
from app.providers.free import FreeMarketDataProvider  # noqa: E402
from app.scanners.definitions import SCANS, run_scan  # noqa: E402
from app.services import breakout_stats as bs  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("study-deck")

# The two patterns the drill trains, and nothing else by default. `power-base`
# is deliberately excluded: it fires ~1,500 times a week, which would swamp the
# deck with marginal examples and dull the eye instead of sharpening it.
DEFAULT_SETUPS = ("vcp", "high-tight-flag")

# A scanner's own trade plan, when it publishes one, reads
# "Entry 123.40 | Stop 118.00 | Risk 4.4%".
TRADE_PLAN_RE = re.compile(
    r"Entry\s+([\d.]+)\s*\|\s*Stop\s+([\d.]+)\s*\|\s*Risk\s+([\d.]+)%"
)


def parse_trade_plan(reasons: list[str]) -> tuple[float | None, float | None, float | None]:
    for reason in reasons:
        match = TRADE_PLAN_RE.search(reason)
        if match:
            return float(match.group(1)), float(match.group(2)), float(match.group(3))
    return None, None, None


def card_id(setup: str, symbol: str, trigger: date) -> str:
    return f"{setup}|{symbol}|{trigger.isoformat()}"


def collect(
    snapshots: list[StockSnapshot],
    as_of: date,
    scans: list,
    universe: dict[str, dict],
    leading_groups: set[str],
    group_of: dict[str, str],
) -> list[dict]:
    """Every match for the chosen setups on this session, with its full reasoning."""
    rows: list[dict] = []
    for scan in scans:
        try:
            matches = run_scan(scan, snapshots)
        except Exception as exc:
            logger.debug("scan %s failed @ %s: %s", scan.id, as_of, exc)
            continue
        for match in matches:
            symbol = str(match.symbol).upper()
            reasons = [str(r) for r in (match.reasons or [])]
            entry, stop, risk = parse_trade_plan(reasons)
            rows.append(
                {
                    "id": card_id(scan.id, symbol, as_of),
                    "setup": scan.id,
                    "label": scan.name,
                    "symbol": symbol,
                    "name": str(match.name or symbol),
                    "trigger_date": as_of.isoformat(),
                    "entry": float(match.last_price or 0.0),
                    "stop": stop,
                    "risk_pct": risk,
                    "score": round(float(match.score or 0.0), 2),
                    "rs_rating": int(getattr(match, "rs_rating", 0) or 0),
                    "group_top_decile": group_of.get(symbol, "") in leading_groups,
                    "reasons": reasons,
                }
            )
            # `entry` from the trade plan is the pivot, not the close. The
            # simulation must use the close the signal actually fired at,
            # which is what `last_price` is — so it is left alone above.
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weeks", type=int, default=104, help="how many weeks back to replay")
    parser.add_argument(
        "--setups",
        default=",".join(DEFAULT_SETUPS),
        help=f"comma-separated scan ids (default: {','.join(DEFAULT_SETUPS)})",
    )
    parser.add_argument("--limit-symbols", type=int, default=0, help="cap symbols (testing only)")
    parser.add_argument("--min-score", type=float, default=0.0, help="drop matches below this score")
    parser.add_argument("--out", default=None, help="output path")
    args = parser.parse_args()

    wanted = [s.strip() for s in args.setups.split(",") if s.strip()]
    by_id = {scan.id: scan for scan in SCANS}
    missing = [s for s in wanted if s not in by_id]
    if missing:
        logger.error("unknown setup id(s): %s", ", ".join(missing))
        logger.error("available: %s", ", ".join(sorted(by_id)))
        return 1
    scans = [by_id[s] for s in wanted]

    data_dir = BACKEND_ROOT / "data"
    out_path = Path(args.out) if args.out else data_dir / "study_deck.json"

    provider = FreeMarketDataProvider(gemini_api_key=None, eod_only_mode=True)
    universe = gbs.load_universe(data_dir)
    benchmark = gbs.load_benchmark(data_dir)

    logger.info("loading bars…")
    frames: dict[str, pd.DataFrame] = {}
    for symbol, frame in bs.iter_symbol_bars(data_dir / "chart_cache"):
        if symbol in universe:
            frames[symbol] = frame
        if args.limit_symbols and len(frames) >= args.limit_symbols:
            break
    if not frames:
        logger.error("no usable symbols in chart_cache — nothing to replay")
        return 1
    logger.info("loaded %d symbols", len(frames))

    last_bar = max(frame.index[-1] for frame in frames.values())
    window_start = (last_bar - timedelta(weeks=args.weeks)).normalize()
    sessions = [ts for ts in benchmark.index if window_start <= ts <= last_bar]
    if not sessions:
        logger.error("no sessions in the requested window")
        return 1
    logger.info(
        "replaying %d sessions (%s → %s) for: %s",
        len(sessions), sessions[0].date(), sessions[-1].date(), ", ".join(wanted),
    )

    cards: list[dict] = []
    t0 = time.time()
    for n, as_of in enumerate(sessions, start=1):
        rows = gbs.build_rows_for_date(provider, frames, universe, benchmark, as_of)
        if len(rows) < 50:
            logger.warning("skipping %s — only %d snapshots rebuilt", as_of.date(), len(rows))
            continue
        provider._apply_rs_rating(rows)
        snapshots: list[StockSnapshot] = []
        for row in rows:
            try:
                snapshots.append(StockSnapshot.model_validate(provider._with_snapshot_fallbacks(row)))
            except Exception as exc:
                logger.debug("validate failed %s: %s", row.get("symbol"), exc)
        if not snapshots:
            continue

        leading, membership = gbs.rank_groups(snapshots)
        day_cards = collect(snapshots, as_of.date(), scans, universe, leading, membership)
        cards.extend(day_cards)
        if n % 10 == 0 or day_cards:
            logger.info(
                "[%d/%d] %s  snapshots=%d  cards=%d  total=%d  (%.1f min)",
                n, len(sessions), as_of.date(), len(snapshots), len(day_cards), len(cards),
                (time.time() - t0) / 60,
            )

    if args.min_score > 0:
        before = len(cards)
        cards = [c for c in cards if c["score"] >= args.min_score]
        logger.info("score filter >= %.1f kept %d of %d", args.min_score, len(cards), before)

    logger.info("simulating %d signals…", len(cards))
    resolved: list[dict] = []
    for card in cards:
        frame = frames.get(card["symbol"])
        if frame is None:
            continue
        trigger = date.fromisoformat(card["trigger_date"])
        forward = frame[frame.index > pd.Timestamp(trigger)]
        signal = bs.Signal(
            setup=card["setup"],
            symbol=card["symbol"],
            trigger_date=trigger,
            entry=card["entry"],
            rs_rating=card["rs_rating"],
            is_ipo=False,
            group_top_decile=card["group_top_decile"],
        )
        outcome = bs.simulate(signal, forward)
        # "open" means the horizon ran past the end of the data — no answer to
        # grade against, so the card is dropped rather than shipped unanswerable.
        if outcome is None or outcome.result == "open":
            continue
        resolved.append(
            {
                **card,
                "result": outcome.result,
                "max_favourable_pct": outcome.max_favourable_pct,
                "final_pct": outcome.final_pct,
                "sessions_held": outcome.sessions_held,
            }
        )

    counts: dict[str, dict[str, int]] = {}
    for card in resolved:
        bucket = counts.setdefault(card["setup"], {"win": 0, "loss": 0, "timeout": 0})
        bucket[card["result"]] = bucket.get(card["result"], 0) + 1

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_start": sessions[0].date().isoformat(),
        "window_end": sessions[-1].date().isoformat(),
        "sessions_replayed": len(sessions),
        "symbols_replayed": len(frames),
        "setups": wanted,
        "counts": counts,
        "rules": bs.rules_payload(),
        "cards": resolved,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload), encoding="utf-8")
    logger.info(
        "wrote %s — %d cards (%.1f MB) in %.1f min",
        out_path, len(resolved), out_path.stat().st_size / 1_048_576, (time.time() - t0) / 60,
    )
    for setup, bucket in sorted(counts.items()):
        logger.info("  %-18s win=%-5d loss=%-5d timeout=%d", setup, bucket["win"], bucket["loss"], bucket["timeout"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
