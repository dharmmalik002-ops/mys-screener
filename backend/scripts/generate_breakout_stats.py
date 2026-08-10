#!/usr/bin/env python3
"""Replay scanners over cached daily bars and write breakout follow-through stats.

Run nightly (see .github/workflows/breakout-stats.yml). Writes
`backend/data/breakout_stats.json`, which the Markets regime endpoint serves.

    python3 scripts/generate_breakout_stats.py --weeks 12
    python3 scripts/generate_breakout_stats.py --weeks 2 --limit-symbols 150   # quick check

Why a script and not an endpoint: rebuilding one symbol-date snapshot costs
~16ms, so a 12-week window over ~1,350 symbols is ~20 minutes of CPU. That is
fine once a night and impossible inside a request.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.models.market import (  # noqa: E402
    ConsolidatingScanRequest,
    MomentumBurstScanRequest,
    StockSnapshot,
)
from app.providers.free import FreeMarketDataProvider  # noqa: E402
from app.scanners.definitions import (  # noqa: E402
    run_consolidating_scan,
    run_momentum_burst_scan,
    run_scan,
)
from app.services import breakout_stats as bs  # noqa: E402
from app.services.industry_groups import _build_group_payload  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("breakout-stats")

BENCHMARK_FILE = "_NSEI__3Y.json"
TOP_DECILE_FRACTION = 0.10


def load_universe(data_dir: Path) -> dict[str, dict]:
    rows = json.loads((data_dir / "free_universe.json").read_text(encoding="utf-8"))
    return {str(r["symbol"]).upper(): r for r in rows if r.get("symbol")}


def load_benchmark(data_dir: Path) -> pd.Series:
    payload = json.loads((data_dir / "chart_cache" / BENCHMARK_FILE).read_text(encoding="utf-8"))
    frame = bs._bars_frame(payload)
    if frame is None:
        raise SystemExit(f"benchmark {BENCHMARK_FILE} has too little history to replay against")
    return frame["Close"]


def build_rows_for_date(
    provider: FreeMarketDataProvider,
    frames: dict[str, pd.DataFrame],
    universe: dict[str, dict],
    benchmark: pd.Series,
    as_of: pd.Timestamp,
) -> list[dict]:
    """Reconstruct every symbol's snapshot row as it stood at `as_of`."""
    bench_cut = benchmark[benchmark.index <= as_of]
    rows: list[dict] = []
    for symbol, frame in frames.items():
        cut = frame[frame.index <= as_of]
        if len(cut) < bs.MIN_BARS_FOR_SNAPSHOT:
            continue
        if cut.index[-1] != as_of:
            # The symbol did not trade that session (halt, suspension, or a
            # listing gap). Carrying the prior close forward would invent a
            # signal on a day the stock had no bar, so it is skipped.
            continue
        instrument = universe[symbol]
        try:
            row = provider._history_to_snapshot(instrument, cut, bench_cut)
        except Exception as exc:  # one bad symbol must not kill the session
            logger.debug("snapshot failed for %s @ %s: %s", symbol, as_of.date(), exc)
            continue
        if row:
            rows.append(row)
    return rows


def rank_groups(snapshots: list[StockSnapshot]) -> tuple[set[str], dict[str, str]]:
    """Return (top-decile group ids, symbol -> group id) as at this date.

    Both come from `_build_group_payload` so the membership matches the ranking
    exactly. An earlier version mapped symbols by their universe `sub_sector`
    string, which never matched the slug-style `group_id` ("__parent__transport")
    — the segmentation silently reported zero leading-group signals every week.

    `_build_group_payload` rather than `build_industry_groups_response` because
    the latter persists rank history, and a replay must never write backdated
    ranks into the live store.
    """
    try:
        group_rows, stock_rows, _ = _build_group_payload(snapshots, [], "india")
    except Exception as exc:
        logger.debug("group payload failed: %s", exc)
        return set(), {}
    if not group_rows:
        return set(), {}
    cutoff = max(1, int(len(group_rows) * TOP_DECILE_FRACTION))
    leading = {str(r["group_id"]) for r in group_rows[:cutoff]}
    # IndustryGroupStockItem calls it `final_group_id` — `group_id` here silently
    # yielded "" for every symbol, which is how the segmentation reported zero
    # leading-group signals. Assert rather than getattr-with-default so a future
    # rename fails loudly instead of quietly zeroing the cohort again.
    membership = {
        str(item.symbol).upper(): str(item.final_group_id)
        for item in stock_rows
        if getattr(item, "symbol", None) and getattr(item, "final_group_id", None)
    }
    if stock_rows and not membership:
        raise RuntimeError("group membership empty — IndustryGroupStockItem fields changed?")
    return leading, membership


def collect_signals(
    snapshots: list[StockSnapshot],
    as_of: date,
    universe: dict[str, dict],
    leading_groups: set[str],
    group_of: dict[str, str],
) -> list[bs.Signal]:
    roster = bs.setup_roster()
    signals: list[bs.Signal] = []

    def emit(setup_id: str, matches) -> None:
        for match in matches:
            symbol = str(match.symbol).upper()
            instrument = universe.get(symbol) or {}
            signals.append(
                bs.Signal(
                    setup=setup_id,
                    symbol=symbol,
                    trigger_date=as_of,
                    entry=float(match.last_price or 0.0),
                    rs_rating=int(getattr(match, "rs_rating", 0) or 0),
                    is_ipo=bs.is_recent_ipo(instrument.get("listing_date"), as_of),
                    group_top_decile=group_of.get(symbol, "") in leading_groups,
                )
            )

    for scan in roster:
        try:
            emit(scan.id, run_scan(scan, snapshots))
        except Exception as exc:
            logger.debug("scan %s failed @ %s: %s", scan.id, as_of, exc)

    # The two batch scanners take a request object rather than a bare snapshot,
    # so they sit outside the registry loop. Defaults match the UI's defaults.
    for setup_id, runner, request in (
        ("momentum-burst", run_momentum_burst_scan, MomentumBurstScanRequest()),
        ("consolidating", run_consolidating_scan, ConsolidatingScanRequest()),
    ):
        try:
            emit(setup_id, runner(request, snapshots))
        except Exception as exc:
            logger.debug("scan %s failed @ %s: %s", setup_id, as_of, exc)

    return signals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weeks", type=int, default=12, help="how many ISO weeks back to replay")
    parser.add_argument("--limit-symbols", type=int, default=0, help="cap symbols (testing only)")
    parser.add_argument("--out", default=None, help="output path")
    args = parser.parse_args()

    data_dir = BACKEND_ROOT / "data"
    out_path = Path(args.out) if args.out else data_dir / "breakout_stats.json"

    provider = FreeMarketDataProvider(gemini_api_key=None, eod_only_mode=True)
    universe = load_universe(data_dir)
    benchmark = load_benchmark(data_dir)

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
    logger.info("replaying %d sessions: %s → %s", len(sessions), sessions[0].date(), sessions[-1].date())

    all_signals: list[bs.Signal] = []
    t0 = time.time()
    for n, as_of in enumerate(sessions, start=1):
        rows = build_rows_for_date(provider, frames, universe, benchmark, as_of)
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

        leading, membership = rank_groups(snapshots)
        day_signals = collect_signals(snapshots, as_of.date(), universe, leading, membership)
        all_signals.extend(day_signals)
        logger.info(
            "[%d/%d] %s  snapshots=%d  signals=%d  (%.1f min elapsed)",
            n, len(sessions), as_of.date(), len(snapshots), len(day_signals), (time.time() - t0) / 60,
        )

    logger.info("simulating %d signals…", len(all_signals))

    forwards: dict[tuple[str, date], pd.DataFrame] = {}
    for signal in all_signals:
        key = (signal.symbol, signal.trigger_date)
        if key in forwards:
            continue
        frame = frames.get(signal.symbol)
        if frame is not None:
            forwards[key] = frame[frame.index > pd.Timestamp(signal.trigger_date)]

    def run(horizon: int) -> list[bs.Outcome]:
        out: list[bs.Outcome] = []
        for sig in all_signals:
            fwd = forwards.get((sig.symbol, sig.trigger_date))
            if fwd is None:
                continue
            res = bs.simulate(sig, fwd, horizon=horizon)
            if res:
                out.append(res)
        return out

    outcomes = run(bs.HORIZON_SESSIONS)

    # The newest week has only a few sessions of forward data. Judging it at the
    # full horizon against older weeks that have run their course would flatter
    # the past every single time (see aggregate()'s docstring). So every week is
    # also scored at the horizon the newest week actually has.
    newest = max((s.trigger_date for s in all_signals), default=None)
    common_horizon = bs.HORIZON_SESSIONS
    if newest is not None:
        available = [
            len(forwards[key]) for key in forwards
            if bs.week_key(key[1]) == bs.week_key(newest)
        ]
        if available:
            common_horizon = max(1, min(bs.HORIZON_SESSIONS, max(available)))

    labels = bs.scan_labels() | {
        "momentum-burst": "Momentum Burst",
        "consolidating": "Consolidating",
    }
    payload = bs.aggregate(outcomes, labels)
    if common_horizon < bs.HORIZON_SESSIONS:
        comparable = bs.aggregate(run(common_horizon), labels)
        comparable["horizon_sessions"] = common_horizon
        payload["comparable"] = bs.slim_comparable(comparable)
        logger.info("like-for-like comparison built at a %d-session horizon", common_horizon)
    else:
        payload["comparable"] = None
    payload.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "as_of_session": sessions[-1].date().isoformat(),
            "symbols_replayed": len(frames),
            "sessions_replayed": len(sessions),
            "signals": len(all_signals),
            "outcomes": len(outcomes),
            "rules": bs.rules_payload(),
        }
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("wrote %s (%.1f KB) in %.1f min", out_path, out_path.stat().st_size / 1024, (time.time() - t0) / 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
