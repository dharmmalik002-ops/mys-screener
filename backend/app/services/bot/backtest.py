"""Orchestrator: build the regime tape, replay every strategy, attribute, validate.

The order matters. Regimes are built first and independently of any strategy
result, so the classification cannot drift toward whatever makes the strategies
look good. Trades are then stamped with the regime that was in force on their
*entry* day — known that morning — and only then does any aggregation happen.

Output is a single artifact consumed by the API. It holds aggregates, the
regime timeline, and a bounded sample of trades; it deliberately does not hold
every trade, because a few hundred thousand rows would make the file unusable
in a browser and nothing in the UI needs them individually.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

from . import attribution as attr
from . import breadth as bre
from . import earnings as earn
from . import learning as learn
from . import macro as mc
from . import policy as pol
from . import regime as rg
from . import survivorship as sv
from .context_series import BENCHMARK_KEY
from .costs import CostModel, DEFAULT_COSTS
from .engine import ExitModel, Trade, simulate_symbol
from .features import build_features
from .history import Bars, iter_bars, read_bars
from .strategies import STRATEGIES, StrategySpec

logger = logging.getLogger(__name__)

ARTIFACT_VERSION = 4
TRADE_SAMPLE_PER_CELL = 25


@dataclass
class BacktestConfig:
    start: date | None = None
    end: date | None = None
    exits: ExitModel = field(default_factory=ExitModel)
    costs: CostModel = DEFAULT_COSTS
    position_value: float = 100_000.0
    train_fraction: float = 0.6
    strategies: tuple[StrategySpec, ...] = STRATEGIES
    limit_symbols: int = 0

    def describe(self) -> dict:
        return {
            "start": self.start.isoformat() if self.start else None,
            "end": self.end.isoformat() if self.end else None,
            "target_r": self.exits.target_r,
            "max_hold_sessions": self.exits.max_hold_sessions,
            "trail_after_r": self.exits.trail_after_r,
            "trail_atr_mult": self.exits.trail_atr_mult,
            "breakeven_after_r": self.exits.breakeven_after_r,
            "position_value": self.position_value,
            "slippage_bps": self.costs.slippage_bps,
            "train_fraction": self.train_fraction,
        }


@dataclass
class MarketContext:
    """The regime tape plus the raw series it was built from."""

    sessions: list[date]
    regimes: list[rg.RegimeRow]
    breadth: list[bre.BreadthRow]
    regime_by_day: dict[date, rg.RegimeRow]


def build_context(data_dir: Path, symbols: list[str] | None = None) -> MarketContext:
    """Sessions, breadth and regime labels — everything strategy-independent."""
    benchmark = read_bars(data_dir, BENCHMARK_KEY)
    if benchmark is None:
        raise RuntimeError(
            f"no benchmark history for {BENCHMARK_KEY} — run scripts/build_deep_history.py first"
        )
    sessions = list(benchmark.dates)
    logger.info("context: %d sessions %s -> %s", len(sessions), sessions[0], sessions[-1])

    breadth_rows = bre.build_breadth(data_dir, sessions, symbols)
    by_day = {row.day: row for row in breadth_rows}

    n = len(sessions)
    above200 = np.full(n, np.nan)
    net_new = np.full(n, np.nan)
    constituents = np.zeros(n, dtype=np.int64)
    for i, day in enumerate(sessions):
        row = by_day.get(day)
        if row:
            above200[i] = row.pct_above_200dma
            net_new[i] = row.net_new_highs_pct
            constituents[i] = row.constituents

    vix = read_bars(data_dir, "INDIAVIX")
    vix_aligned = np.full(n, np.nan)
    if vix is not None:
        vix_by_day = {d: c for d, c in zip(vix.dates, vix.close)}
        for i, day in enumerate(sessions):
            value = vix_by_day.get(day)
            if value is not None:
                vix_aligned[i] = value

    regime_rows = rg.classify(
        sessions=sessions,
        index_close=benchmark.close,
        index_high=benchmark.high,
        breadth_above_200=above200,
        breadth_net_new_highs=net_new,
        constituents=constituents,
        vix_close=vix_aligned,
        min_constituents=bre.MIN_CONSTITUENTS,
    )
    return MarketContext(
        sessions=sessions,
        regimes=regime_rows,
        breadth=breadth_rows,
        regime_by_day={row.day: row for row in regime_rows},
    )


def run_strategies(
    data_dir: Path,
    context: MarketContext,
    config: BacktestConfig,
    symbols: list[str] | None = None,
) -> list[Trade]:
    """Replay every strategy over every symbol and stamp the regime on each trade."""
    trades: list[Trade] = []
    processed = skipped = 0
    align = _benchmark_aligner(data_dir)
    join_earnings = _earnings_joiner(data_dir)

    for bars in iter_bars(data_dir, symbols):
        features = build_features(bars, align(bars), join_earnings(bars))
        if features is None:
            skipped += 1
            continue
        processed += 1

        in_window = _window_mask(bars, config)
        for spec in config.strategies:
            try:
                signals = spec.generate(features)
            except Exception as exc:  # one bad strategy must not kill the run
                logger.warning("strategy %s failed on %s: %s", spec.id, bars.symbol, exc)
                continue
            signals = signals & in_window
            if not signals.any():
                continue
            for trade in simulate_symbol(
                spec, features, signals, config.exits, config.costs, config.position_value
            ):
                row = context.regime_by_day.get(trade.entry_day)
                if row is None:
                    # No regime label for that session means the benchmark did
                    # not trade it — a data artefact, not a tradeable day.
                    continue
                trade.regime = row.regime
                trade.volatility_band = row.volatility_band
                trades.append(trade)

        if processed % 200 == 0:
            logger.info("replayed %d symbols, %s trades so far", processed, f"{len(trades):,}")

    logger.info("replay done: %d symbols (%d too short), %s trades", processed, skipped, f"{len(trades):,}")
    return trades


def _earnings_joiner(data_dir: Path):
    """A function giving a symbol's (positive, negative) surprise windows.

    Returns None-windows when the store is absent, which makes the earnings
    strategies silently never fire rather than crash — the same degradation the
    relative-strength setups use when there is no benchmark. A missing optional
    data source must cost signals, not the whole run.
    """
    if not earn.store_dir(data_dir).exists():
        logger.warning("no earnings history — the earnings setups will not fire")
        return lambda bars: None

    def join(bars: Bars):
        announcements = earn.read_announcements(data_dir, bars.symbol)
        if not announcements:
            return None
        return earn.surprise_flags(bars.dates, announcements)

    return join


def _benchmark_aligner(data_dir: Path):
    """A function mapping a symbol's dates onto benchmark closes.

    Built once and reused across every symbol: the join is a dict lookup per
    bar, and re-deriving the benchmark series 1,500 times would dominate the
    replay. Dates the benchmark did not trade come back nan, which
    `build_features` turns into "no relative-strength opinion" rather than a
    fabricated ratio.
    """
    benchmark = read_bars(data_dir, BENCHMARK_KEY)
    if benchmark is None:
        logger.warning("no benchmark bars — relative-strength setups will not fire")
        return lambda bars: None

    closes = {day: close for day, close in zip(benchmark.dates, benchmark.close)}

    def align(bars: Bars) -> np.ndarray:
        return np.array([closes.get(day, np.nan) for day in bars.dates], dtype=np.float64)

    return align


def run_strategies_multi(
    data_dir: Path,
    context: MarketContext,
    config: BacktestConfig,
    exit_models: dict[str, ExitModel],
    symbols: list[str] | None = None,
) -> dict[str, list[Trade]]:
    """Replay once, settle the same signals under several exit rules.

    Signal generation costs ~40x what settling a trade costs — the indicator
    bundle dominates — so comparing five exit models by running the whole
    backtest five times is four runs of pure waste. Every model here sees
    exactly the same signals, which also makes the comparison clean: any
    difference between them is the exit rule and nothing else.
    """
    out: dict[str, list[Trade]] = {name: [] for name in exit_models}
    processed = 0
    align = _benchmark_aligner(data_dir)
    join_earnings = _earnings_joiner(data_dir)

    for bars in iter_bars(data_dir, symbols):
        features = build_features(bars, align(bars), join_earnings(bars))
        if features is None:
            continue
        processed += 1
        in_window = _window_mask(bars, config)

        for spec in config.strategies:
            try:
                signals = spec.generate(features) & in_window
            except Exception as exc:
                logger.warning("strategy %s failed on %s: %s", spec.id, bars.symbol, exc)
                continue
            if not signals.any():
                continue
            for name, exits in exit_models.items():
                for trade in simulate_symbol(
                    spec, features, signals, exits, config.costs, config.position_value
                ):
                    row = context.regime_by_day.get(trade.entry_day)
                    if row is None:
                        continue
                    trade.regime = row.regime
                    trade.volatility_band = row.volatility_band
                    out[name].append(trade)

        if processed % 200 == 0:
            logger.info("sweep: %d symbols replayed", processed)

    return out


def _window_mask(bars: Bars, config: BacktestConfig) -> np.ndarray:
    mask = np.ones(len(bars), dtype=bool)
    if config.start is not None:
        mask &= np.array([d >= config.start for d in bars.dates])
    if config.end is not None:
        mask &= np.array([d <= config.end for d in bars.dates])
    return mask


def _regime_timeline(rows: list[rg.RegimeRow]) -> list[dict]:
    """Contiguous regime spans rather than 4,600 daily rows.

    The UI draws a timeline ribbon; spans are what it needs and they are ~200
    objects instead of ~4,600, which keeps the artifact small enough to serve.
    """
    spans: list[dict] = []
    for row in rows:
        if spans and spans[-1]["regime"] == row.regime:
            spans[-1]["end"] = row.day.isoformat()
            spans[-1]["sessions"] += 1
            spans[-1]["_closes"].append(row.index_close)
            continue
        spans.append(
            {
                "regime": row.regime,
                "start": row.day.isoformat(),
                "end": row.day.isoformat(),
                "sessions": 1,
                "_closes": [row.index_close],
            }
        )
    for span in spans:
        closes = span.pop("_closes")
        first, last = closes[0], closes[-1]
        span["index_return_pct"] = round((last - first) / first * 100.0, 2) if first else 0.0
        span["label"] = rg.REGIME_LABELS.get(span["regime"], span["regime"])
    return spans


def _regime_distribution(rows: list[rg.RegimeRow]) -> list[dict]:
    total = len(rows) or 1
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.regime] = counts.get(row.regime, 0) + 1
    return sorted(
        (
            {
                "regime": name,
                "label": rg.REGIME_LABELS.get(name, name),
                "sessions": count,
                "pct_of_history": round(100.0 * count / total, 1),
                "note": rg.REGIME_NOTES.get(name, ""),
            }
            for name, count in counts.items()
        ),
        key=lambda r: -r["sessions"],
    )


def _trade_samples(trades: list[Trade]) -> list[dict]:
    """A bounded, deterministic sample per cell for the UI's drill-down."""
    buckets: dict[tuple[str, str], list[Trade]] = {}
    for t in trades:
        buckets.setdefault((t.strategy, t.regime), []).append(t)
    out: list[dict] = []
    for key, group in buckets.items():
        group.sort(key=lambda t: t.entry_day)
        step = max(1, len(group) // TRADE_SAMPLE_PER_CELL)
        out.extend(t.to_dict() for t in group[::step][:TRADE_SAMPLE_PER_CELL])
    return out


def build_artifact(
    data_dir: Path,
    config: BacktestConfig | None = None,
    symbols: list[str] | None = None,
) -> dict:
    """Run everything and return the artifact the API serves."""
    config = config or BacktestConfig()
    context = build_context(data_dir, symbols)
    trades = run_strategies(data_dir, context, config, symbols)

    resolved = [t for t in trades if t.resolved]
    matrix = attr.build_matrix(resolved, rg.REGIMES)
    validated = attr.validate(resolved, rg.REGIMES, config.train_fraction)
    totals = attr.strategy_totals(resolved)
    boundary = attr.split_date(resolved, config.train_fraction)

    playbooks = pol.build_playbooks(validated)
    survivorship = sv.analyse(data_dir, resolved)
    macro_findings = mc.measure_macro_edge(data_dir, resolved, context.sessions)
    # Built from the same resolved trades and the same split date as the
    # attribution above, so the lessons and the matrix can never describe
    # different populations or disagree about what "held out" means.
    learning = learn.build_learning(
        resolved, context.regimes, boundary, data_dir,
        playbooks=[p.to_dict() for p in playbooks], matrix=[c.to_dict() for c in matrix],
    )

    return {
        "artifact_version": ARTIFACT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": config.describe(),
        "coverage": {
            "sessions": len(context.sessions),
            "first_session": context.sessions[0].isoformat() if context.sessions else None,
            "last_session": context.sessions[-1].isoformat() if context.sessions else None,
            "symbols_with_trades": len({t.symbol for t in trades}),
            "trades_total": len(trades),
            "trades_resolved": len(resolved),
            "validation_split": boundary.isoformat() if boundary else None,
        },
        "regime_distribution": _regime_distribution(context.regimes),
        "regime_timeline": _regime_timeline(context.regimes),
        "current_regime": context.regimes[-1].to_dict() if context.regimes else None,
        "strategy_totals": [c.to_dict() for c in totals],
        "matrix": [c.to_dict() for c in matrix],
        "validated": [v.to_dict() for v in validated],
        "playbooks": [p.to_dict() for p in playbooks],
        "survivorship": survivorship,
        "learning": learning,
        "macro": [f.to_dict() for f in macro_findings],
        "strategy_catalogue": [
            {
                "id": s.id,
                "label": s.label,
                "family": s.family,
                "thesis": s.thesis,
                "expects": list(s.expects),
                "stop_atr_mult": s.stop_atr_mult,
            }
            for s in config.strategies
        ],
        "regime_catalogue": [
            {"id": r, "label": rg.REGIME_LABELS[r], "note": rg.REGIME_NOTES[r]} for r in rg.REGIMES
        ],
        "trade_samples": _trade_samples(resolved),
    }
