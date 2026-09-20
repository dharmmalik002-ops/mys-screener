"""Bridge: turn the raw trade population into the learning the UI reads.

Two consumers, one source, and the split is forced by a deployment constraint
worth stating plainly. The Space's pre-receive hook rejects binary files
outright (CLAUDE.md gotcha 21), so the SQLite ledger can never be committed —
it lives in `APP_STATE_DIR` on whatever machine is running, holds individual
trades, and is where live results accumulate. What ships in git is this
module's output: the aggregates, the lessons and the evolution timeline, as
JSON inside `bot_backtest.json`.

So the ledger is the memory and this is the summary of it. Both are built from
the same trades and neither invents anything the other cannot show.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Mapping, Sequence

from . import benchmark as bm
from . import conditions as cond
from . import evolution as evo
from . import portfolio as pf
from . import review as rv
from . import sensitivity as sens
from .engine import Trade
from .regime import REGIME_LABELS
from .strategies import BY_ID

logger = logging.getLogger(__name__)

# The evolution replay is quarterly; more often than that and consecutive
# snapshots share almost every trade, so the "changes" list fills with the same
# cell flickering across a threshold rather than with real reversals.
EVOLUTION_CADENCE_DAYS = 91
MAX_CHANGES_REPORTED = 120


def trade_to_row(trade: Trade, index: int, context_by_day: Mapping[date, Mapping]) -> dict:
    """One trade as a ledger-shaped mapping, with the entry context attached.

    The context is looked up by *entry* day, never exit day: the question the
    record has to answer later is "what was true when the bot committed", and
    the exit-day state is information it did not have.
    """
    context = context_by_day.get(trade.entry_day) or {}
    spec = BY_ID.get(trade.strategy)
    return {
        "id": index,
        "source": "backtest",
        "strategy": trade.strategy,
        "symbol": trade.symbol,
        "signal_day": trade.signal_day.isoformat(),
        "entry_day": trade.entry_day.isoformat(),
        "exit_day": trade.exit_day.isoformat() if trade.exit_day else None,
        "entry": trade.entry,
        "stop": trade.stop,
        "exit_price": trade.exit_price,
        "exit_reason": trade.exit_reason,
        "sessions_held": trade.sessions_held,
        "r_multiple": trade.r_multiple,
        "net_pct": trade.net_pct,
        "mae_r": trade.mae_r,
        "mfe_r": trade.mfe_r,
        "risk_pct": trade.risk_pct,
        "atr_pct_at_entry": trade.atr_pct_at_entry,
        "ret_63_at_entry": trade.ret_63_at_entry,
        "ret_252_at_entry": trade.ret_252_at_entry,
        "dist_52w_high_at_entry": trade.dist_52w_high_at_entry,
        "rel_volume_at_entry": trade.rel_volume_at_entry,
        "turnover_crore_at_entry": trade.turnover_crore_at_entry,
        "above_200dma_pct_at_entry": trade.above_200dma_pct_at_entry,
        "regime": trade.regime,
        "volatility_band": trade.volatility_band,
        "breadth_above_200dma": context.get("breadth_above_200dma"),
        "pct_from_52w_high": context.get("pct_from_52w_high"),
        "vix_percentile": context.get("vix_percentile"),
        "macro_headwinds": context.get("macro_headwinds"),
        "expected_r": None,
        "thesis": spec.thesis if spec else "",
    }


def build_rows(trades: Sequence[Trade], regime_rows: Sequence) -> list[dict]:
    """Ledger-shaped rows for every resolved trade."""
    context_by_day = {
        row.day: {
            "breadth_above_200dma": row.breadth_above_200dma,
            "pct_from_52w_high": row.pct_from_52w_high,
            "vix_percentile": row.vix_percentile,
            # Macro headwind count is not stored per session in the regime
            # table; it is left None rather than back-filled from today's
            # reading, which would be a fabricated entry-time fact.
            "macro_headwinds": None,
        }
        for row in regime_rows
    }
    return [
        trade_to_row(trade, index, context_by_day)
        for index, trade in enumerate(trades)
        if trade.resolved
    ]


def review_population(rows: Sequence[Mapping]) -> tuple[list[dict], dict]:
    """Review every trade, then reduce the reviews to lessons."""
    reviewed: list[dict] = []
    for row in rows:
        verdict = rv.review_trade(row)
        reviewed.append({**dict(row), **verdict.to_dict()})
    return reviewed, rv.summarise_reviews(reviewed)


def review_by_regime(reviewed: Sequence[Mapping]) -> list[dict]:
    """The same reduction, held separately per regime.

    This is the answer to "what kind of trade works in what condition" stated
    in process terms rather than P&L terms — the round-trip rate in a choppy
    tape is a different fact from the average R, and more actionable.
    """
    grouped: dict[str, list[Mapping]] = {}
    for row in reviewed:
        grouped.setdefault(str(row.get("regime") or ""), []).append(row)

    out: list[dict] = []
    for regime, rows in grouped.items():
        if not regime:
            continue
        summary = rv.summarise_reviews(rows)
        if summary["trades"] < rv.MIN_PATTERN_SAMPLE:
            continue
        out.append(
            {
                "regime": regime,
                "label": REGIME_LABELS.get(regime, regime),
                **summary,
            }
        )
    return sorted(out, key=lambda r: -r["trades"])


def build_portfolio_runs(
    rows: Sequence[Mapping],
    snapshots: Sequence[Mapping],
    validation_split: date | None,
    playbooks: Sequence[Mapping] | None = None,
    matrix: Sequence[Mapping] | None = None,
) -> list[pf.PortfolioResult]:
    """The account, run three ways, with the honest one named as such.

    `playbook_held_out` is the number that means something: the cells were
    chosen by a walk-forward split on data before `validation_split`, then held
    fixed while the account trades everything after it. Nothing in that window
    was used to pick a strategy, a threshold or an exit rule. This is also what
    the live scan does, so it is the only run that describes the actual system.

    `reactive_held_out` re-decides eligibility every quarter from recent
    performance. It is reported because it loses money and the reason is worth
    knowing — see the module docstring — not because it is an alternative.

    `no_gating` trades every cell, as the upper bound on what the raw trade
    population is worth before any selection at all.
    """
    runs: list[pf.PortfolioResult] = []

    cells: set[tuple[str, str]] = set()
    expectancy: dict[tuple[str, str], float] = {}
    for book in playbooks or []:
        for entry in book.get("entries") or []:
            key = (str(entry["strategy"]), str(book["regime"]))
            cells.add(key)
            expectancy[key] = float(entry.get("out_sample_r") or 0.0)

    if cells and validation_split:
        # Derived from pre-split trades only, so the held-out window's own
        # results never reach the ranking that selects inside it.
        adjustment = pf.derive_atr_adjustment(rows, validation_split)
        playbook_run = pf.simulate(
            rows, snapshots, start=validation_split, label="playbook_held_out",
            playbook_cells=cells, cell_expectancy=expectancy,
            atr_adjustment=adjustment,
        )
        if playbook_run:
            runs.append(playbook_run)
        # The same account without the volatility adjustment, as the control
        # that shows what the adjustment is actually worth.
        plain = pf.simulate(
            rows, snapshots, start=validation_split, label="playbook_no_vol_adjust",
            playbook_cells=cells, cell_expectancy=expectancy,
        )
        if plain:
            runs.append(plain)

    if validation_split:
        reactive = pf.simulate(rows, snapshots, start=validation_split, label="reactive_held_out")
        if reactive:
            runs.append(reactive)

    ungated = pf.simulate(rows, snapshots, label="no_gating", point_in_time=False)
    if ungated:
        runs.append(ungated)
    return runs


def build_learning(
    trades: Sequence[Trade],
    regime_rows: Sequence,
    validation_split: date | None = None,
    data_dir: Path | None = None,
    playbooks: Sequence[Mapping] | None = None,
    matrix: Sequence[Mapping] | None = None,
) -> dict:
    """Everything the Learning views read, computed from the trade population."""
    rows = build_rows(trades, regime_rows)
    if not rows:
        return {"available": False, "reason": "no resolved trades"}

    reviewed, summary = review_population(rows)
    per_regime = review_by_regime(reviewed)

    # Condition studies share the attribution's split date so "held out" means
    # the same period everywhere in the artifact.
    split = validation_split or date.fromisoformat(
        sorted(r["entry_day"] for r in rows)[int(len(rows) * 0.6)]
    )
    condition_studies = cond.study_all(rows, split)

    snapshots = evo.replay_evolution(rows, cadence_days=EVOLUTION_CADENCE_DAYS)
    changes = evo.summarise_changes(snapshots)
    latest = snapshots[-1] if snapshots else None

    # The full per-snapshot cell list is ~60 cells x ~60 quarters; the timeline
    # only needs the counts, and the latest snapshot carries the detail.
    timeline = [
        {"as_of": s["as_of"], "counts": s["counts"], "tradeable": s["tradeable"]}
        for s in snapshots
    ]

    runs = build_portfolio_runs(rows, snapshots, validation_split, playbooks, matrix)
    cells = {
        (str(entry["strategy"]), str(book["regime"]))
        for book in (playbooks or [])
        for entry in (book.get("entries") or [])
    }
    expectancy_map = {
        (str(entry["strategy"]), str(book["regime"])): float(entry.get("out_sample_r") or 0.0)
        for book in (playbooks or [])
        for entry in (book.get("entries") or [])
    }
    # The eligible pool for the uncertainty estimate is every held-out trade in
    # a playbook cell — what the account could have taken, against what it did.
    eligible_r: list[float] = []
    if validation_split and cells:
        eligible_r = [
            float(r["r_multiple"]) for r in rows
            if r.get("entry_day")
            and date.fromisoformat(str(r["entry_day"])) >= validation_split
            and (str(r["strategy"]), str(r["regime"])) in cells
        ]
    # The risk figure must be the one the run actually used. Passing the old
    # default while the book runs at 0.10% produced a "90% range" of +86% to
    # +276% a year — a number so wrong it was obvious, which is the only reason
    # it was caught. Read it off the config instead of restating it.
    # Across every defensible book structure, not the one that happened to be
    # chosen — see sensitivity.py on why the choice cannot be made reliably.
    config_sensitivity = None
    if validation_split and cells:
        index_cagr = None
        fund_median = None
        if data_dir:
            probe = bm.compare(runs[0], data_dir) if runs else None
            if probe:
                index_cagr, fund_median = probe.index_cagr_pct, probe.fund_median_cagr
        index_series = None
        last_session = None
        if data_dir:
            from .benchmark import INDEX_KEY
            from .history import read_bars

            # The same broad index the benchmark compares against, so the
            # period table and the scorecard cannot disagree about what "the
            # market" did.
            benchmark = read_bars(data_dir, INDEX_KEY)
            if benchmark is not None:
                index_series = {d: float(c) for d, c in zip(benchmark.dates, benchmark.close)}
                last_session = benchmark.last_date
        config_sensitivity = sens.measure(
            rows, cells, expectancy_map, validation_split, index_cagr, fund_median,
            index_series=index_series, end=last_session,
        )

    benchmark = (
        bm.build_benchmark(
            runs, data_dir, eligible_r,
            risk_per_trade_pct=pf.PortfolioConfig().risk_per_trade_pct,
        )
        if data_dir and runs else None
    )

    return {
        "available": True,
        "population": {
            "trades": len(rows),
            "first_entry": min(r["entry_day"] for r in rows),
            "last_entry": max(r["entry_day"] for r in rows),
        },
        "review_summary": summary,
        "review_by_regime": per_regime,
        "portfolio_runs": [r.to_dict() for r in runs],
        "config_sensitivity": config_sensitivity.to_dict() if config_sensitivity else None,
        "benchmark": benchmark,
        "condition_studies": [c.to_dict() for c in condition_studies],
        "condition_split": split.isoformat(),
        "evolution_timeline": timeline,
        "evolution_changes": changes[:MAX_CHANGES_REPORTED],
        "evolution_changes_total": len(changes),
        "cell_status": latest["cells"] if latest else [],
        "cell_status_as_of": latest["as_of"] if latest else None,
        "status_catalogue": [
            {"id": s, "label": evo.STATUS_LABELS[s], "note": evo.STATUS_NOTES[s]}
            for s in evo.STATUSES
        ],
        "verdict_catalogue": [
            {"id": v, "label": rv.VERDICT_LABELS[v], "note": rv.VERDICT_NOTES[v]}
            for v in rv.VERDICTS
        ],
        "method_note": (
            "Cell standing is re-scored quarterly using only trades that had closed by that "
            "date, so the timeline shows what the system actually believed at each point — "
            "including where it was wrong and later stood a strategy down. Nothing is refitted: "
            "a cell that stops paying is retired, not repaired."
        ),
    }
