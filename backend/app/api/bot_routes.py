"""HTTP surface for the regime-aware trading bot. Mounted under `/api/bot`.

Every handler is a **sync** `def`, for the reason spelled out at the top of
`mutual_funds_routes.py`: the work behind these endpoints is blocking file I/O
(a 900 KB artifact, and in a local run a walk over ~1,500 gzipped bar files).
On the event loop one live scan would stall every other request in the worker.

The live/offline split matters here. `deep_history/` is ~100 MB and gitignored,
so it exists on a workstation and never on the Space. Rather than have the Bot
tab silently render empty in production — the failure mode gotchas 14 and 15 in
CLAUDE.md both describe — `/signals` prefers a live scan and falls back to the
committed artifact, and says in the response which one the user is looking at.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services.bot import calibration as cal
from app.services.bot import ledger as lg
from app.services.bot import policy as pol
from app.services.bot import review as rv
from app.services.bot.history import store_summary

logger = logging.getLogger(__name__)

BACKTEST_FILE = "bot_backtest.json"
SIGNALS_FILE = "bot_signals.json"
ROLLING_FILE = "bot_rolling_walkforward.json"
# Beyond this the committed signal list is describing a market that has moved
# on. Better to say so than to present a stale list as today's.
SIGNAL_STALE_DAYS = 6

# A live scan walks ~1,600 gzipped bar files and rebuilds every indicator, which
# measured at 21 s. The result only changes when a new session closes, so it is
# cached — without this every tab switch pays the full scan and the page feels
# broken.
#
# The key includes the live-trade count as well as (session, book size),
# because calibration can suspend a cell and that changes the candidate list.
# Keyed on session alone, recording a trade left the old list served from cache
# and the loop looked broken from the outside.
_SCAN_CACHE: dict[tuple[str, float, int], dict[str, Any]] = {}
_SCAN_CACHE_MAX = 8


def _load(data_dir: Path, name: str) -> dict[str, Any] | None:
    path = data_dir / name
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("bot: unreadable %s: %s", name, exc)
        return None


def _age_days(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        return (date.today() - date.fromisoformat(iso[:10])).days
    except ValueError:
        return None


def build_bot_router(data_dir: Path, state_dir: Path | None = None) -> APIRouter:
    """`state_dir` is where the SQLite ledger lives (APP_STATE_DIR).

    Optional because the ledger is a local research convenience: the Space has
    no way to build one (no bar store, and a .db could not be committed even if
    it did — gotcha 21), so every ledger endpoint degrades to a clear 503 there
    rather than 500ing. The learning aggregates the UI actually needs travel in
    the committed artifact and are served whether or not a ledger exists.
    """
    router = APIRouter(prefix="/api/bot", tags=["bot"])

    def _require_backtest() -> dict[str, Any]:
        artifact = _load(data_dir, BACKTEST_FILE)
        if not artifact:
            # 503 rather than 404: the endpoint exists, the study behind it has
            # not been built. A 404 reads as "no such route" to the client.
            raise HTTPException(
                status_code=503,
                detail="The backtest has not been built yet. Run scripts/run_bot_backtest.py.",
            )
        return artifact

    @router.get("/status")
    def status() -> dict[str, Any]:
        """What the bot knows and how fresh it is — the data-health panel."""
        artifact = _load(data_dir, BACKTEST_FILE)
        signals = _load(data_dir, SIGNALS_FILE)
        store = store_summary(data_dir)
        return {
            "backtest_present": artifact is not None,
            "backtest_generated_at": (artifact or {}).get("generated_at"),
            "coverage": (artifact or {}).get("coverage"),
            "signals_present": signals is not None,
            "signals_as_of": (signals or {}).get("as_of"),
            "signals_age_days": _age_days((signals or {}).get("as_of")),
            "history_store": store,
            "live_scan_available": store.get("present", False),
            "ledger_present": bool(state_dir and lg.ledger_path(state_dir).exists()),
            "walkforward_present": (data_dir / ROLLING_FILE).exists(),
            "learning_present": bool((artifact or {}).get("learning", {}).get("available")),
        }

    @router.get("/backtest")
    def backtest(
        section: str | None = Query(
            None,
            description="Return one section only (matrix, validated, playbooks, macro, survivorship, …)",
        ),
    ) -> dict[str, Any]:
        """The full study. ~900 KB, so the UI asks for sections where it can."""
        artifact = _require_backtest()
        if section:
            if section not in artifact:
                raise HTTPException(status_code=404, detail=f"no such section: {section}")
            return {"section": section, "data": artifact[section], "generated_at": artifact.get("generated_at")}
        return artifact

    @router.get("/playbook")
    def playbook(regime: str | None = Query(None)) -> dict[str, Any]:
        """The regime -> strategy map, optionally for one regime."""
        artifact = _require_backtest()
        books = artifact.get("playbooks") or []
        if regime:
            match = next((b for b in books if b.get("regime") == regime), None)
            if not match:
                raise HTTPException(status_code=404, detail=f"no playbook for regime: {regime}")
            return match
        return {
            "current_regime": artifact.get("current_regime"),
            "playbooks": books,
            "regime_catalogue": artifact.get("regime_catalogue"),
        }

    @router.get("/signals")
    def signals(
        equity: float = Query(1_000_000.0, gt=0, description="book size, for the sizing column"),
        force_offline: bool = Query(False, description="skip the live scan even when it is available"),
    ) -> dict[str, Any]:
        """Today's candidates. Live when the history store is here, else committed."""
        artifact = _require_backtest()
        store = store_summary(data_dir)

        if store.get("present") and not force_offline:
            session = str((artifact.get("current_regime") or {}).get("day") or "")
            live_count = 0
            if state_dir and lg.ledger_path(state_dir).exists():
                with lg.connect(state_dir) as conn:
                    by_source = lg.counts(conn)["by_source"]
                    live_count = int(by_source.get("live", 0)) + int(by_source.get("paper", 0))
            cache_key = (session, round(equity, 2), live_count)
            cached = _SCAN_CACHE.get(cache_key)
            if cached is not None:
                return cached
            try:
                from app.services.bot.live import scan_today

                live_record: list[dict[str, Any]] = []
                if state_dir and lg.ledger_path(state_dir).exists():
                    with lg.connect(state_dir) as conn:
                        for source in ("live", "paper"):
                            live_record.extend(lg.query_trades(conn, source=source, limit=5000))
                audit = cal.calibrate(live_record, artifact.get("playbooks") or [])
                payload = scan_today(data_dir, artifact, equity=equity, calibration=audit)
                payload["source"] = "live"
                payload["generated_at"] = datetime.now(timezone.utc).isoformat()
                if session:
                    if len(_SCAN_CACHE) >= _SCAN_CACHE_MAX:
                        _SCAN_CACHE.clear()
                    _SCAN_CACHE[cache_key] = payload
                return payload
            except Exception as exc:  # fall through to the committed copy
                logger.warning("bot: live scan failed, serving committed signals: %s", exc)

        committed = _load(data_dir, SIGNALS_FILE)
        if not committed:
            raise HTTPException(
                status_code=503,
                detail="No signals available. Run scripts/generate_bot_signals.py where the history store lives.",
            )
        age = _age_days(committed.get("as_of"))
        committed["source"] = "offline"
        committed["age_days"] = age
        committed["stale"] = age is not None and age > SIGNAL_STALE_DAYS
        if committed["stale"]:
            # Surfaced rather than hidden: a stale list looks exactly like a
            # fresh one in the UI, and acting on it is a real loss.
            committed["message"] = (
                f"These signals are {age} days old and are shown for reference only. "
                "Rebuild them before trading."
            ) + (" " + str(committed.get("message") or ""))
        return committed

    @router.get("/walkforward")
    def walkforward() -> dict[str, Any]:
        """The rolling evaluation — the number that actually matters.

        Every other figure this API serves rests on one train/test split, which
        gives a 3.8-year test window. That window happens to contain 2023, the
        single year the system worked, and it dominates the result. This
        endpoint serves the rolling version: the playbook rebuilt each January
        from prior data only, then used to trade that year, eleven times over.
        It is the honest measure of whether the edge persists, and it says it
        does not.
        """
        payload = _load(data_dir, ROLLING_FILE)
        if not payload:
            raise HTTPException(
                status_code=503,
                detail="No rolling evaluation yet. Run scripts/rolling_walkforward.py.",
            )
        return payload

    @router.get("/learning")
    def learning() -> dict[str, Any]:
        """Trade verdicts, entry-condition studies and the evolution timeline."""
        artifact = _require_backtest()
        payload = artifact.get("learning")
        if not payload or not payload.get("available"):
            raise HTTPException(
                status_code=503,
                detail="This backtest predates the learning layer. Rerun scripts/run_bot_backtest.py.",
            )
        return payload

    def _require_ledger() -> Path:
        if state_dir is None or not lg.ledger_path(state_dir).exists():
            raise HTTPException(
                status_code=503,
                detail=(
                    "No trade ledger on this host. Build one where the bar store lives with "
                    "scripts/build_bot_ledger.py. The Learning tab works without it."
                ),
            )
        return state_dir

    @router.get("/ledger/status")
    def ledger_status() -> dict[str, Any]:
        directory = _require_ledger()
        with lg.connect(directory) as conn:
            summary = lg.counts(conn)
            summary["cell_status"] = lg.latest_cell_status(conn)
        summary["path"] = str(lg.ledger_path(directory))
        return summary

    @router.get("/ledger/trades")
    def ledger_trades(
        source: str | None = Query(None, description="backtest | paper | live"),
        strategy: str | None = Query(None),
        regime: str | None = Query(None),
        verdict: str | None = Query(None),
        since: str | None = Query(None, description="ISO date"),
        limit: int = Query(100, ge=1, le=500),
    ) -> dict[str, Any]:
        """Individual trades with their review — the drill-down the aggregates cannot answer."""
        directory = _require_ledger()
        with lg.connect(directory) as conn:
            rows = lg.query_trades(
                conn, source=source, strategy=strategy, regime=regime,
                verdict=verdict, since=since, limit=limit,
            )
        return {"trades": rows, "count": len(rows)}

    @router.get("/ledger/transitions")
    def ledger_transitions() -> dict[str, Any]:
        """Every recorded point where a cell's status changed on this host."""
        directory = _require_ledger()
        with lg.connect(directory) as conn:
            return {"transitions": lg.status_transitions(conn)}

    @router.post("/ledger/trade")
    def record_live_trade(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        """Record a closed paper or live trade, and review it immediately.

        This is the loop's inlet. A trade recorded here joins the same table as
        the 96,401 simulated ones, under the same schema, so every existing
        query works across it — and `calibration` can then ask whether the live
        record still matches what the study predicted.

        `r_multiple` is required rather than derived: the caller knows what it
        actually paid and what its stop actually was, and recomputing it here
        from a nominal entry and stop would quietly discard the slippage that
        makes live results differ from simulated ones in the first place.
        """
        directory = _require_ledger()
        required = ("strategy", "symbol", "entry_day", "exit_day", "r_multiple", "regime")
        missing = [field for field in required if payload.get(field) in (None, "")]
        if missing:
            raise HTTPException(status_code=400, detail=f"missing required fields: {', '.join(missing)}")

        source = str(payload.get("source") or "live")
        if source not in {"live", "paper"}:
            # `backtest` is written by the replay only. Letting a caller insert
            # into it would contaminate the statistical base with hand-entered
            # rows that no study protocol ever saw.
            raise HTTPException(status_code=400, detail="source must be 'live' or 'paper'")

        def number(field: str, default: float = 0.0) -> float:
            try:
                return float(payload.get(field, default))
            except (TypeError, ValueError):
                return default

        trade = lg.LedgerTrade(
            source=source,
            strategy=str(payload["strategy"]),
            symbol=str(payload["symbol"]).upper(),
            signal_day=str(payload.get("signal_day") or payload["entry_day"]),
            entry_day=str(payload["entry_day"]),
            exit_day=str(payload["exit_day"]),
            entry=number("entry"),
            stop=number("stop"),
            exit_price=number("exit_price") or None,
            exit_reason=str(payload.get("exit_reason") or "manual"),
            sessions_held=int(number("sessions_held")),
            r_multiple=number("r_multiple"),
            net_pct=number("net_pct"),
            mae_r=number("mae_r"),
            mfe_r=number("mfe_r"),
            risk_pct=number("risk_pct"),
            atr_pct_at_entry=number("atr_pct_at_entry"),
            regime=str(payload["regime"]),
            volatility_band=str(payload.get("volatility_band") or "normal"),
            breadth_above_200dma=payload.get("breadth_above_200dma"),
            pct_from_52w_high=payload.get("pct_from_52w_high"),
            vix_percentile=payload.get("vix_percentile"),
            macro_headwinds=payload.get("macro_headwinds"),
            expected_r=payload.get("expected_r"),
            thesis=str(payload.get("thesis") or ""),
        )

        with lg.connect(directory) as conn:
            lg.record_trades(conn, [trade])
            pending = lg.unreviewed(conn)
            reviewed = None
            for row in pending:
                verdict = rv.review_trade(row)
                lg.record_review(
                    conn, verdict.trade_id, verdict.verdict, verdict.tags,
                    verdict.stop_quality, verdict.exit_quality,
                    verdict.r_left_on_table, verdict.note,
                )
                if (
                    row["symbol"] == trade.symbol
                    and row["entry_day"] == trade.entry_day
                    and row["strategy"] == trade.strategy
                ):
                    reviewed = verdict.to_dict()
            counts = lg.counts(conn)

        return {"recorded": True, "review": reviewed, "ledger": counts}

    @router.get("/calibration")
    def calibration() -> dict[str, Any]:
        """Whether the live record still matches what the study predicted."""
        artifact = _require_backtest()
        directory = state_dir
        live: list[dict[str, Any]] = []
        if directory and lg.ledger_path(directory).exists():
            with lg.connect(directory) as conn:
                for source in ("live", "paper"):
                    live.extend(lg.query_trades(conn, source=source, limit=5000))
        return cal.calibrate(live, artifact.get("playbooks") or [])

    @router.get("/size")
    def size(
        equity: float = Query(..., gt=0),
        entry: float = Query(..., gt=0),
        stop: float = Query(..., gt=0),
        risk_pct: float = Query(0.75, gt=0, le=5.0),
    ) -> dict[str, Any]:
        """Position sizing, with the arithmetic shown."""
        if stop >= entry:
            raise HTTPException(status_code=400, detail="stop must be below entry for a long position")
        result = pol.position_size(equity, entry, stop, risk_pct)
        result["risk_pct"] = risk_pct
        return result

    return router
