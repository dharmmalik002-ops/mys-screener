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

from fastapi import APIRouter, HTTPException, Query

from app.services.bot import policy as pol
from app.services.bot.history import store_summary

logger = logging.getLogger(__name__)

BACKTEST_FILE = "bot_backtest.json"
SIGNALS_FILE = "bot_signals.json"
# Beyond this the committed signal list is describing a market that has moved
# on. Better to say so than to present a stale list as today's.
SIGNAL_STALE_DAYS = 6

# A live scan walks ~1,600 gzipped bar files and rebuilds every indicator, which
# measured at 21 s. The result only changes when a new session closes, so it is
# cached on (session, book size) — without this every tab switch pays the full
# scan and the page feels broken.
_SCAN_CACHE: dict[tuple[str, float], dict[str, Any]] = {}
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


def build_bot_router(data_dir: Path) -> APIRouter:
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
            cache_key = (session, round(equity, 2))
            cached = _SCAN_CACHE.get(cache_key)
            if cached is not None:
                return cached
            try:
                from app.services.bot.live import scan_today

                payload = scan_today(data_dir, artifact, equity=equity)
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
