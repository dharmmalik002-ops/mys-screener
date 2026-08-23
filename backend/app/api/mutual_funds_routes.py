"""HTTP surface for the mutual-fund screener.

Mounted under `/api/mf`. Kept out of `routes.py` because that module is
already the equity router's 700-line home and none of this shares its
market-scoping (`/api/{market}/...`) — mutual funds are India-only by nature.

Every handler here is a **sync** `def`, not `async def`, and that is
deliberate. `MutualFundService` is fully synchronous and does blocking I/O:
reading NAV files off disk, fetching a fund's holdings, pulling index history
through yfinance. An `async def` handler would run all of that on the event
loop, so one slow benchmark fetch would stall every other request in the
process. A sync handler gets dispatched to FastAPI's threadpool, where
blocking is confined to one worker.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from app.services.mutual_funds.service import MutualFundService


def _require_ready(service: MutualFundService) -> None:
    """503, not 404, when the universe has not loaded.

    Without this, a missing or unreadable `mf_universe.json` makes every fund
    lookup a 404 — indistinguishable from "no such scheme code", so the client
    reports a bad fund rather than a backend that is not up yet.
    """
    if not service.get_status().get("ready"):
        raise HTTPException(
            status_code=503,
            detail="The mutual fund universe is not loaded yet. Retry shortly.",
        )


def _split(value: str | None) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def build_mutual_funds_router(service: MutualFundService) -> APIRouter:
    router = APIRouter(prefix="/api/mf", tags=["mutual-funds"])

    @router.get("/status")
    def status():
        """Whether the universe is built, and how fresh it is."""
        return service.get_status()

    @router.get("/screener")
    def screener(
        category: str | None = Query(default=None, description="Equity | Hybrid"),
        sub_categories: str | None = Query(default=None, description="comma-separated SEBI sub-categories"),
        amcs: str | None = Query(default=None, description="comma-separated fund houses"),
        search: str | None = Query(default=None),
        min_aum: float | None = Query(default=None, description="crore"),
        max_expense: float | None = Query(default=None, description="percent"),
        min_age_years: float | None = Query(default=None),
        max_quartile: int | None = Query(default=None, ge=1, le=4),
        codes: str | None = Query(default=None, description="restrict to these scheme codes"),
        sort_by: str = Query(default="return_3y"),
        sort_dir: str = Query(default="desc"),
        limit: int = Query(default=250, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ):
        return service.get_screener(
            category=category,
            sub_categories=_split(sub_categories),
            amcs=_split(amcs),
            search=search,
            min_aum=min_aum,
            max_expense=max_expense,
            min_age_years=min_age_years,
            max_quartile=max_quartile,
            only_codes=_split(codes),
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )

    @router.get("/categories")
    def categories():
        """Category leaderboard — averages plus the top funds in each."""
        return service.get_category_leaderboard()

    @router.get("/fund/{scheme_code}")
    def fund(scheme_code: str):
        _require_ready(service)
        payload = service.get_fund(scheme_code)
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Unknown scheme code: {scheme_code}")
        return payload

    @router.get("/fund/{scheme_code}/series")
    def fund_series(
        scheme_code: str,
        range: str = Query(default="3y", alias="range"),
        benchmark: str | None = Query(default=None),
        compare: str | None = Query(default=None, description="comma-separated scheme codes to overlay"),
        drawdown: bool = Query(default=False),
    ):
        _require_ready(service)
        payload = service.get_fund_series(
            scheme_code,
            range_key=range,
            benchmark_key=benchmark,
            compare_codes=_split(compare),
            include_drawdown=drawdown,
        )
        if payload is None:
            raise HTTPException(status_code=404, detail=f"No NAV history for scheme code: {scheme_code}")
        return payload

    @router.get("/fund/{scheme_code}/review")
    def fund_review_endpoint(scheme_code: str):
        """How this fund measures against its own category.

        Deterministic arithmetic only. Every figure is reproducible from the
        universe, and nothing here recommends an action — see
        `mutual_funds/fund_review.py` for why that line is drawn.
        """
        _require_ready(service)
        payload = service.get_fund_review(scheme_code)
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Unknown scheme code: {scheme_code}")
        return payload

    @router.get("/fund/{scheme_code}/ai-review")
    def fund_ai_review(scheme_code: str):
        """Plain-English summary of the measured review.

        Describes the evidence and nothing more. It does not recommend buying,
        selling or switching anything — that is personalised investment advice
        and out of scope for this app.
        """
        _require_ready(service)
        return service.get_fund_ai_review(scheme_code)

    @router.get("/portfolio")
    def get_portfolio():
        return service.get_portfolio()

    @router.put("/portfolio")
    def save_portfolio(payload: dict = Body(default_factory=dict)):
        return service.save_portfolio(payload)

    @router.post("/portfolio/sip-preview")
    def sip_preview(payload: dict = Body(default_factory=dict)):
        """Expand a monthly SIP description into transactions, without saving.

        Lets the UI show "36 instalments, 15 Jan 2024 to 15 Dec 2026" for
        confirmation before the user commits it to a position.
        """
        transactions = service.expand_sip(
            start_date=str(payload.get("start_date") or ""),
            end_date=str(payload.get("end_date") or ""),
            amount=payload.get("amount") or 0,
            frequency=str(payload.get("frequency") or "monthly"),
            day_of_month=payload.get("day_of_month"),
            weekday=payload.get("weekday"),
        )
        total = sum(t["amount"] or 0 for t in transactions)
        return {
            "count": len(transactions),
            "total_amount": round(total, 2),
            "first_date": transactions[0]["date"] if transactions else None,
            "last_date": transactions[-1]["date"] if transactions else None,
            "transactions": transactions,
        }

    @router.post("/portfolio/opening-position")
    def opening_position(payload: dict = Body(default_factory=dict)):
        """Seed a holding from units already owned, priced at that date's NAV.

        For an investor who has been running a SIP for years and does not want
        to key in every past instalment.
        """
        transactions = service.opening_position(
            units=payload.get("units") or 0,
            as_of=str(payload.get("as_of") or ""),
        )
        return {"count": len(transactions), "transactions": transactions}

    return router
