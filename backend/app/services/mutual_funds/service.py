"""The Funds page, server side.

Holds one thing in memory: the universe table (~1,600 flat rows). Per-fund NAV
history and holdings are read from disk on demand and LRU-capped, because
holding 1,600 NAV series resident would cost more RAM than the equity snapshot
cache this Space already runs close to the limit on.

Freshness model, in order of preference per fund:
  disk cache (fresh)  ->  fetch from source  ->  disk cache (stale, flagged)
A stale-but-labelled number beats an empty page, so nothing here raises to the
caller for a data-source failure; `stale` and `as_of` travel with the payload
and the UI says so.
"""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import (
    benchmarks, concentration, fund_review, groww_source, index_source, metrics,
    nav_source, overlap, paths, portfolio, portfolio_health, sector_stages,
    statement_import,
)
from .harvest import detail_blob, universe_row

# Chart ranges, in calendar days. "max" is handled separately.
RANGE_DAYS: dict[str, int] = {
    "1m": 31,
    "3m": 92,
    "6m": 183,
    "1y": 366,
    "2y": 731,
    "3y": 1096,
    "5y": 1827,
    "10y": 3653,
}

SORTABLE_FIELDS = {
    "name", "amc", "sub_category", "aum_crore", "expense_ratio", "nav",
    "return_1d", "return_1w", "return_1m", "return_3m", "return_6m",
    "return_1y", "return_2y", "return_3y", "return_5y", "return_7y", "return_10y",
    "cagr_inception", "volatility", "max_drawdown", "current_drawdown",
    "sharpe", "sortino", "alpha", "beta", "up_capture", "down_capture",
    "information_ratio", "tracking_error", "portfolio_turnover", "age_years",
    "rolling3y_median", "rolling5y_median", "rolling3y_min", "rolling5y_min",
    "rank_1y", "rank_3y", "rank_5y", "percentile_1y", "percentile_3y", "percentile_5y",
    "source_rating", "launch_date",
}

_NAV_CACHE_LIMIT = 120
_DETAIL_CACHE_LIMIT = 80


class MutualFundService:
    def __init__(
        self,
        *,
        database_url: str | None = None,
        state_dir: Path | None = None,
        ai_service: Any | None = None,
    ) -> None:
        paths.ensure_dirs()
        self._state_dir = Path(state_dir) if state_dir else None
        self._store = portfolio.MutualFundPortfolioStore(database_url)
        # Optional. The measured review works without it; only the prose layer
        # needs Gemini, and it is cached so a fund is summarised once a day.
        self._ai_service = ai_service
        self._ai_lock = threading.Lock()
        self._ai_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._amfi_lock = threading.Lock()
        self._amfi_cache: list[dict[str, Any]] | None = None
        self._amfi_loaded_at = 0.0

        self._universe_lock = threading.Lock()
        self._universe: dict[str, Any] | None = None
        self._universe_mtime: float = 0.0
        self._by_code: dict[str, dict[str, Any]] = {}

        self._nav_lock = threading.Lock()
        self._nav_cache: "OrderedDict[str, dict]" = OrderedDict()
        self._detail_lock = threading.Lock()
        self._detail_cache: "OrderedDict[str, dict]" = OrderedDict()

    # ------------------------------------------------------------- universe

    def _portfolio_path(self) -> Path:
        if self._state_dir:
            return Path(self._state_dir) / "data" / "mf_portfolio.json"
        return paths.PORTFOLIO_PATH

    def _load_universe(self) -> dict[str, Any]:
        """Read `mf_universe.json`, reloading only when the file changed.

        The daily rebuild replaces the file underneath a running process, so
        an mtime check is what keeps a long-lived Space from serving last
        week's ranks forever.
        """
        with self._universe_lock:
            try:
                mtime = paths.UNIVERSE_PATH.stat().st_mtime
            except OSError:
                if self._universe is None:
                    self._universe = {"funds": [], "categories": {}, "fund_count": 0, "as_of": None, "missing": True}
                    self._by_code = {}
                return self._universe

            if self._universe is not None and mtime == self._universe_mtime:
                return self._universe

            try:
                payload = json.loads(paths.UNIVERSE_PATH.read_text())
            except (OSError, ValueError):
                if self._universe is None:
                    self._universe = {"funds": [], "categories": {}, "fund_count": 0, "as_of": None, "missing": True}
                    self._by_code = {}
                return self._universe

            funds = payload.get("funds") or []
            self._universe = payload
            self._universe_mtime = mtime
            self._by_code = {str(row.get("scheme_code")): row for row in funds if row.get("scheme_code")}
            return self._universe

    # ------------------------------------------------------------ nav access

    def _nav_series(self, scheme_code: str, *, allow_fetch: bool = True) -> dict[str, Any] | None:
        code = str(scheme_code).strip()
        if not code:
            return None
        with self._nav_lock:
            cached = self._nav_cache.get(code)
            if cached is not None:
                self._nav_cache.move_to_end(code)
                if (time.time() - cached.get("_loaded_at", 0)) < paths.NAV_TTL_SECONDS:
                    return cached

        path = paths.nav_path(code)
        payload: dict[str, Any] | None = None
        disk_fresh = False
        try:
            stat = path.stat()
            disk_fresh = (time.time() - stat.st_mtime) < paths.NAV_TTL_SECONDS
            payload = json.loads(path.read_text())
        except (OSError, ValueError):
            payload = None

        if (payload is None or not disk_fresh) and allow_fetch:
            try:
                fetched = nav_source.fetch_nav_history(code)
                paths.ensure_dirs()
                temp = path.with_suffix(".tmp")
                temp.write_text(json.dumps(fetched, separators=(",", ":")))
                temp.replace(path)
                payload = fetched
            except nav_source.NavUnavailable:
                pass  # keep whatever the disk had

        if not isinstance(payload, dict) or not payload.get("dates"):
            return None
        payload["_loaded_at"] = time.time()
        with self._nav_lock:
            self._nav_cache[code] = payload
            self._nav_cache.move_to_end(code)
            while len(self._nav_cache) > _NAV_CACHE_LIMIT:
                self._nav_cache.popitem(last=False)
        return payload

    def _detail(self, scheme_code: str, slug: str | None) -> tuple[dict[str, Any] | None, bool]:
        """Holdings blob for one fund. Returns (payload, is_stale)."""
        code = str(scheme_code).strip()
        with self._detail_lock:
            cached = self._detail_cache.get(code)
            if cached is not None:
                self._detail_cache.move_to_end(code)
                return cached, False

        path = paths.detail_path(code)
        payload: dict[str, Any] | None = None
        fresh = False
        try:
            fresh = (time.time() - path.stat().st_mtime) < paths.DETAIL_TTL_SECONDS
            payload = json.loads(path.read_text())
        except (OSError, ValueError):
            payload = None

        if (payload is None or not fresh) and slug:
            try:
                raw = groww_source.fetch_scheme(slug)
                payload = detail_blob(raw)
                paths.ensure_dirs()
                temp = path.with_suffix(".tmp")
                temp.write_text(json.dumps(payload, separators=(",", ":")))
                temp.replace(path)
                fresh = True
            except groww_source.GrowwUnavailable:
                pass

        if not isinstance(payload, dict):
            return None, False
        with self._detail_lock:
            self._detail_cache[code] = payload
            self._detail_cache.move_to_end(code)
            while len(self._detail_cache) > _DETAIL_CACHE_LIMIT:
                self._detail_cache.popitem(last=False)
        return payload, not fresh

    # --------------------------------------------------------------- screener

    def get_status(self) -> dict[str, Any]:
        universe = self._load_universe()
        return {
            "ready": bool(universe.get("funds")),
            "fund_count": universe.get("fund_count") or 0,
            "as_of": universe.get("as_of"),
            "generated_at": universe.get("generated_at"),
            "category_count": len(universe.get("categories") or {}),
            "ranked_windows": universe.get("ranked_windows") or [],
        }

    def get_screener(
        self,
        *,
        category: str | None = None,
        sub_categories: list[str] | None = None,
        amcs: list[str] | None = None,
        search: str | None = None,
        min_aum: float | None = None,
        max_expense: float | None = None,
        min_age_years: float | None = None,
        max_quartile: int | None = None,
        only_codes: list[str] | None = None,
        sort_by: str = "return_3y",
        sort_dir: str = "desc",
        limit: int = 250,
        offset: int = 0,
    ) -> dict[str, Any]:
        universe = self._load_universe()
        rows: list[dict[str, Any]] = list(universe.get("funds") or [])

        wanted_subs = {value.strip().lower() for value in (sub_categories or []) if value.strip()}
        wanted_amcs = {value.strip().lower() for value in (amcs or []) if value.strip()}
        wanted_codes = {value.strip() for value in (only_codes or []) if value.strip()}
        needle = (search or "").strip().lower()
        category_key = (category or "").strip().lower()

        def keep(row: dict[str, Any]) -> bool:
            if wanted_codes and str(row.get("scheme_code")) not in wanted_codes:
                return False
            if category_key and str(row.get("category") or "").lower() != category_key:
                return False
            if wanted_subs and str(row.get("sub_category") or "").lower() not in wanted_subs:
                return False
            if wanted_amcs and str(row.get("amc") or "").lower() not in wanted_amcs:
                return False
            if needle:
                haystack = f"{row.get('name') or ''} {row.get('amc') or ''} {row.get('sub_category') or ''}".lower()
                if needle not in haystack:
                    return False
            if min_aum is not None and not (isinstance(row.get("aum_crore"), (int, float)) and row["aum_crore"] >= min_aum):
                return False
            if max_expense is not None and not (isinstance(row.get("expense_ratio"), (int, float)) and row["expense_ratio"] <= max_expense):
                return False
            if min_age_years is not None and not (isinstance(row.get("age_years"), (int, float)) and row["age_years"] >= min_age_years):
                return False
            if max_quartile is not None:
                quartile = row.get("quartile_3y") or row.get("quartile_1y")
                if not (isinstance(quartile, int) and quartile <= max_quartile):
                    return False
            return True

        filtered = [row for row in rows if keep(row)]

        field = sort_by if sort_by in SORTABLE_FIELDS else "return_3y"
        descending = str(sort_dir or "desc").lower() != "asc"
        # Rank columns are "1 is best", so a descending request on a rank means
        # ascending on the number. Without this the sort arrow lies.
        if field.startswith("rank_"):
            descending = not descending

        def sort_key(row: dict[str, Any]):
            value = row.get(field)
            if isinstance(value, str):
                return (0, value.lower())
            if isinstance(value, (int, float)):
                return (0, value)
            # Missing values always sort last, in both directions — a fund
            # with no 5-year record should not top a 5-year ranking.
            return (1, float("-inf") if descending else float("inf"))

        filtered.sort(key=sort_key, reverse=descending)
        # Re-apply "missing last" after the reverse flipped it.
        present = [row for row in filtered if isinstance(row.get(field), (int, float, str))]
        absent = [row for row in filtered if not isinstance(row.get(field), (int, float, str))]
        filtered = present + absent

        total = len(filtered)
        window = filtered[max(0, offset): max(0, offset) + max(1, min(limit, 1000))]

        held = {position["scheme_code"] for position in self._raw_portfolio().get("positions", [])}

        return {
            "as_of": universe.get("as_of"),
            "generated_at": universe.get("generated_at"),
            "total": total,
            "returned": len(window),
            "offset": offset,
            "sort_by": field,
            "sort_dir": "desc" if descending else "asc",
            "funds": [{**row, "in_portfolio": str(row.get("scheme_code")) in held} for row in window],
            "facets": self._facets(rows),
            "categories": universe.get("categories") or {},
        }

    def _facets(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        categories: dict[str, int] = {}
        sub_categories: dict[str, dict[str, Any]] = {}
        amcs: dict[str, int] = {}
        for row in rows:
            category = str(row.get("category") or "Other")
            categories[category] = categories.get(category, 0) + 1
            sub = str(row.get("sub_category") or "Other")
            entry = sub_categories.setdefault(sub, {"count": 0, "category": category})
            entry["count"] += 1
            amc = str(row.get("amc") or "Other")
            amcs[amc] = amcs.get(amc, 0) + 1
        return {
            "categories": dict(sorted(categories.items(), key=lambda kv: -kv[1])),
            "sub_categories": dict(sorted(sub_categories.items(), key=lambda kv: -kv[1]["count"])),
            "amcs": dict(sorted(amcs.items(), key=lambda kv: -kv[1])),
        }

    def get_category_leaderboard(self) -> dict[str, Any]:
        """Every sub-category with its average metrics and its top funds.

        This is the "where does my fund sit" view rendered category-first
        rather than fund-first.
        """
        universe = self._load_universe()
        rows = universe.get("funds") or []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row.get("sub_category") or "Other"), []).append(row)

        out: list[dict[str, Any]] = []
        for sub_category, members in grouped.items():
            ranked = sorted(
                (row for row in members if isinstance(row.get("return_3y"), (int, float))),
                key=lambda row: row["return_3y"],
                reverse=True,
            )
            summary = (universe.get("categories") or {}).get(sub_category, {})
            bench = benchmarks.resolve(sub_category, category=members[0].get("category") if members else None)
            out.append({
                "sub_category": sub_category,
                "category": members[0].get("category") if members else None,
                "count": len(members),
                "benchmark_label": bench.label,
                "benchmark_is_reference_only": bench.is_reference_only,
                "avg_return_1y": summary.get("return_1y"),
                "avg_return_3y": summary.get("return_3y"),
                "avg_return_5y": summary.get("return_5y"),
                "avg_expense_ratio": summary.get("expense_ratio"),
                "avg_volatility": summary.get("volatility"),
                "avg_max_drawdown": summary.get("max_drawdown"),
                "leaders": [
                    {
                        "scheme_code": row.get("scheme_code"),
                        "name": row.get("name"),
                        "amc": row.get("amc"),
                        "return_3y": row.get("return_3y"),
                        "return_1y": row.get("return_1y"),
                        "expense_ratio": row.get("expense_ratio"),
                    }
                    for row in ranked[:5]
                ],
            })
        out.sort(key=lambda item: (-(item["count"] or 0), item["sub_category"]))
        return {"as_of": universe.get("as_of"), "categories": out}

    # ------------------------------------------------------------ fund detail

    def get_fund(self, scheme_code: str) -> dict[str, Any] | None:
        self._load_universe()
        row = self._by_code.get(str(scheme_code).strip())
        if row is None:
            return None

        detail, detail_stale = self._detail(row["scheme_code"], row.get("slug"))
        nav = self._nav_series(row["scheme_code"])

        rolling: dict[str, Any] = {}
        recovery: dict[str, Any] = {}
        calendar_years: list[dict[str, Any]] = []
        if nav:
            dates, navs = nav["dates"], nav["navs"]
            for window in (1, 3, 5, 7):
                stats = metrics.rolling_returns(dates, navs, window_years=window, step_days=7)
                if stats["count"]:
                    rolling[f"{window}y"] = stats
            calendar_years = self._calendar_year_returns(dates, navs)
            recovery = self._drawdown_profile(dates, navs)

        bench = benchmarks.ALL_BENCHMARKS.get(str(row.get("benchmark_key"))) or benchmarks.resolve(row.get("sub_category"))
        peers = self._category_peers(row)

        return {
            "fund": row,
            "detail": detail or {},
            "detail_stale": detail_stale,
            "detail_available": detail is not None,
            "rolling_returns": rolling,
            "calendar_year_returns": calendar_years,
            "drawdown_profile": recovery,
            "benchmark": {
                "key": bench.key,
                "label": bench.label,
                "source": bench.source,
                "total_return": bench.total_return,
                "is_reference_only": bench.is_reference_only,
                "notes": bench.notes,
                "official": row.get("official_benchmark"),
            },
            "category_peers": peers,
            "category_summary": (self._load_universe().get("categories") or {}).get(str(row.get("sub_category"))),
        }

    @staticmethod
    def _calendar_year_returns(dates: list[str], navs: list[float]) -> list[dict[str, Any]]:
        """Year-by-year performance — the view that shows whether a fund's
        record is a couple of exceptional years or a steady record."""
        by_year: dict[int, tuple[float, float]] = {}
        for day, nav in zip(dates, navs):
            year = int(day[:4])
            if year in by_year:
                by_year[year] = (by_year[year][0], nav)
            else:
                by_year[year] = (nav, nav)

        years = sorted(by_year)
        out: list[dict[str, Any]] = []
        for index, year in enumerate(years):
            open_nav, close_nav = by_year[year]
            # Chain from the previous year's close so January's move is not
            # silently dropped from every year's number.
            if index > 0:
                open_nav = by_year[years[index - 1]][1]
            if open_nav <= 0:
                continue
            out.append({
                "year": year,
                "return_pct": round((close_nav / open_nav - 1.0) * 100.0, 2),
                "partial": index == len(years) - 1,
            })
        return out[-12:]

    @staticmethod
    def _drawdown_profile(dates: list[str], navs: list[float]) -> dict[str, Any]:
        """Worst falls and how long they took to recover.

        Depth alone is half the story — a 35% fall that took four years to
        recover is a different fund from a 35% fall recovered in seven months.
        """
        peak = navs[0]
        peak_date = dates[0]
        trough = navs[0]
        trough_date = dates[0]
        in_drawdown = False
        episodes: list[dict[str, Any]] = []

        for day, nav in zip(dates, navs):
            if nav >= peak:
                if in_drawdown:
                    episodes.append({
                        "depth_pct": round((trough / peak - 1.0) * 100.0, 2),
                        "peak_date": peak_date,
                        "trough_date": trough_date,
                        "recovery_date": day,
                        "fall_days": (date.fromisoformat(trough_date) - date.fromisoformat(peak_date)).days,
                        "recovery_days": (date.fromisoformat(day) - date.fromisoformat(trough_date)).days,
                        "recovered": True,
                    })
                    in_drawdown = False
                peak, peak_date = nav, day
                trough, trough_date = nav, day
                continue
            if not in_drawdown:
                in_drawdown = True
                trough, trough_date = nav, day
            elif nav < trough:
                trough, trough_date = nav, day

        if in_drawdown and peak > 0:
            episodes.append({
                "depth_pct": round((trough / peak - 1.0) * 100.0, 2),
                "peak_date": peak_date,
                "trough_date": trough_date,
                "recovery_date": None,
                "fall_days": (date.fromisoformat(trough_date) - date.fromisoformat(peak_date)).days,
                "recovery_days": None,
                "recovered": False,
            })

        episodes.sort(key=lambda item: item["depth_pct"])
        material = [episode for episode in episodes if episode["depth_pct"] <= -10.0]
        recovered = [episode for episode in material if episode["recovered"]]
        return {
            "worst": episodes[:5],
            "episode_count": len(material),
            "avg_recovery_days": (
                round(sum(episode["recovery_days"] for episode in recovered) / len(recovered))
                if recovered else None
            ),
        }

    def _category_peers(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        """The fund's whole category, ranked — so the detail view can show
        exactly where it sits rather than just asserting a rank."""
        sub_category = str(row.get("sub_category") or "")
        members = [
            other for other in (self._load_universe().get("funds") or [])
            if str(other.get("sub_category") or "") == sub_category
        ]
        members.sort(
            key=lambda other: (other.get("return_3y") is None, -(other.get("return_3y") or 0)),
        )
        return [
            {
                "scheme_code": other.get("scheme_code"),
                "name": other.get("name"),
                "amc": other.get("amc"),
                "return_1y": other.get("return_1y"),
                "return_3y": other.get("return_3y"),
                "return_5y": other.get("return_5y"),
                "expense_ratio": other.get("expense_ratio"),
                "aum_crore": other.get("aum_crore"),
                "sharpe": other.get("sharpe"),
                "max_drawdown": other.get("max_drawdown"),
                "is_self": str(other.get("scheme_code")) == str(row.get("scheme_code")),
            }
            for other in members
        ]

    # ------------------------------------------------------------ chart series

    def _benchmark_series(self, bench: benchmarks.Benchmark, *, needed_from: str | None) -> tuple[dict[str, Any] | None, str, str]:
        """Resolve a benchmark to an actual series.

        Returns (series, label, source_kind). Falls back to the longer-history
        price index when the preferred index fund launched after the window the
        user asked for — otherwise a 10-year chart would show a benchmark line
        that starts in 2021 with no explanation.
        """
        if bench.source == "mf" and bench.scheme_code:
            series = self._nav_series(bench.scheme_code)
            starts_late = (
                series is not None
                and needed_from is not None
                and series["dates"]
                and series["dates"][0] > needed_from
            )
            if series and not starts_late:
                return series, bench.label, "index_fund_nav"
            if starts_late and bench.fallback_yahoo_symbol:
                try:
                    fallback = index_source.fetch_index_series(bench.fallback_yahoo_symbol)
                    return fallback, bench.fallback_label or bench.label, "price_index"
                except index_source.IndexUnavailable:
                    pass
            if series:
                return series, bench.label, "index_fund_nav"

        symbol = bench.yahoo_symbol or bench.fallback_yahoo_symbol
        if symbol:
            try:
                return index_source.fetch_index_series(symbol), bench.label, "price_index"
            except index_source.IndexUnavailable:
                pass
        return None, bench.label, "unavailable"

    def get_fund_series(
        self,
        scheme_code: str,
        *,
        range_key: str = "3y",
        benchmark_key: str | None = None,
        compare_codes: list[str] | None = None,
        include_drawdown: bool = False,
    ) -> dict[str, Any] | None:
        """Chart payload: the fund, its benchmark, and any compared funds.

        Everything is returned twice — as raw NAV and as growth-of-100 rebased
        to the window's first common date. Rebasing is what makes a fund
        comparable to an index whose level is 24,000, and rebasing *inside the
        window* (not at inception) is what makes the 1-year chart answer "how
        did this year go" instead of "how did 2013 go".
        """
        self._load_universe()
        row = self._by_code.get(str(scheme_code).strip())
        if row is None:
            return None
        nav = self._nav_series(row["scheme_code"])
        if not nav:
            return None

        dates: list[str] = nav["dates"]
        navs: list[float] = nav["navs"]

        key = str(range_key or "3y").lower()
        if key == "max" or key not in RANGE_DAYS:
            start_index = 0
            key = "max" if key == "max" else key
        else:
            cutoff = (date.fromisoformat(dates[-1]) - timedelta(days=RANGE_DAYS[key])).isoformat()
            start_index = 0
            for index, day in enumerate(dates):
                if day >= cutoff:
                    start_index = index
                    break

        window_dates = dates[start_index:]
        window_navs = navs[start_index:]
        if len(window_dates) < 2:
            window_dates, window_navs = dates, navs

        bench = (
            benchmarks.ALL_BENCHMARKS.get(str(benchmark_key))
            if benchmark_key
            else None
        ) or benchmarks.ALL_BENCHMARKS.get(str(row.get("benchmark_key"))) or benchmarks.resolve(row.get("sub_category"))

        bench_series, bench_label, bench_kind = self._benchmark_series(bench, needed_from=window_dates[0])

        series: list[dict[str, Any]] = [{
            "key": str(row["scheme_code"]),
            "label": row.get("name") or row.get("scheme_name"),
            "kind": "fund",
            "dates": window_dates,
            "values": window_navs,
            "rebased": _rebase(window_navs),
        }]

        benchmark_payload: dict[str, Any] | None = None
        if bench_series:
            aligned_dates, fund_leg, bench_leg = metrics.align_series(
                window_dates, window_navs, bench_series["dates"], bench_series["navs"]
            )
            if len(aligned_dates) >= 2:
                fund_growth = fund_leg[-1] / fund_leg[0] - 1.0
                bench_growth = bench_leg[-1] / bench_leg[0] - 1.0
                elapsed = (date.fromisoformat(aligned_dates[-1]) - date.fromisoformat(aligned_dates[0])).days
                stats = metrics.beta_alpha(
                    fund_leg,
                    bench_leg,
                    fund_cagr_pct=metrics.annualise(fund_leg[-1] / fund_leg[0], elapsed),
                    bench_cagr_pct=metrics.annualise(bench_leg[-1] / bench_leg[0], elapsed),
                )
                benchmark_payload = {
                    "key": bench.key,
                    "label": bench_label,
                    "kind": "benchmark",
                    "source_kind": bench_kind,
                    "total_return": bench_kind == "index_fund_nav",
                    "is_reference_only": bench.is_reference_only,
                    "notes": bench.notes,
                    "official_benchmark": row.get("official_benchmark"),
                    "dates": aligned_dates,
                    "values": bench_leg,
                    "rebased": _rebase(bench_leg),
                    "fund_rebased": _rebase(fund_leg),
                    "window_fund_return_pct": round(fund_growth * 100.0, 2),
                    "window_benchmark_return_pct": round(bench_growth * 100.0, 2),
                    "window_excess_pct": round((fund_growth - bench_growth) * 100.0, 2),
                    "window_days": elapsed,
                    **{name: (round(value, 4) if isinstance(value, float) else value) for name, value in stats.items()},
                }

        for code in (compare_codes or [])[:4]:
            other = self._by_code.get(str(code).strip())
            if other is None or str(other["scheme_code"]) == str(row["scheme_code"]):
                continue
            other_nav = self._nav_series(other["scheme_code"])
            if not other_nav:
                continue
            aligned_dates, _, other_leg = metrics.align_series(
                window_dates, window_navs, other_nav["dates"], other_nav["navs"]
            )
            if len(aligned_dates) < 2:
                continue
            series.append({
                "key": str(other["scheme_code"]),
                "label": other.get("name"),
                "kind": "compare",
                "dates": aligned_dates,
                "values": other_leg,
                "rebased": _rebase(other_leg),
            })

        drawdown: list[float] | None = None
        if include_drawdown:
            drawdown = []
            peak = window_navs[0]
            for value in window_navs:
                peak = max(peak, value)
                drawdown.append(round((value / peak - 1.0) * 100.0, 3))

        return {
            "scheme_code": row["scheme_code"],
            "name": row.get("name"),
            "range": key,
            "available_ranges": _available_ranges(dates),
            "as_of": dates[-1],
            "inception": dates[0],
            "series": series,
            "benchmark": benchmark_payload,
            "drawdown": drawdown,
            "benchmark_options": [
                {"key": item.key, "label": item.label, "source": item.source}
                for item in benchmarks.ALL_BENCHMARKS.values()
            ],
        }

    # -------------------------------------------------------------- portfolio

    def _raw_portfolio(self) -> dict[str, Any]:
        path = self._portfolio_path()
        if self._store.is_enabled():
            try:
                stored = self._store.load_data()
                if stored:
                    return portfolio.normalise_payload(stored)
            except Exception:
                pass
        try:
            return portfolio.normalise_payload(json.loads(path.read_text()))
        except (OSError, ValueError):
            return {"updated_at": None, "positions": []}

    @staticmethod
    def _tidy_scheme_name(raw: str | None) -> str | None:
        """Drop the plan/option tail AMFI appends, and de-shout the name.

        "SBI TECHNOLOGY OPPORTUNITIES FUND - Direct Plan - IDCW" becomes "SBI
        Technology Opportunities Fund". The plan variant is surfaced as a badge
        instead, because the untrimmed name is long enough to wreck a table row
        — one of them runs to "Payout of Income Distribution cum capital
        withdrawal option".
        """
        text = str(raw or "").strip()
        if not text:
            return None
        head = re.split(r"\s+-\s+(?:direct|regular)\s+plan", text, flags=re.IGNORECASE)[0]
        head = head.strip(" -–—")
        # AMFI mixes ALL CAPS and title case; normalise the shouting only.
        if head.isupper():
            head = " ".join(
                word if len(word) <= 3 and word.isalpha() and word.upper() == word and len(word) <= 4
                else word.capitalize()
                for word in head.split()
            )
        return head or text

    def _off_universe_identity(self, code: str, nav: dict[str, Any]) -> dict[str, Any]:
        """Identity for a holding outside the Direct-Growth universe.

        An IDCW or Payout plan is the same portfolio as its Growth sibling —
        same manager, same holdings, same category — so the sibling's
        classification is inherited. That keeps allocation grouped under one
        consistent set of category names instead of AMFI's parallel vocabulary
        ("Equity Scheme - Sectoral/ Thematic" alongside our "Sectoral").
        """
        name = self._tidy_scheme_name(nav.get("scheme_name"))
        sibling = None
        if name:
            wanted = _match_key(name)
            for candidate in (self._universe or {}).get("funds") or []:
                if _match_key(candidate.get("name")) == wanted:
                    sibling = candidate
                    break

        category = str(nav.get("scheme_category") or "")
        return {
            "scheme_code": code,
            "name": name or nav.get("scheme_name"),
            "amc": (sibling or {}).get("amc") or nav.get("fund_house"),
            "sub_category": (sibling or {}).get("sub_category")
                            or (category.split(" - ")[-1] if " - " in category else category or None),
            "benchmark_label": (sibling or {}).get("benchmark_label"),
            "expense_ratio": (sibling or {}).get("expense_ratio"),
            "off_universe": True,
            "plan_variant": self._plan_variant(nav.get("scheme_name")),
            # The Growth sibling is where category comparisons can be made.
            "growth_sibling_code": (sibling or {}).get("scheme_code"),
            "growth_sibling_slug": (sibling or {}).get("slug"),
        }

    @staticmethod
    def _plan_variant(raw: str | None) -> str | None:
        text = str(raw or "").lower()
        if "payout" in text:
            return "Payout"
        if "idcw" in text or "dividend" in text:
            return "IDCW"
        return None

    def materialise_due_sips(self) -> int:
        """Turn SIP instalments whose date has arrived into real transactions.

        This is what lets XIRR become available over time. An imported
        statement has no dated cashflows, so it can only ever report P&L; every
        instalment that lands from here on is recorded with its actual date and
        the NAV that applied, and those are exactly the inputs XIRR needs.

        Runs on read and persists only when something changed, so the portfolio
        keeps itself current without a scheduled job.
        """
        state = self._raw_portfolio()
        positions = state.get("positions") or []
        today = date.today().isoformat()
        changed = 0

        for position in positions:
            plan = position.get("sip_plan")
            if not plan or not plan.get("active"):
                continue
            nav = self._nav_series(str(position["scheme_code"]))
            if not nav or not nav.get("dates"):
                continue

            due = [
                item for item in portfolio.upcoming_instalments(plan, count=24)
                if item["date"] <= today
            ]
            if not due:
                continue

            existing = {
                (t["date"], round(t.get("amount") or 0, 2))
                for t in position.get("transactions") or []
            }
            added = 0
            for item in due:
                priced = portfolio.nav_on_or_before(nav["dates"], nav["navs"], item["date"])
                if priced is None:
                    continue
                key = (item["date"], round(item["amount"], 2))
                if key in existing:
                    continue
                position.setdefault("transactions", []).append({
                    "id": str(uuid.uuid4()),
                    "date": item["date"],
                    "type": "buy",
                    "amount": item["amount"],
                    "units": None,
                    "nav": None,
                })
                existing.add(key)
                added += 1

            if added:
                # Advance the plan past everything just recorded.
                remaining = [
                    item for item in portfolio.upcoming_instalments(plan, count=24)
                    if item["date"] > today
                ]
                if remaining:
                    plan["next_date"] = remaining[0]["date"]
                position["transactions"].sort(key=lambda item: item["date"])
                changed += added

        if changed:
            self.save_portfolio({"positions": positions})
        return changed

    def get_portfolio(self) -> dict[str, Any]:
        """Valued portfolio: per-position units, cost, value, gain and XIRR."""
        # Record any SIP instalment that has come due, so the numbers move on
        # their own rather than being frozen at whatever was last entered.
        try:
            self.materialise_due_sips()
        except Exception:
            pass
        state = self._raw_portfolio()
        self._load_universe()

        positions: list[dict[str, Any]] = []
        all_cashflows: list[tuple[date, float]] = []
        total_invested = 0.0
        total_value = 0.0
        total_realised = 0.0
        total_cost_of_sold = 0.0
        monthly_sip_total = 0.0
        active_sips = 0
        next_sips: list[dict[str, Any]] = []

        for position in state.get("positions", []):
            code = str(position["scheme_code"])
            row = self._by_code.get(code)
            nav = self._nav_series(code)
            valued = portfolio.value_position(position, nav_series=nav)
            # A holding can legitimately sit outside the screener universe —
            # an IDCW or Payout plan, which this universe deliberately excludes.
            # It still values correctly off its own NAV series, so fall back to
            # the identity that series carries rather than rendering a blank row.
            if row is None and nav:
                row = self._off_universe_identity(code, nav)
            valued["fund"] = row
            valued["transactions"] = position.get("transactions") or []
            positions.append(valued)

            plan = position.get("sip_plan")
            if plan and plan.get("active"):
                monthly_sip_total += plan.get("monthly_equivalent") or 0.0
                active_sips += 1
                upcoming = portfolio.upcoming_instalments(plan, count=1)
                if upcoming:
                    next_sips.append({
                        "scheme_code": position["scheme_code"],
                        "name": (row or {}).get("name"),
                        "date": upcoming[0]["date"],
                        "amount": upcoming[0]["amount"],
                        "frequency": plan.get("frequency"),
                    })

            total_invested += valued.get("invested") or 0.0
            total_value += valued.get("current_value") or 0.0
            total_realised += valued.get("realised_pnl") or 0.0
            total_cost_of_sold += valued.get("cost_of_units_sold") or 0.0
            for transaction in position.get("transactions") or []:
                nav_on_date = None
                if nav:
                    resolved = portfolio.nav_on_or_before(nav["dates"], nav["navs"], transaction["date"])
                    nav_on_date = transaction.get("nav") or (resolved[1] if resolved else None)
                amount = transaction.get("amount")
                if amount is None and transaction.get("units") and nav_on_date:
                    amount = transaction["units"] * nav_on_date
                if not amount:
                    continue
                when = date.fromisoformat(transaction["date"])
                all_cashflows.append((when, -amount if transaction["type"] == "buy" else amount))

        # One portfolio-level XIRR across every position's cashflows, valued at
        # the latest NAV date we have for anything held.
        valuation_dates = [
            position["latest_nav_date"] for position in positions if position.get("latest_nav_date")
        ]
        portfolio_xirr = None
        if all_cashflows and valuation_dates and total_value > 0:
            all_cashflows.append((date.fromisoformat(max(valuation_dates)), total_value))
            computed = metrics.xirr(all_cashflows)
            if computed is not None and -95.0 < computed < 300.0:
                first = min(when for when, _ in all_cashflows)
                if (date.fromisoformat(max(valuation_dates)) - first).days >= 90:
                    portfolio_xirr = round(computed, 2)

        for position in positions:
            position["weight_pct"] = (
                round((position.get("current_value") or 0.0) / total_value * 100.0, 2)
                if total_value > 0 else None
            )

        # Unrealised on what is still held, plus realised on what was sold —
        # reported separately because that is how a broker statement reads and
        # how the two are taxed.
        unrealised = total_value - total_invested
        total_pnl = unrealised + total_realised
        deployed = total_invested + total_cost_of_sold
        return {
            "updated_at": state.get("updated_at"),
            "as_of": max(valuation_dates) if valuation_dates else None,
            "positions": positions,
            "totals": {
                "position_count": len(positions),
                "open_position_count": sum(1 for p in positions if (p.get("units") or 0) > 0),
                "invested": round(total_invested, 2) if total_invested else None,
                "current_value": round(total_value, 2) if total_value else None,
                "unrealised_pnl": round(unrealised, 2),
                "unrealised_pct": round(unrealised / total_invested * 100.0, 2) if total_invested > 0 else None,
                "realised_pnl": round(total_realised, 2) if total_realised else None,
                "cost_of_units_sold": round(total_cost_of_sold, 2) if total_cost_of_sold else None,
                "gain": round(total_pnl, 2),
                "gain_pct": round(total_pnl / deployed * 100.0, 2) if deployed > 0 else None,
                "xirr": portfolio_xirr,
                "monthly_sip": round(monthly_sip_total, 2) if monthly_sip_total else None,
                "active_sip_count": active_sips,
            },
            "upcoming_sips": sorted(next_sips, key=lambda item: item["date"])[:8],
            "allocation": self._portfolio_allocation(positions, total_value),
        }

    def _portfolio_allocation(self, positions: list[dict[str, Any]], total_value: float) -> dict[str, Any]:
        """Where the portfolio actually sits — by category, by AMC, and by
        look-through stock exposure across the funds held.

        The look-through is the number a fund investor cannot get anywhere
        else: five funds each holding HDFC Bank at 8% is a 8% position, not
        five diversified holdings.
        """
        by_category: dict[str, float] = {}
        by_amc: dict[str, float] = {}
        stock_exposure: dict[str, dict[str, Any]] = {}

        for position in positions:
            value = position.get("current_value") or 0.0
            if value <= 0:
                continue
            row = position.get("fund") or {}
            sub_category = str(row.get("sub_category") or "Unknown")
            amc = str(row.get("amc") or "Unknown")
            by_category[sub_category] = round(by_category.get(sub_category, 0.0) + value, 2)
            by_amc[amc] = round(by_amc.get(amc, 0.0) + value, 2)

            detail, _ = self._detail(str(position["scheme_code"]), row.get("slug"))
            for holding in ((detail or {}).get("holdings") or []):
                if holding.get("asset_class") not in ("equity", "international_equity"):
                    continue
                weight = holding.get("weight_pct")
                if not weight:
                    continue
                name = holding.get("name")
                entry = stock_exposure.setdefault(name, {
                    "name": name,
                    "symbol": holding.get("symbol"),
                    "sector": holding.get("sector"),
                    "cap_class": holding.get("cap_class"),
                    "value": 0.0,
                    "funds": [],
                })
                entry["value"] += value * weight / 100.0
                entry["funds"].append({"name": row.get("name"), "weight_pct": weight})

        look_through = sorted(stock_exposure.values(), key=lambda item: -item["value"])
        for entry in look_through:
            entry["value"] = round(entry["value"], 2)
            entry["weight_pct"] = round(entry["value"] / total_value * 100.0, 2) if total_value > 0 else None
            entry["fund_count"] = len(entry["funds"])

        return {
            "by_sub_category": dict(sorted(by_category.items(), key=lambda kv: -kv[1])),
            "by_amc": dict(sorted(by_amc.items(), key=lambda kv: -kv[1])),
            "look_through_top": look_through[:40],
            "look_through_count": len(look_through),
        }

    def save_portfolio(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalised = portfolio.normalise_payload(payload)
        normalised["updated_at"] = datetime.now(timezone.utc).isoformat()
        path = self._portfolio_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp = path.with_suffix(".tmp")
            temp.write_text(json.dumps(normalised, indent=2))
            temp.replace(path)
        except OSError:
            pass
        if self._store.is_enabled():
            try:
                self._store.save_data(normalised)
            except Exception:
                pass
        return self.get_portfolio()

    def get_fund_review(self, scheme_code: str) -> dict[str, Any] | None:
        """Measured standing of one fund against its own category.

        Deterministic — no AI in this path. The prose layer is a separate call
        so the numbers still render when Gemini is unavailable or rate-limited.
        """
        universe = self._load_universe()
        row = self._by_code.get(str(scheme_code).strip())
        if row is None:
            return None
        sub_category = str(row.get("sub_category") or "")
        peers = [
            other for other in (universe.get("funds") or [])
            if str(other.get("sub_category") or "") == sub_category
        ]
        return fund_review.build_review(
            row,
            peers,
            category_summary=(universe.get("categories") or {}).get(sub_category),
        )

    # ------------------------------------------------------------ scheme search

    def _amfi_index(self) -> list[dict[str, Any]]:
        """Every AMFI scheme, cached for the process.

        The screener universe is Direct/Growth only, on purpose. A *portfolio*
        has to be able to name whatever is actually held — including the IDCW
        and Payout plans this user holds — so entry searches the full AMFI list
        rather than the screener's subset.
        """
        with self._amfi_lock:
            if self._amfi_cache is None or (time.time() - self._amfi_loaded_at) > 24 * 60 * 60:
                try:
                    self._amfi_cache = nav_source.fetch_scheme_index()
                    self._amfi_loaded_at = time.time()
                except nav_source.NavUnavailable:
                    self._amfi_cache = self._amfi_cache or []
            return self._amfi_cache or []

    def search_schemes(self, query: str, *, limit: int = 25, direct_only: bool = True) -> dict[str, Any]:
        """Find a scheme to add to the portfolio, by name."""
        needle = " ".join(str(query or "").lower().split())
        if len(needle) < 3:
            return {"query": query, "results": [], "hint": "Type at least three characters."}

        universe = self._load_universe()
        in_universe = {str(row["scheme_code"]) for row in (universe.get("funds") or [])}
        words = needle.split()

        scored: list[tuple[int, dict[str, Any]]] = []
        for scheme in self._amfi_index():
            name = str(scheme.get("schemeName") or "")
            lowered = name.lower()
            if direct_only and "direct" not in lowered:
                continue
            if not all(word in lowered for word in words):
                continue
            code = str(scheme.get("schemeCode"))
            # Rank: screener funds first (they carry full metadata), then
            # Growth over IDCW, then the shortest name — which is almost always
            # the plain plan rather than a variant with a long tail.
            rank = (0 if code in in_universe else 1, 0 if "growth" in lowered else 1, len(name))
            scored.append((rank, {
                "scheme_code": code,
                "name": name,
                "in_universe": code in in_universe,
                "isin": scheme.get("isinGrowth") or scheme.get("isinDivReinvestment"),
            }))
        scored.sort(key=lambda item: item[0])
        return {"query": query, "results": [item[1] for item in scored[:limit]],
                "total": len(scored)}

    # ------------------------------------------------------------------ import

    def import_statement(self, data: bytes, *, filename: str, replace: bool = False) -> dict[str, Any]:
        """Parse a broker statement and merge it into the portfolio."""
        rows, summary = statement_import.parse_statement(data)
        universe = self._load_universe()
        as_of = universe.get("as_of")
        if not as_of:
            raise statement_import.StatementError("the fund universe is not loaded yet")

        try:
            amfi_by_isin = nav_source.build_isin_index()
        except nav_source.NavUnavailable:
            amfi_by_isin = {}

        built = statement_import.build_positions(
            rows, universe=universe, amfi_by_isin=amfi_by_isin,
            as_of=as_of, source_label=filename,
        )
        if not built["positions"]:
            raise statement_import.StatementError(
                "no holdings in this statement could be matched to a scheme"
            )

        existing = [] if replace else self._raw_portfolio().get("positions", [])
        incoming_codes = {p["scheme_code"] for p in built["positions"]}
        kept = [p for p in existing if str(p.get("scheme_code")) not in incoming_codes]

        saved = self.save_portfolio({"positions": kept + built["positions"]})
        return {
            "imported": len(built["positions"]),
            "kept": len(kept),
            "replaced": len(existing) - len(kept),
            "matched": built["matched"],
            "skipped": built["skipped"],
            "reconciliation": statement_import.reconcile(built["totals"], summary),
            "portfolio": saved,
        }

    def get_portfolio_timeline(self, *, range_key: str = "1y") -> dict[str, Any]:
        """What today's holdings would have been worth through history.

        Units are held constant at what is held now, so this is **not** a
        record of the portfolio's actual past value — the units were not all
        owned for the whole window, and an imported statement carries no
        purchase dates at all. What it does show honestly is how the current
        basket of funds has behaved together, which is the question a
        composition chart cannot answer. The UI labels it as such.

        Only dates where every held fund has a NAV are used, so the line never
        steps down because one scheme was late reporting.
        """
        state = self._raw_portfolio()
        self._load_universe()

        legs: list[tuple[float, dict[str, Any], dict[str, Any] | None]] = []
        for position in state.get("positions", []):
            code = str(position["scheme_code"])
            valued = portfolio.value_position(position, nav_series=self._nav_series(code))
            units = valued.get("units") or 0.0
            if units <= 0:
                continue
            nav = self._nav_series(code)
            if not nav or not nav.get("dates"):
                continue
            legs.append((units, nav, self._by_code.get(code)))

        if not legs:
            return {"range": range_key, "dates": [], "values": [], "invested": None,
                    "series": [], "constant_units": True}

        key = str(range_key or "1y").lower()
        latest = min(nav["dates"][-1] for _, nav, _ in legs)
        if key == "max" or key not in RANGE_DAYS:
            cutoff = max(nav["dates"][0] for _, nav, _ in legs)
        else:
            cutoff = max(
                (date.fromisoformat(latest) - timedelta(days=RANGE_DAYS[key])).isoformat(),
                max(nav["dates"][0] for _, nav, _ in legs),
            )

        lookups = [(units, dict(zip(nav["dates"], nav["navs"])), row) for units, nav, row in legs]
        # Intersect the calendars: a date is usable only if every leg reports.
        common: set[str] | None = None
        for _, lookup, _row in lookups:
            days = {day for day in lookup if cutoff <= day <= latest}
            common = days if common is None else (common & days)
        ordered = sorted(common or [])
        if len(ordered) < 2:
            return {"range": key, "dates": [], "values": [], "invested": None,
                    "series": [], "constant_units": True}

        values = [
            round(sum(units * lookup[day] for units, lookup, _ in lookups), 2)
            for day in ordered
        ]

        # Per-fund contribution, for a stacked view.
        series = []
        for units, lookup, row in lookups:
            series.append({
                "scheme_code": str((row or {}).get("scheme_code") or ""),
                "label": (row or {}).get("name"),
                "values": [round(units * lookup[day], 2) for day in ordered],
            })
        series.sort(key=lambda item: -(item["values"][-1] if item["values"] else 0))

        invested = sum(
            portfolio.value_position(position, nav_series=self._nav_series(str(position["scheme_code"]))).get("invested") or 0.0
            for position in state.get("positions", [])
        )
        return {
            "range": key,
            "available_ranges": _available_ranges(ordered),
            "dates": ordered,
            "values": values,
            "invested": round(invested, 2) or None,
            "series": series,
            "constant_units": True,
            "start_value": values[0],
            "end_value": values[-1],
            "change_pct": round((values[-1] / values[0] - 1) * 100.0, 2) if values[0] else None,
        }

    def get_portfolio_concentration(self) -> dict[str, Any]:
        """Where the portfolio is concentrated, per fund and in aggregate."""
        valued = self.get_portfolio()
        allocation = valued.get("allocation") or {}
        payload = concentration.build(
            valued.get("positions") or [],
            detail_for=self._detail,
            look_through=allocation.get("look_through_top") or [],
        )
        payload["as_of"] = valued.get("as_of")
        return payload

    # ------------------------------------------------------------ sectors

    def _sector_definitions(self) -> list[dict[str, Any]]:
        return [
            {"key": item.key, "name": item.label, "symbol": item.yahoo_symbol}
            for item in benchmarks.SECTOR_BENCHMARKS
            if item.yahoo_symbol
        ]

    def _sector_artifact(self) -> dict[str, Any]:
        """The committed sector-index history, loaded once.

        Yahoo refuses most of these symbols from datacenter IPs, so a live
        fetch on the Space returns three sectors out of sixteen. The artifact
        is what makes the page whole in production; see
        `scripts/build_sector_indices.py`.
        """
        cached = getattr(self, "_sector_blob", None)
        if cached is not None:
            return cached
        path = paths.DATA_DIR / "sector_indices.json"
        try:
            blob = json.loads(path.read_text())
        except (OSError, ValueError):
            blob = {}
        self._sector_blob = blob
        return blob

    def _sector_index_series(self, symbol: str) -> dict[str, Any]:
        """Daily OHLC for one index — live if the feed answers, else shipped.

        Live is preferred so a machine that can reach Yahoo shows today's bar,
        but a refusal falls through to the committed history rather than
        dropping the sector off the page.
        """
        try:
            live = index_source.fetch_index_series(symbol, want_ohlc=True)
            if live.get("dates"):
                return live
        except Exception:
            pass

        for entry in (self._sector_artifact().get("sectors") or {}).values():
            if entry.get("symbol") == symbol:
                daily = entry["daily"]
                return {
                    "symbol": symbol,
                    "dates": daily["dates"],
                    "navs": daily["closes"],
                    "opens": daily["opens"],
                    "highs": daily["highs"],
                    "lows": daily["lows"],
                    "weekly": entry.get("weekly"),
                    "ma30w": daily.get("ma30w"),
                    "from_artifact": True,
                    "is_price_index": True,
                }
        market = self._sector_artifact().get("market") or {}
        if market.get("symbol") == symbol:
            daily = market["daily"]
            return {
                "symbol": symbol,
                "dates": daily["dates"],
                "navs": daily["closes"],
                "opens": daily["opens"],
                "highs": daily["highs"],
                "lows": daily["lows"],
                "from_artifact": True,
                "is_price_index": True,
            }
        raise index_source.IndexUnavailable(f"no live feed or shipped history for {symbol}")

    def _funds_by_sector(self) -> dict[str, list[dict[str, Any]]]:
        """Which funds in the universe track each sector index.

        Reuses `benchmarks.resolve_theme`, the same mapping the fund pages
        already benchmark against — so the funds listed under a sector are
        exactly the ones measured against that sector everywhere else, rather
        than a second, subtly different classification.
        """
        out: dict[str, list[dict[str, Any]]] = {}
        for fund in (self._load_universe().get("funds") or []):
            sub_category = str(fund.get("sub_category") or "")
            if sub_category not in ("Sectoral", "Thematic", "Sectoral / Thematic"):
                continue
            resolved = benchmarks.resolve_theme(fund.get("name"))
            if resolved is None:
                continue
            out.setdefault(resolved.key, []).append({
                "scheme_code": fund.get("scheme_code"),
                "name": fund.get("name"),
                "amc": fund.get("amc"),
                "return_1y": fund.get("return_1y"),
                "return_3y": fund.get("return_3y"),
                "expense_ratio": fund.get("expense_ratio"),
                "aum_crore": fund.get("aum_crore"),
                "percentile_3y": fund.get("percentile_3y"),
            })
        for funds in out.values():
            # Largest first: AUM is the least opinionated ordering available,
            # and ranking them by return here would read as a shortlist.
            funds.sort(key=lambda row: -(row.get("aum_crore") or 0))
        return out

    def get_sector_stages(self) -> dict[str, Any]:
        """Every Nifty sector index, classified by where it sits in its cycle.

        Cached for six hours: the classification moves on weekly closes, so
        recomputing it per request would re-read sixteen index files to produce
        the same answer.
        """
        with self._ai_lock:
            cached = self._ai_cache.get("sector_stages")
            if cached and (time.time() - cached[0]) < 6 * 60 * 60:
                return cached[1]

        try:
            market = self._sector_index_series("^NSEI")
        except Exception:
            market = None

        payload = sector_stages.build(
            self._sector_definitions(),
            series_for=self._sector_index_series,
            market_series=market,
        )
        funds_by_sector = self._funds_by_sector()
        for rows in payload["buckets"].values():
            for row in rows:
                tracking = funds_by_sector.get(row["key"], [])
                row["fund_count"] = len(tracking)
                row["funds"] = tracking[:8]

        with self._ai_lock:
            self._ai_cache["sector_stages"] = (time.time(), payload)
        return payload

    def get_sector_series(self, key: str, *, range_key: str = "3y") -> dict[str, Any] | None:
        """Daily OHLC for one sector index, plus its 30-week average.

        The average is computed on the full history and then windowed, not
        computed on the window — otherwise the first 30 weeks of any range
        would have no average at all.
        """
        definition = next((item for item in self._sector_definitions() if item["key"] == key), None)
        if definition is None:
            return None
        try:
            daily = self._sector_index_series(definition["symbol"])
        except Exception:
            return None
        if not daily.get("dates"):
            return None

        dates = daily["dates"]
        closes = daily["navs"]
        # 30 weeks of trading days, so the daily line carries the same average
        # the weekly classification used. The artifact ships this precomputed
        # over the full history — recomputing it on the stored five-year window
        # would leave the first 150 days of the chart without an average.
        ma = daily.get("ma30w") or sector_stages._sma(closes, sector_stages.MA_WEEKS * 5)

        days = RANGE_DAYS.get(range_key)
        start = 0
        if days:
            cutoff = (date.fromisoformat(dates[-1]) - timedelta(days=days)).isoformat()
            start = next((i for i, value in enumerate(dates) if value >= cutoff), 0)

        window = slice(start, len(dates))
        weekly = sector_stages._to_weekly(dates, closes, daily.get("highs"), daily.get("lows"))
        stage = sector_stages.classify(weekly)

        return {
            "key": key,
            "name": definition["name"],
            "symbol": definition["symbol"],
            "range": range_key,
            "available_ranges": _available_ranges(dates),
            "dates": dates[window],
            "closes": closes[window],
            "opens": (daily.get("opens") or [])[window] if daily.get("opens") else None,
            "highs": (daily.get("highs") or [])[window] if daily.get("highs") else None,
            "lows": (daily.get("lows") or [])[window] if daily.get("lows") else None,
            "ma30w": ma[window],
            "weekly": {
                "dates": weekly["dates"][-260:],
                "opens": [round(v, 2) for v in weekly["opens"][-260:]],
                "closes": [round(v, 2) for v in weekly["closes"][-260:]],
                "highs": [round(v, 2) for v in weekly["highs"][-260:]],
                "lows": [round(v, 2) for v in weekly["lows"][-260:]],
            },
            "stage": stage,
            "is_price_index": True,
        }

    def get_portfolio_overlap(self) -> dict[str, Any]:
        """How much each pair of held funds duplicates the other.

        The multi-fund question `get_portfolio_concentration` does not answer:
        a fund can be perfectly diversified on its own and still be a near-copy
        of the fund beside it.
        """
        valued = self.get_portfolio()
        payload = overlap.build(
            valued.get("positions") or [],
            detail_for=self._detail,
            total_value=(valued.get("totals") or {}).get("current_value"),
        )
        payload["as_of"] = valued.get("as_of")
        return payload

    def get_portfolio_health(self) -> dict[str, Any]:
        """Measured findings about the portfolio as a whole, plus the plot.

        Reports; does not advise. See `portfolio_health.py` for why that line
        is drawn and what it costs to cross it.
        """
        valued = self.get_portfolio()
        universe = self._load_universe()
        payload = portfolio_health.build(
            positions=valued.get("positions") or [],
            allocation=valued.get("allocation") or {},
            totals=valued.get("totals") or {},
            all_funds=universe.get("funds") or [],
            overlap_payload=self.get_portfolio_overlap(),
        )
        payload["as_of"] = valued.get("as_of")
        return payload

    def get_portfolio_peer_comparison(self) -> dict[str, Any]:
        """For every fund held: which funds in its category measured better.

        Purely factual — "these funds returned more over three years while
        charging less and falling less" is an observation, not a suggestion to
        move money. Deterministic, so it renders with or without AI.
        """
        state = self._raw_portfolio()
        universe = self._load_universe()
        all_funds = universe.get("funds") or []

        holdings: list[dict[str, Any]] = []
        # Only what is still held. A fund that was sold carries no ongoing
        # decision, and including it pads the comparison with rows that cannot
        # be acted on.
        valued_by_code = {
            str(item["scheme_code"]): item
            for item in (self.get_portfolio().get("positions") or [])
        }
        for position in state.get("positions", []):
            code = str(position["scheme_code"])
            if (valued_by_code.get(code, {}).get("units") or 0) <= 0:
                continue
            row = self._by_code.get(code)
            if row is None:
                holdings.append({
                    "scheme_code": code,
                    "name": None,
                    "in_universe": False,
                    "peers_ahead": [],
                })
                continue

            sub_category = str(row.get("sub_category") or "")
            peers = [
                other for other in all_funds
                if str(other.get("sub_category") or "") == sub_category
            ]
            review = fund_review.build_review(
                row,
                peers,
                category_summary=(universe.get("categories") or {}).get(sub_category),
            )
            # Widen the peer list here relative to the single-fund review: on a
            # portfolio screen the useful question is "how many in this category
            # did better", so a truncated five would understate it.
            ahead = fund_review.peers_ahead(row, peers, limit=10)
            better_on_return = [
                other for other in peers
                if isinstance(other.get("return_3y"), (int, float))
                and isinstance(row.get("return_3y"), (int, float))
                and other["return_3y"] > row["return_3y"]
            ]
            holdings.append({
                "scheme_code": code,
                "name": row.get("name"),
                "amc": row.get("amc"),
                "sub_category": sub_category,
                "in_universe": True,
                "return_1y": row.get("return_1y"),
                "return_3y": row.get("return_3y"),
                "return_5y": row.get("return_5y"),
                "expense_ratio": row.get("expense_ratio"),
                "max_drawdown": row.get("max_drawdown"),
                "rank_3y": row.get("rank_3y"),
                "rank_count_3y": row.get("rank_count_3y"),
                "quartile_3y": row.get("quartile_3y"),
                "measured_standing": review["measured_standing"],
                "trajectory": review["rank_trajectory"]["direction"],
                "category_avg_3y": (review.get("category_summary") or {}).get("return_3y"),
                "peer_count": len(peers),
                "better_on_3y_count": len(better_on_return),
                "peers_ahead": ahead,
            })

        holdings.sort(key=lambda item: (item.get("measured_standing") is None,
                                        item.get("measured_standing") or 0))
        return {
            "as_of": universe.get("as_of"),
            "holdings": holdings,
            "holding_count": len(holdings),
        }

    def get_portfolio_ai_comparison(self) -> dict[str, Any]:
        """Prose over the peer comparison. Reports, does not advise."""
        comparison = self.get_portfolio_peer_comparison()
        if not comparison["holdings"]:
            return {"available": False, "reason": "No funds in the portfolio yet."}

        service = self._ai_service
        if service is None or not getattr(service, "available", False):
            return {
                "available": False,
                "reason": "AI is not configured on this deployment — the comparison table above "
                          "is unaffected.",
            }

        cache_key = "portfolio:" + ",".join(
            sorted(str(h["scheme_code"]) for h in comparison["holdings"])
        )
        with self._ai_lock:
            cached = self._ai_cache.get(cache_key)
            if cached and (time.time() - cached[0]) < 12 * 60 * 60:
                return cached[1]

        evidence = {
            "as_of": comparison["as_of"],
            "holdings": [
                {
                    "fund": h["name"],
                    "category": h["sub_category"],
                    "return_3y": h["return_3y"],
                    "category_average_3y": h["category_avg_3y"],
                    "rank_in_category_3y": (
                        f"{h['rank_3y']} of {h['rank_count_3y']}"
                        if h.get("rank_3y") and h.get("rank_count_3y") else None
                    ),
                    "measured_standing_percentile": h["measured_standing"],
                    "category_standing_trajectory": h["trajectory"],
                    "expense_ratio": h["expense_ratio"],
                    "worst_fall": h["max_drawdown"],
                    "funds_in_category_with_higher_3y_return": h["better_on_3y_count"],
                    "funds_better_on_return_cost_and_drawdown_together": [
                        {
                            "fund": p["name"],
                            "return_3y": p["return_3y"],
                            "return_ahead_by": p["return_gap"],
                            "expense_ratio": p["expense_ratio"],
                            "worst_fall": p["max_drawdown"],
                        }
                        for p in h["peers_ahead"]
                    ],
                }
                for h in comparison["holdings"] if h["in_universe"]
            ],
        }
        try:
            note = service.generate_portfolio_comparison_note(
                json.dumps(evidence, separators=(",", ":"))
            )
        except Exception as exc:
            return {"available": False, "reason": f"AI note unavailable ({type(exc).__name__})."}

        payload = {"available": True, "note": note}
        with self._ai_lock:
            self._ai_cache[cache_key] = (time.time(), payload)
        return payload

    def get_fund_ai_review(self, scheme_code: str) -> dict[str, Any]:
        """Prose over the measured review. Never raises — the numbers stand alone.

        Cached for a day per fund: the underlying evidence only moves when the
        nightly rebuild lands, so re-generating on every open would burn quota
        to restate the same paragraphs.
        """
        review = self.get_fund_review(scheme_code)
        if review is None:
            return {"available": False, "reason": "unknown scheme code"}

        service = self._ai_service
        if service is None or not getattr(service, "available", False):
            return {
                "available": False,
                "reason": "AI is not configured on this deployment — the measured scorecard above "
                          "is unaffected.",
            }

        key = str(scheme_code)
        with self._ai_lock:
            cached = self._ai_cache.get(key)
            if cached and (time.time() - cached[0]) < 24 * 60 * 60:
                return cached[1]

        # Hand over the evidence only — the prompt forbids deriving figures, so
        # anything not in here cannot legitimately appear in the prose.
        evidence = {
            "fund": review["name"],
            "category": review["sub_category"],
            "benchmark": review["benchmark_label"],
            "peers_in_category": review["peer_count"],
            "measured_standing_percentile": review["measured_standing"],
            "scorecard": [
                {
                    "dimension": item["label"],
                    "value": item["value"],
                    "unit": item["unit"],
                    "category_median": item["category_median"],
                    "percentile_in_category": item["percentile"],
                    "standing": item["standing"],
                }
                for item in review["scorecard"]
            ],
            "rank_trajectory": review["rank_trajectory"],
            "observations": review["signals"],
            "same_category_funds_better_on_return_cost_and_downside": review["peers_ahead"],
        }
        try:
            note = service.generate_fund_review_note(
                json.dumps(evidence, separators=(",", ":")), str(review["name"])
            )
        except Exception as exc:
            return {"available": False, "reason": f"AI note unavailable ({type(exc).__name__})."}

        payload = {"available": True, "note": note, "generated_for": review["name"]}
        with self._ai_lock:
            self._ai_cache[key] = (time.time(), payload)
        return payload

    def get_portfolio_ai_health(self) -> dict[str, Any]:
        """Prose over the measured portfolio findings. Never raises.

        Cached for an hour against a fingerprint of the findings themselves, so
        editing a holding regenerates it but re-opening the page does not.
        """
        health = self.get_portfolio_health()
        if not health.get("available"):
            return {"available": False, "reason": health.get("reason") or "nothing to measure yet"}

        service = self._ai_service
        if service is None or not getattr(service, "available", False):
            return {
                "available": False,
                "reason": "AI is not configured on this deployment — the measured findings above "
                          "are unaffected.",
            }

        fingerprint = "health:" + str(hash(json.dumps(
            [[f["key"], f["tone"], f["metric"]] for f in health["findings"]], sort_keys=True
        )))
        with self._ai_lock:
            cached = self._ai_cache.get(fingerprint)
            if cached and (time.time() - cached[0]) < 60 * 60:
                return cached[1]

        # Hand over the findings only. The prompt forbids deriving figures, so
        # anything not in here cannot legitimately appear in the prose.
        evidence = {
            "fund_count": health["fund_count"],
            "findings": [
                {
                    "topic": item["key"],
                    "tone": item["tone"],
                    "measured": item["headline"],
                    "detail": item["detail"],
                    "metric": item["metric"],
                }
                for item in health["findings"]
            ],
            "positioning": [
                {
                    "fund": point["name"],
                    "category": point["category"],
                    "cost_vs_category_median_pct": point["cost_gap"],
                    "return_3y_vs_category_median_pct": point["return_gap"],
                    "weight_pct": point["weight_pct"],
                }
                for point in (health.get("chart") or {}).get("points") or []
            ],
        }
        try:
            note = service.generate_portfolio_health_note(
                json.dumps(evidence, separators=(",", ":"))
            )
        except Exception as exc:
            return {"available": False, "reason": f"AI note unavailable ({type(exc).__name__})."}

        payload = {"available": True, "note": note}
        with self._ai_lock:
            self._ai_cache[fingerprint] = (time.time(), payload)
        return payload

    def expand_sip(self, **kwargs) -> list[dict[str, Any]]:
        return portfolio.expand_sip(**kwargs)

    def opening_position(self, **kwargs) -> list[dict[str, Any]]:
        return portfolio.opening_position(**kwargs)


def _match_key(name: str | None) -> str:
    """Loose key for pairing a plan variant with its Growth sibling.

    "&" and "and" are used interchangeably in scheme names ("Tata Banking &
    Financial Services" vs "...and Financial Services"), so they have to fold
    together or the sibling is missed.
    """
    text = str(name or "").lower().replace("&", " and ")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def _rebase(values: list[float]) -> list[float]:
    """Growth of 100 from the first point of the window."""
    if not values or values[0] <= 0:
        return []
    base = values[0]
    return [round(value / base * 100.0, 3) for value in values]


def _available_ranges(dates: list[str]) -> list[str]:
    """Only offer a range the fund actually has history for."""
    if len(dates) < 2:
        return ["max"]
    span_days = (date.fromisoformat(dates[-1]) - date.fromisoformat(dates[0])).days
    out = [key for key, days in RANGE_DAYS.items() if days <= span_days + 40]
    out.append("max")
    return out
