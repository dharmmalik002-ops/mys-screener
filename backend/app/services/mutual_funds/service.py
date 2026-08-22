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
import threading
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import benchmarks, groww_source, index_source, metrics, nav_source, paths, portfolio
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
    def __init__(self, *, database_url: str | None = None, state_dir: Path | None = None) -> None:
        paths.ensure_dirs()
        self._state_dir = Path(state_dir) if state_dir else None
        self._store = portfolio.MutualFundPortfolioStore(database_url)

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

    def get_portfolio(self) -> dict[str, Any]:
        """Valued portfolio: per-position units, cost, value, gain and XIRR."""
        state = self._raw_portfolio()
        self._load_universe()

        positions: list[dict[str, Any]] = []
        all_cashflows: list[tuple[date, float]] = []
        total_invested = 0.0
        total_value = 0.0
        total_realised = 0.0

        for position in state.get("positions", []):
            code = str(position["scheme_code"])
            row = self._by_code.get(code)
            nav = self._nav_series(code)
            valued = portfolio.value_position(position, nav_series=nav)
            valued["fund"] = row
            valued["transactions"] = position.get("transactions") or []
            positions.append(valued)

            total_invested += valued.get("invested") or 0.0
            total_value += valued.get("current_value") or 0.0
            total_realised += valued.get("realised") or 0.0
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

        gain = total_value + total_realised - total_invested if total_invested else None
        return {
            "updated_at": state.get("updated_at"),
            "as_of": max(valuation_dates) if valuation_dates else None,
            "positions": positions,
            "totals": {
                "position_count": len(positions),
                "invested": round(total_invested, 2) if total_invested else None,
                "current_value": round(total_value, 2) if total_value else None,
                "realised": round(total_realised, 2) if total_realised else None,
                "gain": round(gain, 2) if gain is not None else None,
                "gain_pct": round(gain / total_invested * 100.0, 2) if gain is not None and total_invested > 0 else None,
                "xirr": portfolio_xirr,
            },
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

    def expand_sip(self, **kwargs) -> list[dict[str, Any]]:
        return portfolio.expand_sip(**kwargs)


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
