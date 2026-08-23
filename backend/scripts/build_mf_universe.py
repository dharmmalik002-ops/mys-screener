#!/usr/bin/env python3
"""Build the mutual-fund screener universe.

Four phases, each independently re-runnable, because the crawl is the slow
part and you rarely want to redo it just to change a metric:

    1. slugs    — read every Direct/Growth scheme from Groww's sitemap
    2. details  — fetch each fund page, cache the detail blob, keep the row
    3. navs     — fetch each fund's full AMFI NAV history from mfapi.in
    4. compute  — derive returns/risk/ranks from NAV, write mf_universe.json

Phase 2 and 3 skip anything already cached and still fresh, so an interrupted
run resumes for free. Only phase 4's output (`mf_universe.json`) is tracked in
git; the per-fund caches are gitignored and rebuild on demand.

    cd backend
    python scripts/build_mf_universe.py                  # full build
    python scripts/build_mf_universe.py --limit 40       # quick sample
    python scripts/build_mf_universe.py --compute-only   # re-derive metrics
    python scripts/build_mf_universe.py --refresh-navs   # force NAV refetch
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.mutual_funds import benchmarks, groww_source, index_source, metrics, nav_source, paths
from app.services.mutual_funds.harvest import _strip_amc, detail_blob, universe_row

REFERENCE_PATH = paths.DATA_DIR / "mf_reference.json"

_print_lock = threading.Lock()


def log(message: str) -> None:
    with _print_lock:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def _write_json(path: Path, payload: object) -> None:
    """Write via a temp file so an interrupted run never leaves a half-written
    cache entry that later parses as valid-but-truncated JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, separators=(",", ":")))
    temp.replace(path)


def _read_json(path: Path) -> object | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def _is_fresh(path: Path, ttl_seconds: int) -> bool:
    try:
        return (time.time() - path.stat().st_mtime) < ttl_seconds
    except OSError:
        return False


# ---------------------------------------------------------------- phase 1-2

def phase_details(slugs: list[str], *, workers: int, refresh: bool, delay: float) -> list[dict]:
    """Fetch and cache each fund's reference + detail data."""
    rows: list[dict] = []
    failures: list[str] = []
    skipped_plans: list[str] = []
    done = 0
    total = len(slugs)

    def work(slug: str) -> tuple[str, dict | None]:
        # Polite pacing: the crawl is spread across `workers` threads, so a
        # per-request sleep keeps the aggregate rate civil rather than
        # hammering 2,000 pages as fast as the pool allows.
        if delay > 0:
            time.sleep(delay)
        try:
            data = groww_source.fetch_scheme_with_retry(slug)
        except groww_source.GrowwUnavailable as exc:
            return slug, {"error": str(exc)}
        row = universe_row(data)
        if not row.get("scheme_code"):
            return slug, {"error": "no scheme_code"}
        # Authoritative Direct/Growth gate — see groww_source.list_scheme_slugs
        # for why this is not done on the slug.
        if not groww_source.is_direct_growth(row):
            return slug, {"skip": f"{row.get('plan')}/{row.get('option')}"}
        _write_json(paths.detail_path(row["scheme_code"]), detail_blob(data))
        return slug, row

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(work, slug): slug for slug in slugs}
        for future in as_completed(futures):
            slug, result = future.result()
            done += 1
            if result is None or "error" in result:
                failures.append(slug)
            elif "skip" in result:
                skipped_plans.append(slug)
            else:
                rows.append(result)
            if done % 100 == 0 or done == total:
                log(f"  details {done}/{total}  ok={len(rows)} other-plan={len(skipped_plans)} failed={len(failures)}")

    if skipped_plans:
        log(f"  {len(skipped_plans)} pages were not Direct/Growth (Regular or IDCW) — excluded")
    if failures:
        log(f"  {len(failures)} pages unreadable (skipped): {failures[:5]}")
    _write_json(REFERENCE_PATH, {"generated_at": datetime.now(timezone.utc).isoformat(), "rows": rows})
    return rows


# ------------------------------------------------------------------ phase 3

def phase_navs(scheme_codes: list[str], *, workers: int, refresh: bool) -> dict[str, dict]:
    """Fetch and cache NAV history for every scheme, plus every benchmark
    index fund the categories resolve to."""
    wanted = list(dict.fromkeys(scheme_codes))
    for bench in benchmarks.ALL_BENCHMARKS.values():
        if bench.source == "mf" and bench.scheme_code and bench.scheme_code not in wanted:
            wanted.append(bench.scheme_code)

    series: dict[str, dict] = {}
    failures: list[str] = []
    done = 0
    total = len(wanted)

    def work(code: str) -> tuple[str, dict | None]:
        path = paths.nav_path(code)
        if not refresh and _is_fresh(path, paths.NAV_TTL_SECONDS):
            cached = _read_json(path)
            if isinstance(cached, dict) and cached.get("dates"):
                return code, cached
        try:
            payload = nav_source.fetch_nav_history(code)
        except nav_source.NavUnavailable:
            # Fall back to a stale cache rather than dropping the fund — a
            # yesterday-fresh NAV series is still a usable screener row.
            cached = _read_json(path)
            return code, cached if isinstance(cached, dict) and cached.get("dates") else None
        _write_json(path, payload)
        return code, payload

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(work, code): code for code in wanted}
        for future in as_completed(futures):
            code, payload = future.result()
            done += 1
            if payload is None:
                failures.append(code)
            else:
                series[code] = payload
            if done % 100 == 0 or done == total:
                log(f"  navs {done}/{total}  ok={len(series)} failed={len(failures)}")

    if failures:
        log(f"  {len(failures)} NAV series unavailable: {failures[:5]}")
    return series


# ----------------------------------------------------------------- phase 3b

def phase_benchmarks(series: dict[str, dict]) -> dict[str, dict]:
    """One usable series per benchmark key.

    Index-fund NAVs come from the NAV cache already fetched in phase 3; price
    indices come from Yahoo. Without this, alpha/beta would be blank for every
    Large Cap, Flexi Cap, Thematic and ELSS fund — i.e. most of the universe —
    because those categories map to price indices rather than index funds.
    """
    out: dict[str, dict] = {}
    for key, bench in benchmarks.ALL_BENCHMARKS.items():
        payload: dict | None = None
        kind = "unavailable"
        if bench.source == "mf" and bench.scheme_code:
            payload = series.get(bench.scheme_code)
            kind = "index_fund_nav" if payload else "unavailable"
        if payload is None:
            symbol = bench.yahoo_symbol or bench.fallback_yahoo_symbol
            if symbol:
                try:
                    # force=True: the build is what keeps the serving
                    # cache warm, so it must not read its own stale copy.
                    payload = index_source.fetch_index_series(symbol, force=True)
                    kind = "price_index"
                except index_source.IndexUnavailable as exc:
                    log(f"  {key}: {exc}")
        if payload and payload.get("dates"):
            out[key] = {**payload, "_kind": kind}
            log(f"  {key}: {len(payload['dates'])} points via {kind}")
    return out


# ------------------------------------------------------------------ phase 4

def _cached_dominant_sector(scheme_code: str) -> str | None:
    """Largest equity sector from the cached holdings blob, if it dominates.

    Only consulted for a themed fund whose name does not state its theme.
    """
    payload = _read_json(paths.detail_path(str(scheme_code)))
    if not isinstance(payload, dict):
        return None
    allocation = payload.get("sector_allocation")
    if not isinstance(allocation, dict) or not allocation:
        return None
    sector, share = max(allocation.items(), key=lambda kv: kv[1] or 0)
    return sector if (share or 0) >= 33.0 else None



# Windows the screener ranks on. Ranking every window would triple the file
# for columns nobody sorts by.
RANKED_WINDOWS = ("1m", "3m", "6m", "1y", "3y", "5y", "10y")

# The screener compares funds on total return, risk-adjusted return and equity
# holdings — the columns that make a debt or commodity fund comparable are
# different ones entirely (YTM, modified duration, credit quality), and none of
# them are in this data. Rather than ship 470 funds with half-empty rows, the
# universe is scoped to what the page can actually screen. Widen this set if
# debt metrics ever get their own columns.
INCLUDED_CATEGORIES = {"Equity", "Hybrid"}


def phase_compute(rows: list[dict], series: dict[str, dict], bench_map: dict[str, dict]) -> dict:
    """Derive every performance number and category rank from NAV."""
    enriched: list[dict] = []
    skipped = 0
    out_of_scope = 0

    for row in rows:
        if str(row.get("category") or "").strip() not in INCLUDED_CATEGORIES:
            out_of_scope += 1
            continue

        # Re-resolve the benchmark here rather than trusting what the crawl
        # froze into the reference row. The sub-category -> index mapping is
        # derived data that changes when the mapping code changes, and having
        # to re-crawl 1,650 pages to pick up a mapping fix (as happened when
        # sector benchmarks were added) is the wrong dependency.
        resolved = benchmarks.resolve(
            row.get("sub_category"),
            category=row.get("category"),
            name=_strip_amc(row.get("name"), row.get("amc")),
            dominant_sector=_cached_dominant_sector(row["scheme_code"]),
        )
        row = {
            **row,
            "benchmark_key": resolved.key,
            "benchmark_label": resolved.label,
            "benchmark_is_reference_only": resolved.is_reference_only,
        }
        code = row["scheme_code"]
        nav = series.get(code)
        if not nav or not nav.get("dates"):
            skipped += 1
            continue
        dates = nav["dates"]
        navs = nav["navs"]

        returns = metrics.point_to_point_returns(dates, navs)
        volatility = metrics.volatility_pct(navs)
        cagr_3y = returns.get("3y")
        # Sharpe/Sortino are quoted on the 3-year CAGR where it exists and
        # since-inception otherwise, so a 2-year-old fund still gets a number
        # instead of a blank column.
        inception_cagr = metrics.since_inception_cagr(dates, navs)
        risk_basis = cagr_3y if cagr_3y is not None else inception_cagr

        out = dict(row)
        out.update({
            "nav_first_date": dates[0],
            "nav_last_date": dates[-1],
            "nav_points": len(dates),
            "nav_latest": navs[-1],
            "age_years": round((metrics._parse(dates[-1]) - metrics._parse(dates[0])).days / 365.25, 2),
            "cagr_inception": inception_cagr,
            "volatility": volatility,
            "max_drawdown": metrics.max_drawdown_pct(navs),
            "current_drawdown": metrics.current_drawdown_pct(navs),
            "sharpe": metrics.sharpe_ratio(risk_basis, volatility),
            "sortino": metrics.sortino_ratio(navs, risk_basis),
        })
        for label, value in returns.items():
            out[f"return_{label}"] = value

        # Rolling 3y/5y — the honest view of a fund's consistency, and the
        # thing a point-to-point table cannot show.
        for window in (3, 5):
            rolling = metrics.rolling_returns(dates, navs, window_years=window, step_days=14)
            out[f"rolling{window}y_median"] = rolling["median"]
            out[f"rolling{window}y_min"] = rolling["min"]
            out[f"rolling{window}y_max"] = rolling["max"]
            out[f"rolling{window}y_pct_negative"] = rolling["pct_negative"]
            out[f"rolling{window}y_count"] = rolling["count"]

        # Alpha / beta / capture against the fund's mapped benchmark.
        bench = benchmarks.ALL_BENCHMARKS.get(row["benchmark_key"]) or benchmarks.resolve(row.get("sub_category"))
        bench_series = bench_map.get(bench.key)
        if bench_series and bench_series.get("dates"):
            _, fund_leg, bench_leg = metrics.align_series(
                dates, navs, bench_series["dates"], bench_series["navs"]
            )
            if len(fund_leg) >= 120:
                bench_returns = metrics.point_to_point_returns(bench_series["dates"], bench_series["navs"])
                stats = metrics.beta_alpha(
                    fund_leg,
                    bench_leg,
                    fund_cagr_pct=cagr_3y,
                    bench_cagr_pct=bench_returns.get("3y"),
                )
                out.update({
                    # A price-index benchmark excludes dividends, so alpha
                    # measured against one is flattered by roughly 1.2% a
                    # year. Flagged rather than silently corrected.
                    "alpha_vs_price_index": bench_series.get("_kind") == "price_index",
                    "benchmark_source_kind": bench_series.get("_kind"),
                    "beta": stats["beta"],
                    "alpha": stats["alpha"],
                    "tracking_error": stats["tracking_error"],
                    "information_ratio": stats["information_ratio"],
                    "up_capture": stats["up_capture"],
                    "down_capture": stats["down_capture"],
                })
        enriched.append(out)

    if out_of_scope:
        log(f"  {out_of_scope} funds outside {sorted(INCLUDED_CATEGORIES)} excluded from the screener")
    if skipped:
        log(f"  {skipped} funds dropped for having no NAV series")

    # Ranks are computed here rather than shipped from the source, so a rank
    # always agrees with the return column next to it.
    for window in RANKED_WINDOWS:
        metrics.assign_ranks(
            enriched,
            value_key=f"return_{window}",
            group_key="sub_category",
            rank_key=f"rank_{window}",
            count_key=f"rank_count_{window}",
            percentile_key=f"percentile_{window}",
            quartile_key=f"quartile_{window}" if window in ("1y", "3y", "5y") else None,
        )

    value_keys = [f"return_{w}" for w in RANKED_WINDOWS] + [
        "expense_ratio", "aum_crore", "volatility", "max_drawdown", "sharpe", "cagr_inception",
    ]
    categories = metrics.category_averages(enriched, group_key="sub_category", value_keys=value_keys)

    def rounded(value):
        return round(value, 4) if isinstance(value, float) else value

    enriched = [{key: rounded(value) for key, value in row.items()} for row in enriched]
    for summary in categories.values():
        for key, value in list(summary.items()):
            summary[key] = rounded(value)

    nav_dates = [row["nav_last_date"] for row in enriched if row.get("nav_last_date")]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of": max(nav_dates) if nav_dates else None,
        "fund_count": len(enriched),
        "ranked_windows": list(RANKED_WINDOWS),
        "categories": categories,
        "benchmarks": {
            key: {
                "label": bench.label,
                "source": bench.source,
                "scheme_code": bench.scheme_code,
                "yahoo_symbol": bench.yahoo_symbol,
                "total_return": bench.total_return,
                "is_reference_only": bench.is_reference_only,
                "notes": bench.notes,
            }
            for key, bench in benchmarks.ALL_BENCHMARKS.items()
        },
        "funds": enriched,
    }


# ----------------------------------------------------------------- entrypoint

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0, help="only crawl the first N funds (testing)")
    parser.add_argument("--workers", type=int, default=6, help="concurrent page fetches (default 6)")
    parser.add_argument("--nav-workers", type=int, default=10, help="concurrent NAV fetches (default 10)")
    parser.add_argument("--delay", type=float, default=0.15, help="per-request pause, seconds")
    parser.add_argument("--compute-only", action="store_true", help="reuse cached reference rows + NAVs")
    parser.add_argument("--refresh-navs", action="store_true", help="refetch NAV even if cached")
    args = parser.parse_args()

    paths.ensure_dirs()
    started = time.time()

    if args.compute_only:
        cached = _read_json(REFERENCE_PATH)
        rows = (cached or {}).get("rows") or []
        if not rows:
            log("no cached reference rows — run without --compute-only first")
            return 1
        log(f"phase 1-2 skipped, {len(rows)} cached reference rows")
    else:
        log("phase 1: reading sitemap")
        slugs = groww_source.list_scheme_slugs()
        if args.limit:
            slugs = slugs[: args.limit]
        log(f"  {len(slugs)} direct-growth schemes")
        log(f"phase 2: fetching fund pages ({args.workers} workers)")
        rows = phase_details(slugs, workers=args.workers, refresh=False, delay=args.delay)
        log(f"  {len(rows)} reference rows")

    log(f"phase 3: fetching NAV history ({args.nav_workers} workers)")
    series = phase_navs(
        [row["scheme_code"] for row in rows],
        workers=args.nav_workers,
        refresh=args.refresh_navs,
    )

    log("phase 3b: fetching benchmark index series")
    bench_series = phase_benchmarks(series)

    log("phase 4: computing returns, risk and category ranks")
    universe = phase_compute(rows, series, bench_series)
    _write_json(paths.UNIVERSE_PATH, universe)

    size_mb = paths.UNIVERSE_PATH.stat().st_size / 1_048_576
    log(
        f"done in {time.time() - started:.0f}s — {universe['fund_count']} funds, "
        f"{len(universe['categories'])} categories, as_of {universe['as_of']}, "
        f"{size_mb:.2f} MB -> {paths.UNIVERSE_PATH}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
