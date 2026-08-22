"""Everything numeric on the Funds page, computed from AMFI NAV.

Deliberately pure-Python (no pandas): the screener holds ~1,600 series and
the HF Space runs on 16 GB shared with the equity snapshot cache, so building
a DataFrame per fund is the wrong trade. Every function here takes two
parallel arrays (`dates`, `navs`, chronological) and returns plain floats.

Two rules the whole page rests on:

* **Nothing here is predictive.** These are measured historical numbers.
* **A number is None rather than wrong.** A fund launched 14 months ago has
  no 3-year return; a gap-riddled series has no 1-year return. Callers render
  a dash, they never render a misleading zero.
"""

from __future__ import annotations

import math
from bisect import bisect_right
from datetime import date, timedelta

# Windows the screener exposes as sortable columns. Anything >= 1y is
# annualised (CAGR); shorter windows stay absolute — annualising a 1-month
# move produces the "this fund returns 400% a year" nonsense seen on some
# retail sites.
RETURN_WINDOWS: tuple[tuple[str, int], ...] = (
    ("1d", 1),
    ("1w", 7),
    ("1m", 30),
    ("3m", 91),
    ("6m", 182),
    ("1y", 365),
    ("2y", 730),
    ("3y", 1095),
    ("5y", 1826),
    ("7y", 2556),
    ("10y", 3652),
)

ANNUALISE_FROM_DAYS = 365
TRADING_DAYS_PER_YEAR = 252
# 6.5% — the ~1y Indian T-bill yield. Used only for Sharpe/Sortino, and the
# same value is applied to every fund, so it shifts the whole column and never
# changes the ordering.
RISK_FREE_RATE = 0.065


def _parse(day: str) -> date:
    return date.fromisoformat(day)


def _as_of_index(dates: list[date], target: date, *, tolerance_days: int) -> int | None:
    """Index of the last NAV on or before `target`, if close enough.

    NAV series skip weekends, market holidays and the odd AMFI outage, so an
    exact date lookup fails constantly. Walking back to the previous
    observation is correct; walking back *months* is not, which is what the
    tolerance guards — a fund that stopped reporting must not silently report
    a stale return.
    """
    position = bisect_right(dates, target) - 1
    if position < 0:
        return None
    if (target - dates[position]).days > tolerance_days:
        return None
    return position


def _tolerance_for(days: int) -> int:
    # A week covers any holiday cluster for short windows; long windows get
    # more slack because the anchor may land in a Diwali/new-year stretch.
    if days <= 7:
        return 5
    if days <= 91:
        return 10
    return 21


def annualise(total_growth: float, days: int) -> float | None:
    """Convert a growth multiple over `days` into a CAGR percentage."""
    if total_growth <= 0 or days <= 0:
        return None
    years = days / ANNUALISE_FROM_DAYS
    if years <= 0:
        return None
    try:
        return (total_growth ** (1.0 / years) - 1.0) * 100.0
    except (OverflowError, ValueError):
        return None


def point_to_point_returns(
    dates: list[str] | list[date],
    navs: list[float],
) -> dict[str, float | None]:
    """Return percentages for every window in RETURN_WINDOWS.

    Windows of a year or more are annualised; shorter ones are absolute.
    """
    if not navs:
        return {label: None for label, _ in RETURN_WINDOWS}
    parsed = [d if isinstance(d, date) else _parse(d) for d in dates]
    latest_date = parsed[-1]
    latest_nav = navs[-1]
    out: dict[str, float | None] = {}
    for label, days in RETURN_WINDOWS:
        anchor = _as_of_index(
            parsed,
            latest_date - timedelta(days=days),
            tolerance_days=_tolerance_for(days),
        )
        if anchor is None or navs[anchor] <= 0:
            out[label] = None
            continue
        growth = latest_nav / navs[anchor]
        elapsed = (latest_date - parsed[anchor]).days
        if days >= ANNUALISE_FROM_DAYS:
            out[label] = annualise(growth, elapsed)
        else:
            out[label] = (growth - 1.0) * 100.0
    return out


def since_inception_cagr(
    dates: list[str] | list[date],
    navs: list[float],
) -> float | None:
    if len(navs) < 2:
        return None
    parsed = [d if isinstance(d, date) else _parse(d) for d in dates]
    elapsed = (parsed[-1] - parsed[0]).days
    if elapsed < 180 or navs[0] <= 0:
        return None
    return annualise(navs[-1] / navs[0], elapsed)


def daily_log_returns(navs: list[float]) -> list[float]:
    out: list[float] = []
    for previous, current in zip(navs, navs[1:]):
        if previous > 0 and current > 0:
            out.append(math.log(current / previous))
    return out


def volatility_pct(navs: list[float]) -> float | None:
    """Annualised standard deviation of daily returns, in percent."""
    series = daily_log_returns(navs)
    if len(series) < 30:
        return None
    mean = sum(series) / len(series)
    variance = sum((value - mean) ** 2 for value in series) / (len(series) - 1)
    return math.sqrt(variance) * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0


def max_drawdown_pct(navs: list[float]) -> float | None:
    """Worst peak-to-trough fall, as a negative percentage."""
    if len(navs) < 2:
        return None
    peak = navs[0]
    worst = 0.0
    for nav in navs:
        if nav > peak:
            peak = nav
        if peak > 0:
            drawdown = (nav / peak - 1.0) * 100.0
            if drawdown < worst:
                worst = drawdown
    return worst


def current_drawdown_pct(navs: list[float]) -> float | None:
    """How far below its all-time-high NAV the fund sits right now."""
    if not navs:
        return None
    peak = max(navs)
    if peak <= 0:
        return None
    return (navs[-1] / peak - 1.0) * 100.0


def sharpe_ratio(cagr_pct: float | None, vol_pct: float | None) -> float | None:
    if cagr_pct is None or vol_pct is None or vol_pct <= 0:
        return None
    return (cagr_pct - RISK_FREE_RATE * 100.0) / vol_pct


def sortino_ratio(navs: list[float], cagr_pct: float | None) -> float | None:
    """Like Sharpe but penalising only downside deviation."""
    if cagr_pct is None:
        return None
    series = daily_log_returns(navs)
    if len(series) < 30:
        return None
    downside = [value for value in series if value < 0]
    if len(downside) < 10:
        return None
    downside_variance = sum(value ** 2 for value in downside) / len(downside)
    downside_vol = math.sqrt(downside_variance) * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0
    if downside_vol <= 0:
        return None
    return (cagr_pct - RISK_FREE_RATE * 100.0) / downside_vol


def rolling_returns(
    dates: list[str] | list[date],
    navs: list[float],
    *,
    window_years: int,
    step_days: int = 7,
) -> dict[str, float | int | None]:
    """Distribution of every `window_years` holding period in the fund's life.

    Point-to-point returns are an accident of the start date — a small-cap
    fund measured from March 2020 looks superb and from January 2018 looks
    poor. Rolling returns are the honest version: how did *every* N-year
    holding period actually turn out. `pct_negative` is the number that
    matters most for the user's 10-session horizon mindset applied to funds.
    """
    parsed = [d if isinstance(d, date) else _parse(d) for d in dates]
    if len(parsed) < 2:
        return {"count": 0, "min": None, "p25": None, "median": None, "avg": None, "max": None, "pct_negative": None}

    window_days = int(window_years * ANNUALISE_FROM_DAYS)
    samples: list[float] = []
    end_index = len(parsed) - 1
    cursor = end_index
    while cursor >= 0:
        end_date = parsed[cursor]
        start_index = _as_of_index(
            parsed,
            end_date - timedelta(days=window_days),
            tolerance_days=_tolerance_for(window_days),
        )
        if start_index is None:
            break
        if navs[start_index] > 0:
            elapsed = (end_date - parsed[start_index]).days
            annualised = annualise(navs[cursor] / navs[start_index], elapsed)
            if annualised is not None:
                samples.append(annualised)
        # Step back roughly `step_days` calendar days rather than a fixed
        # number of rows, so a fund with feed gaps is not over-sampled.
        next_cursor = _as_of_index(
            parsed,
            parsed[cursor] - timedelta(days=step_days),
            tolerance_days=step_days + 7,
        )
        if next_cursor is None or next_cursor >= cursor:
            break
        cursor = next_cursor

    if not samples:
        return {"count": 0, "min": None, "p25": None, "median": None, "avg": None, "max": None, "pct_negative": None}

    samples.sort()
    count = len(samples)

    def quantile(fraction: float) -> float:
        position = fraction * (count - 1)
        low = int(math.floor(position))
        high = min(low + 1, count - 1)
        weight = position - low
        return samples[low] * (1 - weight) + samples[high] * weight

    negatives = sum(1 for value in samples if value < 0)
    return {
        "count": count,
        "min": samples[0],
        "p25": quantile(0.25),
        "median": quantile(0.5),
        "avg": sum(samples) / count,
        "max": samples[-1],
        "pct_negative": negatives / count * 100.0,
    }


def align_series(
    fund_dates: list[str],
    fund_navs: list[float],
    bench_dates: list[str],
    bench_navs: list[float],
) -> tuple[list[str], list[float], list[float]]:
    """Inner-join two NAV series on date.

    Both legs come from the same AMFI feed when the benchmark is an index
    fund, so the calendars usually match exactly; Yahoo index series need the
    join because they carry days AMFI does not (and vice versa).
    """
    bench_lookup = dict(zip(bench_dates, bench_navs))
    dates: list[str] = []
    left: list[float] = []
    right: list[float] = []
    for day, nav in zip(fund_dates, fund_navs):
        other = bench_lookup.get(day)
        if other is None or other <= 0 or nav <= 0:
            continue
        dates.append(day)
        left.append(nav)
        right.append(other)
    return dates, left, right


def beta_alpha(
    fund_navs: list[float],
    bench_navs: list[float],
    *,
    fund_cagr_pct: float | None = None,
    bench_cagr_pct: float | None = None,
) -> dict[str, float | None]:
    """Beta, alpha, tracking error, information ratio, up/down capture.

    Expects the two series already aligned by `align_series`.
    """
    empty = {
        "beta": None,
        "alpha": None,
        "tracking_error": None,
        "information_ratio": None,
        "up_capture": None,
        "down_capture": None,
        "correlation": None,
    }
    fund_returns = daily_log_returns(fund_navs)
    bench_returns = daily_log_returns(bench_navs)
    if len(fund_returns) < 60 or len(fund_returns) != len(bench_returns):
        return empty

    n = len(fund_returns)
    fund_mean = sum(fund_returns) / n
    bench_mean = sum(bench_returns) / n
    covariance = sum(
        (f - fund_mean) * (b - bench_mean) for f, b in zip(fund_returns, bench_returns)
    ) / (n - 1)
    bench_variance = sum((b - bench_mean) ** 2 for b in bench_returns) / (n - 1)
    fund_variance = sum((f - fund_mean) ** 2 for f in fund_returns) / (n - 1)
    if bench_variance <= 0:
        return empty

    beta = covariance / bench_variance
    correlation = (
        covariance / math.sqrt(bench_variance * fund_variance)
        if fund_variance > 0
        else None
    )

    active = [f - b for f, b in zip(fund_returns, bench_returns)]
    active_mean = sum(active) / n
    active_variance = sum((value - active_mean) ** 2 for value in active) / (n - 1)
    tracking_error = math.sqrt(active_variance) * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0

    alpha = None
    if fund_cagr_pct is not None and bench_cagr_pct is not None:
        # Jensen's alpha on the annualised figures: the excess the fund
        # delivered over what its beta exposure to the benchmark alone
        # would have earned.
        risk_free_pct = RISK_FREE_RATE * 100.0
        alpha = fund_cagr_pct - (risk_free_pct + beta * (bench_cagr_pct - risk_free_pct))

    information_ratio = None
    if tracking_error > 0 and fund_cagr_pct is not None and bench_cagr_pct is not None:
        information_ratio = (fund_cagr_pct - bench_cagr_pct) / tracking_error

    up_bench = [(f, b) for f, b in zip(fund_returns, bench_returns) if b > 0]
    down_bench = [(f, b) for f, b in zip(fund_returns, bench_returns) if b < 0]
    up_capture = None
    down_capture = None
    if len(up_bench) >= 20:
        bench_up = sum(b for _, b in up_bench)
        if bench_up != 0:
            up_capture = sum(f for f, _ in up_bench) / bench_up * 100.0
    if len(down_bench) >= 20:
        bench_down = sum(b for _, b in down_bench)
        if bench_down != 0:
            down_capture = sum(f for f, _ in down_bench) / bench_down * 100.0

    return {
        "beta": beta,
        "alpha": alpha,
        "tracking_error": tracking_error,
        "information_ratio": information_ratio,
        "up_capture": up_capture,
        "down_capture": down_capture,
        "correlation": correlation,
    }


def assign_ranks(
    rows: list[dict],
    *,
    value_key: str,
    group_key: str,
    rank_key: str,
    count_key: str,
    percentile_key: str,
    quartile_key: str | None = None,
) -> None:
    """Rank each fund against its own category, in place.

    This is the "am I 1st or 12th in small cap" number the page is built
    around. Funds with no value for the window (too young) are ranked None
    but still counted in nothing — they are excluded from the denominator so
    a 3-year rank reads "4 / 22", not "4 / 31 with 9 blanks".
    """
    buckets: dict[str, list[dict]] = {}
    for row in rows:
        group = str(row.get(group_key) or "Uncategorised")
        buckets.setdefault(group, []).append(row)

    for members in buckets.values():
        ranked = [row for row in members if isinstance(row.get(value_key), (int, float))]
        ranked.sort(key=lambda row: row[value_key], reverse=True)
        total = len(ranked)
        for index, row in enumerate(ranked, start=1):
            row[rank_key] = index
            row[count_key] = total
            # Percentile: 100 = best in category. With one fund in a
            # category the percentile is undefined rather than 100.
            row[percentile_key] = (
                round((total - index) / (total - 1) * 100.0, 1) if total > 1 else None
            )
            if quartile_key:
                row[quartile_key] = min(4, int((index - 1) / total * 4) + 1) if total >= 4 else None
        for row in members:
            if row.get(rank_key) is None:
                row.setdefault(rank_key, None)
                row.setdefault(count_key, total)
                row.setdefault(percentile_key, None)
                if quartile_key:
                    row.setdefault(quartile_key, None)


def category_averages(rows: list[dict], *, group_key: str, value_keys: list[str]) -> dict[str, dict[str, float | int | None]]:
    """Mean of each metric per category, plus the member count.

    Used both for the "vs category average" line on a fund and for the
    category leaderboard.
    """
    buckets: dict[str, list[dict]] = {}
    for row in rows:
        group = str(row.get(group_key) or "Uncategorised")
        buckets.setdefault(group, []).append(row)

    out: dict[str, dict[str, float | int | None]] = {}
    for group, members in buckets.items():
        summary: dict[str, float | int | None] = {"count": len(members)}
        for key in value_keys:
            values = [row[key] for row in members if isinstance(row.get(key), (int, float))]
            summary[key] = sum(values) / len(values) if values else None
        out[group] = summary
    return out


def xirr(cashflows: list[tuple[date, float]], *, guess: float = 0.15) -> float | None:
    """Money-weighted annual return for a set of dated cashflows.

    Outflows (purchases) are negative, the closing value is positive. This is
    the only honest return number for a portfolio built through SIPs, where a
    simple cost-vs-value percentage flatters or punishes depending on when the
    money went in. Newton with a bisection fallback — Newton alone diverges on
    the short, lumpy cashflow sets a real portfolio produces.
    """
    if len(cashflows) < 2:
        return None
    flows = sorted(cashflows, key=lambda item: item[0])
    start = flows[0][0]
    if not any(amount > 0 for _, amount in flows) or not any(amount < 0 for _, amount in flows):
        return None

    def net_present_value(rate: float) -> float:
        total = 0.0
        for when, amount in flows:
            years = (when - start).days / ANNUALISE_FROM_DAYS
            total += amount / ((1.0 + rate) ** years)
        return total

    rate = guess
    for _ in range(60):
        value = net_present_value(rate)
        # Numerical derivative — the analytic one buys nothing here and is
        # easy to get wrong with fractional exponents.
        step = 1e-6
        slope = (net_present_value(rate + step) - value) / step
        if abs(slope) < 1e-12:
            break
        adjusted = rate - value / slope
        if adjusted <= -0.9999:
            break
        if abs(adjusted - rate) < 1e-8:
            return adjusted * 100.0
        rate = adjusted
    else:
        return rate * 100.0

    low, high = -0.9999, 10.0
    low_value = net_present_value(low)
    high_value = net_present_value(high)
    if low_value * high_value > 0:
        return None
    for _ in range(200):
        mid = (low + high) / 2
        mid_value = net_present_value(mid)
        if abs(mid_value) < 1e-9:
            return mid * 100.0
        if low_value * mid_value < 0:
            high = mid
        else:
            low, low_value = mid, mid_value
    return (low + high) / 2 * 100.0
