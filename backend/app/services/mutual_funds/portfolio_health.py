"""What is measurably true about this portfolio, stated as findings.

This is the panel at the foot of the portfolio page. It exists because the
numbers a fund investor needs are spread across a dozen factsheets and none of
them are about *their* portfolio: what the whole book costs to run, how much of
it is duplicated, which corners of the market it does not reach, and which
holdings have trailed their own category for long enough to be a record rather
than a rough patch.

**What this deliberately is not.** It does not recommend funds, rank a
"better" replacement, size a SIP, or time a purchase. That is personalised
investment advice, this app is not a licensed adviser, and the same line is
already drawn in `fund_review.py` for the single-fund case — see gotcha 12 in
CLAUDE.md. Every finding here is a measured fact about holdings the user
already owns, phrased so that the reader draws the conclusion. The test
`test_health_findings_never_instruct_the_reader` enforces it.

The distinction is not cosmetic. "Your portfolio costs 1.4% a year, which is
0.5% above the median for the same categories; on 12 lakh that is 6,000 a
year" is a fact the investor cannot easily compute and is entitled to. "Switch
to the cheaper fund" is a recommendation that depends on exit loads, capital
gains, and goals this app knows nothing about.
"""

from __future__ import annotations

from statistics import median
from typing import Any

# A finding has to clear a bar to be worth the reader's attention. These are
# the bars — descriptive thresholds, not verdicts.
COST_GAP_NOTABLE_PCT = 0.25      # portfolio TER this far above the category median
AMC_CONCENTRATION_PCT = 40.0     # share of the book with one fund house
LAGGING_PERCENTILE = 40.0        # bottom two fifths of its own category
CASH_DRAG_PCT = 8.0              # uninvested share of an equity fund's book
SMALL_POSITION_PCT = 2.0         # a holding too small to change the outcome

# The broad market cap buckets a long-horizon equity investor is usually
# reaching for. Absence is reported as absence, never as "you should add it".
CORE_CAP_CLASSES = ("large", "mid", "small")


def _num(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def _weighted(pairs: list[tuple[float, float]]) -> float | None:
    """Value-weighted mean of (weight, metric) pairs, ignoring blanks."""
    usable = [(w, m) for w, m in pairs if w > 0 and m is not None]
    if not usable:
        return None
    total = sum(w for w, _ in usable)
    return sum(w * m for w, m in usable) / total if total else None


def _finding(
    key: str,
    tone: str,
    headline: str,
    detail: str,
    *,
    metric: str | None = None,
    evidence: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "tone": tone,             # "watch" | "neutral" | "good"
        "headline": headline,
        "detail": detail,
        "metric": metric,
        "evidence": evidence or [],
    }


def cost_finding(holdings: list[dict[str, Any]], category_medians: dict[str, float], total_value: float) -> dict[str, Any] | None:
    """What the book costs to run, against what the same categories charge.

    Expense ratio is the one number in investing that is known in advance and
    compounds against the holder every year, so it earns a finding of its own.
    """
    pairs = [(h["value"], _num((h["fund"] or {}).get("expense_ratio"))) for h in holdings]
    weighted = _weighted(pairs)
    if weighted is None:
        return None

    benchmark_pairs = [
        (h["value"], category_medians.get(str((h["fund"] or {}).get("sub_category"))))
        for h in holdings
    ]
    category_norm = _weighted(benchmark_pairs)
    annual_cost = total_value * weighted / 100.0

    if category_norm is None:
        return _finding(
            "cost", "neutral",
            f"Your funds cost {weighted:.2f}% a year to run",
            f"About {annual_cost:,.0f} rupees a year at today's value, deducted from NAV rather "
            "than billed. Direct plans already strip out distributor commission.",
            metric=f"{weighted:.2f}%",
        )

    gap = weighted - category_norm
    gap_cost = total_value * gap / 100.0
    dearest = sorted(
        [h for h in holdings if _num((h["fund"] or {}).get("expense_ratio")) is not None],
        key=lambda h: -(h["fund"]["expense_ratio"]),
    )[:5]
    evidence = [
        {
            "name": h["fund"].get("name"),
            "scheme_code": h["scheme_code"],
            "value": round(h["fund"]["expense_ratio"], 2),
            "reference": category_medians.get(str(h["fund"].get("sub_category"))),
            "label": h["fund"].get("sub_category"),
        }
        for h in dearest
    ]

    if gap > COST_GAP_NOTABLE_PCT:
        return _finding(
            "cost", "watch",
            f"Your funds cost {weighted:.2f}% a year — {gap:.2f}% above the median for the same categories",
            f"That difference is about {gap_cost:,.0f} rupees a year at today's value, and it is "
            f"charged whether the funds gain or lose. Over twenty years, {gap:.2f}% a year "
            "compounds into a materially smaller corpus. The median direct plan in your own "
            f"categories charges {category_norm:.2f}%.",
            metric=f"+{gap:.2f}%",
            evidence=evidence,
        )
    return _finding(
        "cost", "good",
        f"Your funds cost {weighted:.2f}% a year, in line with their categories",
        f"The median direct plan across the categories you hold charges {category_norm:.2f}%, so "
        f"cost is not working against you here. About {annual_cost:,.0f} rupees a year in total.",
        metric=f"{weighted:.2f}%",
        evidence=evidence,
    )


def lagging_finding(holdings: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Funds in the bottom of their own category over the long windows.

    One weak year is noise. Bottom-half over both three and five years, against
    the fund's own category rather than the market, is a record. Stating it is
    reporting; what to do about it is the holder's call.
    """
    lagging = []
    for holding in holdings:
        fund = holding["fund"] or {}
        p3, p5 = _num(fund.get("percentile_3y")), _num(fund.get("percentile_5y"))
        if p3 is None or p5 is None:
            continue
        if p3 < LAGGING_PERCENTILE and p5 < LAGGING_PERCENTILE:
            lagging.append({
                "name": fund.get("name"),
                "scheme_code": holding["scheme_code"],
                "label": fund.get("sub_category"),
                "value": round(p3, 1),
                "reference": round(p5, 1),
                "weight_pct": holding.get("weight_pct"),
                "return_3y": _num(fund.get("return_3y")),
                "return_5y": _num(fund.get("return_5y")),
                "rank_3y": fund.get("rank_3y"),
                "rank_count_3y": fund.get("rank_count_3y"),
            })
    if not lagging:
        return _finding(
            "lagging", "good",
            "No fund you hold sits in the bottom of its category over both three and five years",
            "Every holding with enough history is at or above the fortieth percentile of its own "
            "peer group on at least one of the two long windows.",
        )

    lagging.sort(key=lambda row: row["value"])
    share = sum(row.get("weight_pct") or 0 for row in lagging)
    names = ", ".join(str(row["name"]) for row in lagging[:3])
    return _finding(
        "lagging", "watch",
        f"{len(lagging)} of your funds have stayed in the bottom {LAGGING_PERCENTILE:.0f}% of their category over both 3 and 5 years",
        f"{names}{' and others' if len(lagging) > 3 else ''} — together {share:.0f}% of your "
        "portfolio. Two long windows agreeing is a record rather than a rough patch: these are "
        "measured against the fund's own peer group, so it is not a market-wide effect. Each "
        "fund's page shows the rank trajectory behind this.",
        metric=f"{share:.0f}% of book",
        evidence=lagging[:6],
    )


def amc_finding(by_amc: dict[str, float], total_value: float) -> dict[str, Any] | None:
    """How much of the book sits with one fund house."""
    if not by_amc or total_value <= 0:
        return None
    top_amc, top_value = max(by_amc.items(), key=lambda kv: kv[1])
    share = top_value / total_value * 100.0
    evidence = [
        {"name": amc, "value": round(value / total_value * 100.0, 1), "label": "of portfolio"}
        for amc, value in sorted(by_amc.items(), key=lambda kv: -kv[1])[:6]
    ]
    if share >= AMC_CONCENTRATION_PCT:
        return _finding(
            "amc", "watch",
            f"{share:.0f}% of your portfolio is with one fund house — {top_amc}",
            "Fund houses share a research desk and a house view, so funds under one roof tend to "
            "move together more than their different labels suggest. This is a process risk no "
            "individual factsheet mentions, because no factsheet knows what else you hold.",
            metric=f"{share:.0f}%",
            evidence=evidence,
        )
    return _finding(
        "amc", "good",
        f"Your money is spread across {len(by_amc)} fund houses",
        f"The largest, {top_amc}, holds {share:.0f}% — no single house's research desk drives the "
        "portfolio.",
        metric=f"{len(by_amc)} AMCs",
        evidence=evidence,
    )


def cap_coverage_finding(cap_totals: dict[str, float]) -> dict[str, Any] | None:
    """Which market cap bands the look-through actually reaches.

    Reported as coverage, not as a target allocation — there is no single right
    large/mid/small split, and asserting one would be advice.
    """
    total = sum(cap_totals.values())
    if total <= 0:
        return None
    shares = {key: cap_totals.get(key, 0.0) / total * 100.0 for key in CORE_CAP_CLASSES}
    missing = [key for key, share in shares.items() if share < 2.0]
    evidence = [
        {"name": f"{key.title()} cap", "value": round(share, 1), "label": "of equity"}
        for key, share in shares.items()
    ]
    parts = ", ".join(f"{share:.0f}% {key}" for key, share in shares.items())
    if missing:
        return _finding(
            "caps", "neutral",
            f"Your equity is {parts}",
            f"Effectively no {' or '.join(missing)} cap exposure once the funds are looked through "
            "to the stocks they actually hold. Whether that suits you depends on your horizon and "
            "how much volatility you want — this is the measurement, not a target.",
            metric=parts,
            evidence=evidence,
        )
    return _finding(
        "caps", "good",
        f"Your equity spans all three market cap bands — {parts}",
        "Measured from the stocks your funds disclose and this app's own market cap data, not from "
        "the funds' category labels.",
        metric=parts,
        evidence=evidence,
    )


def overlap_finding(overlap_payload: dict[str, Any]) -> dict[str, Any] | None:
    """Surface the worst duplicated pair as a portfolio-level finding."""
    pairs = overlap_payload.get("pairs") or []
    if not pairs:
        return None
    notable = [p for p in pairs if (p.get("overlap_pct") or 0) >= 30.0]
    if not notable:
        top = pairs[0]
        return _finding(
            "overlap", "good",
            f"No two of your funds duplicate more than {top['overlap_pct']:.0f}% of each other",
            f"The closest pair is {top['left_name']} and {top['right_name']}. Each fund is "
            "contributing its own book rather than restating another's.",
            metric=f"{top['overlap_pct']:.0f}% max",
        )
    duplicated = sum(p.get("duplicated_value") or 0 for p in notable)
    worst = notable[0]
    return _finding(
        "overlap", "watch",
        f"{len(notable)} pair{'s' if len(notable) != 1 else ''} of your funds "
        f"hold{'' if len(notable) != 1 else 's'} substantially the same stocks",
        f"The closest is {worst['left_name']} and {worst['right_name']} at "
        f"{worst['overlap_pct']:.0f}% overlap across {worst['shared_count']} shared names. About "
        f"{duplicated:,.0f} rupees is invested through more than one route — exposed once, but "
        "carrying each fund's expense ratio. The overlap table above lists every pair.",
        metric=f"{worst['overlap_pct']:.0f}% top pair",
        evidence=[
            {
                "name": f"{p['left_name']} × {p['right_name']}",
                "value": p["overlap_pct"],
                "label": f"{p['shared_count']} shared",
            }
            for p in notable[:5]
        ],
    )


def small_positions_finding(holdings: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Holdings too small to move the portfolio either way."""
    tiny = [h for h in holdings if 0 < (h.get("weight_pct") or 0) < SMALL_POSITION_PCT]
    if len(tiny) < 2:
        return None
    share = sum(h.get("weight_pct") or 0 for h in tiny)
    return _finding(
        "small_positions", "neutral",
        f"{len(tiny)} holdings are under {SMALL_POSITION_PCT:.0f}% of the portfolio each",
        f"Together they are {share:.0f}% of the book. A position this size has to double to move "
        "the total by a percent, so it adds statements to read without much changing the outcome. "
        "Each still carries its own expense ratio and exit load.",
        metric=f"{share:.0f}% in {len(tiny)}",
        evidence=[
            {"name": (h["fund"] or {}).get("name"), "scheme_code": h["scheme_code"],
             "value": round(h.get("weight_pct") or 0, 2), "label": "of portfolio"}
            for h in sorted(tiny, key=lambda h: h.get("weight_pct") or 0)[:6]
        ],
    )


def category_medians(all_funds: list[dict[str, Any]], field: str) -> dict[str, float]:
    """Median of `field` per sub-category across the whole universe."""
    buckets: dict[str, list[float]] = {}
    for fund in all_funds:
        value = _num(fund.get(field))
        if value is None:
            continue
        buckets.setdefault(str(fund.get("sub_category") or "Unknown"), []).append(value)
    return {key: median(values) for key, values in buckets.items() if values}


def positioning_chart(
    holdings: list[dict[str, Any]],
    all_funds: list[dict[str, Any]],
) -> dict[str, Any]:
    """Where each fund you hold sits against its own category, on cost and return.

    Two axes, both already computed elsewhere in the app: annual cost on one,
    three-year return on the other, each expressed as the gap from the fund's
    own category median so that a large cap and a small cap can share a plot.
    A point up and to the left has returned more than its peers while charging
    less than they do.

    This plots holdings the user already owns. It is not a shortlist of funds
    to buy — the app does not produce one.
    """
    cost_medians = category_medians(all_funds, "expense_ratio")
    return_medians = category_medians(all_funds, "return_3y")

    points: list[dict[str, Any]] = []
    for holding in holdings:
        fund = holding["fund"] or {}
        category = str(fund.get("sub_category") or "Unknown")
        cost, ret = _num(fund.get("expense_ratio")), _num(fund.get("return_3y"))
        cost_ref, return_ref = cost_medians.get(category), return_medians.get(category)
        if cost is None or ret is None or cost_ref is None or return_ref is None:
            continue
        points.append({
            "scheme_code": holding["scheme_code"],
            "name": fund.get("name"),
            "category": category,
            "cost_gap": round(cost - cost_ref, 2),
            "return_gap": round(ret - return_ref, 2),
            "expense_ratio": cost,
            "return_3y": ret,
            "category_expense_median": round(cost_ref, 2),
            "category_return_median": round(return_ref, 2),
            "weight_pct": holding.get("weight_pct"),
            "percentile_3y": _num(fund.get("percentile_3y")),
        })

    points.sort(key=lambda row: -(row["weight_pct"] or 0))
    return {
        "points": points,
        "x_label": "Cost vs category median (%)",
        "y_label": "3y return vs category median (%)",
        "note": "Each point is a fund you hold, placed against the median of its own SEBI "
                "sub-category. Up is better return than peers; left is cheaper than peers.",
    }


def build(
    *,
    positions: list[dict[str, Any]],
    allocation: dict[str, Any],
    totals: dict[str, Any],
    all_funds: list[dict[str, Any]],
    overlap_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Every measured finding about the portfolio, plus the positioning plot."""
    holdings = [
        {
            "scheme_code": str(position["scheme_code"]),
            "fund": position.get("fund") or {},
            "value": position.get("current_value") or 0.0,
            "weight_pct": position.get("weight_pct") or 0.0,
        }
        for position in positions
        if (position.get("units") or 0) > 0 and (position.get("current_value") or 0) > 0
    ]
    total_value = float(totals.get("current_value") or 0.0)
    if not holdings or total_value <= 0:
        return {
            "available": False,
            "reason": "Add holdings with a value before this can measure anything.",
            "findings": [],
            "chart": {"points": []},
        }

    cap_totals: dict[str, float] = {}
    for row in allocation.get("look_through_top") or []:
        cap = str(row.get("cap_class") or "").lower()
        if cap in CORE_CAP_CLASSES:
            cap_totals[cap] = cap_totals.get(cap, 0.0) + (row.get("value") or 0.0)

    findings = [
        cost_finding(holdings, category_medians(all_funds, "expense_ratio"), total_value),
        overlap_finding(overlap_payload or {}),
        lagging_finding(holdings),
        cap_coverage_finding(cap_totals),
        amc_finding(allocation.get("by_amc") or {}, total_value),
        small_positions_finding(holdings),
    ]
    findings = [item for item in findings if item]
    # Things to look at first, then the clean bills of health.
    order = {"watch": 0, "neutral": 1, "good": 2}
    findings.sort(key=lambda item: order.get(item["tone"], 3))

    return {
        "available": True,
        "fund_count": len(holdings),
        "total_value": round(total_value, 2),
        "watch_count": sum(1 for item in findings if item["tone"] == "watch"),
        "findings": findings,
        "chart": positioning_chart(holdings, all_funds),
        "disclaimer": (
            "Measurements of the portfolio you already hold, against the categories those funds "
            "sit in. Nothing here is a recommendation to buy, sell or switch anything — this app "
            "is not a licensed investment adviser."
        ),
    }
