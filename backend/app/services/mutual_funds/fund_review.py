"""How a held fund is actually doing against its own category.

This module answers "is this fund pulling its weight" with measured history
and nothing else. It is deliberately split in two:

* **Everything here is arithmetic.** Percentiles, rank trajectories, capture
  ratios, cost gaps — all computed from the universe, all reproducible, all
  reportable with a number attached.
* **The prose layer (AI) is handed these numbers and may only describe them.**
  Same contract as `market_regime.py`: it derives nothing, it invents nothing.

What this module does **not** do, by design: tell anyone to switch funds, size
a SIP, or time a lump sum. Those are personalised investment decisions. The
output here is evidence a person uses to make their own call, which is why
every signal carries the figure it came from rather than a verdict alone.
"""

from __future__ import annotations

from typing import Any

# Each dimension: (key, label, higher_is_better, category-average key, unit)
DIMENSIONS: tuple[tuple[str, str, bool, str, str], ...] = (
    ("return_1y", "1-year return", True, "return_1y", "%"),
    ("return_3y", "3-year return", True, "return_3y", "%"),
    ("return_5y", "5-year return", True, "return_5y", "%"),
    ("rolling3y_median", "Typical 3-year outcome", True, None, "%"),
    ("rolling3y_pct_negative", "3-year periods that lost money", False, None, "%"),
    ("max_drawdown", "Worst fall", True, "max_drawdown", "%"),
    ("down_capture", "Share of benchmark falls taken", False, None, "%"),
    ("sharpe", "Return per unit of risk", True, "sharpe", ""),
    ("expense_ratio", "Expense ratio", False, "expense_ratio", "%"),
    ("alpha", "Alpha vs benchmark", True, None, "%"),
)

# A fund is called out only when it is clearly on one side. Between these, the
# honest answer is "middling", and saying so is more useful than a false signal.
STRONG_PERCENTILE = 70.0
WEAK_PERCENTILE = 30.0


def _percentile_of(value: float, population: list[float], *, higher_is_better: bool) -> float | None:
    """Where `value` sits in `population`, 100 = best.

    Ties are counted as half, so a column where two thirds of funds report the
    identical figure does not hand them all a 100th percentile.
    """
    if not population:
        return None
    below = sum(1 for other in population if (other < value) == higher_is_better and other != value)
    equal = sum(1 for other in population if other == value)
    return round((below + equal / 2) / len(population) * 100.0, 1)


def _numeric(rows: list[dict[str, Any]], key: str) -> list[float]:
    return [row[key] for row in rows if isinstance(row.get(key), (int, float))]


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def build_scorecard(fund: dict[str, Any], peers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The fund on each measured dimension, against its category."""
    others = [peer for peer in peers if str(peer.get("scheme_code")) != str(fund.get("scheme_code"))]
    out: list[dict[str, Any]] = []
    for key, label, higher_is_better, _avg_key, unit in DIMENSIONS:
        value = fund.get(key)
        if not isinstance(value, (int, float)):
            continue
        population = _numeric(peers, key)
        percentile = _percentile_of(float(value), population, higher_is_better=higher_is_better)
        out.append({
            "key": key,
            "label": label,
            "unit": unit,
            "value": round(float(value), 3),
            "category_median": round(_median(_numeric(others, key)) or 0.0, 3) if others else None,
            "percentile": percentile,
            "higher_is_better": higher_is_better,
            "sample": len(population),
            "standing": (
                "strong" if percentile is not None and percentile >= STRONG_PERCENTILE
                else "weak" if percentile is not None and percentile <= WEAK_PERCENTILE
                else "middling"
            ),
        })
    return out


def rank_trajectory(fund: dict[str, Any]) -> dict[str, Any]:
    """Is its standing improving or slipping across horizons?

    Compares the *percentile* rather than the raw rank, because categories
    differ in size and a rank of 20 means different things in a field of 25
    versus 120.
    """
    points = []
    for window in ("5y", "3y", "1y"):
        percentile = fund.get(f"percentile_{window}")
        if isinstance(percentile, (int, float)):
            points.append((window, float(percentile)))
    if len(points) < 2:
        return {"points": points, "direction": "unknown", "change": None}
    change = points[-1][1] - points[0][1]
    return {
        "points": points,
        "change": round(change, 1),
        # Deliberately wide dead zone: category percentiles move a lot on noise,
        # and calling a 6-point drift a decline would be reading tea leaves.
        "direction": "improving" if change >= 15 else "slipping" if change <= -15 else "steady",
    }


def build_signals(
    fund: dict[str, Any],
    scorecard: list[dict[str, Any]],
    trajectory: dict[str, Any],
) -> list[dict[str, Any]]:
    """Factual observations, each carrying the number behind it.

    Phrased as statements of measured fact ("bottom decile on cost"), never as
    instructions ("switch out of this"). The distinction is the whole point of
    this function.
    """
    signals: list[dict[str, Any]] = []
    by_key = {item["key"]: item for item in scorecard}

    def add(kind: str, text: str) -> None:
        signals.append({"kind": kind, "text": text})

    for item in scorecard:
        if item["standing"] == "strong":
            add("strength", f"{item['label']}: {item['value']}{item['unit']} — better than "
                            f"{item['percentile']:.0f}% of its category.")
        elif item["standing"] == "weak":
            add("concern", f"{item['label']}: {item['value']}{item['unit']} — behind "
                           f"{100 - item['percentile']:.0f}% of its category.")

    if trajectory["direction"] == "slipping":
        add("concern", f"Its standing in the category has fallen {abs(trajectory['change']):.0f} "
                       f"percentile points moving from the 5-year to the 1-year window.")
    elif trajectory["direction"] == "improving":
        add("strength", f"Its standing has improved {trajectory['change']:.0f} percentile points "
                        f"from the 5-year to the 1-year window.")

    cost = by_key.get("expense_ratio")
    ret = by_key.get("return_3y")
    if cost and ret and cost["standing"] == "weak" and ret["standing"] == "weak":
        add("concern", "It charges more than most of its category and has returned less over "
                       "three years — the cost is not buying performance.")

    consistency = by_key.get("rolling3y_pct_negative")
    if consistency and isinstance(consistency["value"], (int, float)) and consistency["value"] == 0:
        add("strength", "No three-year holding period in its history has ended in a loss.")

    if not signals:
        add("neutral", "It sits close to its category median on every measured dimension.")
    return signals


def peers_ahead(
    fund: dict[str, Any],
    peers: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Same-category funds that measured better on return, cost and downside.

    This is a filtered peer table, not a shortlist to buy. It exists so the
    comparison is checkable — every column is the same figure the fund itself
    is being judged on — and it deliberately requires a fund to be better on
    *all three* axes rather than just having a higher headline return.
    """
    mine_return = fund.get("return_3y")
    mine_cost = fund.get("expense_ratio")
    mine_drawdown = fund.get("max_drawdown")
    if not isinstance(mine_return, (int, float)):
        return []

    out: list[dict[str, Any]] = []
    for peer in peers:
        if str(peer.get("scheme_code")) == str(fund.get("scheme_code")):
            continue
        peer_return = peer.get("return_3y")
        if not isinstance(peer_return, (int, float)) or peer_return <= mine_return:
            continue
        if isinstance(mine_cost, (int, float)) and isinstance(peer.get("expense_ratio"), (int, float)):
            if peer["expense_ratio"] > mine_cost:
                continue
        if isinstance(mine_drawdown, (int, float)) and isinstance(peer.get("max_drawdown"), (int, float)):
            # max_drawdown is negative; "shallower" means a larger number.
            if peer["max_drawdown"] < mine_drawdown:
                continue
        out.append({
            "scheme_code": peer.get("scheme_code"),
            "name": peer.get("name"),
            "amc": peer.get("amc"),
            "return_3y": peer.get("return_3y"),
            "return_5y": peer.get("return_5y"),
            "expense_ratio": peer.get("expense_ratio"),
            "max_drawdown": peer.get("max_drawdown"),
            "sharpe": peer.get("sharpe"),
            "return_gap": round(peer_return - mine_return, 2),
        })
    out.sort(key=lambda row: -(row["return_gap"] or 0))
    return out[:limit]


def build_review(
    fund: dict[str, Any],
    peers: list[dict[str, Any]],
    *,
    category_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The complete measured picture for one fund against its category."""
    scorecard = build_scorecard(fund, peers)
    trajectory = rank_trajectory(fund)
    signals = build_signals(fund, scorecard, trajectory)

    graded = [item for item in scorecard if item["percentile"] is not None]
    standing = round(sum(item["percentile"] for item in graded) / len(graded), 1) if graded else None

    return {
        "scheme_code": fund.get("scheme_code"),
        "name": fund.get("name"),
        "sub_category": fund.get("sub_category"),
        "benchmark_label": fund.get("benchmark_label"),
        "peer_count": len(peers),
        # Mean percentile across every dimension we could measure. A summary of
        # the evidence, not a rating: it has no opinion on what to do about it.
        "measured_standing": standing,
        "scorecard": scorecard,
        "rank_trajectory": trajectory,
        "signals": signals,
        "peers_ahead": peers_ahead(fund, peers),
        "category_summary": category_summary,
        "strength_count": sum(1 for s in signals if s["kind"] == "strength"),
        "concern_count": sum(1 for s in signals if s["kind"] == "concern"),
    }
