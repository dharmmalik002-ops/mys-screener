"""How much two funds you hold are the same fund wearing different names.

The question this answers is not "is any one fund concentrated" — that is
`concentration.py`. It is the one a multi-fund investor actually has: *if two
of my funds hold the same stocks in the same weights, what is the second one
adding?* Four large caps that each hold the same top twenty names are one
large cap position with four expense ratios attached.

**The measure.** For a pair of funds, overlap is

    sum over stocks held by both of  min(weight_in_A, weight_in_B)

which is the share of a rupee that would sit in identical positions whichever
of the two it went into. It is the standard portfolio-overlap definition and it
has the properties you want: identical portfolios score 100%, disjoint ones
score 0%, and holding the same stock at 8% and 1% contributes 1%, not 8% —
because only 1% of it is genuinely duplicated.

Two funds in the same category will always overlap somewhat; the Nifty 50 is
only fifty stocks. So the reference points below are calibrated per category
pair, and the output states the measured number rather than issuing a verdict.
Everything here is arithmetic over disclosed holdings: it reports what the
weights are and does not tell anyone to sell either fund, which would be a
personalised recommendation.
"""

from __future__ import annotations

from typing import Any

# Where a pair stops looking like "same category, naturally similar" and starts
# looking like "these are the same portfolio". Descriptive labels, not verdicts.
SUBSTANTIAL_PCT = 30.0
HIGH_PCT = 50.0
VERY_HIGH_PCT = 70.0

EQUITY_CLASSES = ("equity", "international_equity")


def _weights(detail: dict[str, Any] | None) -> dict[str, float]:
    """Disclosed equity weights, keyed by the best identifier available.

    Symbol is preferred over name: two AMCs write "HDFC Bank Ltd." and "HDFC
    Bank Limited" for the same company, and matching on the raw string would
    score a real overlap as zero.
    """
    out: dict[str, float] = {}
    for row in ((detail or {}).get("holdings") or []):
        if row.get("asset_class") not in EQUITY_CLASSES:
            continue
        weight = row.get("weight_pct")
        if not isinstance(weight, (int, float)) or weight <= 0:
            continue
        key = str(row.get("symbol") or "").strip().upper() or _name_key(row.get("name"))
        if not key:
            continue
        out[key] = out.get(key, 0.0) + float(weight)
    return out


def _name_key(name: Any) -> str:
    text = str(name or "").lower()
    for noise in (" ltd.", " ltd", " limited", " (india)", " india", " corporation", " corp.", "&", "."):
        text = text.replace(noise, " ")
    return " ".join(text.split())


def _labels(detail: dict[str, Any] | None) -> dict[str, str]:
    """Display name per key, so the shared list reads as company names."""
    out: dict[str, str] = {}
    for row in ((detail or {}).get("holdings") or []):
        if row.get("asset_class") not in EQUITY_CLASSES:
            continue
        key = str(row.get("symbol") or "").strip().upper() or _name_key(row.get("name"))
        if key and key not in out and row.get("name"):
            out[key] = str(row["name"])
    return out


def band(pct: float | None) -> str:
    """Descriptive band for an overlap percentage."""
    if pct is None:
        return "unknown"
    if pct >= VERY_HIGH_PCT:
        return "very_high"
    if pct >= HIGH_PCT:
        return "high"
    if pct >= SUBSTANTIAL_PCT:
        return "substantial"
    return "modest"


def pair_overlap(
    left: dict[str, float],
    right: dict[str, float],
    labels: dict[str, str],
) -> dict[str, Any]:
    """Overlap between two weight maps, plus the names driving it."""
    shared_keys = set(left) & set(right)
    shared: list[dict[str, Any]] = []
    total = 0.0
    for key in shared_keys:
        common = min(left[key], right[key])
        total += common
        shared.append({
            "name": labels.get(key, key),
            "left_pct": round(left[key], 2),
            "right_pct": round(right[key], 2),
            "common_pct": round(common, 2),
        })
    shared.sort(key=lambda row: -row["common_pct"])

    # Also useful, and different: what share of the *smaller* fund's book is
    # duplicated. Two funds can overlap 25% in absolute terms while that 25%
    # is most of what the smaller, more concentrated one owns.
    left_total = sum(left.values()) or 1.0
    right_total = sum(right.values()) or 1.0
    return {
        "overlap_pct": round(total, 2),
        "shared_count": len(shared_keys),
        "left_holdings": len(left),
        "right_holdings": len(right),
        "share_of_left": round(total / left_total * 100.0, 1),
        "share_of_right": round(total / right_total * 100.0, 1),
        "shared_top": shared[:12],
        "band": band(total),
    }


def describe(pairs: list[dict[str, Any]], *, threshold: float = SUBSTANTIAL_PCT) -> list[str]:
    """Plain-English sentences over the pair table.

    Written here rather than by a model on purpose: these are statements of
    arithmetic and they should read identically every time for the same input.
    They describe what is duplicated. They do not say which fund to keep —
    that depends on tax position, exit loads and goals this app knows nothing
    about, and it is advice this app does not give.
    """
    lines: list[str] = []
    notable = [pair for pair in pairs if (pair.get("overlap_pct") or 0) >= threshold]

    if not notable:
        if pairs:
            top = pairs[0]
            lines.append(
                f"No two funds you hold duplicate more than {threshold:.0f}% of each other. The "
                f"closest pair is {top['left_name']} and {top['right_name']} at "
                f"{top['overlap_pct']:.0f}% — they share {top['shared_count']} stocks, which is "
                "ordinary for funds in the same market."
            )
        return lines

    for pair in notable:
        names = ", ".join(row["name"] for row in (pair.get("shared_top") or [])[:4])
        same_category = pair.get("left_category") and pair["left_category"] == pair.get("right_category")
        lines.append(
            f"{pair['left_name']} and {pair['right_name']} overlap {pair['overlap_pct']:.0f}% — "
            f"{pair['shared_count']} stocks in common"
            + (f", led by {names}" if names else "")
            + ". "
            + (
                f"Both are {pair['left_category']} funds, so some of this is the category rather "
                "than the manager"
                if same_category
                else f"They are labelled differently ({pair['left_category']} and "
                     f"{pair['right_category']}) but hold much the same book"
            )
            + f". Together they are {pair['combined_weight_pct']:.0f}% of your portfolio."
        )

    duplicated_value = sum(pair.get("duplicated_value") or 0 for pair in notable)
    if duplicated_value > 0:
        lines.append(
            f"Roughly {duplicated_value:,.0f} rupees of your portfolio sits in positions reached "
            "through more than one of these overlapping pairs. That money is exposed once, not "
            "twice, while paying two expense ratios."
        )
    return lines


def build(
    positions: list[dict[str, Any]],
    *,
    detail_for,
    total_value: float | None = None,
) -> dict[str, Any]:
    """Pairwise overlap across every fund currently held.

    Sold positions are excluded — a fund you no longer own cannot duplicate
    anything. Pairs are returned sorted by overlap, worst first.
    """
    books: list[dict[str, Any]] = []
    for position in positions:
        if (position.get("units") or 0) <= 0:
            continue
        row = position.get("fund") or {}
        detail, _stale = detail_for(str(position["scheme_code"]), row.get("slug"))
        if not (detail or {}).get("holdings") and row.get("growth_sibling_code"):
            # An IDCW plan is the same portfolio as its Growth sibling. Without
            # this a portfolio of dividend plans reports no overlap at all.
            detail, _stale = detail_for(
                str(row["growth_sibling_code"]), row.get("growth_sibling_slug")
            )
        weights = _weights(detail)
        if not weights:
            continue
        books.append({
            "scheme_code": str(position["scheme_code"]),
            "name": row.get("name") or str(position["scheme_code"]),
            "category": row.get("sub_category") or "Unknown",
            "amc": row.get("amc"),
            "weights": weights,
            "labels": _labels(detail),
            "value": position.get("current_value") or 0.0,
            "weight_pct": position.get("weight_pct") or 0.0,
            "portfolio_date": (detail or {}).get("portfolio_date"),
        })

    pairs: list[dict[str, Any]] = []
    for index, left in enumerate(books):
        for right in books[index + 1:]:
            merged_labels = {**right["labels"], **left["labels"]}
            measured = pair_overlap(left["weights"], right["weights"], merged_labels)
            combined = (left["weight_pct"] or 0) + (right["weight_pct"] or 0)
            # Value genuinely duplicated: the overlapping share of the smaller
            # of the two holdings. Counting both sides would double-count the
            # very thing being measured.
            smaller_value = min(left["value"] or 0.0, right["value"] or 0.0)
            pairs.append({
                **measured,
                "left_code": left["scheme_code"],
                "right_code": right["scheme_code"],
                "left_name": left["name"],
                "right_name": right["name"],
                "left_category": left["category"],
                "right_category": right["category"],
                "same_amc": bool(left["amc"] and left["amc"] == right["amc"]),
                "combined_weight_pct": round(combined, 2),
                "duplicated_value": round(smaller_value * measured["overlap_pct"] / 100.0, 2),
                "portfolio_dates": [left["portfolio_date"], right["portfolio_date"]],
            })

    pairs.sort(key=lambda pair: -(pair["overlap_pct"] or 0))
    notable = [pair for pair in pairs if (pair["overlap_pct"] or 0) >= SUBSTANTIAL_PCT]

    return {
        "pairs": pairs,
        "pair_count": len(pairs),
        "funds_compared": len(books),
        "funds_without_holdings": sum(
            1 for position in positions
            if (position.get("units") or 0) > 0
        ) - len(books),
        "notable_count": len(notable),
        "highest_pct": pairs[0]["overlap_pct"] if pairs else None,
        "thresholds": {
            "substantial_pct": SUBSTANTIAL_PCT,
            "high_pct": HIGH_PCT,
            "very_high_pct": VERY_HIGH_PCT,
        },
        "summary": describe(pairs),
    }
