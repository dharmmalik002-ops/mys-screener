"""How concentrated a portfolio actually is, stated plainly.

Two different risks, often confused:

* **Fund-level** — a fund whose top holdings dominate it is a concentrated
  bet whatever its label says. A "diversified" flexi cap with 45% in its top
  five is not diversified.
* **Portfolio-level** — the one a fund investor cannot see without a
  look-through. Five funds each holding the same stock at 8% is a 8% position
  in that stock, not five diversified holdings, and no fund factsheet will
  ever tell you.

Everything here is arithmetic over disclosed holdings. It reports what the
weights are; it does not tell anyone to buy or sell, which would be a
personalised recommendation.
"""

from __future__ import annotations

from typing import Any

# Above this, a fund's top-5 is worth pointing out. Not a rule — SEBI's own
# focused-fund category is allowed up to 30 stocks and routinely runs higher —
# so the label is descriptive ("concentrated"), never a verdict.
TOP5_NOTABLE_PCT = 30.0
TOP10_NOTABLE_PCT = 50.0
SINGLE_STOCK_NOTABLE_PCT = 8.0
PORTFOLIO_STOCK_NOTABLE_PCT = 5.0


def _top_weight(holdings: list[dict[str, Any]], count: int) -> float | None:
    equity = [
        row for row in holdings
        if row.get("asset_class") in ("equity", "international_equity")
        and isinstance(row.get("weight_pct"), (int, float))
    ]
    if not equity:
        return None
    ranked = sorted(equity, key=lambda row: -(row["weight_pct"] or 0))
    return round(sum(row["weight_pct"] or 0 for row in ranked[:count]), 2)


def analyse_fund(fund: dict[str, Any], detail: dict[str, Any] | None) -> dict[str, Any]:
    """Concentration of one fund, from its disclosed portfolio."""
    holdings = (detail or {}).get("holdings") or []
    top5 = _top_weight(holdings, 5)
    top10 = _top_weight(holdings, 10)
    equity = [
        row for row in holdings
        if row.get("asset_class") in ("equity", "international_equity")
        and isinstance(row.get("weight_pct"), (int, float))
    ]
    ranked = sorted(equity, key=lambda row: -(row["weight_pct"] or 0))
    largest = ranked[0] if ranked else None

    flags: list[str] = []
    if top5 is not None and top5 > TOP5_NOTABLE_PCT:
        flags.append("top5")
    if top10 is not None and top10 > TOP10_NOTABLE_PCT:
        flags.append("top10")
    if largest and (largest.get("weight_pct") or 0) > SINGLE_STOCK_NOTABLE_PCT:
        flags.append("single")

    return {
        "scheme_code": fund.get("scheme_code"),
        "name": fund.get("name"),
        "sub_category": fund.get("sub_category"),
        "portfolio_date": (detail or {}).get("portfolio_date"),
        "holdings_count": (detail or {}).get("equity_holdings_count"),
        "top5_pct": top5,
        "top10_pct": top10,
        "largest_name": (largest or {}).get("name"),
        "largest_pct": (largest or {}).get("weight_pct"),
        "sector_count": (detail or {}).get("sector_count"),
        "top_sector": next(iter((detail or {}).get("sector_allocation") or {}), None),
        "top_sector_pct": next(iter(((detail or {}).get("sector_allocation") or {}).values()), None),
        "flags": flags,
        "concentrated": bool(flags),
        "top_holdings": [
            {"name": row.get("name"), "weight_pct": row.get("weight_pct"),
             "symbol": row.get("symbol"), "sector": row.get("sector")}
            for row in ranked[:10]
        ],
    }


def describe(funds: list[dict[str, Any]], look_through: list[dict[str, Any]]) -> list[str]:
    """Plain-English sentences over the numbers above.

    Deliberately written here rather than by a model: these are statements of
    arithmetic, and they should say the same thing every time for the same
    input.
    """
    lines: list[str] = []

    flagged = [fund for fund in funds if "top5" in fund["flags"]]
    if flagged:
        flagged.sort(key=lambda fund: -(fund["top5_pct"] or 0))
        for fund in flagged:
            lines.append(
                f"{fund['name']} keeps {fund['top5_pct']:.0f}% of its portfolio in just five "
                f"stocks — the largest single position is {fund['largest_name']} at "
                f"{fund['largest_pct']:.1f}%. That is a concentrated fund: it will move on a "
                f"handful of names rather than on its category."
            )
    else:
        lines.append(
            "No fund you hold keeps more than "
            f"{TOP5_NOTABLE_PCT:.0f}% of its portfolio in its top five stocks — none of them is "
            "carrying unusual single-stock risk."
        )

    heavy_ten = [
        fund for fund in funds
        if "top10" in fund["flags"] and "top5" not in fund["flags"]
    ]
    for fund in sorted(heavy_ten, key=lambda item: -(item["top10_pct"] or 0)):
        lines.append(
            f"{fund['name']} has {fund['top10_pct']:.0f}% in its top ten, though its top five "
            f"stay under {TOP5_NOTABLE_PCT:.0f}% — concentrated across ten names rather than a few."
        )

    overlapping = [
        row for row in look_through
        if (row.get("fund_count") or 0) > 1
        and (row.get("weight_pct") or 0) >= PORTFOLIO_STOCK_NOTABLE_PCT
    ]
    if overlapping:
        overlapping.sort(key=lambda row: -(row["weight_pct"] or 0))
        for row in overlapping[:5]:
            names = ", ".join(
                str(entry.get("name")) for entry in (row.get("funds") or [])[:4] if entry.get("name")
            )
            lines.append(
                f"{row['name']} is {row['weight_pct']:.1f}% of your entire portfolio, reached "
                f"through {row['fund_count']} different funds ({names}). Each fund looks "
                f"diversified on its own; together they are one position."
            )
    elif look_through:
        biggest = max(look_through, key=lambda row: row.get("weight_pct") or 0)
        lines.append(
            f"Your largest single stock exposure across every fund is {biggest['name']} at "
            f"{(biggest.get('weight_pct') or 0):.1f}% of the portfolio — no stock is quietly "
            "doubling up across funds."
        )

    return lines


def build(
    positions: list[dict[str, Any]],
    *,
    detail_for,
    look_through: list[dict[str, Any]],
) -> dict[str, Any]:
    """Concentration across the funds currently held.

    Sold positions are excluded: a fund you no longer own carries no risk, and
    including it would pad the comparison with irrelevant rows.
    """
    funds: list[dict[str, Any]] = []
    for position in positions:
        if (position.get("units") or 0) <= 0:
            continue
        row = position.get("fund") or {}
        detail, _stale = detail_for(str(position["scheme_code"]), row.get("slug"))
        if not (detail or {}).get("holdings") and row.get("growth_sibling_code"):
            # An IDCW or Payout plan is the same portfolio as its Growth
            # sibling — same manager, same stocks — so the sibling's disclosed
            # holdings are this fund's holdings. Without this, a portfolio full
            # of dividend plans reports no concentration at all.
            detail, _stale = detail_for(
                str(row["growth_sibling_code"]), row.get("growth_sibling_slug")
            )
        analysed = analyse_fund({**row, "scheme_code": position["scheme_code"]}, detail)
        analysed["portfolio_weight_pct"] = position.get("weight_pct")
        if analysed["top5_pct"] is not None or analysed["holdings_count"]:
            funds.append(analysed)

    funds.sort(key=lambda fund: -(fund["top5_pct"] or 0))
    return {
        "funds": funds,
        "concentrated_count": sum(1 for fund in funds if fund["concentrated"]),
        "thresholds": {
            "top5_pct": TOP5_NOTABLE_PCT,
            "top10_pct": TOP10_NOTABLE_PCT,
            "single_stock_pct": SINGLE_STOCK_NOTABLE_PCT,
            "portfolio_stock_pct": PORTFOLIO_STOCK_NOTABLE_PCT,
        },
        "summary": describe(funds, look_through),
    }
