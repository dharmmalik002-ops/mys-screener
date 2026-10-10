"""AI scanner: a sentence in, a filter set out, and the scanner picks the stocks.

"Stocks within 20% of their 52-week high with sales growing 20%+, forming a cup
and handle" goes to the model, which may only answer with filters from
`FIELDS` and pattern ids from `pattern_catalog()`. Everything it returns is
validated against `AiScanRequest`; anything it cannot map is returned as
`unsupported` so the page can say which part of the sentence was NOT applied.

The model never sees prices and never names a stock. The stocks come from the
same filter code as the Custom Scanner (`_passes_custom_filters`) and the same
pattern evaluators as the scanner catalog — the AI only translates. That is the
rule the rest of the app keeps for AI (gotcha 24): Python computes, the model
writes. It also means a result can be re-run and checked without the model,
which is what the editable criteria on the page do.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass
from datetime import date
from functools import partial
from typing import Any, Awaitable, Callable

from pydantic import ValidationError

from app.models.market import AiScanRequest, StockSnapshot
from app.scanners import chart_patterns
from app.scanners.definitions import (
    SCAN_BY_ID,
    _custom_score,
    _custom_sort_value,
    _near_high_distance,
    _passes_custom_filters,
    _return_for_period,
    build_scan_match,
    eligible_for_scan,
)
from app.services import quarterly_growth

logger = logging.getLogger(__name__)

MAX_QUERY_CHARS = 600
DEFAULT_LIMIT = 300


@dataclass(frozen=True)
class FilterField:
    key: str
    label: str
    kind: str  # "min" | "max" | "bool" | "enum" | "int"
    help: str
    unit: str = ""
    column: str | None = None  # metric column shown when this filter is set


def _f(key: str, label: str, kind: str, help: str, unit: str = "", column: str | None = None) -> FilterField:
    return FilterField(key, label, kind, help, unit, column)


# What the model may set. Each min/max pair is listed explicitly so the prompt
# and the page agree on the meaning of every key.
FIELDS: tuple[FilterField, ...] = (
    # Price and size
    _f("min_price", "Price", "min", "last price in rupees", "₹", "price"),
    _f("max_price", "Price", "max", "last price in rupees", "₹", "price"),
    _f("min_market_cap_crore", "Market cap", "min", "market cap in crore. Large cap >= 100000, mid cap 33000-100000, small cap < 33000", " cr", "market_cap"),
    _f("max_market_cap_crore", "Market cap", "max", "market cap in crore", " cr", "market_cap"),
    _f("min_avg_rupee_turnover_20d_crore", "20D avg turnover", "min", "average daily traded value over 20 days, crore. 'liquid' means 10", " cr", "turnover"),
    # Distance from levels (all are positive percentages)
    _f("max_pct_from_52w_high", "Below 52W high", "max", "how far BELOW the 52-week high, in % (positive). 'within 20% of 52-week high' -> 20", "%", "from_52w_high"),
    _f("min_pct_from_52w_high", "Below 52W high", "min", "at least this % below the 52-week high", "%", "from_52w_high"),
    _f("min_pct_from_52w_low", "Above 52W low", "min", "how far ABOVE the 52-week low, in % (positive). '30% above 52-week low' -> 30", "%", "from_52w_low"),
    _f("max_pct_from_52w_low", "Above 52W low", "max", "at most this % above the 52-week low", "%", "from_52w_low"),
    _f("max_pct_from_ath", "Below all-time high", "max", "how far BELOW the all-time high, in % (positive)", "%", "from_ath"),
    _f("min_pct_from_ath", "Below all-time high", "min", "at least this % below the all-time high", "%", "from_ath"),
    # Today
    _f("min_change_pct", "Today's change", "min", "today's % change", "%", "change"),
    _f("max_change_pct", "Today's change", "max", "today's % change", "%", "change"),
    _f("min_gap_pct", "Gap up", "min", "today's opening gap in %", "%", "gap"),
    _f("min_relative_volume", "Relative volume", "min", "today's volume / 20-day average, e.g. 2 means twice normal", "x", "rvol"),
    _f("min_day_range_pct", "Day range", "min", "today's high-low range as % of price", "%", None),
    _f("max_day_range_pct", "Day range", "max", "today's high-low range as % of price", "%", None),
    # Volatility
    _f("min_adr_pct_20", "ADR (20D)", "min", "average daily range % over 20 days; movers >= 4", "%", "adr"),
    _f("max_adr_pct_20", "ADR (20D)", "max", "average daily range % over 20 days; quiet names <= 3", "%", "adr"),
    _f("max_consolidation_range_pct", "Consolidation range", "max", "tight consolidation: high-low range of the last 15 or 25 sessions within this %. 'tight' -> 10", "%", None),
    # Relative strength and returns
    _f("min_rs_rating", "RS rating", "min", "IBD-style relative strength rating 1-99; 'strong RS' -> 80", "", "rs"),
    _f("max_rs_rating", "RS rating", "max", "relative strength rating 1-99", "", "rs"),
    _f("min_nifty_outperformance", "vs Nifty (20D)", "min", "20-day return minus the benchmark's, in percentage points", "pp", None),
    _f("min_three_month_rs", "3M relative strength", "min", "3-month return relative to the benchmark, in percentage points", "pp", None),
    _f("return_period", "Return period", "enum", "period for min/max_return_pct: one of 1D, 1W, 1M, 3M, 6M, 1Y", "", None),
    _f("min_return_pct", "Return", "min", "minimum % return over return_period", "%", "return"),
    _f("max_return_pct", "Return", "max", "maximum % return over return_period", "%", "return"),
    # Moving averages
    _f("above_ema20", "Above 20 EMA", "bool", "close above the 20-day EMA (also for '21 EMA')", "", None),
    _f("above_ema50", "Above 50 EMA", "bool", "close above the 50-day EMA (also for '50 DMA')", "", None),
    _f("above_ema200", "Above 200 EMA", "bool", "close above the 200-day EMA (also for '200 DMA')", "", None),
    _f("price_vs_ma_mode", "Price vs MA", "enum", "'below' with price_vs_ma_key for 'below the 50 EMA'; one of any, above, below", "", None),
    _f("price_vs_ma_key", "MA", "enum", "one of ema10, ema20, ema50, ema200", "", None),
    _f("require_bullish_ma_order", "Bullish MA stack", "bool", "price > 20 EMA > 50 EMA > 200 EMA", "", None),
    _f("require_bearish_ma_order", "Bearish MA stack", "bool", "price < 20 EMA < 50 EMA < 200 EMA", "", None),
    _f("price_to_ma_key", "Extension MA", "enum", "MA used by min/max_price_to_ma_ratio; one of ema10, ema20, ema50, ema200", "", None),
    _f("max_price_to_ma_ratio", "Price / MA", "max", "price divided by price_to_ma_key, e.g. 1.05 = at most 5% above it ('not extended')", "x", None),
    _f("min_price_to_ma_ratio", "Price / MA", "min", "price divided by price_to_ma_key", "x", None),
    # Setups expressed as filters
    _f("minervini_trend_template", "Minervini trend template", "bool", "Stage 2 / trend template (all 8 rules incl. RS 70+)", "", None),
    _f("kullamagi_setup", "Qullamaggie momentum", "bool", "above 20 EMA, +20% in 3 months, strong trend", "", None),
    _f("shakeout_21ema", "Shakeout of 21 EMA", "bool", "dipped under the 21 EMA in the last 5 sessions and closed back above", "", None),
    _f("shakeout_50ema", "Shakeout of 50 EMA", "bool", "dipped under the 50 EMA in the last 5 sessions and closed back above", "", None),
    _f("hide_low_band", "Hide 2%/5% circuit stocks", "bool", "drop stocks in the 2% or 5% price band", "", None),
    _f("listing_date_from", "Listed from", "enum", "listing date on or after, YYYY-MM-DD (for 'IPOs since ...')", "", None),
    _f("listing_date_to", "Listed until", "enum", "listing date on or before, YYYY-MM-DD", "", None),
    # Quarterly fundamentals (BSE filings, standalone)
    _f("min_sales_growth_yoy_pct", "Sales growth YoY", "min", "latest quarter's sales vs the same quarter last year, %. DEFAULT reading of 'sales/revenue growing X%'", "%", "sales_yoy"),
    _f("max_sales_growth_yoy_pct", "Sales growth YoY", "max", "latest quarter's sales growth YoY, %", "%", "sales_yoy"),
    _f("min_profit_growth_yoy_pct", "Net profit growth YoY", "min", "latest quarter's net profit vs the same quarter last year, %. Use this for 'EPS growth' and 'earnings growth' too", "%", "profit_yoy"),
    _f("max_profit_growth_yoy_pct", "Net profit growth YoY", "max", "latest quarter's net profit growth YoY, %", "%", "profit_yoy"),
    _f("min_sales_growth_qoq_pct", "Sales growth QoQ", "min", "latest quarter vs the previous quarter, % (only when the user says QoQ / sequential)", "%", "sales_qoq"),
    _f("min_profit_growth_qoq_pct", "Profit growth QoQ", "min", "net profit, latest quarter vs previous quarter, %", "%", "profit_qoq"),
    _f("min_sales_growth_ttm_pct", "Sales growth TTM", "min", "last 4 quarters vs the 4 before, % (only for TTM / annual / 12-month growth)", "%", "sales_ttm"),
    _f("min_profit_growth_ttm_pct", "Profit growth TTM", "min", "net profit, last 4 quarters vs the 4 before, %", "%", "profit_ttm"),
    _f("growth_quarters", "Quarters in a row", "int", "1-4: the YoY sales/profit floors must hold in each of the newest N quarters ('consistent', 'last 3 quarters')", "", None),
    _f("min_operating_margin_pct", "Operating margin", "min", "latest quarter operating margin, %", "%", "op_margin"),
    _f("max_operating_margin_pct", "Operating margin", "max", "latest quarter operating margin, %", "%", "op_margin"),
    _f("min_net_margin_pct", "Net margin", "min", "latest quarter net profit / sales, %", "%", "net_margin"),
    _f("min_operating_margin_change_yoy_pp", "Margin change YoY", "min", "operating margin change vs the same quarter last year, percentage points; 'margins expanding' -> 0", "pp", "margin_change"),
    _f("require_quarterly_profit", "Profitable last quarter", "bool", "latest quarter net profit above zero", "", None),
    # Pattern options
    _f("resistance_max_below_pct", "Max below resistance", "max", "with near-resistance / ascending-triangle: how close under the level, % (default 5)", "%", None),
    _f("sort_by", "Sort", "enum", "one of pattern, price, change_pct, relative_volume, rs_rating, three_month_rs, stock_return_20d, stock_return_60d, stock_return_12m, market_cap, avg_rupee_volume", "", None),
    _f("sort_order", "Order", "enum", "asc or desc", "", None),
    _f("limit", "Max results", "int", "1-1000", "", None),
)
FIELD_BY_KEY = {field.key: field for field in FIELDS}
# Keys that only shape another key and are never shown as a criterion of their own.
_MODIFIER_KEYS = {"return_period", "price_vs_ma_key", "price_to_ma_key", "sort_by", "sort_order", "limit", "pattern_match"}


@dataclass(frozen=True)
class Column:
    id: str
    label: str
    format: str  # "pct" | "num" | "crore" | "x" | "pp" | "price"


COLUMNS: dict[str, tuple[Column, Callable[[StockSnapshot, Any, AiScanRequest], float | None]]] = {
    "price": (Column("price", "Price", "price"), lambda s, g, r: s.last_price),
    "market_cap": (Column("market_cap", "M cap", "crore"), lambda s, g, r: s.market_cap_crore),
    "turnover": (Column("turnover", "Turnover 20D", "crore"), lambda s, g, r: s.avg_rupee_turnover_20d_crore),
    "from_52w_high": (Column("from_52w_high", "Below 52W high", "pct"), lambda s, g, r: s.pct_from_52w_high),
    "from_52w_low": (Column("from_52w_low", "Above 52W low", "pct"), lambda s, g, r: s.pct_from_52w_low),
    "from_ath": (Column("from_ath", "Below ATH", "pct"), lambda s, g, r: s.pct_from_ath),
    "change": (Column("change", "Chg", "pct"), lambda s, g, r: s.change_pct),
    "gap": (Column("gap", "Gap", "pct"), lambda s, g, r: s.gap_pct),
    "rvol": (Column("rvol", "RVOL", "x"), lambda s, g, r: s.relative_volume),
    "adr": (Column("adr", "ADR 20D", "pct"), lambda s, g, r: s.adr_pct_20),
    "rs": (Column("rs", "RS", "num"), lambda s, g, r: s.rs_rating if s.rs_eligible else None),
    "return": (Column("return", "Return", "pct"), lambda s, g, r: _return_for_period(s, r.return_period)),
    "sales_yoy": (Column("sales_yoy", "Sales YoY", "pct"), lambda s, g, r: g.sales_growth_yoy_pct if g else None),
    "profit_yoy": (Column("profit_yoy", "Profit YoY", "pct"), lambda s, g, r: g.profit_growth_yoy_pct if g else None),
    "sales_qoq": (Column("sales_qoq", "Sales QoQ", "pct"), lambda s, g, r: g.sales_growth_qoq_pct if g else None),
    "profit_qoq": (Column("profit_qoq", "Profit QoQ", "pct"), lambda s, g, r: g.profit_growth_qoq_pct if g else None),
    "sales_ttm": (Column("sales_ttm", "Sales TTM", "pct"), lambda s, g, r: g.sales_growth_ttm_pct if g else None),
    "profit_ttm": (Column("profit_ttm", "Profit TTM", "pct"), lambda s, g, r: g.profit_growth_ttm_pct if g else None),
    "op_margin": (Column("op_margin", "Op margin", "pct"), lambda s, g, r: g.operating_margin_pct if g else None),
    "net_margin": (Column("net_margin", "Net margin", "pct"), lambda s, g, r: g.net_margin_pct if g else None),
    "margin_change": (Column("margin_change", "Margin Δ YoY", "pp"), lambda s, g, r: g.operating_margin_change_yoy_pp if g else None),
}

# ---------------------------------------------------------------------------
# Pattern catalog


@dataclass(frozen=True)
class PatternEntry:
    id: str
    name: str
    description: str
    evaluator: Callable[[StockSnapshot], Any]
    source: str  # "catalog" | "chart"


def pattern_catalog(resistance_max_below_pct: float | None = None) -> dict[str, PatternEntry]:
    out: dict[str, PatternEntry] = {}
    for scan_id, scan in SCAN_BY_ID.items():
        out[scan_id] = PatternEntry(scan_id, scan.name, scan.description, scan.evaluator, "catalog")
    for pattern in chart_patterns.CHART_PATTERNS:
        evaluator = pattern.evaluator
        if resistance_max_below_pct is not None and pattern.id in {"near-resistance", "ascending-triangle"}:
            evaluator = partial(_resistance_with_distance, pattern.id, resistance_max_below_pct)
        out[pattern.id] = PatternEntry(pattern.id, pattern.name, pattern.description, evaluator, "chart")
    return out


def _resistance_with_distance(pattern_id: str, max_below: float, snapshot: StockSnapshot):
    found = chart_patterns.detect_resistance(
        chart_patterns.close_history.closes_for(snapshot), max_below_pct=max_below
    )
    if found is None:
        return None
    if pattern_id == "ascending-triangle":
        if found.higher_lows < 2:
            return None
        return 78 + found.higher_lows * 3, [
            f"Flat top at {found.level:g} ({found.touches} touches), {found.pct_below:.1f}% below",
            f"{found.higher_lows + 1} rising lows underneath",
        ]
    return 75 + found.touches * 3, [
        f"{found.pct_below:.1f}% below resistance at {found.level:g}",
        f"Turned back {found.touches} times over {found.sessions} sessions",
    ]


# How traders say it -> pattern id. Shown to the model; the model decides.
PATTERN_SYNONYMS = {
    "near-resistance": "near the pivot, just under resistance, knocking on resistance, approaching a breakout, coiling under the high, near pivot point",
    "cup-handle": "cup and handle, cup with handle, CwH",
    "vcp": "VCP, volatility contraction, Minervini contraction",
    "high-tight-flag": "high tight flag, HTF, power play",
    "tight-closes": "tight closes, 3 tight closes, tight action (daily; NOT weekly 3-weeks-tight)",
    "ascending-triangle": "ascending triangle, flat top with higher lows",
    "flat-base": "flat base, sideways base, shelf",
    "double-bottom": "double bottom, W pattern, W base",
    "inside-day": "inside day, inside bar",
    "nr7": "NR7, narrow range 7, narrowest range",
    "pocket-pivot": "pocket pivot",
    "darvas-box": "Darvas box",
    "pivot-breakout": "breaking out of a pivot / swing high today",
    "breakout-52w": "52-week high breakout, new 52 week high breakout",
    "breakout-ath": "all-time high breakout",
    "breakout-range": "range breakout, breaking out of a 20-day range",
    "episodic-pivot": "episodic pivot, EP, gap up on huge volume from a base",
    "qullamaggie": "Qullamaggie / Kristjan setup, momentum base after a big move",
    "power-base": "power base, first leg then consolidation",
    "clean-pullback": "clean pullback, orderly pullback in an uptrend",
    "contraction": "3-day contraction above the 50 EMA",
    "rs-line-leads": "RS line at new high before price",
    "minervini-1m": "Stage 2 / Minervini trend template (as a pattern)",
}


# ---------------------------------------------------------------------------
# Prompt


def _field_lines() -> str:
    lines = []
    for field in FIELDS:
        kind = {"min": "number (lower bound)", "max": "number (upper bound)", "bool": "true", "int": "integer", "enum": "string"}[field.kind]
        lines.append(f"- {field.key}: {kind} — {field.help}")
    return "\n".join(lines)


def _pattern_lines() -> str:
    lines = []
    for entry in pattern_catalog().values():
        said = PATTERN_SYNONYMS.get(entry.id)
        line = f"- {entry.id}: {entry.name}. {entry.description}"
        if said:
            line += f" Traders say: {said}."
        lines.append(line)
    return "\n".join(lines)


def build_prompt(query: str, today: date | None = None) -> str:
    today_iso = (today or date.today()).isoformat()
    return f"""You translate a trader's request into filters for an Indian stock scanner (NSE/BSE). Today is {today_iso}.
You do NOT pick stocks and you do NOT know prices. You only choose filters and patterns from the lists below.

Request: \"\"\"{query}\"\"\"

FILTERS you may set (omit everything the request does not ask for):
{_field_lines()}

PATTERNS you may ask for (ids):
{_pattern_lines()}

Rules:
1. Use only the keys and pattern ids listed. Never invent a key.
2. "Within X% of the 52-week high" -> max_pct_from_52w_high = X. "Near the 52-week high" with no number -> 5 and add a note.
3. "Sales / revenue growing X%" -> min_sales_growth_yoy_pct = X (latest quarter vs same quarter last year) unless the request says QoQ, sequential, TTM, annual or yearly. "Consistent" or "last N quarters" -> growth_quarters = N (3 if no number).
4. "EPS growth" or "earnings growth" -> min_profit_growth_yoy_pct, and add a note: EPS as filed is distorted by splits and bonuses, so net profit growth is used.
5. Several patterns joined by "or" -> pattern_match "any"; "and" -> "all". One pattern -> "any".
6. If a pattern only roughly fits (e.g. weekly "3 weeks tight" -> tight-closes), use it and add a note saying it is an approximation.
7. Anything you cannot express with these filters and patterns (e.g. P/E, ROE, debt, promoter holding, FII buying, head and shoulders, management quality, news) goes in "unsupported", quoting the user's words, with a short reason. Do not drop it silently and do not approximate it with an unrelated filter.
8. A number the user gives is used exactly. Defaults you choose for vague words ("liquid", "strong RS", "tight", "large cap") must be mentioned in "notes".
9. Percentages are plain numbers (20 means 20%). Market cap and turnover are in crore.

Reply with one JSON object only:
{{"filters": {{"<key>": <value>}}, "patterns": ["<id>"], "pattern_match": "any" or "all", "unsupported": [{{"text": "<user words>", "reason": "<why>"}}], "notes": ["<short note>"], "summary": "<one sentence restating what will be scanned>"}}"""


# ---------------------------------------------------------------------------
# Parsing the model's answer


def _coerce(field: FilterField, value: Any) -> Any:
    if field.kind in {"min", "max"}:
        if isinstance(value, bool):
            raise ValueError("expected a number")
        if isinstance(value, str):
            value = value.replace("%", "").replace(",", "").strip()
        return float(value)
    if field.kind == "int":
        return int(float(value))
    if field.kind == "bool":
        if isinstance(value, str):
            return value.strip().lower() in {"true", "yes", "1"}
        return bool(value)
    return str(value).strip()


def sanitize(raw: dict[str, Any]) -> dict[str, Any]:
    """Model output -> {"request": AiScanRequest, "unsupported", "notes", "summary"}.

    Unknown keys, unknown patterns and values the request model rejects are
    moved to `unsupported` rather than silently dropped."""
    unsupported: list[dict[str, str]] = []
    for item in raw.get("unsupported") or []:
        if isinstance(item, dict) and str(item.get("text") or "").strip():
            unsupported.append({"text": str(item["text"]).strip()[:200], "reason": str(item.get("reason") or "").strip()[:200]})
        elif isinstance(item, str) and item.strip():
            unsupported.append({"text": item.strip()[:200], "reason": ""})
    notes = [str(note).strip()[:240] for note in (raw.get("notes") or []) if str(note).strip()][:8]

    filters: dict[str, Any] = {}
    raw_filters = raw.get("filters") if isinstance(raw.get("filters"), dict) else {}
    for key, value in raw_filters.items():
        if value is None or value is False:
            continue
        field = FIELD_BY_KEY.get(key)
        if field is None:
            unsupported.append({"text": str(key).replace("_", " "), "reason": "not a filter this scanner has"})
            continue
        try:
            coerced = _coerce(field, value)
        except (TypeError, ValueError):
            unsupported.append({"text": f"{field.label}: {value}", "reason": "value could not be read"})
            continue
        if field.kind == "bool" and not coerced:
            continue
        filters[key] = coerced

    catalog = pattern_catalog()
    patterns: list[str] = []
    for pattern_id in raw.get("patterns") or []:
        pattern_id = str(pattern_id).strip()
        if pattern_id in catalog and pattern_id not in patterns:
            patterns.append(pattern_id)
        elif pattern_id:
            unsupported.append({"text": pattern_id.replace("-", " "), "reason": "not a pattern this scanner can detect"})
    match = raw.get("pattern_match") if raw.get("pattern_match") in {"any", "all"} else "any"

    payload = {**filters, "patterns": patterns[:8], "pattern_match": match}
    payload.setdefault("limit", DEFAULT_LIMIT)
    request = None
    for _ in range(len(payload) + 1):
        try:
            request = AiScanRequest.model_validate(payload)
            break
        except ValidationError as error:
            bad = {str(err["loc"][0]) for err in error.errors() if err.get("loc")}
            if not bad or not bad & payload.keys():
                raise
            for key in bad & payload.keys():
                field = FIELD_BY_KEY.get(key)
                unsupported.append({"text": f"{field.label if field else key}: {payload[key]}", "reason": "value out of range"})
                payload.pop(key)
    if request is None:
        request = AiScanRequest(limit=DEFAULT_LIMIT)

    return {
        "request": request,
        "unsupported": unsupported,
        "notes": notes,
        "summary": str(raw.get("summary") or "").strip()[:400],
    }


# ---------------------------------------------------------------------------
# Describing a request (chips on the page)


def _fmt_number(value: float) -> str:
    return f"{value:g}" if abs(value) < 1e6 else f"{value:,.0f}"


def describe(request: AiScanRequest) -> list[dict[str, Any]]:
    """Every active filter as {key, label, text, value, kind} for the page."""
    defaults = AiScanRequest()
    out: list[dict[str, Any]] = []
    data = request.model_dump(mode="json")
    for field in FIELDS:
        if field.key in _MODIFIER_KEYS:
            continue
        value = data.get(field.key)
        if value is None or value == getattr(defaults, field.key, None) and field.key != "growth_quarters":
            continue
        if field.key == "growth_quarters" and value == 1:
            continue
        if field.kind == "bool":
            if not value:
                continue
            text = field.label
        elif field.kind in {"min", "max"}:
            sign = "≥" if field.kind == "min" else "≤"
            unit = field.unit
            shown = f"₹{_fmt_number(value)}" if unit == "₹" else f"{_fmt_number(value)}{unit}"
            label = field.label
            if field.key in {"min_return_pct", "max_return_pct"}:
                label = f"{request.return_period} return"
            if field.key in {"min_price_to_ma_ratio", "max_price_to_ma_ratio"}:
                label = f"Price / {request.price_to_ma_key.upper()}"
            text = f"{label} {sign} {shown}"
        elif field.key == "price_vs_ma_mode":
            if value == "any":
                continue
            text = f"Price {value} {request.price_vs_ma_key.upper()}"
        elif field.key == "growth_quarters":
            text = f"Growth held {value} quarters in a row"
        else:
            text = f"{field.label}: {value}"
        out.append({"key": field.key, "kind": field.kind, "value": value, "text": text, "group": _group(field.key)})
    return out


def _group(key: str) -> str:
    if any(part in key for part in ("sales", "profit", "margin", "eps", "growth_quarters")):
        return "fundamental"
    return "technical"


def columns_for(request: AiScanRequest) -> list[str]:
    ids: list[str] = []
    data = request.model_dump(mode="json")
    for field in FIELDS:
        if field.column and data.get(field.key) not in (None, False) and field.column not in ids:
            ids.append(field.column)
    return ids


# ---------------------------------------------------------------------------
# Running


def run(request: AiScanRequest, snapshots: list[StockSnapshot]) -> dict[str, Any]:
    catalog = pattern_catalog(request.resistance_max_below_pct)
    patterns = [catalog[pid] for pid in request.patterns if pid in catalog]
    # The pattern lives in `patterns`; the Custom Scanner's single pattern
    # slot stays "any" so _passes_custom_filters does not apply it twice.
    base = request.model_copy(update={"pattern": "any"})

    eligible: dict[str, set[str]] = {}
    for entry in patterns:
        if entry.source == "catalog":
            eligible[entry.id] = {s.symbol for s in eligible_for_scan(entry.id, snapshots)}

    column_ids = columns_for(request)
    items: list[dict[str, Any]] = []
    matches = []
    for snapshot in snapshots:
        if not _passes_custom_filters(snapshot, base):
            continue
        hits: list[tuple[str, float, list[str]]] = []
        for entry in patterns:
            if entry.id in eligible and snapshot.symbol not in eligible[entry.id]:
                continue
            try:
                outcome = entry.evaluator(snapshot)
            except Exception:  # one broken evaluator must not sink the scan
                logger.exception("pattern %s failed on %s", entry.id, snapshot.symbol)
                outcome = None
            if outcome:
                hits.append((entry.name, float(outcome[0]), list(outcome[1])))
        if patterns:
            if request.pattern_match == "all" and len(hits) < len(patterns):
                continue
            if not hits:
                continue
        if hits:
            score = max(hit[1] for hit in hits)
            reasons = [f"{hits[0][0]}: {hits[0][2][0]}"] + hits[0][2][1:]
            for name, _, extra in hits[1:]:
                reasons.append(f"{name}: {extra[0]}" if extra else name)
            pattern_name = " + ".join(hit[0] for hit in hits)
        else:
            try:
                score, reasons, pattern_name = _custom_score(snapshot, base)
            except ValueError:
                continue
            reasons = [reason for reason in reasons if reason != "Custom filter match"]
        growth = quarterly_growth.for_symbol(snapshot.symbol) if any(_group(k) == "fundamental" for k in _active_keys(request)) else None
        if growth is not None:
            reasons.append(f"Latest quarter {growth.latest_period}")
        match = build_scan_match("ai-scan", snapshot, score, reasons[:6], pattern=pattern_name)
        metrics = {}
        for column_id in column_ids:
            getter = COLUMNS[column_id][1]
            try:
                value = getter(snapshot, growth, request)
            except Exception:
                value = None
            metrics[column_id] = round(float(value), 2) if isinstance(value, (int, float)) and value == value else None
        matches.append((match, metrics, [hit[0] for hit in hits], growth.latest_period if growth else None))

    reverse = request.sort_order == "desc"
    matches.sort(
        key=lambda row: (_custom_sort_value(row[0], request.sort_by), row[0].score, row[0].last_price),
        reverse=reverse,
    )
    total = len(matches)
    for match, metrics, matched, period in matches[: request.limit]:
        row = match.model_dump(mode="json")
        row["metrics"] = metrics
        row["matched_patterns"] = matched
        row["latest_quarter"] = period
        items.append(row)

    return {
        "items": items,
        "hit_count": total,
        "universe_count": len(snapshots),
        "columns": [asdict(COLUMNS[cid][0]) for cid in column_ids],
        "criteria": describe(request),
        "patterns": [{"id": entry.id, "name": entry.name, "description": entry.description} for entry in patterns],
        "pattern_match": request.pattern_match,
        "request": request.model_dump(mode="json"),
        "fundamentals_as_of": quarterly_growth.artifact_date(),
    }


def _active_keys(request: AiScanRequest) -> list[str]:
    return [criterion["key"] for criterion in describe(request)]


# ---------------------------------------------------------------------------
# The model call, cached per query


_cache_lock = threading.Lock()
_parse_cache: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_CACHE_SIZE = 200


def normalise_query(query: str) -> str:
    return re.sub(r"\s+", " ", (query or "").strip().lower())


async def parse_with(generate_json: Callable[[str], Awaitable[dict[str, Any]]], query: str) -> dict[str, Any]:
    """`await generate_json(prompt) -> dict` is the model call. Returns the parsed,
    validated result in JSON form (request as a dict) and caches it per query
    for the day."""
    query = (query or "").strip()[:MAX_QUERY_CHARS]
    key = f"{date.today().isoformat()}|{normalise_query(query)}"
    with _cache_lock:
        if key in _parse_cache:
            _parse_cache.move_to_end(key)
            return json.loads(json.dumps(_parse_cache[key]))
    raw = await generate_json(build_prompt(query))
    if not isinstance(raw, dict) or "filters" not in raw and "patterns" not in raw:
        raise ValueError("The AI did not return a filter set")
    parsed = sanitize(raw)
    request: AiScanRequest = parsed["request"]
    result = {
        "query": query,
        "summary": parsed["summary"],
        "notes": parsed["notes"],
        "unsupported": parsed["unsupported"],
        "criteria": describe(request),
        "patterns": [
            {"id": pid, "name": pattern_catalog()[pid].name} for pid in request.patterns
        ],
        "request": request.model_dump(mode="json"),
    }
    with _cache_lock:
        _parse_cache[key] = result
        while len(_parse_cache) > _CACHE_SIZE:
            _parse_cache.popitem(last=False)
    return json.loads(json.dumps(result))


def catalog_payload() -> dict[str, Any]:
    """Pattern list and filter fields, so the page can add criteria by hand."""
    return {
        "patterns": [
            {"id": entry.id, "name": entry.name, "description": entry.description}
            for entry in pattern_catalog().values()
        ],
        "fields": [
            {"key": field.key, "label": field.label, "kind": field.kind, "unit": field.unit, "help": field.help}
            for field in FIELDS
        ],
    }
