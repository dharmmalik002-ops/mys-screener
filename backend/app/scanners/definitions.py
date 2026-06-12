import bisect
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from app.models.market import (
    ConsolidatingScanRequest,
    CustomScanRequest,
    EandCScanRequest,
    MomentumBurstPlan,
    MomentumBurstScanRequest,
    ReturnsScanRequest,
    ScanDescriptor,
    ScanMatch,
    StockSnapshot,
)


ScannerFn = Callable[[StockSnapshot], tuple[float, list[str]] | None]

CONSOLIDATING_RUN_UP_LABEL = "Long Consolidation After a Run-Up"
CONSOLIDATING_BREAKOUT_LABEL = "Near Multi-Year Breakout"
CONSOLIDATING_TIGHTNESS_WINDOW_DAYS = 15
CONSOLIDATING_MAX_TIGHTNESS_RANGE_PCT = 9.0
CONSOLIDATING_BASELINE_CLOSE_TO_52W_HIGH_RATIO = 0.60
CONSOLIDATING_BASE_LOW_TO_52W_HIGH_RATIO = 0.60
CONSOLIDATING_DEEP_BASE_RECOVERY_RATIO = 0.90
CONSOLIDATING_BREAKOUT_PROXIMITY_RATIO = 0.92
CONSOLIDATING_MIN_BREAKOUT_VOLUME = 100_000


@dataclass(frozen=True)
class ScanDefinition:
    id: str
    name: str
    category: str
    description: str
    evaluator: ScannerFn


def scanner_sector_label(sector: str | None, sub_sector: str | None) -> str:
    sector_label = str(sector or "").strip() or "Unclassified"
    sub_sector_label = str(sub_sector or "").strip() or "Unclassified"

    if sector_label == "Financial Services":
        normalized = {
            "Private Sector Bank": "Private Sector Banks",
            "Public Sector Bank": "PSU Banks",
            "Other Bank": "Other Banks",
            "Non Banking Financial Company (NBFC)": "NBFCs",
            "Housing Finance Company": "Housing Finance",
            "Asset Management Company": "Asset Management",
            "Life Insurance": "Life Insurance",
            "General Insurance": "General Insurance",
        }.get(sub_sector_label)
        if normalized:
            return normalized

    if sector_label == "Capital Goods" and sub_sector_label not in {"Unclassified", "Capital Goods"}:
        return f"Capital Goods - {sub_sector_label}"

    if sector_label == "Unclassified" and sub_sector_label != "Unclassified":
        return sub_sector_label

    return sector_label


def _gap_from_level(current: float, level: float) -> float:
    if level == 0:
        return 0.0
    return ((current / level) - 1) * 100


def _near_or_above(current: float, level: float, tolerance_pct: float) -> bool:
    return _gap_from_level(current, level) >= -tolerance_pct


def _near_or_below(current: float, level: float, tolerance_pct: float) -> bool:
    return _gap_from_level(current, level) <= tolerance_pct


def _request_filter_value(request: CustomScanRequest, field_name: str, default=None):
    # Custom scan filters evolve over time; use the serialized payload so removed
    # optional fields stay absent instead of crashing attribute access.
    return request.model_dump(mode="python").get(field_name, default)


def _price_near_day_high(snapshot: StockSnapshot, tolerance_pct: float = 0.35) -> bool:
    return _near_or_above(snapshot.last_price, snapshot.day_high, tolerance_pct)


def _price_near_day_low(snapshot: StockSnapshot, tolerance_pct: float = 0.45) -> bool:
    return _near_or_below(snapshot.last_price, snapshot.day_low, tolerance_pct)


def _bullish_setup(snapshot: StockSnapshot) -> bool:
    return snapshot.ema_stack_bullish and snapshot.trend_strength >= 0.65


def _bearish_setup(snapshot: StockSnapshot) -> bool:
    return snapshot.ema_stack_bearish and snapshot.change_pct <= 0


def _bullish_breakout_ready(snapshot: StockSnapshot) -> bool:
    return (
        _bullish_setup(snapshot)
        and snapshot.relative_volume >= 1.25
        and snapshot.stock_return_20d >= 4
        and snapshot.nifty_outperformance >= 2
        and _price_near_day_high(snapshot)
    )


def _box_depth_pct(top: float, bottom: float) -> float:
    if top <= 0:
        return 0.0
    return ((top - bottom) / top) * 100


def _range_pct_from_low(high: float, low: float) -> float:
    if low <= 0:
        return 0.0
    return ((high - low) / low) * 100


def _day_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.day_high)
    if gap >= -0.25 and snapshot.change_pct >= 0.5:
        score = 74 + snapshot.change_pct + max(snapshot.relative_volume - 1, 0)
        return round(score, 2), ["Trading at session high", f"{gap:.2f}% from day high"]
    return None


def _day_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.day_low)
    if gap <= 0.35 and snapshot.change_pct <= -0.5:
        score = 74 + abs(snapshot.change_pct) + max(snapshot.relative_volume - 1, 0)
        return round(score, 2), ["Trading at session low", f"{gap:.2f}% from day low"]
    return None


def _near_day_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.day_high)
    if -1.35 <= gap < -0.2 and snapshot.change_pct >= 0:
        return round(68 + snapshot.change_pct + snapshot.trend_strength * 5, 2), ["Near day high", f"{gap:.2f}% below day high"]
    return None


def _near_day_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.day_low)
    if 0.25 < gap <= 1.75 and snapshot.change_pct <= 0.5:
        return round(68 + abs(min(snapshot.change_pct, 0)) + (1 - snapshot.trend_strength) * 5, 2), ["Near day low", f"{gap:.2f}% above day low"]
    return None


def _prev_day_high_break(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.previous_day_high_level)
    if gap >= 0.2 and snapshot.change_pct >= 1 and snapshot.relative_volume >= 1.15 and _price_near_day_high(snapshot, 0.45):
        return round(76 + gap * 10 + snapshot.relative_volume, 2), ["Previous day high break", f"Closed above {snapshot.previous_day_high_level:.2f}"]
    return None


def _prev_day_low_break(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.previous_day_low_level)
    if gap <= -0.2 and snapshot.change_pct <= -1 and snapshot.relative_volume >= 1.1 and _price_near_day_low(snapshot, 0.55):
        return round(76 + abs(gap) * 10 + snapshot.relative_volume, 2), ["Previous day low break", f"Closed below {snapshot.previous_day_low_level:.2f}"]
    return None


def _week_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.week_high_level)
    if gap >= -0.25 and snapshot.change_pct >= 0.75:
        score = 73 + snapshot.change_pct + snapshot.relative_volume * 0.8
        return round(score, 2), ["Weekly high", f"{gap:.2f}% from week high"]
    return None


def _week_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.week_low_level)
    if gap <= 0.45 and snapshot.change_pct <= -0.75:
        score = 73 + abs(snapshot.change_pct) + snapshot.relative_volume * 0.8
        return round(score, 2), ["Weekly low", f"{gap:.2f}% from week low"]
    return None


def _month_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.month_high_level)
    if gap >= -0.25 and snapshot.change_pct >= 1 and snapshot.relative_volume >= 1.1:
        return round(75 + snapshot.change_pct + snapshot.relative_volume, 2), ["Monthly high", f"{gap:.2f}% from month high"]
    return None


def _month_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.month_low_level)
    if gap <= 0.55 and snapshot.change_pct <= -1 and snapshot.relative_volume >= 1.0:
        return round(75 + abs(snapshot.change_pct) + snapshot.relative_volume, 2), ["Monthly low", f"{gap:.2f}% from month low"]
    return None


def _six_month_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.high_6m_level)
    if gap >= -0.3 and snapshot.stock_return_60d >= 10 and snapshot.relative_volume >= 1.15:
        return round(77 + snapshot.stock_return_60d * 0.2 + snapshot.relative_volume, 2), ["6-month high", f"60D return {snapshot.stock_return_60d:.2f}%"]
    return None


def _six_month_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.low_6m_level)
    if gap <= 0.7 and snapshot.change_pct <= -1:
        return round(72 + abs(snapshot.change_pct) + snapshot.relative_volume * 0.8, 2), ["6-month low", f"{gap:.2f}% from 6M low"]
    return None


def _high_52w(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.high_52w)
    if gap >= -0.2 and snapshot.change_pct >= 1 and snapshot.relative_volume >= 1.15:
        return round(79 + snapshot.change_pct + snapshot.relative_volume, 2), ["52-week high", f"{gap:.2f}% from 52W high"]
    return None


def _low_52w(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.low_52w_level)
    if gap <= 0.75 and snapshot.change_pct <= -1:
        return round(72 + abs(snapshot.change_pct) + snapshot.relative_volume, 2), ["52-week low", f"{gap:.2f}% from 52W low"]
    return None


def _near_52w_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.high_52w)
    if -1.5 <= gap < -0.2 and snapshot.stock_return_60d >= 6:
        return round(70 + snapshot.stock_return_60d * 0.15 + max(snapshot.change_pct, 0), 2), ["Near 52-week high", f"{gap:.2f}% below 52W high"]
    return None


def _near_52w_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.low_52w_level)
    if 0.5 <= gap <= 3 and snapshot.change_pct <= 0.5:
        return round(66 + abs(min(snapshot.change_pct, 0)) + max(snapshot.relative_volume - 1, 0), 2), ["Near 52-week low", f"{gap:.2f}% above 52W low"]
    return None


def _all_time_high(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.ath)
    if gap >= -0.2 and snapshot.change_pct >= 1 and snapshot.relative_volume >= 1.2:
        return round(82 + snapshot.change_pct + snapshot.relative_volume, 2), ["All-time high", f"{gap:.2f}% from ATH"]
    return None


def _all_time_low(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.atl_level)
    if gap <= 0.8 and snapshot.change_pct <= -1:
        return round(72 + abs(snapshot.change_pct) + snapshot.relative_volume, 2), ["All-time low", f"{gap:.2f}% from ATL"]
    return None


def _near_ath(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.ath)
    if -1.35 <= gap < -0.15 and snapshot.stock_return_20d >= 4 and _bullish_setup(snapshot):
        return round(73 + snapshot.stock_return_20d * 0.25 + snapshot.relative_volume, 2), ["Near ATH", f"{gap:.2f}% below ATH"]
    return None


def _near_atl(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.atl_level)
    if 0.5 <= gap <= 2 and _bearish_setup(snapshot):
        return round(67 + abs(snapshot.change_pct) + snapshot.relative_volume, 2), ["Near ATL", f"{gap:.2f}% above ATL"]
    return None


def _recent_ipo(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    listing_date = snapshot.listing_date
    if listing_date is None:
        return None

    today = date.today()
    days_since_listing = (today - listing_date).days
    if days_since_listing < 0 or days_since_listing > 365:
        return None

    recency_score = max(0.0, 365 - days_since_listing) * 0.08
    score = 72 + recency_score + max(snapshot.stock_return_20d, 0.0) * 0.18 + max(snapshot.change_pct, 0.0) * 0.6
    reasons = [
        f"Listed on {listing_date.isoformat()}",
        f"{days_since_listing} days since listing",
    ]
    if snapshot.stock_return_20d:
        reasons.append(f"20D return {snapshot.stock_return_20d:.2f}%")
    return round(score, 2), reasons[:3]


def _breakout_ath(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    level = snapshot.ath_breakout_level
    gap = _gap_from_level(snapshot.last_price, level)
    if gap >= 0.2 and _bullish_breakout_ready(snapshot) and snapshot.change_pct >= 1.5:
        score = 86 + gap * 14 + snapshot.relative_volume + snapshot.nifty_outperformance * 0.4
        return round(score, 2), ["ATH breakout", f"Closed above prior ATH {level:.2f}"]
    return None


def _breakout_52w(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    level = snapshot.previous_high_52w_level
    gap = _gap_from_level(snapshot.last_price, level)
    if gap >= 0.2 and _bullish_breakout_ready(snapshot) and snapshot.change_pct >= 1.4:
        score = 83 + gap * 12 + snapshot.relative_volume + snapshot.stock_return_60d * 0.1
        return round(score, 2), ["52-week breakout", f"Closed above prior 52W high {level:.2f}"]
    return None


def _range_breakout(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    level = snapshot.range_breakout_level
    gap = _gap_from_level(snapshot.last_price, level)
    if gap >= 0.25 and _bullish_breakout_ready(snapshot) and snapshot.change_pct >= 1.4:
        score = 79 + gap * 10 + snapshot.relative_volume + snapshot.stock_return_20d * 0.2
        return round(score, 2), ["20-day range breakout", f"Above prior 20D range high {level:.2f}"]
    return None


def _volume_price(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    if snapshot.relative_volume >= 2 and abs(snapshot.change_pct) >= 2.5:
        direction = "Bullish expansion" if snapshot.change_pct > 0 else "Bearish expansion"
        location_ok = _price_near_day_high(snapshot, 0.45) if snapshot.change_pct > 0 else _price_near_day_low(snapshot, 0.55)
        if location_ok:
            score = 72 + snapshot.relative_volume * 4 + abs(snapshot.change_pct)
            return round(score, 2), [direction, f"RVOL {snapshot.relative_volume:.2f}x"]
    return None


def _strong_vs_nifty(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    if snapshot.nifty_outperformance >= 5 and snapshot.stock_return_20d >= 4 and _bullish_setup(snapshot):
        score = 72 + snapshot.nifty_outperformance + snapshot.stock_return_60d * 0.18
        return round(score, 2), ["Strong vs Benchmark", f"+{snapshot.nifty_outperformance:.2f}% vs benchmark"]
    return None


def _strong_vs_sector(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    if snapshot.sector_outperformance >= 4 and snapshot.stock_return_20d >= 4 and _bullish_setup(snapshot):
        score = 70 + snapshot.sector_outperformance + snapshot.trend_strength * 10
        return round(score, 2), ["Strong vs sector", f"+{snapshot.sector_outperformance:.2f}% vs sector"]
    return None


def _clean_pullback(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    if snapshot.ema20 is None:
        return None
    if (
        _bullish_setup(snapshot)
        and 2 <= snapshot.pullback_depth_pct <= 6.5
        and snapshot.last_price >= snapshot.ema20 * 0.995
        and 0.7 <= snapshot.relative_volume <= 1.8
        and snapshot.stock_return_20d >= 4
    ):
        score = 74 + snapshot.trend_strength * 12 + snapshot.stock_return_20d * 0.3 - snapshot.pullback_depth_pct
        return round(score, 2), ["Clean pullback", f"Pullback depth {snapshot.pullback_depth_pct:.2f}%"]
    return None


def _darvas_box(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    gap = _gap_from_level(snapshot.last_price, snapshot.darvas_high)
    box_depth = _box_depth_pct(snapshot.darvas_high, snapshot.darvas_low)
    if gap >= 0.2 and _bullish_breakout_ready(snapshot) and snapshot.change_pct >= 1.4 and box_depth <= 18:
        score = 80 + gap * 12 + snapshot.relative_volume + snapshot.stock_return_20d * 0.15
        return round(score, 2), ["Darvas box breakout", f"Box top {snapshot.darvas_high:.2f} | depth {box_depth:.2f}%"]
    return None


def _pivot_breakout(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    pivot_level = max(snapshot.pivot_high, snapshot.range_breakout_level)
    gap = _gap_from_level(snapshot.last_price, pivot_level)
    base_is_tight = abs(_gap_from_level(snapshot.pivot_high, snapshot.range_breakout_level)) <= 3
    if (
        gap >= 0.2
        and base_is_tight
        and _bullish_breakout_ready(snapshot)
        and snapshot.change_pct >= 1.75
        and snapshot.relative_volume >= 1.45
        and snapshot.nifty_outperformance >= 4
    ):
        score = 84 + gap * 14 + snapshot.relative_volume + snapshot.nifty_outperformance * 0.45
        return round(score, 2), ["Pivot breakout", f"Recent swing-high pivot {pivot_level:.2f} cleared on strength"]
    return None


def _evaluate_run_up_consolidation(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    sma50 = snapshot.sma50
    sma200 = snapshot.sma200
    high_52w = snapshot.high_52w
    low_52w = snapshot.low_52w or snapshot.low_52w_level
    avg_volume_50d = snapshot.avg_volume_50d or snapshot.avg_volume_30d or snapshot.avg_volume_20d
    recent_highs = snapshot.recent_highs[-CONSOLIDATING_TIGHTNESS_WINDOW_DAYS :]
    recent_lows = snapshot.recent_lows[-CONSOLIDATING_TIGHTNESS_WINDOW_DAYS :]
    recent_volumes = snapshot.recent_volumes[-10:]

    if sma50 is None or sma200 is None or high_52w <= 0 or low_52w is None:
        return None
    if snapshot.last_price <= sma50 or snapshot.last_price <= sma200 or sma50 <= sma200:
        return None
    if snapshot.last_price < high_52w * CONSOLIDATING_BASELINE_CLOSE_TO_52W_HIGH_RATIO:
        return None
    if len(recent_highs) < CONSOLIDATING_TIGHTNESS_WINDOW_DAYS or len(recent_lows) < CONSOLIDATING_TIGHTNESS_WINDOW_DAYS:
        return None
    if avg_volume_50d <= 0 or len(recent_volumes) < 10:
        return None

    normal_base_ok = low_52w >= high_52w * CONSOLIDATING_BASE_LOW_TO_52W_HIGH_RATIO
    deep_base_recovery_ok = snapshot.last_price >= high_52w * CONSOLIDATING_DEEP_BASE_RECOVERY_RATIO
    if not (normal_base_ok or deep_base_recovery_ok):
        return None

    latest_high = max(recent_highs)
    latest_low = min(recent_lows)
    tightness_pct = _range_pct_from_low(latest_high, latest_low)
    if tightness_pct > CONSOLIDATING_MAX_TIGHTNESS_RANGE_PCT:
        return None

    recent_avg_volume = sum(recent_volumes) / len(recent_volumes)
    if recent_avg_volume >= avg_volume_50d:
        return None

    distance_from_high_pct = max(0.0, ((high_52w - snapshot.last_price) / high_52w) * 100)
    volume_dryup_ratio = 1 - (recent_avg_volume / avg_volume_50d)
    score = (
        82
        + max(0.0, CONSOLIDATING_MAX_TIGHTNESS_RANGE_PCT - tightness_pct) * 1.4
        + max(0.0, 10 - distance_from_high_pct) * 0.8
        + max(volume_dryup_ratio, 0.0) * 10
    )
    recovery_reason = (
        "Base held above 60% of the 52W high"
        if normal_base_ok
        else "Recovered to within 10% of the 52W high after a deeper base"
    )
    reasons = [
        recovery_reason,
        f"15D range tightened to {tightness_pct:.2f}%",
        f"10D avg volume is {(recent_avg_volume / avg_volume_50d):.2f}x of 50D avg",
    ]
    return round(score, 2), reasons


def _evaluate_near_multi_year_breakout(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    sma50 = snapshot.sma50
    reference_high = snapshot.high_3y or snapshot.multi_year_high or snapshot.ath

    if sma50 is None or reference_high is None or reference_high <= 0:
        return None
    if snapshot.last_price <= sma50:
        return None
    if snapshot.last_price < reference_high * CONSOLIDATING_BREAKOUT_PROXIMITY_RATIO:
        return None
    if snapshot.last_price > reference_high:
        return None
    if snapshot.volume < CONSOLIDATING_MIN_BREAKOUT_VOLUME:
        return None

    distance_from_high_pct = max(0.0, ((reference_high - snapshot.last_price) / reference_high) * 100)
    score = (
        84
        + max(0.0, 8 - distance_from_high_pct) * 1.6
        + min(snapshot.volume / CONSOLIDATING_MIN_BREAKOUT_VOLUME, 5.0) * 0.8
        + max(snapshot.stock_return_60d, 0.0) * 0.04
    )
    reasons = [
        f"Within {distance_from_high_pct:.2f}% of the 3Y high",
        f"Holding above the 50D SMA ({sma50:.2f})",
        f"Current volume {snapshot.volume:,}",
    ]
    return round(score, 2), reasons


def _evaluate_consolidating_matches(
    snapshot: StockSnapshot,
    request: ConsolidatingScanRequest,
) -> list[tuple[str, float, list[str]]]:
    if request.min_liquidity_crore is not None and snapshot.avg_rupee_volume_30d_crore < request.min_liquidity_crore:
        return []

    matches: list[tuple[str, float, list[str]]] = []
    if request.enable_run_up_consolidation:
        run_up = _evaluate_run_up_consolidation(snapshot)
        if run_up:
            score, reasons = run_up
            matches.append((CONSOLIDATING_RUN_UP_LABEL, score, reasons))
    if request.enable_near_multi_year_breakout:
        breakout = _evaluate_near_multi_year_breakout(snapshot)
        if breakout:
            score, reasons = breakout
            matches.append((CONSOLIDATING_BREAKOUT_LABEL, score, reasons))
    return matches


def _combine_consolidating_matches(
    matches: list[tuple[str, float, list[str]]],
) -> tuple[float, list[str], str] | None:
    if not matches:
        return None

    labels = [label for label, _, _ in matches]
    combined_reasons: list[str] = []
    if len(labels) > 1:
        combined_reasons.append(f"Matched both: {labels[0]} and {labels[1]}")
    else:
        combined_reasons.append(labels[0])

    for _, _, reasons in matches:
        for reason in reasons:
            if reason not in combined_reasons:
                combined_reasons.append(reason)

    combined_score = max(score for _, score, _ in matches) + (2.0 if len(matches) > 1 else 0.0)
    pattern = " + ".join(labels)
    return round(combined_score, 2), combined_reasons[:3], pattern


def evaluate_consolidating(snapshot: StockSnapshot, request: ConsolidatingScanRequest | None = None) -> tuple[float, list[str]] | None:
    combined = _combine_consolidating_matches(_evaluate_consolidating_matches(snapshot, request or ConsolidatingScanRequest()))
    if not combined:
        return None

    score, reasons, _ = combined
    return score, reasons


def _consolidating(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    return evaluate_consolidating(snapshot)


def _relative_strength(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    if (
        snapshot.rs_eligible
        and snapshot.rs_rating >= 80
        and snapshot.rs_composite >= 8
        and _bullish_setup(snapshot)
        and snapshot.stock_return_60d >= 8
    ):
        return round(72 + snapshot.rs_composite + (snapshot.rs_rating * 0.2), 2), [
            "Relative strength leader",
            f"RS Rating {snapshot.rs_rating}",
        ]
    return None


def _minervini_1m(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    sma50 = snapshot.sma50
    sma150 = snapshot.sma150
    sma200 = snapshot.sma200
    sma200_1m_ago = snapshot.sma200_1m_ago

    if sma50 is None or sma150 is None or sma200 is None or sma200_1m_ago is None:
        return None
    if snapshot.last_price <= sma50 or snapshot.last_price <= sma150 or snapshot.last_price <= sma200:
        return None
    if sma50 <= sma150 or sma50 <= sma200:
        return None
    if sma150 <= sma200:
        return None
    if sma200 <= sma200_1m_ago:
        return None
    if snapshot.pct_from_52w_low < 25:
        return None
    if snapshot.pct_from_52w_high > 25:
        return None

    distance_to_high_score = max(0.0, 25 - snapshot.pct_from_52w_high)
    distance_from_low_score = min(max(snapshot.pct_from_52w_low - 25, 0.0), 40.0)
    sma_trend_pct = ((sma200 / sma200_1m_ago) - 1) * 100 if sma200_1m_ago > 0 else 0.0
    score = (
        80
        + distance_to_high_score * 0.7
        + distance_from_low_score * 0.18
        + max(sma_trend_pct, 0.0) * 8
        + max(snapshot.stock_return_20d, 0.0) * 0.12
        + (snapshot.rs_rating if snapshot.rs_eligible else 0) * 0.08
    )
    reasons = [
        f"Price above 50/150/200 SMA ({sma50:.2f} / {sma150:.2f} / {sma200:.2f})",
        f"200 SMA up vs 1M ago ({sma200_1m_ago:.2f} -> {sma200:.2f})",
        f"{snapshot.pct_from_52w_low:.2f}% above 52W low and {snapshot.pct_from_52w_high:.2f}% below 52W high",
    ]
    return round(score, 2), reasons


def _minervini_5m(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    sma50 = snapshot.sma50
    sma150 = snapshot.sma150
    sma200 = snapshot.sma200
    sma200_1m_ago = snapshot.sma200_1m_ago
    sma200_5m_ago = snapshot.sma200_5m_ago

    if sma50 is None or sma150 is None or sma200 is None or sma200_1m_ago is None or sma200_5m_ago is None:
        return None
    if snapshot.last_price <= sma50 or snapshot.last_price <= sma150 or snapshot.last_price <= sma200:
        return None
    if sma50 <= sma150 or sma50 <= sma200:
        return None
    if sma150 <= sma200:
        return None
    if sma200 <= sma200_1m_ago or sma200 <= sma200_5m_ago:
        return None
    if snapshot.pct_from_52w_low < 30:
        return None
    if snapshot.pct_from_52w_high > 25:
        return None

    distance_to_high_score = max(0.0, 25 - snapshot.pct_from_52w_high)
    distance_from_low_score = min(max(snapshot.pct_from_52w_low - 30, 0.0), 45.0)
    sma_trend_1m_pct = ((sma200 / sma200_1m_ago) - 1) * 100 if sma200_1m_ago > 0 else 0.0
    sma_trend_5m_pct = ((sma200 / sma200_5m_ago) - 1) * 100 if sma200_5m_ago > 0 else 0.0
    score = (
        82
        + distance_to_high_score * 0.7
        + distance_from_low_score * 0.18
        + max(sma_trend_1m_pct, 0.0) * 5
        + max(sma_trend_5m_pct, 0.0) * 4
        + max(snapshot.stock_return_20d, 0.0) * 0.1
        + (snapshot.rs_rating if snapshot.rs_eligible else 0) * 0.08
    )
    reasons = [
        f"Price above 50/150/200 SMA ({sma50:.2f} / {sma150:.2f} / {sma200:.2f})",
        f"200 SMA rising over 1M and 5M ({sma200_5m_ago:.2f} -> {sma200_1m_ago:.2f} -> {sma200:.2f})",
        f"{snapshot.pct_from_52w_low:.2f}% above 52W low and {snapshot.pct_from_52w_high:.2f}% below 52W high",
    ]
    return round(score, 2), reasons


def _e_and_c_expansion(snapshot: StockSnapshot, request: EandCScanRequest) -> tuple[float, list[str]] | None:
    if snapshot.change_pct < request.expansion_min_change_pct:
        return None
    if snapshot.volume <= request.expansion_min_day_volume:
        return None
    if snapshot.relative_volume < request.expansion_min_relative_volume:
        return None
    rvol = round(snapshot.relative_volume, 2)
    reasons = [
        f"Day change +{snapshot.change_pct:.2f}%",
        f"RVOL {rvol:.2f}x",
        f"Volume {snapshot.volume:,}",
    ]
    score = 60.0 + (snapshot.change_pct * 3.0) + (min(rvol, 10.0) * 4.0)
    return round(score, 2), reasons


def run_e_and_c_scan(
    snapshots: list[StockSnapshot],
    request: EandCScanRequest | None = None,
) -> tuple[list[ScanMatch], list[ScanMatch]]:
    active_request = request or EandCScanRequest()
    expansion: list[ScanMatch] = []

    for snapshot in snapshots:
        expansion_result = _e_and_c_expansion(snapshot, active_request)
        if expansion_result is not None:
            score, reasons = expansion_result
            expansion.append(build_scan_match("expansion", snapshot, score, reasons))

    expansion.sort(key=lambda item: item.score, reverse=True)
    return [], expansion


def _ema_expansion(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    return _ema_expansion_with_thresholds(snapshot, min_change_pct=6.5, min_relative_volume=3.0)


def _ema_expansion_with_thresholds(
    snapshot: StockSnapshot,
    *,
    min_change_pct: float,
    min_relative_volume: float,
) -> tuple[float, list[str]] | None:
    avg_vol_20 = snapshot.avg_volume_20d or 0
    volume = snapshot.volume or 0

    # 1. 20-day average volume floor (>= 25,000)
    # 2. Daily volume floor (> 50,000)
    # 3. Price floor (> 30)
    if avg_vol_20 < 25000 or volume <= 50000 or snapshot.last_price <= 30:
        return None

    rvol_20 = volume / avg_vol_20

    if snapshot.change_pct >= min_change_pct and rvol_20 > min_relative_volume:
        score = 75 + rvol_20 * 2 + snapshot.change_pct
        return round(score, 2), [
            "Expansion setup",
            f"Daily Change: {snapshot.change_pct}%",
            f"20-Day RVOL: {rvol_20:.2f}x",
            f"Price: {snapshot.last_price:.2f}",
        ]
    return None


def run_expansion_scan(
    snapshots: list[StockSnapshot],
    *,
    min_change_pct: float = 6.5,
    min_relative_volume: float = 3.0,
) -> list[ScanMatch]:
    """Re-run the expansion scan with user-tunable thresholds.

    The static `ema-expansion` scan in SCANS uses the IBD-default 6.5%/3.0x
    gates. The route also accepts query overrides so the panel can let users
    relax or tighten those gates without a Custom Scanner round-trip.
    """
    matches: list[ScanMatch] = []
    for snapshot in snapshots:
        result = _ema_expansion_with_thresholds(
            snapshot,
            min_change_pct=min_change_pct,
            min_relative_volume=min_relative_volume,
        )
        if result is None:
            continue
        score, reasons = result
        matches.append(build_scan_match("ema-expansion", snapshot, score, reasons))
    matches.sort(key=lambda item: item.score, reverse=True)
    return matches


def _pct_change_between(current: float | None, previous: float | None) -> float | None:
    if current in (None, 0) or previous in (None, 0):
        return None
    return ((float(current) / float(previous)) - 1) * 100


def _snapshot_float(snapshot: StockSnapshot, *field_names: str) -> float | None:
    for field_name in field_names:
        value = getattr(snapshot, field_name, None)
        if value is None:
            continue
        return float(value)
    return None


def _recent_close_anchor_offset(snapshot: StockSnapshot) -> int:
    closes = [float(value) for value in getattr(snapshot, "recent_closes", []) if value is not None]
    if len(closes) < 2:
        return 0

    latest_recorded_change = _pct_change_between(closes[-1], closes[-2])
    if latest_recorded_change is not None and abs(latest_recorded_change - snapshot.change_pct) <= 0.35:
        return 1

    if snapshot.previous_close not in (None, 0):
        previous_close_gap = abs(_pct_change_between(closes[-1], snapshot.previous_close) or 0.0)
        if previous_close_gap <= 0.25:
            return 0

    current_price_gap = abs(_pct_change_between(closes[-1], snapshot.last_price) or 0.0)
    return 1 if current_price_gap <= 0.25 else 0


def _close_n_days_ago(snapshot: StockSnapshot, days: int) -> float | None:
    closes = [float(value) for value in getattr(snapshot, "recent_closes", []) if value is not None]
    if days <= 0 or not closes:
        return None

    index = days + _recent_close_anchor_offset(snapshot)
    if len(closes) < index:
        return None
    return closes[-index]


def _daily_change_n_days_ago(snapshot: StockSnapshot, days: int) -> float | None:
    close_n_days_ago = _close_n_days_ago(snapshot, days)
    close_prev_day = _close_n_days_ago(snapshot, days + 1)
    return _pct_change_between(close_n_days_ago, close_prev_day)


def _return_from_baseline(current_price: float, baseline_close: float | None) -> float | None:
    return _pct_change_between(current_price, baseline_close)


def _return_since_n_days_ago(snapshot: StockSnapshot, days: int) -> float | None:
    current_price = snapshot.last_price
    if current_price <= 0:
        return None

    direct_close = _close_n_days_ago(snapshot, days)
    if direct_close is not None:
        return _return_from_baseline(current_price, direct_close)

    baseline_map: dict[int, float | None] = {
        5: _snapshot_float(snapshot, "baseline_close_5d"),
        10: _snapshot_float(snapshot, "baseline_close_10d", "baseline_close_5d"),
        20: _snapshot_float(snapshot, "baseline_close_20d"),
        30: _snapshot_float(snapshot, "baseline_close_30d", "baseline_close_20d"),
        63: _snapshot_float(snapshot, "baseline_close_63d", "baseline_close_60d"),
        90: _snapshot_float(snapshot, "baseline_close_90d", "baseline_close_63d", "baseline_close_60d"),
    }
    derived = _return_from_baseline(current_price, baseline_map.get(days))
    if derived is not None:
        return derived

    if days == 5:
        return snapshot.stock_return_5d
    if days == 30:
        return snapshot.stock_return_20d
    if days == 90:
        return snapshot.stock_return_60d
    return None


def _contraction(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
    ema50 = snapshot.ema50
    sma50 = snapshot.sma50
    avg_volume_50d = snapshot.avg_volume_50d
    if ema50 is None or sma50 is None or avg_volume_50d is None or sma50 <= 0:
        return None

    if abs(snapshot.change_pct) > 2.5:
        return None

    change_1_day_ago = _daily_change_n_days_ago(snapshot, 1)
    change_2_days_ago = _daily_change_n_days_ago(snapshot, 2)
    has_three_day_context = change_1_day_ago is not None and change_2_days_ago is not None
    if has_three_day_context:
        if abs(change_1_day_ago) > 2.5 or abs(change_2_days_ago) > 3.5:
            has_three_day_context = False
    elif snapshot.history_source != "bhavcopy_patch" and not any(
        _snapshot_float(snapshot, field_name) not in (None, 0)
        for field_name in ("baseline_close_5d", "baseline_close_20d", "baseline_close_60d", "baseline_close_63d")
    ):
        return None

    if snapshot.last_price <= ema50 or snapshot.last_price <= 30:
        return None
    if avg_volume_50d < 50_000 or snapshot.volume <= 25_000:
        return None
    if snapshot.last_price > (1.25 * sma50):
        return None

    trigger_returns = {
        "5D +10%": _return_since_n_days_ago(snapshot, 5),
        "10D +20%": _return_since_n_days_ago(snapshot, 10),
        "30D +20%": _return_since_n_days_ago(snapshot, 30),
        "90D +30%": _return_since_n_days_ago(snapshot, 90),
    }
    matched_triggers = [
        (label, value)
        for label, value in trigger_returns.items()
        if value is not None
        and (
            (label == "5D +10%" and value >= 10.0)
            or (label in {"10D +20%", "30D +20%"} and value >= 20.0)
            or (label == "90D +30%" and value >= 30.0)
        )
    ]
    if not matched_triggers:
        return None

    best_trigger_label, best_trigger_return = max(matched_triggers, key=lambda item: item[1])
    tightness_score = max(0.0, 2.5 - abs(snapshot.change_pct))
    if has_three_day_context:
        assert change_1_day_ago is not None and change_2_days_ago is not None
        tightness_score += max(0.0, 2.5 - abs(change_1_day_ago)) + max(0.0, 3.5 - abs(change_2_days_ago))
    else:
        tightness_score += 3.0
    score = 72 + (tightness_score * 1.8) + min(best_trigger_return, 40.0) * 0.35 + min(avg_volume_50d / 50_000, 4.0) * 1.5
    contraction_label = (
        f"3-day contraction: {snapshot.change_pct:+.2f}%, {change_1_day_ago:+.2f}%, {change_2_days_ago:+.2f}%"
        if has_three_day_context
        else f"EOD contraction: {snapshot.change_pct:+.2f}% with cached run-up context"
    )
    reasons = [
        contraction_label,
        f"Above 50D EMA ({ema50:.2f}) and within 25% of 50D SMA ({sma50:.2f})",
        f"{best_trigger_label} trigger hit at +{best_trigger_return:.2f}% with volume {snapshot.volume:,}",
    ]
    return round(score, 2), reasons


# Positive Earnings: stocks that confirmed a strong reaction to their
# latest quarterly result. Conditions (all required):
#   * result announced within the last 60 days,
#   * close in the top 25% of the candle on the earnings day OR the day
#     after,
#   * gap up >= 1% on the session after earnings,
#   * earnings-day volume >= 2x the prior 50-day average volume,
#   * +10% or better return measured 5 sessions from the earnings day.
POSITIVE_EARNINGS_LOOKBACK_DAYS = 60
POSITIVE_EARNINGS_MIN_CLOSE_IN_RANGE = 0.75
POSITIVE_EARNINGS_MIN_NEXT_DAY_GAP_PCT = 1.0
POSITIVE_EARNINGS_MIN_DAY_RVOL = 2.0
POSITIVE_EARNINGS_MIN_RETURN_5D_PCT = 10.0


def make_positive_earnings_evaluator(
    *,
    lookback_days: int = POSITIVE_EARNINGS_LOOKBACK_DAYS,
    min_close_in_range_pct: float = POSITIVE_EARNINGS_MIN_CLOSE_IN_RANGE,
    min_next_day_gap_pct: float = POSITIVE_EARNINGS_MIN_NEXT_DAY_GAP_PCT,
    min_day_rvol: float = POSITIVE_EARNINGS_MIN_DAY_RVOL,
    min_return_5d_pct: float = POSITIVE_EARNINGS_MIN_RETURN_5D_PCT,
) -> ScannerFn:
    """Factory for a parameterized Positive Earnings evaluator.

    Defaults match the IBD-style spec the scanner shipped with; the
    frontend lets users relax or tighten each gate via the panel.
    """
    def _evaluator(snapshot: StockSnapshot) -> tuple[float, list[str]] | None:
        earnings_date = snapshot.latest_earnings_date
        if earnings_date is None:
            return None
        today = date.today()
        age_days = (today - earnings_date).days
        if age_days < 0 or age_days > lookback_days:
            return None

        close_in_range = snapshot.earnings_close_in_range_pct
        next_day_gap = snapshot.earnings_next_day_gap_pct
        day_rvol = snapshot.earnings_day_rvol_50d
        return_5d = snapshot.earnings_return_5d_pct
        if close_in_range is None or next_day_gap is None or day_rvol is None or return_5d is None:
            return None

        if close_in_range < min_close_in_range_pct:
            return None
        if next_day_gap < min_next_day_gap_pct:
            return None
        if day_rvol < min_day_rvol:
            return None
        if return_5d < min_return_5d_pct:
            return None

        score = round(
            min(return_5d, 80.0) * 0.6
            + min(day_rvol, 10.0) * 4.0
            + min(next_day_gap, 15.0) * 0.8
            + (close_in_range * 10.0),
            2,
        )
        reasons = [
            f"Earnings on {earnings_date.isoformat()} ({age_days}d ago)",
            f"Close at {round(close_in_range * 100)}% of candle range (>={round(min_close_in_range_pct * 100)}%)",
            f"Next-day gap +{next_day_gap:.2f}% (>={min_next_day_gap_pct:.1f}%)",
            f"Earnings-day volume {day_rvol:.2f}x of 50D avg (>={min_day_rvol:.1f}x)",
            f"+{return_5d:.2f}% over 5 sessions post-earnings (>={min_return_5d_pct:.1f}%)",
        ]
        return score, reasons
    return _evaluator


_positive_earnings = make_positive_earnings_evaluator()


SCANS: list[ScanDefinition] = [
    ScanDefinition("day-high", "Day High", "Core", "Stocks trading at session highs.", _day_high),
    ScanDefinition("day-low", "Day Low", "Core", "Stocks trading at session lows.", _day_low),
    ScanDefinition("ipo", "IPO", "Core", "Stocks listed within the last 1 year.", _recent_ipo),
    ScanDefinition("near-day-high", "Near Day High", "Core", "Stocks hovering right under day highs.", _near_day_high),
    ScanDefinition("near-day-low", "Near Day Low", "Core", "Stocks hovering near day lows.", _near_day_low),
    ScanDefinition("prev-day-high-break", "Previous Day High Break", "Core", "Names clearing the prior day's high.", _prev_day_high_break),
    ScanDefinition("prev-day-low-break", "Previous Day Low Break", "Core", "Names breaking below the prior day's low.", _prev_day_low_break),
    ScanDefinition("week-high", "Week High", "Core", "Stocks at weekly highs.", _week_high),
    ScanDefinition("week-low", "Week Low", "Core", "Stocks at weekly lows.", _week_low),
    ScanDefinition("month-high", "Month High", "Core", "Stocks at monthly highs.", _month_high),
    ScanDefinition("month-low", "Month Low", "Core", "Stocks at monthly lows.", _month_low),
    ScanDefinition("six-month-high", "6-Month High", "Core", "Stocks testing their 6-month highs.", _six_month_high),
    ScanDefinition("six-month-low", "6-Month Low", "Core", "Stocks testing their 6-month lows.", _six_month_low),
    ScanDefinition("high-52w", "52-Week High", "Core", "Fresh yearly highs.", _high_52w),
    ScanDefinition("low-52w", "52-Week Low", "Core", "Fresh yearly lows.", _low_52w),
    ScanDefinition("near-52w-high", "Near 52W High", "Core", "Close to yearly highs.", _near_52w_high),
    ScanDefinition("near-52w-low", "Near 52W Low", "Core", "Close to yearly lows.", _near_52w_low),
    ScanDefinition("all-time-high", "All-Time High", "Core", "All-time high candidates.", _all_time_high),
    ScanDefinition("all-time-low", "All-Time Low", "Core", "All-time low candidates.", _all_time_low),
    ScanDefinition("near-ath", "Near ATH", "Core", "Names within striking distance of ATH.", _near_ath),
    ScanDefinition("near-atl", "Near ATL", "Core", "Names within striking distance of ATL.", _near_atl),
    ScanDefinition("breakout-ath", "ATH Breakouts", "Setups", "Fresh all-time-high breakouts with strength.", _breakout_ath),
    ScanDefinition("breakout-52w", "52W Breakouts", "Setups", "Names clearing prior yearly highs.", _breakout_52w),
    ScanDefinition("breakout-range", "Range Breakouts", "Setups", "20-day range expansions with participation.", _range_breakout),
    ScanDefinition("volume-price", "Volume + Price Move", "Setups", "Relative-volume spikes with directional expansion.", _volume_price),
    ScanDefinition("strong-nifty", "Strong vs Benchmark", "Setups", "Stocks beating the benchmark over 20D.", _strong_vs_nifty),
    ScanDefinition("strong-sector", "Strong vs Sector", "Setups", "Stocks leading their own sector basket.", _strong_vs_sector),
    ScanDefinition("clean-pullback", "Clean Pullbacks", "Setups", "Tight pullbacks inside healthy uptrends.", _clean_pullback),
    ScanDefinition("darvas-box", "Darvas Box", "Setups", "Box breakouts with renewed momentum.", _darvas_box),
    ScanDefinition("pivot-breakout", "Pivot Breakouts", "Setups", "Swing-high pivot resolutions with confirmation.", _pivot_breakout),
    ScanDefinition(
        "contraction",
        "Contraction",
        "Setups",
        "Tight 3-day contractions above the 50D EMA with liquidity floors and prior run-up confirmation.",
        _contraction,
    ),
    ScanDefinition("relative-strength", "Relative Strengths", "Setups", "Composite RS leaders across 20D and 60D.", _relative_strength),
    ScanDefinition("minervini-1m", "Minervini 1 Month", "Setups", "Trend template names with price above key SMAs, rising 200 SMA, and strong 52-week positioning.", _minervini_1m),
    ScanDefinition("minervini-5m", "Minervini 5 Months", "Setups", "Trend template names with price above key SMAs, a rising 200 SMA over 1 and 5 months, and strong 52-week positioning.", _minervini_5m),
    ScanDefinition("ema-expansion", "Expansion", "Setups", "Price gain >= 6.5%, 20-day RVOL > 3.0, and liquidity floors (AvgVol20 > 25k, Vol > 50k, Price > 30).", _ema_expansion),
    ScanDefinition(
        "positive-earnings",
        "Positive Earnings",
        "Setups",
        "Result declared in last 60 days with a strong post-earnings reaction: top-quartile close, +1% gap up, 2x volume, and +10% over 5 sessions.",
        _positive_earnings,
    ),
]

SCAN_BY_ID = {scan.id: scan for scan in SCANS}


def build_scan_match(
    scan_id: str,
    snapshot: StockSnapshot,
    score: float,
    reasons: list[str],
    *,
    pattern: str | None = None,
    volume_push_date: str | None = None,
) -> ScanMatch:
    display_sector = scanner_sector_label(snapshot.sector, snapshot.sub_sector)
    return ScanMatch(
        scan_id=scan_id,
        symbol=snapshot.symbol,
        name=snapshot.name,
        exchange=snapshot.exchange,
        listing_date=snapshot.listing_date,
        sector=display_sector,
        sub_sector=snapshot.sub_sector,
        market_cap_crore=snapshot.market_cap_crore,
        last_price=snapshot.last_price,
        change_pct=snapshot.change_pct,
        relative_volume=snapshot.relative_volume,
        avg_rupee_volume_30d_crore=snapshot.avg_rupee_volume_30d_crore,
        score=score,
        pattern=pattern,
        volume_push_date=volume_push_date,
        rs_rating=snapshot.rs_rating if snapshot.rs_eligible else None,
        rs_rating_1d_ago=snapshot.rs_rating_1d_ago if snapshot.rs_eligible else None,
        rs_rating_1w_ago=snapshot.rs_rating_1w_ago if snapshot.rs_eligible else None,
        rs_rating_1m_ago=snapshot.rs_rating_1m_ago if snapshot.rs_eligible else None,
        nifty_outperformance=snapshot.nifty_outperformance,
        sector_outperformance=snapshot.sector_outperformance,
        three_month_rs=snapshot.three_month_rs,
        stock_return_20d=snapshot.stock_return_20d,
        stock_return_60d=snapshot.stock_return_60d,
        stock_return_12m=snapshot.stock_return_12m,
        gap_pct=snapshot.gap_pct,
        reasons=reasons,
    )


def _default_sort(matches: list[ScanMatch]) -> list[ScanMatch]:
    return sorted(
        matches,
        key=lambda item: (
            item.score,
            item.last_price,
            item.relative_volume,
            item.stock_return_20d or 0,
            item.change_pct,
        ),
        reverse=True,
    )


def _passes_min_liquidity(snapshot: StockSnapshot, min_liquidity_crore: float | None) -> bool:
    if min_liquidity_crore is None:
        return True
    return snapshot.avg_rupee_volume_30d_crore >= min_liquidity_crore


def run_scan(scan: ScanDefinition, snapshots: list[StockSnapshot]) -> list[ScanMatch]:
    matches: list[ScanMatch] = []
    for snapshot in snapshots:
        outcome = scan.evaluator(snapshot)
        if not outcome:
            continue
        score, reasons = outcome
        matches.append(build_scan_match(scan.id, snapshot, score, reasons, pattern=scan.name))

    return _default_sort(matches)


def _return_for_period(snapshot: StockSnapshot, period: str) -> float:
    period_map = {
        "1D": snapshot.change_pct,
        "1W": snapshot.stock_return_5d,
        "1M": snapshot.stock_return_20d,
        "3M": snapshot.stock_return_60d,
        "6M": snapshot.stock_return_126d,
        "1Y": snapshot.stock_return_12m,
    }
    return float(period_map.get(period, snapshot.stock_return_12m))


def _near_high_distance(snapshot: StockSnapshot, period: str) -> float:
    if period == "1M":
        return abs(min(_gap_from_level(snapshot.last_price, snapshot.month_high_level), 0))
    if period == "3M":
        return abs(min(_gap_from_level(snapshot.last_price, snapshot.high_3m_level), 0))
    if period == "6M":
        return abs(min(_gap_from_level(snapshot.last_price, snapshot.high_6m_level), 0))
    if period == "52W":
        return snapshot.pct_from_52w_high
    return snapshot.pct_from_ath


def _passes_custom_filters(snapshot: StockSnapshot, request: CustomScanRequest) -> bool:
    if request.min_price is not None and snapshot.last_price < request.min_price:
        return False
    if request.max_price is not None and snapshot.last_price > request.max_price:
        return False
    if request.listing_date_from is not None:
        if snapshot.listing_date is None or snapshot.listing_date < request.listing_date_from:
            return False
    if request.listing_date_to is not None:
        if snapshot.listing_date is None or snapshot.listing_date > request.listing_date_to:
            return False
    if request.min_change_pct is not None and snapshot.change_pct < request.min_change_pct:
        return False
    if request.max_change_pct is not None and snapshot.change_pct > request.max_change_pct:
        return False
    if request.min_relative_volume is not None and snapshot.relative_volume < request.min_relative_volume:
        return False
    if request.min_nifty_outperformance is not None and snapshot.nifty_outperformance < request.min_nifty_outperformance:
        return False
    if request.min_sector_outperformance is not None and snapshot.sector_outperformance < request.min_sector_outperformance:
        return False
    if request.min_rs_rating is not None:
        if not snapshot.rs_eligible or snapshot.rs_rating < request.min_rs_rating:
            return False
    if request.max_rs_rating is not None:
        if not snapshot.rs_eligible or snapshot.rs_rating > request.max_rs_rating:
            return False
    if request.min_stock_return_20d is not None and snapshot.stock_return_20d < request.min_stock_return_20d:
        return False
    if request.min_stock_return_60d is not None and snapshot.stock_return_60d < request.min_stock_return_60d:
        return False
    if request.min_market_cap_crore is not None and snapshot.market_cap_crore < request.min_market_cap_crore:
        return False
    if request.max_market_cap_crore is not None and snapshot.market_cap_crore > request.max_market_cap_crore:
        return False
    if request.min_trend_strength is not None and snapshot.trend_strength < request.min_trend_strength:
        return False
    if request.max_pullback_depth_pct is not None and snapshot.pullback_depth_pct > request.max_pullback_depth_pct:
        return False
    if request.min_avg_rupee_volume_30d_crore is not None and snapshot.avg_rupee_volume_30d_crore < request.min_avg_rupee_volume_30d_crore:
        return False
    if (
        request.min_avg_rupee_turnover_20d_crore is not None
        and snapshot.avg_rupee_turnover_20d_crore < request.min_avg_rupee_turnover_20d_crore
    ):
        return False
    if request.min_pct_from_52w_low is not None and snapshot.pct_from_52w_low < request.min_pct_from_52w_low:
        return False
    if request.max_pct_from_52w_low is not None and snapshot.pct_from_52w_low > request.max_pct_from_52w_low:
        return False
    if request.min_pct_from_52w_high is not None and snapshot.pct_from_52w_high < request.min_pct_from_52w_high:
        return False
    if request.max_pct_from_52w_high is not None and snapshot.pct_from_52w_high > request.max_pct_from_52w_high:
        return False
    if request.min_pct_from_ath is not None and snapshot.pct_from_ath < request.min_pct_from_ath:
        return False
    if request.max_pct_from_ath is not None and snapshot.pct_from_ath > request.max_pct_from_ath:
        return False
    if request.min_gap_pct is not None and snapshot.gap_pct < request.min_gap_pct:
        return False
    if request.max_gap_pct is not None and snapshot.gap_pct > request.max_gap_pct:
        return False
    if request.min_day_range_pct is not None and snapshot.day_range_pct < request.min_day_range_pct:
        return False
    if request.max_day_range_pct is not None and snapshot.day_range_pct > request.max_day_range_pct:
        return False
    if request.min_three_month_rs is not None and snapshot.three_month_rs < request.min_three_month_rs:
        return False
    if request.near_high_period is not None:
        max_distance = request.near_high_max_distance_pct if request.near_high_max_distance_pct is not None else 3.0
        if _near_high_distance(snapshot, request.near_high_period) > max_distance:
            return False
    if request.price_vs_ma_mode != "any":
        ma_value = snapshot.ma_value(request.price_vs_ma_key)
        if ma_value is None:
            return False
        if request.price_vs_ma_mode == "above" and snapshot.last_price < ma_value:
            return False
        if request.price_vs_ma_mode == "below" and snapshot.last_price > ma_value:
            return False
    if request.require_bullish_ma_order and not snapshot.ema_stack_bullish:
        return False
    if request.require_bearish_ma_order and not snapshot.ema_stack_bearish:
        return False
    if request.min_price_to_ma_ratio is not None or request.max_price_to_ma_ratio is not None:
        ma_value = snapshot.ma_value(request.price_to_ma_key)
        if ma_value is None or ma_value <= 0:
            return False
        ratio = snapshot.last_price / ma_value
        if request.min_price_to_ma_ratio is not None and ratio < request.min_price_to_ma_ratio:
            return False
        if request.max_price_to_ma_ratio is not None and ratio > request.max_price_to_ma_ratio:
            return False
    if request.min_return_pct is not None or request.max_return_pct is not None:
        period_return = _return_for_period(snapshot, request.return_period)
        if request.min_return_pct is not None and period_return < request.min_return_pct:
            return False
        if request.max_return_pct is not None and period_return > request.max_return_pct:
            return False
    if request.above_ema20 and (snapshot.ema20 is None or snapshot.last_price < snapshot.ema20):
        return False
    if request.above_ema50 and (snapshot.ema50 is None or snapshot.last_price < snapshot.ema50):
        return False
    if request.above_ema200 and (snapshot.ema200 is None or snapshot.last_price < snapshot.ema200):
        return False

    # Fundamental cached fields (stored as decimals eg 0.3 for 30%)
    min_eps_growth_yoy = _request_filter_value(request, "min_eps_growth_yoy")
    if min_eps_growth_yoy is not None:
        val = getattr(snapshot, "eps_growth_yoy", None)
        if val is None or (val * 100) < min_eps_growth_yoy:
            return False
    min_revenue_growth_yoy = _request_filter_value(request, "min_revenue_growth_yoy")
    if min_revenue_growth_yoy is not None:
        val = getattr(snapshot, "revenue_growth_yoy", None)
        if val is None or (val * 100) < min_revenue_growth_yoy:
            return False
    min_operating_margin = _request_filter_value(request, "min_operating_margin")
    if min_operating_margin is not None:
        val = getattr(snapshot, "operating_margin", None)
        if val is None or (val * 100) < min_operating_margin:
            return False
    min_profit_margin = _request_filter_value(request, "min_profit_margin")
    if min_profit_margin is not None:
        val = getattr(snapshot, "profit_margin", None)
        if val is None or (val * 100) < min_profit_margin:
            return False
    min_roe = _request_filter_value(request, "min_roe")
    if min_roe is not None:
        val = getattr(snapshot, "roe", None)
        if val is None or (val * 100) < min_roe:
            return False
    max_peg_ratio = _request_filter_value(request, "max_peg_ratio")
    if max_peg_ratio is not None:
        val = getattr(snapshot, "peg_ratio", None)
        if val is None or val > max_peg_ratio:
            return False
    min_pe_ratio = _request_filter_value(request, "min_pe_ratio")
    if min_pe_ratio is not None:
        val = getattr(snapshot, "pe_ratio", None)
        if val is None or val < min_pe_ratio:
            return False
    max_pe_ratio = _request_filter_value(request, "max_pe_ratio")
    if max_pe_ratio is not None:
        val = getattr(snapshot, "pe_ratio", None)
        if val is None or val > max_pe_ratio:
            return False

    # Guru & Setup filters
    if _request_filter_value(request, "minervini_trend_template", False):
        if not _minervini_1m(snapshot): 
            return False
            
    if _request_filter_value(request, "kullamagi_setup", False):
        if snapshot.ema20 is None or snapshot.last_price < snapshot.ema20: return False
        if snapshot.stock_return_60d < 20: return False
        if snapshot.trend_strength < 4: return False
            
    if _request_filter_value(request, "shakeout_21ema", False):
        if snapshot.ema20 is None: return False
        if snapshot.last_price < snapshot.ema20: return False
        try:
            # Look back 5 days instead of 3 to catch recovering shakeouts
            recent_low = min(snapshot.recent_lows[-5:])
            if recent_low > snapshot.ema20: return False
        except Exception:
            return False

    if _request_filter_value(request, "shakeout_50ema", False):
        if snapshot.ema50 is None: return False
        if snapshot.last_price < snapshot.ema50: return False
        try:
            recent_low = min(snapshot.recent_lows[-5:])
            if recent_low > snapshot.ema50: return False
        except Exception:
            return False

    max_consolidation_range_pct = _request_filter_value(request, "max_consolidation_range_pct")
    if max_consolidation_range_pct is not None:
        try:
            # Check 15-day and 25-day windows. 
            # If the stock is extremely tight (consolidation) in either window, we keep it.
            for window in [15, 25]:
                h = snapshot.recent_highs[-window:]
                l = snapshot.recent_lows[-window:]
                if len(h) >= 5 and len(l) >= 5:
                    highest = max(h)
                    lowest = min(l)
                    range_pct = ((highest - lowest) / highest) * 100 if highest > 0 else 0
                    if range_pct <= max_consolidation_range_pct:
                        return True # Passed at least one window
            return False # Failed both windows
        except Exception:
            return False

    return True


def _custom_score(snapshot: StockSnapshot, request: CustomScanRequest) -> tuple[float, list[str], str]:
    if request.pattern and request.pattern != "any":
        scan = SCAN_BY_ID.get(request.pattern)
        if scan is None:
            # Unknown / stale pattern (e.g. from older client state) —
            # fall through to the generic "any" scoring below instead of
            # erroring. This keeps the Custom Scanner usable when a
            # scanner is removed from the dropdown.
            pass
        else:
            outcome = scan.evaluator(snapshot)
            if not outcome:
                raise ValueError("pattern did not match")
            score, reasons = outcome
            return score, reasons, scan.name

    rs_component = snapshot.rs_rating if snapshot.rs_eligible else 0
    score = (
        50
        + max(snapshot.relative_volume - 1, 0) * 8
        + max(snapshot.nifty_outperformance, 0) * 1.5
        + (rs_component * 0.3)
        + max(snapshot.stock_return_20d, 0) * 0.4
        + max(snapshot.stock_return_60d, 0) * 0.2
        + snapshot.trend_strength * 12
    )
    reasons: list[str] = ["Custom filter match"]
    if request.min_relative_volume is not None:
        reasons.append(f"RVOL {snapshot.relative_volume:.2f}x")
    elif snapshot.listing_date and (request.listing_date_from is not None or request.listing_date_to is not None):
        reasons.append(f"Listed {snapshot.listing_date.isoformat()}")
    if request.min_rs_rating is not None:
        reasons.append(f"RS Rating {snapshot.rs_rating}")
    elif request.max_rs_rating is not None:
        reasons.append(f"RS Rating {snapshot.rs_rating}")
    if request.min_nifty_outperformance is not None:
        reasons.append(f"RS vs Benchmark {snapshot.nifty_outperformance:.2f}%")
    elif request.min_three_month_rs is not None:
        reasons.append(f"3M RS {snapshot.three_month_rs:.2f}%")
    elif request.min_sector_outperformance is not None:
        reasons.append(f"RS vs Sector {snapshot.sector_outperformance:.2f}%")
    elif request.min_avg_rupee_volume_30d_crore is not None:
        reasons.append(f"30D rupee vol {snapshot.avg_rupee_volume_30d_crore:.2f} Cr")
    elif request.near_high_period is not None:
        reasons.append(f"{request.near_high_period} high distance {_near_high_distance(snapshot, request.near_high_period):.2f}%")
    elif request.min_stock_return_20d is not None:
        reasons.append(f"20D return {snapshot.stock_return_20d:.2f}%")
    else:
        reasons.append(f"Trend strength {snapshot.trend_strength:.2f}")
    return round(score, 2), reasons[:3], "Custom"


def _custom_sort_value(item: ScanMatch, sort_by: str) -> float:
    if sort_by == "price":
        return item.last_price
    if sort_by == "change_pct":
        return item.change_pct
    if sort_by == "listing_date":
        return item.listing_date.toordinal() if item.listing_date else 0
    if sort_by == "relative_volume":
        return item.relative_volume
    if sort_by == "relative_strength":
        return item.nifty_outperformance or 0
    if sort_by == "rs_rating":
        return item.rs_rating or 0
    if sort_by == "three_month_rs":
        return item.three_month_rs or 0
    if sort_by == "stock_return_20d":
        return item.stock_return_20d or 0
    if sort_by == "stock_return_60d":
        return item.stock_return_60d or 0
    if sort_by == "stock_return_12m":
        return item.stock_return_12m or 0
    if sort_by == "market_cap":
        return item.market_cap_crore
    if sort_by == "avg_rupee_volume":
        return item.avg_rupee_volume_30d_crore or 0
    return item.score


def run_custom_scan(request: CustomScanRequest, snapshots: list[StockSnapshot]) -> list[ScanMatch]:
    matches: list[ScanMatch] = []
    for snapshot in snapshots:
        if not _passes_custom_filters(snapshot, request):
            continue
        try:
            score, reasons, pattern = _custom_score(snapshot, request)
        except ValueError:
            continue
        matches.append(build_scan_match("custom-scan", snapshot, score, reasons, pattern=pattern))

    reverse = request.sort_order == "desc"
    matches = sorted(
        matches,
        key=lambda item: (
            _custom_sort_value(item, request.sort_by),
            item.score,
            item.last_price,
            item.relative_volume,
        ),
        reverse=reverse,
    )
    return matches[: request.limit]


def run_returns_scan(request: ReturnsScanRequest, snapshots: list[StockSnapshot]) -> list[ScanMatch]:
    matches: list[ScanMatch] = []
    
    # Map timeframe to the correct attribute dynamically
    return_attr_map: dict[str, str] = {
        "1D": "change_pct",
        "1W": "stock_return_5d",
        "1M": "stock_return_20d",
        "3M": "stock_return_60d",
    }
    attr_name = return_attr_map[request.timeframe]
    
    for snapshot in snapshots:
        if not _passes_min_liquidity(snapshot, request.min_liquidity_crore):
            continue
        val = getattr(snapshot, attr_name, 0.0)
        
        # Apply return bounds
        if request.min_return_pct is not None and val < request.min_return_pct:
            continue
        if request.max_return_pct is not None and val > request.max_return_pct:
            continue
            
        # Apply MA checks
        if request.above_21_ema and (snapshot.ema20 is None or snapshot.last_price < snapshot.ema20):
            continue
        if request.above_50_ema and (snapshot.ema50 is None or snapshot.last_price < snapshot.ema50):
            continue
        if request.above_200_sma and (snapshot.sma200 is None or snapshot.last_price < snapshot.sma200):
            continue
        
        # Check for consolidation after first leg up
        if request.enable_first_leg_up:
            first_leg_return = snapshot.stock_return_40d
            if first_leg_return < request.min_first_leg_up_pct:
                continue
        
        # Check consolidation range and drawdown
        if request.enable_consolidation_filter:
            highs = snapshot.recent_highs[-20:]
            lows = snapshot.recent_lows[-20:]
            if len(highs) < 2 or len(lows) < 2:
                continue
            
            # Find the peak (highest point in recent history)
            peak = max(highs[-10:]) if len(highs) >= 10 else max(highs)
            
            # Calculate current drawdown from peak
            if peak > 0:
                drawdown_pct = ((peak - snapshot.last_price) / peak) * 100
            else:
                drawdown_pct = 0.0
            
            if drawdown_pct > request.max_drawdown_after_leg_up:
                continue
            
            # Check for consolidation: look for a period with tight range
            best_days = 0
            best_range_pct = 0.0
            for days in range(min(request.min_consolidation_days, len(highs)), min(15, len(highs)) + 1):
                window_highs = highs[-days:]
                window_lows = lows[-days:]
                window_high = max(window_highs)
                window_low = min(window_lows)
                if window_high <= 0:
                    continue
                range_pct = ((window_high - window_low) / window_high) * 100
                if range_pct <= request.max_consolidation_range_pct:
                    best_days = days
                    best_range_pct = round(range_pct, 2)
                    break
            
            if best_days < request.min_consolidation_days:
                continue
        
        # Check volume contraction against 50-day MA
        if request.enable_volume_contraction:
            if snapshot.avg_volume_30d <= 0:
                continue
            # Use recent volumes and compare against 50d average (approximated by avg_volume_30d)
            recent_volumes = snapshot.recent_volumes[-5:] if snapshot.recent_volumes else []
            if not recent_volumes:
                continue
            avg_recent_volume = sum(recent_volumes) / len(recent_volumes)
            volume_ratio = avg_recent_volume / snapshot.avg_volume_30d if snapshot.avg_volume_30d > 0 else 1.0
            if volume_ratio > request.max_volume_vs_50d_avg:
                continue
        
        # Check single day price move filter
        if request.enable_price_move_filter:
            day_move = abs(snapshot.change_pct)
            if day_move < request.min_price_move_pct or day_move > request.max_price_move_pct:
                continue
            
        score = 50 + val  # baseline score based on return
        reasons = [f"{request.timeframe} Return: {val:.2f}%"]
        
        if request.above_21_ema or request.above_50_ema or request.above_200_sma:
            reasons.append("Passed MA checks")
        if request.enable_first_leg_up:
            reasons.append(f"First leg up {snapshot.stock_return_40d:.1f}%")
        if request.enable_consolidation_filter:
            reasons.append("Consolidation detected")
        if request.enable_volume_contraction:
            reasons.append("Volume contracted")
            
        matches.append(build_scan_match("returns", snapshot, round(score, 2), reasons[:3], pattern=f"{request.timeframe} Returns"))

    # Sort primarily by the queried return percentage descending
    matches = sorted(
        matches,
        key=lambda item: (
            getattr(item, attr_name, 0.0),
            item.score,
            item.last_price,
            item.relative_volume,
        ),
        reverse=True,
    )
    return matches[: request.limit]


def run_consolidating_scan(request: ConsolidatingScanRequest, snapshots: list[StockSnapshot]) -> list[ScanMatch]:
    matches: list[ScanMatch] = []

    for snapshot in snapshots:
        combined = _combine_consolidating_matches(_evaluate_consolidating_matches(snapshot, request))
        if not combined:
            continue

        score, reasons, pattern = combined
        matches.append(build_scan_match("consolidating", snapshot, score, reasons[:3], pattern=pattern))

    return _default_sort(matches)[: request.limit]


def scan_catalog_with_counts(snapshots: list[StockSnapshot]) -> tuple[list[ScanDescriptor], dict[str, list[ScanMatch]]]:
    descriptors: list[ScanDescriptor] = [
        ScanDescriptor(
            id="custom-scan",
            name="Custom Scanner",
            category="Custom",
            description="Build a scan with your own price, RS, volume, and trend filters.",
            hit_count=0,
        )
    ]
    all_results: dict[str, list[ScanMatch]] = {"custom-scan": []}

    for scan in SCANS:
        results = run_scan(scan, snapshots)
        all_results[scan.id] = results
        descriptors.append(
            ScanDescriptor(
                id=scan.id,
                name=scan.name,
                category=scan.category,
                description=scan.description,
                hit_count=len(results),
            )
        )

    return descriptors, all_results


# ---------------------------------------------------------------------------
# Momentum Burst scanner
# ---------------------------------------------------------------------------
# Catches two states using ONLY moving averages, price action, volume and RS:
#   Type A "Burst"        — a fresh explosive leg (up >= X% in a 3-10 day window).
#   Type B "10/21 EMA Setup" — a stock that already exploded and is now resting
#                          (tight, contracting consolidation surfing a rising EMA).
# No RSI / MACD / ADX / oscillators are used anywhere.

_MB_BURST_TAG = "Burst"
_MB_10EMA_TAG = "10 EMA Setup"
_MB_21EMA_TAG = "21 EMA Setup"
# Sort priority: 10 EMA setups first, then 21 EMA setups, then fresh bursts.
_MB_TAG_RANK = {_MB_10EMA_TAG: 0, _MB_21EMA_TAG: 1, _MB_BURST_TAG: 2}


def _ema_series(values: list[float], span: int) -> list[float]:
    """Standard EMA over ``values`` (seeded with the first value), same length."""
    if not values:
        return []
    k = 2.0 / (span + 1.0)
    out: list[float] = [float(values[0])]
    for v in values[1:]:
        out.append(float(v) * k + out[-1] * (1.0 - k))
    return out


def _mb_closes(snapshot: StockSnapshot) -> list[float]:
    """Long daily close series (~240 bars) from the snapshot's chart grid points."""
    points = getattr(snapshot, "chart_grid_points", None) or []
    closes: list[float] = []
    for p in points:
        value = getattr(p, "value", None)
        if value is None and isinstance(p, dict):
            value = p.get("value")
        if value is None:
            continue
        try:
            fv = float(value)
        except (TypeError, ValueError):
            continue
        if fv > 0:
            closes.append(fv)
    return closes


def _mb_volume_at(vols: list[int], voffset: int, close_index: int) -> float | None:
    """Map a close-series index to the trailing ``recent_volumes`` window.

    ``voffset`` is ``len(closes) - len(vols)``; volumes only cover the last ~20
    sessions, so older indices return ``None`` (unknown) rather than crashing.
    """
    idx = close_index - voffset
    if 0 <= idx < len(vols):
        try:
            return float(vols[idx])
        except (TypeError, ValueError):
            return None
    return None


def _mb_best_window_gain(
    closes: list[float],
    *,
    win_min: int,
    win_max: int,
    end_lo: int,
    end_hi: int,
) -> tuple[float, int, int, int]:
    """Max close-to-close gain over any window length in [win_min, win_max]
    whose END index lies in [end_lo, end_hi]. Returns (gain_pct, days, s, e);
    gain_pct is -inf when no valid window exists.
    """
    n = len(closes)
    best = (float("-inf"), 0, -1, -1)
    end_hi = min(end_hi, n - 1)
    end_lo = max(end_lo, 0)
    for e in range(end_lo, end_hi + 1):
        for w in range(win_min, win_max + 1):
            s = e - w
            if s < 0:
                continue
            base = closes[s]
            if base <= 0:
                continue
            gain = (closes[e] / base - 1.0) * 100.0
            if gain > best[0]:
                best = (gain, w, s, e)
    return best


def _mb_detect_burst(
    closes: list[float],
    vols: list[int],
    ema10: list[float],
    ema21: list[float],
    avg_vol_50d: float | None,
    request: MomentumBurstScanRequest,
) -> tuple[float, int] | None:
    """Type A — fresh explosive leg. Returns (burst_pct, burst_days) or None."""
    n = len(closes)
    need = request.burst_window_max + request.burst_recency_sessions + 1
    if n < need:
        return None
    if not avg_vol_50d or avg_vol_50d <= 0:
        return None  # volume confirmation is required; skip if we can't measure it

    gain, days, s, e = _mb_best_window_gain(
        closes,
        win_min=request.burst_window_min,
        win_max=request.burst_window_max,
        end_lo=n - request.burst_recency_sessions,
        end_hi=n - 1,
    )
    if gain < request.burst_min_gain_pct or s < 0:
        return None

    # The move must be above a rising fast EMA stack right now.
    if not (ema10 and ema21):
        return None
    last_close = closes[-1]
    if not (last_close > ema10[-1] and ema10[-1] > ema21[-1]):
        return None

    # At least one session in the move carried volume >= ratio x 50-day average.
    voffset = n - len(vols)
    threshold = request.burst_min_volume_ratio * avg_vol_50d
    has_volume = False
    for i in range(s + 1, e + 1):
        vol = _mb_volume_at(vols, voffset, i)
        if vol is not None and vol >= threshold:
            has_volume = True
            break
    if not has_volume:
        return None

    return round(gain, 2), days


def _mb_is_contracting(highs: list[float], lows: list[float]) -> bool:
    """True when recent daily ranges are narrower than the first part of the rest."""
    k = len(highs)
    if k < 4 or len(lows) < k:
        return False
    ranges = [h - l for h, l in zip(highs, lows) if h is not None and l is not None]
    if len(ranges) < 4:
        return False
    half = len(ranges) // 2
    first = ranges[:half]
    recent = ranges[half:]
    if not first or not recent:
        return False
    return (sum(recent) / len(recent)) < (sum(first) / len(first))


def _mb_detect_setup(
    snapshot: StockSnapshot,
    closes: list[float],
    ema10: list[float],
    ema21: list[float],
    request: MomentumBurstScanRequest,
) -> MomentumBurstPlan | None:
    """Type B — consolidation near the 10/21 EMA after a prior burst leg.

    Returns a populated MomentumBurstPlan (without tag/rs filled by the caller's
    rs_rating) or None. Tag is set here ("10 EMA Setup" / "21 EMA Setup").
    """
    n = len(closes)
    if n < max(request.setup_move_lookback_sessions, request.consolidation_max_days) + 1:
        return None

    highs = [float(v) for v in (snapshot.recent_highs or []) if v is not None]
    lows = [float(v) for v in (snapshot.recent_lows or []) if v is not None]
    vols = [int(v) for v in (snapshot.recent_volumes or []) if v is not None]
    if len(highs) < request.consolidation_min_days or len(lows) < request.consolidation_min_days:
        return None
    if snapshot.sma50 is None:
        return None

    last_close = closes[-1]
    cons_max = min(request.consolidation_max_days, len(highs), len(lows), len(ema10), len(ema21))

    # --- 1+2. Find the consolidation AND classify the EMA surf together. ---
    # We want the LARGEST recent window that is (a) tight, (b) contracting, and
    # (c) surfing the 10 or 21 EMA. Checking the surf inside the loop matters:
    # right after a steep leg the fast EMA needs a few sessions to catch up, so
    # the surfing window is often shorter than the merely-tight window. The 10
    # EMA tier is preferred over the 21 EMA tier when both qualify (per spec).
    dist = request.ema_surf_distance_pct / 100.0

    def _surfs(cons_closes: list[float], cons_emas: list[float]) -> bool:
        # "Closes hug the EMA during the consolidation": the stock must be
        # surfing RIGHT NOW (latest close within band) and predominantly within
        # band across the rest. Real consolidations have the odd noisy day, so we
        # tolerate one outlier rather than demanding every single close — an
        # all-or-nothing rule effectively never fires on live data.
        if not cons_emas or cons_emas[-1] <= 0:
            return False
        if abs(cons_closes[-1] - cons_emas[-1]) > dist * cons_emas[-1]:
            return False
        violations = sum(1 for c, e in zip(cons_closes, cons_emas) if e > 0 and abs(c - e) > dist * e)
        return violations <= 1

    def _classify(k: int) -> tuple[str, float, float] | None:
        cons_closes = closes[-k:]
        cons_ema10 = ema10[-k:]
        cons_ema21 = ema21[-k:]
        no_close_below_21 = all(c >= e for c, e in zip(cons_closes, cons_ema21))
        if request.include_ema_setups and _surfs(cons_closes, cons_ema10) and no_close_below_21:
            return _MB_10EMA_TAG, ema10[-1], request.max_giveback_10ema_pct / 100.0
        no_close_below_50 = all(c >= snapshot.sma50 for c in cons_closes)
        if request.include_ema_setups and _surfs(cons_closes, cons_ema21) and no_close_below_50:
            return _MB_21EMA_TAG, ema21[-1], request.max_giveback_21ema_pct / 100.0
        return None

    best_k = 0
    best_range_pct = 0.0
    tag = ""
    surfed_ema = 0.0
    max_giveback = 0.0
    for k in range(cons_max, request.consolidation_min_days - 1, -1):
        wh = highs[-k:]
        wl = lows[-k:]
        pivot_high = max(wh)
        pivot_low = min(wl)
        if pivot_high <= 0:
            continue
        range_pct = (pivot_high - pivot_low) / pivot_high * 100.0
        if range_pct > request.consolidation_max_range_pct or not _mb_is_contracting(wh, wl):
            continue
        classification = _classify(k)
        if classification is None:
            continue
        tag, surfed_ema, max_giveback = classification
        best_k = k
        best_range_pct = round(range_pct, 2)
        break
    if best_k < request.consolidation_min_days:
        return None

    cons_high = max(highs[-best_k:])
    cons_low = min(lows[-best_k:])

    # --- 3. Burst leg before the rest: >= setup_min_move_pct move in a
    #         [move_window_min, move_window_max] window inside the lookback. ---
    leg_end_hi = n - best_k  # leg ends at/just before the consolidation begins
    gain, leg_days, ls, le = _mb_best_window_gain(
        closes,
        win_min=request.setup_move_window_min,
        win_max=request.setup_move_window_max,
        end_lo=n - request.setup_move_lookback_sessions,
        end_hi=leg_end_hi,
    )
    if gain < request.setup_min_move_pct or ls < 0:
        return None

    # --- 4. Shallow pullback (giveback of the burst move). ---
    move_high = max(highs[-best_k:] + [closes[le]]) if highs else closes[le]
    move_high = max(move_high, max(closes[ls : le + 1]))
    move_low = min(closes[ls : le + 1])
    move_size = move_high - move_low
    if move_size <= 0:
        return None
    giveback_ratio = max(0.0, (move_high - last_close) / move_size)
    if giveback_ratio >= max_giveback:
        return None

    # --- 5. Volume dry-up: consolidation avg volume < ratio x burst-leg volume. ---
    voffset = n - len(vols)
    leg_vols = [v for v in (_mb_volume_at(vols, voffset, i) for i in range(ls, le + 1)) if v is not None]
    cons_vols = [v for v in (_mb_volume_at(vols, voffset, i) for i in range(n - best_k, n)) if v is not None]
    dryup_ratio: float | None = None
    if cons_vols:
        cons_avg = sum(cons_vols) / len(cons_vols)
        if leg_vols:
            burst_avg = sum(leg_vols) / len(leg_vols)
        elif snapshot.avg_volume_50d:
            burst_avg = float(snapshot.avg_volume_50d)
        else:
            burst_avg = 0.0
        if burst_avg > 0:
            dryup_ratio = round(cons_avg / burst_avg, 3)
            if dryup_ratio >= request.volume_dryup_ratio:
                return None

    # --- 6. Trade plan: breakout entry, tighter stop, 2R/3R targets. ---
    entry = round(cons_high, 2)
    raw_stop = max(cons_low, surfed_ema)  # tighter (higher) of the two, => smaller risk
    if raw_stop >= entry:
        raw_stop = cons_low
    stop = round(raw_stop, 2)
    risk = entry - stop
    risk_pct = round(risk / entry * 100.0, 2) if entry > 0 and risk > 0 else None
    target_2r = round(entry + 2 * risk, 2) if risk > 0 else None
    target_3r = round(entry + 3 * risk, 2) if risk > 0 else None

    return MomentumBurstPlan(
        tag=tag,
        rs_rating=0,  # filled by the caller
        burst_pct=round(gain, 2),
        burst_days=leg_days,
        consolidation_days=best_k,
        consolidation_range_pct=best_range_pct,
        dist_from_10ema_pct=round((last_close - ema10[-1]) / ema10[-1] * 100.0, 2) if ema10[-1] > 0 else None,
        dist_from_21ema_pct=round((last_close - ema21[-1]) / ema21[-1] * 100.0, 2) if ema21[-1] > 0 else None,
        volume_dryup_ratio=dryup_ratio,
        giveback_pct=round(giveback_ratio * 100.0, 2),
        entry=entry,
        stop=stop,
        risk_pct=risk_pct,
        target_2r=target_2r,
        target_3r=target_3r,
    )


def _mb_passes_universe(snapshot: StockSnapshot, request: MomentumBurstScanRequest) -> bool:
    """Universe + liquidity + trend gate, applied before RS percentile ranking."""
    if snapshot.last_price < request.min_price:
        return False
    if snapshot.avg_rupee_turnover_20d_crore < request.min_turnover_crore:
        return False
    if not _passes_min_liquidity(snapshot, request.min_liquidity_crore):
        return False
    # ASM/GSM surveillance exclusion: no surveillance flag exists on the snapshot,
    # so this filter is skipped gracefully (request.exclude_surveillance is a no-op
    # until a surveillance source is wired in).
    # Trend: close > 50 SMA; and 50 SMA > 200 SMA when >= ~250 bars are available.
    if snapshot.sma50 is None or snapshot.last_price <= snapshot.sma50:
        return False
    if snapshot.sma200 is not None and snapshot.sma50 <= snapshot.sma200:
        return False
    return True


def _mb_blended_outperformance(snapshot: StockSnapshot) -> float:
    """RS input: blend 1M/3M/6M outperformance vs Nifty, weighted to recency."""
    op_1m = snapshot.stock_return_20d - snapshot.benchmark_return_20d
    op_3m = snapshot.stock_return_60d - snapshot.benchmark_return_60d
    op_6m = snapshot.stock_return_126d - snapshot.benchmark_return_126d
    return 0.5 * op_1m + 0.3 * op_3m + 0.2 * op_6m


def momentum_burst_rs_ratings(scores: list[float]) -> list[int]:
    """Percentile-rank blended scores into RS ratings 1-99 (ties share a rating).

    Exposed for unit testing the RS math independently of the snapshot plumbing.
    """
    n = len(scores)
    if n == 0:
        return []
    if n == 1:
        return [99]
    sorted_scores = sorted(scores)
    # bisect_left gives the count of values strictly less than each score, so
    # ties share the same rating. Map that count onto the 1-99 RS scale.
    return [
        max(1, min(99, round(1 + 98 * (bisect.bisect_left(sorted_scores, s) / (n - 1)))))
        for s in scores
    ]


def run_bread_butter_scan(snapshots: list["StockSnapshot"]) -> list[ScanMatch]:
    """Bread & Butter — the user's personal playbook, two setups only.

    Hard context filter for BOTH legs: close above the 50 SMA AND 200 SMA.

    A) PULLBACK: a 15%+ leg inside 3-5 sessions (within the last ~20), then an
       ORDERLY return to the 10 or 21 EMA — surfing it for at least 2 sessions
       (closes within ~4% of the EMA, holding above the 21 EMA). Entry at the
       EMA; stop below it, risk capped at 6%.
    B) BREAKOUT: an upmove, then a 12+ session orderly base (<=12% range, low
       chop) within 6% below the pivot. Stop at base low, capped at 6%.
    """
    matches: list[ScanMatch] = []
    for snapshot in snapshots:
        if snapshot.rs_rating < 65 or snapshot.last_price < 50:
            continue
        if snapshot.avg_rupee_volume_30d_crore is not None and snapshot.avg_rupee_volume_30d_crore < 5:
            continue
        # Stage-2 context — non-negotiable: close ABOVE both the 50 and 200
        # SMA, MAs stacked (50 over 200) and the 200 not declining. Stocks
        # missing either MA are excluded rather than given the benefit.
        sma50 = snapshot.sma50
        sma200 = snapshot.sma200
        if (
            not sma50
            or not sma200
            or snapshot.last_price <= sma50
            or snapshot.last_price <= sma200
            or sma50 <= sma200
            or (snapshot.sma200_1m_ago and sma200 < snapshot.sma200_1m_ago * 0.995)
        ):
            continue

        closes = snapshot.recent_closes or []
        highs = snapshot.recent_highs or []
        lows = snapshot.recent_lows or []

        # --- A) Pullback to the 10/21 EMA after a 15%+ burst ---
        ema10 = snapshot.ema10
        ema21 = snapshot.ema20
        volumes = snapshot.recent_volumes or []
        if len(closes) >= 10 and ema21:
            best_burst = 0.0
            burst_days = 0
            burst_has_volume = False
            # The strong upmove must be RECENT: its peak within the last 15
            # sessions (windows starting earlier than that are ignored).
            earliest_start = max(0, len(closes) - 15)
            for window in (3, 4, 5, 6, 7):
                for i in range(earliest_start, len(closes) - window):
                    base = closes[i]
                    if base <= 0:
                        continue
                    gain = (max(closes[i + 1 : i + 1 + window]) / base - 1) * 100
                    if gain > best_burst:
                        best_burst, burst_days = gain, window
                        # Strong volume footprint: at least one burst day at
                        # 1.5x the 20-day average volume.
                        if snapshot.avg_volume_20d > 0 and len(volumes) == len(closes):
                            burst_has_volume = max(volumes[i + 1 : i + 1 + window], default=0) >= snapshot.avg_volume_20d * 1.5
                        else:
                            burst_has_volume = snapshot.relative_volume >= 1.2
            if best_burst >= 15.0 and burst_has_volume:
                surf_ema = None
                tolerance = 0.04
                for label, ema in (("10 EMA", ema10), ("21 EMA", ema21)):
                    if not ema:
                        continue
                    recent = closes[-2:]
                    surf_days = sum(1 for c in recent if abs(c - ema) / ema <= tolerance)
                    # "Stayed there 1 or 2 days": latest close must be at the EMA,
                    # one prior close there strengthens it but isn't required.
                    # Above the 21 EMA, or surfing at most 2% below it.
                    if surf_days >= 1 and abs(closes[-1] - ema) / ema <= tolerance and closes[-1] >= ema21 * 0.98:
                        surf_ema = (label, ema)
                        break
                if surf_ema is not None:
                    label, ema = surf_ema
                    # Orderly: the pullback hasn't broken structure (still above 21 EMA zone).
                    stop = min(ema * 0.97, min(lows[-3:]) if lows[-3:] else ema * 0.97)
                    risk_pct = (1 - stop / snapshot.last_price) * 100
                    if risk_pct <= 6.5:
                        score = 80 + snapshot.rs_rating * 0.2 + best_burst * 0.2
                        matches.append(
                            build_scan_match(
                                "bread-butter",
                                snapshot,
                                round(score, 2),
                                [
                                    f"Pullback surfing {label}",
                                    f"+{best_burst:.0f}% burst in {burst_days} days, now {abs(snapshot.last_price - ema) / ema * 100:.1f}% from {label}",
                                    f"Entry ~{ema:.2f}, stop {stop:.2f} (risk {max(risk_pct, 0):.1f}%, cap 6%)",
                                ],
                            ).model_copy(update={"pattern": "Pullback"})
                        )
                        continue

        # --- B) Orderly pre-breakout base ---
        if len(closes) < 12 or len(highs) < 12 or len(lows) < 12:
            continue
        window_high = max(highs[-12:])
        window_low = min(lows[-12:])
        if window_low <= 0:
            continue
        range_pct = (window_high / window_low - 1) * 100
        if range_pct > 12.0:
            continue
        day_ranges = [(h / l - 1) * 100 for h, l in zip(highs[-12:], lows[-12:]) if l > 0]
        if day_ranges and sum(day_ranges) / len(day_ranges) > 4.2:
            continue
        pivot = snapshot.pivot_high or window_high
        if pivot <= 0:
            continue
        below_pivot_pct = (pivot / snapshot.last_price - 1) * 100
        if not (0.0 <= below_pivot_pct <= 6.0):
            continue
        if snapshot.stock_return_60d < 12 and snapshot.stock_return_126d < 20:
            continue
        stop = window_low
        risk_pct = (1 - stop / snapshot.last_price) * 100
        if risk_pct > 6.0:
            stop = snapshot.last_price * 0.95
            risk_pct = 5.0
        score = 70 + snapshot.rs_rating * 0.2 - range_pct
        matches.append(
            build_scan_match(
                "bread-butter",
                snapshot,
                round(score, 2),
                [
                    "Breakout setup",
                    f"{range_pct:.1f}% base, {below_pivot_pct:.1f}% below pivot {pivot:.2f}",
                    f"Stop {stop:.2f} (risk {risk_pct:.1f}%, cap 6%)",
                ],
            ).model_copy(update={"pattern": "Breakout"})
        )

    matches.sort(key=lambda item: (item.pattern != "Pullback", -item.score))
    return matches


def run_momentum_burst_scan(
    request: MomentumBurstScanRequest, snapshots: list[StockSnapshot]
) -> list[ScanMatch]:
    """Run the Momentum Burst scanner over the snapshot universe."""
    # --- Pass 1: universe / liquidity / trend gate + RS inputs. ---
    pool: list[StockSnapshot] = []
    blended: list[float] = []
    for snapshot in snapshots:
        try:
            if not _mb_passes_universe(snapshot, request):
                continue
        except Exception:
            continue
        pool.append(snapshot)
        blended.append(_mb_blended_outperformance(snapshot))

    if not pool:
        return []

    # --- Pass 2: percentile RS rating across the scanned universe. ---
    ratings = momentum_burst_rs_ratings(blended)
    rs_by_symbol = {pool[i].symbol: ratings[i] for i in range(len(pool))}

    # --- Pass 3: classify each RS-qualifying candidate. ---
    matches: list[ScanMatch] = []
    for snapshot in pool:
        rs = rs_by_symbol.get(snapshot.symbol, 0)
        if rs < request.min_rs_rating:
            continue
        try:
            closes = _mb_closes(snapshot)
            if len(closes) < 30:
                continue  # insufficient history — skip gracefully
            ema10 = _ema_series(closes, 10)
            ema21 = _ema_series(closes, 21)
            vols = [int(v) for v in (snapshot.recent_volumes or []) if v is not None]

            plan: MomentumBurstPlan | None = None
            # Type B (the buyable rest) is the primary list; check it first.
            if request.include_ema_setups:
                plan = _mb_detect_setup(snapshot, closes, ema10, ema21, request)
            # Otherwise look for a fresh explosive leg (Type A).
            if plan is None and request.include_fresh_bursts:
                burst = _mb_detect_burst(closes, vols, ema10, ema21, snapshot.avg_volume_50d, request)
                if burst is not None:
                    burst_pct, burst_days = burst
                    plan = MomentumBurstPlan(
                        tag=_MB_BURST_TAG,
                        rs_rating=rs,
                        burst_pct=burst_pct,
                        burst_days=burst_days,
                        dist_from_10ema_pct=round((closes[-1] - ema10[-1]) / ema10[-1] * 100.0, 2)
                        if ema10 and ema10[-1] > 0
                        else None,
                        dist_from_21ema_pct=round((closes[-1] - ema21[-1]) / ema21[-1] * 100.0, 2)
                        if ema21 and ema21[-1] > 0
                        else None,
                    )
            if plan is None:
                continue
            plan.rs_rating = rs
        except Exception:
            # Never let one malformed snapshot crash the whole scan.
            continue

        score = float(rs) - _MB_TAG_RANK.get(plan.tag, 9) * 0.001  # keep RS as the visible driver
        reasons = _mb_reasons(plan)
        match = build_scan_match("momentum-burst", snapshot, round(score, 3), reasons, pattern=plan.tag)
        match.rs_rating = rs  # surface the scanner's own percentile RS even if rs_eligible is False
        match.momentum_burst = plan
        matches.append(match)

    # --- Sort: 10 EMA setups, then 21 EMA setups, then Bursts; RS desc within each. ---
    matches.sort(
        key=lambda m: (
            _MB_TAG_RANK.get(m.momentum_burst.tag if m.momentum_burst else "", 9),
            -(m.momentum_burst.rs_rating if m.momentum_burst else 0),
            -(m.momentum_burst.burst_pct if m.momentum_burst else 0.0),
        )
    )
    return matches[: request.limit]


def _mb_reasons(plan: MomentumBurstPlan) -> list[str]:
    reasons: list[str] = [f"RS {plan.rs_rating}"]
    if plan.tag == _MB_BURST_TAG:
        reasons.append(f"Fresh burst +{plan.burst_pct:.1f}% over {plan.burst_days}d")
        if plan.dist_from_10ema_pct is not None:
            reasons.append(f"{plan.dist_from_10ema_pct:+.1f}% vs 10 EMA")
    else:
        reasons.append(
            f"{plan.tag}: +{plan.burst_pct:.1f}% leg, {plan.consolidation_days}d rest "
            f"({plan.consolidation_range_pct:.1f}% range)"
        )
        if plan.volume_dryup_ratio is not None:
            reasons.append(f"Vol dry-up {plan.volume_dryup_ratio:.2f}x")
    return reasons[:3]
