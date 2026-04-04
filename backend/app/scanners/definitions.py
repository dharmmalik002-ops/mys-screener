from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from app.models.market import (
    ConsolidatingScanRequest,
    CustomScanRequest,
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
    ScanDefinition("consolidating", "Consolidating", "Setups", "Union of run-up consolidations and stocks coiling just below 3-year highs.", _consolidating),
    ScanDefinition("relative-strength", "Relative Strengths", "Setups", "Composite RS leaders across 20D and 60D.", _relative_strength),
    ScanDefinition("minervini-1m", "Minervini 1 Month", "Setups", "Trend template names with price above key SMAs, rising 200 SMA, and strong 52-week positioning.", _minervini_1m),
    ScanDefinition("minervini-5m", "Minervini 5 Months", "Setups", "Trend template names with price above key SMAs, a rising 200 SMA over 1 and 5 months, and strong 52-week positioning.", _minervini_5m),
]

SCAN_BY_ID = {scan.id: scan for scan in SCANS}


def build_scan_match(
    scan_id: str,
    snapshot: StockSnapshot,
    score: float,
    reasons: list[str],
    *,
    pattern: str | None = None,
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
    if request.min_eps_growth_yoy is not None:
        val = snapshot.eps_growth_yoy
        if val is None or (val * 100) < request.min_eps_growth_yoy:
            return False
    if request.min_revenue_growth_yoy is not None:
        val = snapshot.revenue_growth_yoy
        if val is None or (val * 100) < request.min_revenue_growth_yoy:
            return False
    if request.min_operating_margin is not None:
        val = snapshot.operating_margin
        if val is None or (val * 100) < request.min_operating_margin:
            return False
    if request.min_profit_margin is not None:
        val = snapshot.profit_margin
        if val is None or (val * 100) < request.min_profit_margin:
            return False
    if request.min_roe is not None:
        val = snapshot.roe
        if val is None or (val * 100) < request.min_roe:
            return False
    if request.max_peg_ratio is not None:
        val = snapshot.peg_ratio
        if val is None or val > request.max_peg_ratio:
            return False
    if request.min_pe_ratio is not None:
        val = snapshot.pe_ratio
        if val is None or val < request.min_pe_ratio:
            return False
    if request.max_pe_ratio is not None:
        val = snapshot.pe_ratio
        if val is None or val > request.max_pe_ratio:
            return False

    # Guru & Setup filters
    if request.minervini_trend_template:
        if not _minervini_1m(snapshot): 
            return False
            
    if request.kullamagi_setup:
        if snapshot.ema20 is None or snapshot.last_price < snapshot.ema20: return False
        if snapshot.stock_return_60d < 20: return False
        if snapshot.trend_strength < 4: return False
            
    if request.shakeout_21ema:
        if snapshot.ema20 is None: return False
        if snapshot.last_price < snapshot.ema20: return False
        try:
            # Look back 5 days instead of 3 to catch recovering shakeouts
            recent_low = min(snapshot.recent_lows[-5:])
            if recent_low > snapshot.ema20: return False
        except Exception:
            return False

    if request.shakeout_50ema:
        if snapshot.ema50 is None: return False
        if snapshot.last_price < snapshot.ema50: return False
        try:
            recent_low = min(snapshot.recent_lows[-5:])
            if recent_low > snapshot.ema50: return False
        except Exception:
            return False

    if request.max_consolidation_range_pct is not None:
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
                    if range_pct <= request.max_consolidation_range_pct:
                        return True # Passed at least one window
            return False # Failed both windows
        except Exception:
            return False

    return True


def _custom_score(snapshot: StockSnapshot, request: CustomScanRequest) -> tuple[float, list[str], str]:
    if request.pattern != "any":
        scan = SCAN_BY_ID[request.pattern]
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
