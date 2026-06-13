from app.models.market import StockSnapshot
from app.scanners.definitions import run_bread_butter_scan


def _bread_butter_snapshot(symbol: str, *, last_price: float = 121.0, sma50: float = 95.0, sma200: float = 80.0) -> StockSnapshot:
    closes = [80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 100, 108, 118, 120, 116, 114, 116, 118, 119, 120, 121]
    highs = [close * 1.015 for close in closes]
    lows = [close * 0.985 for close in closes]
    volumes = [500_000] * len(closes)
    volumes[18] = 1_300_000
    volumes[19] = 2_100_000
    volumes[20] = 1_800_000
    volumes[21:] = [750_000] * (len(closes) - 21)
    return StockSnapshot.model_construct(
        symbol=symbol,
        name=symbol,
        exchange="NSE",
        sector="Test",
        sub_sector="Test",
        market_cap_crore=1000,
        last_price=last_price,
        change_pct=1.0,
        volume=750_000,
        avg_volume_20d=600_000,
        avg_volume_30d=600_000,
        day_high=last_price * 1.01,
        day_low=last_price * 0.99,
        ath=130,
        high_52w=130,
        range_high_20d=130,
        sma50=sma50,
        sma200=sma200,
        sma200_1m_ago=78,
        ema10=118,
        ema20=116,
        benchmark_return_20d=0,
        sector_return_20d=0,
        stock_return_20d=25,
        stock_return_60d=30,
        rs_rating=82,
        rs_eligible=True,
        pivot_high=123,
        darvas_high=123,
        darvas_low=110,
        pullback_depth_pct=4,
        trend_strength=0.9,
        recent_closes=closes,
        recent_highs=highs,
        recent_lows=lows,
        recent_volumes=volumes,
        volume_history=volumes * 5,
    )


def test_bread_butter_accepts_stage_two_impulse_ema_rest() -> None:
    matches = run_bread_butter_scan([_bread_butter_snapshot("GOOD")])

    assert len(matches) == 1
    assert matches[0].symbol == "GOOD"
    assert matches[0].pattern == "10 EMA Pullback"
    assert "Stage 2" in matches[0].reasons[0]


def test_bread_butter_rejects_stocks_below_required_smas() -> None:
    matches = run_bread_butter_scan(
        [
            _bread_butter_snapshot("BELOW50", last_price=90, sma50=95, sma200=80),
            _bread_butter_snapshot("BADSTACK", last_price=121, sma50=95, sma200=125),
        ]
    )

    assert matches == []
