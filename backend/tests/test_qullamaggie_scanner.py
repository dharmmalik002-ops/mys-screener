"""Unit tests for the Qullamaggie continuation scanner.

Run: `cd backend && pytest tests/test_qullamaggie_scanner.py`
"""

from __future__ import annotations

import unittest

from app.models.market import StockSnapshot
from app.scanners.definitions import _qullamaggie


def _flag_closes() -> list[float]:
    """40 flat bars @100, a 60-bar ramp to 180 (+80% over ~3 months), then a
    12-session flag drifting 180 -> 168 and settling at 174 (6.7% deep)."""
    closes: list[float] = [100.0] * 40
    for i in range(60):
        closes.append(round(100 + 80 * (i + 1) / 60, 2))
    for i in range(8):  # drift down to 168
        closes.append(round(180 - 12 * (i + 1) / 8, 2))
    closes.extend([169.0, 171.0, 172.5, 174.0])
    return closes


def _make_snapshot(**overrides) -> StockSnapshot:
    closes = overrides.pop("closes", _flag_closes())
    last_price = overrides.pop("last_price", closes[-1])
    grid = [{"time": i, "value": float(c)} for i, c in enumerate(closes)]
    payload = {
        "symbol": "QMTEST",
        "name": "Qullamaggie Test Ltd",
        "exchange": "NSE",
        "sector": "Industrials",
        "sub_sector": "Capital Goods",
        "market_cap_crore": 4000.0,
        "last_price": last_price,
        "change_pct": 0.9,
        "volume": 700_000,
        "avg_volume_20d": 1_000_000,
        "avg_volume_30d": 1_000_000,
        "avg_volume_50d": 1_000_000,
        "day_high": last_price * 1.01,
        "day_low": last_price * 0.99,
        "ath": 185.0,
        "high_52w": 180.0,
        "low_52w": 95.0,
        "range_high_20d": 180.0,
        "benchmark_return_20d": 2.0,
        "benchmark_return_60d": 4.0,
        "benchmark_return_126d": 8.0,
        "sector_return_20d": 2.0,
        "pivot_high": 180.0,
        "darvas_high": 180.0,
        "darvas_low": 168.0,
        "pullback_depth_pct": 3.4,
        "trend_strength": 0.9,
        "sma50": 160.0,
        "sma150": 130.0,
        "sma200": 118.0,
        "ema10": last_price * 0.99,
        "ema20": last_price * 0.97,
        "ema50": last_price * 0.93,
        "adr_pct_20": 4.6,
        "atr14": 7.4,
        "stock_return_20d": 5.0,
        "stock_return_60d": 62.0,
        "stock_return_126d": 74.0,
        "recent_closes": [float(c) for c in closes[-20:]],
        "recent_highs": [round(c * 1.008, 2) for c in closes[-20:]],
        "recent_lows": [round(c * 0.992, 2) for c in closes[-20:]],
        "recent_volumes": [1_200_000] * 15 + [600_000] * 5,
        "chart_grid_points": grid,
    }
    payload.update(overrides)
    return StockSnapshot.model_validate(payload)


class QullamaggieScannerTests(unittest.TestCase):
    def test_textbook_flag_matches(self) -> None:
        result = _qullamaggie(_make_snapshot())
        self.assertIsNotNone(result)
        score, reasons = result
        self.assertGreater(score, 80)
        self.assertIn("Prior move", reasons[0])
        self.assertIn("3M", reasons[0])
        self.assertTrue(any("Entry" in reason for reason in reasons))

    def test_no_prior_move_rejected(self) -> None:
        # Same structure, but the stock never made the move that qualifies it.
        self.assertIsNone(
            _qullamaggie(
                _make_snapshot(stock_return_20d=4.0, stock_return_60d=12.0, stock_return_126d=20.0)
            )
        )

    def test_low_adr_rejected(self) -> None:
        self.assertIsNone(_qullamaggie(_make_snapshot(adr_pct_20=2.1)))

    def test_below_short_mas_rejected(self) -> None:
        snapshot = _make_snapshot()
        self.assertIsNone(
            _qullamaggie(_make_snapshot(ema20=snapshot.last_price * 1.03))
        )

    def test_extended_past_pivot_rejected(self) -> None:
        # Base high 180, now trading 5% through it — the entry is gone.
        closes = _flag_closes()[:-1] + [189.0]
        self.assertIsNone(_qullamaggie(_make_snapshot(closes=closes)))

    def test_deep_base_rejected(self) -> None:
        # 180 -> 110 (39% deep) is a correction, not a flag.
        closes = _flag_closes()[:100]
        closes.extend([170.0, 155.0, 140.0, 125.0, 112.0, 110.0, 118.0, 124.0])
        self.assertIsNone(_qullamaggie(_make_snapshot(closes=closes, last_price=124.0)))

    def test_far_below_pivot_rejected(self) -> None:
        # Still inside a shallow-enough base, but 12% under the pivot — too
        # far from the trigger to be actionable today.
        closes = _flag_closes()[:-4] + [166.0, 160.0, 158.0, 158.4]
        self.assertIsNone(_qullamaggie(_make_snapshot(closes=closes, last_price=158.4)))

    def test_illiquid_rejected(self) -> None:
        self.assertIsNone(_qullamaggie(_make_snapshot(avg_volume_20d=20_000)))


if __name__ == "__main__":
    unittest.main()
