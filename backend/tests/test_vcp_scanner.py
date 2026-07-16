"""Unit tests for the VCP scanner, the 3 Tight Closes scan, and the P2 fixes
(kullamagi trend_strength scale, contraction dry-up gate, ADR custom filter).

Run: `cd backend && pytest tests/test_vcp_scanner.py`
"""

from __future__ import annotations

import unittest

from app.models.market import CustomScanRequest, StockSnapshot
from app.scanners.definitions import (
    _passes_custom_filters,
    _tight_closes,
    _vcp,
    _vcp_contractions,
)


def _textbook_vcp_closes() -> list[float]:
    """20 flat bars @100, ramp to 150 over 25 bars (+50% run-up), then a base:
    T1 150→120 (20%), recover 145; T2 145→130.5 (10%), recover 147;
    T3 147→141 (4.1%), settle ~146.5. Base ≈ 35 sessions, peak 150."""
    closes: list[float] = [100.0] * 20
    for i in range(25):  # run-up
        closes.append(round(100 + 50 * (i + 1) / 25, 2))
    # T1 down 150 -> 120 over 8 bars, back to 145 over 8
    for i in range(8):
        closes.append(round(150 - 30 * (i + 1) / 8, 2))
    for i in range(8):
        closes.append(round(120 + 25 * (i + 1) / 8, 2))
    # T2 down 145 -> 130.5 over 6, back to 147 over 6
    for i in range(6):
        closes.append(round(145 - 14.5 * (i + 1) / 6, 2))
    for i in range(6):
        closes.append(round(130.5 + 16.5 * (i + 1) / 6, 2))
    # T3 down 147 -> 141 over 4, settle at ~146.5
    for i in range(4):
        closes.append(round(147 - 6 * (i + 1) / 4, 2))
    closes.extend([144.0, 145.5, 146.5])
    return closes


def _make_snapshot(**overrides) -> StockSnapshot:
    closes = overrides.pop("closes", _textbook_vcp_closes())
    last_price = overrides.pop("last_price", closes[-1])
    grid = [{"time": i, "value": float(c)} for i, c in enumerate(closes)]
    quiet_vols = [1_000_000] * 15 + [500_000] * 5  # last week dried up vs 1M avg
    base_len = 36
    declining_hist = [1_200_000] * (len(closes) - base_len) + (
        [1_200_000] * (base_len // 2) + [600_000] * (base_len - base_len // 2)
    )
    payload = {
        "symbol": "VCPTEST",
        "name": "VCP Test Ltd",
        "exchange": "NSE",
        "sector": "Industrials",
        "sub_sector": "Capital Goods",
        "market_cap_crore": 5000.0,
        "last_price": last_price,
        "change_pct": 0.4,
        "volume": 480_000,
        "avg_volume_20d": 1_000_000,
        "avg_volume_30d": 1_000_000,
        "avg_volume_50d": 1_000_000,
        "day_high": last_price * 1.005,
        "day_low": last_price * 0.995,
        "ath": 155.0,
        "high_52w": 150.0,
        "low_52w": 90.0,
        "range_high_20d": 147.5,
        "benchmark_return_20d": 2.0,
        "benchmark_return_60d": 4.0,
        "benchmark_return_126d": 8.0,
        "sector_return_20d": 2.0,
        "pivot_high": 147.5,
        "darvas_high": 147.5,
        "darvas_low": 141.0,
        "pullback_depth_pct": 2.3,
        "trend_strength": 0.9,
        "sma50": 138.0,
        "sma150": 125.0,
        "sma200": 112.0,
        "sma200_1m_ago": 108.0,
        "ema10": last_price * 0.995,
        "ema20": last_price * 0.985,
        "ema50": last_price * 0.94,
        "adr_pct_20": 2.4,
        "atr14": 3.2,
        "stock_return_20d": 1.5,
        "stock_return_60d": 25.0,
        "stock_return_126d": 45.0,
        "relative_volume": 0.6,
        "recent_closes": [float(c) for c in closes[-20:]],
        "recent_highs": [round(c * 1.006, 2) for c in closes[-20:]],
        "recent_lows": [round(c * 0.994, 2) for c in closes[-20:]],
        "recent_volumes": quiet_vols,
        "volume_history": declining_hist,
        "chart_grid_points": grid,
    }
    payload.update(overrides)
    return StockSnapshot.model_validate(payload)


class VcpContractionSequenceTests(unittest.TestCase):
    def test_progressive_sequence_detected(self) -> None:
        closes = _textbook_vcp_closes()
        base = closes[44:]  # peak (150, index 44) onward
        depths = _vcp_contractions(base, threshold_pct=4.0)
        self.assertGreaterEqual(len(depths), 3)
        self.assertAlmostEqual(depths[0], 20.0, delta=1.0)
        self.assertAlmostEqual(depths[1], 10.0, delta=1.0)
        self.assertLess(depths[-1], 6.0)  # tight final leg found despite < threshold

    def test_flat_series_has_no_contractions(self) -> None:
        self.assertEqual(_vcp_contractions([100.0] * 40, threshold_pct=4.0), [])


class VcpScannerTests(unittest.TestCase):
    def test_textbook_vcp_matches(self) -> None:
        result = _vcp(_make_snapshot())
        self.assertIsNotNone(result)
        score, reasons = result
        self.assertGreater(score, 84)
        self.assertIn("contractions", reasons[0])
        # trade plan line present (entry/stop/risk)
        self.assertTrue(any("Entry" in reason for reason in reasons))

    def test_wide_and_loose_base_rejected(self) -> None:
        # Expanding pullbacks (10% then 18%) — the anti-VCP.
        closes: list[float] = [100.0] * 20
        for i in range(25):
            closes.append(round(100 + 50 * (i + 1) / 25, 2))
        for i in range(6):  # T1: only 10%
            closes.append(round(150 - 15 * (i + 1) / 6, 2))
        for i in range(6):
            closes.append(round(135 + 13 * (i + 1) / 6, 2))
        for i in range(8):  # T2: 18% — WIDER than T1
            closes.append(round(148 - 26.6 * (i + 1) / 8, 2))
        for i in range(8):
            closes.append(round(121.4 + 25 * (i + 1) / 8, 2))
        closes.extend([146.0, 146.5])
        self.assertIsNone(_vcp(_make_snapshot(closes=closes)))

    def test_extended_past_pivot_rejected(self) -> None:
        closes = _textbook_vcp_closes()
        closes[-1] = 158.0  # 5%+ above the 150 pivot — entry is gone
        snapshot = _make_snapshot(closes=closes, last_price=158.0, high_52w=158.0)
        self.assertIsNone(_vcp(snapshot))

    def test_no_volume_dryup_rejected(self) -> None:
        snapshot = _make_snapshot(recent_volumes=[1_000_000] * 20)  # 1.0x of 50D avg
        self.assertIsNone(_vcp(snapshot))

    def test_broken_trend_rejected(self) -> None:
        snapshot = _make_snapshot(sma50=160.0)  # price below 50 SMA
        self.assertIsNone(_vcp(snapshot))


class TightClosesTests(unittest.TestCase):
    def test_three_tight_closes_match(self) -> None:
        closes = [140.0] * 17 + [146.0, 146.4, 146.2]
        snapshot = _make_snapshot(recent_closes=closes)  # rvol = 0.48 (quiet)
        result = _tight_closes(snapshot)
        self.assertIsNotNone(result)
        _, reasons = result
        self.assertIn("3 closes within", reasons[0])

    def test_loose_closes_rejected(self) -> None:
        closes = [140.0] * 15 + [139.0, 143.0, 146.0, 141.0, 145.0]
        snapshot = _make_snapshot(recent_closes=closes)
        self.assertIsNone(_tight_closes(snapshot))

    def test_loud_volume_rejected(self) -> None:
        closes = [140.0] * 17 + [146.0, 146.4, 146.2]
        # relative_volume is computed as volume / avg_volume_20d -> 2.6x here
        snapshot = _make_snapshot(recent_closes=closes, volume=2_600_000)
        self.assertIsNone(_tight_closes(snapshot))


class CustomFilterFixTests(unittest.TestCase):
    def test_kullamagi_matches_strong_trend(self) -> None:
        request = CustomScanRequest(kullamagi_setup=True)
        snapshot = _make_snapshot(trend_strength=0.9, stock_return_60d=25.0)
        self.assertTrue(_passes_custom_filters(snapshot, request))

    def test_kullamagi_rejects_weak_trend(self) -> None:
        request = CustomScanRequest(kullamagi_setup=True)
        snapshot = _make_snapshot(trend_strength=0.35, stock_return_60d=25.0)
        self.assertFalse(_passes_custom_filters(snapshot, request))

    def test_max_adr_filter(self) -> None:
        request = CustomScanRequest(max_adr_pct_20=5.0)
        self.assertTrue(_passes_custom_filters(_make_snapshot(adr_pct_20=3.0), request))
        self.assertFalse(_passes_custom_filters(_make_snapshot(adr_pct_20=7.0), request))
        # unknown ADR excluded when the bound is set
        self.assertFalse(_passes_custom_filters(_make_snapshot(adr_pct_20=0.0), request))

    def test_min_adr_filter(self) -> None:
        request = CustomScanRequest(min_adr_pct_20=4.0)
        self.assertFalse(_passes_custom_filters(_make_snapshot(adr_pct_20=3.0), request))
        self.assertTrue(_passes_custom_filters(_make_snapshot(adr_pct_20=6.0), request))


if __name__ == "__main__":
    unittest.main()
