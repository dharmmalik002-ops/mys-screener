"""What a round trip actually costs on an Indian equity delivery trade.

This module exists because of how much it changes the answer. A strategy
targeting 5% gross with a 45% hit rate looks profitable until you subtract
~0.55% a round trip, at which point a large part of the library stops working.
Backtests that skip this are the reason paper edges evaporate in live trading,
so costs are charged on every single simulated trade here and are not optional.

Figures are the retail delivery schedule as of 2025-26 (discount broker):

    STT              0.1% on buy + 0.1% on sell     — the big one
    Stamp duty       0.015% on buy only
    Exchange txn     0.00297% both sides (NSE)
    SEBI turnover    0.0001% both sides
    GST              18% on (brokerage + exchange charges)
    Brokerage        0 for delivery at the zero-brokerage brokers
    DP charge        ~₹15 flat on sell, per scrip

Slippage is separate and larger than all of it: signals fill at the next open,
and the next open is not the price on the screen when the scan ran. 15 bps each
way is the default — optimistic for a small cap, roughly right for the liquid
names the turnover filter leaves in.
"""

from __future__ import annotations

from dataclasses import dataclass

STT_BUY_PCT = 0.10
STT_SELL_PCT = 0.10
STAMP_DUTY_BUY_PCT = 0.015
EXCHANGE_TXN_PCT = 0.00297
SEBI_TURNOVER_PCT = 0.0001
GST_RATE = 0.18
BROKERAGE_PCT = 0.0          # zero-brokerage delivery
DP_CHARGE_RUPEES = 15.0
DEFAULT_SLIPPAGE_BPS = 15.0  # each way


@dataclass(frozen=True)
class CostModel:
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS
    brokerage_pct: float = BROKERAGE_PCT
    dp_charge: float = DP_CHARGE_RUPEES

    def fill_price(self, quoted: float, side: str) -> float:
        """Slippage applied against you on both sides, always."""
        drift = quoted * self.slippage_bps / 10_000.0
        return quoted + drift if side == "buy" else quoted - drift

    def charges(self, buy_value: float, sell_value: float) -> float:
        """Statutory + broker charges on one completed round trip, in rupees."""
        brokerage = (buy_value + sell_value) * self.brokerage_pct / 100.0
        stt = buy_value * STT_BUY_PCT / 100.0 + sell_value * STT_SELL_PCT / 100.0
        stamp = buy_value * STAMP_DUTY_BUY_PCT / 100.0
        exchange = (buy_value + sell_value) * EXCHANGE_TXN_PCT / 100.0
        sebi = (buy_value + sell_value) * SEBI_TURNOVER_PCT / 100.0
        gst = (brokerage + exchange) * GST_RATE
        return brokerage + stt + stamp + exchange + sebi + gst + self.dp_charge

    def round_trip_pct(self, price: float, quantity: float) -> float:
        """Total cost as a percentage of the position — the number that matters.

        Reported in the UI so the drag is visible next to the gross edge rather
        than buried in the net figure.
        """
        value = price * quantity
        if value <= 0:
            return 0.0
        return self.charges(value, value) / value * 100.0


DEFAULT_COSTS = CostModel()
