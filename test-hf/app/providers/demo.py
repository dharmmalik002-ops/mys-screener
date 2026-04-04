import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random

from app.models.market import (
    ChartBar,
    CompanyFundamentals,
    CompanyUpdateItem,
    GrowthDriver,
    GrowthSnapshot,
    IndexQuoteItem,
    ProfitLossItem,
    QuarterlyResultItem,
    ShareholdingDelta,
    ShareholdingPatternItem,
    StockSnapshot,
    ValuationSnapshot,
)


class DemoMarketDataProvider:
    def __init__(self) -> None:
        self._sample_path = Path(__file__).resolve().parents[2] / "data" / "sample_universe.json"

    def _load_rows(self) -> list[dict]:
        return json.loads(self._sample_path.read_text(encoding="utf-8"))

    async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        rows = self._load_rows()
        return [
            StockSnapshot.model_validate(row)
            for row in rows
            if float(row["market_cap_crore"]) >= market_cap_min_crore
        ]

    async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        return await self.get_snapshots(market_cap_min_crore)

    def get_snapshot_updated_at(self) -> datetime | None:
        return datetime.now(timezone.utc)

    def get_last_refresh_metadata(self) -> dict[str, object]:
        return {
            "applied_quote_count": 0,
            "historical_rebuild": False,
            "quote_source": None,
        }

    async def get_index_quotes(self, symbols: list[str]) -> list[IndexQuoteItem]:
        now = datetime.now(timezone.utc)
        items: list[IndexQuoteItem] = []
        for index, symbol in enumerate(symbols):
            base_price = 10_000 + (index * 1_250)
            change_pct = round(((index % 5) - 2) * 0.38, 2)
            items.append(
                IndexQuoteItem(
                    symbol=symbol,
                    price=round(base_price * (1 + (change_pct / 100)), 2),
                    change_pct=change_pct,
                    updated_at=now,
                )
            )
        return items

    async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None) -> CompanyFundamentals:
        snapshots = await self.get_snapshots(market_cap_min_crore=0)
        item = snapshot or next((row for row in snapshots if row.symbol == symbol), None)
        name = item.name if item else symbol
        market_cap = item.market_cap_crore if item else None
        sales_base = round((market_cap or 10_000) * 0.04, 2) if market_cap else 100.0
        quarterly_results = [
            QuarterlyResultItem(
                period=period,
                sales_crore=round(sales_base * multiplier, 2),
                expenses_crore=round((sales_base * multiplier) * 0.73, 2),
                operating_profit_crore=round((sales_base * multiplier) * 0.27, 2),
                operating_margin_pct=27.0,
                profit_before_tax_crore=round((sales_base * multiplier) * 0.22, 2),
                net_profit_crore=round((sales_base * multiplier) * 0.17, 2),
                eps=round(8.0 * multiplier, 2),
            )
            for period, multiplier in (
                ("Jun 2025", 0.94),
                ("Sep 2025", 0.98),
                ("Dec 2025", 1.02),
                ("Mar 2026", 1.06),
            )
        ]
        profit_loss = [
            ProfitLossItem(
                period=period,
                sales_crore=round(sales_base * multiplier, 2),
                operating_profit_crore=round((sales_base * multiplier) * 0.26, 2),
                operating_margin_pct=26.0,
                net_profit_crore=round((sales_base * multiplier) * 0.16, 2),
                eps=round(30 * multiplier, 2),
                dividend_payout_pct=32.0,
            )
            for period, multiplier in (
                ("Mar 2022", 0.82),
                ("Mar 2023", 0.9),
                ("Mar 2024", 1.0),
                ("Mar 2025", 1.08),
                ("TTM", 1.12),
            )
        ]

        return CompanyFundamentals(
            symbol=symbol,
            name=name,
            exchange=item.exchange if item else "NSE",
            sector=item.sector if item else "Demo",
            sub_sector=item.sub_sector if item else "Demo Universe",
            about=f"{name} fundamentals are shown from the local fallback dataset while live sources are unavailable.",
            quarterly_results=quarterly_results,
            profit_loss=profit_loss,
            growth=GrowthSnapshot(
                latest_period="Mar 2026",
                sales_qoq_pct=3.92,
                sales_yoy_pct=12.41,
                profit_qoq_pct=5.88,
                profit_yoy_pct=14.77,
                operating_margin_latest_pct=27.0,
                operating_margin_previous_pct=26.2,
                net_margin_latest_pct=17.0,
                net_margin_previous_pct=16.3,
            ),
            valuation=ValuationSnapshot(
                market_cap_crore=market_cap,
                pe_ratio=24.4,
                peg_ratio=1.65,
                operating_margin_pct=27.0,
                net_margin_pct=17.0,
                roce_pct=22.5,
                roe_pct=18.4,
                dividend_yield_pct=1.1,
            ),
            growth_drivers=[
                GrowthDriver(
                    title="Quarterly sales are still expanding",
                    detail="Recent revenue and profit are trending higher in the fallback dataset, which keeps the operating profile constructive.",
                    tone="positive",
                ),
                GrowthDriver(
                    title="Margins remain stable",
                    detail="Operating and net margins are holding up versus the prior quarter in the demo fundamentals profile.",
                    tone="neutral",
                ),
            ],
            recent_updates=[
                CompanyUpdateItem(
                    title="Demo fundamentals active",
                    source="Local fallback",
                    summary="Live company fundamentals are unavailable right now, so the chart is showing cached demo content.",
                    kind="news",
                )
            ],
            shareholding_pattern=[
                ShareholdingPatternItem(period="Sep 2025", promoter_pct=54.2, fii_pct=17.4, dii_pct=13.1, public_pct=15.3, shareholder_count=248_120),
                ShareholdingPatternItem(period="Dec 2025", promoter_pct=54.2, fii_pct=17.8, dii_pct=13.4, public_pct=14.6, shareholder_count=253_440),
            ],
            shareholding_delta=ShareholdingDelta(
                latest_period="Dec 2025",
                previous_period="Sep 2025",
                promoter_change_pct=0.0,
                fii_change_pct=0.4,
                dii_change_pct=0.3,
                public_change_pct=-0.7,
            ),
            data_warnings=["Live fundamentals source unavailable, showing fallback content."],
        )

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 240) -> list[ChartBar]:
        snapshots = await self.get_snapshots(market_cap_min_crore=0)
        snapshot = next((row for row in snapshots if row.symbol == symbol), None)
        base_price = snapshot.last_price if snapshot else 100.0

        seed = int(hashlib.sha256(f"{symbol}:{timeframe}".encode("utf-8")).hexdigest()[:8], 16)
        rng = Random(seed)
        now = datetime.now(timezone.utc)
        step_map = {
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "1D": timedelta(days=1),
            "1W": timedelta(weeks=1),
        }
        step = step_map.get(timeframe, timedelta(days=1))
        started_at = now - (bars * step)
        price = base_price * 0.82
        result: list[ChartBar] = []

        for index in range(bars):
            drift = 0.0018 if index > bars * 0.65 else 0.0009
            wave = math.sin(index / 11) * 0.004
            noise = rng.uniform(-0.012, 0.014)
            open_price = max(1.0, price)
            close_price = max(1.0, open_price * (1 + drift + wave + noise))
            wick = abs(rng.uniform(0.004, 0.018))
            high = max(open_price, close_price) * (1 + wick)
            low = min(open_price, close_price) * (1 - wick * 0.85)
            volume = int((snapshot.avg_volume_20d if snapshot else 100_000) * (0.5 + rng.random() * 1.4))
            timestamp = int((started_at + (index * step)).timestamp())
            result.append(
                ChartBar(
                    time=timestamp,
                    open=round(open_price, 2),
                    high=round(high, 2),
                    low=round(low, 2),
                    close=round(close_price, 2),
                    volume=volume,
                )
            )
            price = close_price

        return result
