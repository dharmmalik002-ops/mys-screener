from datetime import datetime
from typing import Protocol

from app.models.market import ChartBar, CompanyFundamentals, IndexQuoteItem, StockSnapshot


class MarketDataProvider(Protocol):
    async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        ...

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 240) -> list[ChartBar]:
        ...

    async def get_index_quotes(self, symbols: list[str]) -> list[IndexQuoteItem]:
        ...

    async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None) -> CompanyFundamentals:
        ...

    async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        ...

    def get_snapshot_updated_at(self) -> datetime | None:
        ...

    def get_last_refresh_metadata(self) -> dict[str, object]:
        ...
