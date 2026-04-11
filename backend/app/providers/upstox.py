import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

from app.models.market import ChartBar, CompanyFundamentals, IndexQuoteItem, StockSnapshot
from app.providers.demo import DemoMarketDataProvider


class UpstoxMarketDataProvider:
    """
    Free-first live provider.

    This provider falls back to demo data until a valid Upstox access token and a
    prepared `live_universe.json` file are available locally.
    """

    def __init__(
        self,
        access_token: str | None,
        base_url: str,
        live_universe_path: Path,
    ) -> None:
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")
        self.live_universe_path = live_universe_path
        self.demo = DemoMarketDataProvider()

    def _can_use_live_mode(self) -> bool:
        return bool(self.access_token and self.live_universe_path.exists())

    def _load_universe_rows(self) -> list[dict]:
        return json.loads(self.live_universe_path.read_text(encoding="utf-8"))

    async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        if not self._can_use_live_mode():
            return await self.demo.get_snapshots(market_cap_min_crore)

        rows = [
            row
            for row in self._load_universe_rows()
            if float(row.get("market_cap_crore", 0)) >= market_cap_min_crore
        ]

        # Until the historical cache builder is added, live mode reads the enriched
        # snapshot fields from the generated local universe file.
        return [StockSnapshot.model_validate(row) for row in rows]

    async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        return await self.get_snapshots(market_cap_min_crore)

    def get_snapshot_updated_at(self) -> datetime | None:
        if self._can_use_live_mode() and self.live_universe_path.exists():
            return datetime.fromtimestamp(self.live_universe_path.stat().st_mtime, tz=timezone.utc)
        return self.demo.get_snapshot_updated_at()

    def get_last_refresh_metadata(self) -> dict[str, object]:
        return {
            "applied_quote_count": 0,
            "historical_rebuild": False,
            "quote_source": None,
        }

    async def get_index_quotes(self, symbols: list[str]) -> list[IndexQuoteItem]:
        return await self.demo.get_index_quotes(symbols)

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 240) -> list[ChartBar]:
        if not self._can_use_live_mode():
            return await self.demo.get_chart(symbol, timeframe, bars)

        rows = self._load_universe_rows()
        row = next((item for item in rows if item["symbol"] == symbol), None)
        if not row or not row.get("instrument_key"):
            return await self.demo.get_chart(symbol, timeframe, bars)

        instrument_key = quote(row["instrument_key"], safe="")
        if timeframe in {"1D", "1W"}:
            unit, interval = ("days", "1")
            lookback_days = max(420, int(bars * 1.6))
        else:
            unit, interval = ("minutes", "15")
            lookback_days = 15 if timeframe in {"15m", "30m"} else 60
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=lookback_days)
        endpoint = f"{self.base_url}/v3/historical-candle/{instrument_key}/{unit}/{interval}/{end_date.isoformat()}/{start_date.isoformat()}"
        headers = {"Accept": "application/json"}
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(endpoint, headers=headers)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPError:
            return await self.demo.get_chart(symbol, timeframe, bars)

        candles = payload.get("data", {}).get("candles", [])
        parsed = [
            ChartBar(
                time=int(datetime.fromisoformat(candle[0].replace("Z", "+00:00")).timestamp()),
                open=float(candle[1]),
                high=float(candle[2]),
                low=float(candle[3]),
                close=float(candle[4]),
                volume=int(candle[5]),
            )
            for candle in reversed(candles[-bars:])
        ]
        if timeframe == "1W":
            parsed = self._aggregate_weekly_bars(parsed)
            parsed = parsed[-bars:]
        return parsed or await self.demo.get_chart(symbol, timeframe, bars)

    async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None) -> CompanyFundamentals:
        return await self.demo.get_fundamentals(symbol, snapshot)

    def _aggregate_weekly_bars(self, bars: list[ChartBar]) -> list[ChartBar]:
        weekly: list[ChartBar] = []
        current_week: tuple[int, int] | None = None
        bucket: list[ChartBar] = []

        for bar in sorted(bars, key=lambda item: item.time):
            stamp = datetime.fromtimestamp(bar.time, tz=timezone.utc)
            week_key = stamp.isocalendar()[:2]
            if current_week is None:
                current_week = week_key
            if week_key != current_week and bucket:
                weekly.append(self._merge_bar_bucket(bucket))
                bucket = []
                current_week = week_key
            bucket.append(bar)

        if bucket:
            weekly.append(self._merge_bar_bucket(bucket))

        return weekly

    @staticmethod
    def _merge_bar_bucket(bucket: list[ChartBar]) -> ChartBar:
        first = bucket[0]
        last = bucket[-1]
        return ChartBar(
            time=last.time,
            open=first.open,
            high=max(item.high for item in bucket),
            low=min(item.low for item in bucket),
            close=last.close,
            volume=sum(item.volume for item in bucket),
        )
