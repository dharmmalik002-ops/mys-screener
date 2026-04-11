import asyncio
from app.providers.us_free import USFreeMarketDataProvider

async def main():
    provider = USFreeMarketDataProvider()
    rows = [{"symbol": "AAPL", "market_cap_crore": 100000}, {"symbol": "TSLA"}]
    res, sources = provider._fetch_live_quotes_for_rows(rows)
    print("AAPL:", res.get("AAPL"))
    print("TSLA:", res.get("TSLA"))

if __name__ == "__main__":
    asyncio.run(main())
