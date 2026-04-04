import asyncio
from app.providers.free import FreeMarketDataProvider
import time
from pathlib import Path

async def main():
    print("Starting provider...")
    provider = FreeMarketDataProvider()
    print("Fetching universe...")
    t0 = time.time()
    try:
        res = await provider.refresh_snapshots(100.0)
        t1 = time.time()
        print(f"Got {len(res)} snapshots in {t1-t0:.2f}s")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
