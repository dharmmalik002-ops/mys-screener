import asyncio, time
from app.providers.free import FreeMarketDataProvider

async def main():
    provider = FreeMarketDataProvider()
    print("Testing get_snapshots with force_refresh=True")
    t0 = time.time()
    await provider.get_snapshots(100.0, force_refresh=True)
    print(f"Done in {time.time()-t0:.2f}s")

if __name__ == '__main__':
    asyncio.run(main())
