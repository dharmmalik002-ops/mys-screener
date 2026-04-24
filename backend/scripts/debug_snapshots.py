import asyncio
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.providers.free import FreeMarketDataProvider
from app.core.config import get_settings

async def debug():
    settings = get_settings()
    provider = FreeMarketDataProvider(gemini_api_key=settings.gemini_api_key)
    
    print(f"Checking snapshots for market cap >= {settings.market_cap_min_crore}...")
    try:
        snapshots = await provider.get_snapshots(settings.market_cap_min_crore)
        print(f"Successfully loaded {len(snapshots)} snapshots.")
        if snapshots:
            print(f"First stock: {snapshots[0].symbol} - {snapshots[0].last_price}")
    except Exception as e:
        print(f"Error loading snapshots: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug())
