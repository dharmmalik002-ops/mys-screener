import asyncio
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.core.config import get_settings
from app.providers.free import FreeMarketDataProvider
from app.services.dashboard_service import DashboardService

async def main():
    settings = get_settings()
    provider = FreeMarketDataProvider(settings)
    service = DashboardService(provider, settings)
    
    try:
        res = await service.get_sector_tab("1D", "desc")
        print("Success", len(res.sectors))
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
