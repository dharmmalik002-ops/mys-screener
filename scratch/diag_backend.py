
import asyncio
import logging
import sys
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).resolve().parents[1] / "backend"))

from app.core.config import get_settings
from app.providers.factory import build_provider
from app.services.dashboard_service import DashboardService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    settings = get_settings()
    # Explicitly set data dir if needed, but AppConfig should handle it
    
    india_provider = build_provider(settings, market="india")
    service = DashboardService(provider=india_provider, settings=settings)
    
    print("--- Testing Snapshot Load ---")
    try:
        snapshots = await service._snapshots()
        print(f"Loaded {len(snapshots)} snapshots")
        if snapshots:
            print(f"First symbol: {snapshots[0].symbol}, Date: {snapshots[0].history_as_of_date}")
    except Exception as e:
        print(f"Failed to load snapshots: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- Testing Dashboard Build ---")
    try:
        dashboard = await service.build_dashboard()
        print(f"Dashboard generated_at: {dashboard.generated_at}")
        print(f"Top gainers: {len(dashboard.top_gainers)}")
        print(f"Scanners: {len(dashboard.scanners)}")
    except Exception as e:
        print(f"Failed to build dashboard: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
