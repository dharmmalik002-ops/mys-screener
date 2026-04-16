
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

async def main():
    settings = get_settings()
    india_provider = build_provider(settings, market="india")
    service = DashboardService(provider=india_provider, settings=settings)
    
    print("--- Diagnosing Universe 0 ---")
    
    # 1. Check snapshots
    snapshots = await service._snapshots()
    print(f"Total snapshots from provider: {len(snapshots)}")
    
    # 2. Check scan eligible snapshots
    scan_eligible = service._scan_eligible_snapshots(snapshots)
    print(f"Scan eligible snapshots: {len(scan_eligible)}")
    
    if len(snapshots) > 0 and len(scan_eligible) == 0:
        print("DETECTED: Snapshots exist but NONE are scan-eligible.")
        # Check first snapshot why it is NOT eligible
        s = snapshots[0]
        print(f"Sample snapshot: {s.symbol}")
        print(f"Price: {s.last_price}, Cap: {s.market_cap_crore}, Listing: {s.listing_date}")
        
    # 3. Check dashboard build
    try:
        dashboard = await service.build_dashboard()
        print(f"Dashboard symbol count: {len(dashboard.top_gainers) + len(dashboard.top_losers)}")
    except Exception as e:
        print(f"Dashboard build failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
