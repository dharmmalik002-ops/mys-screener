from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys


def generate_historical_breadth(market: str) -> Path:
    backend_root = Path(__file__).resolve().parents[1]
    if str(backend_root) not in sys.path:
        sys.path.insert(0, str(backend_root))

    from app.core.config import get_settings
    from app.providers.factory import build_provider

    settings = get_settings()
    provider = build_provider(settings, market=market)
    asyncio.run(provider.refresh_snapshots(settings.market_cap_min_crore))
    output_path = getattr(provider, "historical_breadth_cache_path", None)
    if output_path is None:
        raise RuntimeError(f"No historical breadth cache path is defined for market '{market}'.")
    if not output_path.exists():
        raise RuntimeError(f"Historical breadth generation finished without creating {output_path}.")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate historical breadth data for the selected market.")
    parser.add_argument("--market", choices=["india", "us"], default="india")
    args = parser.parse_args()
    output_path = generate_historical_breadth(args.market)
    print(f"Generated historical breadth data at {output_path}")


if __name__ == "__main__":
    main()
