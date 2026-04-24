#!/usr/bin/env python3
import asyncio
import traceback
import sys

async def main():
    try:
        from app.core.config import get_settings
        from app.providers.factory import build_provider

        settings = get_settings()
        prov = build_provider(settings, market="india")
        print("Provider:", type(prov).__name__)
        bars = await prov.get_chart("RELIANCE", "1D", bars=520)
        print("Result type:", type(bars))
        print("Bars count:", len(bars))
        if bars:
            b = bars[-1]
            try:
                # pydantic model
                print("Sample bar:", b.model_dump() if hasattr(b, "model_dump") else b)
            except Exception:
                print("Sample bar repr:", repr(b))
    except Exception:
        traceback.print_exc()
        sys.exit(2)

if __name__ == '__main__':
    asyncio.run(main())
