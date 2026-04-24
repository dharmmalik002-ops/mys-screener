from app.core.config import Settings
from app.providers.demo import DemoMarketDataProvider
from app.providers.free import FreeMarketDataProvider
from app.providers.upstox import UpstoxMarketDataProvider


def build_provider(settings: Settings, market: str = "india"):
    normalized_market = str(market or "india").strip().lower()
    if normalized_market != "india":
        raise ValueError(f"Unsupported market: {market}")
    if settings.data_mode == "free":
        return FreeMarketDataProvider(
            gemini_api_key=settings.gemini_api_key,
            eod_only_mode=settings.india_eod_only,
        )
    if settings.data_mode == "upstox":
        return UpstoxMarketDataProvider(
            access_token=settings.upstox_access_token,
            base_url=settings.upstox_base_url,
            live_universe_path=settings.live_universe_path,
        )
    return DemoMarketDataProvider()
