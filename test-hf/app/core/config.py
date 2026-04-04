from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = "development"
    app_name: str = "Mr. Malik Scanner"
    data_mode: Literal["demo", "upstox", "free"] = "free"
    market_cap_min_crore: float = Field(default=800, alias="MARKET_CAP_MIN_CRORE")
    frontend_origin: str = Field(default="http://127.0.0.1:5173", alias="FRONTEND_ORIGIN")
    upstox_access_token: str | None = Field(default=None, alias="UPSTOX_ACCESS_TOKEN")
    upstox_base_url: str = Field(default="https://api.upstox.com", alias="UPSTOX_BASE_URL")
    gemini_api_key: str | None = Field(default=None, alias="GEMINI_API_KEY")
    live_universe_path: Path = Field(default=Path("data/live_universe.json"), alias="LIVE_UNIVERSE_PATH")
    default_timeframe: str = "1D"
    refresh_timeout_seconds: int = Field(default=600, alias="REFRESH_TIMEOUT_SECONDS")
    warm_fundamentals_after_refresh: bool = Field(default=False, alias="WARM_FUNDAMENTALS_AFTER_REFRESH")

    @model_validator(mode="after")
    def resolve_paths(self) -> "Settings":
        backend_root = Path(__file__).resolve().parents[2]
        if not self.live_universe_path.is_absolute():
            object.__setattr__(self, "live_universe_path", backend_root / self.live_universe_path)
        return self

    @property
    def frontend_origins(self) -> list[str]:
        origins = {
            self.frontend_origin.rstrip("/"),
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        }
        return sorted(origins)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
