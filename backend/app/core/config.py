import os
import sys
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
        populate_by_name=True,
    )

    app_env: str = "development"
    app_name: str = "Mr. Malik Scanner"
    data_mode: Literal["demo", "upstox", "free"] = "free"
    market_cap_min_crore: float = Field(default=1500, alias="MARKET_CAP_MIN_CRORE")
    frontend_origin: str | None = Field(default=None, alias="FRONTEND_ORIGIN")
    frontend_origin_regex: str | None = Field(default=r"https://.*\.vercel\.app$", alias="FRONTEND_ORIGIN_REGEX")
    upstox_access_token: str | None = Field(default=None, alias="UPSTOX_ACCESS_TOKEN")
    upstox_base_url: str = Field(default="https://api.upstox.com", alias="UPSTOX_BASE_URL")
    gemini_api_key: str | None = Field(default=None, alias="GEMINI_API_KEY")
    india_eod_only: bool = Field(default=True, alias="INDIA_EOD_ONLY")
    live_universe_path: Path = Field(default=Path("data/live_universe.json"), alias="LIVE_UNIVERSE_PATH")
    default_timeframe: str = "1D"
    refresh_timeout_seconds: int = Field(default=600, alias="REFRESH_TIMEOUT_SECONDS")
    warm_fundamentals_after_refresh: bool = Field(default=False, alias="WARM_FUNDAMENTALS_AFTER_REFRESH")
    startup_cache_warm_enabled: bool = Field(default=False, alias="STARTUP_CACHE_WARM_ENABLED")
    database_url: str | None = Field(default=None, alias="DATABASE_URL")
    app_state_dir: Path | None = Field(default=None, alias="APP_STATE_DIR")
    watchlists_backend: Literal["auto", "file", "database"] = Field(default="auto", alias="WATCHLISTS_BACKEND")
    watchlists_database_connect_timeout_seconds: int = Field(
        default=10,
        alias="WATCHLISTS_DATABASE_CONNECT_TIMEOUT_SECONDS",
    )

    @staticmethod
    def _default_app_state_dir() -> Path:
        if sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "MrMalikScanner"
        if sys.platform == "win32":
            local_appdata = os.environ.get("LOCALAPPDATA")
            base_dir = Path(local_appdata) if local_appdata else (Path.home() / "AppData" / "Local")
            return base_dir / "MrMalikScanner"
        xdg_state_home = os.environ.get("XDG_STATE_HOME")
        base_dir = Path(xdg_state_home) if xdg_state_home else (Path.home() / ".local" / "state")
        return base_dir / "mr-malik-scanner"

    @model_validator(mode="after")
    def resolve_paths(self) -> "Settings":
        backend_root = Path(__file__).resolve().parents[2]
        if not self.live_universe_path.is_absolute():
            object.__setattr__(self, "live_universe_path", backend_root / self.live_universe_path)
        app_state_dir = self.app_state_dir or self._default_app_state_dir()
        if not app_state_dir.is_absolute():
            app_state_dir = backend_root / app_state_dir
        object.__setattr__(self, "app_state_dir", app_state_dir)
        database_url = str(self.database_url or "").strip() or None
        object.__setattr__(self, "database_url", database_url)
        return self

    @property
    def frontend_origins(self) -> list[str]:
        origins = {
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        }
        raw_origins = str(self.frontend_origin or "").strip()
        if raw_origins:
            for candidate in raw_origins.split(","):
                normalized = candidate.strip().rstrip("/")
                if not normalized or "your-frontend-domain" in normalized or "example.com" in normalized:
                    continue
                origins.add(normalized)
        return sorted(origins)

    @property
    def use_watchlists_database(self) -> bool:
        if self.watchlists_backend == "file":
            return False
        return bool(self.database_url)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
