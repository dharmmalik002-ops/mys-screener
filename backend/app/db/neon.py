import json
import logging
from typing import Any

import asyncpg

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None

async def init_db_pool() -> None:
    global _pool
    settings = get_settings()
    if not settings.database_url:
        logger.debug("No DATABASE_URL set; running without Neon Postgres")
        return

    dsn = settings.database_url
    if dsn.startswith("postgresql://"):
        dsn = dsn.replace("postgresql://", "postgres://", 1)

    try:
        _pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            command_timeout=10,
        )
        if _pool is not None:
            async with _pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS watchlists_state (
                        market VARCHAR(50) PRIMARY KEY,
                        state_json JSONB NOT NULL,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    );
                """)
            logger.info("Neon database pool initialized and tables verified.")
    except Exception as exc:
        logger.error("Failed to initialize Neon DB pool: %s", exc)
        _pool = None

async def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

def get_pool() -> asyncpg.Pool | None:
    return _pool

async def get_watchlist(market: str) -> dict[str, Any] | None:
    pool = get_pool()
    if pool is None:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state_json FROM watchlists_state WHERE market = $1", 
                market
            )
            if row:
                return json.loads(row["state_json"])
            return None
    except Exception as exc:
        logger.error("Error reading watchlist from DB for market %s: %s", market, exc)
        return None

async def save_watchlist(market: str, state_json: dict[str, Any]) -> None:
    pool = get_pool()
    if pool is None:
        return
    try:
        json_str = json.dumps(state_json)
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO watchlists_state (market, state_json, updated_at)
                VALUES ($1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (market)
                DO UPDATE SET 
                    state_json = EXCLUDED.state_json,
                    updated_at = CURRENT_TIMESTAMP;
            """, market, json_str)
    except Exception as exc:
        logger.error("Error saving watchlist to DB for market %s: %s", market, exc)
