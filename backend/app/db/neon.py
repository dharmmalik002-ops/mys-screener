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

    # Strip any accidental quotes injected by Hugging Face Secrets UI
    dsn = dsn.strip('"').strip("'")

    if not dsn.startswith("postgres"):
        logger.error("Invalid DATABASE_URL format")
        return

    try:
        # Use simple ssl=True fallback if sslmode is missing, 
        # otherwise rely on the DSN's built-in ?sslmode=require string.
        use_ssl = True if "sslmode" not in dsn and "ssl=" not in dsn else None
        
        kwargs = {
            "min_size": 1,
            "max_size": 5,
            "command_timeout": 10
        }
        if use_ssl is not None:
             kwargs["ssl"] = use_ssl

        _pool = await asyncpg.create_pool(dsn, **kwargs)
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
                state_val = row["state_json"]
                # asyncpg usually returns JSON/JSONB as Python dict/list, but
                # be tolerant: handle str, bytes, memoryview and other cases.
                try:
                    if isinstance(state_val, (bytes, bytearray, memoryview)):
                        text = bytes(state_val).decode("utf-8")
                        return json.loads(text)
                    if isinstance(state_val, str):
                        return json.loads(state_val)
                    if isinstance(state_val, (dict, list)):
                        return state_val
                    # Last-resort: try parsing the string representation.
                    return json.loads(str(state_val))
                except Exception as exc:
                    logger.debug("Unable to deserialize watchlist state for %s: %s (%s)", market, type(state_val), exc)
                    return None
            return None
    except Exception as exc:
        logger.error("Error reading watchlist from DB for market %s: %s", market, exc)
        return None

async def save_watchlist(market: str, state_json: dict[str, Any]) -> None:
    pool = get_pool()
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            # Ensure we pass a JSON string so Postgres can cast to jsonb reliably.
            json_text = json.dumps(state_json)
            await conn.execute("""
                INSERT INTO watchlists_state (market, state_json, updated_at)
                VALUES ($1, $2::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (market)
                DO UPDATE SET 
                    state_json = EXCLUDED.state_json,
                    updated_at = CURRENT_TIMESTAMP;
            """, market, json_text)
    except Exception as exc:
        logger.error("Error saving watchlist to DB for market %s: %s", market, exc)
