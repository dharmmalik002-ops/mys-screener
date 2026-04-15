import asyncio
import asyncpg
import sys

async def main():
    try:
        pool = await asyncio.wait_for(
            asyncpg.create_pool("postgres://a:b@c.d/e", min_size=1, max_size=1, command_timeout=2, ssl="require"),
            timeout=5
        )
        print("Success")
    except Exception as e:
        print(f"Error: {e}")

asyncio.run(main())
