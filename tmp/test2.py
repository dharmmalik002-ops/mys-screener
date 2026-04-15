import asyncio, asyncpg
async def main():
    try:
        pool = await asyncio.wait_for(asyncpg.create_pool('postgres://a:b@c/d?sslmode=require', ssl='require'), timeout=1)
    except Exception as e:
        print(f"E: {type(e)} {e}")
asyncio.run(main())
