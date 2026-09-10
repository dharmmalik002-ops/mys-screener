"""A due post-close refresh must not take the screeners down.

Every screener endpoint starts with `provider.get_snapshots(...)`. When the
market-close refresh was due, that call blocked on a full rebuild of the
universe -- and `_get_or_create_snapshot_request_task` started a SECOND,
unregistered rebuild for every request that had piggy-backed on the first,
because the refresh being due is a condition the rebuild cannot clear on its
own (the session's bhavcopy has to arrive first).

On the live Space, with the bhavcopy pipeline stuck for three days, that meant
every scan request launched its own crawl of ~1,900 symbols and none of them
finished: `/api/scans`, `/api/scan-counts` and `/api/scanner-scorecard` hung
past an hour and the app showed "Request failed: 500" on every screener, while
endpoints that read a committed artifact (`/api/groups`) stayed fast.

These tests pin the two behaviours that keep it up: serve what we already have,
and rebuild once, behind the request.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from app.providers.free import FreeMarketDataProvider


def _provider() -> FreeMarketDataProvider:
    provider = FreeMarketDataProvider.__new__(FreeMarketDataProvider)
    provider._snapshots_memory_cache = {}
    provider._snapshot_request_tasks = {}
    provider._background_snapshot_refresh_tasks = {}
    provider._close_refresh_retry_after = {}
    return provider


class ClosedSessionRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.provider = _provider()
        self.rebuilds = 0

        async def fake_rebuild(market_cap_min_crore, force_refresh=False):
            self.rebuilds += 1
            await asyncio.sleep(0.05)  # a real rebuild is minutes; this stands in
            return ["rebuilt"]

        self.provider._load_snapshots_with_fallback = fake_rebuild  # type: ignore[assignment]
        self.provider._snapshot_memory_signature = lambda: (1.0, 2.0)  # type: ignore[assignment]
        self.provider.preferred_refresh_strategy = lambda: "historical"  # type: ignore[assignment]
        self.provider._market_close_refresh_due = lambda: True  # type: ignore[assignment]
        self.provider._seed_snapshot_cache_needs_refresh = lambda: False  # type: ignore[assignment]

    async def _drain(self) -> None:
        tasks = list(self.provider._background_snapshot_refresh_tasks.values())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_a_warm_cache_is_served_immediately_not_rebuilt(self):
        self.provider._snapshots_memory_cache[500.0] = (1.0, 2.0, ["cached"])
        result = await asyncio.wait_for(self.provider.get_snapshots(500.0), timeout=1)
        self.assertEqual(result, ["cached"], "a due close refresh must not block the request")
        await self._drain()
        self.assertEqual(self.rebuilds, 1, "and it should refresh exactly once, behind the request")

    async def test_a_disk_cache_is_materialised_rather_than_rebuilt(self):
        self.provider._load_cached_snapshot_rows = lambda *_: [{"symbol": "X"}]  # type: ignore[assignment]
        self.provider._materialize_snapshot_rows = lambda rows: ["from-disk"]  # type: ignore[assignment]
        result = await asyncio.wait_for(self.provider.get_snapshots(500.0), timeout=1)
        self.assertEqual(result, ["from-disk"])
        await self._drain()
        self.assertEqual(self.rebuilds, 1)

    async def test_twenty_concurrent_requests_trigger_one_rebuild(self):
        self.provider._snapshots_memory_cache[500.0] = (1.0, 2.0, ["cached"])
        results = await asyncio.wait_for(
            asyncio.gather(*(self.provider.get_snapshots(500.0) for _ in range(20))),
            timeout=2,
        )
        self.assertTrue(all(r == ["cached"] for r in results))
        await self._drain()
        self.assertEqual(self.rebuilds, 1, "this is the stampede that hung the Space")

    async def test_a_refresh_that_cannot_clear_the_condition_backs_off(self):
        # The refresh finishes but the session is still stale -- exactly what
        # happens while the bhavcopy for that session is missing.
        self.provider._snapshots_memory_cache[500.0] = (1.0, 2.0, ["cached"])
        for _ in range(5):
            await asyncio.wait_for(self.provider.get_snapshots(500.0), timeout=1)
            await self._drain()
        self.assertEqual(self.rebuilds, 1, "a due-but-unsatisfiable refresh must not retry per request")

        # ...until the cooldown lapses.
        self.provider._close_refresh_retry_after[500.0] = 0.0
        await asyncio.wait_for(self.provider.get_snapshots(500.0), timeout=1)
        await self._drain()
        self.assertEqual(self.rebuilds, 2)

    async def test_with_nothing_cached_the_request_still_waits_for_a_build(self):
        self.provider._load_cached_snapshot_rows = lambda *_: []  # type: ignore[assignment]
        result = await asyncio.wait_for(self.provider.get_snapshots(500.0), timeout=2)
        self.assertEqual(result, ["rebuilt"], "with no cache there is nothing else to serve")
        self.assertEqual(self.rebuilds, 1)

    async def test_joining_an_in_flight_build_does_not_start_another(self):
        self.provider._load_cached_snapshot_rows = lambda *_: []  # type: ignore[assignment]
        results = await asyncio.wait_for(
            asyncio.gather(*(self.provider.get_snapshots(500.0) for _ in range(10))),
            timeout=2,
        )
        self.assertTrue(all(r == ["rebuilt"] for r in results))
        self.assertEqual(self.rebuilds, 1)


class RefreshStrategySourceTests(unittest.TestCase):
    def test_get_snapshots_no_longer_blocks_on_a_close_refresh(self):
        import inspect

        src = inspect.getsource(FreeMarketDataProvider.get_snapshots)
        historical = [line for line in src.splitlines() if "historical" in line]
        self.assertTrue(historical)
        for line in historical:
            self.assertNotIn("await self._get_or_create_snapshot_request_task", line)
        self.assertIn("_schedule_close_refresh", src)

    def test_the_piggy_back_branch_returns_the_shared_result(self):
        import inspect

        src = inspect.getsource(FreeMarketDataProvider._get_or_create_snapshot_request_task)
        head = src.split("task = asyncio.create_task")[0]
        self.assertNotIn("_load_snapshots_with_fallback", head)
        self.assertIn("return await current_task", head)


if __name__ == "__main__":
    unittest.main()
