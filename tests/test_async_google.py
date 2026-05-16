"""AsyncGoogleClient offloads blocking Google calls off the event loop.

The GoogleClient implementations are synchronous; calling one straight
from the async reconcile path would freeze FastAPI's event loop for
the whole Google round-trip.  AsyncGoogleClient runs each call in a
worker thread, so other coroutines keep running meanwhile.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from app.ledger.async_google import AsyncGoogleClient, as_async_google

pytestmark = pytest.mark.asyncio


class _SlowSyncClient:
    """A sync GoogleClient whose call blocks for 300ms."""

    def __init__(self):
        self.calls = 0

    def list_events(self, *args, **kwargs):
        self.calls += 1
        time.sleep(0.3)  # blocking — must NOT run on the event loop
        return {"items": [], "args": args, "kwargs": kwargs}


async def test_slow_call_does_not_block_the_event_loop():
    client = AsyncGoogleClient(_SlowSyncClient())
    ticks: list[float] = []

    async def _ticker():
        # 10 ticks × 20ms ≈ 200ms — must all land while the 300ms
        # Google call is in flight if the loop is not blocked.
        for _ in range(10):
            ticks.append(time.monotonic())
            await asyncio.sleep(0.02)

    start = time.monotonic()
    result, _ = await asyncio.gather(
        client.list_events("cal-1", show_deleted=True),
        _ticker(),
    )
    elapsed = time.monotonic() - start

    assert result["items"] == []
    assert len(ticks) == 10, "the ticker coroutine was starved"
    # Concurrent: total ≈ max(300ms, 200ms).  If the blocking call had
    # frozen the loop, the ticker would run only after it → ~500ms.
    assert elapsed < 0.45, f"event loop appears to have blocked ({elapsed:.3f}s)"
    # Some ticks landed while the 300ms call was still running.
    assert any(t - start < 0.3 for t in ticks)


async def test_call_forwards_args_and_result():
    client = AsyncGoogleClient(_SlowSyncClient())
    result = await client.list_events("cal-9", page_token="p1")
    assert result["args"] == ("cal-9",)
    assert result["kwargs"] == {"page_token": "p1"}


async def test_as_async_google_is_idempotent():
    inner = _SlowSyncClient()
    once = as_async_google(inner)
    twice = as_async_google(once)
    assert isinstance(once, AsyncGoogleClient)
    assert twice is once  # already wrapped — not re-wrapped
