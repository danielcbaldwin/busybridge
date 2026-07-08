"""Async boundary for the synchronous ``GoogleClient`` protocol.

The ``GoogleClient`` implementations are deliberately synchronous: the
production :class:`~app.ledger.real_google_client.RealGoogleClient`
wraps the blocking ``googleapiclient``, and the in-memory
``FakeGoogleCalendar`` stays trivial so its own unit tests can call it
directly.

But the ledger reconcile path is ``async`` and runs on FastAPI's
single event loop.  Calling a blocking ``RealGoogleClient`` method
straight from there freezes the loop — webhooks, the UI, every other
user's work — for the whole Google round-trip.

:class:`AsyncGoogleClient` wraps *any* synchronous ``GoogleClient``
and re-exposes the same surface as awaitable methods, each offloaded
to a worker thread with :func:`asyncio.to_thread`.  The reconcile path
awaits these; the underlying sync client (real or fake) is untouched,
so the protocol and the fake's direct unit tests do not change.

This is a contained async boundary, not a protocol-wide rewrite: only
the ledger code that actually issues Google calls awaits them.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional


class _AsyncRateLimiter:
    """Token-bucket limiter that paces outbound Google API calls.

    Google enforces a per-user request quota; a large burst — a bulk
    re-render drain, or a full-sync ingest fanning out instance scans —
    blows past it and earns a storm of ``rateLimitExceeded`` 403s that
    then retry and amplify.  This bucket caps the sustained call rate
    (with a small burst for idle bursts) so those bursts flow smoothly
    instead of being throttled.  ``rate <= 0`` disables it.

    The await happens under the lock, which serialises acquirers — the
    intended behaviour: calls leave one at a time, spaced to the rate.
    """

    __slots__ = ("_rate", "_capacity", "_tokens", "_updated", "_lock")

    def __init__(self, rate_per_sec: float, burst: Optional[float] = None):
        self._rate = max(0.0, float(rate_per_sec))
        self._capacity = float(burst) if burst is not None else max(1.0, self._rate)
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self._rate <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                self._capacity, self._tokens + (now - self._updated) * self._rate,
            )
            self._updated = now
            if self._tokens < 1.0:
                await asyncio.sleep((1.0 - self._tokens) / self._rate)
                # Exactly one token accrued over that sleep.
                self._tokens = 1.0
                self._updated = time.monotonic()
            self._tokens -= 1.0


_production_limiter: Optional[_AsyncRateLimiter] = None


def production_rate_limiter() -> _AsyncRateLimiter:
    """The process-wide limiter for real Google traffic, built lazily
    from settings so every reconcile/drain shares one pace.  Tests build
    their own clients around the in-memory fake and never call this, so
    the fake is never throttled."""
    global _production_limiter
    if _production_limiter is None:
        from app.config import get_settings

        rate = get_settings().google_api_rate_limit_per_second
        _production_limiter = _AsyncRateLimiter(rate)
    return _production_limiter


class AsyncGoogleClient:
    """Awaitable adapter over a synchronous ``GoogleClient``.

    Every call is run via :func:`asyncio.to_thread`, so a slow Google
    request occupies a worker thread instead of blocking the event
    loop.  The in-memory fake is fast, but routing it through a thread
    too keeps a single uniform code path (calls within one reconcile
    are awaited one at a time, so the fake is never touched
    concurrently).
    """

    __slots__ = ("_sync", "_limiter")

    def __init__(self, sync_client: Any, limiter: Optional[_AsyncRateLimiter] = None):
        self._sync = sync_client
        self._limiter = limiter

    async def _call(self, fn: Any, *args, **kwargs):
        # Pace real traffic (limiter set) before occupying a worker
        # thread; the fake is wrapped without a limiter so its own tests
        # stay instant.
        if self._limiter is not None:
            await self._limiter.acquire()
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def insert_event(self, *args, **kwargs):
        return await self._call(self._sync.insert_event, *args, **kwargs)

    async def get_event(self, *args, **kwargs):
        return await self._call(self._sync.get_event, *args, **kwargs)

    async def update_event(self, *args, **kwargs):
        return await self._call(self._sync.update_event, *args, **kwargs)

    async def patch_event(self, *args, **kwargs):
        return await self._call(self._sync.patch_event, *args, **kwargs)

    async def delete_event(self, *args, **kwargs):
        return await self._call(self._sync.delete_event, *args, **kwargs)

    async def list_events(self, *args, **kwargs):
        return await self._call(self._sync.list_events, *args, **kwargs)

    async def list_instances(self, *args, **kwargs):
        return await self._call(self._sync.list_instances, *args, **kwargs)

    async def list_calendar_list(self, *args, **kwargs):
        return await self._call(self._sync.list_calendar_list, *args, **kwargs)


def as_async_google(
    client: Any, limiter: Optional[_AsyncRateLimiter] = None,
) -> AsyncGoogleClient:
    """Wrap a synchronous ``GoogleClient`` as an :class:`AsyncGoogleClient`.

    Idempotent: an already-async client is returned unchanged (limiter
    intact), so an entry point can defensively wrap its ``google``
    argument without caring whether a caller already paced it.
    """
    if isinstance(client, AsyncGoogleClient):
        return client
    return AsyncGoogleClient(client, limiter=limiter)
