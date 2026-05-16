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
from typing import Any


class AsyncGoogleClient:
    """Awaitable adapter over a synchronous ``GoogleClient``.

    Every call is run via :func:`asyncio.to_thread`, so a slow Google
    request occupies a worker thread instead of blocking the event
    loop.  The in-memory fake is fast, but routing it through a thread
    too keeps a single uniform code path (calls within one reconcile
    are awaited one at a time, so the fake is never touched
    concurrently).
    """

    __slots__ = ("_sync",)

    def __init__(self, sync_client: Any):
        self._sync = sync_client

    async def insert_event(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.insert_event, *args, **kwargs)

    async def get_event(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.get_event, *args, **kwargs)

    async def update_event(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.update_event, *args, **kwargs)

    async def patch_event(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.patch_event, *args, **kwargs)

    async def delete_event(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.delete_event, *args, **kwargs)

    async def list_events(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.list_events, *args, **kwargs)

    async def list_instances(self, *args, **kwargs):
        return await asyncio.to_thread(self._sync.list_instances, *args, **kwargs)

    async def list_calendar_list(self, *args, **kwargs):
        return await asyncio.to_thread(
            self._sync.list_calendar_list, *args, **kwargs,
        )


def as_async_google(client: Any) -> AsyncGoogleClient:
    """Wrap a synchronous ``GoogleClient`` as an :class:`AsyncGoogleClient`.

    Idempotent: an already-async client is returned unchanged, so an
    entry point can defensively wrap its ``google`` argument without
    caring whether a caller already did.
    """
    if isinstance(client, AsyncGoogleClient):
        return client
    return AsyncGoogleClient(client)
