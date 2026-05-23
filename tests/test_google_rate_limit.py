"""Outbound Google API rate limiter (paces bulk drains / full syncs).

The outbox drain and full-sync ingest can fan out hundreds of Google
calls at once; without pacing they blow past Google's per-user quota and
trigger a storm of ``rateLimitExceeded`` 403s.  The token bucket caps
the sustained rate.  The in-memory fake is wrapped WITHOUT a limiter, so
the test suite is never throttled — verified here too.
"""

from __future__ import annotations

import time

import pytest

from app.ledger.async_google import (
    AsyncGoogleClient,
    _AsyncRateLimiter,
    as_async_google,
)

pytestmark = pytest.mark.asyncio


async def test_limiter_paces_a_burst():
    # rate 50/s, burst 1 → the first call is free, each later one waits
    # ~20ms.  Six calls => >= ~0.1s of enforced spacing.
    rl = _AsyncRateLimiter(rate_per_sec=50, burst=1)
    start = time.monotonic()
    for _ in range(6):
        await rl.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.09, f"limiter did not pace the burst: {elapsed:.3f}s"


async def test_limiter_allows_initial_burst():
    # A full bucket lets the burst through instantly, then paces.
    rl = _AsyncRateLimiter(rate_per_sec=50, burst=5)
    start = time.monotonic()
    for _ in range(5):
        await rl.acquire()
    assert time.monotonic() - start < 0.05, "initial burst should be instant"


async def test_limiter_disabled_is_a_noop():
    rl = _AsyncRateLimiter(rate_per_sec=0)
    start = time.monotonic()
    for _ in range(200):
        await rl.acquire()
    assert time.monotonic() - start < 0.05, "rate<=0 must not pace"


async def test_fake_path_is_not_throttled():
    """A client wrapped without a limiter (the test/fake path) issues
    calls with no pacing — so the suite never slows down."""
    class _Fake:
        def insert_event(self, *a, **k):
            return {"ok": True}

    c = as_async_google(_Fake())  # no limiter
    assert c._limiter is None
    start = time.monotonic()
    for _ in range(100):
        await c.insert_event("cal", {})
    assert time.monotonic() - start < 0.5


async def test_as_async_google_idempotent_preserves_limiter():
    class _Fake:
        def insert_event(self, *a, **k):
            return None

    rl = _AsyncRateLimiter(rate_per_sec=50, burst=1)
    c = as_async_google(_Fake(), limiter=rl)
    # Re-wrapping (defensive downstream calls) returns the same paced
    # client rather than dropping the limiter.
    assert as_async_google(c) is c
    assert isinstance(c, AsyncGoogleClient)
    assert c._limiter is rl
