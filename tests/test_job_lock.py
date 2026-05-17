"""The job-lock primitive must be exclusive and owner-scoped.

acquire_job_lock claims a row via INSERT ... ON CONFLICT DO NOTHING
so a genuinely-held lock reports via rowcount (returns None) while a
transient DB error raises rather than being silently mistaken for
"held".  release_job_lock is scoped by owner so a job whose lock
expired and was taken over cannot delete the successor's lock.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_acquire_is_exclusive_and_release_is_owner_scoped(test_db):
    from app.jobs.sync_job import acquire_job_lock, release_job_lock

    # First acquire wins and is handed a unique owner token.
    owner_a = await acquire_job_lock("jobx")
    assert owner_a

    # A second acquire while the lock is held returns None.
    assert await acquire_job_lock("jobx") is None

    # A release with the WRONG owner must not free the lock.
    await release_job_lock("jobx", "not-the-owner")
    assert await acquire_job_lock("jobx") is None

    # A release with the correct owner frees it for the next claimant.
    await release_job_lock("jobx", owner_a)
    owner_b = await acquire_job_lock("jobx")
    assert owner_b and owner_b != owner_a
    await release_job_lock("jobx", owner_b)


async def test_stale_lock_is_reclaimed(test_db):
    from app.jobs.sync_job import acquire_job_lock, release_job_lock

    # A lock older than the timeout window is treated as abandoned and
    # reclaimed by the next acquirer.
    owner_a = await acquire_job_lock("joby")
    assert owner_a
    owner_b = await acquire_job_lock("joby", timeout_minutes=0)
    assert owner_b and owner_b != owner_a
    await release_job_lock("joby", owner_b)
