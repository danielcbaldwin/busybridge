"""Maintenance mode hard-freezes the sync engine.

While a database restore is in progress the engine must not ingest,
diff, drain, or handle webhooks — the DB file is being swapped out
underneath it.  Maintenance mode is an in-process flag (the DB itself
is what gets restored, so the guard cannot live in the DB).  The
restore's own re-converge pass is the single caller allowed to bypass
it.

Pause modes are also pinned here: a global pause is a HARD freeze
(no drain), a per-user pause is SOFT (drain still runs).
"""

from __future__ import annotations

import asyncio

import pytest

from app.maintenance import enter_maintenance, exit_maintenance, in_maintenance
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def test_reconcile_is_skipped_in_maintenance(test_db):
    from app.ledger.runtime import reconcile_user_by_id

    enter_maintenance()
    try:
        out = await reconcile_user_by_id(99999)
        assert out == {"skipped": "maintenance"}
    finally:
        exit_maintenance()
    assert in_maintenance() is False


async def test_reconcile_bypass_runs_despite_maintenance(test_db):
    """allow_in_maintenance=True must get past the freeze — proven by
    reaching the user lookup, which raises for a missing user."""
    from app.ledger.runtime import reconcile_user_by_id

    enter_maintenance()
    try:
        with pytest.raises(ValueError):
            await reconcile_user_by_id(99999, allow_in_maintenance=True)
    finally:
        exit_maintenance()


async def test_drain_all_due_users_skips_in_maintenance(test_db):
    from app.ledger.runtime import drain_all_due_users

    enter_maintenance()
    try:
        assert await drain_all_due_users() == {}
    finally:
        exit_maintenance()


async def test_pause_mode_distinguishes_global_and_per_user():
    from app.ledger.reconciler import _pause_mode

    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    assert await _pause_mode(db, uid) is None

    await db.execute(
        "UPDATE users SET sync_paused = 1 WHERE id = ?", (uid,),
    )
    await db.commit()
    assert await _pause_mode(db, uid) == "user"

    await db.execute(
        "INSERT INTO settings (key, value_plain) VALUES ('sync_paused', 'true')",
    )
    await db.commit()
    # Global outranks per-user.
    assert await _pause_mode(db, uid) == "global"
    await s.close()


async def test_global_pause_is_a_hard_freeze_but_per_user_drains():
    """A global pause skips the drain entirely (hard freeze); a
    per-user pause still runs the diff+drain loop (soft pause)."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    # Per-user soft pause: the reconciler still runs the drain loop.
    await db.execute(
        "UPDATE users SET sync_paused = 1 WHERE id = ?", (user.user_id,),
    )
    await db.commit()
    soft = await s.run_reconciler("alice")
    assert soft["paused"] is True
    assert soft["drain"], "a per-user pause must still drain the outbox"

    # Global hard freeze: no drain dict is populated at all.
    await db.execute(
        "INSERT INTO settings (key, value_plain) VALUES ('sync_paused', 'true')",
    )
    await db.commit()
    hard = await s.run_reconciler("alice")
    assert hard["paused"] is True
    assert hard["drain"] == {}, "a global pause must not drain"
    await s.close()


async def test_quiescence_returns_immediately_when_no_reconcile_runs():
    from app.maintenance import wait_for_reconcile_quiescence

    await wait_for_reconcile_quiescence(timeout=1.0)


async def test_quiescence_times_out_while_a_reconcile_is_in_flight():
    from app.maintenance import (
        active_reconcile_count,
        track_reconcile,
        wait_for_reconcile_quiescence,
    )

    with track_reconcile():
        assert active_reconcile_count() == 1
        with pytest.raises(TimeoutError):
            await wait_for_reconcile_quiescence(timeout=0.2, poll=0.02)
    # The guard exited — the engine is quiescent again.
    assert active_reconcile_count() == 0
    await wait_for_reconcile_quiescence(timeout=1.0)


async def test_quiescence_drains_a_concurrent_reconcile():
    """A reconcile already running when a restore begins must be
    waited out — quiescence is reached only once it releases."""
    from app.maintenance import track_reconcile, wait_for_reconcile_quiescence

    released = asyncio.Event()

    async def _fake_reconcile():
        with track_reconcile():
            await released.wait()

    task = asyncio.create_task(_fake_reconcile())
    await asyncio.sleep(0.05)  # let it register as in-flight

    # Not quiescent yet — the fake reconcile is still holding its slot.
    with pytest.raises(TimeoutError):
        await wait_for_reconcile_quiescence(timeout=0.15, poll=0.02)

    released.set()
    await task
    # Now it has finished — quiescence is reached.
    await wait_for_reconcile_quiescence(timeout=1.0)
