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
