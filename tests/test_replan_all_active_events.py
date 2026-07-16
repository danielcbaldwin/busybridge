"""``admin_ops.replan_all_active_events`` queues every active
non-user-deleted source event into ``affected_ledger_events`` so the
next reconcile plans them against the CURRENT active-client-calendars
set.  Needed after a calendar connect/disconnect: without it, events
whose content hasn't changed keep their old projection set and the
new target receives no busy blocks (and the disconnected one keeps
stale ones).
"""

from __future__ import annotations

import pytest

from app.ledger import admin_ops


async def _add_user(db, email: str = "u@example.com") -> int:
    row = await (await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES (?, ?, 'U') RETURNING id",
        (email, "g-" + email),
    )).fetchone()
    await db.commit()
    return int(row["id"])


async def _add_event(
    db, *, user_id: int, canonical: str, status: str = "active",
    intentionally_deleted: int = 0,
) -> int:
    row = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type,
               status, user_intentionally_deleted, version)
           VALUES (?, ?, 'client', ?, ?, 1)
           RETURNING id""",
        (user_id, canonical, status, intentionally_deleted),
    )).fetchone()
    await db.commit()
    return int(row["id"])


@pytest.mark.asyncio
async def test_queues_every_active_event(test_db):
    user_id = await _add_user(test_db)
    active_ids = [
        await _add_event(test_db, user_id=user_id, canonical=f"a{i}")
        for i in range(3)
    ]
    # noise: cancelled + intentionally-deleted should be skipped
    await _add_event(test_db, user_id=user_id, canonical="cancelled",
                     status="cancelled")
    await _add_event(test_db, user_id=user_id, canonical="deleted",
                     intentionally_deleted=1)

    queued = await admin_ops.replan_all_active_events(
        test_db, user_id=user_id,
    )
    assert queued == 3

    rows = await (await test_db.execute(
        "SELECT ledger_event_id FROM affected_ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    assert sorted(int(r["ledger_event_id"]) for r in rows) == sorted(active_ids)


@pytest.mark.asyncio
async def test_no_active_events_returns_zero(test_db):
    user_id = await _add_user(test_db, email="empty@example.com")
    queued = await admin_ops.replan_all_active_events(
        test_db, user_id=user_id,
    )
    assert queued == 0


@pytest.mark.asyncio
async def test_idempotent_across_calls(test_db):
    """Two consecutive calls just enqueue duplicate rows — the
    reconciler dedupes by distinct ledger_event_id when it drains
    the queue, so a double-call is safe."""
    user_id = await _add_user(test_db, email="idem@example.com")
    await _add_event(test_db, user_id=user_id, canonical="one")

    first = await admin_ops.replan_all_active_events(test_db, user_id=user_id)
    second = await admin_ops.replan_all_active_events(test_db, user_id=user_id)
    assert first == 1
    assert second == 1

    rows = await (await test_db.execute(
        "SELECT COUNT(*) c FROM affected_ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    assert int(rows["c"]) == 2
