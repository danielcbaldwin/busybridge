"""A poison-pilled projection must be recoverable.

When an outbox op exhausts its retries it is marked
``permanently_failed = 1``; the diff step then excludes it forever
(``app/ledger/diff.py`` — ``AND p.permanently_failed = 0``).  Without
a way back in, a single bad payload freezes that event for good — even
after the user edits it to something Google would accept.

Two recovery paths are pinned here:

* automatic — re-planning the event with a genuinely changed payload
  clears the flag (the failed payload is now moot);
* manual — ``admin_ops.retry_permanent_failures`` un-sticks projections
  on demand.

A re-plan that does NOT change the payload must leave the flag set:
retrying the identical failing write would just churn.
"""

from __future__ import annotations

import pytest

from app.ledger import admin_ops
from app.ledger.planner import plan_for_ledger_event
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _insert_event(db, user_id, *, summary, version=1):
    row = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               source_event_id, is_recurring, status, version,
               summary, start_at, end_at, created_at, updated_at)
           VALUES (?, 'client:1:pp', 'client', NULL, 'pp', 0, 'active', ?,
                   ?, '2026-03-01T09:00:00Z', '2026-03-01T09:30:00Z',
                   '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user_id, version, summary),
    )).fetchone()
    return int(row["id"])


async def _poison_all_projections(db, ledger_event_id):
    await db.execute(
        """UPDATE ledger_projections
              SET permanently_failed = 1, last_error = 'boom'
            WHERE ledger_event_id = ?""",
        (ledger_event_id,),
    )
    await db.commit()


async def _flags(db, ledger_event_id):
    rows = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections "
        "WHERE ledger_event_id = ?",
        (ledger_event_id,),
    )).fetchall()
    return [int(r["permanently_failed"]) for r in rows]


async def test_unchanged_replan_keeps_the_poison_pill_flag():
    """Re-planning with an identical payload must NOT un-stick a
    poison-pilled projection — that would retry the same bad write."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await _insert_event(db, user.user_id, summary="Standup")
    await plan_for_ledger_event(db, ledger_event_id=ev)
    await db.commit()
    await _poison_all_projections(db, ev)

    # Same event, same payload — re-plan changes nothing.
    await plan_for_ledger_event(db, ledger_event_id=ev)
    await db.commit()

    assert _flags_all(await _flags(db, ev), 1), \
        "unchanged re-plan must leave the projection poison-pilled"
    await s.close()


async def test_changed_replan_clears_the_poison_pill_flag():
    """A genuine edit (new payload hash) un-sticks the projection."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await _insert_event(db, user.user_id, summary="Standup")
    await plan_for_ledger_event(db, ledger_event_id=ev)
    await db.commit()
    await _poison_all_projections(db, ev)

    # The user edits the event — new summary, bumped version.
    await db.execute(
        "UPDATE ledger_events SET summary = ?, version = 2 WHERE id = ?",
        ("Standup (rescheduled)", ev),
    )
    await db.commit()
    await plan_for_ledger_event(db, ledger_event_id=ev)
    await db.commit()

    assert _flags_all(await _flags(db, ev), 0), \
        "a changed payload must clear permanently_failed"
    failures = await (await db.execute(
        "SELECT last_error FROM ledger_projections WHERE ledger_event_id = ?",
        (ev,),
    )).fetchall()
    assert all(f["last_error"] is None for f in failures), \
        "stale last_error left behind after recovery"
    await s.close()


async def test_admin_retry_unsticks_permanent_failures():
    """admin_ops.retry_permanent_failures clears the flag and queues a
    reconcile so the diff re-enqueues the op."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await _insert_event(db, user.user_id, summary="Standup")
    await plan_for_ledger_event(db, ledger_event_id=ev)
    await db.commit()
    await _poison_all_projections(db, ev)

    n = await admin_ops.retry_permanent_failures(db, user_id=user.user_id)
    assert n >= 1, "retry reported no projections un-stuck"
    assert _flags_all(await _flags(db, ev), 0)

    # The affected ledger row was queued for the reconciler.
    req = await (await db.execute(
        "SELECT user_id FROM reconcile_requests WHERE user_id = ?",
        (user.user_id,),
    )).fetchone()
    assert req is not None, "retry did not enqueue a reconcile request"

    # A second call is a no-op — nothing left poison-pilled.
    assert await admin_ops.retry_permanent_failures(
        db, user_id=user.user_id,
    ) == 0
    await s.close()


def _flags_all(flags, expected):
    return bool(flags) and all(f == expected for f in flags)
