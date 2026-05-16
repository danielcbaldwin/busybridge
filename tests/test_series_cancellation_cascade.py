"""Series cancellation cascades to modified-instance rows (REWRITE_PLAN.md §6).

A modified instance of a recurring series is a separate ledger row
(``parent_canonical_uid`` set) with its own ``status``.  When the
parent series becomes inactive — cancelled, or the user deleted the
synced copy (``user_intentionally_deleted``) — the instance row keeps
its own ``status=active``, so the planner must still drive the
instance's projections to ``absent``.  Otherwise the modified
occurrence lingers on every target as a ghost.

This is exercised directly at the planner: re-planning the parent
must cascade to its instance children, and an instance whose parent
is inactive must resolve to ``absent``.
"""

from __future__ import annotations

import pytest

from app.ledger.planner import plan_for_ledger_event
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _insert(db, sql, params):
    return int((await (await db.execute(sql + " RETURNING id", params))
                .fetchone())["id"])


async def test_series_cancellation_cascades_to_modified_instances():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    parent_canon = "client:1:weeklyseries"
    parent_id = await _insert(
        db,
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               source_event_id, is_recurring, status, version,
               start_at, end_at, recurrence_rule_json,
               created_at, updated_at)
           VALUES (?, ?, 'client', 1, 'weeklyseries', 1, 'active', 1,
                   '2026-02-02T09:00:00Z', '2026-02-02T09:30:00Z',
                   '["RRULE:FREQ=WEEKLY;COUNT=6"]',
                   '2026-01-01', '2026-01-01')""",
        (uid, parent_canon),
    )
    inst_id = await _insert(
        db,
        """INSERT INTO ledger_events
              (user_id, canonical_uid, parent_canonical_uid, source_type,
               source_calendar_id, source_event_id,
               recurrence_instance_original_start,
               is_recurring, status, version,
               start_at, end_at, created_at, updated_at)
           VALUES (?, ?, ?, 'client', 1, 'weeklyseries',
                   '2026-02-09T09:00:00Z', 0, 'active', 1,
                   '2026-02-09T11:00:00Z', '2026-02-09T11:30:00Z',
                   '2026-01-01', '2026-01-01')""",
        (uid, f"{parent_canon}:inst:2026-02-09T09:00:00Z", parent_canon),
    )
    await db.commit()

    # While the parent is active the modified instance has live
    # projections.
    await plan_for_ledger_event(db, ledger_event_id=inst_id)
    await db.commit()
    before = await (await db.execute(
        "SELECT desired_state FROM ledger_projections WHERE ledger_event_id = ?",
        (inst_id,),
    )).fetchall()
    assert before, "modified instance had no projections"
    assert any(p["desired_state"] != "absent" for p in before)

    # The user deletes the synced series copy from main → the parent
    # series row is flagged user_intentionally_deleted.
    await db.execute(
        "UPDATE ledger_events SET user_intentionally_deleted = 1 WHERE id = ?",
        (parent_id,),
    )
    await db.commit()

    # Re-planning the PARENT must cascade to its modified-instance
    # child, and the child must resolve to absent.
    await plan_for_ledger_event(db, ledger_event_id=parent_id)
    await db.commit()

    after = await (await db.execute(
        "SELECT desired_state FROM ledger_projections WHERE ledger_event_id = ?",
        (inst_id,),
    )).fetchall()
    assert after
    assert all(p["desired_state"] == "absent" for p in after), (
        "parent cancellation did not cascade to the modified instance: "
        f"{[p['desired_state'] for p in after]}"
    )
    await s.close()
