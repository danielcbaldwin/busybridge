"""Recurring parents with only cancelled occurrences must not self-loop."""

from __future__ import annotations

import pytest

from app.ledger.diff import diff_and_enqueue_for_user
from app.ledger.planner import plan_for_ledger_event
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _insert(db, sql, params):
    return int((await (await db.execute(sql + " RETURNING id", params))
                .fetchone())["id"])


async def _one_occurrence_main_native_parent(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    db = await s.setup_db()
    parent_uid = "main_native:1:pathological_R20260601T160000"
    parent_id = await _insert(
        db,
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_event_id,
               is_recurring, status, version,
               summary, start_at, end_at, start_timezone, end_timezone,
               recurrence_rule_json, created_at, updated_at)
           VALUES (?, ?, 'main_native', 'pathological_R20260601T160000',
                   1, 'active', 1,
                   'MLC AIRR - Main Weekly Meeting',
                   '2026-06-01T18:00:00+02:00',
                   '2026-06-01T18:30:00+02:00',
                   'Europe/Brussels', 'Europe/Brussels',
                   '["RRULE:FREQ=WEEKLY;UNTIL=20260608T065959Z;BYDAY=MO"]',
                   '2026-05-29', '2026-05-29')""",
        (user.user_id, parent_uid),
    )
    child_id = await _insert(
        db,
        """INSERT INTO ledger_events
              (user_id, canonical_uid, parent_canonical_uid, source_type,
               source_event_id, recurrence_instance_original_start,
               is_recurring, status, version, created_at, updated_at)
           VALUES (?, ?, ?, 'main_native',
                   'pathological_20260601T160000Z',
                   '2026-06-01T18:00:00+02:00',
                   0, 'cancelled', 1, '2026-05-29', '2026-05-29')""",
        (
            user.user_id,
            f"{parent_uid}:inst:2026-06-01T16:00:00Z",
            parent_uid,
        ),
    )
    await db.commit()
    return user, parent_id, child_id


async def test_parent_with_only_cancelled_rrule_occurrence_is_absent():
    s = Scenario()
    user, parent_id, child_id = await _one_occurrence_main_native_parent(s)
    db = await s.setup_db()

    await plan_for_ledger_event(db, ledger_event_id=parent_id)
    await db.commit()

    parent = await (await db.execute(
        """SELECT target_kind, target_calendar_id, desired_state
             FROM ledger_projections
            WHERE ledger_event_id = ?
            ORDER BY target_kind, target_calendar_id""",
        (parent_id,),
    )).fetchall()
    assert parent
    assert all(row["desired_state"] == "absent" for row in parent)

    child = await (await db.execute(
        """SELECT desired_state
             FROM ledger_projections
            WHERE ledger_event_id = ?""",
        (child_id,),
    )).fetchall()
    assert child
    assert all(row["desired_state"] == "absent" for row in child)
    await s.close()


async def test_absent_parent_suppresses_redundant_child_instance_delete():
    s = Scenario()
    user, parent_id, child_id = await _one_occurrence_main_native_parent(s)
    db = await s.setup_db()

    await plan_for_ledger_event(db, ledger_event_id=parent_id)
    await db.commit()

    # Simulate the live loop state: the recurring parent exists on
    # client calendars, while the cancelled child has not yet been
    # derived/applied for this new parent Google ID.
    for nick, client_id in user.client_calendar_ids.items():
        await db.execute(
            """UPDATE ledger_projections
                  SET current_state = 'present',
                      google_event_id = ?,
                      applied_ledger_version = NULL,
                      applied_payload_hash = NULL
                WHERE ledger_event_id = ?
                  AND target_kind = 'client'
                  AND target_calendar_id = ?""",
            (f"bbparent{client_id:04d}", parent_id, client_id),
        )
    await db.commit()

    await diff_and_enqueue_for_user(
        db,
        user_id=user.user_id,
        main_calendar_id=s.cal("main"),
        google_calendar_id_for={
            cid: s.cal(nick)
            for nick, cid in user.client_calendar_ids.items()
        },
    )
    await db.commit()

    ops = await (await db.execute(
        """SELECT o.operation, o.projection_id, p.ledger_event_id
             FROM outbox_operations o
             JOIN ledger_projections p ON p.id = o.projection_id
            ORDER BY o.id""",
    )).fetchall()
    assert [(op["operation"], op["ledger_event_id"]) for op in ops] == [
        ("delete", parent_id),
        ("delete", parent_id),
    ]

    child_proj = await (await db.execute(
        """SELECT google_event_id, current_state, applied_payload_hash
             FROM ledger_projections
            WHERE ledger_event_id = ?
              AND target_kind = 'client'
            ORDER BY target_calendar_id""",
        (child_id,),
    )).fetchall()
    assert child_proj
    assert all(row["google_event_id"] is None for row in child_proj)
    assert all(row["current_state"] == "absent" for row in child_proj)
    assert all(row["applied_payload_hash"] == "absent" for row in child_proj)
    await s.close()


async def test_absent_child_snaps_after_absent_parent_loses_google_id():
    s = Scenario()
    user, parent_id, child_id = await _one_occurrence_main_native_parent(s)
    db = await s.setup_db()

    await plan_for_ledger_event(db, ledger_event_id=parent_id)
    await db.commit()

    await diff_and_enqueue_for_user(
        db,
        user_id=user.user_id,
        main_calendar_id=s.cal("main"),
        google_calendar_id_for={
            cid: s.cal(nick)
            for nick, cid in user.client_calendar_ids.items()
        },
    )
    await db.commit()

    ops = await (await db.execute(
        "SELECT COUNT(*) AS n FROM outbox_operations",
    )).fetchone()
    assert ops["n"] == 0

    child_proj = await (await db.execute(
        """SELECT current_state, desired_state, google_event_id,
                  applied_payload_hash, applied_ledger_version
             FROM ledger_projections
            WHERE ledger_event_id = ?
              AND target_kind = 'client'
            ORDER BY target_calendar_id""",
        (child_id,),
    )).fetchall()
    assert child_proj
    assert all(row["current_state"] == "absent" for row in child_proj)
    assert all(row["desired_state"] == "absent" for row in child_proj)
    assert all(row["google_event_id"] is None for row in child_proj)
    assert all(row["applied_payload_hash"] == "absent" for row in child_proj)
    assert all(row["applied_ledger_version"] == 1 for row in child_proj)
    await s.close()
