"""Personal calendars are read-only sources.

Legacy versions could create origin writeback projections/outbox rows
targeting a personal calendar.  Those rows must converge as no-ops
after the fix: never create/update, never ``events.patch``, and never
source-delete.
"""

from __future__ import annotations

import json

import pytest

from app.ledger.diff import diff_and_enqueue_for_user
from app.ledger.outbox import drain_user
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _legacy_personal_writeback_row(
    s: Scenario,
    *,
    op: str | None,
    payload: dict | None = None,
):
    s.given_calendar("main")
    s.given_calendar("personal_a")
    user = await s.given_user("alice", main="main", personals=["personal_a"])
    s.given_event(
        "personal_a",
        summary="Private appointment",
        start="2026-02-02T09:00:00Z",
        event_id="personalleg01",
    )
    db = await s.setup_db()
    personal_id = user.personal_calendar_ids["personal_a"]
    led = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               source_event_id, status, version, summary, start_at, end_at,
               origin_writeback_pending, source_delete_pending,
               created_at, updated_at)
           VALUES (?, 'personal:legacy:1', 'personal', ?, ?,
                   'active', 1, 'Private appointment',
                   '2026-02-02T09:00:00Z', '2026-02-02T09:30:00Z',
                   1, 1, '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user.user_id, personal_id, "personalleg01"),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash, desired_ledger_version,
               current_state)
           VALUES (?, 'client', ?, 'present_full_rsvp_only',
                   'legacy-hash', 1, 'present')
           RETURNING id""",
        (int(led["id"]), personal_id),
    )).fetchone()
    if op is not None:
        await db.execute(
            """INSERT INTO outbox_operations
                  (user_id, projection_id, operation, idempotency_key,
                   ledger_version_at_enqueue, desired_payload_hash,
                   target_google_calendar_id, payload_json)
               VALUES (?, ?, ?, ?, 1, 'legacy-hash', ?, ?)""",
            (
                user.user_id,
                int(proj["id"]),
                op,
                f"legacy:{op}",
                s.cal("personal_a"),
                json.dumps(payload, sort_keys=True) if payload is not None else None,
            ),
        )
    await db.commit()
    return user, int(led["id"]), int(proj["id"]), personal_id


async def test_legacy_personal_projection_does_not_enqueue_work():
    s = Scenario()
    user, ledger_id, projection_id, personal_id = await _legacy_personal_writeback_row(
        s,
        op=None,
    )
    db = await s.setup_db()

    enqueued = await diff_and_enqueue_for_user(
        db,
        user_id=user.user_id,
        main_calendar_id=user.main_google_calendar_id,
        google_calendar_id_for={personal_id: s.cal("personal_a")},
    )

    assert enqueued == 0
    pending = await (await db.execute(
        "SELECT COUNT(*) AS n FROM outbox_operations WHERE projection_id = ?",
        (projection_id,),
    )).fetchone()
    assert pending["n"] == 0
    origin = s.google.get_event(s.cal("personal_a"), "personalleg01")
    assert origin["summary"] == "Private appointment"
    await s.close()


async def test_legacy_personal_patch_outbox_row_is_noop():
    s = Scenario()
    user, ledger_id, projection_id, personal_id = await _legacy_personal_writeback_row(
        s,
        op="patch",
        payload={
            "summary": "SHOULD NOT WRITE",
            "start": {"dateTime": "2026-02-02T10:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-02-02T10:30:00Z", "timeZone": "UTC"},
        },
    )
    db = await s.setup_db()

    counters = await drain_user(db, s.google, user_id=user.user_id)

    assert counters["succeeded"] == 1
    origin = s.google.get_event(s.cal("personal_a"), "personalleg01")
    assert origin["summary"] == "Private appointment"
    assert origin["start"]["dateTime"] == "2026-02-02T09:00:00Z"
    led = await (await db.execute(
        "SELECT origin_writeback_pending FROM ledger_events WHERE id = ?",
        (ledger_id,),
    )).fetchone()
    assert not led["origin_writeback_pending"]
    op = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE projection_id = ?",
        (projection_id,),
    )).fetchone()
    assert op["status"] == "done"
    await s.close()


async def test_legacy_personal_delete_source_outbox_row_is_noop():
    s = Scenario()
    user, ledger_id, projection_id, personal_id = await _legacy_personal_writeback_row(
        s,
        op="delete_source",
        payload=None,
    )
    db = await s.setup_db()

    counters = await drain_user(db, s.google, user_id=user.user_id)

    assert counters["succeeded"] == 1
    origin = s.google.get_event(s.cal("personal_a"), "personalleg01")
    assert origin["status"] == "confirmed"
    led = await (await db.execute(
        "SELECT source_delete_pending FROM ledger_events WHERE id = ?",
        (ledger_id,),
    )).fetchone()
    assert not led["source_delete_pending"]
    op = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE projection_id = ?",
        (projection_id,),
    )).fetchone()
    assert op["status"] == "done"
    await s.close()
