"""The global 'pause everything' emergency stop must halt the OUTBOX
DRAIN — the one place that writes to Google — not just ingest/enqueue.

reconcile_user already returns early on a global pause before draining,
but the drain enforces it directly too (defence in depth): no caller can
bypass the kill switch, and flipping it stops queued ops at the next
drain instead of letting the backlog flush. Per-user soft pauses are NOT
affected — those keep draining staged cleanup.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app.database import get_database
from app.ledger.outbox import drain_user
from tests.fakes.google_calendar import FakeGoogleCalendar

UTC = timezone.utc
CAL = "calxample"  # base32hex-safe-ish; FakeGoogleCalendar accepts any id here


async def _seed_pending_create(db):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_events (user_id, canonical_uid, source_type) "
        "VALUES (?, 'uid-1', 'client')", (uid,))
    le = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, desired_state, desired_ledger_version, "
        " current_state) VALUES (?, 'client', 'present', 1, 'absent')", (le,))
    proj = int(cur.lastrowid)
    body = {"summary": "Busy",
            "start": {"dateTime": "2026-07-10T09:00:00Z"},
            "end": {"dateTime": "2026-07-10T10:00:00Z"}}
    cur = await db.execute(
        "INSERT INTO outbox_operations "
        "(user_id, projection_id, operation, idempotency_key, "
        " ledger_version_at_enqueue, target_google_calendar_id, payload_json, "
        " status, next_attempt_at) "
        "VALUES (?, ?, 'create', 'idem-1', 1, ?, ?, 'pending', "
        " '2026-01-01T00:00:00')",
        (uid, proj, CAL, json.dumps(body)))
    op_id = int(cur.lastrowid)
    await db.commit()
    return uid, op_id


async def _set_global_pause(db, on: bool):
    await db.execute(
        "INSERT INTO settings (key, value_plain) VALUES ('sync_paused', ?) "
        "ON CONFLICT(key) DO UPDATE SET value_plain = excluded.value_plain",
        ("true" if on else "false",))
    await db.commit()


def _google():
    g = FakeGoogleCalendar()
    g.add_calendar(CAL)
    return g


async def _status(db, op_id):
    return (await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id=?", (op_id,))
    ).fetchone())["status"]


NOW = datetime(2026, 7, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_global_pause_stops_the_drain(test_db):
    db = await get_database()
    uid, op_id = await _seed_pending_create(db)
    await _set_global_pause(db, True)

    counters = await drain_user(db, _google(), user_id=uid, now=NOW)

    assert counters["processed"] == 0, "drain must not run under global pause"
    assert await _status(db, op_id) == "pending", "op must be left untouched"


@pytest.mark.asyncio
async def test_drain_runs_when_not_paused(test_db):
    # Control: same op, no global pause → it drains. Proves the pause is
    # what stopped it above, not some other condition.
    db = await get_database()
    uid, op_id = await _seed_pending_create(db)
    await _set_global_pause(db, False)

    counters = await drain_user(db, _google(), user_id=uid, now=NOW)

    assert counters["processed"] == 1
    assert await _status(db, op_id) == "done"
