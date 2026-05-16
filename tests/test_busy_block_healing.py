"""A user-deleted managed event must be recreated, not poison-pilled.

If a user deletes one of BusyBridge's managed events (a main copy or a
client busy block), a later source change drives an ``events.update``
that 404s.  404 is a permanent-failure status, so the op would
poison-pill — leaving the event permanently missing.  Instead the
outbox resets the projection so the next diff re-CREATEs it.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.ledger.outbox import drain_user
from tests.fakes.google_calendar import FakeGoogleCalendar
from tests.integration.framework import Scenario

UTC = timezone.utc
pytestmark = pytest.mark.asyncio


async def test_update_404_resets_projection_for_recreate():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    fake = FakeGoogleCalendar()
    fake.add_calendar("main@cal", "Main")

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               summary, created_at, updated_at)
           VALUES (?, 'client:1:x', 'client', 'active', 1,
                   'Meeting', '2026-01-01', '2026-01-01') RETURNING id""",
        (user.user_id,),
    )).fetchone()
    # The projection believes its managed event is live on Google,
    # but the user has deleted it — 'ghost' no longer exists.
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version,
               applied_ledger_version, current_state,
               google_event_id, google_etag)
           VALUES (?, 'main', 'present_full', 'h', 1, 1, 'present',
                   'ghost', 'etag-1') RETURNING id""",
        (int(ev["id"]),),
    )).fetchone()
    await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               payload_json, status, attempts)
           VALUES (?, ?, 'update', 'k1', 1, 'main@cal',
                   '{"summary": "Meeting"}', 'pending', 0)""",
        (user.user_id, int(proj["id"])),
    )
    await db.commit()

    # The update hits a deleted event → 404.
    counters = await drain_user(
        db, fake, user_id=user.user_id, now=datetime.now(UTC),
    )
    assert counters["superseded"] == 1, counters

    row = await (await db.execute(
        """SELECT current_state, google_event_id, applied_ledger_version
             FROM ledger_projections WHERE id = ?""",
        (int(proj["id"]),),
    )).fetchone()
    # Reset so the diff re-CREATEs it — not poison-pilled.
    assert row["current_state"] == "absent"
    assert row["google_event_id"] is None
    assert row["applied_ledger_version"] is None
    await s.close()
