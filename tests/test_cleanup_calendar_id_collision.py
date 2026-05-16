"""cleanup_one_calendar must not cancel unrelated webcal events.

Client/personal calendars (``client_calendars``) and webcal
subscriptions (``webcal_subscriptions``) are numbered in separate
tables, so a webcal subscription id can numerically equal a client
calendar id.  ``cleanup_one_calendar`` cancels ledger events by
``source_calendar_id``; without a ``source_type`` filter it would
also cancel a webcal event that merely shares the numeric id.
"""

from __future__ import annotations

import pytest

from app.ledger.admin_ops import cleanup_one_calendar
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _insert_event(db, user_id, *, canonical_uid, source_type,
                        source_calendar_id):
    await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               status, version, created_at, updated_at)
           VALUES (?, ?, ?, ?, 'active', 1, '2026-01-01', '2026-01-01')""",
        (user_id, canonical_uid, source_type, source_calendar_id),
    )


async def _status(db, canonical_uid):
    row = await (await db.execute(
        "SELECT status FROM ledger_events WHERE canonical_uid = ?",
        (canonical_uid,),
    )).fetchone()
    return row["status"]


async def test_cleanup_calendar_spares_webcal_event_with_colliding_id():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("work")
    user = await s.given_user("alice", main="main", clients=["work"])
    db = await s.setup_db()
    work_id = user.client_calendar_ids["work"]

    # An event genuinely sourced from the work client calendar.
    await _insert_event(
        db, user.user_id,
        canonical_uid="client:work:ev1",
        source_type="client",
        source_calendar_id=work_id,
    )
    # A webcal event whose source_calendar_id (a webcal_subscriptions
    # id, separate table) numerically collides with work_id.
    await _insert_event(
        db, user.user_id,
        canonical_uid="webcal:sub:ev1",
        source_type="webcal",
        source_calendar_id=work_id,
    )
    await db.commit()

    await cleanup_one_calendar(
        db, user_id=user.user_id, client_calendar_id=work_id,
    )

    assert await _status(db, "client:work:ev1") == "cancelled"
    assert await _status(db, "webcal:sub:ev1") == "active", (
        "cleanup cancelled an unrelated webcal event via an id collision"
    )
    await s.close()
