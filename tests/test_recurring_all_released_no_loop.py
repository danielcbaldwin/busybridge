"""A finite series whose every occurrence is planned absent must itself
be planned absent — even when the occurrences are RELEASED (retention)
rather than cancelled.

Regression (production, 2026-11): nine finite recurring series whose
remaining occurrences had all been released by retention (or planned
absent for another reason) stayed ``present`` on a client calendar.
The outbox mirrored the series, then deleted each absent occurrence;
deleting the last one made Google cancel the whole series; ingest read
that as "user deleted our busy block" and reset the projection; the
diff recreated it under a fresh id.  Every reconcile, for weeks
(``google_id_generation`` ~27000), with wrong blocks flickering on the
calendar.

``_recurring_parent_has_no_live_occurrences`` only counted cancelled /
user-deleted children as dead.  The fake Google does not model
"deleting the last occurrence cancels the series", so the end-to-end
suite cannot see the loop; this pins the planner decision directly,
using the production row shapes.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.planner import _recurring_parent_has_no_live_occurrences

pytestmark = pytest.mark.asyncio

PARENT_UID = "personal:4:series_R20210830T210000"
RRULE = '["RRULE:FREQ=WEEKLY;UNTIL=20210920T065959Z"]'
OCCURRENCES = (
    "2021-08-30T15:00:00-06:00",
    "2021-09-06T15:00:00-06:00",
    "2021-09-13T15:00:00-06:00",
)


async def _seed(db, child_states: list[tuple[str, str]]) -> object:
    """Seed the parent plus one child per occurrence.

    ``child_states`` is ``[(status, projection desired_state)]`` per
    occurrence; desired_state ``None`` seeds a child with no projection.
    """
    await db.execute(
        "INSERT INTO users (id, email, google_user_id) VALUES (1, 'u@x', 'g')",
    )
    await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, start_at,
               start_timezone, is_all_day, is_recurring,
               recurrence_rule_json, status)
           VALUES (1, ?, 'personal', ?, 'America/Los_Angeles', 0, 1, ?,
                   'active')""",
        (PARENT_UID, OCCURRENCES[0], RRULE),
    )
    for orig, (status, desired) in zip(OCCURRENCES, child_states):
        cur = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid,
                   source_type, start_at, is_all_day, is_recurring,
                   recurrence_instance_original_start, status)
               VALUES (1, ?, ?, 'personal', ?, 0, 0, ?, ?)""",
            (f"{PARENT_UID}:inst:{orig}", PARENT_UID, orig, orig, status),
        )
        if desired is not None:
            await db.execute(
                """INSERT INTO ledger_projections
                      (ledger_event_id, target_kind, target_calendar_id,
                       desired_state, desired_ledger_version)
                   VALUES (?, 'client', 2, ?, 1)""",
                (cur.lastrowid, desired),
            )
    await db.commit()
    return await (await db.execute(
        "SELECT * FROM ledger_events WHERE canonical_uid = ?", (PARENT_UID,),
    )).fetchone()


async def test_all_released_or_cancelled_occurrences_mean_no_live_occurrences(
    test_db,
):
    db = await get_database()
    parent = await _seed(db, [
        ("released", "absent"),
        ("cancelled", "absent"),
        ("released", "absent"),
    ])
    assert await _recurring_parent_has_no_live_occurrences(db, parent)


async def test_active_child_planned_absent_counts_as_dead(test_db):
    """The live 'Morning Support Standup' shape: an active occurrence
    planned absent (e.g. declined) is deleted from the mirror just the
    same."""
    db = await get_database()
    parent = await _seed(db, [
        ("active", "absent"),
        ("cancelled", "absent"),
        ("cancelled", "absent"),
    ])
    assert await _recurring_parent_has_no_live_occurrences(db, parent)


async def test_one_present_child_keeps_the_series_live(test_db):
    db = await get_database()
    parent = await _seed(db, [
        ("released", "absent"),
        ("released", "present_personal_busy"),
        ("cancelled", "absent"),
    ])
    assert not await _recurring_parent_has_no_live_occurrences(db, parent)


async def test_child_without_projections_is_not_presumed_dead(test_db):
    """No projection rows yet (never planned) says nothing about the
    occurrence — stay conservative and keep the series."""
    db = await get_database()
    parent = await _seed(db, [
        ("released", "absent"),
        ("active", None),
        ("cancelled", "absent"),
    ])
    assert not await _recurring_parent_has_no_live_occurrences(db, parent)


async def test_origin_rsvp_writeback_does_not_keep_the_mirror_live(test_db):
    """The 'Doug - Daniel' shape: a client-sourced occurrence absent on
    the peer but carrying an origin RSVP-writeback projection on its
    own source calendar.  The writeback patches the source in place and
    puts nothing on a mirror, so the occurrence is still dead to the
    mirrored series."""
    from app.ledger.payload import PRESENT_FULL_RSVP_ONLY

    db = await get_database()
    parent = await _seed(db, [
        ("cancelled", "absent"),
        ("released", "absent"),
        ("active", "absent"),
    ])
    child = await (await db.execute(
        """SELECT id FROM ledger_events
            WHERE parent_canonical_uid = ? AND status = 'active'""",
        (PARENT_UID,),
    )).fetchone()
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_ledger_version)
           VALUES (?, 'client', 5, ?, 1)""",
        (child["id"], PRESENT_FULL_RSVP_ONLY),
    )
    await db.commit()
    assert await _recurring_parent_has_no_live_occurrences(db, parent)
