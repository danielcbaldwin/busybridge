"""All-day personal events are not mirrored anywhere.

An all-day event on a personal (read-only) calendar only blocks out the
whole day — on main and on every client calendar — without conveying any
information, making the user look unavailable all day.  By default
(``SYNC_PERSONAL_ALL_DAY_EVENTS`` off) the planner suppresses them
entirely; timed personal events still cast their opaque busy blocks.

Setting the flag True restores the legacy behaviour.  ``admin_ops.
cleanup_personal_all_day_blocks`` re-plans existing all-day personal
events so their already-written blocks are deleted after the default
flips off.
"""

from __future__ import annotations

from types import SimpleNamespace

from app.ledger.payload import ABSENT, PRESENT_PERSONAL_BUSY
from app.ledger.planner import _compute_desired_projections
from tests.integration.framework import Scenario

# asyncio_mode = auto (pytest.ini) runs async tests without an explicit
# mark; the synchronous pure-function tests below stay synchronous.


# ---------------------------------------------------------------------------
# Pure planner unit tests
# ---------------------------------------------------------------------------
def _personal_ledger(*, is_all_day: bool):
    return {
        "user_intentionally_deleted": 0,
        "status": "active",
        "source_type": "personal",
        "show_as": "busy",
        "is_all_day": 1 if is_all_day else 0,
    }


def test_all_day_personal_suppressed_everywhere_by_default():
    desired = _compute_desired_projections(
        _personal_ledger(is_all_day=True),
        sync_personal_all_day=False,
    )
    assert desired == {
        "main": ABSENT,
        "peer_clients": ABSENT,
        "origin_client": ABSENT,
    }


def test_timed_personal_still_busy_everywhere():
    desired = _compute_desired_projections(
        _personal_ledger(is_all_day=False),
        sync_personal_all_day=False,
    )
    assert desired["main"] == PRESENT_PERSONAL_BUSY
    assert desired["peer_clients"] == PRESENT_PERSONAL_BUSY
    assert desired["origin_client"] == ABSENT


def test_all_day_personal_mirrored_when_flag_on():
    desired = _compute_desired_projections(
        _personal_ledger(is_all_day=True),
        sync_personal_all_day=True,
    )
    assert desired["main"] == PRESENT_PERSONAL_BUSY
    assert desired["peer_clients"] == PRESENT_PERSONAL_BUSY


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------
async def test_all_day_personal_not_mirrored_to_main_or_clients():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    # All-day: a date-only start (no time component).
    s.given_event(
        "personal_a", summary="Vacation", start="2026-02-05",
        event_id="persad0001",
    )

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    s.assert_no_event_with_summary("main", "Busy (personal)")
    s.assert_no_event_with_summary("client_a", "Busy (personal)")
    # The source calendar itself is untouched (read-only origin).
    s.assert_event_exists("personal_a", summary="Vacation")
    await s.close()


async def test_multiday_all_day_personal_not_mirrored():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    # A multi-day all-day event (e.g. a week-long trip).
    body = {
        "summary": "Conference trip",
        "start": {"date": "2026-03-09"},
        "end": {"date": "2026-03-14"},
        "id": "persmd0001",
    }
    s.google.insert_event(s.cal("personal_a"), body)

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    s.assert_no_event_with_summary("main", "Busy (personal)")
    s.assert_no_event_with_summary("client_a", "Busy (personal)")
    await s.close()


async def test_recurring_all_day_personal_not_mirrored():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    # An all-day recurring series (a yearly birthday): both the parent
    # and every generated occurrence carry is_all_day, so the whole
    # series is suppressed.
    s.given_recurring_event(
        "personal_a", summary="Birthday", start="2026-02-05",
        rrule="RRULE:FREQ=YEARLY;COUNT=3", event_id="persrec0001",
    )

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    s.assert_no_event_with_summary("main", "Busy (personal)")
    s.assert_no_event_with_summary("client_a", "Busy (personal)")
    await s.close()


async def test_timed_personal_event_still_mirrored_end_to_end():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    s.given_event(
        "personal_a", summary="Dentist",
        start="2026-02-05T09:00:00Z", event_id="timedpers01",
    )

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    # Opaque busy block on main AND on the (non-origin) client calendar.
    s.assert_event_exists("main", summary="Busy (personal)")
    s.assert_event_exists("client_a", summary="Busy (personal)")
    await s.close()


async def test_legacy_flag_restores_all_day_personal_mirror(monkeypatch):
    monkeypatch.setattr(
        "app.ledger.planner.get_settings",
        lambda: SimpleNamespace(sync_personal_all_day_events=True),
    )
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    s.given_event(
        "personal_a", summary="Vacation", start="2026-02-05",
        event_id="persad0002",
    )

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    s.assert_event_exists("main", summary="Busy (personal)")
    s.assert_event_exists("client_a", summary="Busy (personal)")
    await s.close()


async def test_cleanup_enqueues_and_removes_existing_all_day_blocks(monkeypatch):
    """The cleanup op marks an existing all-day personal event for replan
    so its already-written blocks are deleted from main + clients once the
    flag is off — deterministic immediate cleanup instead of waiting for
    the personal calendar's next full sync."""
    from app.ledger.admin_ops import cleanup_personal_all_day_blocks

    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    user = await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    s.given_event(
        "personal_a", summary="Vacation", start="2026-02-05",
        event_id="persad0003",
    )

    # 1. Legacy behaviour: the block exists on main + client.
    legacy = SimpleNamespace(sync_personal_all_day_events=True)
    monkeypatch.setattr("app.ledger.planner.get_settings", lambda: legacy)
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    s.assert_event_exists("main", summary="Busy (personal)")
    s.assert_event_exists("client_a", summary="Busy (personal)")

    # 2. Flag flips off (new default).  The cleanup op enqueues exactly
    #    the all-day personal ledger event for replan and schedules a
    #    reconcile.
    monkeypatch.setattr(
        "app.ledger.planner.get_settings",
        lambda: SimpleNamespace(sync_personal_all_day_events=False),
    )
    db = await s.setup_db()
    led = await (await db.execute(
        """SELECT id FROM ledger_events
            WHERE source_type = 'personal' AND is_all_day = 1
              AND status = 'active'""",
    )).fetchone()
    enqueued = await cleanup_personal_all_day_blocks(db, user_id=user.user_id)
    assert enqueued == 1
    affected = await (await db.execute(
        "SELECT ledger_event_id FROM affected_ledger_events WHERE user_id = ?",
        (user.user_id,),
    )).fetchall()
    assert int(led["id"]) in {int(r["ledger_event_id"]) for r in affected}

    # 3. The next reconcile deletes the blocks everywhere.
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    s.assert_no_event_with_summary("main", "Busy (personal)")
    s.assert_no_event_with_summary("client_a", "Busy (personal)")
    await s.close()
