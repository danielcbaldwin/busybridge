"""Edit-on-main write-back to the source event (REWRITE_PLAN.md §9).

When the user edits one of our managed copies on the main calendar,
:func:`app.ledger.ingest.main._maybe_apply_main_edit_back` classifies
the edit and either propagates it to the calendar that *sourced* the
event or reverts it as drift.

The propagate/revert matrix exercised here:

* client, editable     — time and detail edits propagate.
* client, locked       — time and detail edits revert.
* personal             — main gets only an opaque busy block.  There
                         are no details and no RSVP surface; any drift
                         on that block is reverted, and the source
                         calendar is read-only.

Propagation rides the origin writeback projection — a phantom
projection on the source calendar, delivered as an ``events.patch``
that never creates or deletes the source event.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _locked_client_event(s: Scenario, calendar_nick: str, event_id: str) -> dict:
    """A client event organised by someone else — alice is an
    attendee but not the organizer and there is no guestsCanModify,
    so the event is NOT editable by her."""
    return s.google.insert_event(s.cal(calendar_nick), {
        "id": event_id,
        "summary": "Locked meeting",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "boss@example.com"},
        "attendees": [
            {"email": "boss@example.com", "organizer": True,
             "responseStatus": "accepted"},
            {"email": "alice@example.com", "self": True,
             "responseStatus": "accepted"},
        ],
    })


def _attendee(event: dict, email: str):
    for attendee in event.get("attendees") or []:
        if attendee.get("email") == email:
            return attendee
    return None


async def test_client_time_edit_on_main_propagates_to_source():
    """Dragging an editable client event's main copy moves the real
    source event on the client calendar."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    # No attendees → editable.
    s.given_event(
        "client_a", summary="Project review",
        start="2026-02-02T09:00:00Z", event_id="clienttimeed01",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    main_copy = s.assert_event_exists("main", summary="Project review")
    assert main_copy["start"]["dateTime"] == "2026-02-02T09:00:00Z"

    s.update_event(
        "main", main_copy["id"],
        start="2026-02-02T11:00:00Z", end="2026-02-02T11:30:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    origin = s.google.get_event(s.cal("client_a"), "clienttimeed01")
    assert origin["start"]["dateTime"] == "2026-02-02T11:00:00Z", (
        f"time edit not propagated to source; start={origin.get('start')}"
    )
    await s.close()


async def test_client_detail_edit_on_main_propagates_to_source():
    """Renaming an editable client event's main copy renames the
    real source event."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_event(
        "client_a", summary="Old title",
        start="2026-02-02T09:00:00Z", event_id="clientdetail01",
        location="Room 1",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    main_copy = s.assert_event_exists("main", summary="Old title")
    s.update_event(
        "main", main_copy["id"], summary="New title", location="Room 9",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    origin = s.google.get_event(s.cal("client_a"), "clientdetail01")
    assert origin["summary"] == "New title", (
        f"detail edit not propagated; summary={origin.get('summary')!r}"
    )
    assert origin.get("location") == "Room 9"
    await s.close()


async def test_locked_client_time_edit_on_main_is_reverted():
    """A user may not move an event they do not control: dragging the
    main copy of a locked client event is reverted, and the source
    event is never moved."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    _locked_client_event(s, "client_a", "lockedevent001")
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    main_copy = s.assert_event_exists("main", summary_contains="Locked meeting")
    s.update_event(
        "main", main_copy["id"],
        start="2026-02-02T14:00:00Z", end="2026-02-02T14:30:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # The source event keeps its original time.
    origin = s.google.get_event(s.cal("client_a"), "lockedevent001")
    assert origin["start"]["dateTime"] == "2026-02-02T09:00:00Z", (
        f"a locked event was moved on its source; start={origin.get('start')}"
    )
    # And the main copy is snapped back to the canonical time.
    reverted = s.assert_event_exists("main", summary_contains="Locked meeting")
    assert reverted["start"]["dateTime"] == "2026-02-02T09:00:00Z"
    await s.close()


async def test_personal_time_edit_on_main_is_reverted():
    """Dragging a "Busy (personal)" placeholder on main must never
    patch the real personal source event.  The main copy snaps back to
    the personal source's canonical time."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    await s.given_user("alice", main="main", personals=["personal_a"])
    s.given_event(
        "personal_a", summary="Dentist",
        start="2026-02-02T09:00:00Z", event_id="personaltime01",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    # Main shows an opaque placeholder — no personal detail leaks.
    main_copy = s.assert_event_exists("main", summary="Busy (personal)")
    assert main_copy["start"]["dateTime"] == "2026-02-02T09:00:00Z"

    s.update_event(
        "main", main_copy["id"],
        start="2026-02-02T10:00:00Z", end="2026-02-02T10:30:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    origin = s.google.get_event(s.cal("personal_a"), "personaltime01")
    assert origin["start"]["dateTime"] == "2026-02-02T09:00:00Z", (
        f"personal source was moved by a main placeholder edit; "
        f"start={origin.get('start')}"
    )
    assert origin["summary"] == "Dentist"

    reverted = s.assert_event_exists("main", summary="Busy (personal)")
    assert reverted["start"]["dateTime"] == "2026-02-02T09:00:00Z"
    await s.close()


async def test_personal_busy_block_has_no_rsvp_surface_and_rsvp_drift_reverts():
    """A personal event with attendees still renders as a plain busy
    block on main.  There should be no attendee list to RSVP through;
    if a malformed edit adds one anyway, it is reverted and never
    written back."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    await s.given_user("alice", main="main", personals=["personal_a"])
    s.given_event(
        "personal_a", summary="Dinner",
        start="2026-02-02T09:00:00Z", event_id="personalrsvp01",
        attendees=[
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "sam@example.com", "responseStatus": "accepted"},
        ],
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    main_copy = s.assert_event_exists("main", summary="Busy (personal)")
    assert "attendees" not in main_copy
    assert "location" not in main_copy
    assert "conferenceData" not in main_copy
    assert "Dinner" not in (main_copy.get("description") or "")

    s.update_event(
        "main", main_copy["id"],
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "accepted"}],
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    origin = s.google.get_event(s.cal("personal_a"), "personalrsvp01")
    alice = _attendee(origin, "alice@example.com")
    assert alice is not None and alice["responseStatus"] == "needsAction", (
        f"personal source RSVP was changed by a main placeholder edit; "
        f"attendees={origin.get('attendees')}"
    )
    reverted = s.assert_event_exists("main", summary="Busy (personal)")
    assert reverted.get("attendees") in (None, [])
    await s.close()


async def test_personal_main_copy_delete_is_recreated():
    """Deleting Busy (personal) on main is drift to heal, not a request
    to suppress the personal-source busy block."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    user = await s.given_user("alice", main="main", personals=["personal_a"])
    s.given_event(
        "personal_a", summary="Dentist",
        start="2026-02-02T09:00:00Z", event_id="personaldeleted01",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    main_copy = s.assert_event_exists("main", summary="Busy (personal)")
    s.cancel_event("main", main_copy["id"])
    await s.run_reconciler_until_quiescent("alice", max_passes=8)

    s.assert_event_exists("main", summary="Busy (personal)")
    s.assert_event_exists("personal_a", summary="Dentist")
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT user_intentionally_deleted
             FROM ledger_events
            WHERE user_id = ? AND source_type = 'personal'
              AND source_event_id = ?""",
        (user.user_id, "personaldeleted01"),
    )).fetchone()
    assert row is not None
    assert not row["user_intentionally_deleted"]
    await s.close()


async def test_personal_detail_edit_on_main_is_reverted():
    """Editing the title of a "Busy (personal)" placeholder is a
    placeholder edit, not a real-event edit: it reverts, and the real
    personal event's title is never overwritten."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    await s.given_user("alice", main="main", personals=["personal_a"])
    s.given_event(
        "personal_a", summary="Therapy",
        start="2026-02-02T09:00:00Z", event_id="personaldtl001",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    main_copy = s.assert_event_exists("main", summary="Busy (personal)")
    s.update_event("main", main_copy["id"], summary="Coffee with Sam")
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # The main copy is snapped back to the opaque placeholder.
    s.assert_event_exists("main", summary="Busy (personal)")
    # The real personal event keeps its private title.
    origin = s.google.get_event(s.cal("personal_a"), "personaldtl001")
    assert origin["summary"] == "Therapy", (
        f"a placeholder edit overwrote the personal event; "
        f"summary={origin.get('summary')!r}"
    )
    await s.close()
