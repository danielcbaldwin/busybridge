"""Edit-on-main write-back to the source event (REWRITE_PLAN.md §9).

When the user edits one of our managed copies on the main calendar,
:func:`app.ledger.ingest.main._maybe_apply_main_edit_back` classifies
the edit and either propagates it to the calendar that *sourced* the
event or reverts it as drift.

The propagate/revert matrix exercised here:

* client, editable     — time and detail edits propagate.
* client, locked       — time and detail edits revert.
* personal             — time edits propagate; detail edits revert
                         (the main copy is an opaque placeholder).

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


async def test_personal_time_edit_on_main_propagates_to_personal_source():
    """The reviewer's reproduction: dragging a "Busy (personal)"
    placeholder on main moves the real personal source event, so the
    personal calendar and the ledger never disagree on the time."""
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
    assert origin["start"]["dateTime"] == "2026-02-02T10:00:00Z", (
        f"time edit not propagated to personal source; "
        f"start={origin.get('start')}"
    )
    # The personal detail is untouched — the writeback never carries it.
    assert origin["summary"] == "Dentist"
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
