"""Main-copy feature parity with v1 (REWRITE_PLAN.md §9).

A full copy of a client event on the main calendar must:

* be coloured by the SOURCE CALENDAR's colour (the colour the user
  picked in the UI), so events from different clients are visually
  distinct — and that colour must survive re-ingest (the source event
  itself usually carries no colorId);
* carry the guest list and a ``Source:`` line in its description.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def test_main_copy_colored_by_calendar_and_lists_attendees():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    # The colour the user picked for this calendar in the UI.
    db = await s.setup_db()
    await db.execute(
        "UPDATE client_calendars SET color_id = '3' WHERE id = ?",
        (user.client_calendar_ids["client_a"],),
    )
    await db.commit()
    s.given_event(
        "client_a", summary="Board meeting", description="agenda",
        start="2026-03-03T09:00:00Z",
        attendees=[
            {"email": "bob@example.com", "displayName": "Bob",
             "responseStatus": "accepted"},
            {"email": "carol@example.com", "responseStatus": "declined"},
            {"email": "alice@example.com", "self": True,
             "responseStatus": "accepted"},
        ],
    )
    await s.run_reconciler("alice")

    # Locked (alice is a guest, not the organizer) → 🔒 prefix, so
    # match on the contained title.
    copy = s.assert_event_exists("main", summary_contains="Board meeting")
    # Coloured by the source calendar's UI colour, not the event's own.
    assert copy.get("colorId") == "3"
    desc = copy.get("description") or ""
    assert desc.startswith("agenda")          # real body preserved
    # Guest list — the user's own (self) entry is excluded.
    assert "Attendees (2): 1 yes, 1 no" in desc
    assert "Bob" in desc
    assert "Source: client_a" in desc
    await s.close()


async def test_main_copy_color_survives_reingest():
    """The colour is read from the calendar at render time, so a second
    sync (the source event carries no colorId) must not wipe it."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    db = await s.setup_db()
    await db.execute(
        "UPDATE client_calendars SET color_id = '5' WHERE id = ?",
        (user.client_calendar_ids["client_a"],),
    )
    await db.commit()
    s.given_event("client_a", summary="Sync", start="2026-03-03T09:00:00Z")
    await s.run_reconciler("alice")
    await s.run_reconciler("alice")  # a second pass must keep the colour
    copy = s.assert_event_exists("main", summary="Sync")
    assert copy.get("colorId") == "5"
    await s.close()
