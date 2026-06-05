"""Main-copy feature parity with v1.

A full copy of a client event on the main calendar must:

* be coloured by the SOURCE CALENDAR's colour (the colour the user
  picked in the UI), so events from different clients are visually
  distinct — and that colour must survive re-ingest (the source event
  itself usually carries no colorId);
* carry the guest list and a ``Source:`` line in its description.
"""

from __future__ import annotations

import json

import pytest

from app.ledger.payload import PRESENT_FULL, render_payload
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def test_content_hash_ignores_conference_data_and_html_link():
    """conferenceData and htmlLink are excluded from change detection.

    Google's API returns different conferenceData entry-point URIs
    across reads of the same recurring event (both valid links the
    event carries), so any contribution to the hash — even a normalised
    signature — churns versions into the thousands.  Both fields are
    still STORED and RENDERED onto the main copy; only the hash that
    drives 'did the event change?' ignores them.  A real Meet-link
    change still propagates on the next ingest of any OTHER field
    change or via the periodic content audit.
    """
    from app.ledger.ingest.client import _content_hash

    base = {
        "summary": "Sync", "start_at": "2026-03-03T09:00:00Z",
        "conference_data_json":
            '{"entryPoints":[{"uri":"https://meet.google.com/abc"}]}',
        "source_html_link": "https://cal/eid=1",
    }
    # Different Meet link, different htmlLink, different conferenceData
    # serialisation — must all hash identically.
    different = dict(
        base,
        conference_data_json='{"entryPoints":[{"uri":"https://meet.google.com/xyz"}]}',
        source_html_link="https://cal/eid=2",
    )
    assert _content_hash(base) == _content_hash(different)
    # A real field change → different hash (sanity).
    assert _content_hash(base) != _content_hash(dict(base, summary="Changed"))


async def test_main_copy_carries_conference_data_and_original_link():
    """Meet/Zoom data and a link back to the source event are copied
    onto the main copy (v1 parity)."""
    conf = {
        "conferenceId": "abc-defg-hij",
        "entryPoints": [
            {"entryPointType": "video", "uri": "https://meet.google.com/abc"},
        ],
    }
    row = {
        "summary": "Standup", "description": "notes", "is_all_day": 0,
        "start_at": "2026-03-03T09:00:00Z", "end_at": "2026-03-03T09:15:00Z",
        "user_can_edit": 1, "source_type": "client", "source_label": "client_a",
        "conference_data_json": json.dumps(conf),
        "source_html_link": "https://www.google.com/calendar/event?eid=xyz",
    }
    body = render_payload(
        desired_state=PRESENT_FULL, ledger_row=row, projection_id=1,
        target_kind="main", main_calendar_email="m@example.com",
    )
    assert body["conferenceData"] == conf
    assert (
        "Original event: https://www.google.com/calendar/event?eid=xyz"
        in body["description"]
    )


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


async def test_conference_data_copied_and_no_churn_across_sources():
    """The Meet link reaches the main copy, and storing conferenceData /
    html_link in every ingest path (client + personal + main) keeps a
    second reconcile a no-op — a missed write site would churn here."""
    conf = {
        "conferenceId": "x-y-z",
        "entryPoints": [
            {"entryPointType": "video", "uri": "https://meet.google.com/xyz"},
        ],
    }
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"], personals=["personal_a"],
    )
    s.given_event(
        "client_a", summary="Client call", start="2026-03-03T09:00:00Z",
        conference_data=conf,
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "accepted"}],
    )
    s.given_event(
        "personal_a", summary="Personal call", start="2026-03-04T09:00:00Z",
        conference_data=conf,
    )
    await s.run_reconciler("alice")

    copy = s.assert_event_exists("main", summary_contains="Client call")
    assert copy.get("conferenceData") == conf

    # A second pass must be a no-op: nothing re-enqueued, nothing drained.
    out = await s.run_reconciler("alice")
    assert out.get("drain", {}).get("succeeded", 0) == 0, (
        f"conference data / html link caused churn: {out}"
    )
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
