"""Main-copy feature parity with v1 (REWRITE_PLAN.md §9).

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


async def test_content_hash_conference_data_signature():
    """conferenceData change detection uses a normalised signature.

    Google's conferenceData serialisation varies between reads, so
    hashing it verbatim makes every Meet event churn.  Read-noise
    (entry-point order, extra labels) must hash identically, but a
    GENUINE Meet-link change (a different entry-point URI) must be
    detected so the new link propagates — and so must a real field.
    htmlLink is display-only and never drives a bump.
    """
    from app.ledger.ingest.client import _content_hash

    base = {
        "summary": "Sync", "start_at": "2026-03-03T09:00:00Z",
        "conference_data_json":
            '{"entryPoints":[{"uri":"https://meet.google.com/abc"}]}',
        "source_html_link": "https://cal/eid=1",
    }
    # Read-noise (reordered/extra labels) + different htmlLink → same hash.
    noise = dict(
        base,
        conference_data_json=(
            '{"conferenceSolution":{"name":"Meet"},'
            '"entryPoints":[{"label":"x","uri":"https://meet.google.com/abc"}]}'
        ),
        source_html_link="https://cal/eid=2",
    )
    assert _content_hash(base) == _content_hash(noise)
    # A genuinely different Meet link → different hash (must re-sync).
    new_link = dict(
        base,
        conference_data_json='{"entryPoints":[{"uri":"https://meet.google.com/xyz"}]}',
    )
    assert _content_hash(base) != _content_hash(new_link)
    # A real field change → different hash.
    assert _content_hash(base) != _content_hash(dict(base, summary="Changed"))


async def test_main_copy_carries_conference_data_and_original_link():
    """Meet/Zoom data and a link back to the source event are copied
    onto the main copy (REWRITE_PLAN.md §9 / v1 parity)."""
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
