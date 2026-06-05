"""Edit-on-main RSVP write-back to the origin calendar.

When the user changes their RSVP on the main-calendar copy of a
client-sourced event, that response is written back to the event on
the *origin* client calendar.  The mechanism is a "phantom"
``present_full_rsvp_only`` projection targeting the origin calendar,
delivered as an ``events.patch``.

Two safety properties are verified here as well:

* The patch carries the COMPLETE attendee list, so other guests on
  the source event are never dropped.
* The origin projection can only ever PATCH or no-op — when the event
  is cancelled / intentionally-deleted it must NOT delete the user's
  real source event.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _client_event_with_attendees(s: Scenario, calendar_nick: str, event_id: str):
    """Insert a client event where alice is organizer + a self
    attendee, plus a second guest (bob)."""
    return s.google.insert_event(s.cal(calendar_nick), {
        "id": event_id,
        "summary": "Team sync",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "alice@example.com"},
        "attendees": [
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "bob@example.com", "responseStatus": "accepted"},
        ],
    })


def _attendee(event: dict, email: str):
    for a in event.get("attendees") or []:
        if a.get("email") == email:
            return a
    return None


async def test_rsvp_set_on_main_is_written_back_to_origin():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    _client_event_with_attendees(s, "client_a", "teamsunc00001")

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    # The main copy carries alice as a self-attendee so she can RSVP there.
    main_copy = s.assert_event_exists("main", summary="Team sync")
    self_att = next(
        (a for a in main_copy.get("attendees") or [] if a.get("self")), None,
    )
    assert self_att is not None, (
        f"main copy has no self-attendee; attendees={main_copy.get('attendees')}"
    )
    # The self-attendee MUST carry an email — events.insert rejects a
    # bare {"self": True} with "400 Missing attendee email", which is
    # what kept RSVP'd events off the main calendar in production.
    assert self_att.get("email"), (
        f"main-copy self-attendee has no email; attendee={self_att}"
    )

    # Alice accepts on the main copy.
    s.update_event(
        "main", main_copy["id"],
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "accepted"}],
    )

    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    # The acceptance is written back to the origin client event...
    origin = s.google.get_event(s.cal("client_a"), "teamsunc00001")
    alice = _attendee(origin, "alice@example.com")
    assert alice is not None and alice["responseStatus"] == "accepted", (
        f"RSVP not propagated to origin; attendees={origin.get('attendees')}"
    )
    # ...and the other guest is NOT dropped (patch sent the full list).
    bob = _attendee(origin, "bob@example.com")
    assert bob is not None and bob["responseStatus"] == "accepted"
    await s.close()


async def test_origin_rsvp_projection_never_deletes_the_source_event():
    """When the event is intentionally deleted from main, the origin
    rsvp projection goes ABSENT — but it must NEVER issue a delete
    against the user's real source event."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    _client_event_with_attendees(s, "client_a", "important00001")

    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    main_copy = s.assert_event_exists("main", summary="Team sync")

    # The user deletes the synced copy from their main calendar.
    s.google.delete_event(s.cal("main"), main_copy["id"])

    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    # The source event on the client calendar is untouched.
    origin = s.google.get_event(s.cal("client_a"), "important00001")
    assert origin.get("status") != "cancelled", (
        "the origin rsvp projection deleted the user's real source event"
    )
    assert _attendee(origin, "bob@example.com") is not None
    await s.close()


async def test_rsvp_only_does_not_clobber_source_on_source_side_change():
    """When ANY change comes in on the SOURCE event itself (the user
    RSVPs there directly, another attendee responds, etc.), BB must NOT
    write its cached attendee snapshot back to the source — that's a
    clobber that reverts whatever the user just did.

    Live regression: an accepted RSVP on mlcommons was reverted to
    needsAction because re-ingesting the source bumped desired_hash on
    the rsvp_only writeback projection and the diff fired a patch with
    BB's stale attendees_json.

    The writeback patch is allowed to fire ONLY when
    ``origin_writeback_pending=1`` — i.e. when the change came from a
    user edit on the BB main copy.  A source-ingest bump alone never
    triggers it.
    """
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    _client_event_with_attendees(s, "client_a", "srcrsvp00001")

    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    # Stamp a hash baseline so the next diff sees a settled state.
    await s.run_reconciler_until_quiescent("alice", max_passes=2)

    # Alice accepts directly on the SOURCE (mlcommons-style flow), and
    # any other change happens on it too (e.g. bob's RSVP changes) — the
    # whole point is that the source state is now non-equal to whatever
    # BB has cached.  BB must ingest this and update the ledger, but
    # MUST NOT echo it back as a patch.
    s.update_event(
        "client_a", "srcrsvp00001",
        attendees=[
            {"email": "alice@example.com", "self": True,
             "responseStatus": "accepted"},
            {"email": "bob@example.com", "responseStatus": "declined"},
        ],
    )

    # Snapshot the source state, then reconcile, then assert the source
    # is byte-identical (or at least: alice still accepted, bob still
    # declined).  A clobbering patch would have reverted these to
    # whatever BB had cached from the first ingest.
    await s.run_reconciler_until_quiescent("alice", max_passes=5)
    source = s.google.get_event(s.cal("client_a"), "srcrsvp00001")
    alice = _attendee(source, "alice@example.com")
    bob = _attendee(source, "bob@example.com")
    assert alice is not None and alice["responseStatus"] == "accepted", (
        f"source RSVP for alice was CLOBBERED by BB; attendees="
        f"{source.get('attendees')}"
    )
    assert bob is not None and bob["responseStatus"] == "declined", (
        f"source RSVP for bob was clobbered too; attendees="
        f"{source.get('attendees')}"
    )
    await s.close()


async def test_no_origin_projection_for_a_locked_event_without_an_rsvp():
    """A non-editable client event the user has no RSVP on gets no
    origin writeback projection — there is nothing that could ever be
    written back to the source, so no needless patch is planned.

    (An *editable* event does get the projection even without an
    RSVP, so a later time/detail edit on the main copy can propagate
    — that path is exercised in test_main_edit_writeback.py.)"""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    # Organised by someone else, another guest, alice not an attendee,
    # no guestsCanModify → not editable, and no RSVP for alice.
    s.google.insert_event(s.cal("client_a"), {
        "id": "locked00000001",
        "summary": "Locked block",
        "start": {"dateTime": "2026-02-03T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-03T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "boss@example.com"},
        "attendees": [
            {"email": "boss@example.com", "organizer": True,
             "responseStatus": "accepted"},
        ],
    })
    await s.run_reconciler_until_quiescent("alice", max_passes=3)

    db = await s.setup_db()
    ccid = user.client_calendar_ids["client_a"]
    rows = await (await db.execute(
        """SELECT p.desired_state
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (user.user_id, ccid),
    )).fetchall()
    assert all(r["desired_state"] != "present_full_rsvp_only" for r in rows)
    await s.close()
