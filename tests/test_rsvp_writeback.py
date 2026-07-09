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


async def test_pending_rsvp_survives_source_reingest():
    """THE decline-erasure regression (reproduced pre-fix): a decline
    made on main lives only in ``user_rsvp_status`` + the pending flag;
    if the source is re-ingested before the writeback drains (organizer
    edit, rate-limit backoff, the 10-minute content audit), client
    ingest used to overwrite the column from the source's stale value —
    the pending patch was then superseded by one rendered from the
    reverted row and the decline vanished end-to-end, silently.

    Mirror-of-source must not overwrite user intent: the local RSVP is
    preserved while the flag is armed, and the eventually-delivered
    patch carries the decline even though the organizer moved the
    meeting in between.
    """
    s = Scenario()
    try:
        s.given_calendar("main")
        s.given_calendar("client_a")
        await s.given_user("alice", main="main", clients=["client_a"])
        _client_event_with_attendees(s, "client_a", "teamsunc00002")
        await s.run_reconciler_until_quiescent("alice", max_passes=4)

        # Alice declines on the main copy.
        main_copy = s.assert_event_exists("main", summary="Team sync")
        s.update_event(
            "main", main_copy["id"],
            attendees=[{"email": "alice@example.com", "self": True,
                        "responseStatus": "declined"}],
        )
        # ONE pass: main ingest sees the decline and arms the writeback.
        # Block the drain from delivering it this pass by pausing sync
        # AFTER planning... simplest deterministic lever: capture the flag
        # state, then simulate the organizer's edit landing before the next
        # reconcile delivers the patch.
        db = s._db
        await s.run_ingest_only("alice") if hasattr(s, "run_ingest_only") else None

        # Portable path: run a full pass (the patch may deliver), then
        # explicitly re-arm the exact wedged production state: RSVP stored
        # locally, flag set, patch not yet delivered.
        await s.run_reconciler("alice")
        row = await (await db.execute(
            "SELECT id, user_rsvp_status FROM ledger_events "
            "WHERE source_event_id = 'teamsunc00002'")).fetchone()
        assert row is not None
        await db.execute(
            "UPDATE ledger_events SET user_rsvp_status = 'declined', "
            "origin_writeback_pending = 1 WHERE id = ?", (row["id"],))
        # The organizer resets alice to needsAction on the SOURCE and moves
        # the meeting (a genuine source-side content change).
        s.update_event(
            "client_a", "teamsunc00002",
            start="2026-02-02T11:00:00Z", end="2026-02-02T11:30:00Z",
            attendees=[{"email": "alice@example.com", "self": True,
                        "responseStatus": "needsAction"},
                       {"email": "bob@example.com",
                        "responseStatus": "accepted"}],
        )
        await db.commit()

        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # The local decline survived the re-ingest...
        row = await (await db.execute(
            "SELECT user_rsvp_status, origin_writeback_pending FROM ledger_events "
            "WHERE id = ?", (row["id"],))).fetchone()
        assert row["user_rsvp_status"] == "declined", (
            "source re-ingest clobbered the pending decline"
        )
        # ...and was delivered to the origin despite the concurrent edit.
        origin = s.google.get_event(s.cal("client_a"), "teamsunc00002")
        alice = _attendee(origin, "alice@example.com")
        assert alice is not None and alice["responseStatus"] == "declined", (
            f"decline lost; origin attendees={origin.get('attendees')}"
        )
        # Bob's response (from the fresher source array) is intact.
        bob = _attendee(origin, "bob@example.com")
        assert bob is not None and bob["responseStatus"] == "accepted"
    finally:
        await s.close()


async def test_apply_event_to_ledger_preserves_pending_rsvp_unit():
    """Unit-level discriminator for the clobber itself: with the
    pending flag armed, a source re-read carrying a stale
    responseStatus must not overwrite the local decline (while other
    fields — time, other guests' responses — update normally)."""
    from app.ledger.ingest.client import _apply_event_to_ledger

    s = Scenario()
    try:
        s.given_calendar("main")
        s.given_calendar("client_a")
        await s.given_user("alice", main="main", clients=["client_a"])
        _client_event_with_attendees(s, "client_a", "teamsunc00003")
        await s.run_reconciler_until_quiescent("alice", max_passes=4)

        db = s._db
        row = await (await db.execute(
            "SELECT id FROM ledger_events WHERE source_event_id = 'teamsunc00003'"
        )).fetchone()
        await db.execute(
            "UPDATE ledger_events SET user_rsvp_status = 'declined', "
            "origin_writeback_pending = 1 WHERE id = ?", (row["id"],))
        await db.commit()

        # Source re-read: organizer moved the meeting; alice's entry on the
        # source is stale needsAction; bob newly declined.
        changed = await _apply_event_to_ledger(
            db,
            ledger_event_id=int(row["id"]),
            event={
                "id": "teamsunc00003",
                "etag": "e-new",
                "updated": "2026-02-01T12:00:00Z",
                "summary": "Team sync",
                "status": "confirmed",
                "start": {"dateTime": "2026-02-02T11:00:00Z", "timeZone": "UTC"},
                "end": {"dateTime": "2026-02-02T11:30:00Z", "timeZone": "UTC"},
                "organizer": {"email": "alice@example.com"},
                "attendees": [
                    {"email": "alice@example.com", "self": True,
                     "responseStatus": "needsAction"},
                    {"email": "bob@example.com", "responseStatus": "declined"},
                ],
            },
            user_email="alice@example.com",
        )
        assert changed is True

        after = await (await db.execute(
            "SELECT user_rsvp_status, start_at, attendees_json "
            "FROM ledger_events WHERE id = ?", (row["id"],))).fetchone()
        # Intent preserved...
        assert after["user_rsvp_status"] == "declined"
        # ...content still mirrors the source...
        assert after["start_at"].startswith("2026-02-02T11:00")
        # ...and the fresher attendee array (bob's new decline) was taken.
        assert '"declined"' in (after["attendees_json"] or "")
    finally:
        await s.close()
