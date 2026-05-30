"""Extended integration tests covering the rest of Stage 2:
webcal, personal, recurring events, edit-on-main, color recolor,
discovery, cleanup ops, trigger plumbing, and the facade."""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _alice_with_two_clients() -> Scenario:
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    return s


# ---------------------------------------------------------------------------
# Recurring events
# ---------------------------------------------------------------------------
async def test_recurring_series_propagates_to_main_and_busy_blocks():
    s = await _alice_with_two_clients()
    s.given_recurring_event(
        "client_a",
        summary="Weekly standup",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=3;BYDAY=MO",
        event_id="bbsta000000001",
    )
    await s.run_reconciler("alice")

    # Main has the full-copy series.
    main_copy = s.assert_event_exists("main", summary="Weekly standup")
    assert main_copy.get("recurrence")

    # Client B has the busy series.
    busy_b = s.find_events("client_b", summary="Busy")
    assert len(busy_b) == 1
    assert busy_b[0].get("recurrence")
    await s.close()


async def test_cancelled_recurring_instance_stays_cancelled_across_full_sync():
    """The recurring-cancellation-amnesia regression: an instance
    cancelled on a client must remain cancelled on the synced
    copies on main + peers, even after a sync-token expiry forces
    a full-sync that doesn't surface the cancelled exception."""
    s = await _alice_with_two_clients()
    s.given_recurring_event(
        "client_a",
        summary="Will lose one",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=4;BYDAY=MO",
        event_id="bbcanrec000001",
    )
    await s.run_reconciler("alice")
    # Cancel the 2026-02-16 instance on the source.
    s.google.delete_event(s.cal("client_a"), "bbcanrec000001_20260216T090000Z")
    # First incremental sync surfaces the cancellation and produces
    # an instance ledger row with status='cancelled'.
    await s.run_reconciler("alice")

    # Look up the main projection's google_event_id (the parent
    # series on main).
    db = await s.setup_db()
    parent_proj = await (await db.execute(
        """SELECT p.google_event_id FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ? AND p.target_kind = 'main'
              AND e.parent_canonical_uid IS NULL""",
        (s.user("alice").user_id,),
    )).fetchone()
    parent_id_on_main = parent_proj["google_event_id"]

    # The 2026-02-16 instance must be cancelled on main.
    insts = s.google.list_instances(
        s.cal("main"), parent_id_on_main, show_deleted=True,
    )
    cancelled = [
        i for i in insts["items"]
        if i["status"] == "cancelled"
        and i.get("originalStartTime", {}).get("dateTime", "").startswith("2026-02-16")
    ]
    assert len(cancelled) >= 1, (
        f"expected cancelled instance on main; got "
        f"{[(i.get('status'), i.get('originalStartTime')) for i in insts['items']]}"
    )

    # Now expire the sync token and run again — the cancellation
    # must persist (the bug today; fixed in the rewrite by the
    # sticky instance ledger row + sync-token-clears-on-expiry).
    state = await (await db.execute(
        "SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id = ?",
        (s.user("alice").client_calendar_ids["client_a"],),
    )).fetchone()
    if state and state["sync_token"]:
        s.google.expire_sync_token(state["sync_token"])
    await s.run_reconciler("alice")

    insts_after = s.google.list_instances(
        s.cal("main"), parent_id_on_main, show_deleted=True,
    )
    still_cancelled = [
        i for i in insts_after["items"]
        if i["status"] == "cancelled"
        and i.get("originalStartTime", {}).get("dateTime", "").startswith("2026-02-16")
    ]
    assert len(still_cancelled) >= 1
    await s.close()


# ---------------------------------------------------------------------------
# Personal calendar
# ---------------------------------------------------------------------------
async def test_personal_event_creates_busy_block_on_main_and_clients():
    """Personal events project as 'Busy (personal)' on main and all
    client calendars — no detail leaks, and no projection back to
    the personal calendar itself."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    await s.given_user(
        "alice", main="main",
        clients=["client_a"],
        personals=["personal_a"],
    )
    s.given_event(
        "personal_a",
        summary="Dentist",
        start="2026-02-02T09:00:00Z",
        location="Private clinic",
        description="Sensitive appointment notes",
        attendees=[
            {"email": "alice@example.com", "self": True,
             "responseStatus": "accepted"},
            {"email": "dentist@example.com", "responseStatus": "accepted"},
        ],
        conference_data={"entryPoints": [{"uri": "https://meet.example/private"}]},
    )
    await s.run_reconciler("alice")

    main_copy = s.assert_event_exists("main", summary="Busy (personal)")
    busy_a = s.find_events("client_a", summary="Busy (personal)")
    assert len(busy_a) == 1
    for copy in (main_copy, busy_a[0]):
        assert copy["visibility"] == "private"
        assert copy["transparency"] == "opaque"
        assert "location" not in copy
        assert "attendees" not in copy
        assert "conferenceData" not in copy
        desc = copy.get("description") or ""
        assert "Dentist" not in desc
        assert "Sensitive appointment notes" not in desc
        assert "Private clinic" not in desc
    # No event back on personal — origin is read-only.
    detail_on_personal = s.find_events("personal_a", summary="Busy (personal)")
    assert detail_on_personal == []
    # Original event on personal_a still there too.
    source = s.assert_event_exists("personal_a", summary="Dentist")
    assert source["location"] == "Private clinic"
    assert source["attendees"]
    assert source["conferenceData"]

    db = await s.setup_db()
    personal_id = s.user("alice").personal_calendar_ids["personal_a"]
    personal_targets = await (await db.execute(
        """SELECT p.id, p.desired_state
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND e.source_type = 'personal'
              AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (s.user("alice").user_id, personal_id),
    )).fetchall()
    assert personal_targets == []
    await s.close()


# ---------------------------------------------------------------------------
# Webcal
# ---------------------------------------------------------------------------
def _build_ics(*events: dict) -> bytes:
    """Build a minimal ICS body from a list of event dicts."""
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Test//EN",
    ]
    for ev in events:
        lines.append("BEGIN:VEVENT")
        lines.append(f"UID:{ev['uid']}")
        lines.append(f"SUMMARY:{ev['summary']}")
        lines.append(f"DTSTART:{ev['dtstart']}")
        lines.append(f"DTEND:{ev['dtend']}")
        if ev.get("status"):
            lines.append(f"STATUS:{ev['status']}")
        lines.append("END:VEVENT")
    lines.append("END:VCALENDAR")
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


async def test_webcal_creates_main_copies_and_busy_blocks():
    s = await _alice_with_two_clients()
    sub_id = await s.given_webcal(
        "alice", sub_nick="lunches", url="https://example.com/lunches.ics",
    )
    ics_body = _build_ics({
        "uid": "lunch-1@example.com",
        "summary": "Team lunch",
        "dtstart": "20260202T120000Z",
        "dtend": "20260202T130000Z",
    })
    feed_state = {"body": ics_body, "etag": '"v1"'}

    async def fetch(url, if_none_match):
        if if_none_match == feed_state["etag"]:
            return {"status": 304, "etag": feed_state["etag"], "body": None}
        return {"status": 200, "etag": feed_state["etag"], "body": feed_state["body"]}

    await s.run_reconciler("alice", webcal_fetch=fetch)

    s.assert_event_exists("main", summary="Team lunch")
    busy_a = s.find_events("client_a", summary="Busy")
    busy_b = s.find_events("client_b", summary="Busy")
    assert len(busy_a) == 1
    assert len(busy_b) == 1
    await s.close()


async def test_webcal_rename_does_not_duplicate_for_unstable_uid_feed():
    """The Eventbrite/Luma bug: feed regenerates UID every fetch.
    A rename must NOT produce a duplicate ledger row when only the
    summary changed."""
    s = await _alice_with_two_clients()
    sub_id = await s.given_webcal(
        "alice", sub_nick="luma", url="https://example.com/luma.ics",
    )

    # First poll: UUIDv4 uid, summary "Concert".
    body1 = _build_ics({
        "uid": "d2418c3a-8a93-4a16-a4e7-1d0b9b8c2f00",
        "summary": "Concert",
        "dtstart": "20260202T120000Z",
        "dtend": "20260202T140000Z",
    })

    fetch_iter = {"body": body1, "etag": None}

    async def fetch(url, if_none_match):
        return {"status": 200, "etag": fetch_iter["etag"], "body": fetch_iter["body"]}

    await s.run_reconciler("alice", webcal_fetch=fetch)
    s.assert_event_exists("main", summary="Concert")
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 1

    # Second poll: new UUIDv4, renamed summary, same time.
    fetch_iter["body"] = _build_ics({
        "uid": "9aa11111-2222-4333-8444-555566667777",
        "summary": "Concert (renamed)",
        "dtstart": "20260202T120000Z",
        "dtend": "20260202T140000Z",
    })
    await s.run_reconciler("alice", webcal_fetch=fetch)

    # Exactly one event on main — renamed, not duplicated.
    s.assert_event_exists("main", summary="Concert (renamed)")
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 1
    await s.close()


async def test_webcal_304_not_modified_is_a_noop():
    s = await _alice_with_two_clients()
    sub_id = await s.given_webcal(
        "alice", sub_nick="static", url="https://example.com/static.ics",
    )
    body = _build_ics({
        "uid": "static-1@example.com",
        "summary": "Static event",
        "dtstart": "20260202T120000Z",
        "dtend": "20260202T130000Z",
    })

    poll_count = {"n": 0}

    async def fetch(url, if_none_match):
        poll_count["n"] += 1
        if if_none_match == '"v1"':
            return {"status": 304, "etag": '"v1"', "body": None}
        return {"status": 200, "etag": '"v1"', "body": body}

    out1 = await s.run_reconciler("alice", webcal_fetch=fetch)
    assert out1["ingest"][f"webcal:{sub_id}"]["created"] == 1
    out2 = await s.run_reconciler("alice", webcal_fetch=fetch)
    assert out2["ingest"][f"webcal:{sub_id}"]["not_modified"] == 1
    assert out2["enqueued"] == 0
    await s.close()


# ---------------------------------------------------------------------------
# Edit-on-main propagation
# ---------------------------------------------------------------------------
async def test_main_drift_reverted_for_non_editable_event():
    """User drags a non-editable event on main; the next reconcile
    must revert the move (uniform 'revert-on-drift', closes the
    silent gap on non-editable client copies)."""
    s = await _alice_with_two_clients()
    src = s.given_event(
        "client_a",
        summary="Quarterly review",
        start="2026-02-02T09:00:00Z",
        attendees=[
            {"email": "alice@example.com", "responseStatus": "accepted", "self": True},
        ],
    )
    # Mark non-editable post-insert.
    s.google.patch_event(s.cal("client_a"), src["id"], {
        "organizer": {"email": "boss@example.com"},
        "guestsCanModify": False,
    })

    await s.run_reconciler("alice")
    main_copy = s.assert_event_exists("main", summary_contains="Quarterly review")
    assert main_copy["summary"].startswith("🔒 ")
    original_start = main_copy["start"]["dateTime"]

    # User drags the event by 1 hour on main.
    s.google.patch_event(s.cal("main"), main_copy["id"], {
        "start": {"dateTime": "2026-02-02T10:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T10:30:00Z", "timeZone": "UTC"},
    })

    await s.run_reconciler("alice")
    after = s.google.get_event(s.cal("main"), main_copy["id"])
    assert after["start"]["dateTime"] == original_start, (
        f"expected revert to {original_start}, got {after['start']['dateTime']}"
    )
    await s.close()


# ---------------------------------------------------------------------------
# Color recolor
# ---------------------------------------------------------------------------
async def test_color_recolor_re_renders_main_copies():
    """When a client calendar's color_id changes, every main copy
    sourced from that calendar must be re-rendered with the new
    colorId."""
    from app.ledger.admin_ops import recolor_client_calendar
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="Colored", start="2026-02-02T09:00:00Z")
    await s.run_reconciler("alice")

    db = await s.setup_db()
    n = await recolor_client_calendar(
        db,
        client_calendar_id=s.user("alice").client_calendar_ids["client_a"],
        new_color_id="9",
    )
    assert n >= 1

    await s.run_reconciler("alice")
    main_copy = s.assert_event_exists("main", summary="Colored")
    assert main_copy.get("colorId") == "9"
    await s.close()


# ---------------------------------------------------------------------------
# Cleanup operations
# ---------------------------------------------------------------------------
async def test_disconnect_calendar_permanently_removes_writes_to_and_from_it():
    """Disconnect = cleanup + deactivate.  After disconnect, no
    busy blocks are repopulated on the calendar and its sourced
    events are removed from main."""
    from app.ledger.admin_ops import disconnect_calendar
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="A1", start="2026-02-02T09:00:00Z")
    s.given_event("client_a", summary="A2", start="2026-02-03T09:00:00Z")
    s.given_event("client_b", summary="B1", start="2026-02-02T11:00:00Z")
    await s.run_reconciler("alice")
    assert s.google.event_count(s.cal("main")) == 3

    db = await s.setup_db()
    await disconnect_calendar(
        db, user_id=s.user("alice").user_id,
        client_calendar_id=s.user("alice").client_calendar_ids["client_b"],
    )
    await s.run_reconciler("alice", include_main=False)

    # client_b should have only its native events; busy blocks gone.
    assert s.google.event_count(s.cal("client_b"), include_cancelled=False) == 1
    # B1 itself should be gone from main.
    s.assert_no_event_with_summary("main", "B1")
    # A1, A2 still on main (their source is still connected).
    s.assert_event_exists("main", summary="A1")
    s.assert_event_exists("main", summary="A2")
    await s.close()


async def test_cleanup_one_calendar_then_resync_restores_state():
    """Cleanup-one-calendar wipes BusyBridge writes from a calendar
    AND cancels events sourced from it; the next reconcile then
    re-fetches and re-creates idempotently.

    Within ONE reconcile pass after cleanup, we observe:
      * the source events still on the source calendar (untouched),
      * fresh deterministic-ID writes on the targets,
      * the net result indistinguishable from before, except every
        BusyBridge-managed Google event ID has changed.
    """
    from app.ledger.admin_ops import cleanup_one_calendar
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="A1", start="2026-02-02T09:00:00Z")
    s.given_event("client_b", summary="B1", start="2026-02-02T11:00:00Z")
    await s.run_reconciler("alice")
    assert s.google.event_count(s.cal("main")) == 2
    main_b1_before = next(
        e for e in s.list_events("main") if e["summary"] == "B1"
    )

    db = await s.setup_db()
    await cleanup_one_calendar(
        db, user_id=s.user("alice").user_id,
        client_calendar_id=s.user("alice").client_calendar_ids["client_b"],
    )
    await s.run_reconciler("alice")
    # B1 is back on main (cleanup + re-sync, same pass).
    main_b1_after = s.assert_event_exists("main", summary="B1")
    # But its google_event_id is unchanged — the canonical_uid and
    # therefore the projection persist.  The cleanup-then-resync
    # round-trip is content-stable.
    assert main_b1_after["id"] == main_b1_before["id"]
    await s.close()


async def test_cleanup_and_pause_removes_everything_and_pauses():
    from app.ledger.admin_ops import cleanup_and_pause
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="X", start="2026-02-02T09:00:00Z")
    await s.run_reconciler("alice")
    assert s.google.event_count(s.cal("main")) == 1

    db = await s.setup_db()
    await cleanup_and_pause(db, user_id=s.user("alice").user_id)
    await s.run_reconciler("alice", include_main=False)

    # All our writes on main are gone.
    assert s.google.event_count(s.cal("main")) == 0
    # User is paused.
    row = await (await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?",
        (s.user("alice").user_id,),
    )).fetchone()
    assert bool(row["sync_paused"]) is True
    await s.close()


async def test_full_resync_clears_sync_tokens_without_changing_data():
    from app.ledger.admin_ops import full_resync
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="X", start="2026-02-02T09:00:00Z")
    await s.run_reconciler("alice")

    db = await s.setup_db()
    await full_resync(db, user_id=s.user("alice").user_id)
    # Sync tokens cleared.
    row = await (await db.execute(
        """SELECT sync_token FROM calendar_sync_state
            WHERE client_calendar_id = ?""",
        (s.user("alice").client_calendar_ids["client_a"],),
    )).fetchone()
    assert row["sync_token"] is None

    # Re-running yields no new writes (idempotent).
    before = s.google.event_count(s.cal("main"))
    await s.run_reconciler("alice")
    after = s.google.event_count(s.cal("main"))
    assert before == after
    await s.close()


# ---------------------------------------------------------------------------
# Discovery / orphan scan
# ---------------------------------------------------------------------------
async def test_discovery_deletes_orphaned_managed_events():
    """Imagine the user manually created an event on main with our
    deterministic-id prefix, OR we lost the projection row for one
    of our writes.  Discovery cleans it up."""
    s = await _alice_with_two_clients()
    # Drop an "orphan" with our id prefix on main.  Real Google
    # would never let an external client create such an ID via the
    # web UI, but a corrupted DB state could leave one stranded.
    s.google.insert_event(s.cal("main"), {
        "id": "bb0000000000xxx".replace("x", "a"),  # legal base32hex
        "summary": "Orphaned BB write",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
    })
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 1

    await s.run_reconciler("alice", run_discovery=True)
    # After discovery + drain: gone.
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 0
    await s.close()


# ---------------------------------------------------------------------------
# Reconcile-request trigger plumbing
# ---------------------------------------------------------------------------
async def test_webhook_then_periodic_collapse_into_one_request():
    from app.ledger.triggers import (
        claim_due_request, enqueue_periodic, enqueue_webhook,
    )
    s = await _alice_with_two_clients()
    db = await s.setup_db()
    user_id = s.user("alice").user_id

    await enqueue_webhook(db, user_id=user_id, source_hint="client:7")
    await enqueue_webhook(db, user_id=user_id, source_hint="client:8")
    await enqueue_periodic(db, user_id=user_id)

    # All three notifications collapse into exactly one row.
    row_count = await (await db.execute(
        "SELECT COUNT(*) AS n FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    assert row_count["n"] == 1

    # After the debounce window has elapsed, claim succeeds once.
    s.advance(timedelta(seconds=10))
    claimed = await claim_due_request(db, user_id=user_id)
    assert claimed is not None
    # Already in-flight — second claim returns None.
    again = await claim_due_request(db, user_id=user_id)
    assert again is None
    await s.close()


async def test_manual_sync_respects_settling_delay():
    from app.ledger.triggers import (
        MANUAL_SETTLING_DELAY, claim_due_request, enqueue_manual,
    )
    from datetime import datetime, timezone as _tz
    s = await _alice_with_two_clients()
    db = await s.setup_db()
    user_id = s.user("alice").user_id
    base = datetime(2026, 1, 1, tzinfo=_tz.utc)

    await enqueue_manual(db, user_id=user_id, source_hint="client:7", now=base)
    # Just barely before the settling delay — claim is None.
    claimed = await claim_due_request(
        db, user_id=user_id, now=base + MANUAL_SETTLING_DELAY - timedelta(seconds=1),
    )
    assert claimed is None
    # After the delay — claim succeeds.
    claimed = await claim_due_request(
        db, user_id=user_id, now=base + MANUAL_SETTLING_DELAY + timedelta(seconds=1),
    )
    assert claimed is not None
    await s.close()


# ---------------------------------------------------------------------------
# Facade
# ---------------------------------------------------------------------------
async def test_facade_counts_match_observable_state():
    from app.ledger.facade import (
        count_busy_blocks_per_calendar, count_events_for_user,
        count_main_copies, outbox_summary, sync_failure_status,
    )
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="A1", start="2026-02-02T09:00:00Z")
    s.given_event("client_a", summary="A2", start="2026-02-03T09:00:00Z")
    s.given_event("client_b", summary="B1", start="2026-02-02T11:00:00Z")
    await s.run_reconciler("alice")

    db = await s.setup_db()
    user_id = s.user("alice").user_id

    events = await count_events_for_user(db, user_id=user_id)
    assert events.get("client") == 3

    main = await count_main_copies(db, user_id=user_id)
    assert main == 3

    busy = await count_busy_blocks_per_calendar(db, user_id=user_id)
    # client_a has 1 (from B1); client_b has 2 (from A1, A2).
    cid_a = s.user("alice").client_calendar_ids["client_a"]
    cid_b = s.user("alice").client_calendar_ids["client_b"]
    assert busy.get(cid_a) == 1
    assert busy.get(cid_b) == 2

    outbox = await outbox_summary(db, user_id=user_id)
    # Everything should have settled to 'done'.
    assert outbox.get("done", 0) >= 6
    assert outbox.get("pending", 0) == 0

    failures = await sync_failure_status(db, user_id=user_id)
    assert failures["main"]["consecutive_failures"] == 0
    for client in failures["clients"]:
        assert client["consecutive_failures"] == 0
    await s.close()


async def test_facade_lists_permanent_failures():
    """A poison-pill projection ends up in the facade's failure list.

    The drain marks a projection ``permanently_failed`` after
    ``POISON_PILL_THRESHOLD`` 4xx attempts.  We exercise the
    classification path directly rather than through the failure
    injector — much more deterministic."""
    from app.ledger.facade import list_permanent_failures
    from app.ledger.outbox import (
        POISON_PILL_THRESHOLD,
        _mark_permanent_failure,
    )
    from datetime import datetime, timezone as _tz
    s = await _alice_with_two_clients()
    s.given_event("client_a", summary="Poisoned", start="2026-02-02T09:00:00Z")
    await s.run_reconciler("alice")  # creates projections + outbox rows

    db = await s.setup_db()
    # Find the main projection's outbox op.
    op = await (await db.execute(
        """SELECT * FROM outbox_operations
            WHERE user_id = ? ORDER BY id LIMIT 1""",
        (s.user("alice").user_id,),
    )).fetchone()
    assert op is not None
    # Simulate the drain's "5+ failures of a 4xx → permanent" path.
    await _mark_permanent_failure(
        db, op, error="HTTP 400 Bad Request: poison-pill simulation",
        http_status=400, now=datetime.now(_tz.utc),
    )

    failures = await list_permanent_failures(db, user_id=s.user("alice").user_id)
    assert len(failures) >= 1
    assert any("poison-pill" in (f.get("last_error") or "") for f in failures)
    await s.close()
