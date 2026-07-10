"""Origin-writeback hardening: locked-instance edits, pending-RSVP
preservation, and the personal read-only guarantee.

Three production bug clusters covered here:

1. Locked-instance edits must not corrupt the real source event.
   ``_ingest_managed_recurring_instance`` used to extract fields from
   the RENDERED main copy and arm the writeback with no
   ``user_can_edit`` gating and no lock-prefix stripping — declining
   ONE occurrence of a non-editable client meeting wrote the
   lock-emoji title into the REAL source event (and the main copy then
   rendered a double lock), and dragging one occurrence of a locked
   meeting MOVED the real source meeting.  The instance path now
   mirrors ``_maybe_apply_main_edit_back``'s per-category policy, and
   ``_render_origin_writeback`` sends only the attendee/RSVP fields
   when the row is not editable.

2. A pending RSVP must survive a source re-ingest.  A decline whose
   writeback has not drained yet (backoff, multi-pass queueing, the
   content audit) used to be silently reverted when the source event
   was re-read, because ingest unconditionally overwrote
   ``user_rsvp_status`` / ``attendees_json`` from the source.

3. Personal (and webcal) sources are read-only: no path may ever arm
   ``origin_writeback_pending`` for them — the flag could never clear
   (production had personal rows stuck with it for weeks).
"""

from __future__ import annotations

from datetime import timezone

import pytest
from dateutil.parser import isoparse

from app.ledger.identity import is_managed_google_event_id
from app.ledger.payload import LOCK_PREFIX
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _norm_start(ev: dict) -> str:
    st = ev.get("start") or {}
    if "dateTime" in st:
        return isoparse(st["dateTime"]).astimezone(UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return st.get("date") or ""


def _main_managed_instance(s: Scenario, start: str) -> dict:
    """The managed main-copy instance whose occurrence starts at ``start``."""
    for ev in s.list_events("main", single_events=True):
        if _norm_start(ev) == start and is_managed_google_event_id(
            ev.get("recurringEventId") or ev.get("id") or ""
        ):
            return ev
    raise AssertionError(f"no managed main instance at {start}")


def _attendee(event: dict, email: str):
    for a in event.get("attendees") or []:
        if a.get("email") == email:
            return a
    return None


def _locked_recurring_series(s: Scenario, cal_nick: str, event_id: str) -> dict:
    """A recurring client meeting alice can NOT edit (organized by
    someone else, no guestsCanModify) where she is an attendee."""
    return s.google.insert_event(s.cal(cal_nick), {
        "id": event_id,
        "summary": "Locked weekly",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "recurrence": ["RRULE:FREQ=WEEKLY;COUNT=4"],
        "organizer": {"email": "boss@example.com"},
        "attendees": [
            {"email": "boss@example.com", "organizer": True,
             "responseStatus": "accepted"},
            {"email": "alice@example.com", "self": True,
             "responseStatus": "accepted"},
        ],
    })


def _locked_single_event(s: Scenario, cal_nick: str, event_id: str) -> dict:
    return s.google.insert_event(s.cal(cal_nick), {
        "id": event_id,
        "summary": "Pending sync",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "boss@example.com"},
        "attendees": [
            {"email": "boss@example.com", "organizer": True,
             "responseStatus": "accepted"},
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "bob@example.com", "responseStatus": "accepted"},
        ],
    })


async def _pending_flag_rows(s: Scenario, user_id: int) -> list[dict]:
    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT id, source_type, canonical_uid FROM ledger_events
            WHERE user_id = ? AND origin_writeback_pending = 1""",
        (user_id,),
    )).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Fix 1 — locked-instance edits must not corrupt the real source event
# ---------------------------------------------------------------------------
async def test_locked_instance_decline_updates_rsvp_without_corrupting_source():
    """Declining ONE occurrence of a locked recurring client meeting on
    main writes back ONLY the RSVP: the source occurrence's title keeps
    no lock emoji, its time is unchanged, alice's responseStatus IS
    updated, and the main copy never accumulates a double lock prefix."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    _locked_recurring_series(s, "client_a", "lockedseries01")
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    inst = _main_managed_instance(s, "2026-02-16T09:00:00Z")
    assert inst["summary"] == LOCK_PREFIX + "Locked weekly", (
        f"precondition: rendered copy carries one lock prefix; "
        f"got {inst['summary']!r}"
    )
    atts = [dict(a) for a in inst.get("attendees") or []]
    assert any(a.get("self") for a in atts), "no self-attendee to RSVP with"
    for a in atts:
        if a.get("self"):
            a["responseStatus"] = "declined"
    s.update_event("main", inst["id"], attendees=atts)

    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # The source occurrence: RSVP updated, EVERYTHING else untouched.
    origin_inst = s.google.get_event(
        s.cal("client_a"), "lockedseries01_20260216T090000Z",
    )
    assert origin_inst.get("summary") == "Locked weekly", (
        f"locked-instance decline corrupted the source title: "
        f"{origin_inst.get('summary')!r}"
    )
    assert LOCK_PREFIX.strip() not in (origin_inst.get("summary") or "")
    assert origin_inst["start"]["dateTime"] == "2026-02-16T09:00:00Z", (
        f"locked-instance decline moved the source occurrence: "
        f"{origin_inst.get('start')}"
    )
    alice = _attendee(origin_inst, "alice@example.com")
    assert alice is not None and alice["responseStatus"] == "declined", (
        f"the RSVP decline did not reach the source occurrence: "
        f"{origin_inst.get('attendees')}"
    )
    boss = _attendee(origin_inst, "boss@example.com")
    assert boss is not None and boss["responseStatus"] == "accepted", (
        f"writeback dropped/altered the organizer's entry: "
        f"{origin_inst.get('attendees')}"
    )
    # The parent series on the source is untouched.
    origin_parent = s.google.get_event(s.cal("client_a"), "lockedseries01")
    assert origin_parent["summary"] == "Locked weekly"
    alice_parent = _attendee(origin_parent, "alice@example.com")
    assert alice_parent is not None
    assert alice_parent["responseStatus"] == "accepted", (
        "instance decline leaked onto the parent series"
    )

    # No double prefix across two more reconciles (the pre-fix bug: the
    # rendered "🔒 " was stored to the ledger, so every re-render
    # prepended another one).
    await s.run_reconciler("alice")
    await s.run_reconciler("alice")
    inst_after = _main_managed_instance(s, "2026-02-16T09:00:00Z")
    assert inst_after["summary"] == LOCK_PREFIX + "Locked weekly", (
        f"main copy accumulated prefixes: {inst_after['summary']!r}"
    )
    # And the source is still clean after the extra passes.
    origin_inst2 = s.google.get_event(
        s.cal("client_a"), "lockedseries01_20260216T090000Z",
    )
    assert origin_inst2.get("summary") == "Locked weekly"
    await s.close()


async def test_locked_instance_drag_reverts_and_never_moves_source():
    """Dragging ONE occurrence of a locked recurring client meeting on
    main is drift: the source occurrence must not move, the main copy
    snaps back to the canonical slot, and no writeback stays armed."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    _locked_recurring_series(s, "client_a", "lockedseries02")
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    inst = _main_managed_instance(s, "2026-02-16T09:00:00Z")
    s.update_event(
        "main", inst["id"],
        start="2026-02-16T14:00:00Z", end="2026-02-16T14:30:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # Source: still exactly the canonical occurrence set, nothing moved.
    src_starts = {
        _norm_start(ev)
        for ev in s.list_events("client_a", single_events=True)
        if ev.get("status") != "cancelled"
    }
    assert "2026-02-16T14:00:00Z" not in src_starts, (
        "dragging a locked occurrence on main MOVED the real source meeting"
    )
    assert "2026-02-16T09:00:00Z" in src_starts

    # Main copy: the drag was reverted (parent-path revert policy).
    main_starts = {
        _norm_start(ev)
        for ev in s.list_events("main", single_events=True)
        if ev.get("status") != "cancelled"
    }
    assert "2026-02-16T09:00:00Z" in main_starts, (
        f"main copy occurrence not re-asserted: {sorted(main_starts)}"
    )
    assert "2026-02-16T14:00:00Z" not in main_starts, (
        f"main copy kept the disallowed drag: {sorted(main_starts)}"
    )

    # No writeback armed for a change that may not propagate.
    assert await _pending_flag_rows(s, user.user_id) == []
    await s.close()


async def test_origin_writeback_payload_is_attendees_only_when_locked():
    """Unit check on the renderer: a non-editable row's writeback patch
    body carries ONLY the attendee/RSVP fields; an editable row still
    carries time + detail (the editable case must not regress)."""
    import json

    from app.ledger.payload import render_payload

    row = {
        "source_type": "client",
        "user_can_edit": 0,
        "user_rsvp_status": "declined",
        "summary": LOCK_PREFIX + "Team sync",
        "description": "junk footer",
        "location": "Room 1",
        "start_at": "2026-02-02T09:00:00Z",
        "end_at": "2026-02-02T09:30:00Z",
        "start_timezone": "UTC",
        "end_timezone": "UTC",
        "is_all_day": 0,
        "attendees_json": json.dumps([
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "bob@example.com", "responseStatus": "accepted"},
        ]),
    }
    body = render_payload(
        desired_state="present_full_rsvp_only", ledger_row=row,
    )
    assert set(body.keys()) == {"attendees"}, (
        f"locked writeback must be attendees-only, got {sorted(body)}"
    )
    self_att = next(a for a in body["attendees"] if a.get("self"))
    assert self_att["responseStatus"] == "declined"

    editable = dict(row, user_can_edit=1, summary="Team sync")
    body2 = render_payload(
        desired_state="present_full_rsvp_only", ledger_row=editable,
    )
    assert {"start", "end", "attendees", "summary",
            "description", "location"} <= set(body2.keys())


async def test_strip_copy_summary_prefixes():
    from app.ledger.payload import strip_copy_summary_prefixes

    assert strip_copy_summary_prefixes(LOCK_PREFIX + "Standup") == "Standup"
    assert strip_copy_summary_prefixes(
        LOCK_PREFIX + LOCK_PREFIX + "Standup"
    ) == "Standup", "a double-lock artifact must strip fully"
    assert strip_copy_summary_prefixes("Standup") == "Standup"
    assert strip_copy_summary_prefixes("") == ""
    assert strip_copy_summary_prefixes(None) is None


# ---------------------------------------------------------------------------
# Fix 2 — a pending RSVP survives a source re-ingest
# ---------------------------------------------------------------------------
async def test_pending_decline_survives_source_edit_before_drain():
    """Alice declines on main; BEFORE the writeback drains, the
    organizer edits the source (time move + another guest's RSVP
    change).  The re-ingest must NOT revert alice's pending decline —
    it still reaches the source — while the organizer's move and the
    other guest's fresh response survive."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    _locked_single_event(s, "client_a", "pendingdecl001")
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    main_copy = s.assert_event_exists(
        "main", summary_contains="Pending sync",
    )
    s.update_event(
        "main", main_copy["id"],
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "declined"}],
    )
    # Ingest + plan + enqueue the writeback, but do NOT drain it —
    # models rate-limit backoff / multi-pass queueing.
    await s.run_reconciler("alice", drain=False)

    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT id, user_rsvp_status, origin_writeback_pending
             FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()
    assert row["user_rsvp_status"] == "declined"
    assert row["origin_writeback_pending"] == 1

    # Organizer edits the SOURCE while the writeback is still queued:
    # moves the meeting, and bob flips to tentative.  Alice's entry on
    # the source is still the stale pre-decline value.
    s.update_event(
        "client_a", "pendingdecl001",
        start="2026-02-02T10:00:00Z", end="2026-02-02T10:30:00Z",
        attendees=[
            {"email": "boss@example.com", "organizer": True,
             "responseStatus": "accepted"},
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "bob@example.com", "responseStatus": "tentative"},
        ],
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=8)

    # The ledger kept the pending decline...
    row = await (await db.execute(
        "SELECT user_rsvp_status FROM ledger_events WHERE id = ?",
        (int(row["id"]),),
    )).fetchone()
    assert row["user_rsvp_status"] == "declined", (
        "source re-ingest erased the un-drained decline from the ledger"
    )
    # ...and the writeback eventually delivered it to the source...
    origin = s.google.get_event(s.cal("client_a"), "pendingdecl001")
    alice = _attendee(origin, "alice@example.com")
    assert alice is not None and alice["responseStatus"] == "declined", (
        f"pending decline never reached the source: "
        f"{origin.get('attendees')}"
    )
    # ...without clobbering the other attendee's fresh response or the
    # organizer's move (the locked writeback carries no time fields).
    bob = _attendee(origin, "bob@example.com")
    assert bob is not None and bob["responseStatus"] == "tentative", (
        f"the merge lost bob's fresh source-side response: "
        f"{origin.get('attendees')}"
    )
    assert origin["start"]["dateTime"] == "2026-02-02T10:00:00Z", (
        f"the writeback reverted the organizer's move: {origin.get('start')}"
    )
    await s.close()


async def test_audit_reingest_preserves_pending_decline():
    """The 10-minute content audit re-reads UNCHANGED source events;
    before the fix that read reverted an un-drained decline (the source
    still carries the old response, so the hashes differ and the row
    was overwritten wholesale)."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    _locked_single_event(s, "client_a", "auditdecl00001")
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    main_copy = s.assert_event_exists(
        "main", summary_contains="Pending sync",
    )
    s.update_event(
        "main", main_copy["id"],
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "declined"}],
    )
    await s.run_reconciler("alice", drain=False)

    db = await s.setup_db()
    led = await (await db.execute(
        """SELECT id FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()

    # Audit pass with the drain still held back: it re-reads the
    # (unchanged, still un-patched) source event.
    await s.run_audit("alice", drain=False)

    row = await (await db.execute(
        """SELECT user_rsvp_status, origin_writeback_pending
             FROM ledger_events WHERE id = ?""",
        (int(led["id"]),),
    )).fetchone()
    assert row["user_rsvp_status"] == "declined", (
        "the content audit erased the un-drained decline"
    )
    assert row["origin_writeback_pending"] == 1

    # Once the drain runs, the decline lands on the source.
    await s.run_reconciler_until_quiescent("alice", max_passes=6)
    origin = s.google.get_event(s.cal("client_a"), "auditdecl00001")
    alice = _attendee(origin, "alice@example.com")
    assert alice is not None and alice["responseStatus"] == "declined"
    bob = _attendee(origin, "bob@example.com")
    assert bob is not None and bob["responseStatus"] == "accepted"
    await s.close()


# ---------------------------------------------------------------------------
# Fix 3 — personal sources must never arm origin_writeback_pending
# ---------------------------------------------------------------------------
async def test_personal_instance_edit_on_main_never_arms_writeback():
    """Dragging or RSVP-ing one occurrence of a personal busy copy on
    main must never arm ``origin_writeback_pending`` (personal
    calendars are read-only — the flag could never clear), must never
    touch the personal source, and the busy copy reverts."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    user = await s.given_user("alice", main="main", personals=["personal_a"])
    s.given_recurring_event(
        "personal_a", summary="Private weekly",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=4",
        event_id="privseries0001",
        attendees=[
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "sam@example.com", "responseStatus": "accepted"},
        ],
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    # Drag the 2026-02-16 busy occurrence on main.
    inst = None
    for ev in s.list_events("main", single_events=True):
        if (
            _norm_start(ev) == "2026-02-16T09:00:00Z"
            and (ev.get("summary") or "").startswith("Busy")
        ):
            inst = ev
            break
    assert inst is not None, "personal busy instance not found on main"
    s.update_event(
        "main", inst["id"],
        start="2026-02-16T14:00:00Z", end="2026-02-16T14:30:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # Never armed — for ANY row.
    assert await _pending_flag_rows(s, user.user_id) == [], (
        "a personal-sourced row armed origin_writeback_pending — the "
        "flag can never clear for a read-only source"
    )

    # The personal source is untouched.
    src_starts = {
        _norm_start(ev)
        for ev in s.list_events("personal_a", single_events=True)
        if ev.get("status") != "cancelled"
    }
    assert "2026-02-16T14:00:00Z" not in src_starts, (
        "a main-side busy drag reached the read-only personal source"
    )
    assert "2026-02-16T09:00:00Z" in src_starts
    origin = s.google.get_event(s.cal("personal_a"), "privseries0001")
    assert origin["summary"] == "Private weekly"

    # And the main busy copy reverts to the canonical slot.
    main_starts = {
        _norm_start(ev)
        for ev in s.list_events("main", single_events=True)
        if ev.get("status") != "cancelled"
    }
    assert "2026-02-16T09:00:00Z" in main_starts
    assert "2026-02-16T14:00:00Z" not in main_starts, (
        f"personal busy drag was not reverted on main: {sorted(main_starts)}"
    )

    # Second vector: an RSVP scribbled onto the busy PARENT copy on
    # main (the parent path) must not arm the flag either.
    parent_copy = next(
        e for e in s.list_events("main") if e.get("recurrence")
    )
    s.update_event(
        "main", parent_copy["id"],
        attendees=[{"email": "alice@example.com", "self": True,
                    "responseStatus": "declined"}],
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=6)
    assert await _pending_flag_rows(s, user.user_id) == []
    origin = s.google.get_event(s.cal("personal_a"), "privseries0001")
    alice = _attendee(origin, "alice@example.com")
    assert alice is not None and alice["responseStatus"] == "needsAction", (
        "a busy-copy RSVP was written back to the read-only personal source"
    )
    await s.close()
