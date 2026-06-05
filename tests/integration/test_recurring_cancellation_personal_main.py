"""Recurring-cancellation amnesia: personal + native-main coverage.

The headline bug the rewrite kills is "an instance cancelled on a
source calendar during a sync-token gap is silently lost, leaving a
ghost busy block on the targets".

``tests/soak/test_soak_recurring_365.py`` proves this is fixed for
*client* calendars.  These two tests prove the same for the other
two source types — personal calendars and native main events —
which now route recurring instances through the shared instance
handler and run the same full-sync recovery scan.

The exercised sequence is the genuine amnesia path:

  1. reconcile        -- project the recurring series onto targets
  2. cancel one instance on the source
  3. expire the source's sync token  -- the cancellation is now
     INVISIBLE to the next incremental sync
  4. reconcile        -- the full sync omits the cancelled
     exception; only the events.instances(showDeleted=True)
     recovery scan can surface it

Without the recovery scan step 4 leaves a ghost; with it the
cancelled date is absent on every projected copy.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _proj_parent_id(s, user_id, target_kind, target_calendar_id=None):
    """``google_event_id`` of the recurring-parent projection for one target."""
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT p.google_event_id
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.target_kind = ?
              AND COALESCE(p.target_calendar_id, -1) = COALESCE(?, -1)
              AND e.parent_canonical_uid IS NULL
              AND e.is_recurring = 1
              AND p.google_event_id IS NOT NULL""",
        (user_id, target_kind, target_calendar_id),
    )).fetchone()
    return row["google_event_id"] if row else None


def _cancelled_dates(s, nick, parent_id):
    """Dates (YYYY-MM-DD) cancelled on the projected copy of a series."""
    insts = s.google.list_instances(s.cal(nick), parent_id, show_deleted=True)
    out = set()
    for i in insts["items"]:
        if i.get("status") != "cancelled":
            continue
        ost = i.get("originalStartTime") or {}
        dt = ost.get("dateTime") or ost.get("date") or ""
        if dt:
            out.add(dt[:10])
    return out


def _confirmed_dates(s, nick, parent_id):
    """Dates still confirmed on the projected copy of a series."""
    insts = s.google.list_instances(s.cal(nick), parent_id, show_deleted=True)
    out = set()
    for i in insts["items"]:
        if i.get("status") == "cancelled":
            continue
        start = i.get("start") or {}
        dt = start.get("dateTime") or start.get("date") or ""
        if dt:
            out.add(dt[:10])
    return out


async def test_personal_recurring_cancellation_recovered_after_token_expiry():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("personal_a")
    user = await s.given_user(
        "alice", main="main",
        clients=["client_a"], personals=["personal_a"],
    )
    s.given_recurring_event(
        "personal_a",
        summary="Therapy",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=6;BYDAY=MO",
        event_id="personalrec001",
    )
    await s.run_reconciler("alice")

    # A personal event projects 'Busy (personal)' onto main + clients.
    main_pid = await _proj_parent_id(s, user.user_id, "main")
    client_pid = await _proj_parent_id(
        s, user.user_id, "client", user.client_calendar_ids["client_a"],
    )
    assert main_pid and client_pid

    # Cancel the 2026-02-16 instance on the personal source, then
    # expire the sync token BEFORE any reconcile observes it.
    s.google.delete_event(
        s.cal("personal_a"), "personalrec001_20260216T090000Z",
    )
    db = await s.setup_db()
    state = await (await db.execute(
        "SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id = ?",
        (user.personal_calendar_ids["personal_a"],),
    )).fetchone()
    assert state and state["sync_token"]
    s.google.expire_sync_token(state["sync_token"])

    # Full sync omits the cancelled exception; only the recovery
    # scan can surface it.
    await s.run_reconciler_until_quiescent("alice", max_passes=3)

    for nick, pid in (("main", main_pid), ("client_a", client_pid)):
        assert "2026-02-16" in _cancelled_dates(s, nick, pid), (
            f"ghost instance on {nick}: 2026-02-16 cancelled on the "
            f"personal source but still live on the projected copy"
        )
        assert "2026-02-16" not in _confirmed_dates(s, nick, pid)
    await s.close()


async def test_main_native_recurring_cancellation_recovered_after_token_expiry():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    s.given_recurring_event(
        "main",
        summary="Native main standup",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=6;BYDAY=MO",
        event_id="mainnativerec1",
    )
    await s.run_reconciler("alice")

    # A native main recurring event projects a busy block onto peers.
    client_pid = await _proj_parent_id(
        s, user.user_id, "client", user.client_calendar_ids["client_a"],
    )
    assert client_pid

    # Cancel the 2026-02-16 instance on main, expire main's sync
    # token, then reconcile.
    s.google.delete_event(s.cal("main"), "mainnativerec1_20260216T090000Z")
    db = await s.setup_db()
    state = await (await db.execute(
        "SELECT sync_token FROM main_calendar_sync_state WHERE user_id = ?",
        (user.user_id,),
    )).fetchone()
    assert state and state["sync_token"]
    s.google.expire_sync_token(state["sync_token"])

    await s.run_reconciler_until_quiescent("alice", max_passes=3)

    assert "2026-02-16" in _cancelled_dates(s, "client_a", client_pid), (
        "ghost instance on client_a: 2026-02-16 cancelled on the native "
        "main series but still live on the projected busy block"
    )
    assert "2026-02-16" not in _confirmed_dates(s, "client_a", client_pid)
    await s.close()
