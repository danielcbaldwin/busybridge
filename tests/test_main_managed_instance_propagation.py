"""Phase 1 — main-side managed recurring instance move/edit.

When the user drags or edits a single occurrence of one of our
managed recurring main copies, the change must be mapped back to the
SOURCE series and propagated everywhere:
the source occurrence moves, every peer busy block moves, and no
phantom ``main_native`` row is minted.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _instance_id_for(scenario: Scenario, calendar: str, ymd: str) -> str:
    """The expanded-instance event id whose occurrence date is ``ymd``."""
    for ev in scenario.list_events(calendar, single_events=True):
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp.startswith(ymd):
            return ev["id"]
    raise AssertionError(f"no occurrence on {ymd} on {calendar!r}")


async def _setup_mirrored_series(s: Scenario):
    """A weekly client recurring event mirrored to main + a peer."""
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    series = s.given_recurring_event(
        "client_a",
        summary="Team sync",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=6",
    )
    await s.run_reconciler("alice")
    return user, series


async def test_main_side_instance_move_propagates_to_source_and_peers():
    s = Scenario()
    user, series = await _setup_mirrored_series(s)

    # The managed recurring copy on main.
    main_copy = next(
        e for e in s.list_events("main") if e.get("recurrence")
    )

    # The user drags the 2026-02-16 occurrence on the main copy from
    # 09:00 to 14:00.
    inst_id = _instance_id_for(s, "main", "2026-02-16")
    s.update_event("main", inst_id, start="2026-02-16T14:00:00Z")

    await s.run_reconciler("alice")

    db = await s.setup_db()

    # A source-parented instance ledger row exists — NOT a main_native
    # row, and NOT keyed by the main-copy instance id.
    inst = await (await db.execute(
        """SELECT source_type, parent_canonical_uid, status,
                  start_at, source_event_id, canonical_uid
             FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchall()
    assert len(inst) == 1, f"expected one instance row, got {inst}"
    row = inst[0]
    assert row["source_type"] == "client"
    assert row["status"] == "active"
    assert row["start_at"] == "2026-02-16T14:00:00Z"
    # The main-copy instance id must NOT be stored as the source id.
    assert not (row["source_event_id"] or "").startswith(main_copy["id"])

    native = await (await db.execute(
        """SELECT COUNT(*) AS n FROM ledger_events
            WHERE user_id = ? AND source_type = 'main_native'""",
        (user.user_id,),
    )).fetchone()
    assert native["n"] == 0, "a managed-copy edit must not mint a native row"

    # The source occurrence on client_a moved to 14:00.
    src = _instance_start(s, "client_a", "2026-02-16")
    assert src.startswith("2026-02-16T14:00:00")

    # The peer busy block moved too.
    peer = _instance_start(s, "client_b", "2026-02-16")
    assert peer.startswith("2026-02-16T14:00:00")

    # Every other occurrence is untouched (still 09:00).
    assert _instance_start(s, "client_a", "2026-02-09").startswith(
        "2026-02-09T09:00:00"
    )
    assert _instance_start(s, "client_b", "2026-02-23").startswith(
        "2026-02-23T09:00:00"
    )
    await s.close()


def _instance_start(scenario: Scenario, calendar: str, ymd: str) -> str:
    for ev in scenario.list_events(calendar, single_events=True):
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp.startswith(ymd):
            return stamp
    raise AssertionError(f"no occurrence on {ymd} on {calendar!r}")


async def test_main_side_instance_move_converges_without_churn():
    s = Scenario()
    user, series = await _setup_mirrored_series(s)

    inst_id = _instance_id_for(s, "main", "2026-02-16")
    s.update_event("main", inst_id, start="2026-02-16T14:00:00Z")
    await s.run_reconciler("alice")

    db = await s.setup_db()
    after_first = await (await db.execute(
        """SELECT id, version FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchone()

    # Further reconcile passes (which re-ingest the source exception
    # the writeback just created) must not churn the instance row or
    # duplicate it.
    await s.run_reconciler("alice")
    await s.run_reconciler("alice")

    rows = await (await db.execute(
        """SELECT id, version, origin_writeback_pending FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchall()
    assert len(rows) == 1, "the instance row was duplicated"
    assert rows[0]["id"] == after_first["id"]
    assert rows[0]["version"] == after_first["version"], "instance row churned"
    assert not rows[0]["origin_writeback_pending"], "writeback flag stuck"
    await s.close()


async def test_unmappable_managed_instance_is_skipped_without_phantom():
    """An instance whose managed parent id maps to no projection
    cannot be propagated — it must be skipped, never minted as a
    native row."""
    from app.ledger.ingest.main import _ingest_one_main_event

    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("bob", main="main")
    db = await s.setup_db()

    event = {
        "id": "bb" + "a" * 13 + "_20260315T090000Z",
        "status": "confirmed",
        "recurringEventId": "bb" + "a" * 13,
        "start": {"dateTime": "2026-03-15T11:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-15T12:00:00Z", "timeZone": "UTC"},
        "summary": "orphaned managed instance",
    }
    outcome, ledger_id = await _ingest_one_main_event(
        db, user_id=user.user_id, user_email="bob@example.com", event=event,
    )
    assert outcome == "our_writes_skipped"
    assert ledger_id is None
    n = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?",
        (user.user_id,),
    )).fetchone()
    assert n["n"] == 0
    await s.close()
