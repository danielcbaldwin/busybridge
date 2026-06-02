"""Phase 2 — main-side managed recurring instance cancellation.

When the user cancels a single occurrence of one of our managed
recurring main copies, Option A says propagate it: that one
occurrence is destructively deleted on the real source calendar and
every peer busy block for it is removed.  It must never delete the
parent series, never flag the whole series as intentionally deleted,
and a cancellation ingested FROM the source must not trigger a fresh
destructive delete back at the source.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _occurrence_starts(scenario: Scenario, calendar: str) -> set[str]:
    """The occurrence dates currently present (not cancelled) on a
    calendar, expanded."""
    out: set[str] = set()
    for ev in scenario.list_events(calendar, single_events=True):
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp:
            out.add(stamp[:10])
    return out


def _instance_id_for(scenario: Scenario, calendar: str, ymd: str) -> str:
    for ev in scenario.list_events(calendar, single_events=True):
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp.startswith(ymd):
            return ev["id"]
    raise AssertionError(f"no occurrence on {ymd} on {calendar!r}")


async def _mirrored_series(s: Scenario):
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


_CONSERVATIVE_FIX = pytest.mark.xfail(
    reason=(
        "Destructive main-side->source occurrence-delete propagation is "
        "temporarily disabled by the 2026-06-02 conservative data-loss fix "
        "(it deleted ~170 real source occurrences in an _R loop). The proper "
        "_R fix will re-enable it for genuine user actions; these tests flip "
        "back to passing then."
    ),
    strict=False,
)


@_CONSERVATIVE_FIX
async def test_main_side_instance_cancel_deletes_source_and_peer():
    s = Scenario()
    user, series = await _mirrored_series(s)

    # The user deletes the 2026-02-16 occurrence on the managed copy.
    inst_id = _instance_id_for(s, "main", "2026-02-16")
    s.cancel_event("main", inst_id)

    await s.run_reconciler("alice")

    # The source occurrence on client_a is gone — exactly that one.
    src = _occurrence_starts(s, "client_a")
    assert "2026-02-16" not in src, "source occurrence was not deleted"
    assert {"2026-02-02", "2026-02-09", "2026-02-23"} <= src, (
        "an unrelated source occurrence was lost"
    )

    # The peer busy block for that occurrence is gone too.
    peer = _occurrence_starts(s, "client_b")
    assert "2026-02-16" not in peer
    assert "2026-02-09" in peer and "2026-02-23" in peer

    db = await s.setup_db()

    # The instance row is cancelled; the parent series is untouched —
    # NOT flagged intentionally-deleted, still active.
    parent = await (await db.execute(
        """SELECT status, user_intentionally_deleted FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'
              AND parent_canonical_uid IS NULL""",
        (user.user_id,),
    )).fetchone()
    assert parent["status"] == "active"
    assert not parent["user_intentionally_deleted"], (
        "cancelling one occurrence must not delete the whole series"
    )

    inst = await (await db.execute(
        """SELECT status, parent_canonical_uid, source_delete_pending
             FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchone()
    assert inst["status"] == "cancelled"
    # The destructive delete has drained — the flag is cleared.
    assert not inst["source_delete_pending"]

    # No phantom native row.
    native = await (await db.execute(
        """SELECT COUNT(*) AS n FROM ledger_events
            WHERE user_id = ? AND source_type = 'main_native'""",
        (user.user_id,),
    )).fetchone()
    assert native["n"] == 0
    await s.close()


@_CONSERVATIVE_FIX
async def test_main_side_instance_cancel_converges_without_redelete():
    """Re-reconcile after the destructive delete: the source-side
    re-ingest of the now-cancelled occurrence must not churn the row
    or arm a second destructive delete."""
    s = Scenario()
    user, series = await _mirrored_series(s)

    inst_id = _instance_id_for(s, "main", "2026-02-16")
    s.cancel_event("main", inst_id)
    await s.run_reconciler("alice")

    db = await s.setup_db()
    first = await (await db.execute(
        """SELECT id, version FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchone()

    await s.run_reconciler("alice")
    await s.run_reconciler("alice")

    rows = await (await db.execute(
        """SELECT id, version, source_delete_pending FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchall()
    assert len(rows) == 1, "the instance row was duplicated"
    assert rows[0]["id"] == first["id"]
    assert rows[0]["version"] == first["version"], "instance row churned"
    assert not rows[0]["source_delete_pending"]
    # The occurrence stays gone everywhere.
    assert "2026-02-16" not in _occurrence_starts(s, "client_a")
    assert "2026-02-16" not in _occurrence_starts(s, "client_b")
    await s.close()


async def test_main_side_cancel_does_not_destructively_delete_source():
    """CONSERVATIVE data-loss fix (2026-06-02): cancelling one occurrence
    of a managed recurring copy on main must NOT destructively delete the
    real source occurrence, and must never arm source_delete_pending. The
    occurrence remains on the authoritative source calendar; only the
    mirror copies are affected. (The proper _R fix will restore safe
    propagation for genuine user actions.)"""
    s = Scenario()
    user, series = await _mirrored_series(s)

    inst_id = _instance_id_for(s, "main", "2026-02-16")
    s.cancel_event("main", inst_id)
    await s.run_reconciler("alice")
    await s.run_reconciler("alice")  # re-reconcile: still no source delete

    # The real source occurrence is NOT deleted — BusyBridge must not
    # reach over and delete the authoritative source calendar.
    assert "2026-02-16" in _occurrence_starts(s, "client_a"), (
        "conservative fix: the source occurrence must survive"
    )

    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT source_delete_pending FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchall()
    assert rows, "expected an instance row"
    assert all(not r["source_delete_pending"] for r in rows), (
        "no destructive source delete may be armed"
    )
    await s.close()


async def test_source_side_instance_cancel_does_not_arm_destructive_delete():
    """A cancellation made ON the source calendar removes the peer
    copies but must NOT arm a destructive delete back at the source
    (the source already did the delete itself)."""
    s = Scenario()
    user, series = await _mirrored_series(s)

    # Cancel the occurrence on the SOURCE calendar directly.
    inst_id = _instance_id_for(s, "client_a", "2026-02-16")
    s.cancel_event("client_a", inst_id)
    await s.run_reconciler("alice")

    db = await s.setup_db()
    inst = await (await db.execute(
        """SELECT status, source_delete_pending FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid IS NOT NULL""",
        (user.user_id,),
    )).fetchone()
    assert inst["status"] == "cancelled"
    assert not inst["source_delete_pending"], (
        "a source-ingested cancellation must not arm a destructive delete"
    )
    # The peer busy block was still removed.
    assert "2026-02-16" not in _occurrence_starts(s, "client_b")
    await s.close()


@_CONSERVATIVE_FIX
async def test_move_then_cancel_one_instance_deletes_source_occurrence():
    """The user moves an occurrence (Phase 1), then later cancels that
    same occurrence — the materialised instance must still drive a
    destructive source delete, not a whole-series deletion."""
    s = Scenario()
    user, series = await _mirrored_series(s)

    inst_id = _instance_id_for(s, "main", "2026-02-16")
    s.update_event("main", inst_id, start="2026-02-16T14:00:00Z")
    await s.run_reconciler("alice")

    # Now cancel that (already moved) occurrence on the managed copy.
    moved_id = _instance_id_for(s, "main", "2026-02-16")
    s.cancel_event("main", moved_id)
    await s.run_reconciler("alice")

    assert "2026-02-16" not in _occurrence_starts(s, "client_a")
    assert "2026-02-16" not in _occurrence_starts(s, "client_b")

    db = await s.setup_db()
    parent = await (await db.execute(
        """SELECT status, user_intentionally_deleted FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'
              AND parent_canonical_uid IS NULL""",
        (user.user_id,),
    )).fetchone()
    assert parent["status"] == "active"
    assert not parent["user_intentionally_deleted"]
    await s.close()
