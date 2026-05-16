"""Idempotency properties (REWRITE_PLAN.md §14 Layer 2).

Ingesting the same source state twice — or reconciling an already
quiescent system again — must not duplicate ledger rows, bump
versions, re-render projections, or create extra events on Google.
This is the property the deterministic-ID + content-hash design
exists to guarantee.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _snapshot(s: Scenario, user_id: int) -> tuple[list, list]:
    """A stable snapshot of the ledger + projections (the columns
    that a no-op reconcile must not change — last_seen_at is
    deliberately excluded since ingest always refreshes it)."""
    db = await s.setup_db()
    events = await (await db.execute(
        """SELECT id, canonical_uid, parent_canonical_uid, version, status
             FROM ledger_events WHERE user_id = ? ORDER BY id""",
        (user_id,),
    )).fetchall()
    projections = await (await db.execute(
        """SELECT p.id, p.target_kind, p.target_calendar_id,
                  p.desired_state, p.current_state, p.google_event_id,
                  p.desired_ledger_version, p.applied_ledger_version,
                  p.desired_payload_hash, p.applied_payload_hash
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ? ORDER BY p.id""",
        (user_id,),
    )).fetchall()
    return [dict(r) for r in events], [dict(r) for r in projections]


async def test_reconciling_a_quiescent_system_is_a_noop():
    """Once the system has converged, further reconciles with no
    source changes leave the ledger, the projections, and Google
    byte-for-byte unchanged."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    for i in range(6):
        s.given_event(
            "client_a", summary=f"E{i}",
            start=f"2026-03-{i + 1:02d}T09:00:00Z",
        )
    s.given_recurring_event(
        "client_b", summary="Weekly",
        start="2026-03-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="recur00000001",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    events_before, projections_before = await _snapshot(s, user.user_id)
    main_before = s.google.event_count(s.cal("main"))
    busy_a_before = s.google.event_count(s.cal("client_a"))
    busy_b_before = s.google.event_count(s.cal("client_b"))

    # Reconcile twice more — no source changes in between.
    await s.run_reconciler("alice")
    await s.run_reconciler("alice")

    events_after, projections_after = await _snapshot(s, user.user_id)
    assert events_after == events_before, (
        "ledger rows or versions changed on a no-op reconcile"
    )
    assert projections_after == projections_before, (
        "projections changed on a no-op reconcile"
    )
    assert s.google.event_count(s.cal("main")) == main_before
    assert s.google.event_count(s.cal("client_a")) == busy_a_before
    assert s.google.event_count(s.cal("client_b")) == busy_b_before
    await s.close()


async def test_double_reconcile_after_each_change_never_duplicates():
    """Reconciling twice after every individual change — the
    pattern a flaky webhook delivery produces — yields exactly one
    main copy per source event, never two."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])

    for i in range(5):
        s.given_event(
            "client_a", summary=f"Evt{i}",
            start=f"2026-04-{i + 1:02d}T10:00:00Z",
        )
        # Reconcile twice for every single new event.
        await s.run_reconciler("alice")
        await s.run_reconciler("alice")

    for i in range(5):
        copies = s.find_events("main", summary=f"Evt{i}")
        assert len(copies) == 1, (
            f"Evt{i}: expected exactly one main copy, got {len(copies)}"
        )
    assert s.google.event_count(s.cal("main")) == 5
    await s.close()
