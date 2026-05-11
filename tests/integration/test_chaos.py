"""Layer 4 chaos / concurrency tests (REWRITE_PLAN.md §14).

The ledger architecture is designed so concurrent webhooks, mid-
write crashes, sync-token expiry races, etc. cannot produce
divergent state.  These tests deliberately inject adversarial
timing and verify the system converges to the same answer.

The single-process single-DB nature of the app means we don't
exercise true OS-level concurrency; instead we interleave
deterministic operations against a shared in-memory state and
assert structural invariants hold.
"""

from __future__ import annotations

import random

import pytest

from tests.fakes.failures import NetworkError
from tests.fakes.google_calendar import GoogleApiError
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _user_with_clients() -> Scenario:
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    return s


# ---------------------------------------------------------------------------
# Crash mid-write recovery
# ---------------------------------------------------------------------------
async def test_n_mid_write_crashes_in_a_row_still_converge():
    """Force ``mid_write_crash`` on every operation for the first
    few attempts.  The deterministic ID + 409-as-success path
    must eventually converge to a single main copy."""
    s = await _user_with_clients()
    s.given_event("client_a", summary="Crash N times", start="2026-04-01T09:00:00Z")

    # Force crash on every write for 5 reconciler passes.
    for _ in range(5):
        s.failures.force_next_crash_after_write()
        await s.run_reconciler("alice")

    # Now let the world calm down and converge.
    await s.run_reconciler_until_quiescent("alice", max_passes=10)

    main_copies = s.find_events("main", summary="Crash N times")
    assert len(main_copies) == 1, (
        f"expected exactly one main copy after 5 crashes; got {len(main_copies)}"
    )
    # Peer client_b also has exactly one busy block.
    busy_b = s.find_events("client_b", summary="Busy")
    assert len(busy_b) == 1
    await s.close()


# ---------------------------------------------------------------------------
# Two concurrent triggers (webhook + manual) → one logical pass
# ---------------------------------------------------------------------------
async def test_two_webhooks_within_debounce_window_collapse_to_one_run():
    """Multiple notifications inside the debounce window must
    not produce duplicate writes.

    NB: ``enqueue_webhook`` uses wall-clock ``datetime.now(UTC)``
    for the ``scheduled_for`` field (production semantics), so we
    can't validate the debounce window timing here — only the
    coalescing.  Five enqueues collapse to one row, and a single
    drain produces one main copy.
    """
    from app.ledger.triggers import enqueue_webhook
    s = await _user_with_clients()
    s.given_event("client_a", summary="Burst", start="2026-04-01T09:00:00Z")

    db = await s.setup_db()
    user_id = s.user("alice").user_id

    # Five webhooks fire in rapid succession.
    for _ in range(5):
        await enqueue_webhook(db, user_id=user_id, source_hint="client:1")

    # There's a single row in reconcile_requests (coalesced).
    row_count = (await (await db.execute(
        "SELECT COUNT(*) AS n FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone())["n"]
    assert row_count == 1

    # Drive the reconciler to convergence (bypassing the queue's
    # scheduled_for; the framework calls reconcile_user directly).
    await s.run_reconciler("alice")
    main_copies = s.find_events("main", summary="Burst")
    assert len(main_copies) == 1
    await s.close()


# ---------------------------------------------------------------------------
# Sync-token expiry mid-flight
# ---------------------------------------------------------------------------
async def test_sync_token_expires_between_ingest_and_drain():
    """Sync token expires during a reconcile pass.  Outbox must
    still drain pending ops; next pass falls back to full sync
    without losing the projection state."""
    from datetime import timedelta
    s = Scenario(sync_token_ttl=timedelta(hours=1))
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_event("client_a", summary="A", start="2026-04-01T09:00:00Z")
    await s.run_reconciler("alice")
    s.assert_event_exists("main", summary="A")

    # Token expires.
    s.advance(timedelta(hours=2))
    # Add a new source event.
    s.given_event("client_a", summary="B", start="2026-04-02T09:00:00Z")
    await s.run_reconciler("alice")

    # Both events present on main; neither duplicated.
    s.assert_event_exists("main", summary="A")
    s.assert_event_exists("main", summary="B")
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 2
    await s.close()


# ---------------------------------------------------------------------------
# Network jitter over a burst of operations
# ---------------------------------------------------------------------------
async def test_targeted_failures_recover_with_retry():
    """Inject specific failures via ``force_next`` (deterministic)
    and verify the system recovers to the intended state on the
    next pass.  Each forced error fires exactly once; the next
    reconcile re-attempts.

    The outbox backoff uses wall-clock time, so we don't try to
    simulate slow recovery; we force one transient failure at a
    time and verify the retry semantics in isolation.
    """
    s = await _user_with_clients()
    for i in range(3):
        s.given_event(
            "client_a",
            summary=f"E{i}",
            start=f"2026-04-{i+1:02d}T09:00:00Z",
        )

    # First pass: force a 503 on the very first API call.  The
    # reconciler catches it and continues; remaining work succeeds.
    s.failures.force_next(GoogleApiError(503, "Service Unavailable", "transient"))
    await s.run_reconciler("alice")

    # Second pass: force a network error mid-stream.
    s.failures.force_next(NetworkError("transient flap"))
    await s.run_reconciler("alice")

    # Final clean pass.
    await s.run_reconciler_until_quiescent(
        "alice", max_passes=3, advance_between_passes=1,
    )

    main = s.google.event_count(s.cal("main"), include_cancelled=False)
    assert main == 3, f"expected 3 main copies, got {main}"
    await s.close()


# ---------------------------------------------------------------------------
# Rapid edit ping-pong (drift revert)
# ---------------------------------------------------------------------------
async def test_rapid_main_drift_converges_without_pingpong():
    """User keeps dragging a non-editable event on main; the
    revert mechanism must converge and stop the loop."""
    s = await _user_with_clients()
    src = s.given_event(
        "client_a",
        summary="Quarterly",
        start="2026-04-01T09:00:00Z",
        attendees=[
            {"email": "alice@example.com", "responseStatus": "accepted", "self": True},
        ],
    )
    s.google.patch_event(s.cal("client_a"), src["id"], {
        "organizer": {"email": "boss@example.com"},
        "guestsCanModify": False,
    })
    await s.run_reconciler("alice")
    main_copy = s.assert_event_exists("main", summary_contains="Quarterly")

    # Drag, reconcile, drag, reconcile — 5 times.
    for offset_hours in (1, 2, 3, 4, 5):
        s.google.patch_event(s.cal("main"), main_copy["id"], {
            "start": {"dateTime": f"2026-04-01T{9+offset_hours:02d}:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": f"2026-04-01T{9+offset_hours:02d}:30:00Z", "timeZone": "UTC"},
        })
        await s.run_reconciler("alice")

    # After the dust settles: event back to 09:00.
    after = s.google.get_event(s.cal("main"), main_copy["id"])
    assert after["start"]["dateTime"] == "2026-04-01T09:00:00Z", (
        f"expected revert to 09:00; got {after['start']['dateTime']}"
    )
    await s.close()


# ---------------------------------------------------------------------------
# Recurring cancellation under sync-token churn
# ---------------------------------------------------------------------------
async def test_recurring_cancellation_survives_repeated_token_expiry():
    """The recurring-cancellation amnesia regression: cancel one
    instance, then churn the sync token repeatedly.  The
    cancellation must remain visible on main throughout."""
    from datetime import timedelta
    s = Scenario(sync_token_ttl=timedelta(hours=1))
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_recurring_event(
        "client_a",
        summary="Standup",
        start="2026-04-06T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=6;BYDAY=MO",
        event_id="bbstand0000001",
    )
    await s.run_reconciler("alice")

    # Cancel the 2026-04-20 instance.
    s.google.delete_event(s.cal("client_a"), "bbstand0000001_20260420T090000Z")
    await s.run_reconciler("alice")

    # Find the parent on main.
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT p.google_event_id FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ? AND p.target_kind = 'main'
              AND e.parent_canonical_uid IS NULL""",
        (s.user("alice").user_id,),
    )).fetchone()
    parent_main_id = row["google_event_id"]

    # Churn through 5 full-sync cycles.
    for _ in range(5):
        s.advance(timedelta(hours=2))
        await s.run_reconciler("alice")

    # Cancellation still visible.
    insts = s.google.list_instances(
        s.cal("main"), parent_main_id, show_deleted=True,
    )
    cancelled = [
        i for i in insts["items"]
        if i["status"] == "cancelled"
        and i.get("originalStartTime", {}).get("dateTime", "").startswith("2026-04-20")
    ]
    assert len(cancelled) >= 1
    await s.close()
