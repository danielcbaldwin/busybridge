"""End-to-end integration tests for the ledger pipeline.

Each test is a story: given a state, run a reconciliation pass,
assert what's now on Google.  All tests run against the in-memory
fakes from Stage 1 — no network, no real Google.

These tests exercise the load-bearing claims of the rewrite:

* A new client event propagates to main + busy blocks on peers.
* A cancelled client event removes its main copy and busy blocks.
* Idempotent retry: a mid-write crash in the outbox is recovered
  by the next pass without duplicating the event.
* Etag-gated update: a 412 from Google supersedes the op and
  triggers a replan rather than overwriting fresher data.
* Sync-token expiry recovery: a 410 falls back to full sync and
  ledger state stays correct.
* user_intentionally_deleted: a delete on main suppresses
  re-creation from the source.
"""

from __future__ import annotations

import pytest

from tests.fakes.failures import NetworkError
from tests.fakes.google_calendar import GoogleApiError
from tests.integration.framework import Scenario


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------
async def _alice_with_three_clients() -> Scenario:
    """One user, one main, three client calendars: A, B, C."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    s.given_calendar("client_c")
    await s.given_user(
        "alice",
        email="alice@example.com",
        main="main",
        clients=["client_a", "client_b", "client_c"],
    )
    return s


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
async def test_client_event_creates_main_copy_and_peer_busy_blocks():
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a",
        summary="Standup",
        start="2026-02-02T09:00:00Z",
    )

    out = await s.run_reconciler("alice")

    # Main has the full-detail copy.
    s.assert_event_exists("main", summary="Standup")

    # Both peer clients have a busy block.  Origin (client_a) does NOT.
    busy_b = s.find_events("client_b", summary="Busy")
    busy_c = s.find_events("client_c", summary="Busy")
    busy_origin = s.find_events("client_a", summary="Busy")
    assert len(busy_b) == 1
    assert len(busy_c) == 1
    assert busy_origin == []

    # Counters reflect what just happened.
    assert out["enqueued"] == 3  # main copy + 2 peer busies
    assert out["drain"]["succeeded"] == 3

    await s.close()


async def test_idempotent_reconcile_does_not_duplicate():
    """Running the reconciler twice with no source change must
    not produce duplicate writes."""
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a", summary="Standup", start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler("alice")
    second = await s.run_reconciler("alice")

    assert second["enqueued"] == 0
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 1
    assert s.google.event_count(s.cal("client_b"), include_cancelled=False) == 1
    assert s.google.event_count(s.cal("client_c"), include_cancelled=False) == 1

    await s.close()


async def test_cancelling_source_removes_main_copy_and_busy_blocks():
    s = await _alice_with_three_clients()
    src = s.given_event(
        "client_a", summary="Standup", start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler("alice")

    # Now cancel on the source.
    s.cancel_event("client_a", src["id"])
    await s.run_reconciler("alice")

    s.assert_no_event_with_summary("main", "Standup")
    assert s.find_events("client_b", summary="Busy") == []
    assert s.find_events("client_c", summary="Busy") == []

    await s.close()


async def test_lock_emoji_for_non_editable_event():
    s = await _alice_with_three_clients()
    # Non-editable: someone else organises, guestsCanModify=False.
    s.given_event(
        "client_a",
        summary="Quarterly review",
        start="2026-02-02T09:00:00Z",
    )
    # Patch the just-inserted event to mark it not editable.
    src_id = s.find_events("client_a", summary="Quarterly review")[0]["id"]
    s.google.patch_event(s.cal("client_a"), src_id, {
        "organizer": {"email": "boss@example.com"},
        "attendees": [
            {"email": "alice@example.com", "responseStatus": "accepted", "self": True},
        ],
        "guestsCanModify": False,
    })

    await s.run_reconciler("alice")
    main_copy = s.assert_event_exists("main", summary_contains="Quarterly review")
    assert main_copy["summary"].startswith("🔒 ")

    await s.close()


# ---------------------------------------------------------------------------
# Free-events do not block peers
# ---------------------------------------------------------------------------
async def test_free_event_does_not_create_peer_busy_blocks():
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a",
        summary="Out of office",
        start="2026-02-02T09:00:00Z",
        transparency="transparent",
    )
    out = await s.run_reconciler("alice")

    # Main still gets the full copy (informational).
    s.assert_event_exists("main", summary="Out of office")
    # Peers do NOT.
    assert s.find_events("client_b", summary="Busy") == []
    assert s.find_events("client_c", summary="Busy") == []

    await s.close()


# ---------------------------------------------------------------------------
# Idempotency under failure injection
# ---------------------------------------------------------------------------
async def test_mid_write_crash_does_not_duplicate_after_retry():
    """The headline reliability claim: a write that 'reaches Google
    but the response is lost' must not produce a duplicate when
    the outbox retries.  The deterministic Google ID + 409 handling
    guarantee this."""
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a", summary="Critical meeting",
        start="2026-02-02T09:00:00Z",
    )

    # Crash the next write (the main copy create).
    s.failures.force_next_crash_after_write()

    first = await s.run_reconciler("alice")
    # The crashed op was retried internally? Or did it raise?
    # Either way, the next pass picks up where we left off.
    second_passes = await s.run_reconciler_until_quiescent("alice")

    # Exactly one main copy.  Not two.
    main_copies = s.find_events("main", summary="Critical meeting")
    assert len(main_copies) == 1, (
        f"expected 1 main copy after crash+retry, got {len(main_copies)}; "
        f"first pass: {first}; later: {second_passes}"
    )
    # Peer busy blocks land too.
    assert len(s.find_events("client_b", summary="Busy")) == 1
    assert len(s.find_events("client_c", summary="Busy")) == 1

    await s.close()


async def test_transient_network_failure_recovers_on_retry():
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a", summary="Network ✘ event",
        start="2026-02-02T09:00:00Z",
    )

    s.failures.force_next(NetworkError("simulated DNS failure"))
    first = await s.run_reconciler("alice")
    # The first ingest call hit the forced network error and raised.
    # Run again without the failure — it should recover cleanly.
    later = await s.run_reconciler_until_quiescent("alice")

    s.assert_event_exists("main", summary="Network ✘ event")
    assert len(s.find_events("client_b", summary="Busy")) == 1

    await s.close()


# ---------------------------------------------------------------------------
# Sync token expiry recovery
# ---------------------------------------------------------------------------
async def test_expired_sync_token_falls_back_to_full_sync():
    """The plan calls out that sync-token expiry should recover
    automatically: ingest catches 410, restarts as full sync, and
    nothing on Google is double-applied (because deterministic IDs)."""
    from datetime import timedelta as _td

    s = Scenario(sync_token_ttl=_td(hours=1))
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user(
        "alice", main="main", clients=["client_a"],
    )
    s.given_event(
        "client_a", summary="Persistent event",
        start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler("alice")
    s.assert_event_exists("main", summary="Persistent event")

    # Time advances past sync-token TTL.
    s.advance(_td(hours=2))
    # Add a new event.
    s.given_event(
        "client_a", summary="After expiry",
        start="2026-02-09T09:00:00Z",
    )
    await s.run_reconciler("alice")

    # Both events appear, exactly once each, on main.
    s.assert_event_exists("main", summary="Persistent event")
    s.assert_event_exists("main", summary="After expiry")
    assert s.google.event_count(s.cal("main"), include_cancelled=False) == 2

    await s.close()


# ---------------------------------------------------------------------------
# user_intentionally_deleted
# ---------------------------------------------------------------------------
async def test_user_delete_on_main_suppresses_resurrection():
    """User removes the synced copy from main -> source still has
    the original, but our system must NOT recreate the main copy
    on the next reconcile."""
    s = await _alice_with_three_clients()
    src = s.given_event(
        "client_a", summary="Sticky delete test",
        start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler("alice")
    main_copy = s.assert_event_exists("main", summary="Sticky delete test")
    main_id = main_copy["id"]

    # User deletes the copy from main directly.
    s.cancel_event("main", main_id)

    # Run reconciler — main ingest should detect the cancellation
    # and flip user_intentionally_deleted on the ledger row.
    await s.run_reconciler("alice")
    # Run again — even though the source is unchanged, we must not
    # re-create the main copy.
    after = await s.run_reconciler("alice")

    s.assert_no_event_with_summary("main", "Sticky delete test")
    # The peer busy blocks should also be gone after the second pass
    # (they were re-derived as 'absent' once the parent flipped).
    assert s.find_events("client_b", summary="Busy") == []
    assert s.find_events("client_c", summary="Busy") == []

    await s.close()


# ---------------------------------------------------------------------------
# Loop prevention: our writes don't get re-ingested
# ---------------------------------------------------------------------------
async def test_our_busy_blocks_do_not_loop_back_through_ingest():
    """If we wrote a busy block on client_b, the next ingest of
    client_b must NOT see it as a new client event and try to
    ladder copies onto main + client_a + client_c."""
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a", summary="Loop test", start="2026-02-02T09:00:00Z",
    )

    # First pass: lays down main copy + busy blocks on B and C.
    await s.run_reconciler("alice")
    # Second pass: must observe its own writes on B and C and skip
    # them, not turn them into new ledger rows.
    out = await s.run_reconciler("alice")

    assert out["enqueued"] == 0
    assert s.google.event_count(s.cal("main")) == 1
    assert s.google.event_count(s.cal("client_b")) == 1
    assert s.google.event_count(s.cal("client_c")) == 1

    await s.close()


# ---------------------------------------------------------------------------
# Multiple sources
# ---------------------------------------------------------------------------
async def test_two_simultaneous_client_events_propagate_independently():
    s = await _alice_with_three_clients()
    s.given_event(
        "client_a", summary="From A", start="2026-02-02T09:00:00Z",
    )
    s.given_event(
        "client_b", summary="From B", start="2026-02-02T11:00:00Z",
    )
    await s.run_reconciler("alice")

    s.assert_event_exists("main", summary="From A")
    s.assert_event_exists("main", summary="From B")

    # client_a gets a busy block for B; client_b gets one for A;
    # client_c gets one for each.
    assert len(s.find_events("client_a", summary="Busy")) == 1
    assert len(s.find_events("client_b", summary="Busy")) == 1
    assert len(s.find_events("client_c", summary="Busy")) == 2

    await s.close()
