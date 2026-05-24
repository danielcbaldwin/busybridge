"""Reproduction: a deleted recurring-instance copy must be re-created.

A modified/rescheduled occurrence's mirror copy can be deleted on
Google without the user intending to cancel the occurrence — e.g. the
discovery/orphan scan wiped it during the churn incident.  The next
reconcile must re-create it.  Today instance projections are
UPDATE-only (the diff rewrites an instance CREATE into an UPDATE), so a
gone instance copy loops UPDATE->404 and never comes back — the
residual tail of the v2 deploy incident.

We trigger it via a PEER busy-block deletion (client_b), which BB
treats as drift to *repair* (recreate), not as a user cancellation
(that path is main-only) — isolating the recreate bug cleanly.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _occ_id(s: Scenario, cal: str, ymd: str):
    for ev in s.list_events(cal, single_events=True):
        st = ev.get("start", {})
        stamp = st.get("dateTime") or st.get("date") or ""
        if stamp.startswith(ymd):
            return ev["id"]
    return None


def _occ_present(s: Scenario, cal: str, ymd: str) -> bool:
    return _occ_id(s, cal, ymd) is not None


async def test_deleted_recurring_instance_busyblock_is_recreated():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    s.given_recurring_event(
        "client_a", summary="Team sync",
        start="2026-02-02T09:00:00Z", rrule="RRULE:FREQ=WEEKLY;COUNT=6",
    )
    await s.run_reconciler("alice")

    # Move the 2026-02-16 occurrence on the SOURCE → a modified instance,
    # which is mirrored as a busy block on the peer (client_b).
    src = _occ_id(s, "client_a", "2026-02-16")
    assert src, "source occurrence not found"
    s.update_event("client_a", src, start="2026-02-16T14:00:00Z")
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    peer = _occ_id(s, "client_b", "2026-02-16")
    assert peer, "moved occurrence not mirrored to peer client_b"

    # Simulate the churn / orphan-scan wiping that occurrence's copy on
    # the peer (NOT a user cancellation — the occurrence stays active).
    s.google.delete_event(s.cal("client_b"), peer)
    assert not _occ_present(s, "client_b", "2026-02-16"), "delete precondition"

    await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # BB must repair the drift by re-creating the occurrence's busy block.
    assert _occ_present(s, "client_b", "2026-02-16"), (
        "deleted recurring-instance busy block was NOT re-created"
    )
    # ...and it must reflect the *moved* time (a faithful recreate of the
    # modified occurrence, not a stale 09:00 expansion).
    revived = _occ_id(s, "client_b", "2026-02-16")
    ev = next(e for e in s.list_events("client_b", single_events=True)
              if e["id"] == revived)
    assert ev["status"] != "cancelled", "revived occurrence still cancelled"
    assert (ev.get("start", {}).get("dateTime") or "").startswith(
        "2026-02-16T14:00:00"
    ), f"revived occurrence at wrong time: {ev.get('start')}"
    await s.close()
