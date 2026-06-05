"""Revert-on-drift for busy blocks on client calendars.

If the user moves or edits a "Busy" block that BusyBridge wrote onto
a client calendar, the next ingest sees the etag no longer matches
what we stored and the diff re-asserts the canonical payload — the
block snaps back.  Previously this revert was applied only to copies
on the main calendar; client busy blocks drifted silently.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def test_moved_client_busy_block_is_reverted():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    s.given_event(
        "client_a", summary="Meeting", start="2026-02-02T09:00:00Z",
        event_id="meeting0000001",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    blocks = s.find_events("client_b", summary="Busy")
    assert len(blocks) == 1
    busy = blocks[0]
    assert busy["start"]["dateTime"] == "2026-02-02T09:00:00Z"

    # The user drags the busy block to a different time on client_b.
    s.update_event(
        "client_b", busy["id"],
        start="2026-02-02T14:00:00Z", end="2026-02-02T14:30:00Z",
    )
    moved = s.google.get_event(s.cal("client_b"), busy["id"])
    assert moved["start"]["dateTime"] == "2026-02-02T14:00:00Z"

    # The next reconcile detects the drift and reverts it.
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    after = s.find_events("client_b", summary="Busy")
    assert len(after) == 1
    assert after[0]["start"]["dateTime"] == "2026-02-02T09:00:00Z", (
        "moved client busy block was not reverted to its canonical time"
    )
    await s.close()
