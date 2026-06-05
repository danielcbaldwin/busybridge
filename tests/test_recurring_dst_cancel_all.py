"""Cancelling every occurrence of a DST-crossing recurring series must prune
the mirror — no ghost recurring copy left on main.

Regression: ``_recurring_parent_has_no_live_occurrences`` expanded the source
RRULE on the series' FIXED start offset, ignoring its IANA ``start_timezone``.
For a series crossing a DST transition, every post-transition occurrence was
computed an hour off, so its key never matched the (correctly-keyed) cancelled
child rows and the parent was wrongly judged still-live → the recurring mirror
on main was never deleted.  The expansion is now anchored in the IANA zone.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _cancel_all_and_collect_ghosts(tz, start):
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    kw = dict(summary="Standup", start=start, rrule="RRULE:FREQ=WEEKLY;COUNT=5")
    if tz:
        kw["timezone"] = tz
    s.given_recurring_event("client_a", **kw)
    await s.run_reconciler_until_quiescent("alice", max_passes=8)

    # User cancels EVERY occurrence on the source.
    for ev in s.list_events("client_a", single_events=True):
        if ev.get("status") != "cancelled":
            s.google.delete_event(s.cal("client_a"), ev["id"])
    await s.run_reconciler_until_quiescent("alice", max_passes=12)

    ghosts = [
        ev["id"]
        for ev in s.list_events("main", single_events=False)
        if ev.get("recurrence") and ev.get("status") != "cancelled"
    ]
    await s.close()
    return ghosts


async def test_utc_series_cancel_all_prunes_mirror():
    assert await _cancel_all_and_collect_ghosts(None, "2026-03-05T09:00:00Z") == []


async def test_ny_series_no_dst_cancel_all_prunes_mirror():
    assert await _cancel_all_and_collect_ghosts(
        "America/New_York", "2026-02-05T09:00:00-05:00"
    ) == []


async def test_dst_crossing_series_cancel_all_prunes_mirror():
    # 2026-03-05 (EST) through 04-02 (EDT) — straddles the 2026-03-08 change.
    ghosts = await _cancel_all_and_collect_ghosts(
        "America/New_York", "2026-03-05T09:00:00-05:00"
    )
    assert ghosts == [], (
        f"DST-crossing series left a live recurring mirror ghost on main: {ghosts}"
    )
