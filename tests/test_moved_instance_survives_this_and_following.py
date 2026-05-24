"""Moved single instances of recurring events must survive forever.

The core, permanent use case: a user moves ONE occurrence of a repeating
meeting (a modified instance), and later edits the same series with
Google's "this and following" option (which splits it into a
``<base>_R<date>`` segment).

Google's split model is *additive*: the base series is truncated with an
UNTIL and coexists with each ``_R<date>`` segment; every segment covers a
distinct date range, and a modified instance belongs to whichever segment
its ``recurringEventId`` names.  The ingest's ``_R`` re-key wrongly
treated a new segment as a REPLACEMENT of the base and bulk-re-parented
ALL modified instances onto the newest segment.  A pre-boundary moved
occurrence then derived its instance id against a series that doesn't
contain it (``<later_segment>_<earlier_stamp>``) → permanent 404 → the
mirror silently vanished.  This is the MLC-meeting residual from the v2
incident, and it recurs on every "this and following" edit.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _occ(s: Scenario, cal: str, ymd: str):
    """Return the single-instance event on ``cal`` starting on ``ymd``."""
    for ev in s.list_events(cal, single_events=True):
        st = ev.get("start", {})
        stamp = st.get("dateTime") or st.get("date") or ""
        if stamp.startswith(ymd) and ev.get("status") != "cancelled":
            return ev
    return None


async def test_moved_instance_survives_this_and_following_split():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    series = s.given_recurring_event(
        "client_a", summary="Team sync",
        start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=5)

    # 1. Move ONE occurrence (2026-02-16) to a new time → modified
    #    instance on the master series.  Mirrors to client_b at 14:00.
    occ = _occ(s, "client_a", "2026-02-16")
    assert occ, "source occurrence not found"
    s.update_event("client_a", occ["id"], start="2026-02-16T14:00:00Z")
    await s.run_reconciler_until_quiescent("alice", max_passes=5)
    peer = _occ(s, "client_b", "2026-02-16")
    assert peer, "moved occurrence not mirrored to peer"
    assert (peer["start"]["dateTime"]).startswith("2026-02-16T14:00:00"), \
        f"mirror at wrong time before split: {peer['start']}"

    # 2. Later, edit the series "this and following" at 2026-03-02
    #    (AFTER the moved occurrence).  Google truncates the master and
    #    creates a <base>_R<date> segment for 03-02 onward.  The moved
    #    02-16 occurrence is BEFORE the boundary → stays on the master.
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"],
        from_dt="2026-03-02T09:00:00Z",
        new_rrule="RRULE:FREQ=WEEKLY;COUNT=6",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=8)

    # 3. Pre-boundary regular occurrences must still be mirrored — the
    #    re-key renamed the master onto the _R segment, which wipes
    #    everything before the boundary.
    assert _occ(s, "client_b", "2026-02-09") is not None, (
        "pre-boundary regular occurrence (02-09) vanished after the split"
    )
    # 4. The post-boundary segment must mirror too.
    assert _occ(s, "client_b", "2026-03-09") is not None, (
        "post-split segment occurrences not mirrored to peer"
    )
    # 5. The moved occurrence's mirror must still be present at 14:00.
    survived = _occ(s, "client_b", "2026-02-16")
    assert survived is not None, "moved occurrence's mirror vanished after split"
    assert (survived["start"]["dateTime"]).startswith("2026-02-16T14:00:00"), \
        f"moved mirror at wrong time after split: {survived['start']}"

    # 6. THE REAL TEST: simulate churn deleting the moved occurrence's
    #    busy block, then reconcile.  It must RE-CREATE — which is only
    #    possible if it's still parented to a series that actually
    #    contains 2026-02-16.  Under the re-key bug it's parented to the
    #    03-02 segment, so the derived id 404s forever and never returns.
    s.google.delete_event(s.cal("client_b"), survived["id"])
    assert _occ(s, "client_b", "2026-02-16") is None, "delete precondition"
    await s.run_reconciler_until_quiescent("alice", max_passes=8)
    recreated = _occ(s, "client_b", "2026-02-16")
    assert recreated is not None, (
        "moved occurrence's busy block could NOT be re-created after the "
        "this-and-following edit — it is parented to a segment that does "
        "not contain its date (the _R re-key corruption)"
    )
    assert (recreated["start"]["dateTime"]).startswith("2026-02-16T14:00:00"), \
        f"re-created mirror at wrong time: {recreated['start']}"
    await s.close()
