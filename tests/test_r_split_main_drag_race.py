"""A main-side drag landing on the base series during the split→reconcile
race window must not leave a duplicate / orphan post-boundary copy.

Race: the source is split "this and following" at a boundary, and BEFORE
BusyBridge reconciles (truncates its own base mirror), the user drags a
POST-boundary occurrence on the main calendar — whose recurringEventId is
still the (untruncated) base bb-series.  Main ingest mapped it back to the
base, minting a base-parented instance for a date the now-truncated base no
longer covers.  The _R segment independently mirrors that date, so main ended
up with TWO copies (stable, churn=0): the segment's regular occurrence AND the
stale base-parented drag.

Correct steady state: exactly one main copy and one peer busy block for the
occurrence, agreeing on a single time, the real source occurrence untouched,
and zero churn.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _occs(s, cal, ymd):
    out = []
    for ev in s.list_events(cal, single_events=True):
        st = ev.get("start", {})
        stamp = st.get("dateTime") or st.get("date") or ""
        if stamp.startswith(ymd) and ev.get("status") != "cancelled":
            out.append(ev)
    return out


def _times(evs):
    return sorted({e["start"].get("dateTime") or e["start"].get("date") for e in evs})


def _sum_change(s):
    return sum(c.change_counter for c in s.google._calendars.values())


async def test_main_drag_postboundary_during_split_race_no_dup():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user("alice", main="main", clients=["client_a", "client_b"])
    series = s.given_recurring_event(
        "client_a", summary="Team sync",
        start="2026-02-02T09:00:00Z", rrule="RRULE:FREQ=WEEKLY;COUNT=10",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=8)

    row = await (await s._db.execute(
        "SELECT p.google_event_id FROM ledger_projections p "
        "JOIN ledger_events e ON e.id=p.ledger_event_id "
        "WHERE e.user_id=? AND e.source_event_id=? AND p.target_kind='main'",
        (user.user_id, series["id"]),
    )).fetchone()
    base_main_gid = row["google_event_id"]

    # 1. Split the SOURCE this-and-following at 2026-03-02.
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"],
        from_dt="2026-03-02T09:00:00Z", new_rrule="RRULE:FREQ=WEEKLY;COUNT=6",
    )
    # 2. BEFORE reconciling, drag a POST-boundary occurrence (03-09) on MAIN —
    #    recurringEventId is still the untruncated base bb-series.
    s.update_event("main", f"{base_main_gid}_20260309T090000Z",
                   start="2026-03-09T14:00:00Z")
    # 3. Reconcile several sync cycles. The stray base-parented copy is adopted
    #    first, then detected as orphaned (date not covered by the truncated
    #    base) on a later ingest cycle and reverted — eventual consistency.
    for _ in range(4):
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

    # Steady state must be churn-free.
    before = _sum_change(s)
    await s.run_reconciler_until_quiescent("alice", max_passes=6)
    churn = _sum_change(s) - before

    main = _occs(s, "main", "2026-03-09")
    peer = _occs(s, "client_b", "2026-03-09")
    src = _occs(s, "client_a", "2026-03-09")

    assert len(main) == 1, (
        f"I3: expected exactly one main copy for 03-09, found {len(main)}: "
        f"{[(e['id'], e['start']) for e in main]}"
    )
    assert len(peer) == 1, f"I3: expected one client_b block for 03-09, found {len(peer)}"
    assert _times(main) == _times(peer), (
        f"I1: main {_times(main)} and peer {_times(peer)} disagree on 03-09 time"
    )
    assert len(src) == 1, f"I5: the real source occurrence must survive, found {len(src)}"
    assert churn == 0, f"I5: steady state must be churn-free, got change delta {churn}"
    await s.close()
