"""Permanent I1-I5 invariants for "change all events from here forward"
(``_R`` this-and-following) recurring-mirror correctness.

These lock in the forward steady-state behaviour the ledger achieves today so
it can never silently regress:

* I1 full coverage  — every live source occurrence (pre- AND post-boundary)
  has exactly one main full copy and one peer busy block at its current time.
* I2 no stale copy   — no mirror survives at the old time after a split.
* I3 no dup/ghost    — never two main copies / two peer blocks for one date.
* I4 main-side delete of a moved copy resolves deterministically — an
  IN-RANGE occurrence's delete is honored as user intent (sticky
  suppression, mirror-only; see ingest/main's tri-state _series_covers
  gate), while an out-of-range one (the _R artifact shape) still reverts
  (covered in test_main_managed_instance_cancellation).  Either way: no
  404-forever loops.
* I5 convergence     — steady state is churn-free and the real source
  occurrence (client_a) is never destructively deleted.

The companion files cover the race/edge fixes:
``test_r_split_main_drag_race`` (the split→reconcile race duplicate).
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


# --- helpers ---------------------------------------------------------------
def _live(s, nick, ymd):
    out = []
    for ev in s.list_events(nick, single_events=True):
        st = ev.get("start", {})
        stamp = st.get("dateTime") or st.get("date") or ""
        if stamp.startswith(ymd) and ev.get("status") != "cancelled":
            out.append(ev)
    return out


def _start(ev):
    return ev["start"].get("dateTime") or ev["start"].get("date")


def _sum_change(s):
    return sum(c.change_counter for c in s.google._calendars.values())


async def _q(s, passes=10):
    await s.run_reconciler_until_quiescent("alice", max_passes=passes)


async def _setup(s, clients=("client_a", "client_b")):
    s.given_calendar("main")
    for c in clients:
        s.given_calendar(c)
    return await s.given_user("alice", main="main", clients=list(clients))


async def _assert_churn_free(s):
    before = _sum_change(s)
    await _q(s)
    assert _sum_change(s) - before == 0, "I5: steady state is not churn-free"


# --- I1/I2/I3: clean split with a time change ------------------------------
async def test_time_change_split_pre_old_post_new_single_copies():
    s = Scenario()
    await _setup(s)
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_start="2026-03-02T11:00:00Z", new_rrule="RRULE:FREQ=WEEKLY;COUNT=6")
    await _q(s)

    for nick in ("main", "client_b"):
        pre = _live(s, nick, "2026-02-23")
        post = _live(s, nick, "2026-03-09")
        assert len(pre) == 1, f"I3 {nick} pre: {[_start(e) for e in pre]}"
        assert _start(pre[0]).startswith("2026-02-23T09:00"), f"I2 {nick} pre time"
        assert len(post) == 1, f"I3 {nick} post: {[_start(e) for e in post]}"
        assert _start(post[0]).startswith("2026-03-09T11:00"), (
            f"I1/I2 {nick} post must be at the NEW time, got {_start(post[0])}")
    await _assert_churn_free(s)
    await s.close()


# --- I4: pre-boundary moved instance — main-side delete is honored ----------
async def test_preboundary_moved_instance_main_delete_suppresses_in_range():
    """BEHAVIOR CHANGE: a delete on main of a moved occurrence that is
    still IN the truncated base's range cannot be an _R-split artifact
    (splits only cancel occurrences beyond the boundary), so it is now
    honored as user intent: the mirror is sticky-suppressed instead of
    resurrected (the old I4 "re-creates after churn" expectation).  The
    real source occurrence still survives (I5) and there is no
    404-forever loop — the suppression converges churn-free."""
    s = Scenario()
    await _setup(s)
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10")
    await _q(s)
    occ = _live(s, "client_a", "2026-02-16")[0]
    s.update_event("client_a", occ["id"], start="2026-02-16T14:00:00Z")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_rrule="RRULE:FREQ=WEEKLY;COUNT=6")
    await _q(s)

    main216 = _live(s, "main", "2026-02-16")
    assert len(main216) == 1 and _start(main216[0]).startswith("2026-02-16T14:00")
    # Delete the MAIN full copy: 02-16 is pre-boundary (in range of the
    # truncated base), so the delete is user intent — sticky suppression.
    s.google.delete_event(s.cal("main"), main216[0]["id"])
    await _q(s)
    assert _live(s, "main", "2026-02-16") == [], (
        "in-range main-side delete of a moved occurrence must suppress "
        "the copy, not resurrect it")
    assert _live(s, "client_b", "2026-02-16") == [], (
        "the suppressed occurrence's peer busy block must go too")
    # The real source occurrence is untouched (I5).
    assert len(_live(s, "client_a", "2026-02-16")) == 1
    # Sticky + churn-free (no 404-forever re-create loop).
    await _q(s)
    assert _live(s, "main", "2026-02-16") == []
    await _assert_churn_free(s)
    await s.close()


# --- I4: post-boundary modified instance under _R — delete is honored -------
async def test_postboundary_segment_modify_main_delete_suppresses():
    """BEHAVIOR CHANGE (same as the pre-boundary test above): the moved
    occurrence is parented to the live _R SEGMENT and is inside the
    segment's range, so a main-side delete of its copy is user intent —
    sticky suppression, source untouched, no re-create loop."""
    s = Scenario()
    await _setup(s)
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_rrule="RRULE:FREQ=WEEKLY;COUNT=6")
    await _q(s)
    seg = _live(s, "client_a", "2026-03-16")[0]
    s.update_event("client_a", seg["id"], start="2026-03-16T15:00:00Z")
    await _q(s)
    m = _live(s, "main", "2026-03-16")
    assert len(m) == 1 and _start(m[0]).startswith("2026-03-16T15:00")
    s.google.delete_event(s.cal("main"), m[0]["id"])
    await _q(s)
    assert _live(s, "main", "2026-03-16") == [], (
        "in-range (segment) main-side delete must suppress the copy")
    # The real source occurrence on the segment is untouched (I5).
    assert len(_live(s, "client_a", "2026-03-16")) == 1
    await _q(s)
    assert _live(s, "main", "2026-03-16") == []
    await _assert_churn_free(s)
    await s.close()


# --- I1/I3/I5: multi-split chain -------------------------------------------
async def test_multi_split_chain_each_segment_single_copy_no_churn():
    s = Scenario()
    await _setup(s)
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=16")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_start="2026-03-02T11:00:00Z", new_rrule="RRULE:FREQ=WEEKLY;COUNT=10")
    await _q(s)
    seg = None
    for ev in s.list_events("client_a", single_events=False):
        if "_R" in ev.get("id", ""):
            seg = ev
    assert seg is not None
    s.reschedule_recurring_this_and_following(
        "client_a", seg["id"], from_dt="2026-04-06T11:00:00Z",
        new_start="2026-04-06T13:00:00Z", new_rrule="RRULE:FREQ=WEEKLY;COUNT=6")
    await _q(s)

    s1 = _live(s, "main", "2026-03-09")   # first segment, 11:00
    s2 = _live(s, "main", "2026-04-13")   # second segment, 13:00
    assert len(s1) == 1 and _start(s1[0]).startswith("2026-03-09T11:00")
    assert len(s2) == 1 and _start(s2[0]).startswith("2026-04-13T13:00")
    await _assert_churn_free(s)
    await s.close()


# --- I3/I5: whole-segment cancel; pre survives, post vanishes --------------
async def test_cancel_R_segment_removes_post_keeps_pre():
    s = Scenario()
    await _setup(s)
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_rrule="RRULE:FREQ=WEEKLY;COUNT=6")
    await _q(s)
    seg = next(ev for ev in s.list_events("client_a", single_events=False)
               if "_R" in ev.get("id", ""))
    s.google.delete_event(s.cal("client_a"), seg["id"])
    await _q(s)
    assert _live(s, "main", "2026-03-09") == [], "post-boundary copies must vanish"
    assert len(_live(s, "main", "2026-02-23")) == 1, "pre-boundary must survive"
    await _assert_churn_free(s)
    await s.close()


# --- I1: RSVP / multi-peer across the split --------------------------------
async def test_multi_peer_busy_blocks_across_split():
    s = Scenario()
    await _setup(s, clients=("client_a", "client_b", "client_c"))
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_rrule="RRULE:FREQ=WEEKLY;COUNT=6")
    await _q(s)
    for ymd in ("2026-02-23", "2026-03-09"):
        for peer in ("client_b", "client_c"):
            assert len(_live(s, peer, ymd)) == 1, f"{peer} missing block {ymd}"
        # origin client_a keeps its own source occurrence, no extra busy copy.
        assert len(_live(s, "client_a", ymd)) == 1
    await _assert_churn_free(s)
    await s.close()


# --- I1/I3: ALL-DAY split, no boundary double-cover ------------------------
async def test_all_day_split_no_boundary_double_cover():
    s = Scenario()
    await _setup(s)
    series = s.given_recurring_event(
        "client_a", summary="AllDay", start="2026-02-02",
        rrule="RRULE:FREQ=DAILY;COUNT=10")
    await _q(s)
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-02-06",
        new_start="2026-02-06", new_rrule="RRULE:FREQ=DAILY;COUNT=6")
    await _q(s)
    for nick in ("main", "client_b"):
        starts = sorted(_start(e) for e in s.list_events(nick, single_events=True)
                        if e.get("status") != "cancelled")
        dups = sorted({d for d in starts if starts.count(d) > 1})
        assert not dups, f"I3 {nick} all-day duplicate on {dups}"
        assert len(starts) == 10, f"I1 {nick} all-day coverage: {starts}"
    await _assert_churn_free(s)
    await s.close()
