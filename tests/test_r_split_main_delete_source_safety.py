"""Phase-0 safety FLOOR for re-enabling delete propagation.

Reproduces the data-loss incident's SHAPE — a "this and following" (``_R``)
split, followed by the user deleting a *post-boundary* occurrence of the managed
copy on MAIN — and locks in the invariant that BusyBridge must NEVER
destructively delete the real source occurrence on ``client_a`` (the path that
deleted ~166 real MLCommons occurrences on 2026-06-02), nor arm
``source_delete_pending`` off this churn-artifact path.

This asserts the CURRENT safe behaviour (source survives, mirror re-asserted,
no endless churn). It is the floor the delete-propagation work must keep green:
when genuine organizer-delete propagation is re-enabled it is gated on positive
user intent + provenance, so THIS artifact path must still never touch the
source. If this test ever goes red, we are repeating the incident.

See DELETE_PROPAGATION_PLAN.md (Phase 0). Complements
``test_r_split_mirror_invariants`` (forward correctness) and
``test_main_managed_instance_cancellation`` (plain cancel survival).
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _live_dates(s: Scenario, nick: str) -> set[str]:
    """YYYY-MM-DD of every non-cancelled occurrence currently on a calendar."""
    out: set[str] = set()
    for ev in s.list_events(nick, single_events=True):
        if ev.get("status") == "cancelled":
            continue
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp:
            out.add(stamp[:10])
    return out


def _instance_id_for(s: Scenario, nick: str, ymd: str) -> str:
    for ev in s.list_events(nick, single_events=True):
        start = ev.get("start", {})
        stamp = start.get("dateTime") or start.get("date") or ""
        if stamp.startswith(ymd) and ev.get("status") != "cancelled":
            return ev["id"]
    raise AssertionError(f"no live occurrence on {ymd} on {nick!r}")


def _writes(s: Scenario) -> int:
    return sum(c.change_counter for c in s.google._calendars.values())


async def _q(s: Scenario, passes: int = 10) -> None:
    await s.run_reconciler_until_quiescent("alice", max_passes=passes)


async def test_r_split_then_main_delete_never_deletes_source():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    series = s.given_recurring_event(
        "client_a", summary="Sync", start="2026-02-02T09:00:00Z",
        rrule="RRULE:FREQ=WEEKLY;COUNT=10",
    )
    await _q(s)

    # "This and following" split at 2026-03-02, moved to 11:00 — produces the
    # real ``_R`` segment (the post-boundary occurrences live under it).
    s.reschedule_recurring_this_and_following(
        "client_a", series["id"], from_dt="2026-03-02T09:00:00Z",
        new_start="2026-03-02T11:00:00Z", new_rrule="RRULE:FREQ=WEEKLY;COUNT=6",
    )
    await _q(s)
    assert "2026-03-16" in _live_dates(s, "client_a"), "setup: post-split occ live on source"
    assert "2026-03-16" in _live_dates(s, "main"), "setup: post-split occ mirrored to main"

    # The artifact-prone action: the user deletes a POST-boundary occurrence of
    # the managed copy on MAIN (the exact churn that, pre-fix, looped and
    # destroyed real source occurrences).
    s.cancel_event("main", _instance_id_for(s, "main", "2026-03-16"))
    await _q(s)
    await _q(s)  # extra passes to surface any churn / re-delete

    # FLOOR 1 — the real source occurrence on client_a SURVIVES.
    src = _live_dates(s, "client_a")
    assert "2026-03-16" in src, (
        "the real source occurrence must NOT be destructively deleted"
    )
    for d in ("2026-02-16", "2026-03-09", "2026-03-23"):
        assert d in src, f"an unrelated source occurrence ({d}) was lost"

    # FLOOR 2 — nothing destructive was armed, anywhere.
    db = await s.setup_db()
    rows = await (await db.execute(
        "SELECT source_delete_pending FROM ledger_events WHERE user_id = ?",
        (user.user_id,),
    )).fetchall()
    assert rows and all(not r["source_delete_pending"] for r in rows), (
        "source_delete_pending must never be armed by the _R/main-delete artifact path"
    )

    # FLOOR 3 — steady state is churn-free (no endless Google writes).
    before = _writes(s)
    await _q(s)
    assert _writes(s) - before == 0, "must converge churn-free, not loop"
    await s.close()
