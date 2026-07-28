"""A regenerated parent series id must drag its instance overrides along.

``outbox._do_create`` inserts under a deterministic id.  If the user has
deleted one of our managed events, Google keeps that id reserved as a
cancelled tombstone, so the insert 409s; ``_do_create`` then bumps
``google_id_generation``, derives a fresh id, and retries.  A mirrored
recurring series therefore legitimately changes Google id over its
lifetime.

Instance projections address their occurrence as
``<parent_google_event_id>_<stamp>`` (``diff`` derives it), and nothing
used to tell them the prefix had moved:

* A *present* occurrence override keeps pointing at the burned prefix,
  so its UPDATEs 404 forever.
* A *cancelled* occurrence is worse.  Its projection is
  ``desired_state='absent'`` and quiescent (``applied == desired``), so
  the diff never re-selects it.  The recreated series carries the source
  RRULE **verbatim** — no EXDATE — so Google expands the very occurrence
  the source had cancelled, and the resulting busy block is owned by
  nobody: no reconcile, drain, drift-revert or content-audit pass ever
  revisits it (the audit is source-side and skips cancelled rows).

Live regression: a personal-source "Bi-Weekly All-Hands" series reached
``google_id_generation = 3`` on a client calendar; its cancelled
2026-07-28 occurrence sat converged with a cleared ``google_event_id``
while the recreated series cast a permanent 2pm "Busy" block.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio

# Weekly series on 2026-02-02, 02-09, 02-16, 02-23 at 09:00Z.
_SERIES_ID = "allhands000001"
_RRULE = "RRULE:FREQ=WEEKLY;COUNT=4"
_CANCELLED_DATE = "2026-02-16"
_CANCELLED_INSTANCE = f"{_SERIES_ID}_20260216T090000Z"


def _live_dates(s: Scenario, cal: str, summary: str) -> list[str]:
    """Start dates of the live occurrences of ``summary`` on ``cal``, with
    recurring series expanded the way a calendar UI shows them."""
    out = []
    for ev in s.list_events(cal, single_events=True):
        if ev.get("status") == "cancelled" or ev.get("summary") != summary:
            continue
        st = ev.get("start", {})
        out.append((st.get("dateTime") or st.get("date") or "")[:10])
    return sorted(out)


def _busy_series(s: Scenario, cal: str) -> dict:
    """The single mirrored recurring busy master on ``cal``."""
    masters = [
        ev for ev in s.find_events(cal, summary="Busy")
        if ev.get("recurrence")
    ]
    assert len(masters) == 1, (
        f"expected one recurring busy master on {cal!r}, got "
        f"{[(e.get('id'), e.get('recurrence')) for e in masters]}"
    )
    return masters[0]


async def _mirror_with_cancelled_occurrence(s: Scenario):
    """client_a hosts a weekly series whose 2026-02-16 occurrence is
    cancelled; client_b holds the mirrored busy series."""
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    s.given_recurring_event(
        "client_a", summary="All-Hands", event_id=_SERIES_ID,
        start="2026-02-02T09:00:00Z", rrule=_RRULE,
    )
    s.cancel_event("client_a", _CANCELLED_INSTANCE)
    await s.run_reconciler_until_quiescent("alice", max_passes=5)
    return user


async def _instance_projections(s: Scenario, user, client_nick: str):
    """Every instance projection targeting ``client_nick``."""
    db = await s.setup_db()
    return await (await db.execute(
        """SELECT p.id, p.google_event_id, p.desired_state,
                  p.applied_ledger_version, p.desired_ledger_version,
                  p.applied_payload_hash, p.desired_payload_hash,
                  e.status AS ledger_status
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND e.parent_canonical_uid IS NOT NULL
              AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (user.user_id, user.client_calendar_ids[client_nick]),
    )).fetchall()


async def _parent_generation(s: Scenario, user, client_nick: str) -> int:
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT MAX(p.google_id_generation) AS gen
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND e.parent_canonical_uid IS NULL
              AND e.recurrence_rule_json IS NOT NULL
              AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (user.user_id, user.client_calendar_ids[client_nick]),
    )).fetchone()
    return int(row["gen"] or 0)


async def test_cancelled_occurrence_survives_a_parent_id_regeneration():
    s = Scenario()
    try:
        user = await _mirror_with_cancelled_occurrence(s)

        # Baseline: the mirror shows three occurrences, not four.
        assert _live_dates(s, "client_b", "Busy") == [
            "2026-02-02", "2026-02-09", "2026-02-23",
        ]
        first = _busy_series(s, "client_b")
        assert await _parent_generation(s, user, "client_b") == 0

        # The user deletes our managed busy series off client_b.  Google
        # keeps that deterministic id reserved as a cancelled tombstone.
        s.cancel_event("client_b", first["id"])

        # Revert-on-drift re-asserts the series; the insert 409s on the
        # tombstone, burns the id and lands on a fresh generation.
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        second = _busy_series(s, "client_b")
        assert second["id"] != first["id"], (
            "expected the tombstoned series id to be burned and regenerated"
        )
        assert await _parent_generation(s, user, "client_b") > 0

        # THE INVARIANT: the recreated series must still not show the
        # occurrence the source had cancelled.
        assert _live_dates(s, "client_b", "Busy") == [
            "2026-02-02", "2026-02-09", "2026-02-23",
        ], (
            "the recreated series expanded the cancelled occurrence into a "
            "phantom busy block that nothing owns"
        )

        # ...and it is addressed under the NEW prefix, converged.
        for proj in await _instance_projections(s, user, "client_b"):
            if proj["ledger_status"] != "cancelled":
                continue
            assert proj["google_event_id"] is None or (
                proj["google_event_id"].startswith(second["id"])
            ), (
                f"cancelled occurrence still addresses the burned prefix: "
                f"{proj['google_event_id']}"
            )
    finally:
        await s.close()


async def test_all_instance_projections_converge_after_regeneration():
    """The re-diverged instances must settle, not churn forever."""
    s = Scenario()
    try:
        user = await _mirror_with_cancelled_occurrence(s)
        first = _busy_series(s, "client_b")
        s.cancel_event("client_b", first["id"])
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        # A further pass must find nothing left to do.
        out = await s.run_reconciler("alice")
        assert out["enqueued"] == 0, (
            f"reconcile is not quiescent after regeneration: {out}"
        )
        for proj in await _instance_projections(s, user, "client_b"):
            assert (
                proj["applied_ledger_version"] == proj["desired_ledger_version"]
                and proj["applied_payload_hash"] == proj["desired_payload_hash"]
            ), f"instance projection {proj['id']} never converged"
    finally:
        await s.close()


async def test_sibling_calendar_is_untouched_by_one_targets_regeneration():
    """Each target holds its own deterministic id, so burning one must not
    disturb another target's instance projections."""
    s = Scenario()
    try:
        user = await _mirror_with_cancelled_occurrence(s)
        before = {
            p["id"]: p["google_event_id"]
            for p in await _instance_projections(s, user, "client_a")
        }

        first = _busy_series(s, "client_b")
        s.cancel_event("client_b", first["id"])
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        after = {
            p["id"]: p["google_event_id"]
            for p in await _instance_projections(s, user, "client_a")
        }
        assert after == before, (
            "client_a's instance projections were collaterally repointed by "
            "client_b's id regeneration"
        )
    finally:
        await s.close()


async def test_source_side_occurrences_are_unaffected():
    """Sanity: none of this writes to the calendar that sourced the
    series."""
    s = Scenario()
    try:
        await _mirror_with_cancelled_occurrence(s)
        first = _busy_series(s, "client_b")
        s.cancel_event("client_b", first["id"])
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        assert _live_dates(s, "client_a", "All-Hands") == [
            "2026-02-02", "2026-02-09", "2026-02-23",
        ]
    finally:
        await s.close()
