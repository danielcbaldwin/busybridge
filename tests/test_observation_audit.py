"""Observation audit (app/ledger/observe.py): trust, but verify.

Convergence is certified by internal bookkeeping only; these tests
prove the observation pass turns "bookkeeping says converged but Google
disagrees" into a marked divergence that the very next reconcile heals
— and, just as important, that a consistent world is left completely
alone (the audit must never become a churn source).

Each test drives the real pipeline (ingest → plan → diff → drain)
against the fake Google, tampers behind the ledger's back, runs
``observe_user``, and asserts both the marking (DB state, counters) and
the end-to-end heal (Google state after reconcile, then the zero-op
steady state — the adversarial suite's churn detector).
"""

from __future__ import annotations

import contextlib
from datetime import timezone

import pytest

from app.ledger.identity import (
    derive_google_event_id,
    derive_instance_google_event_id,
)
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio

UTC = timezone.utc


@contextlib.asynccontextmanager
async def scenario():
    """Scenario with guaranteed teardown (see test_adversarial_recurrence)."""
    s = Scenario()
    try:
        yield s
    finally:
        with contextlib.suppress(Exception):
            await s.close()


async def _alice_two_clients(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    return await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )


async def _projection(
    s: Scenario,
    *,
    source_event_id: str,
    target_kind: str,
    target_calendar_id=None,
    original_start: str | None = None,
):
    """The projection row for a source event (or one of its instance
    rows when ``original_start`` is given) on one target."""
    db = await s.setup_db()
    conditions = "e.source_event_id = ? AND p.target_kind = ?"
    params: list = [source_event_id, target_kind]
    if original_start is None:
        conditions += " AND e.parent_canonical_uid IS NULL"
    else:
        conditions += (
            " AND e.parent_canonical_uid IS NOT NULL"
            " AND e.recurrence_instance_original_start LIKE ?"
        )
        params.append(f"{original_start}%")
    if target_calendar_id is None:
        conditions += " AND p.target_calendar_id IS NULL"
    else:
        conditions += " AND p.target_calendar_id = ?"
        params.append(int(target_calendar_id))
    row = await (await db.execute(
        f"""SELECT p.*, e.canonical_uid, e.status AS ledger_status
              FROM ledger_projections p
              JOIN ledger_events e ON e.id = p.ledger_event_id
             WHERE {conditions}""",
        params,
    )).fetchone()
    assert row is not None, (
        f"no projection for source_event_id={source_event_id} "
        f"target={target_kind}:{target_calendar_id} start={original_start}"
    )
    return row


async def _assert_zero_op(s: Scenario, label: str) -> None:
    out = await s.run_reconciler("alice")
    assert out["enqueued"] == 0, (
        f"{label}: reconcile with no source change enqueued "
        f"{out['enqueued']} op(s)"
    )


# ---------------------------------------------------------------------------
# The audit must be churn-free on a consistent world
# ---------------------------------------------------------------------------
async def test_consistent_world_marks_nothing_and_causes_no_churn():
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_event("client_a", summary="Standup", event_id="obsstandup01")
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsrecur01",
        )
        await s.run_reconciler_until_quiescent("alice")

        out = await s.run_observation("alice")
        assert out["divergent"] == 0, out
        assert out["checked"] > 0, "audit inspected nothing"
        assert out["consistent"] == out["checked"], out
        # The instances scan ran for the recurring copies and found
        # nothing to correct.
        assert out["instance_scans"] > 0, out
        assert out["instance_ids_corrected"] == 0, out

        await _assert_zero_op(s, "post-observation")

        # Rotation stamps every inspected row.
        db = await s.setup_db()
        row = await (await db.execute(
            """SELECT COUNT(*) AS n FROM ledger_projections p
                 JOIN ledger_events e ON e.id = p.ledger_event_id
                WHERE e.user_id = ? AND p.last_observed_at IS NOT NULL""",
            (user.user_id,),
        )).fetchone()
        assert int(row["n"]) >= out["checked"]


# ---------------------------------------------------------------------------
# Present-direction: our copy vanished / was edited behind our back
# ---------------------------------------------------------------------------
async def test_deleted_busy_block_detected_and_recreated():
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_event("client_a", summary="Kickoff", event_id="obskickoff01")
        await s.run_reconciler_until_quiescent("alice")

        proj = await _projection(
            s, source_event_id="obskickoff01", target_kind="client",
            target_calendar_id=user.client_calendar_ids["client_b"],
        )
        gid = proj["google_event_id"]
        # The client deletes our busy block; no reconcile runs to see it.
        s.google.delete_event(s.cal("client_b"), gid)

        out = await s.run_observation("alice")
        assert out["missing_reset"] == 1, out
        assert out["divergent"] >= 1, out

        # Marked exactly like client ingest's reset-for-recreate.
        fresh = await _projection(
            s, source_event_id="obskickoff01", target_kind="client",
            target_calendar_id=user.client_calendar_ids["client_b"],
        )
        assert fresh["current_state"] == "absent"
        assert fresh["google_event_id"] is None
        assert fresh["applied_ledger_version"] is None

        await s.run_reconciler_until_quiescent("alice")
        healed = await _projection(
            s, source_event_id="obskickoff01", target_kind="client",
            target_calendar_id=user.client_calendar_ids["client_b"],
        )
        assert healed["current_state"] == "present"
        live = s.google.get_event(s.cal("client_b"), healed["google_event_id"])
        assert live["status"] == "confirmed"
        assert live["summary"] == "Busy"
        await _assert_zero_op(s, "post-heal")
        # And the audit agrees the world is consistent again.
        again = await s.run_observation("alice")
        assert again["divergent"] == 0, again


async def test_edited_busy_block_detected_and_reverted():
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_event("client_a", summary="Kickoff", event_id="obsedit01")
        await s.run_reconciler_until_quiescent("alice")

        proj = await _projection(
            s, source_event_id="obsedit01", target_kind="client",
            target_calendar_id=user.client_calendar_ids["client_b"],
        )
        gid, stored_etag = proj["google_event_id"], proj["google_etag"]
        # The client edits our busy block in place (etag bumps).
        body = dict(s.google.get_event(s.cal("client_b"), gid))
        body["summary"] = "Totally not busy"
        s.google.update_event(s.cal("client_b"), gid, body)

        out = await s.run_observation("alice")
        assert out["drift_marked"] == 1, out

        fresh = await _projection(
            s, source_event_id="obsedit01", target_kind="client",
            target_calendar_id=user.client_calendar_ids["client_b"],
        )
        assert fresh["applied_ledger_version"] is None
        # Client targets refresh the etag for a direct one-pass heal
        # (mirroring client ingest's drift revert).
        assert fresh["google_etag"] != stored_etag

        await s.run_reconciler_until_quiescent("alice")
        live = s.google.get_event(s.cal("client_b"), gid)
        assert live["summary"] == "Busy"
        await _assert_zero_op(s, "post-revert")


async def test_edited_main_copy_keeps_stale_etag_and_still_heals():
    """MAIN-target drift is marked WITHOUT refreshing the stored etag:
    the corrective update must 412 → supersede → re-run so main ingest
    gets a full cycle to classify any un-ingested user edit first (the
    RSVP-writeback ordering guarantee).  A personal-source busy copy is
    used so the expected outcome is unambiguous: personal sources are
    read-only, the edit is pure drift and must be reverted."""
    async with scenario() as s:
        s.given_calendar("main")
        s.given_calendar("personal_cal")
        await s.given_user("alice", main="main", personals=["personal_cal"])
        s.given_event("personal_cal", summary="Dentist", event_id="obspers01")
        await s.run_reconciler_until_quiescent("alice")

        proj = await _projection(
            s, source_event_id="obspers01", target_kind="main",
        )
        gid, stored_etag = proj["google_event_id"], proj["google_etag"]
        body = dict(s.google.get_event(s.cal("main"), gid))
        body["summary"] = "Renamed by hand"
        s.google.update_event(s.cal("main"), gid, body)

        out = await s.run_observation("alice")
        assert out["drift_marked"] == 1, out

        fresh = await _projection(
            s, source_event_id="obspers01", target_kind="main",
        )
        assert fresh["applied_ledger_version"] is None
        # Deliberately stale: the 412 path owns the heal ordering.
        assert fresh["google_etag"] == stored_etag

        await s.run_reconciler_until_quiescent("alice")
        live = s.google.get_event(s.cal("main"), gid)
        assert live["summary"] == "Busy (personal)"
        await _assert_zero_op(s, "post-main-revert")


# ---------------------------------------------------------------------------
# Absent-direction: the delete that never really happened
# ---------------------------------------------------------------------------
async def test_phantom_absent_projection_marked_and_deleted():
    """A projection converged as absent (delete recorded as success)
    while the event is in fact LIVE on Google — the 404-as-success
    class.  Today no ingest path can heal this (client ingest's drift
    revert nulls the version, but the diff no-ops an absent projection
    whose current_state is 'absent'); the audit records the observed
    truth (current_state='present') so the delete actually re-fires."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        db = await s.setup_db()
        client_b_id = user.client_calendar_ids["client_b"]

        ev = await (await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, source_type, source_calendar_id,
                   status, version, summary, created_at, updated_at)
               VALUES (?, 'client:1:obsphantom', 'client', ?, 'cancelled', 2,
                       'Busy', '2026-01-01', '2026-01-01') RETURNING id""",
            (user.user_id, user.client_calendar_ids["client_a"]),
        )).fetchone()
        proj = await (await db.execute(
            """INSERT INTO ledger_projections
                  (ledger_event_id, target_kind, target_calendar_id,
                   desired_state, desired_payload_hash, desired_ledger_version,
                   current_state, applied_payload_hash, applied_ledger_version)
               VALUES (?, 'client', ?, 'absent', 'absent', 2,
                       'absent', 'absent', 2) RETURNING id""",
            (int(ev["id"]), client_b_id),
        )).fetchone()
        pid = int(proj["id"])
        gid = derive_google_event_id(pid)
        # The event our "successful" delete supposedly removed.
        inserted = s.google.insert_event(
            s.cal("client_b"),
            {
                "id": gid, "summary": "Busy",
                "start": {"dateTime": "2026-03-02T09:00:00Z"},
                "end": {"dateTime": "2026-03-02T09:30:00Z"},
            },
        )
        await db.execute(
            "UPDATE ledger_projections SET google_event_id = ?, google_etag = ? "
            "WHERE id = ?",
            (gid, inserted.get("etag"), pid),
        )
        await db.commit()

        out = await s.run_observation("alice")
        assert out["phantom_marked"] == 1, out

        row = await (await db.execute(
            "SELECT current_state, applied_ledger_version "
            "FROM ledger_projections WHERE id = ?", (pid,),
        )).fetchone()
        assert row["current_state"] == "present"
        assert row["applied_ledger_version"] is None

        await s.run_reconciler_until_quiescent("alice")
        live = s.google.get_event(s.cal("client_b"), gid)
        assert live["status"] == "cancelled", "phantom copy was not deleted"
        await _assert_zero_op(s, "post-phantom-delete")


# ---------------------------------------------------------------------------
# Instance observation: observed ids beat derived ids
# ---------------------------------------------------------------------------
async def _move_source_occurrence(
    s: Scenario, cal_nick: str, series_id: str, stamp: str, new_time: str,
) -> dict:
    """Move one occurrence of a source series on its own calendar
    (materialises an override at the derived id, like the Google UI)."""
    iid = f"{series_id}_{stamp}"
    body = dict(s.google.get_event(s.cal(cal_nick), iid))
    body["start"] = {"dateTime": new_time}
    end = new_time[:11] + f"{int(new_time[11:13]) + 1:02d}" + new_time[13:]
    body["end"] = {"dateTime": end}
    return s.google.update_event(s.cal(cal_nick), iid, body)


async def test_wrong_stored_instance_id_corrected_from_observation():
    """An instance projection 'converged' against a wrongly-derived id
    never delivered to the real occurrence.  The parent scan records
    the OBSERVED id over the stored one and marks the row, and the next
    reconcile asserts our payload against the true occurrence."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsseries01",
        )
        await s.run_reconciler_until_quiescent("alice")
        await _move_source_occurrence(
            s, "client_a", "obsseries01",
            "20260216T090000Z", "2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice")

        client_b_id = user.client_calendar_ids["client_b"]
        child = await _projection(
            s, source_event_id="obsseries01_20260216T090000Z",
            target_kind="client", target_calendar_id=client_b_id,
            original_start="2026-02-16",
        )
        true_id = child["google_event_id"]
        parent = await _projection(
            s, source_event_id="obsseries01", target_kind="client",
            target_calendar_id=client_b_id,
        )
        # Historical wrong derivation: the stored id points at a
        # DIFFERENT (valid, live) occurrence of the same busy series.
        wrong_id = f"{parent['google_event_id']}_20260223T090000Z"
        assert wrong_id != true_id
        db = await s.setup_db()
        await db.execute(
            "UPDATE ledger_projections SET google_event_id = ? WHERE id = ?",
            (wrong_id, int(child["id"])),
        )
        await db.commit()

        out = await s.run_observation("alice")
        assert out["instance_ids_corrected"] == 1, out

        fresh = await _projection(
            s, source_event_id="obsseries01_20260216T090000Z",
            target_kind="client", target_calendar_id=client_b_id,
            original_start="2026-02-16",
        )
        assert fresh["google_event_id"] == true_id
        assert fresh["applied_ledger_version"] is None

        await s.run_reconciler_until_quiescent("alice")
        live = s.google.get_event(s.cal("client_b"), true_id)
        assert live["status"] == "confirmed"
        assert live["start"].get("dateTime", "").startswith("2026-02-16T14:00")
        # The wrong slot was never touched: still a clean synthesized
        # occurrence at 09:00, not an override at 14:00.
        other = s.google.get_event(s.cal("client_b"), wrong_id)
        assert other["start"].get("dateTime", "").startswith("2026-02-23T09:00")
        await _assert_zero_op(s, "post-id-correction")


async def test_tampered_instance_with_ledger_row_marked_for_revive():
    """A client cancels one occurrence of our busy series for an
    occurrence we track (a moved instance): the parent scan sees the
    cancelled override and marks the projection so the diff's
    status:confirmed update revives it."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsseries02",
        )
        await s.run_reconciler_until_quiescent("alice")
        await _move_source_occurrence(
            s, "client_a", "obsseries02",
            "20260216T090000Z", "2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice")

        client_b_id = user.client_calendar_ids["client_b"]
        child = await _projection(
            s, source_event_id="obsseries02_20260216T090000Z",
            target_kind="client", target_calendar_id=client_b_id,
            original_start="2026-02-16",
        )
        # The client deletes exactly that busy occurrence.
        s.google.delete_event(s.cal("client_b"), child["google_event_id"])

        out = await s.run_observation("alice")
        assert (
            out["instance_revive_marked"] + out["missing_reset"] >= 1
        ), out
        assert out["divergent"] >= 1, out

        await s.run_reconciler_until_quiescent("alice")
        live = s.google.get_event(s.cal("client_b"), child["google_event_id"])
        assert live["status"] == "confirmed", "occurrence was not revived"
        await _assert_zero_op(s, "post-revive")


async def test_tampered_unmatched_occurrence_counted_not_healed():
    """A client cancels an occurrence we have NO ledger row for (an
    unmodified occurrence of the busy series).  Healing needs the
    instance-level-restore machinery this codebase deliberately defers
    — the audit makes the tampering VISIBLE (counter + warning) and
    must not mint rows or mark anything."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsseries03",
        )
        await s.run_reconciler_until_quiescent("alice")

        client_b_id = user.client_calendar_ids["client_b"]
        parent = await _projection(
            s, source_event_id="obsseries03", target_kind="client",
            target_calendar_id=client_b_id,
        )
        s.google.delete_event(
            s.cal("client_b"), f"{parent['google_event_id']}_20260216T090000Z",
        )

        out = await s.run_observation("alice")
        assert out["tampered_unmatched"] >= 1, out
        assert out["divergent"] == 0, out
        db = await s.setup_db()
        row = await (await db.execute(
            """SELECT COUNT(*) AS n FROM ledger_events
                WHERE user_id = ? AND canonical_uid LIKE '%:inst:%'""",
            (user.user_id,),
        )).fetchone()
        assert int(row["n"]) == 0, "audit minted a phantom instance row"


async def test_cancelled_occurrence_revived_on_target_is_redeleted():
    """The source cancelled an occurrence and our delete converged —
    then the occurrence comes back to life on the target (user restore,
    or real Google dropping the exception on an RRULE edit).  The
    parent scan re-fires the delete, gated on the ledger row being
    cancelled (source truth)."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsseries04",
        )
        await s.run_reconciler_until_quiescent("alice")
        # Source cancels occurrence 3.
        s.google.delete_event(
            s.cal("client_a"), "obsseries04_20260216T090000Z",
        )
        await s.run_reconciler_until_quiescent("alice")

        client_b_id = user.client_calendar_ids["client_b"]
        child = await _projection(
            s, source_event_id="obsseries04_20260216T090000Z",
            target_kind="client", target_calendar_id=client_b_id,
            original_start="2026-02-16",
        )
        assert child["ledger_status"] == "cancelled"
        assert child["desired_state"] == "absent"
        gid = child["google_event_id"]
        cancelled = s.google.get_event(s.cal("client_b"), gid)
        assert cancelled["status"] == "cancelled"

        # A consistent cancelled occurrence must NOT be touched.
        quiet = await s.run_observation("alice")
        assert quiet["divergent"] == 0, quiet
        await _assert_zero_op(s, "post-quiet-observation")

        # Now the occurrence comes back to life on the target.
        body = dict(cancelled)
        body["status"] = "confirmed"
        s.google.update_event(s.cal("client_b"), gid, body)

        out = await s.run_observation("alice")
        assert out["instance_delete_marked"] == 1, out

        await s.run_reconciler_until_quiescent("alice")
        live = s.google.get_event(s.cal("client_b"), gid)
        assert live["status"] == "cancelled", "revived occurrence not re-deleted"
        await _assert_zero_op(s, "post-redelete")


async def test_structurally_absent_instance_never_touched_by_scan():
    """THE synthesis trap: a desired-absent instance projection whose
    ledger row is still ACTIVE (structurally absent — e.g. gid never
    assigned) sits under a live parent whose ``events.instances``
    listing contains a SYNTHESIZED live occurrence at that slot.  The
    scan must not record the synthesized id or mark the row — doing so
    would make the diff cancel a live occurrence of our own series."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsseries05",
        )
        await s.run_reconciler_until_quiescent("alice")

        db = await s.setup_db()
        client_b_id = user.client_calendar_ids["client_b"]
        parent_row = await (await db.execute(
            "SELECT id, canonical_uid FROM ledger_events "
            "WHERE user_id = ? AND source_event_id = 'obsseries05'",
            (user.user_id,),
        )).fetchone()
        # An ACTIVE instance row whose busy projection is structurally
        # absent and converged, with no Google id ever assigned.
        inst = await (await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid,
                   source_type, source_calendar_id, status, version,
                   summary, recurrence_instance_original_start,
                   created_at, updated_at)
               VALUES (?, ?, ?, 'client', ?, 'active', 1, 'Weekly',
                       '2026-02-16T09:00:00+00:00', '2026-01-01',
                       '2026-01-01') RETURNING id""",
            (
                user.user_id,
                f"{parent_row['canonical_uid']}:inst:2026-02-16T09:00:00Z",
                parent_row["canonical_uid"],
                user.client_calendar_ids["client_a"],
            ),
        )).fetchone()
        proj = await (await db.execute(
            """INSERT INTO ledger_projections
                  (ledger_event_id, target_kind, target_calendar_id,
                   desired_state, desired_payload_hash,
                   desired_ledger_version, current_state,
                   applied_payload_hash, applied_ledger_version)
               VALUES (?, 'client', ?, 'absent', 'absent', 1,
                       'absent', 'absent', 1) RETURNING id""",
            (int(inst["id"]), client_b_id),
        )).fetchone()
        await db.commit()

        out = await s.run_observation("alice")
        assert out["divergent"] == 0, out
        assert out["instance_ids_corrected"] == 0, out
        assert out["instance_delete_marked"] == 0, out

        row = await (await db.execute(
            "SELECT google_event_id, applied_ledger_version, current_state "
            "FROM ledger_projections WHERE id = ?",
            (int(proj["id"]),),
        )).fetchone()
        assert row["google_event_id"] is None, (
            "scan recorded a synthesized occurrence id onto a "
            "structurally-absent instance projection"
        )
        assert row["applied_ledger_version"] == 1
        assert row["current_state"] == "absent"

        # And the live occurrence on the busy series stayed live.
        busy_parent = await _projection(
            s, source_event_id="obsseries05", target_kind="client",
            target_calendar_id=client_b_id,
        )
        await s.run_reconciler_until_quiescent("alice")
        occ = s.google.get_event(
            s.cal("client_b"),
            f"{busy_parent['google_event_id']}_20260216T090000Z",
        )
        assert occ["status"] == "confirmed"


# ---------------------------------------------------------------------------
# Sampling mechanics
# ---------------------------------------------------------------------------
async def test_rotation_sweeps_oldest_observation_first():
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_event("client_a", summary="One", event_id="obsrot01")
        s.given_event("client_a", summary="Two", event_id="obsrot02")
        await s.run_reconciler_until_quiescent("alice")

        db = await s.setup_db()

        async def observed_count() -> int:
            row = await (await db.execute(
                """SELECT COUNT(*) AS n FROM ledger_projections p
                     JOIN ledger_events e ON e.id = p.ledger_event_id
                    WHERE e.user_id = ? AND p.last_observed_at IS NOT NULL""",
                (user.user_id,),
            )).fetchone()
            return int(row["n"])

        assert (await s.run_observation("alice", sample_size=1))["checked"] <= 1
        first = await observed_count()
        assert first == 1
        await s.run_observation("alice", sample_size=1)
        assert await observed_count() == 2, (
            "second pass re-inspected the already-observed row instead of "
            "rotating to the next one"
        )


async def test_settle_window_skips_recently_written_rows():
    """Rows whose projection changed within the quiet window are not
    inspected — our own write may not be visible to a Google read
    replica yet, and a false 'missing' would trigger a spurious
    re-assert."""
    from datetime import timedelta

    from app.ledger.observe import DEFAULT_MIN_QUIET_SECONDS, observe_user

    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_event("client_a", summary="Fresh", event_id="obsfresh01")
        await s.run_reconciler_until_quiescent("alice")

        db = await s.setup_db()
        mapping = {
            cid: s.cal(nick)
            for nick, cid in user.client_calendar_ids.items()
        }
        common = dict(
            user_id=user.user_id,
            main_google_calendar_id=user.main_google_calendar_id,
            google_calendar_id_for=mapping,
            sample_size=100,
        )
        # The projections were stamped 'now' (outbox timestamps run on
        # the scenario clock) — inside the window, so nothing is
        # inspected yet.
        out = await observe_user(db, s.google, now=s.clock.now(), **common)
        assert out["checked"] == 0, out
        # Past the settle window the same rows are inspected.
        later = s.clock.now() + timedelta(seconds=DEFAULT_MIN_QUIET_SECONDS + 60)
        out = await observe_user(db, s.google, now=later, **common)
        assert out["checked"] > 0, out


# ---------------------------------------------------------------------------
# Ingest-side observation (item 2's second surface): a managed main-copy
# exception delivered by sync records its REAL id over the stored one.
# ---------------------------------------------------------------------------
async def test_main_ingest_records_observed_instance_id():
    """The user drags one occurrence of a managed copy on main; a later
    delivery of that exception must correct an instance projection that
    holds a different (wrongly-derived) id — observation as truth."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_recurring_event(
            "client_a", summary="Weekly",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4", event_id="obsmain01",
        )
        await s.run_reconciler_until_quiescent("alice")

        main_parent = await _projection(
            s, source_event_id="obsmain01", target_kind="main",
        )
        # The user drags occurrence 3 of the managed copy on main.
        main_iid = f"{main_parent['google_event_id']}_20260216T090000Z"
        body = dict(s.google.get_event(s.cal("main"), main_iid))
        body["start"] = {"dateTime": "2026-02-16T15:00:00Z"}
        body["end"] = {"dateTime": "2026-02-16T16:00:00Z"}
        s.google.update_event(s.cal("main"), main_iid, body)
        await s.run_reconciler_until_quiescent("alice")

        child_main = await _projection(
            s, source_event_id="obsmain01_20260216T090000Z",
            target_kind="main", original_start="2026-02-16",
        )
        assert child_main["google_event_id"] == main_iid

        # Corrupt the stored id (historical wrong derivation), then
        # deliver the exception again by touching it on main so the
        # next ingest re-sees it.
        db = await s.setup_db()
        await db.execute(
            """UPDATE ledger_projections SET google_event_id = ?
                WHERE id = ?""",
            (f"{main_parent['google_event_id']}_20260223T090000Z",
             int(child_main["id"])),
        )
        await db.commit()
        body2 = dict(s.google.get_event(s.cal("main"), main_iid))
        body2["start"] = {"dateTime": "2026-02-16T15:30:00Z"}
        body2["end"] = {"dateTime": "2026-02-16T16:30:00Z"}
        s.google.update_event(s.cal("main"), main_iid, body2)
        await s.run_reconciler_until_quiescent("alice")

        healed = await _projection(
            s, source_event_id="obsmain01_20260216T090000Z",
            target_kind="main", original_start="2026-02-16",
        )
        assert healed["google_event_id"] == main_iid, (
            "main ingest did not record the observed exception id over "
            "the wrongly-stored one"
        )
        await _assert_zero_op(s, "post-ingest-id-record")


async def test_record_observed_main_instance_id_unit():
    """Unit contract of the recording helper: corrects a differing or
    NULL stored id (nulling the applied stamp only when the row had
    claimed convergence), no-ops on a matching id, never touches
    non-main projections."""
    from app.ledger.ingest.main import _record_observed_main_instance_id

    async with scenario() as s:
        user = await _alice_two_clients(s)
        db = await s.setup_db()
        ev = await (await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid, source_type,
                   status, version, summary, created_at, updated_at)
               VALUES (?, 'client:1:p:inst:x', 'client:1:p', 'client',
                       'active', 1, 'Weekly', '2026-01-01', '2026-01-01')
               RETURNING id""",
            (user.user_id,),
        )).fetchone()
        lid = int(ev["id"])
        main_proj = await (await db.execute(
            """INSERT INTO ledger_projections
                  (ledger_event_id, target_kind, desired_state,
                   desired_payload_hash, desired_ledger_version,
                   current_state, google_event_id, google_etag,
                   applied_payload_hash, applied_ledger_version)
               VALUES (?, 'main', 'present_full', 'h', 1,
                       'present', 'bbwrongid_20260101', 'e-1', 'h', 1)
               RETURNING id""",
            (lid,),
        )).fetchone()
        client_proj = await (await db.execute(
            """INSERT INTO ledger_projections
                  (ledger_event_id, target_kind, target_calendar_id,
                   desired_state, desired_payload_hash,
                   desired_ledger_version, current_state,
                   google_event_id, applied_payload_hash,
                   applied_ledger_version)
               VALUES (?, 'client', ?, 'present_busy', 'h', 1,
                       'present', 'bbclientid_20260101', 'h', 1)
               RETURNING id""",
            (lid, user.client_calendar_ids["client_b"]),
        )).fetchone()
        await db.commit()

        await _record_observed_main_instance_id(
            db, ledger_event_id=lid, observed_id="bbtrueid_20260101",
        )
        row = await (await db.execute(
            "SELECT * FROM ledger_projections WHERE id = ?",
            (int(main_proj["id"]),),
        )).fetchone()
        assert row["google_event_id"] == "bbtrueid_20260101"
        assert row["applied_ledger_version"] is None, (
            "a projection converged against the wrong id must be re-asserted"
        )
        assert row["google_etag"] is None

        # Idempotent on a matching id: restore a converged stamp and
        # verify the second call leaves it alone.
        await db.execute(
            "UPDATE ledger_projections SET applied_ledger_version = 1 "
            "WHERE id = ?", (int(main_proj["id"]),),
        )
        await _record_observed_main_instance_id(
            db, ledger_event_id=lid, observed_id="bbtrueid_20260101",
        )
        row = await (await db.execute(
            "SELECT applied_ledger_version FROM ledger_projections WHERE id = ?",
            (int(main_proj["id"]),),
        )).fetchone()
        assert row["applied_ledger_version"] == 1

        # A NULL stored id (post-404 reset) is recorded over, without
        # inventing an applied stamp.
        await db.execute(
            "UPDATE ledger_projections SET google_event_id = NULL, "
            "applied_ledger_version = NULL WHERE id = ?",
            (int(main_proj["id"]),),
        )
        await _record_observed_main_instance_id(
            db, ledger_event_id=lid, observed_id="bbtrueid_20260101",
        )
        row = await (await db.execute(
            "SELECT google_event_id, applied_ledger_version "
            "FROM ledger_projections WHERE id = ?",
            (int(main_proj["id"]),),
        )).fetchone()
        assert row["google_event_id"] == "bbtrueid_20260101"
        assert row["applied_ledger_version"] is None

        # The client-target projection is never touched.
        row = await (await db.execute(
            "SELECT google_event_id, applied_ledger_version "
            "FROM ledger_projections WHERE id = ?",
            (int(client_proj["id"]),),
        )).fetchone()
        assert row["google_event_id"] == "bbclientid_20260101"
        assert row["applied_ledger_version"] == 1


# ---------------------------------------------------------------------------
# Guard rails
# ---------------------------------------------------------------------------
async def test_permanently_failed_and_pending_rows_not_sampled():
    """Permanently-failed rows are the operator surface (the admin
    retry owns them) and rows with queued outbox work are about to
    change — the audit must skip both."""
    async with scenario() as s:
        user = await _alice_two_clients(s)
        s.given_event("client_a", summary="Kickoff", event_id="obsguard01")
        await s.run_reconciler_until_quiescent("alice")

        db = await s.setup_db()
        proj = await _projection(
            s, source_event_id="obsguard01", target_kind="client",
            target_calendar_id=user.client_calendar_ids["client_b"],
        )
        await db.execute(
            "UPDATE ledger_projections SET permanently_failed = 1 WHERE id = ?",
            (int(proj["id"]),),
        )
        await db.commit()
        # Delete the event: a failed row must NOT be resurrected by the
        # audit even though its copy is genuinely gone.
        s.google.delete_event(s.cal("client_b"), proj["google_event_id"])

        out = await s.run_observation("alice")
        row = await (await db.execute(
            "SELECT permanently_failed, applied_ledger_version, "
            "last_observed_at FROM ledger_projections WHERE id = ?",
            (int(proj["id"]),),
        )).fetchone()
        assert row["permanently_failed"] == 1
        assert row["applied_ledger_version"] is not None
        assert row["last_observed_at"] is None
        assert out["missing_reset"] == 0, out
