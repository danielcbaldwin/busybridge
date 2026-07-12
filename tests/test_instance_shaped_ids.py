"""Events whose id has the derived-instance shape ``<base>_<stamp>``.

Two latent production bugs around ids shaped
``<base>_<YYYYMMDDTHHMMSSZ>`` (timed) / ``<base>_<YYYYMMDD>`` (all-day)
arriving in ingest:

* **Bug 1 — detached cancellations were dropped (ghost occurrences).**
  Real Google sometimes delivers a cancelled recurring-instance
  exception WITHOUT ``recurringEventId`` — observed in production when
  a series is split/truncated so the master no longer generates the
  overridden occurrence ("Google keeps that id only as a *cancelled*
  exception with no recurringEventId", see
  ``app.ledger.outbox._retire_orphaned_instance_tombstone``).  Ingest
  routed instances only via ``recurringEventId``, so the detached form
  fell to the top-level cancelled branch, matched no canonical uid,
  and was silently skipped: the stale override outlived its series as
  a ghost busy block, and main-ingest's churn-breaker then actively
  re-created the ghost mirror.  Fixed by
  ``client._maybe_ingest_detached_cancellation`` (shared by client,
  personal, and native-main ingest).

* **Bug 2 — single-occurrence tampering with OUR busy blocks minted a
  phantom ledger row.**  When the client deletes or moves ONE
  occurrence of a busy-block series we wrote, Google delivers
  ``<bb-id>_<stamp>`` with ``recurringEventId = <bb-id>``; neither the
  exact projection lookup (parent id) nor
  ``is_managed_google_event_id`` (suffix breaks the strict shape)
  recognised it, so the instance handler minted a phantom row parented
  to a canonical uid that can never exist.  Fixed: never minted, a
  WARNING names the tampered occurrence, and it is counted
  (``tampered_managed_instance``).  The occurrence itself is NOT
  auto-healed — re-asserting the parent cannot clear an exception, and
  an instance-level heal would need exactly the ledger row that must
  not be minted — so the warn-and-skip is asserted here.
"""

from __future__ import annotations

import logging
from datetime import timezone

import pytest

from app.ledger.identity import is_managed_google_event_id
from app.ledger.ingest.client import (
    _ingest_one_event,
    _split_instance_shaped_id,
    _stamp_to_original_start,
)
from tests.integration.framework import Scenario
from tests.test_adversarial_recurrence import (
    _assert_zero_op_steady_state,
    _ledger_invariants,
    _occurrences,
    scenario,
)

pytestmark = pytest.mark.asyncio

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _three_cal_alice(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    return await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )


def _managed_series_id(s: Scenario, nick: str) -> str:
    """The id of the (single) managed recurring series on a calendar."""
    for ev in s.list_events(nick):
        if ev.get("recurrence") and is_managed_google_event_id(ev.get("id")):
            return ev["id"]
    raise AssertionError(f"no managed recurring series found on {nick!r}")


async def _ledger_row_count(s: Scenario, user_id: int) -> int:
    db = await s.setup_db()
    row = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    return int(row["n"])


async def _instance_row_status(
    s: Scenario, user_id: int, parent_canonical: str, original_start: str,
):
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT status FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid = ?
              AND recurrence_instance_original_start = ?""",
        (user_id, parent_canonical, original_start),
    )).fetchone()
    return None if row is None else row["status"]


# ---------------------------------------------------------------------------
# The shape helper itself: strict on exactly Google's two stamp forms.
# ---------------------------------------------------------------------------
async def test_split_instance_shaped_id_is_strict():
    # The two real forms.
    assert _split_instance_shaped_id("abc123_20260216T090000Z") == (
        "abc123", "20260216T090000Z",
    )
    assert _split_instance_shaped_id("abc123_20260216") == (
        "abc123", "20260216",
    )
    # An instance OF a "_R" this-and-following segment splits at the
    # LAST underscore, keeping the segment id intact as the base.
    assert _split_instance_shaped_id(
        "abc_r20260101t000000z_20260216T090000Z"
    ) == ("abc_r20260101t000000z", "20260216T090000Z")

    # Everything else must NOT match.
    for bad in (
        None,
        "",
        "abc",
        "abc_",
        "_20260216",                    # empty base
        "abc_R20260216T090000Z",        # segment marker, not a stamp
        "abc_20260216T090000",          # timed stamp without Z
        "abc_2026021",                  # 7 digits
        "abc_202602166",                # 9 digits
        "abc_20261340",                 # month 13 / day 40
        "abc_20260216T256161Z",         # hour 25, minute/second 61
        "abc_20260216x",                # trailing junk
    ):
        assert _split_instance_shaped_id(bad) is None, bad


async def test_stamp_to_original_start_forms():
    assert _stamp_to_original_start("20260216T090000Z") == (
        "2026-02-16T09:00:00Z", False,
    )
    assert _stamp_to_original_start("20260216") == ("2026-02-16", True)


# ---------------------------------------------------------------------------
# Bug 1 guard-rail: an instance-shaped id whose base has NO parent
# ledger row keeps the conservative skip (never misrouted).
# ---------------------------------------------------------------------------
async def test_detached_cancellation_without_parent_row_keeps_skip():
    async with scenario() as s:
        s.given_calendar("main")
        s.given_calendar("client_a")
        user = await s.given_user("alice", main="main", clients=["client_a"])
        db = await s.setup_db()

        outcome, ledger_id = await _ingest_one_event(
            db,
            user_id=user.user_id,
            client_calendar_id=user.client_calendar_ids["client_a"],
            user_email=user.email,
            event={"id": "orphanbase_20260216T090000Z", "status": "cancelled"},
        )
        assert (outcome, ledger_id) == ("skipped", None)
        assert await _ledger_row_count(s, user.user_id) == 0, (
            "a detached cancellation with no parent ledger row must not "
            "mint anything"
        )


# ---------------------------------------------------------------------------
# Bug 1 — client source: the ghost-occurrence end-to-end scenario.
# ---------------------------------------------------------------------------
async def test_detached_cancellation_cancels_ghost_occurrence_client():
    """Recurring client series with a moved instance; the series is
    truncated before the moved occurrence and Google delivers the
    override's cancellation DETACHED (no recurringEventId).  The
    ledger instance row must go cancelled, busy blocks for that
    occurrence must disappear everywhere, and no ghost may be
    re-created by main ingest on later reconciles."""
    async with scenario() as s:
        user = await _three_cal_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Detach probe",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=6",
            event_id="detachseries01",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Move the 2026-02-16 occurrence so an override exists.
        s.update_event(
            "client_a", f"{series['id']}_20260216T090000Z",
            start="2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        assert "2026-02-16T14:00:00Z" in _occurrences(
            s, "client_b", managed_only=True,
        ), "precondition: moved busy block exists on the peer"

        # Organizer truncates the series BEFORE the moved occurrence.
        s.update_event(
            "client_a", series["id"],
            recurrence=["RRULE:FREQ=WEEKLY;UNTIL=20260209T235959Z"],
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)
        # (Documented fake gap: the out-of-range override stays live on
        # the source until Google's cancellation arrives, so the peer
        # rightly still mirrors it.)
        assert "2026-02-16T14:00:00Z" in _occurrences(
            s, "client_b", managed_only=True,
        )

        # Real Google now keeps the override id only as a *cancelled*
        # exception with NO recurringEventId (the production shape from
        # outbox._retire_orphaned_instance_tombstone).
        s.google.detach_cancelled_instance(
            s.cal("client_a"), f"{series['id']}_20260216T090000Z",
        )
        passes = await s.run_reconciler_until_quiescent("alice", max_passes=6)
        assert passes[0]["enqueued"] > 0, (
            "precondition: the detached cancellation must arm the "
            "mirror deletes (it used to be silently dropped)"
        )

        # The ledger instance row went cancelled.
        cid_a = user.client_calendar_ids["client_a"]
        status = await _instance_row_status(
            s, user.user_id,
            f"client:{cid_a}:{series['id']}", "2026-02-16T09:00:00Z",
        )
        assert status == "cancelled", (
            f"detached cancellation did not reach the instance ledger "
            f"row (status={status!r})"
        )

        # Busy blocks / copies for the occurrence are gone everywhere.
        live = {"2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z"}
        for nick, kw in (("client_b", dict(managed_only=True)),
                         ("main", dict(summary_contains="Detach probe"))):
            occ = _occurrences(s, nick, **kw)
            assert set(occ) == live, (
                f"ghost occurrence still on {nick}: {sorted(occ)}"
            )

        # Absorb the known FINDING-1 echo pass, then demand steady state.
        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"detached cancel (echo pass absorbed {echo['enqueued']} ops)",
        )

        # NO ghost re-creation by main ingest across two more reconciles.
        for i in range(2):
            out = await s.run_reconciler("alice")
            assert out["enqueued"] == 0, (
                f"reconcile {i + 1} after convergence enqueued "
                f"{out['enqueued']} op(s)"
            )
            for nick, kw in (("client_b", dict(managed_only=True)),
                             ("main", dict(summary_contains="Detach probe"))):
                occ = _occurrences(s, nick, **kw)
                assert "2026-02-16T14:00:00Z" not in occ, (
                    f"ghost RE-CREATED on {nick} at pass {i + 1}"
                )
                assert "2026-02-16T09:00:00Z" not in occ, (
                    f"original slot resurrected on {nick} at pass {i + 1}"
                )

        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Bug 1 — personal source: the same routing gap existed there.
# ---------------------------------------------------------------------------
async def test_detached_cancellation_personal_source():
    async with scenario() as s:
        s.given_calendar("main")
        s.given_calendar("client_a")
        s.given_calendar("personal_a")
        user = await s.given_user(
            "alice", main="main", clients=["client_a"],
            personals=["personal_a"],
        )
        series = s.given_recurring_event(
            "personal_a", summary="Gym block",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="detachpersonal1",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        s.update_event(
            "personal_a", f"{series['id']}_20260216T090000Z",
            start="2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        assert "2026-02-16T14:00:00Z" in _occurrences(
            s, "client_a", managed_only=True,
        ), "precondition: moved personal busy block exists on the client"

        s.update_event(
            "personal_a", series["id"],
            recurrence=["RRULE:FREQ=WEEKLY;UNTIL=20260209T235959Z"],
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        s.google.detach_cancelled_instance(
            s.cal("personal_a"), f"{series['id']}_20260216T090000Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        pid = user.personal_calendar_ids["personal_a"]
        status = await _instance_row_status(
            s, user.user_id,
            f"personal:{pid}:{series['id']}", "2026-02-16T09:00:00Z",
        )
        assert status == "cancelled", (
            f"detached PERSONAL cancellation dropped (status={status!r})"
        )

        live = {"2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z"}
        for nick in ("client_a", "main"):
            occ = _occurrences(s, nick, managed_only=True)
            assert set(occ) == live, (
                f"ghost personal busy block on {nick}: {sorted(occ)}"
            )

        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"personal detached cancel (echo absorbed {echo['enqueued']})",
        )


# ---------------------------------------------------------------------------
# Bug 1 — native main series: the same routing gap existed there.
# ---------------------------------------------------------------------------
async def test_detached_cancellation_native_main_series():
    async with scenario() as s:
        user = await _three_cal_alice(s)
        series = s.given_recurring_event(
            "main", summary="Native standup",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="detachnative01",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        s.update_event(
            "main", f"{series['id']}_20260216T090000Z",
            start="2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        assert "2026-02-16T14:00:00Z" in _occurrences(
            s, "client_a", managed_only=True,
        ), "precondition: moved native occurrence casts a busy block"

        s.update_event(
            "main", series["id"],
            recurrence=["RRULE:FREQ=WEEKLY;UNTIL=20260209T235959Z"],
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        s.google.detach_cancelled_instance(
            s.cal("main"), f"{series['id']}_20260216T090000Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        status = await _instance_row_status(
            s, user.user_id,
            f"main_native:{user.user_id}:{series['id']}",
            "2026-02-16T09:00:00Z",
        )
        assert status == "cancelled", (
            f"detached NATIVE-MAIN cancellation dropped (status={status!r})"
        )

        live = {"2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z"}
        for nick in ("client_a", "client_b"):
            occ = _occurrences(s, nick, managed_only=True)
            assert set(occ) == live, (
                f"ghost busy block for native occurrence on {nick}: "
                f"{sorted(occ)}"
            )

        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"native detached cancel (echo absorbed {echo['enqueued']})",
        )


# ---------------------------------------------------------------------------
# Bug 2 — the client DELETES one occurrence of OUR busy series.
# ---------------------------------------------------------------------------
async def test_tampered_delete_of_busy_occurrence_never_mints_phantom(caplog):
    async with scenario() as s:
        user = await _three_cal_alice(s)
        s.given_recurring_event(
            "client_a", summary="Tamper probe",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="tamperseries01",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        bb_id = _managed_series_id(s, "client_b")
        rows_before = await _ledger_row_count(s, user.user_id)

        # The client deletes ONE occurrence of our busy series on their
        # calendar → Google delivers <bb-id>_<stamp> with
        # recurringEventId = <bb-id>.
        s.google.delete_event(
            s.cal("client_b"), f"{bb_id}_20260216T090000Z",
        )
        with caplog.at_level(
            logging.WARNING, logger="app.ledger.ingest.client",
        ):
            out = await s.run_reconciler("alice")

        # Counted, warned, and NOT minted.
        cid_b = user.client_calendar_ids["client_b"]
        assert out["ingest"][f"client:{cid_b}"].get(
            "tampered_managed_instance"
        ) == 1, out["ingest"]
        assert f"{bb_id}_20260216T090000Z" in caplog.text, (
            "the WARNING must name the tampered occurrence"
        )
        assert await _ledger_row_count(s, user.user_id) == rows_before, (
            "tampering minted a ledger row"
        )
        db = await s.setup_db()
        phantom = await (await db.execute(
            """SELECT COUNT(*) AS n FROM ledger_events
                WHERE user_id = ? AND parent_canonical_uid = ?""",
            (user.user_id, f"client:{cid_b}:{bb_id}"),
        )).fetchone()
        assert int(phantom["n"]) == 0, (
            "phantom instance row parented to the never-existing "
            f"canonical uid client:{cid_b}:{bb_id}"
        )

        # Warn-and-skip (documented): the tampered occurrence is NOT
        # auto-healed — its busy block stays missing on client_b — but
        # the series is untouched otherwise, on every calendar.
        busy_b = _occurrences(s, "client_b", managed_only=True)
        assert set(busy_b) == {
            "2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
            "2026-02-23T09:00:00Z",
        }, f"series damaged beyond the tampered occurrence: {sorted(busy_b)}"

        full = {"2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
                "2026-02-16T09:00:00Z", "2026-02-23T09:00:00Z"}
        src = _occurrences(s, "client_a", summary_contains="Tamper probe")
        assert set(src) == full, (
            f"tampering leaked to the SOURCE series: {sorted(src)}"
        )
        main_occ = _occurrences(s, "main", summary_contains="Tamper probe")
        assert set(main_occ) == full, (
            f"tampering leaked to the main copy: {sorted(main_occ)}"
        )

        # Zero-op steady state: no churn, no phantom-driven diff loop.
        await _assert_zero_op_steady_state(s, "alice", "tamper delete A")
        await _assert_zero_op_steady_state(s, "alice", "tamper delete B")
        assert await _ledger_row_count(s, user.user_id) == rows_before

        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Bug 2 — the client MOVES one occurrence of OUR busy series.
# ---------------------------------------------------------------------------
async def test_tampered_move_of_busy_occurrence_never_mints_phantom(caplog):
    async with scenario() as s:
        user = await _three_cal_alice(s)
        s.given_recurring_event(
            "client_a", summary="Tamper probe",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="tamperseries02",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        bb_id = _managed_series_id(s, "client_b")
        rows_before = await _ledger_row_count(s, user.user_id)

        # The client drags ONE occurrence of our busy series to 16:00.
        s.update_event(
            "client_b", f"{bb_id}_20260216T090000Z",
            start="2026-02-16T16:00:00Z",
        )
        with caplog.at_level(
            logging.WARNING, logger="app.ledger.ingest.client",
        ):
            out = await s.run_reconciler("alice")

        cid_b = user.client_calendar_ids["client_b"]
        assert out["ingest"][f"client:{cid_b}"].get(
            "tampered_managed_instance"
        ) == 1, out["ingest"]
        assert f"{bb_id}_20260216T090000Z" in caplog.text
        assert await _ledger_row_count(s, user.user_id) == rows_before, (
            "tampering minted a ledger row"
        )
        db = await s.setup_db()
        phantom = await (await db.execute(
            """SELECT COUNT(*) AS n FROM ledger_events
                WHERE user_id = ? AND parent_canonical_uid = ?""",
            (user.user_id, f"client:{cid_b}:{bb_id}"),
        )).fetchone()
        assert int(phantom["n"]) == 0

        # Source and main are untouched.
        full = {"2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
                "2026-02-16T09:00:00Z", "2026-02-23T09:00:00Z"}
        assert set(
            _occurrences(s, "client_a", summary_contains="Tamper probe")
        ) == full
        assert set(
            _occurrences(s, "main", summary_contains="Tamper probe")
        ) == full

        # Warn-and-skip (documented): the moved busy block stays where
        # the client dragged it; the rest of the series is intact.
        busy_b = _occurrences(s, "client_b", managed_only=True)
        assert set(busy_b) == {
            "2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
            "2026-02-16T16:00:00Z", "2026-02-23T09:00:00Z",
        }, f"series damaged beyond the tampered occurrence: {sorted(busy_b)}"

        # Zero-op steady state — in particular the pre-fix phantom row
        # must not churn the diff forever.
        await _assert_zero_op_steady_state(s, "alice", "tamper move A")
        await _assert_zero_op_steady_state(s, "alice", "tamper move B")
        assert await _ledger_row_count(s, user.user_id) == rows_before
