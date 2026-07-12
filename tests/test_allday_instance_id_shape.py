"""Instance-ID stamp shape for timed<->all-day CONVERTED occurrences.

Google keys a recurring-instance id to the ORIGINAL occurrence slot:
an all-day series issues ``<parent>_YYYYMMDD`` ids and a timed series
``<parent>_YYYYMMDDTHHMMSSZ`` ids — and the id does NOT change when
the user converts that single occurrence to the other display form
(both are ordinary Google UI/API actions).

The latent bug: ``derive_instance_google_event_id`` used to pick the
stamp form from the caller-supplied ``is_all_day`` flag, and callers
passed the instance ROW's display shape (the override's).  For a
converted occurrence the flag and the stored original-slot string
disagree, so the ledger derived an id Google never issued:

* UPDATE on the wrong id -> 404 -> replan -> same wrong id -> silent
  infinite loop (the resurrected op resets attempts, never poisons);
* DELETE on the wrong id -> 404-treated-as-success -> phantom busy
  block forever.

These tests drive the FULL pipeline (ingest -> plan -> diff -> drain)
against the fake Google — whose ``_parse_instance_id`` is now STRICT
like real Google about stamp-vs-parent shape — and assert the derived
ids carry the ORIGINAL slot's shape, the mirrors converge, and a
reconcile with no source change enqueues 0 ops.
"""

from __future__ import annotations

import contextlib
from datetime import timezone

import pytest
from dateutil.parser import isoparse

from app.ledger.identity import (
    derive_instance_google_event_id as derive,
    is_managed_google_event_id,
)
from app.ledger.payload import EP_PROJ_ID
from tests.integration.framework import Scenario

UTC = timezone.utc


# ---------------------------------------------------------------------------
# (d) Unit tests: stamp form follows the SHAPE of original_start, not
# the caller's flag.
# ---------------------------------------------------------------------------
class TestDeriveStampShape:
    def test_date_shape_with_all_day_flag(self):
        assert derive("bbp", "2026-03-10", True) == "bbp_20260310"

    def test_date_shape_with_timed_flag_still_date_stamp(self):
        # THE bug direction: a date-only original slot whose override
        # is displayed timed (row flag False).  The old code derived
        # bbp_20260310T000000Z — an id Google never issued.
        assert derive("bbp", "2026-03-10", False) == "bbp_20260310"

    def test_timed_shape_with_timed_flag(self):
        assert derive("bbp", "2026-03-10T13:00:00Z", False) == (
            "bbp_20260310T130000Z"
        )

    def test_timed_shape_with_all_day_flag_still_timed_stamp(self):
        # THE bug, other direction: a timed original slot whose
        # override is displayed all-day (row flag True).  The old code
        # string-mangled the datetime into bbp_20260310 (date stamp) —
        # again an id Google never issued.
        assert derive("bbp", "2026-03-10T13:00:00Z", True) == (
            "bbp_20260310T130000Z"
        )

    def test_timed_shape_non_utc_offset_with_all_day_flag(self):
        # Shape detection must not bypass the UTC normalisation.
        assert derive("bbp", "2026-03-10T14:30:00-05:00", True) == (
            "bbp_20260310T193000Z"
        )

    def test_empty_falls_back_to_flag_all_day(self):
        # Empty input has no shape: the flag remains the (historical,
        # degenerate-but-deterministic) fallback discriminator.
        assert derive("bbp", "", True) == "bbp_"

    def test_empty_falls_back_to_flag_timed(self):
        assert derive("bbp", "", False) == "bbp_Z"


# ---------------------------------------------------------------------------
# End-to-end helpers (Scenario idioms per tests/test_adversarial_recurrence.py)
# ---------------------------------------------------------------------------
@contextlib.asynccontextmanager
async def scenario():
    """Scenario with GUARANTEED teardown (see test_adversarial_recurrence)."""
    s = Scenario()
    try:
        yield s
    finally:
        with contextlib.suppress(Exception):
            await s.close()


def _norm_start(ev: dict) -> str:
    st = ev.get("start") or {}
    if "dateTime" in st:
        return isoparse(st["dateTime"]).astimezone(UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return st.get("date") or ""


def _is_managed(ev: dict) -> bool:
    if is_managed_google_event_id(ev.get("id") or ""):
        return True
    priv = (ev.get("extendedProperties") or {}).get("private") or {}
    return bool(priv.get(EP_PROJ_ID))


def _occurrences(
    s: Scenario,
    nick: str,
    *,
    managed_only: bool = False,
    summary_contains: str | None = None,
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for ev in s.list_events(nick, single_events=True):
        if ev.get("status") == "cancelled":
            continue
        if managed_only and not _is_managed(ev):
            continue
        if summary_contains is not None and summary_contains not in (
            ev.get("summary") or ""
        ):
            continue
        out.setdefault(_norm_start(ev), []).append(ev["id"])
    return out


async def _assert_zero_op_steady_state(s: Scenario, user: str, label: str):
    out = await s.run_reconciler(user)
    assert out["enqueued"] == 0, (
        f"{label}: op churn — reconcile with NO source change enqueued "
        f"{out['enqueued']} op(s); planned={out['planned']} "
        f"drain={out['drain']} (the 404-replan loop looks exactly like "
        f"this: the same wrong-shape id re-enqueued every pass)"
    )
    return out


async def _instance_projection_ids(s: Scenario, user_id: int) -> list[str]:
    """Non-null google_event_ids of every instance projection row."""
    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT p.google_event_id AS gid
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND e.parent_canonical_uid IS NOT NULL
              AND p.google_event_id IS NOT NULL""",
        (user_id,),
    )).fetchall()
    return [r["gid"] for r in rows]


async def _two_client_alice(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    return await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )


# ---------------------------------------------------------------------------
# (a) + (b): all-day client series; one occurrence converted to a
# TIMED slot, then that same occurrence cancelled.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_allday_series_occurrence_converted_to_timed_then_cancelled():
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Offsite block",
            start="2026-03-03",                 # all-day (date only)
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="cnvallda000001",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        peer0 = _occurrences(s, "client_b", managed_only=True)
        assert set(peer0) == {
            "2026-03-03", "2026-03-10", "2026-03-17", "2026-03-24",
        }, f"baseline all-day mirror wrong: {sorted(peer0)}"

        # (a) Convert the 2026-03-10 occurrence to a TIMED slot on the
        # client.  Google keeps the instance id keyed to the original
        # ALL-DAY slot: <series>_20260310 (no time stamp).
        s.update_event(
            "client_a", f"{series['id']}_20260310",
            start="2026-03-10T13:00:00Z",
            end="2026-03-10T13:30:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Every derived instance id must use the DATE form of the
        # ORIGINAL slot — deriving from the override's timed display
        # shape (the old bug) yields ..._20260310T000000Z / T130000Z,
        # which real Google (and the now-strict fake) 404s forever.
        inst_ids = await _instance_projection_ids(s, user.user_id)
        assert inst_ids, "expected instance projections for the override"
        for gid in inst_ids:
            assert gid.endswith("_20260310"), (
                f"derived instance id {gid!r} does not use the original "
                f"slot's DATE stamp form"
            )
            assert "T" not in gid.rsplit("_", 1)[1], (
                f"derived instance id {gid!r} carries a timed stamp for "
                f"an all-day original slot"
            )

        # The mirror UPDATE actually landed: the busy block moved to
        # the timed slot on the peer, and the main copy followed.
        peer = _occurrences(s, "client_b", managed_only=True)
        assert set(peer) == {
            "2026-03-03", "2026-03-10T13:00:00Z",
            "2026-03-17", "2026-03-24",
        }, (
            f"peer busy blocks after all-day->timed conversion: "
            f"{sorted(peer)} (a 404'd update leaves the stale all-day "
            f"block at 2026-03-10)"
        )
        dupes = {k: v for k, v in peer.items() if len(v) > 1}
        assert not dupes, f"duplicate busy blocks: {dupes}"
        main_occ = _occurrences(s, "main", summary_contains="Offsite block")
        assert "2026-03-10T13:00:00Z" in main_occ and "2026-03-10" not in main_occ, (
            f"main copy did not follow the conversion: {sorted(main_occ)}"
        )

        # No 404 replan loop: a further reconcile with no source
        # change enqueues 0 ops.
        await _assert_zero_op_steady_state(
            s, "alice", "(a) all-day->timed conversion",
        )

        # (b) Now cancel that same (converted) occurrence on the client.
        s.cancel_event("client_a", f"{series['id']}_20260310")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # The busy block for it must ACTUALLY disappear — a DELETE
        # against the wrong-shape id 404s, is treated as success, and
        # strands the block forever (the phantom-busy failure mode).
        for nick, kw in (("client_b", dict(managed_only=True)),
                         ("main", dict(summary_contains="Offsite block"))):
            occ = _occurrences(s, nick, **kw)
            assert "2026-03-10T13:00:00Z" not in occ and "2026-03-10" not in occ, (
                f"phantom copy for the cancelled converted occurrence "
                f"still on {nick}: {sorted(occ)}"
            )
            assert {"2026-03-03", "2026-03-17", "2026-03-24"} <= set(occ), (
                f"cancellation removed the wrong occurrences on {nick}: "
                f"{sorted(occ)}"
            )

        # Absorb the known FINDING-1 single echo pass after an
        # instance-level cancellation (see test_adversarial_recurrence),
        # then demand steady state — still catches unbounded 404 churn.
        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"(b) cancel converted occurrence (echo pass absorbed "
            f"{echo['enqueued']} ops — FINDING-1)",
        )


# ---------------------------------------------------------------------------
# (c) Timed client series; one occurrence converted to ALL-DAY, then
# cancelled — the same assertions in the other direction.
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_timed_series_occurrence_converted_to_allday_then_cancelled():
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Weekly review",
            start="2026-03-03T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="cnvtimed000001",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        peer0 = _occurrences(s, "client_b", managed_only=True)
        assert set(peer0) == {
            "2026-03-03T09:00:00Z", "2026-03-10T09:00:00Z",
            "2026-03-17T09:00:00Z", "2026-03-24T09:00:00Z",
        }, f"baseline timed mirror wrong: {sorted(peer0)}"

        # Convert the 2026-03-10 occurrence to ALL-DAY on the client.
        # Google keeps the instance id keyed to the original TIMED
        # slot: <series>_20260310T090000Z.
        s.update_event(
            "client_a", f"{series['id']}_20260310T090000Z",
            start="2026-03-10",
            end="2026-03-11",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Every derived instance id must use the TIMED (UTC) form of
        # the ORIGINAL slot — deriving from the override's all-day
        # display shape (the old bug) yields ..._20260310, which real
        # Google (and the now-strict fake) 404s forever.
        inst_ids = await _instance_projection_ids(s, user.user_id)
        assert inst_ids, "expected instance projections for the override"
        for gid in inst_ids:
            assert gid.endswith("_20260310T090000Z"), (
                f"derived instance id {gid!r} does not use the original "
                f"slot's TIMED stamp form"
            )

        peer = _occurrences(s, "client_b", managed_only=True)
        assert set(peer) == {
            "2026-03-03T09:00:00Z", "2026-03-10",
            "2026-03-17T09:00:00Z", "2026-03-24T09:00:00Z",
        }, (
            f"peer busy blocks after timed->all-day conversion: "
            f"{sorted(peer)} (a 404'd update leaves the stale timed "
            f"block at 2026-03-10T09:00:00Z)"
        )
        dupes = {k: v for k, v in peer.items() if len(v) > 1}
        assert not dupes, f"duplicate busy blocks: {dupes}"
        main_occ = _occurrences(s, "main", summary_contains="Weekly review")
        assert "2026-03-10" in main_occ and "2026-03-10T09:00:00Z" not in main_occ, (
            f"main copy did not follow the conversion: {sorted(main_occ)}"
        )

        await _assert_zero_op_steady_state(
            s, "alice", "(c) timed->all-day conversion",
        )

        # And the cancel leg in this direction: the DELETE must hit
        # the real (timed-stamp) id, not a date-stamp phantom.
        s.cancel_event("client_a", f"{series['id']}_20260310T090000Z")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        for nick, kw in (("client_b", dict(managed_only=True)),
                         ("main", dict(summary_contains="Weekly review"))):
            occ = _occurrences(s, nick, **kw)
            assert "2026-03-10" not in occ and "2026-03-10T09:00:00Z" not in occ, (
                f"phantom copy for the cancelled converted occurrence "
                f"still on {nick}: {sorted(occ)}"
            )

        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"(c) cancel converted occurrence (echo pass absorbed "
            f"{echo['enqueued']} ops — FINDING-1)",
        )
