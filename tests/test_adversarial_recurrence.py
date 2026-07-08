"""Adversarial scenario tests for recurring events + single-instance exceptions.

Each test drives the FULL pipeline (ingest -> plan -> diff -> drain,
multiple reconcile passes) against the fake Google, then asserts
BUSINESS invariants:

  * every live occurrence of a source series has exactly one busy
    block per peer calendar (and one full copy on main);
  * no busy block exists for cancelled/excluded occurrences;
  * a reconcile pass with NO source change enqueues 0 ops (the churn
    detector);
  * the ledger holds no duplicate projection rows per
    (occurrence, target).

Tests that expose real defects are marked ``xfail(strict=False)`` with
the finding named in the reason; a pass there means the bug got fixed.

FINDINGS (2026-07-08 adversarial run):

* FINDING-1 (self-write echo churn, bounded): after ANY instance-level
  change (cancel one occurrence, RSVP writeback), the reconcile AFTER
  the one that converged enqueues a redundant op per target (2 here)
  repeating writes that already landed, then settles: op counts are
  [.., 2, 0, 0, ..].  The pipeline re-ingests its OWN writes (the
  tombstone it materialised on main, the RSVP patch it applied to the
  origin), re-marks the event affected
  (app/ledger/ingest/main.py:582-598 "genuine removal" branch), the
  planner bumps desired_ledger_version even when nothing changed
  (app/ledger/planner.py:476-490), and the diff treats a bare version
  mismatch as divergence (app/ledger/diff.py:277-282).  See
  test_finding1_second_reconcile_after_instance_cancel_not_zero_ops.

* FINDING-2 (fake fidelity / latent app assumption): deleting a
  recurring parent in the fake does NOT cascade-cancel its exception
  overrides (tests/fakes/google_calendar.py delete_event, ~line 674),
  while real Google removes the whole series.  The app RELIES on that
  cascade: diff.py:146-157 snaps instance projections to applied
  without issuing per-instance deletes when the parent is
  desired-absent.  On the fake this strands a live busy block for a
  moved occurrence forever.  See test_s7 (xfail).

* COVERAGE GAP: the fake's ``update_event`` with a truncated RRULE
  keeps out-of-range exception overrides alive; real Google cancels
  them.  S1b compensates by cancelling the override explicitly.
"""

from __future__ import annotations

import contextlib
from datetime import timezone

import pytest
from dateutil.parser import isoparse

from app.ledger.identity import is_managed_google_event_id
from app.ledger.payload import EP_PROJ_ID
from tests.integration.framework import Scenario
from tests.soak.invariants import InvariantChecker

pytestmark = pytest.mark.asyncio

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@contextlib.asynccontextmanager
async def scenario():
    """Scenario with GUARANTEED teardown.

    A failing assertion (a finding!) must still close the in-memory
    aiosqlite connection — its worker thread is non-daemon, and an
    unclosed connection wedges the pytest process at interpreter
    shutdown."""
    s = Scenario()
    try:
        yield s
    finally:
        with contextlib.suppress(Exception):
            await s.close()


def _norm_start(ev: dict) -> str:
    """Canonical start stamp: UTC 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYY-MM-DD'."""
    st = ev.get("start") or {}
    if "dateTime" in st:
        return isoparse(st["dateTime"]).astimezone(UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return st.get("date") or ""


def _is_managed(ev: dict) -> bool:
    """True if the event is one of ours (managed id or projection tag)."""
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
    """Map of confirmed occurrence start -> [event ids] on a calendar,
    with recurring series expanded (single_events=True)."""
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


def _assert_exactly_one_per_occurrence(
    occ_map: dict[str, list[str]], where: str,
) -> None:
    dupes = {k: v for k, v in occ_map.items() if len(v) > 1}
    assert not dupes, (
        f"{where}: multiple busy blocks for one occurrence: {dupes}"
    )


async def _assert_zero_op_steady_state(
    s: Scenario, user: str, label: str,
) -> dict:
    """THE churn detector: one more reconcile with no source change
    must enqueue 0 ops."""
    out = await s.run_reconciler(user)
    assert out["enqueued"] == 0, (
        f"{label}: op churn — reconcile with NO source change enqueued "
        f"{out['enqueued']} op(s); planned={out['planned']} "
        f"drain={out['drain']}"
    )
    return out


async def _dup_ledger_rows(s: Scenario, user_id: int) -> list[str]:
    """Duplicate present-projection rows per (series, occurrence, target)."""
    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT COALESCE(e.parent_canonical_uid, e.canonical_uid) AS series_uid,
                  COALESCE(e.recurrence_instance_original_start, '') AS occ,
                  p.target_kind AS kind,
                  COALESCE(p.target_calendar_id, -1) AS tgt,
                  COUNT(*) AS n
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.current_state = 'present'
            GROUP BY series_uid, occ, kind, tgt
           HAVING n > 1""",
        (user_id,),
    )).fetchall()
    return [
        f"{r['n']} present projections for series={r['series_uid']} "
        f"occ={r['occ'] or '<parent>'} target={r['kind']}:{r['tgt']}"
        for r in rows
    ]


async def _ledger_invariants(s: Scenario, user, client_nicks: list[str]) -> list[str]:
    """Reuse the soak invariant checkers (1-5, 8) + the per-occurrence
    duplicate-row check."""
    checker = InvariantChecker(
        db=await s.setup_db(),
        google=s.google,
        oracle=None,
        user_id=user.user_id,
        main_google_id=user.main_google_calendar_id,
        client_google_ids={n: s.cal(n) for n in client_nicks},
    )
    out = await checker.check_oracle_free()
    out += await _dup_ledger_rows(s, user.user_id)
    return out


async def _client_sync_token(s: Scenario, client_calendar_db_id: int) -> str:
    db = await s.setup_db()
    row = await (await db.execute(
        "SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id = ?",
        (client_calendar_db_id,),
    )).fetchone()
    assert row and row["sync_token"], "no sync token recorded for calendar"
    return row["sync_token"]


async def _two_client_alice(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    return await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )


# ---------------------------------------------------------------------------
# FINDING-1 — the key churn detector.  A reconcile pass with NO source
# change must enqueue 0 ops; after an instance cancellation it does not.
# ---------------------------------------------------------------------------
@pytest.mark.xfail(
    strict=False,
    reason=(
        "FINDING-1: self-write echo churn — the reconcile after an "
        "instance cancellation converged enqueues 2 redundant delete ops "
        "(one per target) repeating deletes that already landed, because "
        "main ingest re-ingests BusyBridge's own tombstone "
        "(app/ledger/ingest/main.py:582-598), the planner bumps "
        "desired_ledger_version on a no-change replan "
        "(app/ledger/planner.py:476-490), and the diff enqueues on bare "
        "version mismatch (app/ledger/diff.py:277-282).  Bounded: counts "
        "settle as [2, 0, 0, ...]."
    ),
)
async def test_finding1_second_reconcile_after_instance_cancel_not_zero_ops():
    async with scenario() as s:
        await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Echo probe",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=6",
            event_id="advechoprobe01",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        s.google.delete_event(
            s.cal("client_a"), f"{series['id']}_20260216T090000Z",
        )
        # This pass observes the cancellation and converges (outbox
        # drains to zero within the pass).
        first = await s.run_reconciler("alice")
        assert first["enqueued"] > 0, "precondition: cancellation seen"

        # Evidence gathering: the next passes should ALL be zero-op.
        counts = []
        for _ in range(4):
            counts.append((await s.run_reconciler("alice"))["enqueued"])
        assert counts[0] == 0, (
            f"FINDING-1: reconcile passes with NO source change enqueued "
            f"{counts} ops (eventually settles, so the churn is bounded "
            f"— but every instance-level change costs one extra cycle of "
            f"redundant Google writes, one per projected target)"
        )


# ---------------------------------------------------------------------------
# Scenario 1 — modified instance, then organizer truncates the series
# with UNTIL *before* the modified occurrence.
# ---------------------------------------------------------------------------
async def test_s1a_modified_instance_reaches_steady_state():
    """User moves ONE occurrence of a recurring client meeting; after
    convergence a further reconcile must enqueue 0 ops and the peer
    must show exactly one busy block per source occurrence."""
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Client sync",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=6",
            event_id="advseries00001",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Move the 2026-02-16 occurrence to 14:00 on the SOURCE.
        s.update_event(
            "client_a", f"{series['id']}_20260216T090000Z",
            start="2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        src = _occurrences(s, "client_a", summary_contains="Client sync")
        peer = _occurrences(s, "client_b", managed_only=True)
        assert set(src) == {
            "2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
            "2026-02-16T14:00:00Z", "2026-02-23T09:00:00Z",
            "2026-03-02T09:00:00Z", "2026-03-09T09:00:00Z",
        }, f"source series wrong after instance move: {sorted(src)}"
        assert set(peer) == set(src), (
            f"peer busy blocks diverge from source occurrences: "
            f"peer={sorted(peer)} src={sorted(src)}"
        )
        _assert_exactly_one_per_occurrence(peer, "client_b")

        await _assert_zero_op_steady_state(s, "alice", "S1a after instance move")
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


async def test_s1b_series_truncated_before_modified_instance():
    """Organizer truncates the series with UNTIL before the modified
    occurrence: the stale exception's busy block must converge (peer
    mirrors exactly the source's live set) and two consecutive
    reconciles with no source change must enqueue 0 ops."""
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Client sync",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=6",
            event_id="advseries00002",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Modified instance at 2026-02-16 (moved to 14:00), converged.
        s.update_event(
            "client_a", f"{series['id']}_20260216T090000Z",
            start="2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Organizer now truncates the series to end after 2026-02-09 —
        # BEFORE the modified instance's original occurrence.
        s.update_event(
            "client_a", series["id"],
            recurrence=["RRULE:FREQ=WEEKLY;UNTIL=20260209T235959Z"],
        )
        passes = await s.run_reconciler_until_quiescent("alice", max_passes=6)
        op_counts = [p["enqueued"] for p in passes]

        # Business invariant: the peer mirrors exactly what the source
        # still contains (whatever Google decides that is), one busy per
        # occurrence, and nothing keeps churning.
        src = _occurrences(s, "client_a", summary_contains="Client sync")
        peer = _occurrences(s, "client_b", managed_only=True)
        _assert_exactly_one_per_occurrence(peer, "client_b")
        assert set(peer) == set(src), (
            f"after UNTIL truncation the peer diverges from the source: "
            f"peer={sorted(peer)} src={sorted(src)}; "
            f"ops per reconcile pass since truncation={op_counts}"
        )

        # THE churn detector, twice.
        await _assert_zero_op_steady_state(
            s, "alice",
            f"S1b truncation pass A (ops per pass so far={op_counts})",
        )
        await _assert_zero_op_steady_state(
            s, "alice",
            f"S1b truncation pass B (ops per pass so far={op_counts})",
        )

        # COVERAGE GAP in the fake: real Google cancels exception
        # overrides that fall outside a truncated RRULE; the fake's
        # update_event does not, so the source still reports the moved
        # 02-16 exception as live (and the mirror above rightly kept
        # it).  Complete the real-world sequence by cancelling the
        # override the way Google would have:
        assert "2026-02-16T14:00:00Z" in _occurrences(
            s, "client_a", summary_contains="Client sync",
        ), "fake kept the out-of-range override live (documented gap)"
        s.cancel_event("client_a", f"{series['id']}_20260216T090000Z")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        # Absorb the known FINDING-1 echo pass, then demand steady state.
        echo = await s.run_reconciler("alice")
        peer2 = _occurrences(s, "client_b", managed_only=True)
        assert set(peer2) == {
            "2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
        }, (
            f"stale exception's busy block not removed after the "
            f"truncation completed: {sorted(peer2)}"
        )
        await _assert_zero_op_steady_state(
            s, "alice",
            f"S1b final (echo pass absorbed {echo['enqueued']} ops — "
            f"FINDING-1)",
        )
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 2 — cancelled instance must stay cancelled across sync-token
# expiry (full re-sync omits cancelled exceptions: the amnesia quirk).
# ---------------------------------------------------------------------------
async def test_s2_cancelled_instance_sticky_across_token_expiry():
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Weekly 1:1",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=6",
            event_id="advseries00003",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Cancel the 2026-02-16 occurrence on the client calendar.
        s.google.delete_event(
            s.cal("client_a"), f"{series['id']}_20260216T090000Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        for nick, kw in (("client_b", dict(managed_only=True)),
                         ("main", dict(summary_contains="Weekly 1:1"))):
            occ = _occurrences(s, nick, **kw)
            assert "2026-02-16T09:00:00Z" not in occ, (
                f"busy/copy for the cancelled occurrence still on {nick}: "
                f"{sorted(occ)}"
            )
            assert "2026-02-09T09:00:00Z" in occ
            assert "2026-02-23T09:00:00Z" in occ

        # Now expire the source's sync token so the next reconcile does
        # a FULL re-sync, which omits cancelled instance exceptions.
        token = await _client_sync_token(
            s, user.client_calendar_ids["client_a"],
        )
        s.google.expire_sync_token(token)
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        for nick, kw in (("client_b", dict(managed_only=True)),
                         ("main", dict(summary_contains="Weekly 1:1"))):
            occ = _occurrences(s, nick, **kw)
            assert "2026-02-16T09:00:00Z" not in occ, (
                f"cancellation amnesia: full re-sync after token expiry "
                f"REVIVED the cancelled occurrence on {nick}: {sorted(occ)}"
            )
            _assert_exactly_one_per_occurrence(occ, nick)

        await _assert_zero_op_steady_state(s, "alice", "S2 post-expiry")
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 3 — instance override delivered BEFORE its parent in one
# incremental sync page.
# ---------------------------------------------------------------------------
async def test_s3_override_arrives_before_parent_in_one_sync_page():
    """The fake orders incremental sync by change cursor.  Creating the
    parent, materialising an override, then touching the parent makes
    the override's cursor LOWER than the parent's — so a single
    incremental page delivers the (never-before-seen) child override
    first, then its parent.  Ingest must not drop or duplicate it."""
    async with scenario() as s:
        user = await _two_client_alice(s)
        # Establish sync state on an EMPTY client so the next reconcile
        # is incremental and sees everything below in one page.
        await s.run_reconciler_until_quiescent("alice", max_passes=3)

        series = s.given_recurring_event(
            "client_a", summary="Ordered sync",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="advseries00004",
        )                                                    # change_seq N
        s.update_event(
            "client_a", f"{series['id']}_20260209T090000Z",
            start="2026-02-09T15:00:00Z",
        )                                                    # override: N+1
        s.update_event("client_a", series["id"], summary="Ordered sync v2")
        # parent now N+2 > override — child sorts FIRST in the page.

        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        src = _occurrences(s, "client_a", summary_contains="Ordered sync")
        peer = _occurrences(s, "client_b", managed_only=True)
        assert set(src) == {
            "2026-02-02T09:00:00Z", "2026-02-09T15:00:00Z",
            "2026-02-16T09:00:00Z", "2026-02-23T09:00:00Z",
        }, f"source set wrong: {sorted(src)}"
        assert set(peer) == set(src), (
            f"child-before-parent ordering broke the mirror: "
            f"peer={sorted(peer)} src={sorted(src)}"
        )
        _assert_exactly_one_per_occurrence(peer, "client_b")

        await _assert_zero_op_steady_state(s, "alice", "S3 out-of-order child")
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 4 — all-day recurring CLIENT event with one cancelled
# occurrence (all-day derived IDs use the _YYYYMMDD stamp).
# ---------------------------------------------------------------------------
async def test_s4_all_day_recurring_client_cancelled_occurrence():
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Client onsite",
            start="2026-02-02",              # all-day (date only)
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="advonsite00001",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        peer0 = _occurrences(s, "client_b", managed_only=True)
        assert set(peer0) == {
            "2026-02-02", "2026-02-09", "2026-02-16", "2026-02-23",
        }, (
            f"all-day series not mirrored occurrence-for-occurrence: "
            f"{sorted(peer0)}"
        )

        # Cancel the 2026-02-16 occurrence (all-day derived id: _YYYYMMDD).
        s.google.delete_event(s.cal("client_a"), f"{series['id']}_20260216")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        for nick, kw in (("client_b", dict(managed_only=True)),
                         ("main", dict(summary_contains="Client onsite"))):
            occ = _occurrences(s, nick, **kw)
            assert "2026-02-16" not in occ, (
                f"all-day cancelled occurrence still live on {nick}: "
                f"{sorted(occ)}"
            )
            assert {"2026-02-02", "2026-02-09", "2026-02-23"} <= set(occ), (
                f"all-day cancellation removed the wrong occurrences on "
                f"{nick}: {sorted(occ)}"
            )
            _assert_exactly_one_per_occurrence(occ, nick)

        # Absorb the known FINDING-1 echo pass (redundant re-deletes of
        # the already-cancelled _YYYYMMDD mirrors), then demand steady
        # state — this still catches any UNBOUNDED all-day 404 churn.
        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"S4 all-day cancel (echo pass absorbed {echo['enqueued']} "
            f"ops — FINDING-1)",
        )
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 5 — weekly 09:00 America/New_York series across the DST
# spring-forward (2026-03-08) with an exception right after it.
# ---------------------------------------------------------------------------
async def test_s5_dst_series_with_exception_after_transition():
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="NY standup",
            start="2026-02-23T09:00:00-05:00",   # 14:00Z while EST
            timezone="America/New_York",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="advdstseries01",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Wall clock 09:00 NY: 14:00Z before 2026-03-08, 13:00Z after.
        expected = {
            "2026-02-23T14:00:00Z", "2026-03-02T14:00:00Z",
            "2026-03-09T13:00:00Z", "2026-03-16T13:00:00Z",
        }
        src = _occurrences(s, "client_a", summary_contains="NY standup")
        assert set(src) == expected, (
            f"fake expanded source wrongly: {sorted(src)}"
        )
        peer = _occurrences(s, "client_b", managed_only=True)
        assert set(peer) == expected, (
            f"DST: busy blocks not at the source's UTC instants: "
            f"peer={sorted(peer)} expected={sorted(expected)}"
        )

        # Exception on the first post-transition occurrence (03-09,
        # 13:00Z): user pushes it one hour later to 10:00 NY = 14:00Z.
        s.update_event(
            "client_a", f"{series['id']}_20260309T130000Z",
            start="2026-03-09T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        peer2 = _occurrences(s, "client_b", managed_only=True)
        assert "2026-03-09T14:00:00Z" in peer2, (
            f"post-DST exception's busy block missing / at wrong UTC "
            f"instant: {sorted(peer2)}"
        )
        assert "2026-03-09T13:00:00Z" not in peer2, (
            f"stale pre-move busy block still present at 13:00Z: "
            f"{sorted(peer2)}"
        )
        _assert_exactly_one_per_occurrence(peer2, "client_b")

        await _assert_zero_op_steady_state(s, "alice", "S5 DST exception")
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 6 — decline ONE instance of the managed copy on main; the
# RSVP must reach the origin instance and nothing may echo.
# ---------------------------------------------------------------------------
async def test_s6_rsvp_decline_one_instance_writes_back_no_echo():
    async with scenario() as s:
        user = await _two_client_alice(s)
        s.google.insert_event(s.cal("client_a"), {
            "id": "advrsvpseries1",
            "summary": "Recurring client mtg",
            "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
            "recurrence": ["RRULE:FREQ=WEEKLY;COUNT=4"],
            "organizer": {"email": "bob@example.com"},
            "attendees": [
                {"email": "alice@example.com", "self": True,
                 "responseStatus": "accepted"},
                {"email": "bob@example.com", "responseStatus": "accepted"},
            ],
        })
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Find the managed main copy's 2026-02-16 instance.
        main_inst = None
        for ev in s.list_events("main", single_events=True):
            if _norm_start(ev) == "2026-02-16T09:00:00Z" and _is_managed(ev):
                main_inst = ev
                break
        assert main_inst is not None, "managed main copy instance not found"
        atts = [dict(a) for a in main_inst.get("attendees") or []]
        assert any(a.get("self") for a in atts), (
            f"main-copy instance carries no self-attendee to RSVP with: "
            f"{atts}"
        )
        for a in atts:
            if a.get("self"):
                a["responseStatus"] = "declined"
        s.update_event("main", main_inst["id"], attendees=atts)

        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        # The decline must reach the ORIGIN instance (not the series).
        origin_inst = s.google.get_event(
            s.cal("client_a"), "advrsvpseries1_20260216T090000Z",
        )
        alice = next(
            (a for a in origin_inst.get("attendees") or []
             if a.get("email") == "alice@example.com"), None,
        )
        assert alice is not None and (
            alice.get("responseStatus") == "declined"
        ), (
            f"instance RSVP decline did not reach the origin occurrence; "
            f"origin attendees={origin_inst.get('attendees')}"
        )
        # Other guests must survive the writeback.
        bob = next(
            (a for a in origin_inst.get("attendees") or []
             if a.get("email") == "bob@example.com"), None,
        )
        assert bob is not None and bob.get("responseStatus") == "accepted", (
            f"writeback dropped/altered the other guest: "
            f"{origin_inst.get('attendees')}"
        )
        # The parent series' own RSVP must be untouched.
        origin_parent = s.google.get_event(
            s.cal("client_a"), "advrsvpseries1",
        )
        alice_parent = next(
            (a for a in origin_parent.get("attendees") or []
             if a.get("email") == "alice@example.com"), None,
        )
        assert alice_parent is not None and (
            alice_parent.get("responseStatus") == "accepted"
        ), (
            f"instance-level decline leaked onto the PARENT series: "
            f"{origin_parent.get('attendees')}"
        )

        # Absorb the known FINDING-1 echo pass (re-ingesting our own
        # origin patch triggers redundant UPDATEs of the main copy and
        # the busy block), then demand two zero-op reconciles: the
        # writeback must not ping-pong between main and the origin.
        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"S6 RSVP echo pass A (absorbed {echo['enqueued']} ops — "
            f"FINDING-1)",
        )
        await _assert_zero_op_steady_state(s, "alice", "S6 RSVP echo pass B")
        # And the source must not have been clobbered by the echo.
        origin_inst2 = s.google.get_event(
            s.cal("client_a"), "advrsvpseries1_20260216T090000Z",
        )
        alice2 = next(
            (a for a in origin_inst2.get("attendees") or []
             if a.get("email") == "alice@example.com"), None,
        )
        assert alice2 is not None and (
            alice2.get("responseStatus") == "declined"
        ), f"echo pass reverted the RSVP: {origin_inst2.get('attendees')}"
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 7 — series deleted on the client while a modified-instance
# busy block exists: every child must be cleaned up.
# ---------------------------------------------------------------------------
@pytest.mark.xfail(
    strict=False,
    reason=(
        "FINDING-2: the moved occurrence's busy block "
        "(<busyparent>_20260216T090000Z on client_b) stays CONFIRMED "
        "forever after the series is deleted, while its ledger "
        "projection claims current_state='absent'.  The diff assumes "
        "deleting the managed parent series cascades to exception "
        "overrides (app/ledger/diff.py:146-157 snaps the instance "
        "projection to applied WITHOUT a per-instance delete) — true on "
        "real Google, but the fake's delete_event "
        "(tests/fakes/google_calendar.py:674) does not cascade to "
        "recurringEventId children, so this is primarily a fake-fidelity "
        "gap that also documents a real (untested-in-prod) app "
        "assumption."
    ),
)
async def test_s7_series_delete_cleans_up_modified_instance_children():
    async with scenario() as s:
        user = await _two_client_alice(s)
        series = s.given_recurring_event(
            "client_a", summary="Doomed series",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="advseries00007",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Move one occurrence so a modified-instance busy block exists.
        s.update_event(
            "client_a", f"{series['id']}_20260216T090000Z",
            start="2026-02-16T14:00:00Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        peer = _occurrences(s, "client_b", managed_only=True)
        assert "2026-02-16T14:00:00Z" in peer, (
            "precondition: moved busy exists"
        )

        # User deletes the whole series on the client calendar.  (Real
        # Google cancels the exception rows too; mirror that here so
        # the fake's source state matches reality.)
        s.cancel_event("client_a", f"{series['id']}_20260216T090000Z")
        s.cancel_event("client_a", series["id"])
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        peer_after = _occurrences(s, "client_b", managed_only=True)
        assert peer_after == {}, (
            f"orphan busy blocks survive the series delete on client_b: "
            f"{peer_after}"
        )
        main_after = _occurrences(s, "main", summary_contains="Doomed series")
        assert main_after == {}, (
            f"main copies survive the series delete: {main_after}"
        )

        # Ledger: no projection may still claim 'present'.
        db = await s.setup_db()
        row = await (await db.execute(
            """SELECT COUNT(*) AS n
                 FROM ledger_projections p
                 JOIN ledger_events e ON e.id = p.ledger_event_id
                WHERE e.user_id = ? AND p.current_state = 'present'""",
            (user.user_id,),
        )).fetchone()
        assert int(row["n"]) == 0, (
            f"{row['n']} projections still 'present' after full series "
            f"delete"
        )

        await _assert_zero_op_steady_state(s, "alice", "S7 series delete")
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations


# ---------------------------------------------------------------------------
# Scenario 8 — two clients with recurring meetings in the SAME slot;
# one client cancels one occurrence.
# ---------------------------------------------------------------------------
async def test_s8_same_slot_cancellation_only_affects_owning_client():
    async with scenario() as s:
        user = await _two_client_alice(s)
        a_series = s.given_recurring_event(
            "client_a", summary="A weekly",
            start="2026-02-02T09:00:00Z",
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="advsameslot0a1",
        )
        s.given_recurring_event(
            "client_b", summary="B weekly",
            start="2026-02-02T09:00:00Z",       # SAME slot
            rrule="RRULE:FREQ=WEEKLY;COUNT=4",
            event_id="advsameslot0b1",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        # Baseline: each client sees exactly ONE busy block per
        # occurrence (the other client's); main sees both full copies.
        full = {"2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
                "2026-02-16T09:00:00Z", "2026-02-23T09:00:00Z"}
        for nick in ("client_a", "client_b"):
            busy = _occurrences(s, nick, managed_only=True)
            assert set(busy) == full, (
                f"baseline busy set wrong on {nick}: {sorted(busy)}"
            )
            _assert_exactly_one_per_occurrence(busy, nick)
        assert set(_occurrences(s, "main", summary_contains="A weekly")) == full
        assert set(_occurrences(s, "main", summary_contains="B weekly")) == full

        # Client A cancels its 2026-02-16 occurrence.
        s.google.delete_event(
            s.cal("client_a"), f"{a_series['id']}_20260216T090000Z",
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        # Only A's projections lose that occurrence:
        busy_on_b = _occurrences(s, "client_b", managed_only=True)  # from A
        assert "2026-02-16T09:00:00Z" not in busy_on_b, (
            f"A's cancelled occurrence still busy on client_b: "
            f"{sorted(busy_on_b)}"
        )
        assert set(busy_on_b) == full - {"2026-02-16T09:00:00Z"}

        busy_on_a = _occurrences(s, "client_a", managed_only=True)  # from B
        assert set(busy_on_a) == full, (
            f"cancelling A's occurrence wrongly touched B's busy block on "
            f"client_a: {sorted(busy_on_a)}"
        )
        a_main = _occurrences(s, "main", summary_contains="A weekly")
        b_main = _occurrences(s, "main", summary_contains="B weekly")
        assert set(a_main) == full - {"2026-02-16T09:00:00Z"}, (
            f"main copy of A wrong: {sorted(a_main)}"
        )
        assert set(b_main) == full, f"main copy of B wrong: {sorted(b_main)}"

        # Absorb the known FINDING-1 echo pass, then demand steady state
        # (still catches cross-client ping-pong between the two series).
        echo = await s.run_reconciler("alice")
        await _assert_zero_op_steady_state(
            s, "alice",
            f"S8 same-slot cancel (echo pass absorbed {echo['enqueued']} "
            f"ops — FINDING-1)",
        )
        violations = await _ledger_invariants(s, user, ["client_a", "client_b"])
        assert not violations, violations
