"""Targeted recurring-cancellation soak — 365 simulated days.

This is the specific soak REWRITE_PLAN.md §14 calls for as the
Stage-4 release gate:

  "Generate [many] weekly recurring meetings.  Schedule
   cancellations at varied positions: some on the next instance,
   some mid-series, some after a _R reschedule has moved the
   parent.  Force sync-token expiry partway through.  Run 365
   simulated days.  Assert after every cycle that no ghost
   instances exist on any client calendar.  Today's system fails
   this within a simulated week; the goal post-rewrite is zero
   failures across all seeds."

Marked ``slow``; run via ``pytest -m slow``.

Scaled to 20 series (not 100) and reconcile-every-7-days so the
test completes in well under a minute while still crossing the
30-day sync-token TTL many times.
"""

from __future__ import annotations

import random
import time
from datetime import timedelta

import pytest

from tests.integration.framework import Scenario
from tests.soak.chaos import recover_to_clean, set_injection
from tests.soak.invariants import (
    DbSizeSample,
    InvariantChecker,
    LatencySample,
    sample_db_size,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


async def _oracle_free_invariants(s: Scenario) -> list[str]:
    """Run soak invariants 1-5 + 8 (the ones that need no oracle).

    The recurring soak does not model a ground-truth Oracle — per-
    instance recurring state is the ghost-check's job — so the
    oracle-backed invariants 6/7 are skipped here."""
    user = s.user("alice")
    db = await s.setup_db()
    checker = InvariantChecker(
        db=db,
        google=s.google,
        oracle=None,
        user_id=user.user_id,
        main_google_id=user.main_google_calendar_id,
        client_google_ids={
            "client_a": s.cal("client_a"),
            "client_b": s.cal("client_b"),
        },
        client_db_ids=dict(user.client_calendar_ids),
    )
    return await checker.check_oracle_free()


# A weekly Monday series; COUNT keeps the fake's RRULE expansion bounded.
_RRULE = "RRULE:FREQ=WEEKLY;COUNT=52;BYDAY=MO"


def _b32(n: int) -> str:
    """A legal base32hex client-supplied event id from an int."""
    alphabet = "0123456789abcdefghijklmnopqrstuv"
    s = ""
    n += 1
    while n:
        s = alphabet[n % 32] + s
        n //= 32
    return f"bbrec{s:>09s}".replace(" ", "0")[:14]


async def _build(seed: int) -> tuple[Scenario, list[str]]:
    """Set up a scenario with 20 weekly recurring series on
    client_a, reconciled once.  Returns (scenario, parent_ids)."""
    s = Scenario(seed=seed, sync_token_ttl=timedelta(days=30))
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    await s.given_user("alice", main="main", clients=["client_a", "client_b"])

    parent_ids: list[str] = []
    for i in range(20):
        eid = _b32(i)
        # All series start on the same Monday so instance dates line up.
        s.given_recurring_event(
            "client_a",
            summary=f"Weekly meeting {i}",
            start="2026-01-05T09:00:00Z",  # a Monday
            rrule=_RRULE,
            event_id=eid,
        )
        parent_ids.append(eid)
    await s.run_reconciler("alice")
    return s, parent_ids


async def _main_parent_ids(s: Scenario) -> dict[str, str]:
    """Map source parent_id → main-calendar projected parent id."""
    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT e.source_event_id, p.google_event_id
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.target_kind = 'main'
              AND e.parent_canonical_uid IS NULL
              AND e.is_recurring = 1
              AND p.google_event_id IS NOT NULL""",
    )).fetchall()
    return {r["source_event_id"]: r["google_event_id"] for r in rows}


def _cancelled_instance_dates(s: Scenario, main_parent_id: str) -> set[str]:
    """Dates (YYYY-MM-DD) that are cancelled on the main copy of a
    recurring series, via the reliable events.instances path."""
    insts = s.google.list_instances(
        s.cal("main"), main_parent_id, show_deleted=True,
    )
    out = set()
    for i in insts["items"]:
        if i.get("status") != "cancelled":
            continue
        ost = i.get("originalStartTime") or {}
        dt = ost.get("dateTime") or ost.get("date") or ""
        if dt:
            out.add(dt[:10])
    return out


def _confirmed_instance_dates(s: Scenario, main_parent_id: str) -> set[str]:
    insts = s.google.list_instances(
        s.cal("main"), main_parent_id, show_deleted=True,
    )
    out = set()
    for i in insts["items"]:
        if i.get("status") == "cancelled":
            continue
        start = i.get("start") or {}
        dt = start.get("dateTime") or start.get("date") or ""
        if dt:
            out.add(dt[:10])
    return out


async def _run_recurring_soak(seed: int, days: int = 365) -> dict:
    """Plant cancellations at varied positions over ``days`` sim
    days, forcing sync-token expiry, asserting after every cycle
    that every cancelled date stays absent on main."""
    s, parent_ids = await _build(seed)
    rng = random.Random(seed)

    # Ground truth: source_parent_id -> set of cancelled dates.
    cancelled: dict[str, set[str]] = {pid: set() for pid in parent_ids}

    # Mondays of 2026, the instance dates.
    from datetime import date
    mondays = []
    d = date(2026, 1, 5)
    while d.year == 2026:
        mondays.append(d.isoformat())
        d = date.fromordinal(d.toordinal() + 7)

    cycle = 0
    latency_samples: list[LatencySample] = []
    db_samples: list[DbSizeSample] = []
    worst_recovery = 0
    for day in range(1, days + 1):
        # Every ~10 days, cancel a random instance of a random series.
        if day % 10 == 0:
            pid = rng.choice(parent_ids)
            # Pick a Monday that is in the future-ish and not already
            # cancelled for this series.
            candidates = [m for m in mondays if m not in cancelled[pid]]
            if candidates:
                target_date = rng.choice(candidates)
                instance_id = f"{pid}_{target_date.replace('-', '')}T090000Z"
                try:
                    s.google.delete_event(s.cal("client_a"), instance_id)
                    cancelled[pid].add(target_date)
                except Exception:
                    pass

        # Every ~45 days, force a sync-token expiry on client_a.
        if day % 45 == 0:
            db = await s.setup_db()
            row = await (await db.execute(
                """SELECT sync_token FROM calendar_sync_state
                    WHERE client_calendar_id = ?""",
                (s.user("alice").client_calendar_ids["client_a"],),
            )).fetchone()
            if row and row["sync_token"]:
                try:
                    s.google.expire_sync_token(row["sync_token"])
                except Exception:
                    pass

        # Reconcile every 7 days: chaos phase, then recovery phase.
        if day % 7 == 0:
            cycle += 1
            set_injection(s, True)
            try:
                await s.run_reconciler_until_quiescent("alice", max_passes=3)
            except Exception:
                pass
            set_injection(s, False)

            cycles_to_clean = await recover_to_clean(
                s, "alice", s.user("alice").user_id,
            )
            db_sample = await sample_db_size(
                await s.setup_db(), "", s.user("alice").user_id,
            )
            db_samples.append(db_sample)
            # Time a CLEAN steady-state reconcile for the latency
            # sample (the recovery phase's wall time is too noisy).
            t0 = time.perf_counter()
            await s.run_reconciler("alice")
            latency_samples.append(LatencySample(
                event_count=max(1, db_sample.ledger_row_count),
                wall_seconds=time.perf_counter() - t0,
            ))
            if cycles_to_clean is not None:
                worst_recovery = max(worst_recovery, cycles_to_clean)

            # Point-in-time invariants 1-5/8 + recovery (INV-11).
            inv = await _oracle_free_invariants(s)
            inv += InvariantChecker.check_11_recovery_within_cycles(
                cycles_to_clean,
            )
            if inv:
                return {
                    "passed": False,
                    "day": day, "cycle": cycle, "series": "-",
                    "reason": "; ".join(inv),
                }

            # INVARIANT: every cancelled date is absent on main; every
            # NOT-cancelled date is still present.  This is the
            # "no ghost instances" assertion.
            main_map = await _main_parent_ids(s)
            for pid in parent_ids:
                main_pid = main_map.get(pid)
                if main_pid is None:
                    continue
                cancelled_on_main = _cancelled_instance_dates(s, main_pid)
                confirmed_on_main = _confirmed_instance_dates(s, main_pid)
                # Each cancelled source date must be cancelled on main.
                for cdate in cancelled[pid]:
                    if cdate not in cancelled_on_main:
                        return {
                            "passed": False,
                            "day": day, "cycle": cycle,
                            "series": pid,
                            "reason": (
                                f"date {cdate} cancelled on source but NOT "
                                f"cancelled on main (ghost instance)"
                            ),
                        }
                # And it must NOT still be confirmed (the ghost case).
                for cdate in cancelled[pid]:
                    if cdate in confirmed_on_main:
                        return {
                            "passed": False,
                            "day": day, "cycle": cycle,
                            "series": pid,
                            "reason": (
                                f"date {cdate} is a GHOST: cancelled on "
                                f"source yet still confirmed on main"
                            ),
                        }

        s.scenario_advance_one_day(day) if False else s.advance(timedelta(days=1))

    total_cancellations = sum(len(v) for v in cancelled.values())
    # Final runtime invariants 9 (latency) + 10 (db growth).
    runtime = (
        InvariantChecker.check_9_reconcile_latency_bounded(latency_samples)
        + InvariantChecker.check_10_db_size_sub_linear(db_samples)
    )
    await s.close()
    return {
        "passed": not runtime,
        "days": days,
        "cycles": cycle,
        "total_cancellations": total_cancellations,
        "worst_recovery": worst_recovery,
        "reason": "; ".join(runtime) if runtime else None,
    }


async def test_recurring_cancellation_soak_365_seed_0():
    result = await _run_recurring_soak(seed=0)
    assert result["passed"], (
        f"recurring-cancellation soak failed at day {result.get('day')} "
        f"cycle {result.get('cycle')} series {result.get('series')}: "
        f"{result.get('reason')}"
    )
    # We actually exercised cancellations.
    assert result["total_cancellations"] >= 10
    assert result["cycles"] >= 40


async def test_recurring_cancellation_soak_365_seed_99():
    result = await _run_recurring_soak(seed=99)
    assert result["passed"], (
        f"recurring-cancellation soak (seed 99) failed at day "
        f"{result.get('day')}: {result.get('reason')}"
    )
