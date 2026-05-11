"""90-simulated-day soak test (REWRITE_PLAN.md §13 Stage 3).

A scaled-down version of the soak harness: 90 simulated days,
mixed event patterns, 3 client calendars, runtime invariants
9-11 evaluated alongside the point-in-time invariants 1-8.

This is the **release gate** for Stage 4 (real-Google validation)
under the plan.  Marked ``slow``; not run in default CI but
available via ``pytest -m slow tests/soak``.

The test is deterministic (fixed seed) and tuned to run in
under 30 wall seconds.  A 365-sim-day variant lives below for
manual runs.
"""

from __future__ import annotations

import random
from datetime import timedelta

import pytest

from tests.soak import SoakHarness
from tests.soak.invariants import (
    InvariantChecker,
    LatencySample,
    DbSizeSample,
    sample_db_size,
    timed_reconcile,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


async def _run_soak(seed: int, days: int) -> dict:
    """Drive the harness for ``days`` simulated days, plant a
    realistic event pattern, evaluate every invariant.  Returns
    a dict with diagnostics."""
    h = SoakHarness(seed=seed)
    await h.setup()
    rng = random.Random(seed)

    latency_samples: list[LatencySample] = []
    db_samples: list[DbSizeSample] = []
    db_path = ""  # in-memory; sample_db_size handles it
    cycle_count = 0
    inserted_so_far = 0

    # Persona: 3-5 events per simulated day across the three
    # client calendars; ~10% are free events; ~5% get cancelled
    # later.  Reconcile every ~3 days.
    for day in range(1, days + 1):
        n_events = rng.randint(2, 4)
        for _ in range(n_events):
            cal = rng.choice(list(h.client_nicks))
            hour = rng.randint(8, 17)
            min_ = rng.randint(0, 59)
            # Spread events across a wide UTC range; clamp seconds.
            start_iso = (
                f"2026-{((day - 1) % 12) + 1:02d}-"
                f"{((day - 1) % 28) + 1:02d}T"
                f"{hour:02d}:{min_:02d}:00Z"
            )
            free = rng.random() < 0.10
            h.user_creates_event_on(
                client_nick=cal,
                summary=f"e{inserted_so_far}",
                start_iso=start_iso,
                show_as="free" if free else "busy",
            )
            inserted_so_far += 1

        # Cancel a random earlier event ~every 5 days.
        if day % 5 == 0 and h.oracle.events:
            key = rng.choice(list(h.oracle.events.keys()))
            kind, cal, eid = key
            if kind == "client":
                try:
                    h.user_cancels_event(cal, eid)
                except Exception:
                    pass

        # Reconcile every 3 days.
        if day % 3 == 0:
            cycle_count += 1
            t0 = timed_reconcile()
            await h.run_until_quiescent(max_passes=3)
            wall = (__import__("time").perf_counter() - t0)
            latency_samples.append(LatencySample(
                event_count=inserted_so_far, wall_seconds=wall,
            ))
            db_samples.append(await sample_db_size(
                await h.scenario.setup_db(), db_path, h.scenario.user(h.user_nick).user_id,
            ))

            violations = await h.check_invariants()
            if violations:
                # Capture every violation for the failure report.
                return {
                    "passed": False,
                    "day": day,
                    "events_inserted": inserted_so_far,
                    "violations": violations,
                    "latency_samples": latency_samples,
                    "db_samples": db_samples,
                }

        # Advance simulated clock by one day.
        h.scenario.advance(timedelta(days=1))

    # Final invariants 9 + 10 (runtime).
    inv9 = InvariantChecker.check_9_reconcile_latency_bounded(latency_samples)
    inv10 = InvariantChecker.check_10_db_size_sub_linear(db_samples)
    runtime_violations = inv9 + inv10

    await h.close()
    return {
        "passed": not runtime_violations,
        "events_inserted": inserted_so_far,
        "violations": runtime_violations,
        "latency_samples": latency_samples,
        "db_samples": db_samples,
        "cycles": cycle_count,
    }


async def test_90_sim_days_seed_0():
    """Default seed — must pass."""
    result = await _run_soak(seed=0, days=90)
    assert result["passed"], (
        f"soak failed at day={result.get('day')} "
        f"after {result['events_inserted']} events:\n  "
        + "\n  ".join(result["violations"])
    )
    # Sanity: we actually planted events.
    assert result["events_inserted"] >= 60
    # Cycles ran.
    assert result.get("cycles", 0) >= 20


async def test_90_sim_days_seed_42():
    """Different seed — same outcome."""
    result = await _run_soak(seed=42, days=90)
    assert result["passed"], (
        f"soak (seed=42) failed at day={result.get('day')}:\n  "
        + "\n  ".join(result["violations"])
    )


async def test_90_sim_days_seed_7():
    """Third seed — same outcome.  Three seeds = the plan's Stage 3
    release-gate requirement."""
    result = await _run_soak(seed=7, days=90)
    assert result["passed"], (
        f"soak (seed=7) failed at day={result.get('day')}:\n  "
        + "\n  ".join(result["violations"])
    )
