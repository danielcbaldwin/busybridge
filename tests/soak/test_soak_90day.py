"""90-simulated-day soak test (REWRITE_PLAN.md §13 Stage 3 / §14).

The Stage-3 release gate: 90 simulated days on three random seeds
**with full failure injection enabled** — network errors, rate
limits, 5xx, sync-token expiry, and mid-write crashes — and every
invariant (1-11) green throughout.

Each reconcile cycle runs in two phases:

1. **Chaos phase** — reconcile with all five failure modes active.
2. **Recovery phase** — failure injection OFF; reconcile passes are
   run until the outbox drains, *counting* the passes.  Invariant
   11 requires recovery within a bounded number of cycles.

Then the point-in-time invariants 1-8 are checked, and the runtime
invariants 9 (latency) and 10 (db growth) at the end.

Marked ``slow``.  Deterministic (fixed seeds).
"""

from __future__ import annotations

import random
import time
from datetime import timedelta

import pytest

from tests.soak import SoakHarness
from tests.soak.chaos import recover_to_clean, set_injection
from tests.soak.invariants import (
    DbSizeSample,
    InvariantChecker,
    LatencySample,
    sample_db_size,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


async def _run_soak(seed: int, days: int) -> dict:
    """Drive the harness for ``days`` simulated days under failure
    injection; evaluate every invariant after each cycle."""
    h = SoakHarness(seed=seed)
    await h.setup()
    rng = random.Random(seed)

    latency_samples: list[LatencySample] = []
    db_samples: list[DbSizeSample] = []
    cycle_count = 0
    inserted_so_far = 0
    worst_recovery = 0

    for day in range(1, days + 1):
        # Persona: 2-4 events/day across the client calendars;
        # ~10% free; a cancellation roughly every 5 days.
        for _ in range(rng.randint(2, 4)):
            cal = rng.choice(list(h.client_nicks))
            hour = rng.randint(8, 17)
            minute = rng.randint(0, 59)
            start_iso = (
                f"2026-{((day - 1) % 12) + 1:02d}-"
                f"{((day - 1) % 28) + 1:02d}T{hour:02d}:{minute:02d}:00Z"
            )
            free = rng.random() < 0.10
            h.user_creates_event_on(
                client_nick=cal,
                summary=f"e{inserted_so_far}",
                start_iso=start_iso,
                show_as="free" if free else "busy",
            )
            inserted_so_far += 1

        if day % 5 == 0 and h.oracle.events:
            kind, cal, eid = rng.choice(list(h.oracle.events.keys()))
            if kind == "client":
                try:
                    h.user_cancels_event(cal, eid)
                except Exception:
                    pass

        # Reconcile every 3 days: chaos phase, then recovery phase.
        if day % 3 == 0:
            cycle_count += 1

            set_injection(h.scenario, True)
            try:
                await h.run_until_quiescent(max_passes=3)
            except Exception:
                # Chaos may surface; the recovery phase converges it.
                pass
            set_injection(h.scenario, False)

            uid = h.scenario.user(h.user_nick).user_id
            cycles_to_clean = await recover_to_clean(
                h.scenario, h.user_nick, uid,
            )

            db_sample = await sample_db_size(
                await h.scenario.setup_db(), "", uid,
            )
            db_samples.append(db_sample)
            # Time a CLEAN steady-state reconcile (system already
            # converged, no chaos) for the latency sample — the
            # recovery phase's wall time is too noisy to trend.
            t0 = time.perf_counter()
            await h.scenario.run_reconciler(h.user_nick)
            latency_samples.append(LatencySample(
                event_count=max(1, db_sample.ledger_row_count),
                wall_seconds=time.perf_counter() - t0,
            ))
            if cycles_to_clean is not None:
                worst_recovery = max(worst_recovery, cycles_to_clean)

            # INV-11 (recovery) + the point-in-time invariants 1-8.
            violations = InvariantChecker.check_11_recovery_within_cycles(
                cycles_to_clean,
            )
            violations += await h.check_invariants()
            if violations:
                return {
                    "passed": False,
                    "day": day,
                    "events_inserted": inserted_so_far,
                    "violations": violations,
                    "cycles": cycle_count,
                    "worst_recovery": worst_recovery,
                }

        h.scenario.advance(timedelta(days=1))

    # Final runtime invariants 9 + 10.
    runtime = (
        InvariantChecker.check_9_reconcile_latency_bounded(latency_samples)
        + InvariantChecker.check_10_db_size_sub_linear(db_samples)
    )
    await h.close()
    return {
        "passed": not runtime,
        "events_inserted": inserted_so_far,
        "violations": runtime,
        "cycles": cycle_count,
        "worst_recovery": worst_recovery,
    }


async def _assert_soak(seed: int) -> None:
    result = await _run_soak(seed=seed, days=90)
    assert result["passed"], (
        f"soak (seed={seed}) failed at day={result.get('day')} "
        f"after {result['events_inserted']} events:\n  "
        + "\n  ".join(result["violations"])
    )
    assert result["events_inserted"] >= 60
    assert result["cycles"] >= 20
    # Recovery stayed within the invariant-11 bound throughout.
    assert result["worst_recovery"] <= 5


async def test_90_sim_days_seed_0_with_failure_injection():
    await _assert_soak(seed=0)


async def test_90_sim_days_seed_42_with_failure_injection():
    await _assert_soak(seed=42)


async def test_90_sim_days_seed_7_with_failure_injection():
    """Three seeds with full failure injection = the §14 Stage-3
    release-gate requirement."""
    await _assert_soak(seed=7)
