"""Smoke tests for the soak harness.

Verifies:
* :class:`SoakHarness` boots and connects to the integration
  framework.
* A simple user persona (3 events, no chaos) produces 0
  invariant violations.
* Invariant violations surface correctly when state is
  deliberately corrupted.
* A small persona of 25 random events runs to quiescence and
  produces 0 violations.

The 90/365-day adversarial soaks (REWRITE_PLAN.md Stage 3 release
gates) are intentionally not run in normal CI — they're available
under ``pytest -m soak`` once a runner is large enough.
"""

from __future__ import annotations

import random

import pytest

from tests.soak import SoakHarness

pytestmark = pytest.mark.asyncio


async def test_soak_harness_boots_and_passes_with_no_actions():
    h = SoakHarness()
    await h.setup()
    await h.run_reconciler()
    violations = await h.check_invariants()
    assert violations == []
    await h.close()


async def test_soak_harness_passes_after_simple_events():
    h = SoakHarness()
    await h.setup()
    h.user_creates_event_on(
        client_nick="client_a",
        summary="Project sync",
        start_iso="2026-06-01T09:00:00Z",
    )
    h.user_creates_event_on(
        client_nick="client_b",
        summary="Team standup",
        start_iso="2026-06-01T10:00:00Z",
    )
    await h.run_until_quiescent()
    await h.assert_invariants()
    await h.close()


async def test_soak_harness_detects_deliberate_violation():
    """If we delete a Google event the projection thinks is live,
    invariant 2 catches it."""
    h = SoakHarness()
    await h.setup()
    h.user_creates_event_on(
        client_nick="client_a",
        summary="Will be torn",
        start_iso="2026-06-02T09:00:00Z",
    )
    await h.run_until_quiescent()

    # Sabotage: delete one of our writes directly on Google
    # without telling the ledger.
    events = h.scenario.list_events(h.main_nick)
    target = next(e for e in events if e["summary"] == "Will be torn")
    h.scenario.google.delete_event(
        h.scenario.cal(h.main_nick), target["id"],
    )

    violations = await h.check_invariants()
    assert any("INV-2" in v for v in violations), violations
    await h.close()


async def test_soak_harness_25_random_events_runs_clean():
    """A small persona of 25 random insertions across 3 clients
    converges to a clean state."""
    h = SoakHarness(seed=42)
    await h.setup()
    rng = random.Random(0)

    base_minute = 0
    for _ in range(25):
        cal = rng.choice(list(h.client_nicks))
        # Spread events across June so per-event start times don't
        # collide.
        day = rng.randint(1, 28)
        hour = rng.randint(0, 23)
        start_iso = f"2026-06-{day:02d}T{hour:02d}:00:00Z"
        h.user_creates_event_on(
            client_nick=cal,
            summary=f"e{base_minute}",
            start_iso=start_iso,
        )
        base_minute += 1

    await h.run_until_quiescent(max_passes=5)
    await h.assert_invariants()
    await h.close()
