"""Shared failure-injection helpers for the soak tests.

REWRITE_PLAN.md §14 makes the Stage-3 release gate "90 simulated
days ... with full failure injection enabled".  These helpers turn
the five injection modes on/off and drive the recovery phase that
invariant 11 (recovery-within-N-cycles) measures.
"""

from __future__ import annotations

from datetime import timedelta

from tests.integration.framework import Scenario

# Modest enough that the system still converges, aggressive enough
# that every reconcile cycle sees real chaos.
FAILURE_RATES = {
    "network_error_rate": 0.04,
    "rate_limit_rate": 0.04,
    "server_error_rate": 0.04,
    "sync_token_expiry_rate": 0.08,
    "mid_write_crash_rate": 0.03,
}


def set_injection(scenario: Scenario, on: bool) -> None:
    """Enable or disable every failure-injection mode at once."""
    for name, rate in FAILURE_RATES.items():
        setattr(scenario.failures, name, rate if on else 0.0)


async def outbox_pending(scenario: Scenario, user_id: int) -> int:
    """Count the user's pending / in-flight outbox operations."""
    db = await scenario.setup_db()
    row = await (await db.execute(
        """SELECT COUNT(*) AS n FROM outbox_operations
            WHERE user_id = ? AND status IN ('pending', 'in_flight')""",
        (user_id,),
    )).fetchone()
    return int(row["n"] or 0)


async def recover_to_clean(
    scenario: Scenario,
    user_nick: str,
    user_id: int,
    *,
    max_passes: int = 6,
    advance_seconds: int = 180,
) -> int | None:
    """With failure injection assumed OFF, run reconcile passes until
    the outbox drains.  Returns the pass count (invariant 11), or
    ``None`` if a clean state was never reached.

    The simulated clock is advanced generously between passes so any
    backed-off retry becomes due (outbox backoff caps at 60s)."""
    for n in range(1, max_passes + 1):
        await scenario.run_reconciler(user_nick)
        if await outbox_pending(scenario, user_id) == 0:
            return n
        scenario.advance(timedelta(seconds=advance_seconds))
    return None if await outbox_pending(scenario, user_id) > 0 else max_passes
