"""scan-orphans dry_run=true must be a true preview (REWRITE_PLAN.md §5.5).

A dry-run orphan scan classifies and counts orphans but persists
nothing: no synthetic ledger / projection tombstone, and no outbox
row.  Otherwise the leftover projection survives the dry-run and a
later normal reconcile drains it — deleting the previewed orphan on
Google for real.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio

# A managed-looking Google id (``bb`` + 13 base32hex chars) with no
# matching projection — exactly what discovery classifies as an orphan.
_ORPHAN_ID = "bb0000000000001"


async def test_orphan_scan_dry_run_persists_nothing_and_keeps_the_orphan():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")

    # An event that "looks like ours" (managed id prefix) but matches
    # no projection → discovery classifies it as an orphan.
    s.given_event("main", summary="Stale orphan", event_id=_ORPHAN_ID)

    # Dry-run orphan scan: classifies the orphan but must persist nothing.
    out = await s.run_reconciler(
        "alice", include_main=False, run_discovery=True, dry_run=True,
    )
    assert out["discovery"]["orphans_deleted"] == 1, out["discovery"]

    db = await s.setup_db()

    async def _count(sql: str, *params) -> int:
        row = await (await db.execute(sql, params)).fetchone()
        return int(row["c"])

    assert await _count(
        "SELECT COUNT(*) AS c FROM ledger_events WHERE user_id = ?",
        user.user_id,
    ) == 0, "dry-run persisted a synthetic ledger_event"
    assert await _count(
        "SELECT COUNT(*) AS c FROM ledger_projections",
    ) == 0, "dry-run persisted a synthetic projection"
    assert await _count(
        "SELECT COUNT(*) AS c FROM outbox_operations WHERE user_id = ?",
        user.user_id,
    ) == 0, "dry-run left an outbox row"

    # A later normal reconcile (no discovery) must NOT delete the
    # previewed orphan — there is no leftover projection to drain.
    await s.run_reconciler("alice")
    still = s.google.get_event(s.cal("main"), _ORPHAN_ID)
    assert still.get("status") != "cancelled", (
        "the previewed orphan was deleted by a later reconcile"
    )
    await s.close()


async def test_orphan_scan_without_dry_run_still_schedules_the_delete():
    """The real (non-dry) orphan scan is unchanged: it schedules the
    tombstone and the orphan is deleted on Google."""
    s = Scenario()
    s.given_calendar("main")
    await s.given_user("alice", main="main")
    s.given_event("main", summary="Stale orphan", event_id=_ORPHAN_ID)

    await s.run_reconciler("alice", include_main=False, run_discovery=True)

    deleted = s.google.get_event(s.cal("main"), _ORPHAN_ID)
    assert deleted.get("status") == "cancelled", (
        "a real orphan scan should delete the orphan"
    )
    await s.close()
