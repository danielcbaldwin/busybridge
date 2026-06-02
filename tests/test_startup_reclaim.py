"""On startup, work left in_flight by a prior (possibly crashed) process
must be reclaimed immediately — not left for the 15-minute stale sweepers,
which would otherwise leave a post-restart window where affected users do
no syncing. Single-process model makes this safe: at boot nothing else
holds a claim.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.runtime import reclaim_in_flight_on_startup

pytestmark = pytest.mark.asyncio


async def _seed(db):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_events (user_id, canonical_uid, source_type) "
        "VALUES (?, 'uid-1', 'client')", (uid,))
    le = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, desired_state, desired_ledger_version, "
        " current_state) VALUES (?, 'client', 'present', 1, 'absent')", (le,))
    proj = int(cur.lastrowid)

    async def _op(idem, status):
        await db.execute(
            "INSERT INTO outbox_operations "
            "(user_id, projection_id, operation, idempotency_key, "
            " ledger_version_at_enqueue, target_google_calendar_id, status, "
            " started_at) "
            "VALUES (?, ?, 'create', ?, 1, 'cal', ?, '2026-06-01T00:00:00')",
            (uid, proj, idem, status))

    await _op("op-inflight", "in_flight")
    await _op("op-done", "done")
    # A claimed request (scheduled_for consumed to NULL by claim) + an
    # idle one that must be left alone.
    await db.execute(
        "INSERT INTO reconcile_requests (user_id, in_flight, scheduled_for) "
        "VALUES (?, 1, NULL)", (uid,))
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('v@x.com', 'g2')")
    uid2 = int(cur.lastrowid)
    await db.execute(
        "INSERT INTO reconcile_requests (user_id, in_flight, scheduled_for) "
        "VALUES (?, 0, '2026-06-01T00:00:00')", (uid2,))
    await db.commit()
    return uid, uid2


async def test_startup_reclaims_in_flight_work(test_db):
    db = await get_database()
    uid, uid2 = await _seed(db)

    result = await reclaim_in_flight_on_startup()

    assert result == {"outbox_ops": 1, "reconcile_requests": 1}

    # in_flight op → pending; done op untouched.
    statuses = {
        r["idempotency_key"]: r["status"]
        for r in await (await db.execute(
            "SELECT idempotency_key, status FROM outbox_operations")).fetchall()
    }
    assert statuses["op-inflight"] == "pending"
    assert statuses["op-done"] == "done"

    # The claimed request is released AND made due again (scheduled_for set).
    claimed = await (await db.execute(
        "SELECT in_flight, scheduled_for FROM reconcile_requests WHERE user_id=?",
        (uid,))).fetchone()
    assert claimed["in_flight"] == 0
    assert claimed["scheduled_for"] is not None, "reclaimed request must be re-due"

    # The idle request is left exactly as it was.
    idle = await (await db.execute(
        "SELECT in_flight, scheduled_for FROM reconcile_requests WHERE user_id=?",
        (uid2,))).fetchone()
    assert idle["in_flight"] == 0
    assert idle["scheduled_for"] == "2026-06-01T00:00:00"


async def test_startup_reclaim_noop_when_nothing_in_flight(test_db):
    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    # No in-flight rows at all.
    result = await reclaim_in_flight_on_startup()
    assert result == {"outbox_ops": 0, "reconcile_requests": 0}
