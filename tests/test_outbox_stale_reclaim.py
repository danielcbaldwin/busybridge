"""Outbox ops stuck in_flight by a crashed drain must be reclaimed.

``_claim_next`` only ever selects ``pending`` rows.  An op that was
claimed (status ``in_flight``, ``started_at`` stamped) by a drain that
then crashed is otherwise stranded forever — never retried.
``_reclaim_stale_operations`` resets ops in-flight past
``_STALE_OP_TIMEOUT`` back to ``pending``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.ledger.outbox import _STALE_OP_TIMEOUT, _reclaim_stale_operations
from tests.integration.framework import Scenario

UTC = timezone.utc
pytestmark = pytest.mark.asyncio


async def _make_in_flight_op(db, user_id, *, started_at):
    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'client:1:x', 'client', 'active', 1,
                   '2026-01-01', '2026-01-01') RETURNING id""",
        (user_id,),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version, current_state)
           VALUES (?, 'main', 'present_full', 'h', 1, 'present')
           RETURNING id""",
        (int(ev["id"]),),
    )).fetchone()
    op = await (await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, attempts, started_at)
           VALUES (?, ?, 'update', 'k1', 1, 'main@cal', 'in_flight', 1, ?)
           RETURNING id""",
        (user_id, int(proj["id"]), started_at),
    )).fetchone()
    await db.commit()
    return int(op["id"])


async def _status(db, op_id):
    row = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id = ?", (op_id,),
    )).fetchone()
    return row["status"]


async def test_stale_in_flight_op_is_reclaimed():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    op_id = await _make_in_flight_op(
        db, user.user_id,
        started_at=(now - _STALE_OP_TIMEOUT - timedelta(minutes=1)).isoformat(),
    )

    reclaimed = await _reclaim_stale_operations(db, user_id=user.user_id, now=now)
    assert reclaimed == 1
    assert await _status(db, op_id) == "pending"
    await s.close()


async def test_fresh_in_flight_op_is_left_alone():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    op_id = await _make_in_flight_op(
        db, user.user_id,
        started_at=(now - timedelta(minutes=1)).isoformat(),
    )

    reclaimed = await _reclaim_stale_operations(db, user_id=user.user_id, now=now)
    assert reclaimed == 0
    assert await _status(db, op_id) == "in_flight", \
        "an op still within the timeout was wrongly reclaimed"
    await s.close()
