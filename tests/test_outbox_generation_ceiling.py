"""Regression for the runaway deterministic-id generation blocker.

When every id _do_create derives collides with a cancelled tombstone,
the per-attempt loop raises, the op is retried, and it resumes from the
*persisted* generation — so without an absolute ceiling the generation
climbs without bound (observed at 3492 in production) while the real
event is never mirrored and ~hundreds of API calls are burned per drain.

These tests assert the absolute ceiling (_MAX_TOTAL_ID_GENERATIONS):
once reached, the op is marked a permanent failure (and the projection
permanently_failed), instead of burning ids forever.
"""

from datetime import datetime, timezone

import pytest

from app.ledger import outbox
from app.ledger.outbox import _MAX_TOTAL_ID_GENERATIONS, _do_create

UTC = timezone.utc


class _HttpErr(Exception):
    def __init__(self, status):
        super().__init__(f"http {status}")
        self.status = status


class _BurnedGoogle:
    """Every insert 409s; every GET says the id is a cancelled tombstone,
    so _do_create burns the generation on every attempt."""

    def __init__(self):
        self.insert_calls = 0

    async def insert_event(self, cal_id, body):
        self.insert_calls += 1
        raise _HttpErr(409)

    async def get_event(self, cal_id, gid):
        return {"status": "cancelled"}


class _BurnedGoogleSync:
    """Synchronous twin of _BurnedGoogle for drain_user tests — the
    drain wraps its client with as_async_google (asyncio.to_thread),
    which requires a sync GoogleClient."""

    def insert_event(self, cal_id, body):
        raise _HttpErr(409)

    def get_event(self, cal_id, gid):
        return {"status": "cancelled"}


async def _seed_op(db, *, start_generation, status="in_flight"):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')"
    )
    user_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_events (user_id, canonical_uid, source_type) "
        "VALUES (?, 'uid-1', 'client')",
        (user_id,),
    )
    le_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, desired_state, desired_ledger_version, "
        " current_state, google_id_generation) "
        "VALUES (?, 'client', 'present', 1, 'absent', ?)",
        (le_id, start_generation),
    )
    proj_id = int(cur.lastrowid)
    # ``status`` defaults to in_flight for the direct _do_create tests;
    # the drain-level test seeds a due *pending* op instead so
    # drain_user's _claim_next picks it up.
    cur = await db.execute(
        "INSERT INTO outbox_operations "
        "(user_id, projection_id, operation, idempotency_key, "
        " ledger_version_at_enqueue, target_google_calendar_id, "
        " payload_json, status, next_attempt_at) "
        "VALUES (?, ?, 'create', 'idem-1', 1, 'cal-google-id', "
        " '{\"summary\": \"X\"}', ?, '2020-01-01T00:00:00+00:00')",
        (user_id, proj_id, status),
    )
    op_id = int(cur.lastrowid)
    await db.commit()
    op = await (await db.execute(
        "SELECT * FROM outbox_operations WHERE id=?", (op_id,))).fetchone()
    return op, proj_id


@pytest.mark.asyncio
async def test_resumed_op_at_ceiling_gives_up_without_burning_more(test_db):
    db = test_db
    op, proj_id = await _seed_op(db, start_generation=_MAX_TOTAL_ID_GENERATIONS)
    g = _BurnedGoogle()

    outcome = await _do_create(
        db, g, op, "cal-google-id", {"summary": "X"}, now=datetime.now(UTC),
    )

    # The give-up must be REPORTED, not just recorded: _execute_op
    # propagates this outcome into drain counters, so returning
    # normally without it would count the give-up as "succeeded".
    assert outcome == "failed_permanent"
    assert g.insert_calls == 0  # gave up immediately, no API calls burned
    op_row = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id=?", (op["id"],))).fetchone()
    assert op_row["status"] == "permanent_failure"
    proj = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections WHERE id=?", (proj_id,)
    )).fetchone()
    assert proj["permanently_failed"] == 1


@pytest.mark.asyncio
async def test_burning_stops_at_ceiling(test_db):
    db = test_db
    op, proj_id = await _seed_op(db, start_generation=_MAX_TOTAL_ID_GENERATIONS - 1)
    g = _BurnedGoogle()

    outcome = await _do_create(
        db, g, op, "cal-google-id", {"summary": "X"}, now=datetime.now(UTC),
    )

    assert outcome == "failed_permanent"
    # One more burn pushed generation to the cap, then it gave up.
    assert g.insert_calls == 1
    proj = await (await db.execute(
        "SELECT google_id_generation, permanently_failed "
        "FROM ledger_projections WHERE id=?", (proj_id,))).fetchone()
    assert proj["google_id_generation"] == _MAX_TOTAL_ID_GENERATIONS
    assert proj["permanently_failed"] == 1
    op_row = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id=?", (op["id"],))).fetchone()
    assert op_row["status"] == "permanent_failure"


@pytest.mark.asyncio
async def test_give_up_counts_as_failed_permanent_in_drain_counters(test_db):
    """Regression: _execute_op used to fall through to a blanket
    'succeeded' for creates, so the burned-id give-up — the case that
    should alert operators — was counted as a success (succeeded=1,
    failed_permanent=0) by drain_user and everything above it."""
    db = test_db
    op, proj_id = await _seed_op(
        db, start_generation=_MAX_TOTAL_ID_GENERATIONS, status="pending",
    )

    counters = await outbox.drain_user(
        db, _BurnedGoogleSync(),
        user_id=int(op["user_id"]), now=datetime.now(UTC),
    )

    assert counters["processed"] == 1
    assert counters["failed_permanent"] == 1
    assert counters["succeeded"] == 0
    op_row = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id=?", (op["id"],))).fetchone()
    assert op_row["status"] == "permanent_failure"
