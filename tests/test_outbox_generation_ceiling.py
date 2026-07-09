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


class _TombstoneOnceGoogle:
    """First insert collides with a cancelled tombstone (our own prior
    delete), the retry at the bumped generation succeeds — the normal
    absent->present toggle shape."""

    def __init__(self):
        self.insert_calls = 0

    async def insert_event(self, cal_id, body):
        self.insert_calls += 1
        if self.insert_calls == 1:
            raise _HttpErr(409)
        return {"id": body["id"], "etag": "e2"}

    async def get_event(self, cal_id, gid):
        return {"status": "cancelled"}


async def _proj_row(db, proj_id):
    return await (await db.execute(
        "SELECT * FROM ledger_projections WHERE id=?", (proj_id,))).fetchone()


@pytest.mark.asyncio
async def test_high_lifetime_generation_with_advanced_floor_still_creates(test_db):
    """The ceiling is PER EPISODE (generation - floor), not lifetime.

    Regression: production projections reached generation 3000+ purely
    from routine delete/recreate toggles (each burns exactly one id by
    design); a lifetime cap falsely bricked them the moment they crossed
    50 even though every episode converged in one bump.
    """
    db = test_db
    op, proj_id = await _seed_op(db, start_generation=3492)
    await db.execute(
        "UPDATE ledger_projections SET google_id_generation_floor = 3492 "
        "WHERE id = ?", (proj_id,))
    await db.commit()
    g = _TombstoneOnceGoogle()

    outcome = await _do_create(
        db, g, op, "cal-google-id", {"summary": "X"}, now=datetime.now(UTC),
    )

    assert outcome == "succeeded"
    assert g.insert_calls == 2  # one tombstone collision, one success
    proj = await _proj_row(db, proj_id)
    # The successful create ends the episode: floor catches up.
    assert proj["google_id_generation"] == 3493
    assert proj["google_id_generation_floor"] == 3493
    assert proj["permanently_failed"] == 0


@pytest.mark.asyncio
async def test_fifty_burns_within_one_episode_still_gives_up(test_db):
    """The pathology the ceiling exists for is unchanged: 50 burned ids
    with no successful create in between → permanent failure + alert."""
    db = test_db
    op, proj_id = await _seed_op(db, start_generation=3492)
    await db.execute(
        "UPDATE ledger_projections SET google_id_generation_floor = ? "
        "WHERE id = ?", (3492 - _MAX_TOTAL_ID_GENERATIONS, proj_id))
    await db.commit()
    g = _BurnedGoogle()

    outcome = await _do_create(
        db, g, op, "cal-google-id", {"summary": "X"}, now=datetime.now(UTC),
    )

    assert outcome == "failed_permanent"
    assert g.insert_calls == 0  # episode budget already exhausted
    proj = await _proj_row(db, proj_id)
    assert proj["permanently_failed"] == 1


@pytest.mark.asyncio
async def test_admin_retry_resets_episode_and_recovers(test_db):
    """retry_permanent_failures must reset the episode floor; without it
    the retried create insta-fails on the entry check and the admin
    action is a documented-but-broken no-op loop."""
    from app.ledger import admin_ops

    db = test_db
    op, proj_id = await _seed_op(db, start_generation=_MAX_TOTAL_ID_GENERATIONS)
    g = _BurnedGoogle()
    outcome = await _do_create(
        db, g, op, "cal-google-id", {"summary": "X"}, now=datetime.now(UTC),
    )
    assert outcome == "failed_permanent"

    user_id = int(op["user_id"])
    n = await admin_ops.retry_permanent_failures(db, user_id=user_id)
    assert n == 1
    proj = await _proj_row(db, proj_id)
    assert proj["permanently_failed"] == 0
    assert proj["google_id_generation_floor"] == proj["google_id_generation"]

    # Re-seed a fresh pending op (the failed one is terminal) and prove
    # the retried create actually reaches Google and converges.
    cur = await db.execute(
        "INSERT INTO outbox_operations "
        "(user_id, projection_id, operation, idempotency_key, "
        " ledger_version_at_enqueue, target_google_calendar_id, "
        " payload_json, status, next_attempt_at) "
        "VALUES (?, ?, 'create', 'idem-2', 1, 'cal-google-id', "
        " '{\"summary\": \"X\"}', 'in_flight', '2020-01-01T00:00:00+00:00')",
        (user_id, proj_id),
    )
    await db.commit()
    op2 = await (await db.execute(
        "SELECT * FROM outbox_operations WHERE id=?", (cur.lastrowid,))).fetchone()
    g2 = _TombstoneOnceGoogle()
    outcome = await _do_create(
        db, g2, op2, "cal-google-id", {"summary": "X"}, now=datetime.now(UTC),
    )
    assert outcome == "succeeded"
    assert g2.insert_calls == 2


@pytest.mark.asyncio
async def test_giveup_alert_fires_once_per_failure_episode(test_db, monkeypatch):
    """A flapping event that re-triggers the give-up (planner hash change
    clears permanently_failed, create insta-fails again) must not email
    the operator on every flap — only the first give-up of an episode
    alerts."""
    import app.alerts.email as alerts

    calls = []

    async def fake_queue_alert(**kw):
        calls.append(kw)

    monkeypatch.setattr(alerts, "queue_alert", fake_queue_alert)

    db = test_db
    op, proj_id = await _seed_op(db, start_generation=_MAX_TOTAL_ID_GENERATIONS)
    g = _BurnedGoogle()
    now = datetime.now(UTC)

    assert await _do_create(db, g, op, "cal-google-id", {"summary": "X"}, now=now) \
        == "failed_permanent"
    assert len(calls) == 1

    # Second give-up while still permanently_failed: no new alert.
    cur = await db.execute(
        "INSERT INTO outbox_operations "
        "(user_id, projection_id, operation, idempotency_key, "
        " ledger_version_at_enqueue, target_google_calendar_id, "
        " payload_json, status, next_attempt_at) "
        "VALUES (?, ?, 'create', 'idem-3', 1, 'cal-google-id', "
        " '{\"summary\": \"X\"}', 'in_flight', '2020-01-01T00:00:00+00:00')",
        (int(op["user_id"]), proj_id),
    )
    await db.commit()
    op2 = await (await db.execute(
        "SELECT * FROM outbox_operations WHERE id=?", (cur.lastrowid,))).fetchone()
    assert await _do_create(db, g, op2, "cal-google-id", {"summary": "X"}, now=now) \
        == "failed_permanent"
    assert len(calls) == 1
