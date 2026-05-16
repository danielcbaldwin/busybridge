"""Integrity status must come from live ledger state.

The legacy ``integrity_status`` table is never written under the
ledger architecture — its consistency-check job is a no-op — so any
dashboard reading it shows a permanently blank panel.
``facade.integrity_status_for_user`` derives the signal from ledger
projections + the outbox instead.
"""

from __future__ import annotations

import pytest

from app.ledger import facade
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _event(db, user_id):
    row = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'client:1:i', 'client', 'active', 1,
                   '2026-01-01', '2026-01-01') RETURNING id""",
        (user_id,),
    )).fetchone()
    return int(row["id"])


async def _projection(db, ledger_event_id, *, diverged, permanently_failed=0):
    # applied == desired unless we want divergence.
    applied = None if diverged else 1
    row = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version,
               applied_ledger_version, applied_payload_hash,
               current_state, permanently_failed)
           VALUES (?, 'main', 'present_full', 'h', 1, ?, 'h',
                   'present', ?) RETURNING id""",
        (ledger_event_id, applied, permanently_failed),
    )).fetchone()
    return int(row["id"])


async def test_integrity_ok_when_everything_is_applied():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await _event(db, user.user_id)
    await _projection(db, ev, diverged=False)
    await db.commit()

    out = await facade.integrity_status_for_user(db, user_id=user.user_id)
    assert out["status"] == "ok"
    assert out["issues_found"] == 0
    await s.close()


async def test_integrity_warning_on_a_diverged_projection():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await _event(db, user.user_id)
    await _projection(db, ev, diverged=True)
    await db.commit()

    out = await facade.integrity_status_for_user(db, user_id=user.user_id)
    assert out["status"] == "warning"
    assert out["diverged"] == 1
    assert out["permanent_failures"] == 0
    await s.close()


async def test_integrity_error_on_a_permanent_failure():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await _event(db, user.user_id)
    proj = await _projection(db, ev, diverged=True, permanently_failed=1)
    await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, attempts)
           VALUES (?, ?, 'update', 'k', 1, 'main@cal',
                   'permanent_failure', 5)""",
        (user.user_id, proj),
    )
    await db.commit()

    out = await facade.integrity_status_for_user(db, user_id=user.user_id)
    assert out["status"] == "error"
    assert out["permanent_failures"] == 1
    # The poison-pilled projection is excluded from the diverged count.
    assert out["diverged"] == 0
    await s.close()
