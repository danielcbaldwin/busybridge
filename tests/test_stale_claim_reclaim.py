"""A crashed reconcile must not freeze a user out permanently.

``claim_due_request`` sets ``reconcile_requests.in_flight = 1``; a
clean pass clears it via ``release_request``.  If the process dies
mid-reconcile the row stays stuck — and ``drain_all_due_users`` only
SELECTs ``in_flight = 0`` rows, so that user would never reconcile
again.  ``STALE_CLAIM_TIMEOUT`` bounds how long a claim can be held;
past it the claim is treated as abandoned and reclaimed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.ledger.triggers import (
    STALE_CLAIM_TIMEOUT,
    claim_due_request,
    reclaim_stale_requests,
)
from tests.integration.framework import Scenario

UTC = timezone.utc
pytestmark = pytest.mark.asyncio


async def _put_request(db, user_id, *, in_flight, last_run_at, scheduled_for):
    await db.execute(
        """INSERT INTO reconcile_requests
              (user_id, in_flight, last_run_at, scheduled_for, enqueued_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            user_id,
            1 if in_flight else 0,
            last_run_at.isoformat() if last_run_at else None,
            scheduled_for.isoformat() if scheduled_for else None,
            (last_run_at or scheduled_for or datetime.now(UTC)).isoformat(),
        ),
    )
    await db.commit()


async def _in_flight(db, user_id):
    row = await (await db.execute(
        "SELECT in_flight FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    return bool(row["in_flight"])


async def test_stale_in_flight_claim_is_reclaimed():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    await _put_request(
        db, user.user_id,
        in_flight=True,
        last_run_at=now - STALE_CLAIM_TIMEOUT - timedelta(minutes=5),
        scheduled_for=now - timedelta(hours=1),
    )

    reclaimed = await reclaim_stale_requests(db, now=now)
    assert reclaimed == 1
    assert not await _in_flight(db, user.user_id)
    await s.close()


async def test_fresh_in_flight_claim_is_left_alone():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    await _put_request(
        db, user.user_id,
        in_flight=True,
        last_run_at=now - timedelta(minutes=1),
        scheduled_for=now - timedelta(hours=1),
    )

    reclaimed = await reclaim_stale_requests(db, now=now)
    assert reclaimed == 0
    assert await _in_flight(db, user.user_id), \
        "a reconcile still within the timeout was wrongly reclaimed"
    await s.close()


async def test_claim_due_request_takes_over_a_stale_claim():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    await _put_request(
        db, user.user_id,
        in_flight=True,
        last_run_at=now - STALE_CLAIM_TIMEOUT - timedelta(minutes=1),
        scheduled_for=now - timedelta(hours=1),
    )

    claimed = await claim_due_request(db, user_id=user.user_id, now=now)
    assert claimed is not None, "stale claim was not taken over"
    await s.close()


async def test_two_claims_cannot_both_win_a_due_request():
    """Compare-and-claim: a second claim of a request the first call
    already grabbed returns None, even though it was never released —
    the conditional UPDATE, not a select-then-update, is what makes
    the claim safe under a race."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    await _put_request(
        db, user.user_id,
        in_flight=False,
        last_run_at=None,
        scheduled_for=now - timedelta(minutes=1),
    )

    first = await claim_due_request(db, user_id=user.user_id, now=now)
    second = await claim_due_request(db, user_id=user.user_id, now=now)
    assert first is not None, "first claim should have won the request"
    assert second is None, "a second claim double-won the same request"
    await s.close()


async def test_claim_due_request_respects_a_fresh_claim():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)

    await _put_request(
        db, user.user_id,
        in_flight=True,
        last_run_at=now - timedelta(minutes=2),
        scheduled_for=now - timedelta(hours=1),
    )

    claimed = await claim_due_request(db, user_id=user.user_id, now=now)
    assert claimed is None, "a live in-flight reconcile was double-claimed"
    await s.close()
