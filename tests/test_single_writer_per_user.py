"""Single writer per user.

Every sync trigger — a webhook's delayed drain, the periodic
scheduler, a manual sync — funnels through ``reconcile_user_by_id``.
No two reconciles for the *same* user may run concurrently (that
would be two ingest/diff/drain passes racing over one user's
calendars); reconciles for *different* users may run in parallel.
"""

from __future__ import annotations

import asyncio

import pytest

from app.database import get_database
from app.ledger.runtime import reconcile_user_by_id, set_google_client_factory
from tests.fakes.google_calendar import FakeGoogleCalendar

pytestmark = pytest.mark.asyncio


async def _seed_user(db, *, email: str, main_cal: str) -> int:
    uid = int((await (await db.execute(
        """INSERT INTO users
              (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, 'U', ?) RETURNING id""",
        (email, f"g-{email}", main_cal),
    )).fetchone())["id"])
    await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'home', ?, ?, ?)""",
        (uid, email, b"x", b"y"),
    )
    return uid


def _tracking_factory(shared: FakeGoogleCalendar, state: dict):
    """A google-client factory that records how many reconciles are
    inside their locked body at once.  It is invoked from within
    ``reconcile_user_by_id``'s per-user lock, and yields the event
    loop (sleep) so an overlapping reconcile would be observed."""

    async def factory(user_id, email):
        state["active"] += 1
        state["max"] = max(state["max"], state["active"])
        await asyncio.sleep(0.03)
        state["active"] -= 1
        return shared

    return factory


async def test_concurrent_reconciles_for_one_user_are_serialized(test_db):
    db = await get_database()
    uid = await _seed_user(db, email="solo@test", main_cal="main@solo.test")
    await db.commit()
    shared = FakeGoogleCalendar()
    shared.add_calendar("main@solo.test")

    state = {"active": 0, "max": 0}
    set_google_client_factory(_tracking_factory(shared, state))
    try:
        results = await asyncio.gather(
            reconcile_user_by_id(uid),
            reconcile_user_by_id(uid),
        )
    finally:
        set_google_client_factory(None)

    assert all(isinstance(r, dict) for r in results)
    assert state["max"] == 1, (
        f"two reconciles for one user overlapped (peak concurrency "
        f"{state['max']}) — single-writer-per-user is not enforced"
    )


async def test_reconciles_for_different_users_run_concurrently(test_db):
    db = await get_database()
    uid_a = await _seed_user(db, email="a@test", main_cal="main@a.test")
    uid_b = await _seed_user(db, email="b@test", main_cal="main@b.test")
    await db.commit()
    shared = FakeGoogleCalendar()
    shared.add_calendar("main@a.test")
    shared.add_calendar("main@b.test")

    state = {"active": 0, "max": 0}
    set_google_client_factory(_tracking_factory(shared, state))
    try:
        await asyncio.gather(
            reconcile_user_by_id(uid_a),
            reconcile_user_by_id(uid_b),
        )
    finally:
        set_google_client_factory(None)

    # The lock is per-user, not global — different users overlap.
    assert state["max"] == 2, (
        f"reconciles for different users were serialized (peak "
        f"concurrency {state['max']}) — the lock is too coarse"
    )
