"""Circuit breaker (REWRITE_PLAN.md §12).

When EVERY active calendar for a user has failed 3+ times in a row,
the breaker auto-pauses THAT USER's sync (the per-user
``users.sync_paused`` flag) and emails them.  It is per-user: one
user's dead calendars must never pause sync for everyone else, and
a single healthy calendar keeps a user's breaker open.
"""

from __future__ import annotations

import pytest

from app.database import get_database, get_setting
from app.jobs.sync_job import _check_circuit_breaker

pytestmark = pytest.mark.asyncio


async def _user(db, email: str) -> int:
    row = await (await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, 'CB') RETURNING id""",
        (email, email),
    )).fetchone()
    return int(row["id"])


async def _calendar(db, user_id: int, gid: str, *, failures: int) -> None:
    tok = await (await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?) RETURNING id""",
        (user_id, f"{gid}.tok", b"x", b"y"),
    )).fetchone()
    cal = await (await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id, display_name,
               calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'client', 1) RETURNING id""",
        (user_id, int(tok["id"]), gid, gid),
    )).fetchone()
    await db.execute(
        """INSERT INTO calendar_sync_state
              (client_calendar_id, consecutive_failures)
           VALUES (?, ?)""",
        (int(cal["id"]), failures),
    )


async def _is_paused(db, user_id: int) -> bool:
    row = await (await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user_id,),
    )).fetchone()
    return bool(row and row["sync_paused"])


async def test_circuit_breaker_pauses_only_the_failing_user(
    test_db, monkeypatch,
):
    db = await get_database()
    uid = await _user(db, "cb@example.com")
    await _calendar(db, uid, "c1@g", failures=3)
    await _calendar(db, uid, "c2@g", failures=5)
    await db.commit()

    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert await _is_paused(db, uid)
    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "circuit_breaker"
    assert alerts[0]["user_id"] == uid
    # The GLOBAL pause switch must NOT be flipped — only this user.
    assert await get_setting("sync_paused") is None


async def test_circuit_breaker_does_not_pause_other_users(
    test_db, monkeypatch,
):
    """One user's all-failing calendars must not pause anyone else —
    the per-user-detection / global-effect bug."""
    db = await get_database()
    failing = await _user(db, "failing@example.com")
    await _calendar(db, failing, "f1@g", failures=4)
    await _calendar(db, failing, "f2@g", failures=4)
    healthy = await _user(db, "healthy@example.com")
    await _calendar(db, healthy, "h1@g", failures=0)
    await _calendar(db, healthy, "h2@g", failures=0)
    await db.commit()

    async def fake_queue_alert(**kwargs):
        pass

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert await _is_paused(db, failing)
    assert not await _is_paused(db, healthy), (
        "a healthy user was paused by another user's circuit breaker"
    )
    assert await get_setting("sync_paused") is None


async def test_circuit_breaker_stays_open_when_one_calendar_is_healthy(
    test_db, monkeypatch,
):
    db = await get_database()
    uid = await _user(db, "ok@example.com")
    await _calendar(db, uid, "c1@g", failures=5)   # failing
    await _calendar(db, uid, "c2@g", failures=0)   # healthy
    await db.commit()

    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert not await _is_paused(db, uid)
    assert alerts == []
