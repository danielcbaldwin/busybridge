"""Per-calendar sync-failure alerting (REWRITE_PLAN.md §12).

Distinct from the circuit breaker (which pauses sync only when
EVERY calendar fails): when a single calendar is stuck at 5+
consecutive failures the user gets an email alert naming it.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.jobs.sync_job import _alert_failing_calendars

pytestmark = pytest.mark.asyncio


async def _user(db, email="f@e.com") -> int:
    row = await (await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, 'F') RETURNING id""",
        (email, email),
    )).fetchone()
    return int(row["id"])


async def _oauth_token(db, user_id, email) -> int:
    row = await (await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?) RETURNING id""",
        (user_id, email, b"x", b"y"),
    )).fetchone()
    return int(row["id"])


async def _calendar(db, user_id, gid, name, failures, error=None) -> int:
    token_id = await _oauth_token(db, user_id, f"{gid}.tok")
    row = await (await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id, display_name,
               calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'client', 1) RETURNING id""",
        (user_id, token_id, gid, name),
    )).fetchone()
    cal_id = int(row["id"])
    await db.execute(
        """INSERT INTO calendar_sync_state
              (client_calendar_id, consecutive_failures, last_error)
           VALUES (?, ?, ?)""",
        (cal_id, failures, error),
    )
    return cal_id


async def test_alert_fires_only_for_calendars_at_five_plus_failures(
    test_db, monkeypatch,
):
    db = await get_database()
    uid = await _user(db)
    await _calendar(db, uid, "cal1@g", "Client One", 6, "boom")
    await _calendar(db, uid, "cal2@g", "Client Two", 1)
    await db.commit()

    calls: list[dict] = []

    async def fake_queue_alert(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _alert_failing_calendars()

    assert len(calls) == 1
    assert calls[0]["alert_type"] == "calendar_sync_failing"
    assert calls[0]["user_id"] == uid
    # The failing calendar is named; the healthy one is not.
    assert "Client One" in calls[0]["details"]
    assert "Client Two" not in calls[0]["details"]


async def test_no_alert_when_no_calendar_is_failing(test_db, monkeypatch):
    db = await get_database()
    uid = await _user(db, "ok@e.com")
    await _calendar(db, uid, "cal@g", "Healthy", 0)
    await db.commit()

    calls: list[dict] = []

    async def fake_queue_alert(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _alert_failing_calendars()

    assert calls == []
