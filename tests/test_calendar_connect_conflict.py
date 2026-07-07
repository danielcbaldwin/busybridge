"""Duplicate calendar connection race.

The connect endpoints are check-then-insert with a slow Google verify
call in between, so two concurrent connects of the same calendar can
both pass the "already connected" check.  The partial UNIQUE index on
active (user_id, google_calendar_id) makes the loser's INSERT fail,
and the endpoints translate that into HTTP 409.
"""

from __future__ import annotations

import sqlite3

import pytest
from fastapi import HTTPException

from app.auth.session import User
from app.database import get_database


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, main_calendar_id)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, "User", "main-cal"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(user_id: int, email: str, account_type: str = "client") -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email, access_token_encrypted, refresh_token_encrypted)
           VALUES (?, ?, ?, ?, ?)
           RETURNING id""",
        (user_id, account_type, email, b"a", b"r"),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


def _user_model(user_id: int, email: str) -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"{email}-google",
        display_name="User",
        main_calendar_id="main-cal",
        is_admin=False,
    )


async def _count_active(user_id: int, google_calendar_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """SELECT COUNT(*) AS n FROM client_calendars
           WHERE user_id = ? AND google_calendar_id = ? AND is_active = TRUE""",
        (user_id, google_calendar_id),
    )
    return int((await cursor.fetchone())["n"])


@pytest.mark.asyncio
async def test_client_double_connect_race_returns_409(test_db, monkeypatch):
    """A concurrent connect whose INSERT lands between this request's
    duplicate check and its own INSERT must produce a 409, not a
    duplicate active row."""
    from app.api.calendars import ConnectCalendarRequest, connect_client_calendar

    user_id = await _insert_user("race@example.com", "race-google")
    token_id = await _insert_token(user_id, "race-client@example.com")
    user = _user_model(user_id, "race@example.com")

    real_db = await get_database()

    class RacingDB:
        """Delegates to the real connection, but lands a concurrent
        request's duplicate INSERT right after this request's
        already-connected check has passed."""

        def __init__(self):
            self.fired = False

        def __getattr__(self, name):
            return getattr(real_db, name)

        async def execute(self, sql, *args):
            cursor = await real_db.execute(sql, *args)
            if not self.fired and "SELECT id FROM client_calendars" in sql:
                self.fired = True
                await real_db.execute(
                    """INSERT INTO client_calendars
                       (user_id, oauth_token_id, google_calendar_id, display_name)
                       VALUES (?, ?, ?, ?)""",
                    (user_id, token_id, "cal-race", "Cal"),
                )
                await real_db.commit()
            return cursor

    racing_db = RacingDB()

    async def fake_get_database():
        return racing_db

    async def fake_fetch_calendar(_user_id, _email, _calendar_id):
        return {"summary": "Cal"}

    monkeypatch.setattr("app.api.calendars.get_database", fake_get_database)
    monkeypatch.setattr("app.auth.google.fetch_calendar", fake_fetch_calendar)

    with pytest.raises(HTTPException) as exc:
        await connect_client_calendar(
            request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-race"),
            user=user,
        )
    assert exc.value.status_code == 409
    assert "already connected" in exc.value.detail.lower()

    # Only the winner's row exists.
    assert await _count_active(user_id, "cal-race") == 1


@pytest.mark.asyncio
async def test_personal_double_connect_race_returns_409(test_db, monkeypatch):
    """Same race for the personal-calendars batch connect endpoint."""
    from app.api.personal_calendars import (
        ConnectPersonalCalendarRequest,
        connect_personal_calendars,
    )

    user_id = await _insert_user("prace@example.com", "prace-google")
    token_id = await _insert_token(user_id, "prace-personal@example.com", account_type="personal")
    user = _user_model(user_id, "prace@example.com")

    async def racing_fetch_calendar(_user_id, _email, _calendar_id):
        db = await get_database()
        await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name, calendar_type)
               VALUES (?, ?, ?, ?, 'personal')""",
            (user_id, token_id, "personal-race", "Personal"),
        )
        await db.commit()
        return {"summary": "Personal"}

    monkeypatch.setattr("app.auth.google.fetch_calendar", racing_fetch_calendar)

    with pytest.raises(HTTPException) as exc:
        await connect_personal_calendars(
            request=ConnectPersonalCalendarRequest(
                token_id=token_id,
                calendars=[{"calendar_id": "personal-race", "display_name": "Personal"}],
            ),
            user=user,
        )
    assert exc.value.status_code == 409
    assert "already connected" in exc.value.detail.lower()

    assert await _count_active(user_id, "personal-race") == 1


@pytest.mark.asyncio
async def test_sequential_double_connect_still_400(test_db, monkeypatch):
    """The non-racing duplicate path keeps its original 400."""
    from app.api.calendars import ConnectCalendarRequest, connect_client_calendar

    user_id = await _insert_user("twice@example.com", "twice-google")
    token_id = await _insert_token(user_id, "twice-client@example.com")
    user = _user_model(user_id, "twice@example.com")

    async def fake_fetch_calendar(_user_id, _email, _calendar_id):
        return {"summary": "Cal"}

    monkeypatch.setattr("app.auth.google.fetch_calendar", fake_fetch_calendar)
    monkeypatch.setattr("app.utils.tasks.create_background_task", lambda coro, *a, **kw: coro.close())

    first = await connect_client_calendar(
        request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-twice"),
        user=user,
    )
    assert first.google_calendar_id == "cal-twice"

    with pytest.raises(HTTPException) as exc:
        await connect_client_calendar(
            request=ConnectCalendarRequest(token_id=token_id, calendar_id="cal-twice"),
            user=user,
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_unique_index_allows_inactive_duplicates(test_db):
    """Disconnect + reconnect leaves is_active = FALSE rows behind —
    the UNIQUE index is partial, so only ACTIVE duplicates are refused."""
    user_id = await _insert_user("inactive@example.com", "inactive-google")
    token_id = await _insert_token(user_id, "inactive-client@example.com")

    db = await get_database()
    # An active row plus an inactive duplicate (a prior disconnect).
    await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, 'cal-dup', 'Old', FALSE)""",
        (user_id, token_id),
    )
    await db.execute(
        """INSERT INTO client_calendars
           (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
           VALUES (?, ?, 'cal-dup', 'Current', TRUE)""",
        (user_id, token_id),
    )
    await db.commit()

    # A second ACTIVE row for the same (user, calendar) is refused.
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute(
            """INSERT INTO client_calendars
               (user_id, oauth_token_id, google_calendar_id, display_name, is_active)
               VALUES (?, ?, 'cal-dup', 'Dupe', TRUE)""",
            (user_id, token_id),
        )
