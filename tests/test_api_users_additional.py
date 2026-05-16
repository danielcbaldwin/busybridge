"""Additional coverage tests for api.users."""

from __future__ import annotations

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


def _user_model(user_id: int, email: str) -> User:
    return User(
        id=user_id,
        email=email,
        google_user_id=f"{email}-google",
        display_name="User",
        main_calendar_id="main-cal",
        is_admin=False,
    )


@pytest.mark.asyncio
async def test_list_my_calendars_error_path(test_db, monkeypatch):
    """User calendar list endpoint should raise HTTP 500 when Google fetch fails."""
    from app.api.users import list_my_calendars

    user_id = await _insert_user("users-extra@example.com", "users-extra-google")
    user = _user_model(user_id, "users-extra@example.com")

    async def exploding_fetch(*_args, **_kwargs):
        raise RuntimeError("google down")

    monkeypatch.setattr(
        "app.auth.google.fetch_calendar_list", exploding_fetch,
    )

    with pytest.raises(HTTPException) as exc:
        await list_my_calendars(user=user)
    assert exc.value.status_code == 500
    assert "Failed to list calendars" in exc.value.detail
