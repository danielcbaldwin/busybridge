"""Additional coverage tests for auth.session branches."""

from __future__ import annotations

import pytest
from starlette.requests import Request

from app.auth.session import (
    SESSION_COOKIE_NAME,
    User,
    create_session_token,
    get_current_user,
    get_user_by_id,
    require_admin,
)
from app.database import get_database


def _request_with_cookie(token: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"cookie", f"{SESSION_COOKIE_NAME}={token}".encode())],
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_get_user_by_id_missing_returns_none(test_db):
    """Missing user lookup should return None."""
    assert await get_user_by_id(999999) is None


@pytest.mark.asyncio
async def test_get_current_user_success_and_require_admin_success(test_db):
    """Current-user resolver and admin guard should return user objects on success."""
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, is_admin)
           VALUES ('session-ok@example.com', 'session-ok-google', 'Session User', FALSE)
           RETURNING id"""
    )
    user_id = (await cursor.fetchone())["id"]
    await db.commit()

    token = create_session_token(user_id=user_id, email="session-ok@example.com", is_admin=False)
    resolved = await get_current_user(_request_with_cookie(token))
    assert resolved.id == user_id

    admin_user = User(
        id=1,
        email="admin@example.com",
        google_user_id="admin-google",
        display_name="Admin",
        main_calendar_id="main",
        is_admin=True,
    )
    assert await require_admin(admin_user) is admin_user


@pytest.mark.asyncio
async def test_bumping_token_version_revokes_an_old_session(test_db):
    """A session token is rejected once the user's
    session_token_version has moved past the token's stamp."""
    from app.auth.session import get_current_user_optional

    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES ('revoke@example.com', 'revoke-google', 'Revoke User')
           RETURNING id"""
    )
    user_id = (await cursor.fetchone())["id"]
    await db.commit()

    # A token minted at the current version resolves fine.
    token = create_session_token(
        user_id=user_id, email="revoke@example.com", token_version=0,
    )
    assert (await get_current_user_optional(_request_with_cookie(token))) is not None

    # An admin force-reauth bumps the version — the old token is dead.
    await db.execute(
        "UPDATE users SET session_token_version = session_token_version + 1 "
        "WHERE id = ?",
        (user_id,),
    )
    await db.commit()
    assert (await get_current_user_optional(_request_with_cookie(token))) is None
