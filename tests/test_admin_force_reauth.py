"""Admin force-reauth must clear ALL of a user's webhook channels,
including the main-calendar channel (client_calendar_id IS NULL).
"""

from __future__ import annotations

import pytest

from app.api.admin import force_user_reauth
from app.auth.session import User
from app.database import get_database

pytestmark = pytest.mark.asyncio


def _admin() -> User:
    return User(
        id=1, email="admin@example.com", google_user_id="g-admin",
        is_admin=True,
    )


async def test_force_reauth_clears_main_calendar_webhook(test_db):
    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name, main_calendar_id) "
        "VALUES ('u@example.com', 'g-u', 'U', 'u@example.com') RETURNING id"
    )
    uid = int((await cur.fetchone())["id"])
    # A main-calendar webhook row — client_calendar_id is NULL.
    await db.execute(
        """INSERT INTO webhook_channels
              (user_id, calendar_type, client_calendar_id, channel_id,
               resource_id, token, expiration)
           VALUES (?, 'main', NULL, 'chan-main', 'res-main', '',
                   '2999-01-01T00:00:00')""",
        (uid,),
    )
    await db.commit()

    await force_user_reauth(uid, admin=_admin())

    remaining = await (await db.execute(
        "SELECT COUNT(*) FROM webhook_channels WHERE user_id = ?", (uid,),
    )).fetchone()
    assert remaining[0] == 0, "force-reauth must remove the main webhook row"
