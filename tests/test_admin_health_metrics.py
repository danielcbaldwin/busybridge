"""Admin health metrics must compare timestamps correctly.

webhook_channels.expiration and users.last_login_at are stored as
ISO-8601 strings with a 'T' separator, while SQLite's datetime('now')
yields a space-separated string.  A raw string comparison therefore
mis-orders same-day values (the 'T' sorts after the space), which
let expired webhooks count as active.  The queries wrap the column
in datetime() so both sides are normalised.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.auth.session import User
from app.database import get_database

pytestmark = pytest.mark.asyncio


def _admin() -> User:
    return User(
        id=1, email="admin@example.com", google_user_id="g-admin",
        is_admin=True,
    )


async def test_expired_webhook_is_not_counted_active(test_db):
    """A webhook that expired earlier *today* must not be counted as
    active — the bug the datetime() normalisation fixes."""
    from app.api.admin import get_system_health

    db = await get_database()
    cur = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES ('u@example.com', 'g-u', 'U') RETURNING id"""
    )
    uid = int((await cur.fetchone())["id"])

    past = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    future = (datetime.utcnow() + timedelta(days=2)).isoformat()
    for chan, exp in (("expired", past), ("live", future)):
        await db.execute(
            """INSERT INTO webhook_channels
                  (user_id, calendar_type, channel_id, resource_id,
                   token, expiration)
               VALUES (?, 'main', ?, ?, '', ?)""",
            (uid, chan, f"res-{chan}", exp),
        )
    await db.commit()

    health = await get_system_health(_admin())
    # Only the future-dated channel is active; the same-day expired
    # one must be excluded despite the T-vs-space format difference.
    assert health.webhooks_active == 1
