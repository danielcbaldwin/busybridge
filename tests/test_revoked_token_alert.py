"""A revoked Google token must not fail silently.

When an account's refresh token is revoked (``invalid_grant``) the
ledger runtime cannot build a client for it.  ``build_user_google_access``
skips that account — but it must also tell the user, otherwise their
calendar quietly stops syncing with no signal at all.
``runtime._alert_if_token_revoked`` queues a ``token_revoked`` alert
for a revocation; a transient build failure must not.
"""

from __future__ import annotations

import pytest

from app.database import get_database, set_setting
from app.ledger.runtime import _alert_if_token_revoked


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    row = await (await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES (?, ?, ?) RETURNING id",
        (email, google_user_id, "User"),
    )).fetchone()
    await db.commit()
    return int(row["id"])


async def _token_revoked_alert_count() -> int:
    db = await get_database()
    rows = await (await db.execute(
        "SELECT id FROM alert_queue WHERE alert_type = 'token_revoked'",
    )).fetchall()
    return len(rows)


@pytest.mark.asyncio
async def test_revoked_token_queues_alert(test_db):
    user_id = await _insert_user("revoked@example.com", "revoked-google")
    await set_setting("alerts_enabled", "true")

    revoked = ValueError(
        'Token refresh failed: {"error": "invalid_grant"}'
    )
    handled = await _alert_if_token_revoked(
        user_id, "revoked@example.com", revoked,
    )
    assert handled is True
    assert await _token_revoked_alert_count() >= 1


@pytest.mark.asyncio
async def test_transient_build_error_does_not_alert(test_db):
    user_id = await _insert_user("flaky@example.com", "flaky-google")
    await set_setting("alerts_enabled", "true")

    transient = RuntimeError("connection reset by peer")
    handled = await _alert_if_token_revoked(
        user_id, "flaky@example.com", transient,
    )
    assert handled is False
    assert await _token_revoked_alert_count() == 0
