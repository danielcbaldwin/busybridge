"""Reconnecting a Google email must update its account type.

store_oauth_tokens upserts on (user_id, google_account_email).  If the
same email is reconnected as a different account type (home → client,
etc.) the row's account_type must follow, or downstream account-type
checks behave inconsistently.
"""

from __future__ import annotations

import pytest

from app.auth.google import get_oauth_token, store_oauth_tokens
from app.database import get_database
from app.encryption import init_encryption_manager


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    row = await (await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES (?, ?, 'U') RETURNING id",
        (email, google_user_id),
    )).fetchone()
    await db.commit()
    return int(row["id"])


@pytest.mark.asyncio
async def test_reconnect_updates_account_type_on_conflict(
    test_db, test_encryption_key,
):
    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user("user@example.com", "g-user")

    await store_oauth_tokens(
        user_id=user_id, account_type="home",
        email="acct@example.com",
        access_token="a1", refresh_token="r1",
    )
    # The same email is reconnected, now as a client account.
    await store_oauth_tokens(
        user_id=user_id, account_type="client",
        email="acct@example.com",
        access_token="a2", refresh_token="r2",
    )

    token = await get_oauth_token(user_id, "acct@example.com")
    assert token is not None
    assert token["account_type"] == "client", (
        "account_type was not updated on reconnect"
    )
