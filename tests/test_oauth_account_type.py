"""Reconnecting a Google email must update its account type — with one
exception: a ``home`` token is never silently downgraded.

store_oauth_tokens upserts on (user_id, google_account_email).  If the
same email is reconnected as a different account type (personal ↔
client) the row's account_type must follow, or downstream account-type
checks behave inconsistently.

The one asymmetric case: a ``home`` account (the account the user logs
into BusyBridge with) staying ``home`` even when the SAME email is
later connected as a personal-source calendar.  The reconciler resolves
the home account by
``SELECT ... FROM oauth_tokens WHERE account_type='home'``; if the
insert path clobbered that to 'personal', ``_resolve_home_email`` would
return None and every reconcile would silently no-op with
``skipped=no_home_oauth_token``.  Preserving home is a one-way guard —
non-home types still upgrade normally.
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
async def test_reconnect_updates_non_home_account_type_on_conflict(
    test_db, test_encryption_key,
):
    """A non-home reconnect (personal ↔ client) updates account_type."""
    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user("user@example.com", "g-user")

    await store_oauth_tokens(
        user_id=user_id, account_type="personal",
        email="acct@example.com",
        access_token="a1", refresh_token="r1",
    )
    await store_oauth_tokens(
        user_id=user_id, account_type="client",
        email="acct@example.com",
        access_token="a2", refresh_token="r2",
    )

    token = await get_oauth_token(user_id, "acct@example.com")
    assert token is not None
    assert token["account_type"] == "client", (
        "account_type was not updated on personal→client reconnect"
    )


@pytest.mark.asyncio
async def test_reconnect_preserves_home_account_type(
    test_db, test_encryption_key,
):
    """A 'home' token is never silently downgraded when the same email
    is reconnected as a personal or client source.

    Without this guard, connecting the same Google account you log in
    with as ALSO a personal calendar rewrites its account_type to
    'personal', and the reconciler's home lookup then returns nothing.
    """
    init_encryption_manager(test_encryption_key)
    user_id = await _insert_user("user@example.com", "g-user")

    await store_oauth_tokens(
        user_id=user_id, account_type="home",
        email="acct@example.com",
        access_token="a1", refresh_token="r1",
    )
    await store_oauth_tokens(
        user_id=user_id, account_type="personal",
        email="acct@example.com",
        access_token="a2", refresh_token="r2",
    )
    await store_oauth_tokens(
        user_id=user_id, account_type="client",
        email="acct@example.com",
        access_token="a3", refresh_token="r3",
    )

    token = await get_oauth_token(user_id, "acct@example.com")
    assert token is not None
    assert token["account_type"] == "home", (
        "home account_type was clobbered by later personal/client reconnect"
    )
