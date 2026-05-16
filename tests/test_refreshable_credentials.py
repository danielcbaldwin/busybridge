"""A reconcile pass's Google credentials must be able to refresh.

``runtime._default_google_client_factory`` builds the per-account
``RealGoogleClient``.  A reconcile + outbox-drain pass can run longer
than an access token's ~1h lifetime; if the Credentials carry only the
access token, ``googleapiclient`` cannot re-mint it and every op past
the hour mark 401s — which the outbox would then poison-pill.
``build_user_credentials`` must produce a Credentials that also
carries the refresh token and OAuth client config.
"""

from __future__ import annotations

import pytest

from app.auth.google import build_user_credentials, store_oauth_tokens
from app.database import get_database
from app.encryption import encrypt_value, init_encryption_manager


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    row = await (await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES (?, ?, ?) RETURNING id",
        (email, google_user_id, "User"),
    )).fetchone()
    await db.commit()
    return int(row["id"])


@pytest.mark.asyncio
async def test_build_user_credentials_is_refresh_capable(
    test_db, test_encryption_key,
):
    init_encryption_manager(test_encryption_key)
    db = await get_database()
    await db.execute(
        "INSERT INTO organization "
        "(google_workspace_domain, google_client_id_encrypted, "
        " google_client_secret_encrypted) VALUES (?, ?, ?)",
        (
            "example.com",
            encrypt_value("client.apps.googleusercontent.com"),
            encrypt_value("super-secret-value"),
        ),
    )
    await db.commit()

    user_id = await _insert_user("home@example.com", "home-google")
    await store_oauth_tokens(
        user_id=user_id,
        account_type="home",
        email="home@example.com",
        access_token="access-current",
        refresh_token="refresh-current",
        expires_in=3600,
    )

    creds = await build_user_credentials(user_id, "home@example.com")

    assert creds.token == "access-current"
    # The fields googleapiclient needs to refresh the token itself:
    assert creds.refresh_token == "refresh-current"
    assert creds.client_id == "client.apps.googleusercontent.com"
    assert creds.client_secret == "super-secret-value"
    assert creds.token_uri, "token endpoint not set — cannot refresh"
    # google-auth needs a naive-UTC expiry; a tz-aware one raises when
    # compared against utcnow().
    assert creds.expiry is not None
    assert creds.expiry.tzinfo is None

    # scopes is intentionally NOT pinned: the builder serves home,
    # client, and personal accounts, which hold different scope sets,
    # and a refresh does not need the list.
    assert not creds.scopes

    # A bare-token Credentials (the old behaviour) carries none of the
    # above — it simply cannot refresh.
    from google.oauth2.credentials import Credentials
    bare = Credentials(token="x")
    assert not bare.refresh_token
