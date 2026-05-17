"""Factory reset must invalidate existing session cookies.

After a reset the first new admin reuses user_id 1 and
session_token_version 0, so an old signed cookie would authenticate
as that brand-new admin unless the session secret is rotated.
"""

from __future__ import annotations

import pytest

import app.config as config
from app.api.admin import FactoryResetRequest, factory_reset
from app.auth.session import User
from app.config import get_session_secret

pytestmark = pytest.mark.asyncio


async def test_factory_reset_rotates_the_session_secret(test_db):
    config._session_secret_cache = None
    secret_before = get_session_secret()
    assert secret_before

    await factory_reset(
        FactoryResetRequest(confirmation="RESET"),
        admin=User(id=1, email="a@example.com", google_user_id="g", is_admin=True),
    )

    secret_after = get_session_secret()
    assert secret_after, "a fresh session secret must be generated"
    assert secret_after != secret_before, (
        "factory reset must rotate the session secret so old cookies "
        "no longer validate"
    )
