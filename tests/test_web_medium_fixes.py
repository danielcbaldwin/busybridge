"""Regression tests for the medium-severity web-route review fixes.

Covers:
  * ledger admin ops 404 on a nonexistent user (previously: sync-now
    500'd on the FK; resume / full-resync / retry-failed /
    cleanup-and-pause reported fake success after updating 0 rows)
  * webcal create: fetch-returned-None yields the "Could not fetch"
    400 detail (previously re-labelled as a parse failure by the
    generic exception handler)
  * /auth/connect-client and /auth/connect-personal redirect an
    expired browser session to /app/login instead of raw 401 JSON
  * OAuth 'error' query values are URL-encoded into redirect URLs
  * select_calendar_page uses the verified token row's account email,
    not the free-form `email` query param
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.database import get_database


def _request(path: str = "/", method: str = "GET") -> Request:
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": b"",
    }
    return Request(scope)


async def _insert_user(email: str, google_user_id: str) -> int:
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name, created_at)
           VALUES (?, ?, ?, ?)
           RETURNING id""",
        (email, google_user_id, email.split("@")[0], datetime.utcnow().isoformat()),
    )
    row = await cursor.fetchone()
    await db.commit()
    return row["id"]


async def _insert_token(user_id: int, account_type: str, email: str) -> int:
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


# ---------------------------------------------------------------------------
# 1. ledger admin — nonexistent user must 404, not 500 / fake success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ledger_admin_ops_404_for_nonexistent_user(test_db):
    from app.api.ledger_admin import (
        cleanup_and_pause,
        full_resync,
        resume_sync,
        retry_failed,
        sync_now,
    )

    missing = 424242
    for op in (cleanup_and_pause, resume_sync, full_resync, retry_failed, sync_now):
        with pytest.raises(HTTPException) as exc:
            await op(user_id=missing)
        assert exc.value.status_code == 404, op.__name__
        assert exc.value.detail == "User not found"


@pytest.mark.asyncio
async def test_ledger_admin_ops_succeed_for_existing_user(test_db):
    """The guard must not reject real users."""
    from app.api.ledger_admin import resume_sync, sync_now

    user_id = await _insert_user("ledger-guard@example.com", "ledger-guard-google")

    assert (await resume_sync(user_id=user_id)) == {"status": "resumed"}
    assert (await sync_now(user_id=user_id)) == {"status": "enqueued"}


# ---------------------------------------------------------------------------
# 2. webcal create — None fetch keeps its own 400 detail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webcal_create_none_fetch_keeps_fetch_detail(test_db, monkeypatch):
    from app.api.webcal import CreateWebcalRequest, create_webcal_subscription

    user_id = await _insert_user("webcal-fetch@example.com", "webcal-fetch-google")

    async def fetch_none(_url):
        return None, None

    monkeypatch.setattr("app.utils.ics_fetch.fetch_ics_feed", fetch_none)
    monkeypatch.setattr("app.utils.ics_fetch.validate_url_for_ssrf", lambda _url: None)

    with pytest.raises(HTTPException) as exc:
        await create_webcal_subscription(
            CreateWebcalRequest(url="https://feeds.example.com/team.ics"),
            user=SimpleNamespace(id=user_id),
        )
    assert exc.value.status_code == 400
    # The 400 raised for a None fetch used to be swallowed by the
    # generic `except Exception` and re-labelled as a parse failure.
    assert exc.value.detail == "Could not fetch ICS feed from this URL"


# ---------------------------------------------------------------------------
# 3. connect-client / connect-personal — browser session expiry redirects
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connect_endpoints_redirect_unauthenticated_to_login(test_db):
    from app.auth.routes import connect_client, connect_personal

    for endpoint in (connect_client, connect_personal):
        response = await endpoint(_request("/auth/connect-x"))
        assert response.status_code == 302, endpoint.__name__
        assert response.headers["location"] == "/app/login"


@pytest.mark.asyncio
async def test_connect_client_non_401_http_exceptions_propagate(test_db, monkeypatch):
    """Only session-expiry 401s become redirects; other errors keep
    their API semantics (e.g. the 500 for missing OAuth creds)."""
    from app.auth.routes import connect_client

    async def fake_user(_request):
        return SimpleNamespace(id=1, email="u@example.com", is_admin=False)

    async def missing_creds():
        raise ValueError("missing")

    monkeypatch.setattr("app.auth.routes.get_current_user", fake_user)
    monkeypatch.setattr("app.auth.routes.get_oauth_credentials", missing_creds)
    with pytest.raises(HTTPException) as exc:
        await connect_client(_request("/auth/connect-client"))
    assert exc.value.status_code == 500


# ---------------------------------------------------------------------------
# 4. OAuth 'error' values are URL-encoded into redirect locations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oauth_callback_error_param_is_url_encoded(test_db):
    from app.auth.routes import oauth_callback

    response = await oauth_callback(_request("/auth/callback"), error="a&next=//evil #x")
    assert response.status_code == 302
    location = response.headers["location"]
    assert location == "/app/login?error=a%26next%3D%2F%2Fevil+%23x"


@pytest.mark.asyncio
async def test_connect_callback_error_params_are_url_encoded(test_db):
    from app.auth.routes import connect_client_callback, connect_personal_callback

    hostile = "denied&injected=1#frag"
    client_resp = await connect_client_callback(_request(), error=hostile)
    assert client_resp.headers["location"] == (
        "/app?error=client_connect_failed&reason=denied%26injected%3D1%23frag"
    )

    personal_resp = await connect_personal_callback(_request(), error=hostile)
    assert personal_resp.headers["location"] == (
        "/app?error=personal_connect_failed&reason=denied%26injected%3D1%23frag"
    )


# ---------------------------------------------------------------------------
# 5. select_calendar_page — verified token email wins over the query param
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_calendar_uses_token_email_not_query_param(test_db, monkeypatch):
    from app.ui.routes import select_calendar_page

    user_id = await _insert_user("owner@example.com", "owner-google")
    token_id = await _insert_token(user_id, "client", "verified-client@example.com")

    async def auth_user(_request):
        return SimpleNamespace(
            id=user_id,
            email="owner@example.com",
            is_admin=False,
            display_name="owner",
            main_calendar_id="main",
        )

    fetched_emails: list[str] = []

    async def fake_fetch_calendar_list(_user_id, email):
        fetched_emails.append(email)
        return [{"id": "c1", "summary": "Cal 1", "accessRole": "owner"}]

    monkeypatch.setattr("app.ui.routes.get_current_user_optional", auth_user)
    monkeypatch.setattr("app.auth.google.fetch_calendar_list", fake_fetch_calendar_list)

    response = await select_calendar_page(
        _request("/app/calendars/select"),
        token_id=token_id,
        email="attacker-other-account@example.com",  # mismatched on purpose
    )
    assert response.status_code == 200
    # The fetch and the rendered page must use the token row's verified
    # account email, not the attacker-controlled query param.
    assert fetched_emails == ["verified-client@example.com"]
    assert response.context["email"] == "verified-client@example.com"
