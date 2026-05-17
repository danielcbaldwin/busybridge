"""Additional branch coverage tests for ui.setup."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.database import get_database


def _oobe_cookie_headers() -> list:
    """Carry the active OOBE session token so setup requests pass the
    browser-binding guard the wizard now enforces from request one."""
    import app.ui.setup as _setup
    token = _setup._oobe_data.get("_session_token")
    if not token:
        return []
    return [(b"cookie", f"{_setup._OOBE_COOKIE}={token}".encode())]


def _request(path: str = "/setup") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": _oobe_cookie_headers(),
    }
    return Request(scope)


class FakeFormRequest:
    """Minimal request object with async form() for route unit tests."""

    def __init__(self, form_data: dict):
        self._form_data = form_data
        self.url = SimpleNamespace(path="/setup")

    @property
    def cookies(self) -> dict:
        import app.ui.setup as _setup
        token = _setup._oobe_data.get("_session_token")
        return {_setup._OOBE_COOKIE: token} if token else {}

    async def form(self):
        return self._form_data


@pytest.mark.asyncio
async def test_setup_step6_existing_key_context_and_step2_completed_guard(test_db, monkeypatch):
    """Setup wizard should reuse existing key context and step 2 should reject completed setup.

    Step 5 (service-account upload) was removed at the Stage-5
    cutover.  The encryption-key step is now step 6, and any
    request for step 5 redirects there."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_2, setup_wizard

    setup_module._oobe_data.clear()
    # Step 6 only renders once the earlier steps are complete.
    setup_module._oobe_data.update({
        "encryption_key": b"2" * 32,
        "encryption_key_b64": "existing-key-b64",
        "client_id": "good.apps.googleusercontent.com",
        "client_secret": "secret",
        "admin_email": "admin@example.com",
        "smtp_enabled": False,
    })

    async def oobe_incomplete():
        return False

    monkeypatch.setattr("app.ui.setup.is_oobe_completed", oobe_incomplete)
    # step=5 redirects to step=6 now.
    redirect = await setup_wizard(_request("/setup"), step=5)
    assert redirect.status_code == 302
    assert redirect.headers["location"] == "/setup?step=6"
    rendered = await setup_wizard(_request("/setup"), step=6)
    assert rendered.status_code == 200
    assert rendered.context["encryption_key_b64"] == "existing-key-b64"

    async def oobe_complete():
        return True

    monkeypatch.setattr("app.ui.setup.is_oobe_completed", oobe_complete)
    with pytest.raises(HTTPException) as exc:
        await setup_step_2(
            FakeFormRequest(
                {
                    "client_id": "good.apps.googleusercontent.com",
                    "client_secret": "secret",
                }
            )
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_setup_step5_is_a_redirect_post_cutover():
    """Step 5 (service-account upload) was removed; the POST handler
    is a thin redirect to step 6."""
    from app.ui.setup import setup_step_5
    response = await setup_step_5(FakeFormRequest({}))
    assert response.status_code == 302
    assert response.headers["location"] == "/setup?step=6"


@pytest.mark.asyncio
async def test_setup_step6_completes_setup_and_disables_alerts(test_db, monkeypatch, tmp_path):
    """Step 6 (final completion) generates the encryption key when
    missing, creates the key directory, and disables alerts when
    SMTP was not configured."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_6

    setup_module._oobe_data.clear()
    setup_module._oobe_data.update(
        {
            "client_id": "good.apps.googleusercontent.com",
            "client_secret": "secret",
            "domain": "example.com",
            "admin_email": "admin2@example.com",
            "admin_google_id": "g-admin-2",
            "admin_name": "Admin Two",
            "admin_access_token": "access",
            "admin_refresh_token": "refresh",
            "admin_token_expiry": 1200,
            "smtp_enabled": False,
        }
    )

    nested_dir = tmp_path / "nested" / "keys"
    key_path = nested_dir / "enc.key"
    monkeypatch.setattr(
        "app.ui.setup.get_settings",
        lambda: SimpleNamespace(encryption_key_file=str(key_path)),
    )
    monkeypatch.setattr("app.ui.setup.generate_encryption_key", lambda: b"1" * 32)

    response = await setup_step_6(FakeFormRequest({"confirmed": "on"}))
    assert response.status_code == 302
    assert response.headers["location"].startswith("/setup?step=7")
    assert key_path.exists()
    assert setup_module._oobe_data == {}

    db = await get_database()
    cursor = await db.execute(
        "SELECT value_plain FROM settings WHERE key = 'alerts_enabled'",
    )
    row = await cursor.fetchone()
    assert row["value_plain"] == "false"


@pytest.mark.asyncio
async def test_setup_step3_callback_enforces_test_mode_home_allowlist(test_db, monkeypatch):
    """Step-3 callback should enforce TEST_MODE home-account allowlist."""
    from app.ui import setup as setup_module
    from app.ui.setup import step_3_callback

    setup_module._oobe_data.clear()
    setup_module._oobe_data.update(
        {
            "client_id": "good.apps.googleusercontent.com",
            "client_secret": "secret",
            "oauth_state": "state-123",
        }
    )

    monkeypatch.setattr(
        "app.ui.setup.get_settings",
        lambda: SimpleNamespace(public_url="http://localhost:3000", test_mode=True),
    )

    async def fake_exchange(_code, _redirect_uri, _client_id, _client_secret):
        return {"access_token": "a", "refresh_token": "r", "expires_in": 300}

    async def fake_user_info_blocked(_access):
        return {"email": "blocked@gmail.com", "name": "Blocked", "id": "g-blocked"}

    monkeypatch.setattr("app.auth.google.exchange_code_for_tokens", fake_exchange)
    monkeypatch.setattr("app.auth.google.get_user_info", fake_user_info_blocked)

    monkeypatch.setattr("app.ui.setup.get_test_mode_home_allowlist", lambda: set())
    missing_allowlist = await step_3_callback(
        _request("/setup/step/3/callback"),
        code="auth-code",
        state="state-123",
    )
    assert "test_mode_no_home_allowlist" in missing_allowlist.headers["location"]

    monkeypatch.setattr(
        "app.ui.setup.get_test_mode_home_allowlist",
        lambda: {"allowed@gmail.com"},
    )
    blocked = await step_3_callback(
        _request("/setup/step/3/callback"),
        code="auth-code",
        state="state-123",
    )
    assert "admin_not_allowed" in blocked.headers["location"]

    async def fake_user_info_allowed(_access):
        return {"email": "allowed@gmail.com", "name": "Allowed", "id": "g-allowed"}

    monkeypatch.setattr("app.auth.google.get_user_info", fake_user_info_allowed)
    allowed = await step_3_callback(
        _request("/setup/step/3/callback"),
        code="auth-code",
        state="state-123",
    )
    assert allowed.status_code == 302
    assert allowed.headers["location"] == "/setup?step=3"
    assert setup_module._oobe_data["admin_email"] == "allowed@gmail.com"
