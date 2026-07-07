"""End-to-end OOBE (out-of-box experience) verification.

Drives the setup wizard from a completely empty database through
to a working post-cutover deployment.  This is the test that
proves the step-5-removed wizard actually
boots cold — the rewrite changed the wizard's step numbering and
removed the service-account step, so the whole flow needs an
end-to-end check, not just per-step unit tests.

Flow exercised: 1 (welcome) → 2 (Google creds) → 3 (admin OAuth)
→ 4 (email) → 6 (encryption + commit) → 7 (complete).  Step 5
(service-account upload) was removed and now redirects to 6.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.database import get_database, is_oobe_completed


def _oobe_cookies() -> dict:
    """The OOBE session cookie for the browser that started the wizard.

    setup_step_2 binds the flow to a random token; every later setup
    route demands the matching cookie, so the test requests must carry
    it once step 2 has run.
    """
    from app.ui import setup as _setup
    token = _setup._oobe_data.get("_session_token")
    return {_setup._OOBE_COOKIE: token} if token else {}


def _form_request(fields: dict):
    """A minimal Request stand-in whose .form() yields ``fields`` and
    whose cookies carry the active OOBE session token."""
    cookies = _oobe_cookies()

    class _Req:
        async def form(self):
            return fields
    req = _Req()
    req.cookies = cookies
    return req


def _get_request(path: str = "/setup"):
    from starlette.requests import Request
    headers = []
    for name, value in _oobe_cookies().items():
        headers.append((b"cookie", f"{name}={value}".encode()))
    return Request({
        "type": "http", "method": "GET", "path": path, "headers": headers,
        "query_string": b"",
    })


@pytest.mark.asyncio
async def test_oobe_completes_from_empty_database(test_db, tmp_path, monkeypatch):
    """Walk the full wizard.  Asserts that after step 6 the org +
    admin user + OAuth token exist, the encryption key is written,
    and is_oobe_completed() flips to True."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_2, setup_step_4, setup_step_6, setup_wizard

    setup_module._oobe_data.clear()

    # Fresh DB → OOBE not complete.
    assert await is_oobe_completed() is False

    # The setup wizard should render step 1 when OOBE is incomplete.
    page1 = await setup_wizard(_get_request("/setup"), step=1)
    assert page1.status_code == 200

    # --- Step 2: Google OAuth credentials -----------------------------
    r2 = await setup_step_2(_form_request({
        "client_id": "1234567890-abc.apps.googleusercontent.com",
        "client_secret": "GOCSPX-test-secret-value",
    }))
    assert r2.status_code == 302
    assert r2.headers["location"] == "/setup?step=3"
    assert setup_module._oobe_data["client_id"].endswith(
        ".apps.googleusercontent.com"
    )

    # --- Step 3: admin OAuth callback (simulated) ---------------------
    # In production step 3 is a Google redirect; here we populate
    # the OOBE data the way step_3_callback would have.
    setup_module._oobe_data.update({
        "domain": "example.com",
        "admin_email": "admin@example.com",
        "admin_google_id": "g-admin-001",
        "admin_name": "Org Admin",
        "admin_access_token": "ya29.fake-access-token",
        "admin_refresh_token": "1//fake-refresh-token",
        "admin_token_expiry": 3600,
    })

    # --- Step 4: email / SMTP (disabled) ------------------------------
    r4 = await setup_step_4(_form_request({"enabled": ""}))
    assert r4.status_code == 302
    assert setup_module._oobe_data["smtp_enabled"] is False

    # --- Step 5 is gone: requesting it redirects to step 6 ------------
    r5 = await setup_wizard(_get_request("/setup"), step=5)
    assert r5.status_code == 302
    assert r5.headers["location"] == "/setup?step=6"

    # --- Step 6 render: must generate + expose an encryption key ------
    key_path = tmp_path / "secrets" / "encryption.key"
    _fake_settings = SimpleNamespace(
        encryption_key_file=str(key_path),
        test_mode=False,
        public_url="http://localhost:3000",
    )
    monkeypatch.setattr(
        "app.ui.setup.get_settings", lambda: _fake_settings,
    )
    page6 = await setup_wizard(_get_request("/setup"), step=6)
    assert page6.status_code == 200
    assert "encryption_key_b64" in setup_module._oobe_data

    # --- Step 6 commit ------------------------------------------------
    # Capture the wizard's session token first: the commit clears
    # _oobe_data, and the step-7 page is only served to the browser
    # carrying this token.
    completing_cookie = setup_module._oobe_data["_session_token"]
    r6 = await setup_step_6(_form_request({"confirmed": "on"}))
    assert r6.status_code == 302
    assert r6.headers["location"].startswith("/setup?step=7")

    # --- Step 7: the redirect target must actually render --------------
    # is_oobe_completed() is True now, but the completed-check exempts
    # step 7 for the browser that just committed.
    from starlette.requests import Request
    page7 = await setup_wizard(
        Request({
            "type": "http", "method": "GET", "path": "/setup",
            "headers": [(
                b"cookie",
                f"{setup_module._OOBE_COOKIE}={completing_cookie}".encode(),
            )],
            "query_string": b"step=7",
        }),
        step=7,
    )
    assert page7.status_code == 200
    assert b"Setup Complete" in page7.body

    # Any other browser (no cookie) still bounces to the app.
    other = await setup_wizard(_get_request("/setup"), step=7)
    assert other.status_code == 302
    assert other.headers["location"] == "/app"

    # --- Post-conditions ---------------------------------------------
    # Encryption key file written, owner-only (0600).
    assert key_path.exists()
    assert len(key_path.read_bytes()) == 32
    import stat
    assert stat.S_IMODE(key_path.stat().st_mode) == 0o600

    # OOBE data cleared.
    assert setup_module._oobe_data == {}

    # is_oobe_completed() now True.
    assert await is_oobe_completed() is True

    db = await get_database()
    # Organization row exists with the right domain.
    org = await (await db.execute(
        "SELECT google_workspace_domain FROM organization",
    )).fetchone()
    assert org["google_workspace_domain"] == "example.com"

    # Admin user exists, flagged is_admin.
    admin = await (await db.execute(
        "SELECT id, email, is_admin FROM users WHERE email = 'admin@example.com'",
    )).fetchone()
    assert admin is not None
    assert bool(admin["is_admin"]) is True

    # Home OAuth token stored for the admin.
    tok = await (await db.execute(
        """SELECT account_type, google_account_email FROM oauth_tokens
            WHERE user_id = ?""",
        (admin["id"],),
    )).fetchone()
    assert tok["account_type"] == "home"
    assert tok["google_account_email"] == "admin@example.com"

    # The new ledger tables exist (init_schema ran).
    for table in ("ledger_events", "ledger_projections",
                  "outbox_operations", "reconcile_requests"):
        row = await (await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )).fetchone()
        assert row is not None, f"{table} missing after OOBE"

    # Legacy tables NOT present (cutover migration ran).
    for table in ("event_mappings", "busy_blocks"):
        row = await (await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )).fetchone()
        assert row is None, f"legacy {table} should not exist post-cutover"


@pytest.mark.asyncio
async def test_oobe_step6_rejects_unconfirmed(test_db, tmp_path, monkeypatch):
    """Step 6 must refuse to commit if the operator hasn't ticked
    the 'I saved the encryption key' box."""
    from app.ui import setup as setup_module
    from app.ui.setup import setup_step_6

    setup_module._oobe_data.clear()
    setup_module._oobe_data.update({
        "domain": "example.com",
        "admin_email": "admin@example.com",
        "admin_google_id": "g-admin",
        "admin_name": "Admin",
        "admin_access_token": "a",
        "admin_refresh_token": "r",
        "admin_token_expiry": 3600,
        "client_id": "x.apps.googleusercontent.com",
        "client_secret": "s",
        "encryption_key_b64": "abc",
        "smtp_enabled": False,
    })
    monkeypatch.setattr(
        "app.ui.setup.get_settings",
        lambda: SimpleNamespace(encryption_key_file=str(tmp_path / "k")),
    )
    r = await setup_step_6(_form_request({"confirmed": ""}))
    assert r.status_code == 200  # re-renders with an error
    assert "must confirm" in r.context["error"].lower()
    # Nothing was committed.
    assert await is_oobe_completed() is False
