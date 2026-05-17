"""OOBE (Out-of-Box Experience) setup wizard routes."""

import asyncio
import json
import logging
import os
import secrets
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.config import get_settings, get_test_mode_home_allowlist
from app.database import get_database, is_oobe_completed, set_setting
from app.encryption import (
    generate_encryption_key,
    key_to_base64,
    EncryptionManager,
    init_encryption_manager,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/setup", tags=["setup"])

templates = Jinja2Templates(directory="app/ui/templates")

# Temporary storage for OOBE data.  Single-process by design; the
# wizard binds it to one browser via the _session_token below so a
# second, concurrent visitor to a not-yet-configured instance cannot
# read the in-progress state or hijack the admin account.
_oobe_data: dict = {}

_OOBE_COOKIE = "oobe_session"

# Serialises the first-request bind so two concurrent first visitors
# cannot both mint a session token.
_oobe_lock = asyncio.Lock()

# Default path for service account key inside the container
SA_KEY_PATH = "/secrets/sa-key.json"


async def _reject_if_oobe_done() -> None:
    """Refuse a setup step once the instance is already configured."""
    if await is_oobe_completed():
        raise HTTPException(status_code=400, detail="Setup already completed")


async def _ensure_oobe_session(request: Request) -> str:
    """Return the OOBE session token, minting it on the first request.

    The wizard is bound to one browser from its very first request:
    the first caller mints a random token — under a lock, so two
    concurrent first visitors cannot both bind — and every later
    request must present the matching cookie or it is refused.  This
    is what stops a second, concurrent visitor to a not-yet-configured
    instance from reading the in-progress state (the generated
    encryption key) or hijacking the admin account.
    """
    async with _oobe_lock:
        token = _oobe_data.get("_session_token")
        if token is None:
            token = secrets.token_urlsafe(32)
            _oobe_data["_session_token"] = token
            return token
    cookies = getattr(request, "cookies", None) or {}
    if cookies.get(_OOBE_COOKIE) != token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Setup is already in progress in another browser session.",
        )
    return token


def _require_oobe_session(request: Request) -> None:
    """Reject a setup request not from the browser that started the
    wizard.  Used by the step POSTs, which are never legitimately the
    first request in a real flow."""
    token = _oobe_data.get("_session_token")
    if not token:
        return
    cookies = getattr(request, "cookies", None) or {}
    if cookies.get(_OOBE_COOKIE) != token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Setup is already in progress in another browser session.",
        )


def _with_oobe_cookie(response, token: str):
    """Attach the OOBE session cookie to ``response`` and return it."""
    response.set_cookie(
        _OOBE_COOKIE, token,
        max_age=3600, httponly=True, samesite="lax",
        secure=get_settings().public_url.lower().startswith("https://"),
    )
    return response


def _first_incomplete_step() -> int:
    """The earliest setup step whose prerequisite is not yet met.

    Step 6 generates and reveals the master encryption key, so it must
    not be reachable until Google credentials (step 2), admin OAuth
    (step 3), and the email step (step 4) have all been completed.
    """
    if "client_id" not in _oobe_data:
        return 2
    if "admin_email" not in _oobe_data:
        return 3
    if "smtp_enabled" not in _oobe_data:
        return 4
    return 6


class Step2Request(BaseModel):
    """Google credentials request."""
    client_id: str
    client_secret: str


class Step4Request(BaseModel):
    """Email settings request."""
    enabled: bool = False
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    from_address: Optional[str] = None
    alert_emails: Optional[str] = None


@router.get("", response_class=HTMLResponse)
async def setup_wizard(
    request: Request,
    step: int = 1,
    error: Optional[str] = None,
    sa: Optional[str] = None,
    sa_email: Optional[str] = None,
):
    """OOBE setup wizard."""
    if await is_oobe_completed():
        return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)

    # Bind the wizard to this browser on its very first request.
    token = await _ensure_oobe_session(request)

    # Step 5 (service-account upload) was removed at the Stage-5
    # cutover; the wizard now jumps step 4 → step 6 (encryption).
    # We keep step number 6 stable so existing redirects still land
    # on the right page.
    template_map = {
        1: "setup/step1_welcome.html",
        2: "setup/step2_credentials.html",
        3: "setup/step3_admin.html",
        4: "setup/step4_email.html",
        6: "setup/step6_encryption.html",
        7: "setup/step7_complete.html",
    }
    if step == 5:
        # Anyone arriving at step 5 gets bounced to step 6.
        return _with_oobe_cookie(
            RedirectResponse(url="/setup?step=6", status_code=status.HTTP_302_FOUND),
            token,
        )

    # Step 6 generates and reveals the master encryption key — gate it
    # behind the earlier steps so a direct ?step=6 can never make the
    # wizard mint or display a key before credentials + admin OAuth.
    if step == 6:
        need = _first_incomplete_step()
        if need != 6:
            return _with_oobe_cookie(
                RedirectResponse(
                    url=f"/setup?step={need}",
                    status_code=status.HTTP_302_FOUND,
                ),
                token,
            )

    template = template_map.get(step, "setup/step1_welcome.html")

    settings = get_settings()

    context = {
        "step": step,
        "oobe_data": _oobe_data,
        "error": error,
        "test_mode": settings.test_mode,
        "allowed_home_emails": sorted(get_test_mode_home_allowlist()) if settings.test_mode else [],
        "public_url": settings.public_url.rstrip("/"),
    }

    # Step 5 (service-account) and SA context fields were removed
    # at the Stage-5 cutover; step 7 no longer renders SA banners.
    if step == 7:
        context["sa_uploaded"] = False
        context["sa_email"] = ""

    # For step 6, generate encryption key if not already done
    if step == 6 and "encryption_key" not in _oobe_data:
        key = generate_encryption_key()
        _oobe_data["encryption_key"] = key
        _oobe_data["encryption_key_b64"] = key_to_base64(key)
        context["encryption_key_b64"] = _oobe_data["encryption_key_b64"]
    elif step == 6:
        context["encryption_key_b64"] = _oobe_data.get("encryption_key_b64")

    return _with_oobe_cookie(
        templates.TemplateResponse(request, template, context=context),
        token,
    )


@router.post("/step/2")
async def setup_step_2(request: Request):
    """Handle step 2 - Google credentials."""
    await _reject_if_oobe_done()
    token = await _ensure_oobe_session(request)

    form = await request.form()
    client_id = form.get("client_id", "").strip()
    client_secret = form.get("client_secret", "").strip()

    # Validate
    settings = get_settings()
    if not client_id or not client_secret:
        return _with_oobe_cookie(templates.TemplateResponse(request, "setup/step2_credentials.html", context={
            "step": 2,
            "error": "Client ID and Client Secret are required",
            "client_id": client_id,
            "public_url": settings.public_url.rstrip("/"),
        }), token)

    if not client_id.endswith(".apps.googleusercontent.com"):
        return _with_oobe_cookie(templates.TemplateResponse(request, "setup/step2_credentials.html", context={
            "step": 2,
            "error": "Invalid Client ID format",
            "client_id": client_id,
            "public_url": settings.public_url.rstrip("/"),
        }), token)

    # Store temporarily
    _oobe_data["client_id"] = client_id
    _oobe_data["client_secret"] = client_secret

    return _with_oobe_cookie(
        RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND),
        token,
    )


@router.post("/step/2/test")
async def test_credentials(request: Request):
    """Test OAuth credentials."""
    await _reject_if_oobe_done()
    form = await request.form()
    client_id = form.get("client_id", "").strip()
    client_secret = form.get("client_secret", "").strip()

    from app.auth.google import test_oauth_credentials

    is_valid = await test_oauth_credentials(client_id, client_secret)

    return {"valid": is_valid}


@router.get("/step/3/auth")
async def step_3_auth(request: Request):
    """Initiate OAuth for admin user."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    if "client_id" not in _oobe_data:
        return RedirectResponse(url="/setup?step=2", status_code=status.HTTP_302_FOUND)

    from app.auth.google import build_auth_url, HOME_SCOPES

    settings = get_settings()
    state = secrets.token_urlsafe(32)
    _oobe_data["oauth_state"] = state

    redirect_uri = f"{settings.public_url}/setup/step/3/callback"

    auth_url = build_auth_url(
        client_id=_oobe_data["client_id"],
        redirect_uri=redirect_uri,
        scopes=HOME_SCOPES,
        state=state,
        prompt="consent select_account"
    )

    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


@router.get("/step/3/callback")
async def step_3_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    """OAuth callback for admin user."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    if error:
        return RedirectResponse(url=f"/setup?step=3&error={error}", status_code=status.HTTP_302_FOUND)

    if state != _oobe_data.get("oauth_state"):
        return RedirectResponse(url="/setup?step=3&error=invalid_state", status_code=status.HTTP_302_FOUND)

    settings = get_settings()
    redirect_uri = f"{settings.public_url}/setup/step/3/callback"

    try:
        from app.auth.google import exchange_code_for_tokens, get_user_info

        # Exchange code for tokens
        tokens = await exchange_code_for_tokens(
            code,
            redirect_uri,
            _oobe_data["client_id"],
            _oobe_data["client_secret"]
        )

        # Get user info
        user_info = await get_user_info(tokens["access_token"])

        admin_email = user_info["email"].strip().lower()
        settings = get_settings()
        if settings.test_mode:
            home_allowlist = get_test_mode_home_allowlist()
            if not home_allowlist:
                return RedirectResponse(url="/setup?step=3&error=test_mode_no_home_allowlist", status_code=status.HTTP_302_FOUND)
            if admin_email not in home_allowlist:
                return RedirectResponse(url="/setup?step=3&error=admin_not_allowed", status_code=status.HTTP_302_FOUND)

        # Store in oobe data
        _oobe_data["admin_email"] = admin_email
        _oobe_data["admin_name"] = user_info.get("name", admin_email.split("@")[0])
        _oobe_data["admin_google_id"] = user_info["id"]
        _oobe_data["admin_access_token"] = tokens["access_token"]
        _oobe_data["admin_refresh_token"] = tokens.get("refresh_token", "")
        _oobe_data["admin_token_expiry"] = tokens.get("expires_in")
        _oobe_data["domain"] = admin_email.split("@")[1]

        return RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND)

    except Exception as e:
        logger.exception(f"OAuth callback error: {e}")
        return RedirectResponse(url=f"/setup?step=3&error=oauth_failed", status_code=status.HTTP_302_FOUND)


@router.post("/step/3/confirm")
async def step_3_confirm(request: Request):
    """Confirm admin user and domain."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    if "admin_email" not in _oobe_data:
        return RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND)

    return RedirectResponse(url="/setup?step=4", status_code=status.HTTP_302_FOUND)


@router.post("/step/4")
async def setup_step_4(request: Request):
    """Handle step 4 - Email settings."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    if "admin_email" not in _oobe_data:
        return RedirectResponse(url="/setup?step=3", status_code=status.HTTP_302_FOUND)
    form = await request.form()
    enabled = form.get("enabled") == "on"

    if enabled:
        _oobe_data["smtp_enabled"] = True
        _oobe_data["smtp_host"] = form.get("smtp_host", "").strip()
        _oobe_data["smtp_port"] = int(form.get("smtp_port", "587"))
        _oobe_data["smtp_username"] = form.get("smtp_username", "").strip()
        _oobe_data["smtp_password"] = form.get("smtp_password", "").strip()
        _oobe_data["smtp_from_address"] = form.get("from_address", "").strip()
        _oobe_data["alert_emails"] = form.get("alert_emails", "").strip()
    else:
        _oobe_data["smtp_enabled"] = False

    return RedirectResponse(url="/setup?step=5", status_code=status.HTTP_302_FOUND)


@router.post("/step/4/test")
async def test_email(request: Request):
    """Send test email."""
    await _reject_if_oobe_done()
    form = await request.form()

    # This would actually test the email settings
    # For now, return success
    return {"success": True}


@router.post("/step/5")
async def setup_step_5(request: Request):
    """Service-account upload step was removed at the Stage-5
    cutover; this handler just redirects to step 6 so existing
    bookmarks / old form submits don't 404."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    return RedirectResponse(url="/setup?step=6", status_code=status.HTTP_302_FOUND)


@router.post("/step/5/skip")
@router.post("/step/5/continue")
async def setup_step_5_bypass(request: Request):
    """Legacy handlers — service-account upload step is gone."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    return RedirectResponse(url="/setup?step=6", status_code=status.HTTP_302_FOUND)


@router.post("/step/6")
async def setup_step_6(request: Request):
    """Complete setup and save everything."""
    await _reject_if_oobe_done()
    _require_oobe_session(request)
    # Never write a key or create the org/admin unless every prior
    # step is complete — a direct POST /step/6 from a fresh browser
    # must not be able to mint a key or seed an admin account.
    if _first_incomplete_step() != 6:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Setup steps are incomplete.",
        )
    form = await request.form()
    confirmed = form.get("confirmed") == "on"

    if not confirmed:
        return templates.TemplateResponse(request, "setup/step6_encryption.html", context={
            "step": 6,
            "error": "You must confirm that you have saved the encryption key",
            "encryption_key_b64": _oobe_data.get("encryption_key_b64"),
        })

    # Save encryption key to file
    settings = get_settings()
    key = _oobe_data.get("encryption_key")

    if not key:
        key = generate_encryption_key()
        _oobe_data["encryption_key"] = key

    # Ensure the key directory exists, owner-only.
    key_dir = os.path.dirname(settings.encryption_key_file)
    if key_dir and not os.path.exists(key_dir):
        os.makedirs(key_dir, mode=0o700, exist_ok=True)

    # Write the master key 0600 (owner read/write only).  This key
    # protects every OAuth token and derives the session secret.
    # os.open with the mode set avoids the brief world-readable window
    # a plain open() would leave; the explicit chmod also tightens the
    # file if it somehow already existed with looser permissions.
    fd = os.open(
        settings.encryption_key_file,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    with os.fdopen(fd, "wb") as f:
        f.write(key)
    os.chmod(settings.encryption_key_file, 0o600)

    # Initialize encryption manager
    enc = init_encryption_manager(key)

    # Save everything to database.  The organization row, the admin
    # user, and the admin OAuth token are written as ONE transaction:
    # on the autocommit connection the organization INSERT would
    # otherwise commit on its own, and a crash before the users INSERT
    # would leave an org with no admin — is_oobe_completed() then
    # reports setup complete and the instance is permanently locked
    # with no way to log in.
    db = await get_database()

    from datetime import datetime, timedelta
    token_expiry = None
    if _oobe_data.get("admin_token_expiry"):
        expires_in_seconds = int(_oobe_data["admin_token_expiry"])
        token_expiry = (
            datetime.utcnow() + timedelta(seconds=expires_in_seconds)
        ).isoformat()

    await db.execute("BEGIN IMMEDIATE")
    try:
        # Create organization
        await db.execute(
            """INSERT INTO organization
               (google_workspace_domain, google_client_id_encrypted, google_client_secret_encrypted)
               VALUES (?, ?, ?)""",
            (
                _oobe_data["domain"],
                enc.encrypt(_oobe_data["client_id"]),
                enc.encrypt(_oobe_data["client_secret"]),
            )
        )

        # Create admin user
        cursor = await db.execute(
            """INSERT INTO users (email, google_user_id, display_name, is_admin)
               VALUES (?, ?, ?, TRUE)
               RETURNING id""",
            (_oobe_data["admin_email"], _oobe_data["admin_google_id"], _oobe_data["admin_name"])
        )
        user_row = await cursor.fetchone()
        user_id = user_row["id"]

        # Store admin's OAuth tokens with expiry
        await db.execute(
            """INSERT INTO oauth_tokens
               (user_id, account_type, google_account_email,
                access_token_encrypted, refresh_token_encrypted, token_expiry)
               VALUES (?, 'home', ?, ?, ?, ?)""",
            (
                user_id,
                _oobe_data["admin_email"],
                enc.encrypt(_oobe_data["admin_access_token"]),
                enc.encrypt(_oobe_data["admin_refresh_token"]),
                token_expiry,
            )
        )
        await db.execute("COMMIT")
    except BaseException:
        await db.execute("ROLLBACK")
        raise

    # SMTP settings are non-critical and written separately — if the
    # process dies here the instance is already usable and the admin
    # can configure email from the dashboard.
    if _oobe_data.get("smtp_enabled"):
        await set_setting("smtp_host", _oobe_data.get("smtp_host", ""))
        await set_setting("smtp_port", str(_oobe_data.get("smtp_port", 587)))
        await set_setting("smtp_username", _oobe_data.get("smtp_username", ""))
        if _oobe_data.get("smtp_password"):
            await set_setting("smtp_password", _oobe_data["smtp_password"], is_sensitive=True, encrypt_func=enc.encrypt)
        await set_setting("smtp_from_address", _oobe_data.get("smtp_from_address", ""))
        await set_setting("alert_emails", _oobe_data.get("alert_emails", ""))
        await set_setting("alerts_enabled", "true")
    else:
        await set_setting("alerts_enabled", "false")

    # Service-account activation block was removed at the
    # Stage-5 cutover (REWRITE_PLAN.md §1).

    # Clear OOBE data
    _oobe_data.clear()

    logger.info("OOBE setup completed successfully")

    return RedirectResponse(
        url="/setup?step=7", status_code=status.HTTP_302_FOUND,
    )


@router.get("/complete")
async def setup_complete(request: Request):
    """Final step redirect to dashboard."""
    return RedirectResponse(url="/app", status_code=status.HTTP_302_FOUND)
