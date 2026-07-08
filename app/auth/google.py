"""Google OAuth helpers."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlencode

import httpx
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from app.config import get_settings
from app.database import get_database
from app.encryption import encrypt_value, decrypt_value

logger = logging.getLogger(__name__)

# Google OAuth endpoints
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

# Required scopes
HOME_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar",
]

CLIENT_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar",
]

PERSONAL_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar.readonly",
]


async def get_oauth_credentials() -> tuple[str, str]:
    """Get OAuth credentials from database."""
    from app.encryption import get_encryption_manager

    db = await get_database()
    cursor = await db.execute("SELECT * FROM organization LIMIT 1")
    org = await cursor.fetchone()

    if not org:
        raise ValueError("Organization not configured")

    enc = get_encryption_manager()
    client_id = enc.decrypt(org["google_client_id_encrypted"])
    client_secret = enc.decrypt(org["google_client_secret_encrypted"])

    return client_id, client_secret


def build_auth_url(
    client_id: str,
    redirect_uri: str,
    scopes: list[str],
    state: str,
    login_hint: Optional[str] = None,
    prompt: str = "consent"
) -> str:
    """Build Google OAuth authorization URL."""
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes),
        "access_type": "offline",
        "state": state,
        "prompt": prompt,
    }

    if login_hint:
        params["login_hint"] = login_hint

    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


async def exchange_code_for_tokens(
    code: str,
    redirect_uri: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None
) -> dict:
    """Exchange authorization code for tokens."""
    if not client_id or not client_secret:
        client_id, client_secret = await get_oauth_credentials()

    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )

        if response.status_code != 200:
            logger.error(f"Token exchange failed: {response.text}")
            raise ValueError(f"Token exchange failed: {response.text}")

        return response.json()


async def refresh_access_token(
    refresh_token: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None
) -> dict:
    """Refresh an access token."""
    if not client_id or not client_secret:
        client_id, client_secret = await get_oauth_credentials()

    async with httpx.AsyncClient() as client:
        response = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
            },
        )

        if response.status_code != 200:
            logger.error(f"Token refresh failed: {response.text}")
            raise ValueError(f"Token refresh failed: {response.text}")

        return response.json()


async def get_user_info(access_token: str) -> dict:
    """Get user info from Google."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )

        if response.status_code != 200:
            logger.error(f"Failed to get user info: {response.text}")
            raise ValueError(f"Failed to get user info: {response.text}")

        return response.json()


async def store_oauth_tokens(
    user_id: int,
    account_type: str,
    email: str,
    access_token: str,
    refresh_token: str,
    expires_in: Optional[int] = None
) -> int:
    """Store OAuth tokens in database."""
    db = await get_database()
    now = datetime.utcnow()

    expiry = None
    if expires_in:
        expiry = (now + timedelta(seconds=expires_in)).isoformat()

    access_encrypted = encrypt_value(access_token)
    refresh_encrypted = encrypt_value(refresh_token)

    cursor = await db.execute(
        """INSERT INTO oauth_tokens
           (user_id, account_type, google_account_email,
            access_token_encrypted, refresh_token_encrypted, token_expiry, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(user_id, google_account_email) DO UPDATE SET
           account_type = excluded.account_type,
           access_token_encrypted = excluded.access_token_encrypted,
           refresh_token_encrypted = excluded.refresh_token_encrypted,
           token_expiry = excluded.token_expiry,
           updated_at = excluded.updated_at
           RETURNING id""",
        (user_id, account_type, email, access_encrypted, refresh_encrypted, expiry, now.isoformat())
    )
    row = await cursor.fetchone()
    await db.commit()

    return row["id"]


async def get_oauth_token(user_id: int, email: str) -> Optional[dict]:
    """Get OAuth token for a user's account."""
    db = await get_database()
    cursor = await db.execute(
        """SELECT * FROM oauth_tokens
           WHERE user_id = ? AND google_account_email = ?""",
        (user_id, email)
    )
    row = await cursor.fetchone()
    if row:
        return dict(row)
    return None


def _parse_expiry_naive_utc(value: str) -> datetime:
    """Parse a stored ``token_expiry`` into a naive-UTC datetime.

    Stored values are normally naive UTC, but an old, imported, or
    future tz-aware row would otherwise raise ``TypeError`` when
    compared against ``datetime.utcnow()``.  A tz-aware value is
    folded to naive UTC so every comparison is safe.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


async def get_valid_access_token(user_id: int, email: str) -> str:
    """Get a valid access token, refreshing if needed."""
    token_data = await get_oauth_token(user_id, email)
    if not token_data:
        raise ValueError(f"No token found for user {user_id}, email {email}")

    access_token = decrypt_value(token_data["access_token_encrypted"])
    refresh_token = decrypt_value(token_data["refresh_token_encrypted"])

    # Check if token is expired or will expire soon
    expiry = token_data.get("token_expiry")
    if expiry:
        expiry_dt = _parse_expiry_naive_utc(expiry)
        if datetime.utcnow() >= expiry_dt - timedelta(minutes=5):
            # Token expired or expiring soon, refresh it
            logger.info(f"Refreshing token for user {user_id}, email {email}")

            # Retry logic for transient network errors
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    new_tokens = await refresh_access_token(refresh_token)
                    access_token = new_tokens["access_token"]

                    # Store the new tokens
                    new_refresh = new_tokens.get("refresh_token", refresh_token)
                    await store_oauth_tokens(
                        user_id=user_id,
                        account_type=token_data["account_type"],
                        email=email,
                        access_token=access_token,
                        refresh_token=new_refresh,
                        expires_in=new_tokens.get("expires_in")
                    )
                    break  # Success, exit retry loop
                except Exception as e:
                    # Permanent errors (invalid_grant, etc.) - don't retry
                    if "invalid_grant" in str(e).lower():
                        logger.error(f"Token refresh failed with permanent error: {e}")
                        raise
                    # Network/transient errors - retry
                    if attempt < max_retries - 1:
                        import asyncio
                        wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.warning(f"Token refresh attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Failed to refresh token after {max_retries} attempts: {e}")
                        raise

    return access_token


async def build_user_credentials(user_id: int, email: str) -> Credentials:
    """Build a *refresh-capable* Credentials for a user's account.

    :func:`get_valid_access_token` refreshes proactively (inside the
    5-minute pre-expiry window) and persists, but a single reconcile +
    outbox-drain pass can outlive even a freshly minted access token
    (~1h).  A ``Credentials(token=...)`` with nothing else cannot
    refresh itself, so googleapiclient would simply 401 mid-pass.

    Handing googleapiclient a Credentials that also carries the
    refresh token, token URI, and OAuth client config lets it
    transparently re-mint the access token when it expires, so a long
    pass survives a token expiry instead of failing every remaining
    op.  (That in-memory refresh is not persisted; the next pass
    re-mints via ``get_valid_access_token`` as usual.)
    """
    access_token = await get_valid_access_token(user_id, email)
    token_data = await get_oauth_token(user_id, email)
    if not token_data:
        raise ValueError(f"No token found for user {user_id}, email {email}")
    refresh_token = decrypt_value(token_data["refresh_token_encrypted"])
    client_id, client_secret = await get_oauth_credentials()

    credentials = Credentials(
        token=access_token,
        refresh_token=refresh_token,
        token_uri=GOOGLE_TOKEN_URL,
        client_id=client_id,
        client_secret=client_secret,
        # scopes is deliberately omitted: this builder serves home,
        # client, and personal accounts alike, which hold different
        # scope sets.  A refresh does not need the scope list — Google
        # reports the actual granted scopes back — and hard-coding
        # HOME_SCOPES here would be misleading for the others.
    )
    # google-auth compares expiry against a naive UTC now(); the token
    # store writes a naive-UTC isoformat, so parsing yields the right
    # shape.  A tz-aware value (older rows / future changes) is folded
    # back to naive UTC so the comparison cannot raise.
    raw_expiry = token_data.get("token_expiry")
    if raw_expiry:
        try:
            credentials.expiry = _parse_expiry_naive_utc(raw_expiry)
        except ValueError:
            pass
    return credentials


# Socket timeout for every Google HTTP call.  googleapiclient is
# synchronous; without this a hung connection blocks indefinitely.
GOOGLE_HTTP_TIMEOUT = 30


def build_calendar_service(
    credentials: Credentials, timeout: int = GOOGLE_HTTP_TIMEOUT,
):
    """Build a Calendar API service whose HTTP layer carries a socket
    timeout, so a hung Google connection fails after ``timeout``
    seconds instead of hanging forever.

    Every Google Calendar call in the app should go through a service
    built here (directly, or via :class:`RealGoogleClient`, the
    legacy ``GoogleCalendarClient``, or the ``fetch_*`` helpers below)
    so they all share the timeout."""
    import httplib2
    from google_auth_httplib2 import AuthorizedHttp

    authed_http = AuthorizedHttp(
        credentials,
        http=httplib2.Http(timeout=timeout),
    )
    return build(
        "calendar", "v3", http=authed_http, cache_discovery=False,
    )


def get_calendar_service(credentials: Credentials):
    """Build Google Calendar API service (with the socket timeout)."""
    return build_calendar_service(credentials)


async def get_calendar_service_for_user(user_id: int, email: str):
    """Get Calendar service for a user's account."""
    credentials = await build_user_credentials(user_id, email)
    return build_calendar_service(credentials)


async def fetch_calendar_list(user_id: int, email: str) -> list[dict]:
    """List a Google account's calendars.

    Uses refresh-capable credentials and a timeout-bounded service,
    and runs the blocking call off the event loop with
    :func:`asyncio.to_thread` — safe to call from an async route.
    Executes through the shared retry + rate-limit wrapper so these
    calls honour the same global quota and transient-error handling
    as every other Google call in the app.
    """
    # Late import: app/sync/google_calendar.py imports helpers from
    # this module, so keep the dependency one-way at import time.
    from app.sync.google_calendar import execute_with_retry

    service = await get_calendar_service_for_user(user_id, email)
    result = await asyncio.to_thread(
        execute_with_retry, service.calendarList().list()
    )
    return result.get("items", [])


async def fetch_calendar(
    user_id: int, email: str, calendar_id: str,
) -> dict:
    """Fetch one calendar's metadata; raises if it is inaccessible.

    Same guarantees as :func:`fetch_calendar_list` — timeout-bounded,
    offloaded off the event loop, and executed through the shared
    retry + rate-limit wrapper."""
    from app.sync.google_calendar import execute_with_retry

    service = await get_calendar_service_for_user(user_id, email)
    return await asyncio.to_thread(
        execute_with_retry, service.calendars().get(calendarId=calendar_id)
    )


async def test_oauth_credentials(client_id: str, client_secret: str) -> bool:
    """Test if OAuth credentials are valid by making a simple API call."""
    # We can't fully validate credentials without a token exchange
    # But we can at least check they're in the right format
    if not client_id or not client_secret:
        return False

    if not client_id.endswith(".apps.googleusercontent.com"):
        return False

    if len(client_secret) < 10:
        return False

    return True
