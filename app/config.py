"""Application configuration management."""

import os
import re
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


# Temporary OOBE session secret (generated once per process)
_oobe_session_secret: Optional[str] = None
# Cached resolved session secret (env var or persisted file).
_session_secret_cache: Optional[str] = None


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_path: str = "/data/calendar-sync.db"

    # Encryption
    encryption_key_file: str = "/secrets/encryption.key"

    # Server
    public_url: str = "http://localhost:3000"
    log_level: str = "info"
    log_dir: str = "/data/logs"

    # Session
    session_secret_key: Optional[str] = None  # If unset, an independent random secret is generated and persisted
    session_expire_days: int = 7

    # Runtime features
    enable_webhooks: bool = True
    # The ledger pipeline is the only sync path post-cutover; the
    # default is True so out-of-the-box deployments actually sync.
    # Setting this False stops the scheduler's drain job — useful
    # only as a temporary rollback switch during incident response.
    enable_ledger_jobs: bool = True
    # Dry-run / shadow mode.  When True, the reconciler still
    # ingests from Google and computes the full plan + outbox, but
    # the outbox is NEVER drained — nothing is written back to
    # Google.  Pending outbox rows are the preview of what WOULD
    # be written.  Used for the staging-validation window: point at
    # real Google, watch what the new system would do, without
    # touching anything.
    ledger_dry_run: bool = False
    # Phase-1 organizer-delete propagation (DELETE_PROPAGATION_PLAN.md).
    #   "off"    — never propagate a delete to the source (current safe default).
    #   "shadow" — log what WOULD be source-deleted, but never delete.
    #   "on"     — when the user deletes our managed copy of a NON-recurring
    #              CLIENT event they can edit on main, delete it on the source
    #              calendar too.
    # Recurring / per-occurrence ("_R") delete propagation is NOT covered here;
    # it stays disarmed pending the Layer-1/2 work in the plan.
    delete_propagation_mode: str = "off"

    # All-day personal events.  Default (False): all-day events from a
    # personal (read-only) calendar are NOT mirrored anywhere — they
    # only block out the entire day on main and on every client calendar
    # without conveying any information, making the user look unavailable
    # all day.  Timed personal events still cast their opaque busy blocks
    # as before.  Set True to restore the legacy behavior (all-day
    # personal events cast full-day "Personal" busy blocks on main +
    # clients).
    sync_personal_all_day_events: bool = False

    # Test mode controls
    test_mode: bool = False
    test_mode_allowed_home_emails: str = ""
    test_mode_allowed_client_emails: str = ""

    # Rate limiting
    rate_limit_per_minute: int = 60
    webhook_rate_limit_per_minute: int = 30
    auth_rate_limit_per_minute: int = 10
    # Sustained cap (calls/sec) on OUTBOUND Google Calendar API calls,
    # applied across the whole process so a bulk drain or full-sync
    # ingest is paced under Google's quota instead of triggering
    # rateLimitExceeded 403 storms.  Google's documented limits are
    # 600 requests/min per user (=10/s, sliding window) and 10,000/min
    # per project.  5/s = 300/min is half the per-user ceiling — quiet
    # (no 403 noise) with ample margin even if all traffic lands on one
    # account.  Recovery is pass-cadence bound, not rate bound, so a
    # gentler rate does not slow it.  <= 0 disables.
    google_api_rate_limit_per_second: float = 5.0
    # Whether to trust X-Real-IP / X-Forwarded-For for the client IP.
    # Only enable when the app sits behind a reverse proxy that
    # overwrites these headers; otherwise a client can spoof them to
    # dodge per-IP rate limits.  Off by default (safe for a direct
    # deployment).
    trust_proxy_headers: bool = False

    # Sync settings
    sync_interval_minutes: int = 5
    webhook_renewal_hours: int = 6
    consistency_check_hours: int = 1
    # How often the content-audit job runs (re-verifies ingested source
    # content against Google to catch drift incremental sync can't see —
    # the create-then-rename race).  Cheap (a few list calls/run), so it
    # runs every 10 minutes by default.
    content_audit_minutes: int = 10
    token_refresh_minutes: int = 30
    alert_process_minutes: int = 1

    # Retention settings (days)
    event_retention_days: int = 30
    recurring_soft_delete_days: int = 30
    audit_log_retention_days: int = 90
    disconnected_calendar_retention_days: int = 30

    # Expired one-off events.  When True (default), a non-recurring event
    # whose end is past ``event_retention_days`` is RELEASED rather than
    # deleted: its copies are left frozen on main + every client calendar
    # and it is retired from sync (planner, diff, and ingest all skip a
    # ``released`` row), so old calendar history is preserved instead of
    # being erased at the retention window.  Set False to restore the
    # legacy behavior (cancel the event and delete its managed copies once
    # past the window).  Genuine user cancellations are deleted in either
    # mode — only age-based expiry is affected.
    release_expired_events: bool = True

    # Service account
    # service_account_key_file was removed at the cutover.  Kept as a
    # no-op field on Settings only if the env var is set, since
    # pydantic-settings rejects unknown env vars at parse time.
    service_account_key_file: Optional[str] = None  # unused; retained for env-var back-compat

    # Google Calendar
    calendar_sync_tag: str = "calendarSyncEngine"
    managed_event_prefix: str = "[BusyBridge]"
    busy_block_title: str = "Busy"
    personal_busy_block_title: str = "Personal"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def _parse_email_allowlist(raw: Optional[str]) -> set[str]:
    """Parse comma/newline/semicolon separated email allowlist values."""
    if not raw:
        return set()

    allowlist: set[str] = set()
    for token in re.split(r"[,\n;]+", raw):
        email = token.strip().lower()
        if email:
            allowlist.add(email)
    return allowlist


def get_test_mode_home_allowlist() -> set[str]:
    """Get normalized TEST_MODE home-account allowlist."""
    return _parse_email_allowlist(get_settings().test_mode_allowed_home_emails)


def get_test_mode_client_allowlist() -> set[str]:
    """Get normalized TEST_MODE client-account allowlist."""
    return _parse_email_allowlist(get_settings().test_mode_allowed_client_emails)


def get_encryption_key() -> bytes:
    """Load encryption key from file."""
    settings = get_settings()
    key_file = settings.encryption_key_file

    if not os.path.exists(key_file):
        raise RuntimeError(
            f"Encryption key file not found at {key_file}. "
            "Complete the setup wizard first."
        )

    with open(key_file, "rb") as f:
        key = f.read()
        # Only strip trailing newlines/carriage returns that might be added by text editors
        # Don't use general .strip() as it can corrupt binary keys
        while key and key[-1:] in (b'\n', b'\r'):
            key = key[:-1]

    if len(key) < 32:
        raise RuntimeError("Invalid encryption key: must be at least 32 bytes")

    return key


def _read_or_create_session_secret(path: str) -> str:
    """Return the secret stored at ``path``, generating and persisting
    a fresh random one (owner-readable only) on first use."""
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            stored = f.read().strip()
        if stored:
            return stored
    import secrets
    new_secret = secrets.token_urlsafe(48)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(new_secret)
    os.chmod(path, 0o600)
    return new_secret


def get_session_secret() -> str:
    """Return the JWT signing secret.

    Resolution order:

    1. an explicitly configured ``SESSION_SECRET_KEY``;
    2. otherwise a random secret persisted to a ``session_secret`` file
       beside the encryption key.  This is deliberately independent of
       the encryption key — deriving it from the encryption key would
       make the two a single point of compromise;
    3. before that directory exists (pre-OOBE) a per-process random
       secret is used.

    Cases 1 and 2 are cached for the lifetime of the process.
    """
    global _session_secret_cache, _oobe_session_secret
    if _session_secret_cache is not None:
        return _session_secret_cache

    settings = get_settings()
    if settings.session_secret_key:
        _session_secret_cache = settings.session_secret_key
        return _session_secret_cache

    secret_dir = os.path.dirname(settings.encryption_key_file) or "."
    if os.path.isdir(secret_dir):
        secret = _read_or_create_session_secret(
            os.path.join(secret_dir, "session_secret")
        )
        _session_secret_cache = secret
        return secret

    # No persistent location yet (pre-OOBE) — per-process secret.
    if _oobe_session_secret is None:
        import secrets
        _oobe_session_secret = secrets.token_urlsafe(32)
    return _oobe_session_secret
