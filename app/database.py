"""Database connection and schema management."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

import aiosqlite

from app.config import get_settings

logger = logging.getLogger(__name__)

# Global database connection pool
_db_connection: Optional[aiosqlite.Connection] = None
_db_lock = asyncio.Lock()


SCHEMA = """
-- The home organization (single row)
CREATE TABLE IF NOT EXISTS organization (
    id INTEGER PRIMARY KEY,
    google_workspace_domain TEXT NOT NULL UNIQUE,
    google_client_id_encrypted BLOB NOT NULL,
    google_client_secret_encrypted BLOB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP
);

-- System settings
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value_encrypted BLOB,
    value_plain TEXT,
    is_sensitive BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Users in the home org
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    google_user_id TEXT NOT NULL UNIQUE,
    display_name TEXT,
    main_calendar_id TEXT,
    is_admin BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

-- OAuth tokens (encrypted at rest)
CREATE TABLE IF NOT EXISTS oauth_tokens (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_type TEXT NOT NULL,
    google_account_email TEXT NOT NULL,
    access_token_encrypted BLOB NOT NULL,
    refresh_token_encrypted BLOB NOT NULL,
    token_expiry TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(user_id, google_account_email)
);

-- Connected client calendars
CREATE TABLE IF NOT EXISTS client_calendars (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    oauth_token_id INTEGER NOT NULL REFERENCES oauth_tokens(id),
    google_calendar_id TEXT NOT NULL,
    display_name TEXT,
    color_id TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    disconnected_at TIMESTAMP
);

-- Tracks sync state for each calendar
CREATE TABLE IF NOT EXISTS calendar_sync_state (
    id INTEGER PRIMARY KEY,
    client_calendar_id INTEGER NOT NULL REFERENCES client_calendars(id) ON DELETE CASCADE,
    sync_token TEXT,
    last_full_sync TIMESTAMP,
    last_incremental_sync TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    last_error TEXT,
    UNIQUE(client_calendar_id)
);

-- Audit log
CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    calendar_id INTEGER REFERENCES client_calendars(id),
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_log_created ON sync_log(created_at);
CREATE INDEX IF NOT EXISTS idx_sync_log_user ON sync_log(user_id, created_at);

-- Webhook registrations
CREATE TABLE IF NOT EXISTS webhook_channels (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    calendar_type TEXT NOT NULL,
    client_calendar_id INTEGER REFERENCES client_calendars(id),
    channel_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    token TEXT NOT NULL DEFAULT '',
    expiration TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_webhook_expiration ON webhook_channels(expiration);

-- Email alert queue
CREATE TABLE IF NOT EXISTS alert_queue (
    id INTEGER PRIMARY KEY,
    alert_type TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job locking
CREATE TABLE IF NOT EXISTS job_locks (
    job_name TEXT PRIMARY KEY,
    locked_at TIMESTAMP,
    locked_by TEXT
);

-- Main calendar sync state (for tracking main calendar changes)
CREATE TABLE IF NOT EXISTS main_calendar_sync_state (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sync_token TEXT,
    last_full_sync TIMESTAMP,
    last_incremental_sync TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    last_error TEXT,
    UNIQUE(user_id)
);

-- OAuth state storage (replaces in-memory dict)
CREATE TABLE IF NOT EXISTS oauth_states (
    state TEXT PRIMARY KEY,
    state_type TEXT NOT NULL,
    user_id INTEGER,
    next_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_oauth_states_expiry ON oauth_states(expires_at);

-- Per-user integrity monitoring status (one row per user, upserted after each check)
CREATE TABLE IF NOT EXISTS integrity_status (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    last_check_at TIMESTAMP,
    issues_found INTEGER DEFAULT 0,
    issues_auto_fixed INTEGER DEFAULT 0,
    unresolved_issues INTEGER DEFAULT 0,
    consecutive_check_failures INTEGER DEFAULT 0,
    details_json TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Webcal/ICS subscriptions
CREATE TABLE IF NOT EXISTS webcal_subscriptions (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    display_prefix TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN DEFAULT TRUE,
    poll_interval_minutes INTEGER DEFAULT 5,
    last_poll_at TIMESTAMP,
    last_etag TEXT,
    last_success_at TIMESTAMP,
    consecutive_failures INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(user_id, url)
);
"""


async def get_database() -> aiosqlite.Connection:
    """Get the database connection, creating it if necessary."""
    global _db_connection

    async with _db_lock:
        if _db_connection is None:
            settings = get_settings()
            _db_connection = await aiosqlite.connect(
                settings.database_path, isolation_level=None
            )
            _db_connection.row_factory = aiosqlite.Row
            await _db_connection.execute("PRAGMA foreign_keys = ON")
            await _db_connection.execute("PRAGMA journal_mode = WAL")
            # Without this, a writer that hits SQLite's write lock fails
            # immediately with "database is locked".  The whole app shares
            # this one autocommit connection across ~10 scheduler jobs,
            # webhook drains, and UI requests, so colliding writes are
            # routine — wait for the lock instead of erroring out.
            await _db_connection.execute("PRAGMA busy_timeout = 5000")
            await init_schema(_db_connection)
        return _db_connection


async def init_schema(db: aiosqlite.Connection) -> None:
    """Initialize database schema."""
    await db.executescript(SCHEMA)
    await db.commit()

    # Migrations for columns added after initial release.
    # ALTER TABLE IF NOT EXISTS ... ADD COLUMN is not supported in older
    # SQLite, so we swallow the "duplicate column" error instead.
    # All event_mappings / sa_tier migrations have been removed at the
    # Stage-5 cutover; the legacy tables they targeted are no longer in
    # the schema.  A `DROP TABLE IF EXISTS` pass below cleans up
    # databases that survived from before the cutover.
    migrations = [
        "ALTER TABLE webhook_channels ADD COLUMN token TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE client_calendars ADD COLUMN calendar_type TEXT NOT NULL DEFAULT 'client'",
        "ALTER TABLE users ADD COLUMN sync_paused BOOLEAN DEFAULT FALSE",
    ]
    for stmt in migrations:
        try:
            await db.execute(stmt)
            await db.commit()
        except Exception:
            pass  # column already exists

    # Drop pre-cutover legacy tables if they exist (no-op on fresh DBs).
    for legacy_table in ("busy_blocks", "event_mappings"):
        try:
            await db.execute(f"DROP TABLE IF EXISTS {legacy_table}")
            await db.commit()
        except Exception:
            pass

    # Ledger tables (REWRITE_PLAN.md §4).  Additive — they sit
    # alongside the legacy event_mappings/busy_blocks until the
    # Stage-5 cutover.
    try:
        from app.ledger.schema import init_ledger_schema
        await init_ledger_schema(db)
    except Exception as e:
        logger.warning("ledger schema init failed (non-fatal): %s", e)

    logger.info("Database schema initialized")


async def close_database() -> None:
    """Close the database connection."""
    global _db_connection

    async with _db_lock:
        if _db_connection is not None:
            await _db_connection.close()
            _db_connection = None
            logger.info("Database connection closed")


async def replace_database_file(source_db_path: str, dest_db_path: str) -> None:
    """Swap the database file at ``dest_db_path`` for the contents of
    ``source_db_path``.

    The shared connection is closed first and reopens lazily on the
    new file via :func:`get_database`.  The close + file swap happen
    under ``_db_lock``, so no concurrent ``get_database`` can reopen a
    connection on a half-written file.

    Callers MUST be holding maintenance mode (see :mod:`app.maintenance`)
    so the scheduler / reconciler / webhook paths are frozen and no
    in-flight query is racing the swap.  Stale ``-wal`` / ``-shm``
    sidecars from the old database are removed so SQLite cannot apply
    a mismatched write-ahead log to the new file.

    ``dest_db_path`` must be a real filesystem path — an in-memory
    (``:memory:``) database has no file to replace.

    The swap is crash-safe: the backup is copied to a staging file in
    the destination's own directory, fsync'd, then ``os.replace``'d
    over the destination — an atomic rename.  A crash mid-copy leaves
    the original database intact; only a fully-written file ever
    appears at ``dest_db_path``.
    """
    import os
    import shutil
    import tempfile

    if not dest_db_path or dest_db_path == ":memory:":
        raise RuntimeError(
            "replace_database_file needs a file-backed database; "
            f"got {dest_db_path!r}"
        )

    global _db_connection
    async with _db_lock:
        if _db_connection is not None:
            await _db_connection.close()
            _db_connection = None
        for suffix in ("-wal", "-shm"):
            try:
                os.remove(dest_db_path + suffix)
            except FileNotFoundError:
                pass
        dest_dir = os.path.dirname(os.path.abspath(dest_db_path)) or "."
        fd, staging = tempfile.mkstemp(dir=dest_dir, suffix=".restore")
        os.close(fd)
        try:
            shutil.copyfile(source_db_path, staging)
            with open(staging, "rb") as f:
                os.fsync(f.fileno())
            os.replace(staging, dest_db_path)
        except BaseException:
            try:
                os.remove(staging)
            except FileNotFoundError:
                pass
            raise
        # fsync the directory so the rename itself is durable.
        try:
            dir_fd = os.open(dest_dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
        logger.info("Database file replaced from %s", source_db_path)


@asynccontextmanager
async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    """Context manager for getting database connection."""
    db = await get_database()
    try:
        yield db
    finally:
        pass  # Connection is managed globally


async def is_oobe_completed() -> bool:
    """Check if the Out-of-Box Experience has been completed."""
    db = await get_database()
    cursor = await db.execute("SELECT COUNT(*) FROM organization")
    row = await cursor.fetchone()
    return row[0] > 0


async def get_organization() -> Optional[dict]:
    """Get the organization configuration."""
    db = await get_database()
    cursor = await db.execute("SELECT * FROM organization LIMIT 1")
    row = await cursor.fetchone()
    if row:
        return dict(row)
    return None


async def get_setting(key: str) -> Optional[dict]:
    """Get a setting by key."""
    db = await get_database()
    cursor = await db.execute(
        "SELECT * FROM settings WHERE key = ?", (key,)
    )
    row = await cursor.fetchone()
    if row:
        return dict(row)
    return None


async def set_setting(
    key: str,
    value: str,
    is_sensitive: bool = False,
    encrypt_func=None
) -> None:
    """Set a setting value."""
    db = await get_database()
    now = datetime.utcnow().isoformat()

    if is_sensitive and encrypt_func:
        value_encrypted = encrypt_func(value)
        await db.execute(
            """INSERT INTO settings (key, value_encrypted, is_sensitive, updated_at)
               VALUES (?, ?, TRUE, ?)
               ON CONFLICT(key) DO UPDATE SET
               value_encrypted = excluded.value_encrypted,
               is_sensitive = TRUE,
               updated_at = excluded.updated_at""",
            (key, value_encrypted, now)
        )
    else:
        await db.execute(
            """INSERT INTO settings (key, value_plain, is_sensitive, updated_at)
               VALUES (?, ?, FALSE, ?)
               ON CONFLICT(key) DO UPDATE SET
               value_plain = excluded.value_plain,
               is_sensitive = FALSE,
               updated_at = excluded.updated_at""",
            (key, value, now)
        )
    await db.commit()
