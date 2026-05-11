"""Test that the Stage-5 cutover migration is idempotent against
pre-cutover databases.

A real user database that survives the cutover has rows in tables
that no longer exist in the post-cutover ``SCHEMA``:
``event_mappings`` and ``busy_blocks``.  The migration in
:func:`app.database.init_schema` calls ``DROP TABLE IF EXISTS`` on
those tables; this test exercises that path end-to-end.

It also checks that user-data tables (``users``, ``oauth_tokens``,
``client_calendars``) survive untouched.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import aiosqlite
import pytest

from app.database import init_schema


@pytest.mark.asyncio
async def test_cutover_migration_drops_legacy_tables_and_preserves_user_data():
    """Build a pre-cutover database, run init_schema, verify the
    cutover happens cleanly and user data survives."""

    # Set up a SQLite file pretending to be a pre-cutover database:
    # the LEGACY tables are present; everything else gets created
    # fresh by ``init_schema``.
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        with sqlite3.connect(db_path) as legacy:
            legacy.executescript(
                """
                -- User data we want to preserve across the cutover.
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    google_user_id TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    main_calendar_id TEXT,
                    is_admin BOOLEAN DEFAULT FALSE,
                    sa_tier INTEGER DEFAULT 0,
                    sync_paused BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login_at TIMESTAMP
                );
                CREATE TABLE oauth_tokens (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    account_type TEXT NOT NULL,
                    google_account_email TEXT NOT NULL,
                    access_token_encrypted BLOB NOT NULL,
                    refresh_token_encrypted BLOB NOT NULL,
                    token_expiry TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP,
                    UNIQUE(user_id, google_account_email)
                );
                CREATE TABLE client_calendars (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    oauth_token_id INTEGER NOT NULL,
                    google_calendar_id TEXT NOT NULL,
                    display_name TEXT,
                    color_id TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    disconnected_at TIMESTAMP,
                    calendar_type TEXT NOT NULL DEFAULT 'client'
                );
                -- Legacy tables — should be DROPped by init_schema.
                CREATE TABLE event_mappings (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    origin_type TEXT NOT NULL,
                    origin_calendar_id INTEGER,
                    origin_event_id TEXT NOT NULL,
                    main_event_id TEXT,
                    is_recurring BOOLEAN DEFAULT FALSE,
                    deleted_at TIMESTAMP
                );
                CREATE TABLE busy_blocks (
                    id INTEGER PRIMARY KEY,
                    event_mapping_id INTEGER NOT NULL,
                    client_calendar_id INTEGER NOT NULL,
                    busy_block_event_id TEXT NOT NULL
                );
                """
            )
            # Seed data the user would not want to lose.
            legacy.execute(
                """INSERT INTO users (id, email, google_user_id, display_name,
                                       main_calendar_id, is_admin)
                   VALUES (1, 'alice@example.com', 'g-alice', 'Alice',
                           'alice@example.com', 1)"""
            )
            legacy.execute(
                """INSERT INTO oauth_tokens
                      (user_id, account_type, google_account_email,
                       access_token_encrypted, refresh_token_encrypted)
                   VALUES (1, 'home', 'alice@example.com', x'00', x'00')"""
            )
            legacy.execute(
                """INSERT INTO client_calendars
                      (user_id, oauth_token_id, google_calendar_id,
                       display_name, is_active)
                   VALUES (1, 1, 'work@cal', 'Work', 1)"""
            )
            # Pre-cutover ledger-irrelevant rows we expect to be dropped.
            legacy.execute(
                """INSERT INTO event_mappings
                      (user_id, origin_type, origin_event_id, is_recurring)
                   VALUES (1, 'main', 'old-evt-1', 0)"""
            )
            legacy.execute(
                """INSERT INTO busy_blocks
                      (event_mapping_id, client_calendar_id, busy_block_event_id)
                   VALUES (1, 1, 'old-busy-1')"""
            )
            legacy.commit()

        # Now open with aiosqlite and run init_schema (the cutover
        # migration that drops legacy tables + creates the ledger).
        db = await aiosqlite.connect(db_path)
        try:
            db.row_factory = aiosqlite.Row
            await init_schema(db)

            # Legacy tables: gone.
            for legacy_table in ("event_mappings", "busy_blocks"):
                row = await (await db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (legacy_table,),
                )).fetchone()
                assert row is None, f"{legacy_table} should have been dropped"

            # Ledger tables: present.
            for ledger_table in (
                "ledger_events", "ledger_projections",
                "outbox_operations", "reconcile_requests",
            ):
                row = await (await db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (ledger_table,),
                )).fetchone()
                assert row is not None, f"{ledger_table} should have been created"

            # User data: untouched.
            row = await (await db.execute(
                "SELECT email FROM users WHERE id = 1",
            )).fetchone()
            assert row["email"] == "alice@example.com"

            row = await (await db.execute(
                "SELECT google_account_email FROM oauth_tokens WHERE user_id = 1",
            )).fetchone()
            assert row["google_account_email"] == "alice@example.com"

            row = await (await db.execute(
                "SELECT google_calendar_id FROM client_calendars WHERE user_id = 1",
            )).fetchone()
            assert row["google_calendar_id"] == "work@cal"

            # Repeat call is idempotent.
            await init_schema(db)
        finally:
            await db.close()
    finally:
        try:
            os.remove(db_path)
        except OSError:
            pass


@pytest.mark.asyncio
async def test_cutover_migration_on_fresh_database_is_clean():
    """A brand-new DB (no legacy tables) gets the post-cutover
    schema with no errors."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        db = await aiosqlite.connect(db_path)
        try:
            db.row_factory = aiosqlite.Row
            await init_schema(db)
            # Ledger tables: present.  Legacy tables: never created.
            for ledger_table in ("ledger_events", "outbox_operations"):
                row = await (await db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (ledger_table,),
                )).fetchone()
                assert row is not None
            for legacy_table in ("event_mappings", "busy_blocks"):
                row = await (await db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (legacy_table,),
                )).fetchone()
                assert row is None
        finally:
            await db.close()
    finally:
        try:
            os.remove(db_path)
        except OSError:
            pass
